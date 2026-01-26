package mvcc

import (
	"log/slog"

	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// Vacuum scans a table and reclaims dead tuple slots.
// A slot is dead if:
//   - xmax != 0 (has been deleted/superseded)
//   - xmax transaction has committed
//   - xmax < oldestActiveTx (no active transaction can see this version)
//
// Returns the number of slots reclaimed.
func Vacuum(
	tableName string,
	layout *record.Layout,
	tx *transaction.Transaction,
	commitLog *transaction.CommitLog,
	indexInfo map[string]*metadata.IndexInfo,
	oldestActiveTx int64,
) (int, error) {
	ts, err := table.NewTableScan(tx, layout, tableName)
	if err != nil {
		return 0, err
	}
	defer ts.Close()

	reclaimed := 0
	for {
		hasNext, err := ts.NextRaw()
		if err != nil {
			return reclaimed, err
		}
		if !hasNext {
			break
		}

		xmax, err := ts.GetXmax()
		if err != nil {
			return reclaimed, err
		}

		// Not deleted
		if xmax == 0 {
			continue
		}

		// Deleting transaction not committed
		if !commitLog.IsCommitted(xmax) {
			continue
		}

		// Some active transaction might still see this version
		if xmax >= oldestActiveTx {
			continue
		}

		// This version is dead - remove index entries and reclaim slot
		for fieldName, ii := range indexInfo {
			val, err := ts.GetValue(fieldName)
			if err != nil {
				slog.Warn("vacuum: failed to get field value for index cleanup", "field", fieldName, "error", err)
				continue
			}
			rid, err := ts.GetRID()
			if err != nil {
				slog.Warn("vacuum: failed to get RID for index cleanup", "error", err)
				continue
			}
			idx, err := ii.Open()
			if err != nil {
				slog.Warn("vacuum: failed to open index", "field", fieldName, "error", err)
				continue
			}
			err = idx.Delete(val, rid)
			if err != nil {
				slog.Warn("vacuum: failed to delete index entry", "field", fieldName, "error", err)
			}
			idx.Close()
		}

		err = ts.ReclaimSlot()
		if err != nil {
			return reclaimed, err
		}
		reclaimed++
	}

	return reclaimed, nil
}
