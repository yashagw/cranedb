package transaction

import (
	"github.com/yashagw/cranedb/internal/buffer"
)

// RedoAndTrack calls record.Redo and, if the record was redone, fixes up the
// buffer LSN and dirty page table so the page can be flushed and recovered.
// If dpt is nil, dirty page tracking is skipped (used by crash recovery).
func RedoAndTrack(record DataModificationRecord, tx *Transaction, dpt *buffer.DirtyPageTable) error {
	redone, err := record.Redo(tx)
	if err != nil {
		return err
	}

	blk := record.Block()
	if redone {
		if dpt != nil {
			lsn := record.LSN()
			buff, pinErr := tx.Pin(blk)
			if pinErr == nil && buff != nil {
				buff.SetModifiedLSN(lsn)
			}
			dpt.Add(blk, lsn)
		}
		tx.Unpin(blk)
	}

	return nil
}
