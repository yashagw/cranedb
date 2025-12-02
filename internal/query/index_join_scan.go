package query

import (
	"github.com/yashagw/cranedb/internal/index"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/table"
)

var (
	_ scan.Scan = (*IndexJoinScan)(nil)
)

// IndexJoinScan implements an index join scan.
// It joins a left-hand scan with a right-hand table scan using an index.
type IndexJoinScan struct {
	lhs       scan.Scan
	idx       index.Index
	joinField string
	rhs       *table.TableScan
	hasLHS    bool // tracks whether we have a current lhs record
}

// NewIndexJoinScan creates a new IndexJoinScan.
func NewIndexJoinScan(lhs scan.Scan, idx index.Index, joinField string, rhs *table.TableScan) (*IndexJoinScan, error) {
	ijs := &IndexJoinScan{
		lhs:       lhs,
		idx:       idx,
		joinField: joinField,
		rhs:       rhs,
	}
	err := ijs.BeforeFirst()
	if err != nil {
		return nil, err
	}
	return ijs, nil
}

// BeforeFirst positions the scan before the first record.
// It positions lhs before first, moves to the first record, and resets the index.
func (ijs *IndexJoinScan) BeforeFirst() error {
	if err := ijs.lhs.BeforeFirst(); err != nil {
		return err
	}
	hasNext, err := ijs.lhs.Next()
	if err != nil {
		return err
	}
	if !hasNext {
		// No records in lhs, nothing to join
		ijs.hasLHS = false
		return nil
	}
	ijs.hasLHS = true
	return ijs.resetIndex()
}

// Next moves to the next record in the join.
// It loops: if index has next, move rhs to that RID and return true.
// If not, advance lhs and reset index. Returns false when no more records.
func (ijs *IndexJoinScan) Next() (bool, error) {
	// If we don't have a current lhs record (empty lhs), return false immediately
	if !ijs.hasLHS {
		return false, nil
	}

	for {
		hasNext, err := ijs.idx.Next()
		if err != nil {
			return false, err
		}
		if hasNext {
			// Index has a matching record, get its RID and move rhs to it
			rid, err := ijs.idx.GetDataRid()
			if err != nil {
				return false, err
			}
			err = ijs.rhs.MoveToRID(rid)
			if err != nil {
				return false, err
			}
			return true, nil
		}

		// No more matches for current lhs record, advance lhs
		hasNext, err = ijs.lhs.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			// No more records in lhs
			ijs.hasLHS = false
			return false, nil
		}

		// Reset index for new lhs record
		if err := ijs.resetIndex(); err != nil {
			return false, err
		}
	}
}

// GetInt returns the integer value of the specified field.
// It checks rhs first, then lhs.
func (ijs *IndexJoinScan) GetInt(fldname string) (int, error) {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetInt(fldname)
	}
	return ijs.lhs.GetInt(fldname)
}

// GetBool returns the boolean value of the specified field.
// It checks rhs first, then lhs.
func (ijs *IndexJoinScan) GetBool(fldname string) (bool, error) {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetBool(fldname)
	}
	return ijs.lhs.GetBool(fldname)
}

// GetString returns the string value of the specified field.
// It checks rhs first, then lhs.
func (ijs *IndexJoinScan) GetString(fldname string) (string, error) {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetString(fldname)
	}
	return ijs.lhs.GetString(fldname)
}

// GetValue returns the value of the specified field.
// It checks rhs first, then lhs.
func (ijs *IndexJoinScan) GetValue(fldname string) (any, error) {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetValue(fldname)
	}
	return ijs.lhs.GetValue(fldname)
}

// HasField checks if the scan contains the specified field.
// It checks both rhs and lhs.
func (ijs *IndexJoinScan) HasField(fldname string) bool {
	return ijs.rhs.HasField(fldname) || ijs.lhs.HasField(fldname)
}

// Close closes all underlying scans and the index.
func (ijs *IndexJoinScan) Close() {
	ijs.lhs.Close()
	ijs.idx.Close()
	ijs.rhs.Close()
}

// resetIndex resets the index to search for the current join field value from lhs.
func (ijs *IndexJoinScan) resetIndex() error {
	searchKey, err := ijs.lhs.GetValue(ijs.joinField)
	if err != nil {
		return err
	}
	return ijs.idx.BeforeFirst(searchKey)
}
