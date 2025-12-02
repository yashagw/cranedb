package query

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/temptable"
)

var (
	_ scan.Scan = (*SortScan)(nil)
)

// SortScan merges two sorted runs from temporary tables.
// It implements a two-way merge of sorted data.
type SortScan struct {
	s1           scan.UpdateScan
	s2           scan.UpdateScan
	comp         *RecordComparator
	hasMore1     bool
	hasMore2     bool
	currentScan  scan.UpdateScan
	savedPos     []*record.RID
	savedCurrent scan.UpdateScan
}

// NewSortScan creates a new SortScan that merges the given sorted runs.
// It expects at least one run, and optionally a second run.
func NewSortScan(runs []*temptable.TempTable, comp *RecordComparator) (*SortScan, error) {
	if len(runs) == 0 {
		return nil, fmt.Errorf("at least one run is required")
	}

	ss := &SortScan{
		comp: comp,
	}

	// Open first run
	s1, err := runs[0].Open()
	if err != nil {
		return nil, err
	}
	ss.s1 = s1
	err = s1.BeforeFirst()
	if err != nil {
		s1.Close()
		return nil, err
	}
	hasMore1, err := s1.Next()
	if err != nil {
		s1.Close()
		return nil, err
	}
	ss.hasMore1 = hasMore1

	// Open second run if available
	if len(runs) > 1 {
		s2, err := runs[1].Open()
		if err != nil {
			s1.Close()
			return nil, err
		}
		ss.s2 = s2
		err = s2.BeforeFirst()
		if err != nil {
			s1.Close()
			s2.Close()
			return nil, err
		}
		hasMore2, err := s2.Next()
		if err != nil {
			s1.Close()
			s2.Close()
			return nil, err
		}
		ss.hasMore2 = hasMore2
	}

	return ss, nil
}

// BeforeFirst positions the scan before the first record.
func (ss *SortScan) BeforeFirst() error {
	err := ss.s1.BeforeFirst()
	if err != nil {
		return err
	}
	hasMore1, err := ss.s1.Next()
	if err != nil {
		return err
	}
	ss.hasMore1 = hasMore1

	if ss.s2 != nil {
		err = ss.s2.BeforeFirst()
		if err != nil {
			return err
		}
		hasMore2, err := ss.s2.Next()
		if err != nil {
			return err
		}
		ss.hasMore2 = hasMore2
	}

	ss.currentScan = nil
	return nil
}

// Next moves to the next record in the merged sorted order.
func (ss *SortScan) Next() (bool, error) {
	if ss.currentScan == ss.s1 && ss.s1 != nil {
		hasMore1, err := ss.s1.Next()
		if err != nil {
			return false, err
		}
		ss.hasMore1 = hasMore1
	} else if ss.currentScan == ss.s2 && ss.s2 != nil {
		hasMore2, err := ss.s2.Next()
		if err != nil {
			return false, err
		}
		ss.hasMore2 = hasMore2
	}

	// Check if we're done
	if !ss.hasMore1 && !ss.hasMore2 {
		return false, nil
	}

	// Determine which scan to use next
	if ss.hasMore1 && ss.hasMore2 {
		result, err := ss.comp.Compare(ss.s1, ss.s2)
		if err != nil {
			return false, err
		}
		if result < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}

	return true, nil
}

// GetInt returns the integer value of the specified field from the current record.
func (ss *SortScan) GetInt(fldname string) (int, error) {
	if ss.currentScan == nil {
		return 0, fmt.Errorf("no current record")
	}
	return ss.currentScan.GetInt(fldname)
}

// GetBool returns the boolean value of the specified field from the current record.
func (ss *SortScan) GetBool(fldname string) (bool, error) {
	if ss.currentScan == nil {
		return false, fmt.Errorf("no current record")
	}
	return ss.currentScan.GetBool(fldname)
}

// GetString returns the string value of the specified field from the current record.
func (ss *SortScan) GetString(fldname string) (string, error) {
	if ss.currentScan == nil {
		return "", fmt.Errorf("no current record")
	}
	return ss.currentScan.GetString(fldname)
}

// GetValue returns the value of the specified field from the current record.
func (ss *SortScan) GetValue(fldname string) (any, error) {
	if ss.currentScan == nil {
		return nil, fmt.Errorf("no current record")
	}
	return ss.currentScan.GetValue(fldname)
}

// HasField checks if the scan contains the specified field.
func (ss *SortScan) HasField(fldname string) bool {
	if ss.currentScan == nil {
		return false
	}
	return ss.currentScan.HasField(fldname)
}

// Close releases the resources held by the scan.
func (ss *SortScan) Close() {
	if ss.s1 != nil {
		ss.s1.Close()
	}
	if ss.s2 != nil {
		ss.s2.Close()
	}
}

// SavePosition saves the current position of both scans.
func (ss *SortScan) SavePosition() error {
	rid1, err := ss.s1.GetRID()
	if err != nil {
		return err
	}

	var rid2 *record.RID
	if ss.s2 != nil {
		rid2, err = ss.s2.GetRID()
		if err != nil {
			return err
		}
	}

	ss.savedPos = []*record.RID{rid1, rid2}
	ss.savedCurrent = ss.currentScan
	return nil
}

// RestorePosition restores the saved position of both scans.
func (ss *SortScan) RestorePosition() error {
	if len(ss.savedPos) < 1 {
		return fmt.Errorf("no saved position")
	}

	err := ss.s1.MoveToRID(ss.savedPos[0])
	if err != nil {
		return err
	}
	ss.hasMore1 = true

	if ss.s2 != nil && len(ss.savedPos) > 1 && ss.savedPos[1] != nil {
		err = ss.s2.MoveToRID(ss.savedPos[1])
		if err != nil {
			return err
		}
		ss.hasMore2 = true
	} else {
		ss.hasMore2 = false
	}

	ss.currentScan = ss.savedCurrent
	return nil
}
