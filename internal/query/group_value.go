package query

import (
	"slices"

	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/utils"
)

// GroupValue represents a group value for comparing records in GROUP BY operations.
type GroupValue struct {
	vals map[string]any
}

// NewGroupValue creates a new GroupValue from the current record in the scan.
func NewGroupValue(s scan.Scan, fields []string) (*GroupValue, error) {
	gv := &GroupValue{
		vals: make(map[string]any),
	}

	for _, fldname := range fields {
		val, err := s.GetValue(fldname)
		if err != nil {
			return nil, err
		}

		gv.vals[fldname] = val
	}

	return gv, nil
}

func (gv *GroupValue) GetVal(fldname string) any {
	return gv.vals[fldname]
}

// Equals checks if this GroupValue is equal to another GroupValue.
func (gv *GroupValue) Equals(other *GroupValue) bool {
	if len(gv.vals) != len(other.vals) {
		return false
	}

	for fldname, v1 := range gv.vals {
		v2 := other.GetVal(fldname)
		if v2 == nil {
			return false
		}

		// Compare values directly
		if v1 != v2 {
			return false
		}
	}

	return true
}

func (gv *GroupValue) HashCode() int {
	hashval := 0
	for _, val := range gv.vals {
		h, err := utils.HashValue(val)
		if err == nil {
			hashval += int(h)
		}
	}
	return hashval
}

func (gv *GroupValue) Contains(fldname string) bool {
	_, ok := gv.vals[fldname]
	return ok
}

func (gv *GroupValue) Fields() []string {
	fields := make([]string, 0, len(gv.vals))
	for fldname := range gv.vals {
		fields = append(fields, fldname)
	}
	slices.Sort(fields)
	return fields
}
