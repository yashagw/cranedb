package query

import (
	"fmt"
	"slices"

	"github.com/yashagw/cranedb/internal/query/aggregations"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ scan.Scan = (*GroupByScan)(nil)
)

// GroupByScan implements grouping and aggregation operations.
// It groups records by specified fields and applies aggregation functions to each group.
// The input scan should be sorted by the grouping fields.
type GroupByScan struct {
	s           scan.Scan
	groupFields []string
	aggFns      []aggregations.AggregationFunction
	groupVal    *GroupValue
	moreGroups  bool
}

// NewGroupByScan creates a new GroupByScan.
// The input scan should be sorted by the grouping fields.
func NewGroupByScan(s scan.Scan, groupFields []string, aggFns []aggregations.AggregationFunction) (*GroupByScan, error) {
	gbs := &GroupByScan{
		s:           s,
		groupFields: groupFields,
		aggFns:      aggFns,
	}

	if err := gbs.BeforeFirst(); err != nil {
		return nil, err
	}

	return gbs, nil
}

// BeforeFirst positions the scan before the first group.
func (gbs *GroupByScan) BeforeFirst() error {
	if err := gbs.s.BeforeFirst(); err != nil {
		return err
	}

	hasNext, err := gbs.s.Next()
	if err != nil {
		return err
	}
	gbs.moreGroups = hasNext

	return nil
}

// Next moves to the next group and processes all records in that group.
// It processes the first record to initialize aggregations, then continues reading
// records until a different group is found. Returns true if there is a next group.
func (gbs *GroupByScan) Next() (bool, error) {
	if !gbs.moreGroups {
		return false, nil
	}

	// Process the first record of the group
	for _, fn := range gbs.aggFns {
		if err := fn.ProcessFirst(gbs.s); err != nil {
			return false, err
		}
	}

	// Create the group value for the current group
	var err error
	gbs.groupVal, err = NewGroupValue(gbs.s, gbs.groupFields)
	if err != nil {
		return false, err
	}

	// Process remaining records in the same group
	for {
		hasNext, err := gbs.s.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			gbs.moreGroups = false
			break
		}

		// Check if this record belongs to the same group
		gv, err := NewGroupValue(gbs.s, gbs.groupFields)
		if err != nil {
			return false, err
		}

		if !gbs.groupVal.Equals(gv) {
			// Different group - we'll process it in the next call to Next()
			break
		}

		// Same group - process this record
		for _, fn := range gbs.aggFns {
			if err := fn.ProcessNext(gbs.s); err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

func (gbs *GroupByScan) GetInt(fldname string) (int, error) {
	val, err := gbs.GetValue(fldname)
	if err != nil {
		return 0, err
	}

	intVal, ok := val.(int)
	if !ok {
		return 0, fmt.Errorf("field %s is not an integer", fldname)
	}

	return intVal, nil
}

func (gbs *GroupByScan) GetBool(fldname string) (bool, error) {
	val, err := gbs.GetValue(fldname)
	if err != nil {
		return false, err
	}

	boolVal, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("field %s is not a boolean", fldname)
	}

	return boolVal, nil
}

func (gbs *GroupByScan) GetString(fldname string) (string, error) {
	val, err := gbs.GetValue(fldname)
	if err != nil {
		return "", err
	}

	strVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("field %s is not a string", fldname)
	}

	return strVal, nil
}

// GetValue returns the value of the specified field.
// It returns either a group field value or an aggregation function value.
func (gbs *GroupByScan) GetValue(fldname string) (any, error) {
	if slices.Contains(gbs.groupFields, fldname) {
		if gbs.groupVal == nil {
			return nil, fmt.Errorf("no current group")
		}
		// Return the primitive value directly
		val := gbs.groupVal.GetVal(fldname)
		if val == nil {
			return nil, fmt.Errorf("no value for field %s", fldname)
		}
		return val, nil
	}

	for _, fn := range gbs.aggFns {
		if fn.FieldName() == fldname {
			// Aggregation functions return primitive types (int or string)
			return fn.Value(), nil
		}
	}

	return nil, fmt.Errorf("no field %s", fldname)
}

func (gbs *GroupByScan) HasField(fldname string) bool {
	if slices.Contains(gbs.groupFields, fldname) {
		return true
	}

	for _, fn := range gbs.aggFns {
		if fn.FieldName() == fldname {
			return true
		}
	}

	return false
}

func (gbs *GroupByScan) Close() {
	gbs.s.Close()
}
