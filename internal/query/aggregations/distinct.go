package aggregations

import (
	"fmt"
	"strings"

	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ AggregationFunction = (*DistinctFn)(nil)
)

// DistinctFn implements the DISTINCT aggregation function.
type DistinctFn struct {
	fldname string
	values  []any
	seen    map[any]struct{}
}

func NewDistinctFn(fldname string) *DistinctFn {
	return &DistinctFn{
		fldname: fldname,
		seen:    make(map[any]struct{}),
	}
}

// ProcessFirst processes the first record in the group.
func (d *DistinctFn) ProcessFirst(s scan.Scan) error {
	val, err := s.GetValue(d.fldname)
	if err != nil {
		return err
	}

	d.values = []any{val}
	d.seen = map[any]struct{}{val: {}}
	return nil
}

// ProcessNext processes the next record in the group.
func (d *DistinctFn) ProcessNext(s scan.Scan) error {
	val, err := s.GetValue(d.fldname)
	if err != nil {
		return err
	}

	if _, exists := d.seen[val]; !exists {
		d.values = append(d.values, val)
		d.seen[val] = struct{}{}
	}

	return nil
}

// FieldName returns the name of the aggregation field.
func (d *DistinctFn) FieldName() string {
	return "distinctof" + d.fldname
}

// Value returns the computed distinct values as a comma-separated string.
func (d *DistinctFn) Value() any {
	strValues := make([]string, len(d.values))
	for i, v := range d.values {
		strValues[i] = fmt.Sprint(v)
	}
	return strings.Join(strValues, ", ")
}
