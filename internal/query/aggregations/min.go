package aggregations

import (
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/utils"
)

var (
	_ AggregationFunction = (*MinFn)(nil)
)

// MinFn implements the MIN aggregation function.
type MinFn struct {
	fldname string
	val     any
}

func NewMinFn(fldname string) *MinFn {
	return &MinFn{
		fldname: fldname,
	}
}

// ProcessFirst processes the first record in the group.
func (m *MinFn) ProcessFirst(s scan.Scan) error {
	val, err := s.GetValue(m.fldname)
	if err != nil {
		return err
	}

	m.val = val
	return nil
}

// ProcessNext processes the next record in the group.
func (m *MinFn) ProcessNext(s scan.Scan) error {
	val, err := s.GetValue(m.fldname)
	if err != nil {
		return err
	}

	if utils.CompareValues(val, m.val) < 0 {
		m.val = val
	}

	return nil
}

// FieldName returns the name of the aggregation field.
func (m *MinFn) FieldName() string {
	return "minof" + m.fldname
}

// Value returns the computed minimum value.
func (m *MinFn) Value() any {
	return m.val
}
