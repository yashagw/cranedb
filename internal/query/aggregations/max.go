package aggregations

import (
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/utils"
)

var (
	_ AggregationFunction = (*MaxFn)(nil)
)

// MaxFn implements the MAX aggregation function.
type MaxFn struct {
	fldname string
	val     any
}

func NewMaxFn(fldname string) *MaxFn {
	return &MaxFn{
		fldname: fldname,
	}
}

// ProcessFirst processes the first record in the group.
func (m *MaxFn) ProcessFirst(s scan.Scan) error {
	val, err := s.GetValue(m.fldname)
	if err != nil {
		return err
	}

	m.val = val
	return nil
}

// ProcessNext processes the next record in the group.
func (m *MaxFn) ProcessNext(s scan.Scan) error {
	val, err := s.GetValue(m.fldname)
	if err != nil {
		return err
	}

	if utils.CompareValues(val, m.val) > 0 {
		m.val = val
	}

	return nil
}

// FieldName returns the name of the aggregation field.
func (m *MaxFn) FieldName() string {
	return "maxof" + m.fldname
}

// Value returns the computed maximum value.
func (m *MaxFn) Value() any {
	return m.val
}
