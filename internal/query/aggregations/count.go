package aggregations

import (
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ AggregationFunction = (*CountFn)(nil)
)

// CountFn implements the COUNT aggregation function.
type CountFn struct {
	fldname string
	count   int
}

func NewCountFn(fldname string) *CountFn {
	return &CountFn{
		fldname: fldname,
		count:   0,
	}
}

// ProcessFirst processes the first record in the group.
func (c *CountFn) ProcessFirst(s scan.Scan) error {
	_, err := s.GetValue(c.fldname)
	if err != nil {
		return err
	}

	c.count = 1
	return nil
}

// ProcessNext processes the next record in the group.
func (c *CountFn) ProcessNext(s scan.Scan) error {
	_, err := s.GetValue(c.fldname)
	if err != nil {
		return err
	}

	c.count++
	return nil
}

// FieldName returns the name of the aggregation field.
func (c *CountFn) FieldName() string {
	return "countof" + c.fldname
}

// Value returns the computed count value.
func (c *CountFn) Value() any {
	return c.count
}


