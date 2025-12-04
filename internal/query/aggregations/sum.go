package aggregations

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ AggregationFunction = (*SumFn)(nil)
)

// SumFn implements the SUM aggregation function.
type SumFn struct {
	fldname string
	sum     any
}

func NewSumFn(fldname string) *SumFn {
	return &SumFn{
		fldname: fldname,
	}
}

// ProcessFirst processes the first record in the group.
func (s *SumFn) ProcessFirst(scan scan.Scan) error {
	val, err := scan.GetValue(s.fldname)
	if err != nil {
		return err
	}

	s.sum = val
	return nil
}

// ProcessNext processes the next record in the group.
func (s *SumFn) ProcessNext(scan scan.Scan) error {
	val, err := scan.GetValue(s.fldname)
	if err != nil {
		return err
	}

	// Add the value to the sum based on type
	switch sumVal := s.sum.(type) {
	case int:
		switch valInt := val.(type) {
		case int:
			s.sum = sumVal + valInt
		case float64:
			s.sum = float64(sumVal) + valInt
		default:
			return fmt.Errorf("cannot sum non-numeric value for field %s", s.fldname)
		}
	case float64:
		switch valFloat := val.(type) {
		case int:
			s.sum = sumVal + float64(valFloat)
		case float64:
			s.sum = sumVal + valFloat
		default:
			return fmt.Errorf("cannot sum non-numeric value for field %s", s.fldname)
		}
	default:
		return fmt.Errorf("cannot sum non-numeric value for field %s", s.fldname)
	}

	return nil
}

// FieldName returns the name of the aggregation field.
func (s *SumFn) FieldName() string {
	return "sumof" + s.fldname
}

// Value returns the computed sum value.
func (s *SumFn) Value() any {
	return s.sum
}



