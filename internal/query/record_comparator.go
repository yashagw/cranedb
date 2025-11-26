package query

import (
	"github.com/yashagw/cranedb/internal/scan"
)

// RecordComparator compares two scans based on a list of field names.
// It compares fields in order, returning the first non-zero comparison result.
type RecordComparator struct {
	fields []string
}

// NewRecordComparator creates a new RecordComparator with the given sort fields.
func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{
		fields: fields,
	}
}

// Fields returns the fields to sort by.
func (rc *RecordComparator) Fields() []string {
	return rc.fields
}

// Compare compares two scans based on the sort fields.
// Returns -1 if s1 < s2, 0 if s1 == s2, 1 if s1 > s2.
func (rc *RecordComparator) Compare(s1, s2 scan.Scan) (int, error) {
	for _, fldname := range rc.fields {
		val1, err := s1.GetValue(fldname)
		if err != nil {
			return 0, err
		}
		val2, err := s2.GetValue(fldname)
		if err != nil {
			return 0, err
		}

		var const1, const2 *Constant
		switch v := val1.(type) {
		case int:
			const1 = NewIntConstant(v)
		case string:
			const1 = NewStringConstant(v)
		default:
			intVal, err := s1.GetInt(fldname)
			if err == nil {
				const1 = NewIntConstant(intVal)
			} else {
				strVal, err := s1.GetString(fldname)
				if err != nil {
					return 0, err
				}
				const1 = NewStringConstant(strVal)
			}
		}

		switch v := val2.(type) {
		case int:
			const2 = NewIntConstant(v)
		case string:
			const2 = NewStringConstant(v)
		default:
			intVal, err := s2.GetInt(fldname)
			if err == nil {
				const2 = NewIntConstant(intVal)
			} else {
				strVal, err := s2.GetString(fldname)
				if err != nil {
					return 0, err
				}
				const2 = NewStringConstant(strVal)
			}
		}

		result := const1.CompareTo(const2)
		if result != 0 {
			return result, nil
		}
	}
	return 0, nil
}
