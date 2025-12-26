package query

import (
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ scan.Scan = (*LimitScan)(nil)
)

type LimitScan struct {
	input       scan.Scan
	limit       int
	offset      int
	count       int
	offsetEnded bool
}

func NewLimitScan(input scan.Scan, limit int, offset int) *LimitScan {
	return &LimitScan{
		input:  input,
		limit:  limit,
		offset: offset,
	}
}

func (s *LimitScan) BeforeFirst() error {
	s.count = 0
	s.offsetEnded = false
	return s.input.BeforeFirst()
}

func (s *LimitScan) Next() (bool, error) {
	if !s.offsetEnded {
		for i := 0; i < s.offset; i++ {
			hasNext, err := s.input.Next()
			if err != nil {
				return false, err
			}
			if !hasNext {
				return false, nil
			}
		}
		s.offsetEnded = true
	}

	if s.limit > 0 && s.count >= s.limit {
		return false, nil
	}

	hasNext, err := s.input.Next()
	if err != nil {
		return false, err
	}
	if hasNext {
		s.count++
	}
	return hasNext, nil
}

func (s *LimitScan) GetInt(fldname string) (int, error) {
	return s.input.GetInt(fldname)
}

func (s *LimitScan) GetBool(fldname string) (bool, error) {
	return s.input.GetBool(fldname)
}

func (s *LimitScan) GetString(fldname string) (string, error) {
	return s.input.GetString(fldname)
}

func (s *LimitScan) GetValue(fldname string) (any, error) {
	return s.input.GetValue(fldname)
}

func (s *LimitScan) HasField(fldname string) bool {
	return s.input.HasField(fldname)
}

func (s *LimitScan) Close() {
	s.input.Close()
}
