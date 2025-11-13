package query

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ scan.UpdateScan = (*SelectScan)(nil)
)

type SelectScan struct {
	input     scan.Scan
	predicate Predicate
}

func NewSelectScan(input scan.Scan, predicate Predicate) *SelectScan {
	return &SelectScan{
		input:     input,
		predicate: predicate,
	}
}

func (s *SelectScan) BeforeFirst() error {
	return s.input.BeforeFirst()
}

func (s *SelectScan) Next() (bool, error) {
	for {
		hasNext, err := s.input.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			return false, nil
		}
		satisfied, err := s.predicate.IsSatisfied(s.input)
		if err != nil {
			return false, err
		}
		if satisfied {
			return true, nil
		}
	}
}

func (s *SelectScan) GetInt(fldname string) (int, error) {
	return s.input.GetInt(fldname)
}

func (s *SelectScan) GetString(fldname string) (string, error) {
	return s.input.GetString(fldname)
}

func (s *SelectScan) GetValue(fldname string) (any, error) {
	return s.input.GetValue(fldname)
}

func (s *SelectScan) HasField(fldname string) bool {
	return s.input.HasField(fldname)
}

func (s *SelectScan) Close() {
	s.input.Close()
}

func (s *SelectScan) SetInt(fldname string, val int) error {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf("input is not an scan.UpdateScan")
	}
	return updateScan.SetInt(fldname, val)
}

func (s *SelectScan) SetString(fldname string, val string) error {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf("input is not an scan.UpdateScan")
	}
	return updateScan.SetString(fldname, val)
}

func (s *SelectScan) Insert() error {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf("input is not an scan.UpdateScan")
	}
	return updateScan.Insert()
}

func (s *SelectScan) Delete() error {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf("input is not an scan.UpdateScan")
	}
	return updateScan.Delete()
}

func (s *SelectScan) GetRID() (*record.RID, error) {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		return nil, fmt.Errorf("input is not an scan.UpdateScan")
	}
	return updateScan.GetRID()
}

func (s *SelectScan) MoveToRID(rid *record.RID) error {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf("input is not an scan.UpdateScan")
	}
	return updateScan.MoveToRID(rid)
}
