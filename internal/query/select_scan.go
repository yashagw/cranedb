package query

import (
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

func (s *SelectScan) BeforeFirst() {
	s.input.BeforeFirst()
}

func (s *SelectScan) Next() bool {
	for s.input.Next() {
		if s.predicate.IsSatisfied(s.input) {
			return true
		}
	}
	return false
}

func (s *SelectScan) GetInt(fldname string) int {
	return s.input.GetInt(fldname)
}

func (s *SelectScan) GetString(fldname string) string {
	return s.input.GetString(fldname)
}

func (s *SelectScan) GetValue(fldname string) any {
	return s.input.GetValue(fldname)
}

func (s *SelectScan) HasField(fldname string) bool {
	return s.input.HasField(fldname)
}

func (s *SelectScan) Close() {
	s.input.Close()
}

func (s *SelectScan) SetInt(fldname string, val int) {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		panic("input is not an UpdateScan")
	}
	updateScan.SetInt(fldname, val)
}

func (s *SelectScan) SetString(fldname string, val string) {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		panic("input is not an UpdateScan")
	}
	updateScan.SetString(fldname, val)
}

func (s *SelectScan) Insert() {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		panic("input is not an UpdateScan")
	}
	updateScan.Insert()
}

func (s *SelectScan) Delete() {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		panic("input is not an UpdateScan")
	}
	updateScan.Delete()
}

func (s *SelectScan) GetRid() *record.RID {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		panic("input is not an UpdateScan")
	}
	return updateScan.GetRid()
}

func (s *SelectScan) MoveToRid(rid *record.RID) {
	updateScan, ok := s.input.(scan.UpdateScan)
	if !ok {
		panic("input is not an UpdateScan")
	}
	updateScan.MoveToRid(rid)
}
