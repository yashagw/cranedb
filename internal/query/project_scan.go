package query

import (
	"slices"

	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ scan.Scan = (*ProjectScan)(nil)
)

type ProjectScan struct {
	input     scan.Scan
	fieldList []string
}

func NewProjectScan(input scan.Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		input:     input,
		fieldList: fieldList,
	}
}

func (s *ProjectScan) BeforeFirst() {
	s.input.BeforeFirst()
}

func (s *ProjectScan) Next() bool {
	return s.input.Next()
}

func (s *ProjectScan) GetInt(fldname string) int {
	if !s.HasField(fldname) {
		panic("field not found")
	}
	return s.input.GetInt(fldname)
}

func (s *ProjectScan) GetString(fldname string) string {
	if !s.HasField(fldname) {
		panic("field not found")
	}
	return s.input.GetString(fldname)
}

func (s *ProjectScan) GetValue(fldname string) any {
	if !s.HasField(fldname) {
		panic("field not found")
	}
	return s.input.GetValue(fldname)
}

func (s *ProjectScan) HasField(fldname string) bool {
	return slices.Contains(s.fieldList, fldname)
}

func (s *ProjectScan) Close() {
	s.input.Close()
}
