package query

import (
	"fmt"
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

func (s *ProjectScan) BeforeFirst() error {
	return s.input.BeforeFirst()
}

func (s *ProjectScan) Next() (bool, error) {
	return s.input.Next()
}

func (s *ProjectScan) GetInt(fldname string) (int, error) {
	if !s.HasField(fldname) {
		return 0, fmt.Errorf("field not found: %s", fldname)
	}
	return s.input.GetInt(fldname)
}

func (s *ProjectScan) GetString(fldname string) (string, error) {
	if !s.HasField(fldname) {
		return "", fmt.Errorf("field not found: %s", fldname)
	}
	return s.input.GetString(fldname)
}

func (s *ProjectScan) GetValue(fldname string) (any, error) {
	if !s.HasField(fldname) {
		return nil, fmt.Errorf("field not found: %s", fldname)
	}
	return s.input.GetValue(fldname)
}

func (s *ProjectScan) HasField(fldname string) bool {
	return slices.Contains(s.fieldList, fldname)
}

func (s *ProjectScan) Close() {
	s.input.Close()
}
