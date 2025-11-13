package plan

import (
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ Plan = (*SelectPlan)(nil)
)

// IndexSelectPlan is the Plan for a selection (WHERE clause) with index.
type IndexSelectPlan struct {
	p         Plan
	indexInfo *metadata.IndexInfo
	value     any
}

func NewIndexSelectPlan(p Plan, indexInfo *metadata.IndexInfo, value any) *IndexSelectPlan {
	return &IndexSelectPlan{
		p:         p,
		indexInfo: indexInfo,
		value:     value,
	}
}

func (isp *IndexSelectPlan) Open() (scan.Scan, error) {
	inputScan, err := isp.p.Open()
	if err != nil {
		return nil, err
	}
	index, err := isp.indexInfo.Open()
	if err != nil {
		return nil, err
	}
	return scan.NewIndexSelectScan(inputScan, index, isp.value), nil
}

// BlocksAccessed returns index traversal cost plus matching data records.
func (isp *IndexSelectPlan) BlocksAccessed() int {
	return isp.indexInfo.BlocksAccessed() + isp.RecordsOutput()
}

// RecordsOutput returns the number of search key values for the index.
func (isp *IndexSelectPlan) RecordsOutput() int {
	return isp.indexInfo.RecordsOutput()
}

// DistinctValues delegates to the index.
func (isp *IndexSelectPlan) DistinctValues(fieldName string) int {
	return isp.indexInfo.DistinctValues(fieldName)
}

// Schema returns the schema of the data table.
func (isp *IndexSelectPlan) Schema() *record.Schema {
	return isp.p.Schema()
}
