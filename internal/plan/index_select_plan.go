package plan

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/table"
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
	inputTableScan, ok := inputScan.(*table.TableScan)
	if !ok {
		return nil, fmt.Errorf("input scan is not a TableScan")
	}
	return query.NewIndexSelectScan(inputTableScan, index, isp.value)
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
