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
	_ Plan = (*IndexJoinPlan)(nil)
)

// IndexJoinPlan is the Plan for an index join operation.
// It joins two plans using an index on the join field.
type IndexJoinPlan struct {
	p1        Plan
	p2        Plan
	indexInfo *metadata.IndexInfo
	joinField string
	schema    *record.Schema
}

// NewIndexJoinPlan creates a new IndexJoinPlan.
func NewIndexJoinPlan(p1 Plan, p2 Plan, indexInfo *metadata.IndexInfo, joinField string) *IndexJoinPlan {
	schema := record.NewSchema()
	schema.CopyAll(p1.Schema())
	schema.CopyAll(p2.Schema())
	return &IndexJoinPlan{
		p1:        p1,
		p2:        p2,
		indexInfo: indexInfo,
		joinField: joinField,
		schema:    schema,
	}
}

// Open opens the index join scan.
// It opens p1 as a scan, p2 as a TableScan, opens the index, and returns an IndexJoinScan.
func (ijp *IndexJoinPlan) Open() (scan.Scan, error) {
	s, err := ijp.p1.Open()
	if err != nil {
		return nil, err
	}

	// p2 must be a TablePlan that opens to a TableScan
	tsScan, err := ijp.p2.Open()
	if err != nil {
		s.Close()
		return nil, err
	}

	ts, ok := tsScan.(*table.TableScan)
	if !ok {
		s.Close()
		tsScan.Close()
		return nil, fmt.Errorf("p2 is not a TableScan")
	}

	idx, err := ijp.indexInfo.Open()
	if err != nil {
		s.Close()
		ts.Close()
		return nil, err
	}

	return query.NewIndexJoinScan(s, idx, ijp.joinField, ts)
}

// BlocksAccessed returns the estimated number of blocks accessed.
// Formula: p1.blocksAccessed() + (p1.recordsOutput() * ii.blocksAccessed()) + recordsOutput()
func (ijp *IndexJoinPlan) BlocksAccessed() int {
	return ijp.p1.BlocksAccessed() + (ijp.p1.RecordsOutput() * ijp.indexInfo.BlocksAccessed()) + ijp.RecordsOutput()
}

// RecordsOutput returns the estimated number of output records.
// Formula: p1.recordsOutput() * ii.recordsOutput()
func (ijp *IndexJoinPlan) RecordsOutput() int {
	return ijp.p1.RecordsOutput() * ijp.indexInfo.RecordsOutput()
}

// DistinctValues returns the estimated number of distinct values for a field.
// It delegates to the plan that contains the field.
func (ijp *IndexJoinPlan) DistinctValues(fldname string) (int, error) {
	if ijp.p1.Schema().HasField(fldname) {
		return ijp.p1.DistinctValues(fldname)
	}
	return ijp.p2.DistinctValues(fldname)
}

// Schema returns the combined schema of both plans.
func (ijp *IndexJoinPlan) Schema() *record.Schema {
	return ijp.schema
}

// Explain returns a string representation of the plan tree.
func (ijp *IndexJoinPlan) Explain(indent string, lastChild bool) string {
	result := indent + "IndexJoinPlan(joinField: " + ijp.joinField + ")\n"

	// Determine child indent
	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}

	// First child (p1) - not last
	p1Prefix := childIndent + "├─ "
	p1Lines := ijp.p1.Explain(p1Prefix, false)
	result += p1Lines + "\n"

	// Second child (p2) - last
	p2Prefix := childIndent + "└─ "
	p2Lines := ijp.p2.Explain(p2Prefix, true)
	result += p2Lines

	return result
}
