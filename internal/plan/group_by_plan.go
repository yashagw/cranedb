package plan

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/query/aggregations"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ Plan = (*GroupByPlan)(nil)
)

// GroupByPlan is the Plan for grouping records and applying aggregation functions.
// It wraps the underlying plan in a SortPlan to ensure records are sorted by group fields.
type GroupByPlan struct {
	p           Plan
	groupFields []string
	aggFns      []aggregations.AggregationFunction
	sch         *record.Schema
}

// NewGroupByPlan creates a new GroupByPlan.
// The underlying plan is wrapped in a SortPlan sorted by the group fields.
func NewGroupByPlan(tx *transaction.Transaction, p Plan, groupFields []string, aggFns []aggregations.AggregationFunction) *GroupByPlan {
	sortedPlan := NewSortPlan(p, groupFields, tx)

	sch := record.NewSchema()
	for _, fldname := range groupFields {
		sch.Copy(p.Schema(), fldname)
	}
	for _, fn := range aggFns {
		if _, ok := fn.(*aggregations.DistinctFn); ok {
			sch.AddStringField(fn.FieldName(), 500)
		} else {
			sch.AddIntField(fn.FieldName())
		}
	}

	return &GroupByPlan{
		p:           sortedPlan,
		groupFields: groupFields,
		aggFns:      aggFns,
		sch:         sch,
	}
}

func (gbp *GroupByPlan) Open() (scan.Scan, error) {
	s, err := gbp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewGroupByScan(s, gbp.groupFields, gbp.aggFns)
}

// BlocksAccessed returns the estimated number of blocks accessed.
func (gbp *GroupByPlan) BlocksAccessed() int {
	return gbp.p.BlocksAccessed()
}

// RecordsOutput returns the estimated number of output records (groups).
// This is the product of distinct values for each group field.
func (gbp *GroupByPlan) RecordsOutput() int {
	numGroups := 1
	for _, fldname := range gbp.groupFields {
		distinct, err := gbp.p.DistinctValues(fldname)
		if err != nil {
			// If we can't get distinct values, return 1 as a safe default
			return 1
		}
		numGroups *= distinct
	}
	return numGroups
}

// DistinctValues returns the estimated number of distinct values for a field.
// If the field is in the underlying plan's schema, return its distinct values.
// Otherwise, return the number of groups
func (gbp *GroupByPlan) DistinctValues(fldname string) (int, error) {
	if gbp.p.Schema().HasField(fldname) {
		return gbp.p.DistinctValues(fldname)
	}
	return gbp.RecordsOutput(), nil
}

// Schema returns the schema of the output records.
func (gbp *GroupByPlan) Schema() *record.Schema {
	return gbp.sch
}

// Explain returns a string representation of the plan tree.
func (gbp *GroupByPlan) Explain(indent string, lastChild bool) string {
	groupFieldsStr := "["
	for i, f := range gbp.groupFields {
		if i > 0 {
			groupFieldsStr += ", "
		}
		groupFieldsStr += f
	}
	groupFieldsStr += "]"

	aggFnsStr := "["
	for i, fn := range gbp.aggFns {
		if i > 0 {
			aggFnsStr += ", "
		}
		aggFnsStr += fn.FieldName()
	}
	aggFnsStr += "]"

	result := indent + fmt.Sprintf("GroupByPlan(groupFields: %s, aggFns: %s)\n", groupFieldsStr, aggFnsStr)

	// Determine child indent
	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}
	childLines := gbp.p.Explain(childIndent+"└─ ", true)
	result += childLines
	return result
}
