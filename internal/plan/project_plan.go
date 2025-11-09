package plan

import (
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ Plan = (*ProjectPlan)(nil)
)

// ProjectPlan is the Plan for a projection (SELECT fields).
type ProjectPlan struct {
	p      Plan
	schema *record.Schema
}

func NewProjectPlan(p Plan, fieldList []string) *ProjectPlan {
	schema := record.NewSchema()
	for _, fldname := range fieldList {
		schema.Copy(p.Schema(), fldname)
	}
	return &ProjectPlan{
		p:      p,
		schema: schema,
	}
}

func (pp *ProjectPlan) Open() scan.Scan {
	s := pp.p.Open()
	return query.NewProjectScan(s, pp.schema.Fields())
}

// BlocksAccessed returns the same as the underlying plan (projection doesn't change block access).
func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.p.BlocksAccessed()
}

// RecordsOutput returns the same as the underlying plan (projection doesn't filter rows).
func (pp *ProjectPlan) RecordsOutput() int {
	return pp.p.RecordsOutput()
}

// DistinctValues delegates to the underlying plan.
func (pp *ProjectPlan) DistinctValues(fldname string) int {
	return pp.p.DistinctValues(fldname)
}

// Schema returns the schema with only the projected fields.
func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}
