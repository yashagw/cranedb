package plan

import (
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ Plan = (*ProductPlan)(nil)
)

// ProductPlan is the Plan for a Cartesian product (cross join).
type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema *record.Schema
}

func NewProductPlan(p1 Plan, p2 Plan) *ProductPlan {
	schema := record.NewSchema()
	schema.CopyAll(p1.Schema())
	schema.CopyAll(p2.Schema())
	return &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}
}

func (pp *ProductPlan) Open() (scan.Scan, error) {
	s1, err := pp.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := pp.p2.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2), nil
}

// BlocksAccessed uses nested loop cost model: p1.blocks + (p1.records * p2.blocks).
func (pp *ProductPlan) BlocksAccessed() int {
	return pp.p1.BlocksAccessed() + (pp.p1.RecordsOutput() * pp.p2.BlocksAccessed())
}

// RecordsOutput returns the Cartesian product size: p1.records * p2.records.
func (pp *ProductPlan) RecordsOutput() int {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

// DistinctValues delegates to whichever underlying plan contains the field.
func (pp *ProductPlan) DistinctValues(fldname string) (int, error) {
	if pp.p1.Schema().HasField(fldname) {
		return pp.p1.DistinctValues(fldname)
	}
	return pp.p2.DistinctValues(fldname)
}

// Schema returns the combined schema of both plans.
func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
