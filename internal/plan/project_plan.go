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

func (pp *ProjectPlan) Open() (scan.Scan, error) {
	s, err := pp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, pp.schema.Fields()), nil
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
func (pp *ProjectPlan) DistinctValues(fldname string) (int, error) {
	return pp.p.DistinctValues(fldname)
}

// Schema returns the schema with only the projected fields.
func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}

// Explain returns a string representation of the plan tree.
func (pp *ProjectPlan) Explain(indent string, lastChild bool) string {
	fields := pp.schema.Fields()
	fieldsStr := "["
	for i, f := range fields {
		if i > 0 {
			fieldsStr += ", "
		}
		fieldsStr += f
	}
	fieldsStr += "]"
	result := indent + "ProjectPlan(fields: " + fieldsStr + ")\n"

	// Determine child indent
	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}
	childLines := pp.p.Explain(childIndent+"└─ ", true)
	result += childLines
	return result
}
