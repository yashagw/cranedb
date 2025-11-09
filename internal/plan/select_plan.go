package plan

import (
	"math"

	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ Plan = (*SelectPlan)(nil)
)

// SelectPlan is the Plan for a selection (WHERE clause).
type SelectPlan struct {
	p    Plan
	pred *query.Predicate
}

func NewSelectPlan(p Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

func (sp *SelectPlan) Open() scan.Scan {
	s := sp.p.Open()
	return query.NewSelectScan(s, *sp.pred)
}

// BlocksAccessed returns the same as the underlying plan (selection doesn't change block access).
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

// RecordsOutput estimates output records as input records / reduction factor.
func (sp *SelectPlan) RecordsOutput() int {
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

// DistinctValues returns:
// - 1 if the field is equated with a constant
// - min of both fields if equated with another field
// - underlying plan's value otherwise
func (sp *SelectPlan) DistinctValues(fldname string) int {
	if sp.pred.EquatesWithConstant(fldname) != nil {
		return 1
	}

	fldname2 := sp.pred.EquatesWithField(fldname)
	if fldname2 != nil {
		return int(math.Min(float64(sp.p.DistinctValues(fldname)), float64(sp.p.DistinctValues(*fldname2))))
	}

	return sp.p.DistinctValues(fldname)
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}
