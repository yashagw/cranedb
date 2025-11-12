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

func (sp *SelectPlan) Open() (scan.Scan, error) {
	s, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	return scan.NewSelectScan(s, sp.pred), nil
}

// BlocksAccessed returns the same as the underlying plan (selection doesn't change block access).
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

// RecordsOutput estimates output records as input records / reduction factor.
func (sp *SelectPlan) RecordsOutput() int {
	reductionFactor, err := sp.pred.ReductionFactor(sp.p)
	if err != nil {
		// If we can't calculate reduction factor, return input records (no reduction)
		return sp.p.RecordsOutput()
	}
	if reductionFactor == 0 {
		// Avoid division by zero
		return sp.p.RecordsOutput()
	}
	return sp.p.RecordsOutput() / reductionFactor
}

// DistinctValues returns:
// - 1 if the field is equated with a constant
// - min of both fields if equated with another field
// - underlying plan's value otherwise
func (sp *SelectPlan) DistinctValues(fldname string) (int, error) {
	if sp.pred.EquatesWithConstant(fldname) != nil {
		return 1, nil
	}

	fldname2 := sp.pred.EquatesWithField(fldname)
	if fldname2 != nil {
		val1, err := sp.p.DistinctValues(fldname)
		if err != nil {
			return 0, err
		}
		val2, err := sp.p.DistinctValues(*fldname2)
		if err != nil {
			return 0, err
		}
		return int(math.Min(float64(val1), float64(val2))), nil
	}

	return sp.p.DistinctValues(fldname)
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}
