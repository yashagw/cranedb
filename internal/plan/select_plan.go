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
	// Validate predicate before opening scan to catch errors early
	// This prevents buffer/lock issues from occurring during query execution
	if err := sp.pred.Validate(sp.p.Schema()); err != nil {
		return nil, err
	}

	s, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(s, *sp.pred), nil
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

// DistinctValues returns the estimated number of distinct values for a field after applying the predicate.
func (sp *SelectPlan) DistinctValues(fldname string) (int, error) {
	// Check for field = constant (equality)
	if sp.pred.EquatesWithConstant(fldname) != nil {
		return 1, nil
	}

	// Check for field op constant (any operator)
	if _, operator, found := sp.pred.ComparesWithConstant(fldname); found {
		originalDistinct, err := sp.p.DistinctValues(fldname)
		if err != nil {
			return 0, err
		}

		switch operator {
		case query.OpEQ:
			return 1, nil
		case query.OpNE:
			// field != constant: exclude one value, so distinct - 1 values remain
			reduced := originalDistinct - 1
			if reduced < 1 {
				reduced = 1
			}
			return reduced, nil
		case query.OpGT, query.OpLT, query.OpGE, query.OpLE:
			// Single-sided range: roughly half the distinct values
			reduced := originalDistinct / 2
			if reduced < 1 {
				reduced = 1
			}
			return reduced, nil
		default:
			// Unknown operator, return original
			return originalDistinct, nil
		}
	}

	// Check for field = field
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

	// No specific comparison found, return underlying plan's value
	return sp.p.DistinctValues(fldname)
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}

// Explain returns a string representation of the plan tree.
func (sp *SelectPlan) Explain(indent string, lastChild bool) string {
	predStr := sp.pred.String()
	if predStr == "" {
		predStr = "(no predicate)"
	}
	result := indent + "SelectPlan(predicate: " + predStr + ")\n"

	// Determine child indent
	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}
	childLines := sp.p.Explain(childIndent+"└─ ", true)
	result += childLines
	return result
}
