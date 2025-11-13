package query

import (
	"strings"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

// Predicate represents a conjunction of terms (ANDed together).
type Predicate struct {
	terms []Term
}

// NewPredicate creates a new Predicate with a single term.
func NewPredicate(term Term) *Predicate {
	return &Predicate{
		terms: []Term{term},
	}
}

// ConjunctWith adds all terms from another predicate to this one (AND operation).
func (p *Predicate) ConjunctWith(other Predicate) {
	p.terms = append(p.terms, other.terms...)
}

// IsSatisfied checks if all terms in the predicate are true for the current record in the scan.
func (p *Predicate) IsSatisfied(s scan.Scan) (bool, error) {
	for _, t := range p.terms {
		satisfied, err := t.IsSatisfied(s)
		if err != nil {
			return false, err
		}
		if !satisfied {
			return false, nil
		}
	}
	return true, nil
}

// SelectSubPred returns a new predicate containing only the terms whose fields exist in the given schema.
// Returns nil if no terms apply to the schema.
func (p *Predicate) SelectSubPred(sch *record.Schema) *Predicate {
	result := &Predicate{
		terms: make([]Term, 0),
	}
	for _, t := range p.terms {
		if t.AppliesTo(sch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// JoinSubPred returns a new predicate containing only the join terms (e.g., field1 = field2)
// where one field is from sch1 and the other is from sch2. Terms that apply to only one schema are excluded.
// Returns nil if no join terms exist.
func (p *Predicate) JoinSubPred(sch1, sch2 *record.Schema) *Predicate {
	result := &Predicate{
		terms: make([]Term, 0),
	}
	newSch := record.NewSchema()
	newSch.CopyAll(sch1)
	newSch.CopyAll(sch2)

	for _, t := range p.terms {
		if !t.AppliesTo(sch1) && !t.AppliesTo(sch2) && t.AppliesTo(newSch) {
			result.terms = append(result.terms, t)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// EquatesWithConstant returns the constant that a field is equated with, if any.
func (p *Predicate) EquatesWithConstant(fldname string) *Constant {
	for _, t := range p.terms {
		c := t.EquatesWithConstant(fldname)
		if c != nil {
			return c
		}
	}
	return nil
}

// EquatesWithField checks if the given field is equated with another field (e.g., field1 = field2).
// If found, returns the name of the other field; otherwise returns nil.
func (p *Predicate) EquatesWithField(fldname string) *string {
	for _, t := range p.terms {
		s := t.EquatesWithField(fldname)
		if s != nil {
			return s
		}
	}
	return nil
}

// ReductionFactor estimates how much the predicate will reduce the result set.
// It multiplies the reduction factors of all individual terms.
// Each term's reduction factor is calculated based on the distinct values of the field it operates on.
func (p *Predicate) ReductionFactor(plan interface{ DistinctValues(string) (int, error) }) (int, error) {
	factor := 1
	for _, t := range p.terms {
		termFactor, err := t.ReductionFactor(plan)
		if err != nil {
			return 0, err
		}
		factor *= termFactor
	}
	return factor, nil
}

// String returns a string representation of the predicate.
func (p *Predicate) String() string {
	if len(p.terms) == 0 {
		return ""
	}
	var parts []string
	for _, t := range p.terms {
		parts = append(parts, t.String())
	}
	return strings.Join(parts, " and ")
}

// GetTerms returns a copy of the terms slice
func (p *Predicate) GetTerms() []Term {
	result := make([]Term, len(p.terms))
	copy(result, p.terms)
	return result
}

// IsEmpty returns true if the predicate has no terms
func (p *Predicate) IsEmpty() bool {
	return len(p.terms) == 0
}
