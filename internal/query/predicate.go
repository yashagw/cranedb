package query

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

// boolOp represents the type of boolean operation for a predicate node.
type boolOp int

const (
	boolOpLeaf boolOp = iota
	boolOpAnd
	boolOpOr
)

// Predicate represents a boolean expression over terms.
// It can be:
//   - a leaf node wrapping a single Term
//   - an AND/OR node combining two child predicates
type Predicate struct {
	term  *Term
	op    boolOp
	left  *Predicate
	right *Predicate
}

// NewPredicate creates a new Predicate with a single term.
func NewPredicate(term Term) *Predicate {
	return &Predicate{
		op:   boolOpLeaf,
		term: &term,
	}
}

// Clone creates a deep copy of the predicate tree.
func (p *Predicate) Clone() *Predicate {
	if p == nil {
		return nil
	}
	cp := &Predicate{op: p.op}
	if p.term != nil {
		t := *p.term
		cp.term = &t
	}
	cp.left = p.left.Clone()
	cp.right = p.right.Clone()
	return cp
}

// IsEmpty returns true if the predicate represents an empty expression.
// The zero value of Predicate is considered empty.
func (p *Predicate) IsEmpty() bool {
	if p == nil {
		return true
	}
	if p.op == boolOpLeaf {
		return p.term == nil
	}
	return p.left == nil && p.right == nil
}

// And combines two predicates with a logical AND and returns the result.
func And(left, right *Predicate) *Predicate {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return &Predicate{
		op:    boolOpAnd,
		left:  left,
		right: right,
	}
}

// Or combines two predicates with a logical OR and returns the result.
func Or(left, right *Predicate) *Predicate {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return &Predicate{
		op:    boolOpOr,
		left:  left,
		right: right,
	}
}

// IsSatisfied checks if all terms in the predicate are true for the current record in the scan.
func (p *Predicate) IsSatisfied(s scan.Scan) (bool, error) {
	if p == nil || p.IsEmpty() {
		return true, nil
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return true, nil
		}
		return p.term.IsSatisfied(s)
	case boolOpAnd:
		leftVal, err := p.left.IsSatisfied(s)
		if err != nil {
			return false, err
		}
		if !leftVal {
			return false, nil
		}
		return p.right.IsSatisfied(s)
	case boolOpOr:
		leftVal, err := p.left.IsSatisfied(s)
		if err != nil {
			return false, err
		}
		if leftVal {
			return true, nil
		}
		return p.right.IsSatisfied(s)
	default:
		return false, nil
	}
}

// IsConjunctive returns true if the predicate is a pure AND of terms (no OR nodes).
func (p *Predicate) IsConjunctive() bool {
	if p == nil || p.IsEmpty() {
		return true
	}

	switch p.op {
	case boolOpLeaf:
		return true
	case boolOpAnd:
		return p.left.IsConjunctive() && p.right.IsConjunctive()
	default:
		return false
	}
}

// SelectSubPred returns a new predicate containing only the terms whose fields exist in the given schema.
// Returns nil if no terms apply to the schema.
func (p *Predicate) SelectSubPred(sch *record.Schema) *Predicate {
	if p == nil || p.IsEmpty() {
		return nil
	}

	switch p.op {
	case boolOpLeaf:
		if p.term != nil && p.term.AppliesTo(sch) {
			return p.Clone()
		}
		return nil
	case boolOpAnd:
		left := p.left.SelectSubPred(sch)
		right := p.right.SelectSubPred(sch)
		return And(left, right)
	case boolOpOr:
		left := p.left.SelectSubPred(sch)
		right := p.right.SelectSubPred(sch)
		return Or(left, right)
	default:
		return nil
	}
}

// JoinSubPred returns a new predicate containing only the join terms (e.g., field1 = field2)
// where one field is from sch1 and the other is from sch2. Terms that apply to only one schema are excluded.
// Returns nil if no join terms exist.
func (p *Predicate) JoinSubPred(sch1, sch2 *record.Schema) *Predicate {
	if p == nil || p.IsEmpty() {
		return nil
	}

	newSch := record.NewSchema()
	newSch.CopyAll(sch1)
	newSch.CopyAll(sch2)

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return nil
		}
		t := *p.term
		if !t.AppliesTo(sch1) && !t.AppliesTo(sch2) && t.AppliesTo(newSch) {
			return p.Clone()
		}
		return nil
	case boolOpAnd:
		left := p.left.JoinSubPred(sch1, sch2)
		right := p.right.JoinSubPred(sch1, sch2)
		return And(left, right)
	case boolOpOr:
		left := p.left.JoinSubPred(sch1, sch2)
		right := p.right.JoinSubPred(sch1, sch2)
		return Or(left, right)
	default:
		return nil
	}
}

// TODO: These helpers do a simple depth‑first search and return only the first
// matching leaf term. This is usually OK for pure AND predicates but is lossy
// for OR trees (e.g. multiple alternative constants/fields). If planners need
// more accuracy, consider returning all matching terms or a normalized form.

// EquatesWithConstant returns the constant that a field is equated with, if any.
func (p *Predicate) EquatesWithConstant(fldname string) *Constant {
	if p == nil || p.IsEmpty() {
		return nil
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return nil
		}
		return p.term.EquatesWithConstant(fldname)
	case boolOpAnd, boolOpOr:
		if c := p.left.EquatesWithConstant(fldname); c != nil {
			return c
		}
		return p.right.EquatesWithConstant(fldname)
	default:
		return nil
	}
}

// EquatesWithField checks if the given field is equated with another field (e.g., field1 = field2).
// If found, returns the name of the other field; otherwise returns nil.
func (p *Predicate) EquatesWithField(fldname string) *string {
	if p == nil || p.IsEmpty() {
		return nil
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return nil
		}
		return p.term.EquatesWithField(fldname)
	case boolOpAnd, boolOpOr:
		if s := p.left.EquatesWithField(fldname); s != nil {
			return s
		}
		return p.right.EquatesWithField(fldname)
	default:
		return nil
	}
}

// ComparesWithConstant checks if any term compares the given field with a constant (any operator).
// Returns the constant, operator, and true if found; nil, OpEQ, false otherwise.
// This is used for estimating distinct values after filtering.
func (p *Predicate) ComparesWithConstant(fldname string) (*Constant, Operator, bool) {
	if p == nil || p.IsEmpty() {
		return nil, OpEQ, false
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return nil, OpEQ, false
		}
		return p.term.ComparesWithConstant(fldname)
	case boolOpAnd, boolOpOr:
		if c, op, found := p.left.ComparesWithConstant(fldname); found {
			return c, op, true
		}
		return p.right.ComparesWithConstant(fldname)
	default:
		return nil, OpEQ, false
	}
}

// ReductionFactor estimates how much the predicate will reduce the result set.
// It multiplies the reduction factors of all individual terms.
// Each term's reduction factor is calculated based on the distinct values of the field it operates on.
func (p *Predicate) ReductionFactor(plan interface{ DistinctValues(string) (int, error) }) (int, error) {
	if p == nil || p.IsEmpty() {
		return 1, nil
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return 1, nil
		}
		return p.term.ReductionFactor(plan)
	case boolOpAnd:
		leftFactor, err := p.left.ReductionFactor(plan)
		if err != nil {
			return 0, err
		}
		rightFactor, err := p.right.ReductionFactor(plan)
		if err != nil {
			return 0, err
		}
		return leftFactor * rightFactor, nil
	case boolOpOr:
		// For OR, use a conservative estimate: take the smaller (more selective) factor.
		leftFactor, err := p.left.ReductionFactor(plan)
		if err != nil {
			return 0, err
		}
		rightFactor, err := p.right.ReductionFactor(plan)
		if err != nil {
			return 0, err
		}
		if leftFactor < rightFactor {
			return leftFactor, nil
		}
		return rightFactor, nil
	default:
		return 1, nil
	}
}

// String returns a string representation of the predicate.
func (p *Predicate) String() string {
	if p == nil || p.IsEmpty() {
		return ""
	}
	return p.stringWithPrecedence(0)
}

// stringWithPrecedence returns the string representation using simple precedence:
// AND binds tighter than OR. Parentheses are added where needed to preserve semantics.
// parentPrec is the precedence of the parent context (0 = lowest, 1 = OR, 2 = AND).
func (p *Predicate) stringWithPrecedence(parentPrec int) string {
	if p == nil || p.IsEmpty() {
		return ""
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return ""
		}
		return p.term.String()
	case boolOpAnd, boolOpOr:
		var myPrec int
		var opStr string
		if p.op == boolOpAnd {
			myPrec = 2
			opStr = " and "
		} else {
			myPrec = 1
			opStr = " or "
		}

		leftStr := p.left.stringWithPrecedence(myPrec)
		rightStr := p.right.stringWithPrecedence(myPrec)
		if leftStr == "" {
			return rightStr
		}
		if rightStr == "" {
			return leftStr
		}

		combined := leftStr + opStr + rightStr
		if myPrec < parentPrec {
			return "(" + combined + ")"
		}
		return combined
	default:
		return ""
	}
}

// GetTerms returns a copy of the terms slice
func (p *Predicate) GetTerms() []Term {
	var result []Term
	p.collectTerms(&result)
	return result
}

// collectTerms appends all leaf terms from the predicate tree into dst.
func (p *Predicate) collectTerms(dst *[]Term) {
	if p == nil || p.IsEmpty() {
		return
	}

	switch p.op {
	case boolOpLeaf:
		if p.term != nil {
			*dst = append(*dst, *p.term)
		}
	case boolOpAnd, boolOpOr:
		p.left.collectTerms(dst)
		p.right.collectTerms(dst)
	}
}

// Validate checks if all terms in the predicate are valid for the given schema.
// Returns an error if any term uses range operators with boolean fields.
func (p *Predicate) Validate(sch *record.Schema) error {
	if p == nil || p.IsEmpty() {
		return nil
	}

	switch p.op {
	case boolOpLeaf:
		if p.term == nil {
			return nil
		}
		return p.term.Validate(sch)
	case boolOpAnd, boolOpOr:
		if err := p.left.Validate(sch); err != nil {
			return err
		}
		return p.right.Validate(sch)
	default:
		return nil
	}
}
