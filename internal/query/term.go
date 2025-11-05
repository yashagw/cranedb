package query

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

// Term represents a boolean comparison between two expressions
// (e.g., field = constant, field = field, constant = constant).
type Term struct {
	left  Expression
	right Expression
}

// NewTerm creates a new Term with two expressions
func NewTerm(left Expression, right Expression) *Term {
	return &Term{
		left:  left,
		right: right,
	}
}

// String returns a string representation of the term
func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.left.String(), t.right.String())
}

// IsSatisfied checks if the term is true for the current record in the scan.
func (t *Term) IsSatisfied(s scan.Scan) bool {
	lhsVal := t.left.Evaluate(s)
	rhsVal := t.right.Evaluate(s)
	return (&rhsVal).Equals(&lhsVal)
}

// appliesTo checks if both expressions of the term apply to the given schema.
func (t *Term) AppliesTo(sch *record.Schema) bool {
	return t.left.AppliesTo(sch) && t.right.AppliesTo(sch)
}

// EquatesWithConstant checks if this term is "field = constant" or "constant = field" for the given field name.
// If yes, it returns the constant on the other side; otherwise, it returns nil.
func (t *Term) EquatesWithConstant(fieldName string) *Constant {
	if t.left.IsFieldName() && t.left.AsFieldName() == fieldName && !t.right.IsFieldName() {
		constVal := t.right.AsConstant()
		return &constVal
	} else if t.right.IsFieldName() && t.right.AsFieldName() == fieldName && !t.left.IsFieldName() {
		constVal := t.left.AsConstant()
		return &constVal
	}
	return nil
}

// EquatesWithField checks if this term is "field = field" for the given field name.
// If yes, it returns the name of the field on the other side; otherwise, it returns nil.
func (t *Term) EquatesWithField(fldName string) *string {
	if t.left.IsFieldName() && t.left.AsFieldName() == fldName && t.right.IsFieldName() {
		field := t.right.AsFieldName()
		return &field
	} else if t.right.IsFieldName() && t.right.AsFieldName() == fldName && t.left.IsFieldName() {
		field := t.left.AsFieldName()
		return &field
	} else {
		return nil
	}
}
