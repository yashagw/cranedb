package query

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

// Operator represents a comparison operator
type Operator int

const (
	OpEQ Operator = iota // =
	OpNE                 // !=
	OpGT                 // >
	OpLT                 // <
	OpGE                 // >=
	OpLE                 // <=
)

func (op Operator) String() string {
	switch op {
	case OpEQ:
		return "="
	case OpNE:
		return "!="
	case OpGT:
		return ">"
	case OpLT:
		return "<"
	case OpGE:
		return ">="
	case OpLE:
		return "<="
	default:
		return "?"
	}
}

// flipOperator flips operators
func (op Operator) flip() Operator {
	switch op {
	case OpGT:
		return OpLT
	case OpLT:
		return OpGT
	case OpGE:
		return OpLE
	case OpLE:
		return OpGE
	case OpEQ, OpNE:
		return op // These are symmetric
	default:
		return op
	}
}

// Term represents a boolean comparison between two expressions
// (e.g., field = constant, field = field, constant = constant, field > constant).
type Term struct {
	left     Expression
	right    Expression
	operator Operator
}

// NewTerm creates a new Term with two expressions and an operator
func NewTerm(left Expression, right Expression, operator Operator) *Term {
	return &Term{
		left:     left,
		right:    right,
		operator: operator,
	}
}

// String returns a string representation of the term
func (t *Term) String() string {
	return fmt.Sprintf("%s %s %s", t.left.String(), t.operator.String(), t.right.String())
}

// IsSatisfied checks if the term is true for the current record in the scan.
func (t *Term) IsSatisfied(s scan.Scan) (bool, error) {
	lhsVal, err := t.left.Evaluate(s)
	if err != nil {
		return false, err
	}
	rhsVal, err := t.right.Evaluate(s)
	if err != nil {
		return false, err
	}

	// Validate: boolean values cannot use range operators (<, >, <=, >=)
	if t.operator == OpGT || t.operator == OpLT || t.operator == OpGE || t.operator == OpLE {
		if lhsVal.IsBool() || rhsVal.IsBool() {
			return false, fmt.Errorf("comparison operators %s cannot be used with boolean fields", t.operator.String())
		}
	}

	compareResult := (&lhsVal).CompareTo(&rhsVal)
	switch t.operator {
	case OpEQ:
		return compareResult == 0, nil
	case OpNE:
		return compareResult != 0, nil
	case OpGT:
		return compareResult > 0, nil
	case OpLT:
		return compareResult < 0, nil
	case OpGE:
		return compareResult >= 0, nil
	case OpLE:
		return compareResult <= 0, nil
	default:
		return false, fmt.Errorf("unknown operator: %v", t.operator)
	}
}

// appliesTo checks if both expressions of the term apply to the given schema.
func (t *Term) AppliesTo(sch *record.Schema) bool {
	return t.left.AppliesTo(sch) && t.right.AppliesTo(sch)
}

// Validate checks if the term is valid for the given schema.
// Returns an error if range operators (<, >, <=, >=) are used with boolean fields.
func (t *Term) Validate(sch *record.Schema) error {
	// Check if range operators are used with boolean fields
	if t.operator == OpGT || t.operator == OpLT || t.operator == OpGE || t.operator == OpLE {
		// Check if left side is a boolean field
		if t.left.IsFieldName() {
			fieldName := t.left.AsFieldName()
			if sch.HasField(fieldName) && sch.Type(fieldName) == record.FieldTypeBool {
				return fmt.Errorf("comparison operators %s cannot be used with boolean field '%s'", t.operator.String(), fieldName)
			}
		}
		// Check if right side is a boolean field
		if t.right.IsFieldName() {
			fieldName := t.right.AsFieldName()
			if sch.HasField(fieldName) && sch.Type(fieldName) == record.FieldTypeBool {
				return fmt.Errorf("comparison operators %s cannot be used with boolean field '%s'", t.operator.String(), fieldName)
			}
		}
		// Check if left side is a boolean constant
		if !t.left.IsFieldName() {
			constVal := t.left.AsConstant()
			if (&constVal).IsBool() {
				return fmt.Errorf("comparison operators %s cannot be used with boolean constants", t.operator.String())
			}
		}
		// Check if right side is a boolean constant
		if !t.right.IsFieldName() {
			constVal := t.right.AsConstant()
			if (&constVal).IsBool() {
				return fmt.Errorf("comparison operators %s cannot be used with boolean constants", t.operator.String())
			}
		}
	}
	return nil
}

// EquatesWithConstant checks if this term is "field = constant" or "constant = field" for the given field name.
func (t *Term) EquatesWithConstant(fieldName string) *Constant {
	if t.operator != OpEQ {
		return nil
	}
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
func (t *Term) EquatesWithField(fldName string) *string {
	if t.operator != OpEQ {
		return nil
	}
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

// ComparesWithConstant checks if this term compares a field with a constant (any operator).
func (t *Term) ComparesWithConstant(fieldName string) (*Constant, Operator, bool) {
	if t.left.IsFieldName() && t.left.AsFieldName() == fieldName && !t.right.IsFieldName() {
		constVal := t.right.AsConstant()
		return &constVal, t.operator, true
	} else if t.right.IsFieldName() && t.right.AsFieldName() == fieldName && !t.left.IsFieldName() {
		constVal := t.left.AsConstant()
		return &constVal, t.operator.flip(), true
	}
	return nil, OpEQ, false
}

// ReductionFactor estimates the reduction factor for this term.
func (t *Term) ReductionFactor(plan interface{ DistinctValues(string) (int, error) }) (int, error) {
	var lhsName, rhsName string

	if t.left.IsFieldName() {
		lhsName = t.left.AsFieldName()
	}

	if t.right.IsFieldName() {
		rhsName = t.right.AsFieldName()
	}

	// If both sides are field names (field op field), return max of distinct values
	// Note: For non-equality operators, this is a rough estimate
	if lhsName != "" && rhsName != "" {
		lhsDistinct, err := plan.DistinctValues(lhsName)
		if err != nil {
			return 0, err
		}
		rhsDistinct, err := plan.DistinctValues(rhsName)
		if err != nil {
			return 0, err
		}
		maxDistinct := lhsDistinct
		if rhsDistinct > maxDistinct {
			maxDistinct = rhsDistinct
		}

		// For equality, return max distinct values
		// For other operators, adjust the reduction factor
		if t.operator == OpEQ {
			return maxDistinct, nil
		}
		// For non-equality field comparisons, use a conservative estimate
		// This is less selective than equality
		return maxDistinct / 2, nil
	}

	// If one side is a field name (field op constant or constant op field)
	if lhsName != "" || rhsName != "" {
		fieldName := lhsName
		if fieldName == "" {
			fieldName = rhsName
		}

		distinct, err := plan.DistinctValues(fieldName)
		if err != nil {
			return 0, err
		}

		switch t.operator {
		case OpEQ:
			// field = constant: 1/N records remain
			return distinct, nil
		case OpNE:
			// field != constant: (distinct-1)/distinct records remain
			// Reduction factor = distinct / (distinct-1)
			// Example: If field has 10 distinct values, 9/10 records match != constant
			if distinct > 1 {
				return distinct / (distinct - 1), nil
			}
			return 1, nil
		case OpGT, OpLT, OpGE, OpLE:
			// Single-sided range: roughly 50% of records remain
			// Reduction factor = 2
			return 2, nil
		default:
			return distinct, nil
		}
	}

	// If neither side is a field (constant op constant), return 1
	// This means all or no records match depending on the comparison
	return 1, nil
}

// GetLHS returns the left-hand side expression
func (t *Term) GetLHS() *Expression {
	return &t.left
}

// GetRHS returns the right-hand side expression
func (t *Term) GetRHS() *Expression {
	return &t.right
}

// GetOperator returns the operator for this term
func (t *Term) GetOperator() Operator {
	return t.operator
}
