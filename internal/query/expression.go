package query

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

// Expression represents either a constant value or a field name in a query.
type Expression struct {
	val     Constant
	fldName *string
}

// NewConstantExpression creates a new Expression with a constant value.
func NewConstantExpression(val Constant) *Expression {
	return &Expression{
		val: val,
	}
}

// NewFieldNameExpression creates a new Expression with a field name.
func NewFieldNameExpression(fldName string) *Expression {
	return &Expression{
		fldName: &fldName,
	}
}

// isFieldName checks if the expression is a field name.
func (e *Expression) IsFieldName() bool {
	return e.fldName != nil
}

// asConstant returns the constant value of the expression.
func (e *Expression) AsConstant() Constant {
	return e.val
}

// asFieldName returns the field name of the expression.
func (e *Expression) AsFieldName() string {
	return *e.fldName
}

// String returns a string representation of the expression.
func (e *Expression) String() string {
	if e.IsFieldName() {
		return e.AsFieldName()
	}
	return e.val.String()
}

// evaluate returns the value of the expression for the current record in the scan.
func (e *Expression) Evaluate(s scan.Scan) (Constant, error) {
	if e.IsFieldName() {
		val, err := s.GetValue(e.AsFieldName())
		if err != nil {
			return Constant{}, err
		}
		// Convert primitive values to Constant
		switch v := val.(type) {
		case int:
			return *NewIntConstant(v), nil
		case string:
			return *NewStringConstant(v), nil
		case Constant:
			return v, nil
		default:
			return Constant{}, fmt.Errorf("unsupported value type: %T", v)
		}
	}
	return e.val, nil
}

// appliesTo checks if the expression applies to the given schema.
func (e *Expression) AppliesTo(schema *record.Schema) bool {
	if e.IsFieldName() {
		return schema.HasField(e.AsFieldName())
	}
	return true
}
