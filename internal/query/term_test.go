package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
)

func TestTermBasic(t *testing.T) {
	// Test creating term with constant = constant
	leftConst := NewConstantExpression(*NewIntConstant(10))
	rightConst := NewConstantExpression(*NewIntConstant(20))
	term := NewTerm(*leftConst, *rightConst, OpEQ)
	require.NotNil(t, term)
	assert.Equal(t, "10 = 20", term.String())

	// Test creating term with field = constant
	fieldExpr := NewFieldNameExpression("age")
	constExpr := NewConstantExpression(*NewIntConstant(25))
	term2 := NewTerm(*fieldExpr, *constExpr, OpEQ)
	require.NotNil(t, term2)
	assert.Equal(t, "age = 25", term2.String())

	// Test creating term with field = field
	fieldExpr1 := NewFieldNameExpression("name")
	fieldExpr2 := NewFieldNameExpression("alias")
	term3 := NewTerm(*fieldExpr1, *fieldExpr2, OpEQ)
	require.NotNil(t, term3)
	assert.Equal(t, "name = alias", term3.String())

	// Test AppliesTo with schema
	schema := record.NewSchema()
	schema.AddIntField("age")
	schema.AddStringField("name", 20)

	// Both expressions apply to schema
	assert.True(t, term2.AppliesTo(schema))

	// One expression doesn't apply
	missingFieldExpr := NewFieldNameExpression("missing")
	term4 := NewTerm(*fieldExpr, *missingFieldExpr, OpEQ)
	assert.False(t, term4.AppliesTo(schema))
}

func TestTermEquatesWithConstant(t *testing.T) {
	fieldExpr := NewFieldNameExpression("age")
	constExpr := NewConstantExpression(*NewIntConstant(25))

	// Test field = constant
	term := NewTerm(*fieldExpr, *constExpr, OpEQ)
	result := term.EquatesWithConstant("age")
	require.NotNil(t, result)
	assert.Equal(t, 25, result.AsInt())

	// Test constant = field (reversed)
	term2 := NewTerm(*constExpr, *fieldExpr, OpEQ)
	result2 := term2.EquatesWithConstant("age")
	require.NotNil(t, result2)
	assert.Equal(t, 25, result2.AsInt())

	// Test field name doesn't match
	result3 := term.EquatesWithConstant("name")
	assert.Nil(t, result3)

	// Test field = field (no constant)
	fieldExpr2 := NewFieldNameExpression("name")
	term3 := NewTerm(*fieldExpr, *fieldExpr2, OpEQ)
	result4 := term3.EquatesWithConstant("age")
	assert.Nil(t, result4)

	// Test non-equality operator returns nil
	term4 := NewTerm(*fieldExpr, *constExpr, OpGT)
	result5 := term4.EquatesWithConstant("age")
	assert.Nil(t, result5)
}

func TestTermEquatesWithField(t *testing.T) {
	fieldExpr1 := NewFieldNameExpression("name")
	fieldExpr2 := NewFieldNameExpression("alias")

	// Test field = field
	term := NewTerm(*fieldExpr1, *fieldExpr2, OpEQ)
	result := term.EquatesWithField("name")
	require.NotNil(t, result)
	assert.Equal(t, "alias", *result)

	// Test field = field (other side)
	result2 := term.EquatesWithField("alias")
	require.NotNil(t, result2)
	assert.Equal(t, "name", *result2)

	// Test field name doesn't match
	result3 := term.EquatesWithField("age")
	assert.Nil(t, result3)

	// Test field = constant (not field = field)
	fieldExpr3 := NewFieldNameExpression("age")
	constExpr := NewConstantExpression(*NewIntConstant(25))
	term2 := NewTerm(*fieldExpr3, *constExpr, OpEQ)
	result4 := term2.EquatesWithField("age")
	assert.Nil(t, result4)

	// Test creating term with boolean constant
	boolFieldExpr := NewFieldNameExpression("active")
	boolConstExpr := NewConstantExpression(*NewBoolConstant(true))
	termBool := NewTerm(*boolFieldExpr, *boolConstExpr, OpEQ)
	require.NotNil(t, termBool)
	assert.Equal(t, "active = true", termBool.String())

	// Test EquatesWithConstant with boolean
	resultBool := termBool.EquatesWithConstant("active")
	require.NotNil(t, resultBool)
	assert.Equal(t, true, resultBool.AsBool())

	// Test boolean constant = field (reversed)
	termBool2 := NewTerm(*boolConstExpr, *boolFieldExpr, OpEQ)
	resultBool2 := termBool2.EquatesWithConstant("active")
	require.NotNil(t, resultBool2)
	assert.Equal(t, true, resultBool2.AsBool())

	// Test non-equality operator returns nil
	term3 := NewTerm(*fieldExpr1, *fieldExpr2, OpNE)
	result5 := term3.EquatesWithField("name")
	assert.Nil(t, result5)
}

func TestTermComparisonOperators(t *testing.T) {
	// Test with constants first since we need a scan for field expressions
	leftConst10 := NewConstantExpression(*NewIntConstant(10))
	leftConst20 := NewConstantExpression(*NewIntConstant(20))
	rightConst10 := NewConstantExpression(*NewIntConstant(10))
	rightConst15 := NewConstantExpression(*NewIntConstant(15))

	// Test equality (=)
	termEQ := NewTerm(*leftConst10, *rightConst10, OpEQ)
	assert.Equal(t, "10 = 10", termEQ.String())

	// Test not equal (!=)
	termNE := NewTerm(*leftConst10, *rightConst15, OpNE)
	assert.Equal(t, "10 != 15", termNE.String())

	// Test greater than (>)
	termGT := NewTerm(*leftConst20, *rightConst10, OpGT)
	assert.Equal(t, "20 > 10", termGT.String())

	// Test less than (<)
	termLT := NewTerm(*leftConst10, *rightConst15, OpLT)
	assert.Equal(t, "10 < 15", termLT.String())

	// Test greater than or equal (>=)
	termGE1 := NewTerm(*leftConst10, *rightConst10, OpGE)
	assert.Equal(t, "10 >= 10", termGE1.String())
	termGE2 := NewTerm(*leftConst20, *rightConst10, OpGE)
	assert.Equal(t, "20 >= 10", termGE2.String())

	// Test less than or equal (<=)
	termLE1 := NewTerm(*leftConst10, *rightConst10, OpLE)
	assert.Equal(t, "10 <= 10", termLE1.String())
	termLE2 := NewTerm(*leftConst10, *rightConst15, OpLE)
	assert.Equal(t, "10 <= 15", termLE2.String())

	// Test string comparisons
	strLeft := NewConstantExpression(*NewStringConstant("apple"))
	strRight := NewConstantExpression(*NewStringConstant("banana"))
	termStrGT := NewTerm(*strRight, *strLeft, OpGT)
	assert.Equal(t, "banana > apple", termStrGT.String())
	termStrLT := NewTerm(*strLeft, *strRight, OpLT)
	assert.Equal(t, "apple < banana", termStrLT.String())

	// Test boolean comparisons
	boolLeft := NewConstantExpression(*NewBoolConstant(false))
	boolRight := NewConstantExpression(*NewBoolConstant(true))
	termBoolLT := NewTerm(*boolLeft, *boolRight, OpLT)
	assert.Equal(t, "false < true", termBoolLT.String())
	termBoolGT := NewTerm(*boolRight, *boolLeft, OpGT)
	assert.Equal(t, "true > false", termBoolGT.String())
}
