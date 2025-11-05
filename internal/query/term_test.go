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
	term := NewTerm(*leftConst, *rightConst)
	require.NotNil(t, term)
	assert.Equal(t, "10 = 20", term.String())

	// Test creating term with field = constant
	fieldExpr := NewFieldNameExpression("age")
	constExpr := NewConstantExpression(*NewIntConstant(25))
	term2 := NewTerm(*fieldExpr, *constExpr)
	require.NotNil(t, term2)
	assert.Equal(t, "age = 25", term2.String())

	// Test creating term with field = field
	fieldExpr1 := NewFieldNameExpression("name")
	fieldExpr2 := NewFieldNameExpression("alias")
	term3 := NewTerm(*fieldExpr1, *fieldExpr2)
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
	term4 := NewTerm(*fieldExpr, *missingFieldExpr)
	assert.False(t, term4.AppliesTo(schema))
}

func TestTermEquatesWithConstant(t *testing.T) {
	fieldExpr := NewFieldNameExpression("age")
	constExpr := NewConstantExpression(*NewIntConstant(25))

	// Test field = constant
	term := NewTerm(*fieldExpr, *constExpr)
	result := term.EquatesWithConstant("age")
	require.NotNil(t, result)
	assert.Equal(t, 25, result.AsInt())

	// Test constant = field (reversed)
	term2 := NewTerm(*constExpr, *fieldExpr)
	result2 := term2.EquatesWithConstant("age")
	require.NotNil(t, result2)
	assert.Equal(t, 25, result2.AsInt())

	// Test field name doesn't match
	result3 := term.EquatesWithConstant("name")
	assert.Nil(t, result3)

	// Test field = field (no constant)
	fieldExpr2 := NewFieldNameExpression("name")
	term3 := NewTerm(*fieldExpr, *fieldExpr2)
	result4 := term3.EquatesWithConstant("age")
	assert.Nil(t, result4)
}

func TestTermEquatesWithField(t *testing.T) {
	fieldExpr1 := NewFieldNameExpression("name")
	fieldExpr2 := NewFieldNameExpression("alias")

	// Test field = field
	term := NewTerm(*fieldExpr1, *fieldExpr2)
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
	term2 := NewTerm(*fieldExpr3, *constExpr)
	result4 := term2.EquatesWithField("age")
	assert.Nil(t, result4)
}
