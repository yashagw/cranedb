package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
)

func TestPredicateBasic(t *testing.T) {
	// Create a simple predicate with one term
	fieldExpr := NewFieldNameExpression("age")
	constExpr := NewConstantExpression(*NewIntConstant(25))
	term := NewTerm(*fieldExpr, *constExpr)
	pred := NewPredicate(*term)
	require.NotNil(t, pred)
	assert.Equal(t, "age = 25", pred.String())

	// Test ConjunctWith
	fieldExpr2 := NewFieldNameExpression("name")
	constExpr2 := NewConstantExpression(*NewStringConstant("John"))
	term2 := NewTerm(*fieldExpr2, *constExpr2)
	pred2 := NewPredicate(*term2)
	pred.ConjunctWith(*pred2)
	assert.Equal(t, "age = 25 and name = John", pred.String())

	// Test empty predicate string
	emptyPred := &Predicate{terms: []Term{}}
	assert.Equal(t, "", emptyPred.String())
}

func TestPredicateSelectSubPred(t *testing.T) {
	// Create schema
	schema := record.NewSchema()
	schema.AddIntField("age")
	schema.AddStringField("name", 20)

	// Create predicate with terms that apply to schema
	term1 := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
	term2 := NewTerm(*NewFieldNameExpression("name"), *NewConstantExpression(*NewStringConstant("John")))
	pred := NewPredicate(*term1)
	pred.ConjunctWith(*NewPredicate(*term2))

	// SelectSubPred should return all terms that apply
	result := pred.SelectSubPred(schema)
	require.NotNil(t, result)
	assert.Equal(t, "age = 25 and name = John", result.String())

	// Create predicate with term that doesn't apply
	term3 := NewTerm(*NewFieldNameExpression("missing"), *NewConstantExpression(*NewIntConstant(10)))
	pred3 := NewPredicate(*term3)
	result2 := pred3.SelectSubPred(schema)
	assert.Nil(t, result2)

	// Mixed case: some terms apply, some don't
	pred4 := NewPredicate(*term1)
	pred4.ConjunctWith(*pred3)
	result3 := pred4.SelectSubPred(schema)
	require.NotNil(t, result3)
	assert.Equal(t, "age = 25", result3.String())
}

func TestPredicateJoinSubPred(t *testing.T) {
	// Create two schemas
	schema1 := record.NewSchema()
	schema1.AddIntField("id")
	schema1.AddStringField("name", 20)

	schema2 := record.NewSchema()
	schema2.AddIntField("user_id")
	schema2.AddStringField("city", 20)

	// Create join term: schema1.id = schema2.user_id
	joinTerm := NewTerm(*NewFieldNameExpression("id"), *NewFieldNameExpression("user_id"))
	pred := NewPredicate(*joinTerm)

	// JoinSubPred should return terms that don't apply to either schema individually
	// but apply to the combined schema
	result := pred.JoinSubPred(schema1, schema2)
	require.NotNil(t, result)
	assert.Equal(t, "id = user_id", result.String())

	// Term that applies to schema1 only should not be in join predicate
	term1 := NewTerm(*NewFieldNameExpression("id"), *NewConstantExpression(*NewIntConstant(1)))
	pred2 := NewPredicate(*term1)
	result2 := pred2.JoinSubPred(schema1, schema2)
	assert.Nil(t, result2)

	// Term that applies to schema2 only should not be in join predicate
	term2 := NewTerm(*NewFieldNameExpression("user_id"), *NewConstantExpression(*NewIntConstant(1)))
	pred3 := NewPredicate(*term2)
	result3 := pred3.JoinSubPred(schema1, schema2)
	assert.Nil(t, result3)
}

func TestPredicateEquatesWithConstant(t *testing.T) {
	// Create predicate with field = constant
	term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
	pred := NewPredicate(*term)

	// Should find the constant for the matching field
	result := pred.EquatesWithConstant("age")
	require.NotNil(t, result)
	assert.Equal(t, 25, result.AsInt())

	// Should return nil for non-matching field
	result2 := pred.EquatesWithConstant("name")
	assert.Nil(t, result2)

	// Test with multiple terms
	term2 := NewTerm(*NewFieldNameExpression("name"), *NewConstantExpression(*NewStringConstant("John")))
	pred.ConjunctWith(*NewPredicate(*term2))

	result3 := pred.EquatesWithConstant("name")
	require.NotNil(t, result3)
	assert.Equal(t, "John", result3.AsString())

	// Test with field = field (no constant)
	term3 := NewTerm(*NewFieldNameExpression("id"), *NewFieldNameExpression("age"))
	pred3 := NewPredicate(*term3)
	result4 := pred3.EquatesWithConstant("id")
	assert.Nil(t, result4)
}

func TestPredicateEquatesWithField(t *testing.T) {
	// Create predicate with field = field
	term := NewTerm(*NewFieldNameExpression("id"), *NewFieldNameExpression("age"))
	pred := NewPredicate(*term)

	// Should find the other field for the matching field
	result := pred.EquatesWithField("id")
	require.NotNil(t, result)
	assert.Equal(t, "age", *result)

	// Should work from the other side
	result2 := pred.EquatesWithField("age")
	require.NotNil(t, result2)
	assert.Equal(t, "id", *result2)

	// Should return nil for non-matching field
	result3 := pred.EquatesWithField("name")
	assert.Nil(t, result3)

	// Test with multiple terms
	term2 := NewTerm(*NewFieldNameExpression("name"), *NewFieldNameExpression("alias"))
	pred.ConjunctWith(*NewPredicate(*term2))

	result4 := pred.EquatesWithField("name")
	require.NotNil(t, result4)
	assert.Equal(t, "alias", *result4)

	// Test with field = constant (not field = field)
	term3 := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
	pred3 := NewPredicate(*term3)
	result5 := pred3.EquatesWithField("age")
	assert.Nil(t, result5)
}
