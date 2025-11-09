package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
)

func TestSelectPlanMethods(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	// Create a table
	tableName := "students"
	layout := record.NewLayoutFromSchema(schema)
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Insert some test data
	ts := record.NewTableScan(tx, layout, tableName)
	for i := 1; i <= 10; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		ts.SetString("name", "Person")
		ts.SetInt("age", 20+(i%3)) // ages: 20, 21, 22
	}
	ts.Close()

	// Create a TablePlan
	tablePlan := NewTablePlan(tableName, tx, md)

	// Create a predicate: age = 20
	fieldExpr := query.NewFieldNameExpression("age")
	constExpr := query.NewConstantExpression(*query.NewIntConstant(20))
	term := query.NewTerm(*fieldExpr, *constExpr)
	pred := query.NewPredicate(*term)

	// Create a SelectPlan
	selectPlan := NewSelectPlan(tablePlan, pred)

	// Test Schema - should return same schema as underlying plan
	resultSchema := selectPlan.Schema()
	require.NotNil(t, resultSchema)
	assert.Equal(t, tablePlan.Schema(), resultSchema)

	// Test BlocksAccessed - should be same as underlying plan
	assert.Equal(t, tablePlan.BlocksAccessed(), selectPlan.BlocksAccessed())

	// Test RecordsOutput - should be reduced by predicate's reduction factor
	tableRecords := tablePlan.RecordsOutput()
	selectRecords := selectPlan.RecordsOutput()
	assert.True(t, selectRecords <= tableRecords)
	assert.True(t, selectRecords > 0)

	// Test DistinctValues for field with constant predicate
	distinctAge := selectPlan.DistinctValues("age")
	assert.Equal(t, 1, distinctAge, "Field equated with constant should have 1 distinct value")

	// Test DistinctValues for field not in predicate
	distinctId := selectPlan.DistinctValues("id")
	assert.Equal(t, tablePlan.DistinctValues("id"), distinctId)

	// Test Open - should return a scan
	scan := selectPlan.Open()
	require.NotNil(t, scan)
	scan.Close()
}

func TestSelectPlanDistinctValuesWithFieldEquality(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema
	schema := record.NewSchema()
	schema.AddIntField("field1")
	schema.AddIntField("field2")

	// Create a table
	tableName := "test_table"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Create a TablePlan
	tablePlan := NewTablePlan(tableName, tx, md)

	// Create a predicate: field1 = field2
	fieldExpr1 := query.NewFieldNameExpression("field1")
	fieldExpr2 := query.NewFieldNameExpression("field2")
	term := query.NewTerm(*fieldExpr1, *fieldExpr2)
	pred := query.NewPredicate(*term)

	// Create a SelectPlan
	selectPlan := NewSelectPlan(tablePlan, pred)

	// Test DistinctValues - should return min of the two fields
	distinctField1 := selectPlan.DistinctValues("field1")
	distinctField2 := selectPlan.DistinctValues("field2")

	// Both should be the same (minimum of the two)
	assert.Equal(t, distinctField1, distinctField2)
}

func TestSelectPlanRecordsOutputCalculation(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("value")

	// Create a table
	tableName := "data"
	layout := record.NewLayoutFromSchema(schema)
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Insert data with known distinct values
	ts := record.NewTableScan(tx, layout, tableName)
	for i := 1; i <= 20; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		ts.SetInt("value", i%5) // 5 distinct values: 0,1,2,3,4
	}
	ts.Close()

	// Create a TablePlan
	tablePlan := NewTablePlan(tableName, tx, md)

	// Create a predicate: value = 0
	fieldExpr := query.NewFieldNameExpression("value")
	constExpr := query.NewConstantExpression(*query.NewIntConstant(0))
	term := query.NewTerm(*fieldExpr, *constExpr)
	pred := query.NewPredicate(*term)

	// Create a SelectPlan
	selectPlan := NewSelectPlan(tablePlan, pred)

	// Test RecordsOutput calculation
	tableRecords := tablePlan.RecordsOutput()
	selectRecords := selectPlan.RecordsOutput()

	// RecordsOutput should be tableRecords / reductionFactor
	assert.Equal(t, 20, tableRecords)
	assert.True(t, selectRecords < tableRecords)
	assert.True(t, selectRecords > 0)
}
