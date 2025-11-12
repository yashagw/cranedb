package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

func TestProductPlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create two tables with different schemas
	schema1 := record.NewSchema()
	schema1.AddIntField("student_id")
	schema1.AddStringField("name", 20)
	table1 := "students"
	err := md.CreateTable(table1, schema1, tx)
	require.NoError(t, err)

	schema2 := record.NewSchema()
	schema2.AddIntField("course_id")
	schema2.AddStringField("title", 30)
	table2 := "courses"
	err = md.CreateTable(table2, schema2, tx)
	require.NoError(t, err)

	// Insert data into both tables
	layout1 := record.NewLayoutFromSchema(schema1)
	ts1, err := scan.NewTableScan(tx, layout1, table1)
	require.NoError(t, err)
	err = ts1.BeforeFirst()
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		err = ts1.Insert()
		require.NoError(t, err)
		err = ts1.SetInt("student_id", i)
		require.NoError(t, err)
		err = ts1.SetString("name", "Student")
		require.NoError(t, err)
	}
	ts1.Close()

	layout2 := record.NewLayoutFromSchema(schema2)
	ts2, err := scan.NewTableScan(tx, layout2, table2)
	require.NoError(t, err)
	err = ts2.BeforeFirst()
	require.NoError(t, err)
	for i := 1; i <= 2; i++ {
		err = ts2.Insert()
		require.NoError(t, err)
		err = ts2.SetInt("course_id", i*100)
		require.NoError(t, err)
		err = ts2.SetString("title", "Course")
		require.NoError(t, err)
	}
	ts2.Close()

	// Create TablePlans and ProductPlan
	tablePlan1, err := NewTablePlan(table1, tx, md)
	require.NoError(t, err)
	tablePlan2, err := NewTablePlan(table2, tx, md)
	require.NoError(t, err)
	productPlan := NewProductPlan(tablePlan1, tablePlan2)

	// Test Schema - should contain fields from both tables
	schema := productPlan.Schema()
	require.NotNil(t, schema)
	assert.True(t, schema.HasField("student_id"))
	assert.True(t, schema.HasField("name"))
	assert.True(t, schema.HasField("course_id"))
	assert.True(t, schema.HasField("title"))

	// Test RecordsOutput - should be product of both plans
	assert.Equal(t, 3, tablePlan1.RecordsOutput())
	assert.Equal(t, 2, tablePlan2.RecordsOutput())
	assert.Equal(t, 6, productPlan.RecordsOutput()) // 3 * 2

	// Test BlocksAccessed - should be p1.blocks + (p1.records * p2.blocks)
	expectedBlocks := tablePlan1.BlocksAccessed() + (tablePlan1.RecordsOutput() * tablePlan2.BlocksAccessed())
	assert.Equal(t, expectedBlocks, productPlan.BlocksAccessed())

	// Test DistinctValues - should delegate to appropriate plan
	val1, err := tablePlan1.DistinctValues("student_id")
	require.NoError(t, err)
	val2, err := productPlan.DistinctValues("student_id")
	require.NoError(t, err)
	assert.Equal(t, val1, val2)
	val3, err := tablePlan1.DistinctValues("name")
	require.NoError(t, err)
	val4, err := productPlan.DistinctValues("name")
	require.NoError(t, err)
	assert.Equal(t, val3, val4)
	val5, err := tablePlan2.DistinctValues("course_id")
	require.NoError(t, err)
	val6, err := productPlan.DistinctValues("course_id")
	require.NoError(t, err)
	assert.Equal(t, val5, val6)
	val7, err := tablePlan2.DistinctValues("title")
	require.NoError(t, err)
	val8, err := productPlan.DistinctValues("title")
	require.NoError(t, err)
	assert.Equal(t, val7, val8)

	// Test Open
	scan, err := productPlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)
	scan.Close()
}
