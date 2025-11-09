package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
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
	ts1 := record.NewTableScan(tx, layout1, table1)
	for i := 1; i <= 3; i++ {
		ts1.Insert()
		ts1.SetInt("student_id", i)
		ts1.SetString("name", "Student")
	}
	ts1.Close()

	layout2 := record.NewLayoutFromSchema(schema2)
	ts2 := record.NewTableScan(tx, layout2, table2)
	for i := 1; i <= 2; i++ {
		ts2.Insert()
		ts2.SetInt("course_id", i*100)
		ts2.SetString("title", "Course")
	}
	ts2.Close()

	// Create TablePlans and ProductPlan
	tablePlan1 := NewTablePlan(table1, tx, md)
	tablePlan2 := NewTablePlan(table2, tx, md)
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
	assert.Equal(t, tablePlan1.DistinctValues("student_id"), productPlan.DistinctValues("student_id"))
	assert.Equal(t, tablePlan1.DistinctValues("name"), productPlan.DistinctValues("name"))
	assert.Equal(t, tablePlan2.DistinctValues("course_id"), productPlan.DistinctValues("course_id"))
	assert.Equal(t, tablePlan2.DistinctValues("title"), productPlan.DistinctValues("title"))

	// Test Open
	scan := productPlan.Open()
	require.NotNil(t, scan)
	scan.Close()
}
