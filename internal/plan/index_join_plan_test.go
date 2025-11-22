package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// setupIndexJoinPlanTest creates two tables for index join plan testing
func setupIndexJoinPlanTest(t *testing.T) (string, *transaction.Transaction, *metadata.Manager, func()) {
	testDir, tx, md, cleanup := setupTestDB(t)

	// Create Students table
	studentsSchema := record.NewSchema()
	studentsSchema.AddIntField("id")
	studentsSchema.AddStringField("name", 20)
	studentsSchema.AddIntField("dept_id")
	err := md.CreateTable("Students", studentsSchema, tx)
	require.NoError(t, err)

	// Create Departments table
	deptSchema := record.NewSchema()
	deptSchema.AddIntField("dept_id")
	deptSchema.AddStringField("dept_name", 20)
	err = md.CreateTable("Departments", deptSchema, tx)
	require.NoError(t, err)

	// Insert some data into Students
	studentsLayout, err := md.GetTableLayout("Students", tx)
	require.NoError(t, err)
	studentsTS, err := table.NewTableScan(tx, studentsLayout, "Students")
	require.NoError(t, err)

	students := []struct {
		id     int
		name   string
		deptID int
	}{
		{1, "Alice", 1},
		{2, "Bob", 2},
		{3, "Charlie", 1},
	}

	err = studentsTS.BeforeFirst()
	require.NoError(t, err)
	for _, student := range students {
		err = studentsTS.Insert()
		require.NoError(t, err)
		err = studentsTS.SetInt("id", student.id)
		require.NoError(t, err)
		err = studentsTS.SetString("name", student.name)
		require.NoError(t, err)
		err = studentsTS.SetInt("dept_id", student.deptID)
		require.NoError(t, err)
	}
	studentsTS.Close()

	// Insert some data into Departments
	deptLayout, err := md.GetTableLayout("Departments", tx)
	require.NoError(t, err)
	deptTS, err := table.NewTableScan(tx, deptLayout, "Departments")
	require.NoError(t, err)

	departments := []struct {
		deptID   int
		deptName string
	}{
		{1, "CS"},
		{2, "Math"},
	}

	err = deptTS.BeforeFirst()
	require.NoError(t, err)
	for _, dept := range departments {
		err = deptTS.Insert()
		require.NoError(t, err)
		err = deptTS.SetInt("dept_id", dept.deptID)
		require.NoError(t, err)
		err = deptTS.SetString("dept_name", dept.deptName)
		require.NoError(t, err)
	}
	deptTS.Close()

	// Create index on Departments.dept_id
	err = md.CreateIndex("dept_id_index", "Departments", "dept_id", tx)
	require.NoError(t, err)

	// Populate the index
	indexInfoMap, err := md.GetIndexInfo("Departments", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["dept_id"]
	require.True(t, exists, "Index info for dept_id should exist")
	idx, err := indexInfo.Open()
	require.NoError(t, err)

	deptTS, err = table.NewTableScan(tx, deptLayout, "Departments")
	require.NoError(t, err)
	err = deptTS.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := deptTS.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		deptID, err := deptTS.GetInt("dept_id")
		require.NoError(t, err)
		rid, err := deptTS.GetRID()
		require.NoError(t, err)
		err = idx.Insert(deptID, rid)
		require.NoError(t, err)
	}
	deptTS.Close()
	idx.Close()

	return testDir, tx, md, cleanup
}

// TestIndexJoinPlanSchema tests that the schema combines fields from both plans
func TestIndexJoinPlanSchema(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	// Create plans
	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo("Departments", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["dept_id"]
	require.True(t, exists, "Index info for dept_id should exist")

	// Create index join plan
	indexJoinPlan := NewIndexJoinPlan(studentsPlan, departmentsPlan, indexInfo, "dept_id")

	// Test schema
	schema := indexJoinPlan.Schema()
	require.NotNil(t, schema)

	// Should have fields from Students (lhs)
	assert.True(t, schema.HasField("id"))
	assert.True(t, schema.HasField("name"))
	assert.True(t, schema.HasField("dept_id"))

	// Should have fields from Departments (rhs)
	assert.True(t, schema.HasField("dept_name"))

	// dept_id exists in both, but should still be in schema
}

// TestIndexJoinPlanRecordsOutput tests the records output estimation
func TestIndexJoinPlanRecordsOutput(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	// Create plans
	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo("Departments", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["dept_id"]
	require.True(t, exists, "Index info for dept_id should exist")

	// Create index join plan
	indexJoinPlan := NewIndexJoinPlan(studentsPlan, departmentsPlan, indexInfo, "dept_id")

	// Test RecordsOutput
	// Formula: p1.recordsOutput() * ii.recordsOutput()
	p1Records := studentsPlan.RecordsOutput()
	iiRecords := indexInfo.RecordsOutput()
	expectedRecords := p1Records * iiRecords

	actualRecords := indexJoinPlan.RecordsOutput()
	assert.Equal(t, expectedRecords, actualRecords, "RecordsOutput should be p1.recordsOutput() * ii.recordsOutput()")
}

// TestIndexJoinPlanBlocksAccessed tests the blocks accessed estimation
func TestIndexJoinPlanBlocksAccessed(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	// Create plans
	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo("Departments", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["dept_id"]
	require.True(t, exists, "Index info for dept_id should exist")

	// Create index join plan
	indexJoinPlan := NewIndexJoinPlan(studentsPlan, departmentsPlan, indexInfo, "dept_id")

	// Test BlocksAccessed
	// Formula: p1.blocksAccessed() + (p1.recordsOutput() * ii.blocksAccessed()) + recordsOutput()
	p1Blocks := studentsPlan.BlocksAccessed()
	p1Records := studentsPlan.RecordsOutput()
	iiBlocks := indexInfo.BlocksAccessed()
	recordsOutput := indexJoinPlan.RecordsOutput()
	expectedBlocks := p1Blocks + (p1Records * iiBlocks) + recordsOutput

	actualBlocks := indexJoinPlan.BlocksAccessed()
	assert.Equal(t, expectedBlocks, actualBlocks, "BlocksAccessed should match formula")
}

// TestIndexJoinPlanDistinctValues tests distinct values delegation
func TestIndexJoinPlanDistinctValues(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	// Create plans
	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo("Departments", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["dept_id"]
	require.True(t, exists, "Index info for dept_id should exist")

	// Create index join plan
	indexJoinPlan := NewIndexJoinPlan(studentsPlan, departmentsPlan, indexInfo, "dept_id")

	// Test DistinctValues for field in p1
	val1, err := studentsPlan.DistinctValues("id")
	require.NoError(t, err)
	val2, err := indexJoinPlan.DistinctValues("id")
	require.NoError(t, err)
	assert.Equal(t, val1, val2, "DistinctValues for field in p1 should delegate to p1")

	// Test DistinctValues for field in p2
	val3, err := departmentsPlan.DistinctValues("dept_name")
	require.NoError(t, err)
	val4, err := indexJoinPlan.DistinctValues("dept_name")
	require.NoError(t, err)
	assert.Equal(t, val3, val4, "DistinctValues for field in p2 should delegate to p2")
}

// TestIndexJoinPlanOpen tests opening the index join scan
func TestIndexJoinPlanOpen(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	// Create plans
	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo("Departments", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["dept_id"]
	require.True(t, exists, "Index info for dept_id should exist")

	// Create index join plan
	indexJoinPlan := NewIndexJoinPlan(studentsPlan, departmentsPlan, indexInfo, "dept_id")

	// Test Open
	scan, err := indexJoinPlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)

	// Verify we can iterate
	count := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		// Verify we can access fields from both sides
		studentID, err := scan.GetInt("id")
		require.NoError(t, err)
		studentName, err := scan.GetString("name")
		require.NoError(t, err)
		deptName, err := scan.GetString("dept_name")
		require.NoError(t, err)

		t.Logf("Join result: %s (id=%d) -> %s", studentName, studentID, deptName)
	}

	// Should have join results (3 students joined with departments)
	assert.Greater(t, count, 0, "Should have at least one join result")

	scan.Close()
}

// TestIndexJoinPlanOpenWithNonTablePlan tests that Open returns error when p2 is not a TablePlan
func TestIndexJoinPlanOpenWithNonTablePlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a table
	schema := record.NewSchema()
	schema.AddIntField("id")
	err := md.CreateTable("TestTable", schema, tx)
	require.NoError(t, err)

	// Create plans
	tablePlan, err := NewTablePlan("TestTable", tx, md)
	require.NoError(t, err)

	// Create a product plan (not a table plan) for p2
	productPlan := NewProductPlan(tablePlan, tablePlan)

	// Create an index first
	err = md.CreateIndex("id_index", "TestTable", "id", tx)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo("TestTable", tx)
	require.NoError(t, err)
	indexInfo, exists := indexInfoMap["id"]
	require.True(t, exists, "Index info for id should exist")

	// Create index join plan
	indexJoinPlan := NewIndexJoinPlan(tablePlan, productPlan, indexInfo, "id")

	// Open should fail because p2 opens to a ProductScan, not TableScan
	scan, err := indexJoinPlan.Open()
	require.Error(t, err, "Open should fail when p2 is not a TableScan")
	require.Nil(t, scan)
	assert.Contains(t, err.Error(), "not a TableScan", "Error should mention TableScan")
}

