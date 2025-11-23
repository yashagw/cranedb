package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

func TestMaterializePlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("status")
	schema.AddStringField("name", 20)
	tableName := "test"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Insert test data
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		ts.SetInt("status", i%3) // 3 distinct values
		ts.SetString("name", "test")
	}
	ts.Close()

	// Create TablePlan and MaterializePlan
	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)
	materializePlan := NewMaterializePlan(tx, tablePlan)

	// Test Schema - should be same as source plan
	materializedSchema := materializePlan.Schema()
	require.NotNil(t, materializedSchema)
	assert.True(t, materializedSchema.HasField("id"))
	assert.True(t, materializedSchema.HasField("status"))
	assert.True(t, materializedSchema.HasField("name"))

	// Test RecordsOutput - should be same as source plan
	assert.Equal(t, tablePlan.RecordsOutput(), materializePlan.RecordsOutput())
	assert.Equal(t, 10, materializePlan.RecordsOutput())

	// Test DistinctValues - should delegate to source plan
	tableId, err := tablePlan.DistinctValues("id")
	require.NoError(t, err)
	materializedId, err := materializePlan.DistinctValues("id")
	require.NoError(t, err)
	assert.Equal(t, tableId, materializedId)
	assert.Equal(t, 10, materializedId)

	tableStatus, err := tablePlan.DistinctValues("status")
	require.NoError(t, err)
	materializedStatus, err := materializePlan.DistinctValues("status")
	require.NoError(t, err)
	assert.Equal(t, tableStatus, materializedStatus)
	assert.Equal(t, 3, materializedStatus)

	// Test BlocksAccessed - should calculate based on materialized table size
	blocksAccessed := materializePlan.BlocksAccessed()
	assert.True(t, blocksAccessed >= 1, "Should access at least 1 block")

	// Test Open - should materialize the data
	scan, err := materializePlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)

	// Verify we can read all records
	count := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		// Verify we can read values
		id, err := scan.GetInt("id")
		require.NoError(t, err)
		assert.True(t, id >= 1 && id <= 10)

		status, err := scan.GetInt("status")
		require.NoError(t, err)
		assert.True(t, status >= 0 && status < 3)

		name, err := scan.GetString("name")
		require.NoError(t, err)
		assert.Equal(t, "test", name)
	}
	assert.Equal(t, 10, count, "Should read all 10 records")

	// Test that we can scan multiple times (materialized data persists)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	count2 := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count2++
	}
	assert.Equal(t, 10, count2, "Should be able to scan again")

	scan.Close()
}

func TestMaterializePlanWithEmptySource(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	tableName := "test"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Create TablePlan and MaterializePlan (no data inserted)
	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)
	materializePlan := NewMaterializePlan(tx, tablePlan)

	// Test Open with empty source
	scan, err := materializePlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)

	// Should have no records
	hasNext, err := scan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)

	scan.Close()
}

func TestMaterializePlanWithProjectPlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("age")
	schema.AddStringField("name", 20)
	tableName := "test"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Insert test data
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	for i := 1; i <= 5; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		ts.SetInt("age", 20+i)
		ts.SetString("name", "user")
	}
	ts.Close()

	// Create TablePlan -> ProjectPlan -> MaterializePlan
	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)
	projectPlan := NewProjectPlan(tablePlan, []string{"id", "name"})
	materializePlan := NewMaterializePlan(tx, projectPlan)

	// Test Schema - should only have projected fields
	materializedSchema := materializePlan.Schema()
	require.NotNil(t, materializedSchema)
	assert.True(t, materializedSchema.HasField("id"))
	assert.True(t, materializedSchema.HasField("name"))
	assert.False(t, materializedSchema.HasField("age"))

	// Test Open
	scan, err := materializePlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)

	// Verify we can read projected fields
	count := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		id, err := scan.GetInt("id")
		require.NoError(t, err)
		assert.True(t, id >= 1 && id <= 5)

		name, err := scan.GetString("name")
		require.NoError(t, err)
		assert.Equal(t, "user", name)

		// age should not be accessible
		assert.False(t, scan.HasField("age"))
	}
	assert.Equal(t, 5, count)

	scan.Close()
}
