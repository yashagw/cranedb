package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

func TestTablePlan(t *testing.T) {
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

	// Create TablePlan and test all Plan interface methods
	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)

	// Test Schema
	schema2 := tablePlan.Schema()
	require.NotNil(t, schema2)
	assert.True(t, schema2.HasField("id"))
	assert.True(t, schema2.HasField("status"))
	assert.True(t, schema2.HasField("name"))

	// Test statistics methods
	assert.Equal(t, 10, tablePlan.RecordsOutput())
	assert.True(t, tablePlan.BlocksAccessed() >= 1)
	distinctId, err := tablePlan.DistinctValues("id")
	require.NoError(t, err)
	assert.Equal(t, 10, distinctId)
	distinctStatus, err := tablePlan.DistinctValues("status")
	require.NoError(t, err)
	assert.Equal(t, 3, distinctStatus)

	// Test Open
	scan, err := tablePlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)
	scan.Close()
}

func TestTablePlanNonExistentTable(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := NewTablePlan("nonexistent", tx, md)
	assert.Error(t, err, "Should return error for nonexistent table")
}
