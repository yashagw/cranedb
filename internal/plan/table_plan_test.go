package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
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
	ts := record.NewTableScan(tx, layout, tableName)
	for i := 1; i <= 10; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		ts.SetInt("status", i%3) // 3 distinct values
		ts.SetString("name", "test")
	}
	ts.Close()

	// Create TablePlan and test all Plan interface methods
	tablePlan := NewTablePlan(tableName, tx, md)

	// Test Schema
	schema2 := tablePlan.Schema()
	require.NotNil(t, schema2)
	assert.True(t, schema2.HasField("id"))
	assert.True(t, schema2.HasField("status"))
	assert.True(t, schema2.HasField("name"))

	// Test statistics methods
	assert.Equal(t, 10, tablePlan.RecordsOutput())
	assert.True(t, tablePlan.BlocksAccessed() >= 1)
	assert.Equal(t, 10, tablePlan.DistinctValues("id"))
	assert.Equal(t, 3, tablePlan.DistinctValues("status"))

	// Test Open
	scan := tablePlan.Open()
	require.NotNil(t, scan)
	scan.Close()
}

func TestTablePlanNonExistentTable(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	assert.Panics(t, func() {
		NewTablePlan("nonexistent", tx, md)
	})
}
