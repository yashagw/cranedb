package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
)

func TestProjectPlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table with multiple fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("age")
	schema.AddStringField("name", 20)
	schema.AddStringField("email", 50)
	tableName := "test"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Create TablePlan and ProjectPlan that projects only id and name
	tablePlan := NewTablePlan(tableName, tx, md)
	fieldList := []string{"id", "name"}
	projectPlan := NewProjectPlan(tablePlan, fieldList)

	// Test Schema - should only have projected fields
	projectedSchema := projectPlan.Schema()
	require.NotNil(t, projectedSchema)
	assert.True(t, projectedSchema.HasField("id"))
	assert.True(t, projectedSchema.HasField("name"))
	assert.False(t, projectedSchema.HasField("age"))
	assert.False(t, projectedSchema.HasField("email"))

	// Test BlocksAccessed - same as underlying plan
	assert.Equal(t, tablePlan.BlocksAccessed(), projectPlan.BlocksAccessed())

	// Test RecordsOutput - same as underlying plan
	assert.Equal(t, tablePlan.RecordsOutput(), projectPlan.RecordsOutput())

	// Test DistinctValues - delegates to underlying plan
	assert.Equal(t, tablePlan.DistinctValues("id"), projectPlan.DistinctValues("id"))
	assert.Equal(t, tablePlan.DistinctValues("name"), projectPlan.DistinctValues("name"))

	// Test Open
	scan := projectPlan.Open()
	require.NotNil(t, scan)
	scan.Close()
}
