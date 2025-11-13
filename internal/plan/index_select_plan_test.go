package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

func TestIndexSelectPlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table with multiple fields
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
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i)
		require.NoError(t, err)
		err = ts.SetInt("status", i%3) // 3 distinct values: 0, 1, 2
		require.NoError(t, err)
		err = ts.SetString("name", "test")
		require.NoError(t, err)
	}
	ts.Close()

	// Create an index on the "id" field
	indexName := "test_id_idx"
	err = md.CreateIndex(indexName, tableName, "id", tx)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo(tableName, tx)
	require.NoError(t, err)
	require.NotNil(t, indexInfoMap)
	indexInfo, exists := indexInfoMap["id"]
	require.True(t, exists, "Index info for 'id' field should exist")
	require.NotNil(t, indexInfo)

	// Create TablePlan and IndexSelectPlan
	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)
	searchValue := 5
	indexSelectPlan := NewIndexSelectPlan(tablePlan, indexInfo, searchValue)

	// Test Schema - should return the schema of the underlying table
	planSchema := indexSelectPlan.Schema()
	require.NotNil(t, planSchema)
	assert.True(t, planSchema.HasField("id"))
	assert.True(t, planSchema.HasField("status"))
	assert.True(t, planSchema.HasField("name"))

	// Test RecordsOutput - should delegate to indexInfo
	expectedRecords := indexInfo.RecordsOutput()
	actualRecords := indexSelectPlan.RecordsOutput()
	assert.Equal(t, expectedRecords, actualRecords)

	// Test BlocksAccessed - should be indexInfo.BlocksAccessed() + RecordsOutput()
	expectedBlocks := indexInfo.BlocksAccessed() + indexSelectPlan.RecordsOutput()
	actualBlocks := indexSelectPlan.BlocksAccessed()
	assert.Equal(t, expectedBlocks, actualBlocks)

	// Test DistinctValues - should delegate to indexInfo
	// For the indexed field, should return 1
	indexedFieldDistinct := indexSelectPlan.DistinctValues("id")
	assert.Equal(t, 1, indexedFieldDistinct)

	// For non-indexed fields, should return the stat info distinct values
	statusDistinct, err := tablePlan.DistinctValues("status")
	require.NoError(t, err)
	indexSelectStatusDistinct := indexSelectPlan.DistinctValues("status")
	assert.Equal(t, statusDistinct, indexSelectStatusDistinct)

	nameDistinct, err := tablePlan.DistinctValues("name")
	require.NoError(t, err)
	indexSelectNameDistinct := indexSelectPlan.DistinctValues("name")
	assert.Equal(t, nameDistinct, indexSelectNameDistinct)

	// Test Open
	scan, err := indexSelectPlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)
	scan.Close()
}

func TestIndexSelectPlanWithStringField(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table with string field
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	tableName := "test"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Insert test data
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for i := 1; i <= 5; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i)
		require.NoError(t, err)
		err = ts.SetString("name", "test")
		require.NoError(t, err)
	}
	ts.Close()

	// Create an index on the "name" field (string field)
	indexName := "test_name_idx"
	err = md.CreateIndex(indexName, tableName, "name", tx)
	require.NoError(t, err)

	// Get index info
	indexInfoMap, err := md.GetIndexInfo(tableName, tx)
	require.NoError(t, err)
	require.NotNil(t, indexInfoMap)
	indexInfo, exists := indexInfoMap["name"]
	require.True(t, exists, "Index info for 'name' field should exist")
	require.NotNil(t, indexInfo)

	// Create TablePlan and IndexSelectPlan
	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)
	searchValue := "test"
	indexSelectPlan := NewIndexSelectPlan(tablePlan, indexInfo, searchValue)

	// Test Schema
	planSchema := indexSelectPlan.Schema()
	require.NotNil(t, planSchema)
	assert.True(t, planSchema.HasField("id"))
	assert.True(t, planSchema.HasField("name"))

	// Test RecordsOutput
	expectedRecords := indexInfo.RecordsOutput()
	actualRecords := indexSelectPlan.RecordsOutput()
	assert.Equal(t, expectedRecords, actualRecords)

	// Test BlocksAccessed
	expectedBlocks := indexInfo.BlocksAccessed() + indexSelectPlan.RecordsOutput()
	actualBlocks := indexSelectPlan.BlocksAccessed()
	assert.Equal(t, expectedBlocks, actualBlocks)

	// Test DistinctValues for indexed string field
	indexedFieldDistinct := indexSelectPlan.DistinctValues("name")
	assert.Equal(t, 1, indexedFieldDistinct)

	// Test Open
	scan, err := indexSelectPlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)
	scan.Close()
}
