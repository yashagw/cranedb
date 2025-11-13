package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

func TestBasicUpdatePlanner_ExecuteInsert(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	tableName := "students"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Insert data
	insertData := parserdata.NewInsertData(
		tableName,
		[]string{"id", "name"},
		[]any{1, "Alice"},
	)

	count, err := planner.ExecuteInsert(insertData, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the insert
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)
	found := false
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ts.GetInt("id")
		require.NoError(t, err)
		name, err := ts.GetString("name")
		require.NoError(t, err)
		if id == 1 && name == "Alice" {
			found = true
			break
		}
	}
	ts.Close()
	assert.True(t, found, "Inserted record should be found")
}

func TestBasicUpdatePlanner_ExecuteInsertWithIndex(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	tableName := "students"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Create index
	err = md.CreateIndex("idx_name", tableName, "name", tx)
	require.NoError(t, err)

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Insert data
	insertData := parserdata.NewInsertData(
		tableName,
		[]string{"id", "name"},
		[]any{1, "Alice"},
	)

	count, err := planner.ExecuteInsert(insertData, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the insert
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)
	found := false
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ts.GetInt("id")
		require.NoError(t, err)
		name, err := ts.GetString("name")
		require.NoError(t, err)
		if id == 1 && name == "Alice" {
			found = true
			break
		}
	}

	// Verify the stored index data using IndexSelectScan
	indexInfoMap, err := md.GetIndexInfo(tableName, tx)
	require.NoError(t, err)
	require.NotNil(t, indexInfoMap)
	indexInfo, exists := indexInfoMap["name"]
	require.True(t, exists, "Index info for 'name' field should exist")
	require.NotNil(t, indexInfo)

	// Open the index
	idx, err := indexInfo.Open()
	require.NoError(t, err)
	require.NotNil(t, idx)

	// Create a new table scan for the index scan
	ts2, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)

	// Create IndexSelectScan to search for "Alice"
	// Note: IndexSelectScan.Close() will close both the index and table scan
	indexScan, err := query.NewIndexSelectScan(ts2, idx, "Alice")
	require.NoError(t, err)
	require.NotNil(t, indexScan)
	defer indexScan.Close()

	// Verify we can find the record using the index
	hasNext, err := indexScan.Next()
	require.NoError(t, err)
	assert.True(t, hasNext, "Index should find the record with name='Alice'")

	// Verify the record data matches
	foundId, err := indexScan.GetInt("id")
	require.NoError(t, err)
	foundName, err := indexScan.GetString("name")
	require.NoError(t, err)
	assert.Equal(t, 1, foundId)
	assert.Equal(t, "Alice", foundName)

	// Verify there are no more records with this value
	hasNext, err = indexScan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "Should only find one record with name='Alice'")

	ts.Close()
	assert.True(t, found, "Inserted record should be found")
}

func TestBasicUpdatePlanner_ExecuteDelete(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	tableName := "students"
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
		if i == 3 {
			err = ts.SetString("name", "ToDelete")
			require.NoError(t, err)
		} else {
			err = ts.SetString("name", "Student")
			require.NoError(t, err)
		}
	}
	ts.Close()

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Delete records where name = "ToDelete"
	term := query.NewTerm(
		*query.NewFieldNameExpression("name"),
		*query.NewConstantExpression(*query.NewStringConstant("ToDelete")),
	)
	pred := query.NewPredicate(*term)

	deleteData := parserdata.NewDeleteData(tableName, pred)
	count, err := planner.ExecuteDelete(deleteData, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Should delete 1 record (id=3)")

	// Verify deletion
	ts, err = table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)
	remaining := 0
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		remaining++
		name, err := ts.GetString("name")
		require.NoError(t, err)
		assert.Equal(t, "Student", name, "Only 'Student' records should remain")
	}
	ts.Close()
	assert.Equal(t, 4, remaining)
}

func TestBasicUpdatePlanner_ExecuteModify(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a schema and table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	tableName := "students"
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	// Insert test data
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i)
		require.NoError(t, err)
		err = ts.SetString("name", "OldName")
		require.NoError(t, err)
	}
	ts.Close()

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Update records where id = 2
	term := query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewConstantExpression(*query.NewIntConstant(2)),
	)
	pred := query.NewPredicate(*term)

	newValue := query.NewConstantExpression(*query.NewStringConstant("NewName"))
	modifyData := parserdata.NewModifyData(tableName, "name", newValue, pred)

	count, err := planner.ExecuteModify(modifyData, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Should update 1 record")

	// Verify update
	ts, err = table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	updatedCount := 0
	oldNameCount := 0
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ts.GetString("name")
		require.NoError(t, err)
		if name == "NewName" {
			updatedCount++
			id, err := ts.GetInt("id")
			require.NoError(t, err)
			assert.Equal(t, 2, id)
		} else {
			oldNameCount++
			id, err := ts.GetInt("id")
			require.NoError(t, err)
			assert.NotEqual(t, 2, id)
		}
	}
	ts.Close()
	assert.Equal(t, 1, updatedCount)
	assert.Equal(t, 2, oldNameCount)
}

func TestBasicUpdatePlanner_ExecuteCreateTable(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Create table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 30)
	createTableData := parserdata.NewCreateTableData("newtable", schema)

	count, err := planner.ExecuteCreateTable(createTableData, tx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify table exists by trying to get its layout
	layout, err := md.GetTableLayout("newtable", tx)
	require.NoError(t, err)
	assert.NotNil(t, layout)
	assert.True(t, layout.GetSchema().HasField("id"))
	assert.True(t, layout.GetSchema().HasField("name"))
}

func TestBasicUpdatePlanner_ExecuteCreateView(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a base table first
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err := md.CreateTable("students", schema, tx)
	require.NoError(t, err)

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Create view
	queryData := parserdata.NewQueryData(
		[]string{"id", "name"},
		[]string{"students"},
		nil,
	)
	createViewData := parserdata.NewCreateViewData("studentview", queryData)

	count, err := planner.ExecuteCreateView(createViewData, tx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify view exists
	viewDef, err := md.GetViewDef("studentview", tx)
	require.NoError(t, err)
	assert.Equal(t, "SELECT id, name FROM students", viewDef)
}

func TestBasicUpdatePlanner_ExecuteCreateIndex(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create planner
	planner := NewBasicUpdatePlanner(md)

	// Create a base table first
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err := md.CreateTable("students", schema, tx)
	require.NoError(t, err)

	// Create index
	createIndexData := parserdata.NewCreateIndexData("idx_name", "students", "name")
	count, err := planner.ExecuteCreateIndex(createIndexData, tx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify index exists
	tableIndexInfo, err := md.GetIndexInfo("students", tx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(tableIndexInfo))
	indexInfo, ok := tableIndexInfo["name"]
	require.True(t, ok)
	require.NotNil(t, indexInfo)
	require.Equal(t, "idx_name", indexInfo.IndexName())
	require.Equal(t, "name", indexInfo.FieldName())
	require.Equal(t, schema, indexInfo.TableSchema())
}
