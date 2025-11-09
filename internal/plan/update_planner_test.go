package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
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
	ts := record.NewTableScan(tx, layout, tableName)
	found := false
	for ts.Next() {
		if ts.GetInt("id") == 1 && ts.GetString("name") == "Alice" {
			found = true
			break
		}
	}
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
	ts := record.NewTableScan(tx, layout, tableName)
	for i := 1; i <= 5; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		if i == 3 {
			ts.SetString("name", "ToDelete")
		} else {
			ts.SetString("name", "Student")
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
	ts = record.NewTableScan(tx, layout, tableName)
	remaining := 0
	for ts.Next() {
		remaining++
		name := ts.GetString("name")
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
	ts := record.NewTableScan(tx, layout, tableName)
	for i := 1; i <= 3; i++ {
		ts.Insert()
		ts.SetInt("id", i)
		ts.SetString("name", "OldName")
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
	ts = record.NewTableScan(tx, layout, tableName)
	updatedCount := 0
	oldNameCount := 0
	for ts.Next() {
		name := ts.GetString("name")
		if name == "NewName" {
			updatedCount++
			assert.Equal(t, 2, ts.GetInt("id"))
		} else {
			oldNameCount++
			assert.NotEqual(t, 2, ts.GetInt("id"))
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
