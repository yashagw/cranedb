package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// Helper to create and populate a table with test data
func createTableWithData(t *testing.T, tableName string, schema *record.Schema, md *metadata.Manager, tx *transaction.Transaction, dataFn func(*table.TableScan)) {
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	if dataFn != nil {
		layout := record.NewLayoutFromSchema(schema)
		ts, err := table.NewTableScan(tx, layout, tableName)
		if err != nil {
			t.Fatalf("Failed to create table scan: %v", err)
		}
		dataFn(ts)
		ts.Close()
	}
}

// Helper to count scan results
func countScanResults(s scan.Scan) (int, error) {
	count := 0
	for {
		hasNext, err := s.Next()
		if err != nil {
			return count, err
		}
		if !hasNext {
			break
		}
		count++
	}
	return count, nil
}

func TestBasicQueryPlanner_SingleTableWithPredicate(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	createTableWithData(t, "students", schema, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		for i := 1; i <= 5; i++ {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", i)
			require.NoError(t, err)
			err = ts.SetString("name", "Student")
			require.NoError(t, err)
			err = ts.SetInt("age", 20+i)
			require.NoError(t, err)
		}
	})

	planner := NewBasicQueryPlanner(md)
	pred := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewConstantExpression(*query.NewIntConstant(2)),
	))

	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name"}, []string{"students"}, pred,
	), tx)
	require.NoError(t, err)

	// Verify schema projection
	assert.True(t, plan.Schema().HasField("id"))
	assert.False(t, plan.Schema().HasField("age"))

	// Verify results
	queryScan, err := plan.Open()
	require.NoError(t, err)
	defer queryScan.Close()
	err = queryScan.BeforeFirst()
	require.NoError(t, err)
	count := 0
	for {
		hasNext, err := queryScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
		id, err := queryScan.GetInt("id")
		require.NoError(t, err)
		assert.Equal(t, 2, id)
	}
	assert.Equal(t, 1, count)
}

func TestBasicQueryPlanner_NoPredicate(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	createTableWithData(t, "products", schema, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		for i := 1; i <= 3; i++ {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", i)
			require.NoError(t, err)
			err = ts.SetString("name", "Product")
			require.NoError(t, err)
		}
	})

	planner := NewBasicQueryPlanner(md)
	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name"}, []string{"products"}, nil,
	), tx)
	require.NoError(t, err)

	scan, err := plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	count, err := countScanResults(scan)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestBasicQueryPlanner_CartesianProduct(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	s1 := record.NewSchema()
	s1.AddIntField("sid")
	s2 := record.NewSchema()
	s2.AddIntField("cid")

	createTableWithData(t, "students", s1, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		for i := 1; i <= 2; i++ {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("sid", i)
			require.NoError(t, err)
		}
	})
	createTableWithData(t, "courses", s2, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		for i := 1; i <= 2; i++ {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("cid", i)
			require.NoError(t, err)
		}
	})

	planner := NewBasicQueryPlanner(md)
	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"sid", "cid"}, []string{"students", "courses"}, nil,
	), tx)
	require.NoError(t, err)

	scan, err := plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)
	count, err := countScanResults(scan)
	require.NoError(t, err)
	assert.Equal(t, 4, count) // 2 * 2 = 4
}

func TestBasicQueryPlanner_JoinWithPredicate(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create students table: id, name
	s1 := record.NewSchema()
	s1.AddIntField("id")
	s1.AddStringField("name", 20)
	md.CreateTable("students", s1, tx)
	ts1, err := table.NewTableScan(tx, record.NewLayoutFromSchema(s1), "students")
	require.NoError(t, err)

	ts1.Insert()
	ts1.SetInt("id", 1)
	ts1.SetString("name", "Alice")

	ts1.Insert()
	ts1.SetInt("id", 2)
	ts1.SetString("name", "Bob")

	ts1.Insert()
	ts1.SetInt("id", 3)
	ts1.SetString("name", "Charlie")

	ts1.Insert()
	ts1.SetInt("id", 4)
	ts1.SetString("name", "Diana")
	ts1.Close()

	// Create enrollments table: student_id, course
	s2 := record.NewSchema()
	s2.AddIntField("student_id")
	s2.AddStringField("course", 20)
	md.CreateTable("enrollments", s2, tx)
	ts2, err := table.NewTableScan(tx, record.NewLayoutFromSchema(s2), "enrollments")
	require.NoError(t, err)

	ts2.Insert()
	ts2.SetInt("student_id", 1)
	ts2.SetString("course", "Math")

	ts2.Insert()
	ts2.SetInt("student_id", 2)
	ts2.SetString("course", "Physics")

	ts2.Insert()
	ts2.SetInt("student_id", 2)
	ts2.SetString("course", "Chemistry")

	ts2.Insert()
	ts2.SetInt("student_id", 3)
	ts2.SetString("course", "History")
	ts2.Close()

	// Query: SELECT name, course FROM students, enrollments
	// WHERE id = student_id AND name = "Bob"
	planner := NewBasicQueryPlanner(md)

	// Create predicate: id = student_id AND name = "Bob"
	term1 := query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewFieldNameExpression("student_id"),
	)
	term2 := query.NewTerm(
		*query.NewFieldNameExpression("name"),
		*query.NewConstantExpression(*query.NewStringConstant("Bob")),
	)
	pred := query.NewPredicate(*term1)
	pred.ConjunctWith(*query.NewPredicate(*term2))

	plan, _ := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"name", "course"}, []string{"students", "enrollments"}, pred,
	), tx)

	// Should return 2 records: Bob enrolled in Physics and Chemistry
	scan, err := plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	courses := []string{}
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := scan.GetString("name")
		require.NoError(t, err)
		assert.Equal(t, "Bob", name)
		course, err := scan.GetString("course")
		require.NoError(t, err)
		courses = append(courses, course)
	}

	assert.Equal(t, 2, len(courses))
	assert.Contains(t, courses, "Physics")
	assert.Contains(t, courses, "Chemistry")
}

// TestBasicQueryPlanner_WithIndex tests that the planner uses indexes when available
func TestBasicQueryPlanner_WithIndex(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a table with multiple fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")
	schema.AddStringField("department", 30)

	createTableWithData(t, "employees", schema, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Insert test data
		employees := []struct {
			id   int
			name string
			age  int
			dept string
		}{
			{1, "Alice", 25, "Engineering"},
			{2, "Bob", 30, "Marketing"},
			{3, "Charlie", 28, "Engineering"},
			{4, "Diana", 32, "Sales"},
			{5, "Eve", 27, "Engineering"},
		}

		for _, emp := range employees {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", emp.id)
			require.NoError(t, err)
			err = ts.SetString("name", emp.name)
			require.NoError(t, err)
			err = ts.SetInt("age", emp.age)
			require.NoError(t, err)
			err = ts.SetString("department", emp.dept)
			require.NoError(t, err)
		}
	})

	// Create an index on the id field
	err := md.CreateIndex("emp_id_idx", "employees", "id", tx)
	require.NoError(t, err)

	// Create an index on the department field
	err = md.CreateIndex("emp_dept_idx", "employees", "department", tx)
	require.NoError(t, err)

	planner := NewBasicQueryPlanner(md)

	// Test 1: Query with indexed field (id = 3)
	pred1 := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewConstantExpression(*query.NewIntConstant(3)),
	))

	plan1, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name", "department"}, []string{"employees"}, pred1,
	), tx)
	require.NoError(t, err)

	// Verify the plan uses index (should have lower cost than table scan)
	tablePlan, err := NewTablePlan("employees", tx, md)
	require.NoError(t, err)

	// Index plan should be more efficient
	assert.True(t, plan1.BlocksAccessed() <= tablePlan.BlocksAccessed(),
		"Index plan should be at least as efficient as table scan")

	// Verify results
	scan1, err := plan1.Open()
	require.NoError(t, err)
	defer scan1.Close()
	err = scan1.BeforeFirst()
	require.NoError(t, err)

	count := 0
	for {
		hasNext, err := scan1.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
		id, err := scan1.GetInt("id")
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		name, err := scan1.GetString("name")
		require.NoError(t, err)
		assert.Equal(t, "Charlie", name)
	}
	assert.Equal(t, 1, count)

	// Test 2: Query with indexed string field (department = "Engineering")
	pred2 := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("department"),
		*query.NewConstantExpression(*query.NewStringConstant("Engineering")),
	))

	plan2, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"name", "age"}, []string{"employees"}, pred2,
	), tx)
	require.NoError(t, err)

	// Verify results - should find 3 engineering employees
	scan2, err := plan2.Open()
	require.NoError(t, err)
	defer scan2.Close()
	err = scan2.BeforeFirst()
	require.NoError(t, err)

	engineeringCount := 0
	engineeringNames := []string{}
	for {
		hasNext, err := scan2.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		engineeringCount++
		name, err := scan2.GetString("name")
		require.NoError(t, err)
		engineeringNames = append(engineeringNames, name)
	}

	assert.Equal(t, 3, engineeringCount)
	assert.Contains(t, engineeringNames, "Alice")
	assert.Contains(t, engineeringNames, "Charlie")
	assert.Contains(t, engineeringNames, "Eve")
}

// TestBasicQueryPlanner_MultipleIndexes tests choosing the best index when multiple are available
func TestBasicQueryPlanner_MultipleIndexes(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table with multiple indexed fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("category_id")
	schema.AddStringField("name", 20)
	schema.AddStringField("status", 10)

	err := md.CreateTable("products", schema, tx)
	require.NoError(t, err)

	// Create indexes BEFORE inserting data
	err = md.CreateIndex("products_id_idx", "products", "id", tx)
	require.NoError(t, err)
	err = md.CreateIndex("products_cat_idx", "products", "category_id", tx)
	require.NoError(t, err)

	// Insert data using UpdatePlanner to ensure indexes are populated
	updatePlanner := NewBasicUpdatePlanner(md)

	for i := 1; i <= 20; i++ {
		// Create insert data
		fields := []string{"id", "category_id", "name", "status"}
		values := []interface{}{i, i % 5, "Product", "active"}
		insertData := parserdata.NewInsertData("products", fields, values)

		_, err = updatePlanner.ExecuteInsert(insertData, tx)
		require.NoError(t, err)
	}

	planner := NewBasicQueryPlanner(md)

	// Query with condition on id (highly selective - should prefer id index)
	pred1 := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewConstantExpression(*query.NewIntConstant(15)),
	))

	plan1, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name"}, []string{"products"}, pred1,
	), tx)
	require.NoError(t, err)

	// Verify it returns correct result
	scan1, err := plan1.Open()
	require.NoError(t, err)
	defer scan1.Close()
	err = scan1.BeforeFirst()
	require.NoError(t, err)
	count1, err := countScanResults(scan1)
	require.NoError(t, err)
	t.Logf("Query on id=15 found %d records", count1)
	assert.Equal(t, 1, count1)

	// Query with condition on category_id (less selective)
	pred2 := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("category_id"),
		*query.NewConstantExpression(*query.NewIntConstant(2)),
	))

	plan2, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "category_id"}, []string{"products"}, pred2,
	), tx)
	require.NoError(t, err)

	// Should return 4 products (20/5 = 4 products per category)
	scan2, err := plan2.Open()
	require.NoError(t, err)
	defer scan2.Close()
	err = scan2.BeforeFirst()
	require.NoError(t, err)
	count2, err := countScanResults(scan2)
	require.NoError(t, err)
	t.Logf("Query on category_id=2 found %d records", count2)
	assert.Equal(t, 4, count2)
}

// TestBasicQueryPlanner_IndexWithStringField tests index optimization with string fields
func TestBasicQueryPlanner_IndexWithStringField(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a table with string field that we'll index
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("status", 20)
	schema.AddStringField("category", 30)

	err := md.CreateTable("items", schema, tx)
	require.NoError(t, err)

	// Create index on string field BEFORE inserting data
	err = md.CreateIndex("items_status_idx", "items", "status", tx)
	require.NoError(t, err)

	// Insert data with index in place
	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, "items")
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)

	// Insert test data with different statuses
	statuses := []string{"active", "inactive", "pending", "active", "active", "inactive"}
	for i, status := range statuses {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i+1)
		require.NoError(t, err)
		err = ts.SetString("status", status)
		require.NoError(t, err)
		err = ts.SetString("category", "test")
		require.NoError(t, err)
	}
	ts.Close()

	planner := NewBasicQueryPlanner(md)

	// Query for items with status = "active"
	pred := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("status"),
		*query.NewConstantExpression(*query.NewStringConstant("active")),
	))

	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "status", "category"}, []string{"items"}, pred,
	), tx)
	require.NoError(t, err)

	// Verify results - should find 3 "active" items
	scan, err := plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	count := 0
	activeIDs := []int{}
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
		id, err := scan.GetInt("id")
		require.NoError(t, err)
		status, err := scan.GetString("status")
		require.NoError(t, err)
		assert.Equal(t, "active", status)
		activeIDs = append(activeIDs, id)
	}

	t.Logf("Found %d active items with IDs: %v", count, activeIDs)
	assert.Equal(t, 3, count, "Should find exactly 3 active items")

	// Verify the plan is using index (should have low cost)
	cost := plan.BlocksAccessed()
	t.Logf("Index plan cost: %d", cost)
	assert.True(t, cost <= 5, "Index plan should have low cost")
}
