package plan

import (
	"fmt"
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
		query.OpEQ,
	))

	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name"}, []string{"students"}, pred,
	), tx, nil)
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
	), tx, nil)
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
	), tx, nil)
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
		query.OpEQ,
	)
	term2 := query.NewTerm(
		*query.NewFieldNameExpression("name"),
		*query.NewConstantExpression(*query.NewStringConstant("Bob")),
		query.OpEQ,
	)
	pred := query.And(
		query.NewPredicate(*term1),
		query.NewPredicate(*term2),
	)

	plan, _ := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"name", "course"}, []string{"students", "enrollments"}, pred,
	), tx, nil)

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

	err := md.CreateTable("employees", schema, tx)
	require.NoError(t, err)

	// Create indexes BEFORE inserting data
	err = md.CreateIndex("emp_id_idx", "employees", "id", tx)
	require.NoError(t, err)

	err = md.CreateIndex("emp_dept_idx", "employees", "department", tx)
	require.NoError(t, err)

	// Insert data using UpdatePlanner so indexes are populated
	updatePlanner := NewBasicUpdatePlanner(md)
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
		insertData := parserdata.NewInsertData("employees",
			[]string{"id", "name", "age", "department"},
			[]interface{}{emp.id, emp.name, emp.age, emp.dept})
		_, err = updatePlanner.ExecuteInsert(insertData, tx)
		require.NoError(t, err)
	}

	planner := NewBasicQueryPlanner(md)

	// Test 1: Query with indexed field (id = 3)
	pred1 := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("id"),
		*query.NewConstantExpression(*query.NewIntConstant(3)),
		query.OpEQ,
	))

	plan1, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name", "department"}, []string{"employees"}, pred1,
	), tx, nil)
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
		query.OpEQ,
	))

	plan2, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"name", "age"}, []string{"employees"}, pred2,
	), tx, nil)
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
		query.OpEQ,
	))

	plan1, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name"}, []string{"products"}, pred1,
	), tx, nil)
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
		query.OpEQ,
	))

	plan2, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "category_id"}, []string{"products"}, pred2,
	), tx, nil)
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

	// Insert data using UpdatePlanner so indexes are populated
	updatePlanner := NewBasicUpdatePlanner(md)
	statuses := []string{"active", "inactive", "pending", "active", "active", "inactive"}
	for i, status := range statuses {
		insertData := parserdata.NewInsertData("items",
			[]string{"id", "status", "category"},
			[]interface{}{i + 1, status, "test"})
		_, err = updatePlanner.ExecuteInsert(insertData, tx)
		require.NoError(t, err)
	}

	planner := NewBasicQueryPlanner(md)

	// Query for items with status = "active"
	pred := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("status"),
		*query.NewConstantExpression(*query.NewStringConstant("active")),
		query.OpEQ,
	))

	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "status", "category"}, []string{"items"}, pred,
	), tx, nil)
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

// TestExplainPlan tests EXPLAIN functionality with various query types
func TestExplainPlan(t *testing.T) {
	t.Run("BasicQuery", func(t *testing.T) {
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
		updatePlanner := NewBasicUpdatePlanner(md)
		fullPlanner := NewPlanner(planner, updatePlanner)

		explainSQL := "EXPLAIN SELECT id, name FROM products"
		planTree, err := fullPlanner.ExplainPlan(explainSQL, tx, nil)
		require.NoError(t, err)

		expected := `ProjectPlan(fields: [id, name])
    └─ TablePlan(products)`
		assert.Equal(t, expected, planTree)
	})

	t.Run("WithJoin", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		s1 := record.NewSchema()
		s1.AddIntField("id")
		s1.AddStringField("name", 20)
		s1.AddIntField("student_dept_id")
		createTableWithData(t, "students", s1, md, tx, func(ts *table.TableScan) {
			err := ts.BeforeFirst()
			require.NoError(t, err)
			for i := 1; i <= 3; i++ {
				err = ts.Insert()
				require.NoError(t, err)
				err = ts.SetInt("id", i)
				require.NoError(t, err)
				err = ts.SetString("name", "Student")
				require.NoError(t, err)
				err = ts.SetInt("student_dept_id", i%2+1)
				require.NoError(t, err)
			}
		})

		s2 := record.NewSchema()
		s2.AddIntField("dept_id")
		s2.AddStringField("dept_name", 20)
		createTableWithData(t, "departments", s2, md, tx, func(ts *table.TableScan) {
			err := ts.BeforeFirst()
			require.NoError(t, err)
			for i := 1; i <= 2; i++ {
				err = ts.Insert()
				require.NoError(t, err)
				err = ts.SetInt("dept_id", i)
				require.NoError(t, err)
				err = ts.SetString("dept_name", "Dept")
				require.NoError(t, err)
			}
		})

		err := md.CreateIndex("dept_id_idx", "departments", "dept_id", tx)
		require.NoError(t, err)

		planner := NewBasicQueryPlanner(md)
		updatePlanner := NewBasicUpdatePlanner(md)
		fullPlanner := NewPlanner(planner, updatePlanner)

		explainSQL := "EXPLAIN SELECT name, dept_name FROM students, departments WHERE student_dept_id = dept_id"
		planTree, err := fullPlanner.ExplainPlan(explainSQL, tx, nil)
		require.NoError(t, err)

		expected := `ProjectPlan(fields: [name, dept_name])
    └─ SelectPlan(predicate: student_dept_id = dept_id)
    └─     └─ ProductPlan
    └─     └─     ├─ TablePlan(departments)
    └─     └─     └─ TablePlan(students)`
		assert.Equal(t, expected, planTree)
	})

	t.Run("IndexSelectUsed", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		schema := record.NewSchema()
		schema.AddIntField("id")
		schema.AddStringField("name", 20)
		schema.AddIntField("age")

		err := md.CreateTable("employees", schema, tx)
		require.NoError(t, err)

		err = md.CreateIndex("emp_id_idx", "employees", "id", tx)
		require.NoError(t, err)

		updatePlanner := NewBasicUpdatePlanner(md)
		// Insert enough records to make index beneficial
		for i := 1; i <= 50; i++ {
			fields := []string{"id", "name", "age"}
			values := []interface{}{i, fmt.Sprintf("Employee%d", i), 20 + i}
			insertData := parserdata.NewInsertData("employees", fields, values)
			_, err = updatePlanner.ExecuteInsert(insertData, tx)
			require.NoError(t, err)
		}

		// Invalidate stats to ensure index is used
		// TODO: figure out why we need to invalidate stats to ensure index is used
		md.InvalidateStats("employees")

		planner := NewBasicQueryPlanner(md)
		fullPlanner := NewPlanner(planner, updatePlanner)

		explainSQL := "EXPLAIN SELECT id, name FROM employees WHERE id = 25"
		planTree, err := fullPlanner.ExplainPlan(explainSQL, tx, nil)
		require.NoError(t, err)

		expected := `ProjectPlan(fields: [id, name])
    └─ IndexSelectPlan(id=25)
    └─     └─ TablePlan(employees)`

		assert.Equal(t, expected, planTree)
	})

	t.Run("ComplexQuery", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		s1 := record.NewSchema()
		s1.AddIntField("id")
		s1.AddStringField("name", 20)
		s1.AddStringField("category", 20)
		s1.AddIntField("student_dept_id")
		err := md.CreateTable("students", s1, tx)
		require.NoError(t, err)

		s2 := record.NewSchema()
		s2.AddIntField("dept_id")
		s2.AddStringField("dept_name", 20)
		err = md.CreateTable("departments", s2, tx)
		require.NoError(t, err)

		// Create indexes BEFORE inserting data
		err = md.CreateIndex("students_id_idx", "students", "id", tx)
		require.NoError(t, err)
		err = md.CreateIndex("students_category_idx", "students", "category", tx)
		require.NoError(t, err)
		err = md.CreateIndex("dept_id_idx", "departments", "dept_id", tx)
		require.NoError(t, err)

		updatePlanner := NewBasicUpdatePlanner(md)
		for i := 1; i <= 60; i++ {
			// Make most students have Category1 to get many records after index filter
			category := "Category1"
			if i%10 == 0 {
				category = fmt.Sprintf("Category%d", ((i-1)%5)+1)
			}
			insertData := parserdata.NewInsertData("students",
				[]string{"id", "name", "student_dept_id", "category"},
				[]interface{}{i, "Student", i%10 + 1, category})
			_, err = updatePlanner.ExecuteInsert(insertData, tx)
			require.NoError(t, err)
		}

		for i := 1; i <= 25; i++ {
			insertData := parserdata.NewInsertData("departments",
				[]string{"dept_id", "dept_name"},
				[]interface{}{i, fmt.Sprintf("Dept%d", i)})
			_, err = updatePlanner.ExecuteInsert(insertData, tx)
			require.NoError(t, err)
		}

		// Invalidate stats to ensure index is used
		// TODO: figure out why we need to invalidate stats to ensure index is used
		md.InvalidateStats("students")
		md.InvalidateStats("departments")

		planner := NewBasicQueryPlanner(md)
		fullPlanner := NewPlanner(planner, updatePlanner)

		explainSQL := "EXPLAIN SELECT name, dept_name FROM students, departments WHERE student_dept_id = dept_id and category = 'Category1' and dept_name = 'Dept1'"
		planTree, err := fullPlanner.ExplainPlan(explainSQL, tx, nil)
		require.NoError(t, err)

		expected := `ProjectPlan(fields: [name, dept_name])
    └─ SelectPlan(predicate: student_dept_id = dept_id and category = Category1 and dept_name = Dept1)
    └─     └─ ProductPlan
    └─     └─     ├─ SelectPlan(predicate: dept_name = Dept1)
    └─     └─     ├─ │   └─ TablePlan(departments)
    └─     └─     └─ IndexSelectPlan(category='Category1')
    └─     └─     └─     └─ TablePlan(students)`
		assert.Equal(t, expected, planTree)
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		planner := NewBasicQueryPlanner(md)
		updatePlanner := NewBasicUpdatePlanner(md)
		fullPlanner := NewPlanner(planner, updatePlanner)

		explainSQL := "EXPLAIN SELECT * FROM nonexistent"
		_, err := fullPlanner.ExplainPlan(explainSQL, tx, nil)
		require.Error(t, err, "Should return error for invalid query")
	})

	t.Run("NotExplainStatement", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		planner := NewBasicQueryPlanner(md)
		updatePlanner := NewBasicUpdatePlanner(md)
		fullPlanner := NewPlanner(planner, updatePlanner)

		selectSQL := "SELECT * FROM products"
		_, err := fullPlanner.ExplainPlan(selectSQL, tx, nil)
		require.Error(t, err, "Should return error for non-EXPLAIN statement")
		assert.True(t, err.Error() == "not an EXPLAIN statement" || err.Error() == "bad syntax",
			"Error should indicate it's not an EXPLAIN statement or bad syntax, got: %s", err.Error())
	})
}

func TestBasicQueryPlanner_WithWhereAndOrderBy(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	createTableWithData(t, "students", schema, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		// Insert data in unsorted order
		testData := []struct {
			id   int
			name string
			age  int
		}{
			{3, "Charlie", 25},
			{1, "Alice", 20},
			{4, "David", 30},
			{2, "Bob", 22},
			{5, "Eve", 28},
		}
		for _, data := range testData {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", data.id)
			require.NoError(t, err)
			err = ts.SetString("name", data.name)
			require.NoError(t, err)
			err = ts.SetInt("age", data.age)
			require.NoError(t, err)
		}
	})

	planner := NewBasicQueryPlanner(md)
	pred := query.NewPredicate(*query.NewTerm(
		*query.NewFieldNameExpression("age"),
		*query.NewConstantExpression(*query.NewIntConstant(22)),
		query.OpEQ,
	))

	// Test WHERE age = 22 ORDER BY id
	plan, err := planner.CreatePlan(parserdata.NewQueryDataWithSort(
		[]string{"id", "name", "age"}, []string{"students"}, pred, []string{"id"},
	), tx, nil)
	require.NoError(t, err)

	queryScan, err := plan.Open()
	require.NoError(t, err)
	defer queryScan.Close()

	err = queryScan.BeforeFirst()
	require.NoError(t, err)

	lastId := 0
	count := 0
	for {
		hasNext, err := queryScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := queryScan.GetInt("id")
		require.NoError(t, err)
		age, err := queryScan.GetInt("age")
		require.NoError(t, err)
		assert.Equal(t, 22, age, "Should only have records with age = 22")
		assert.True(t, id > lastId, "Records should be sorted by id")
		lastId = id
		count++
	}
	assert.True(t, count > 0, "Should have some filtered records")
}

func TestBasicQueryPlanner_ComparisonOperators(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a table with integer, string, and bool fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("age")
	schema.AddStringField("name", 20)
	schema.AddIntField("score")
	schema.AddBoolField("active")

	createTableWithData(t, "students", schema, md, tx, func(ts *table.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		testData := []struct {
			id     int
			name   string
			age    int
			score  int
			active bool
		}{
			{1, "Alice", 20, 85, true},
			{2, "Bob", 25, 90, false},
			{3, "Charlie", 30, 75, true},
			{4, "Diana", 35, 95, true},
			{5, "Eve", 40, 80, false},
			{6, "Frank", 25, 88, true},
			{7, "Grace", 30, 92, false},
		}
		for _, data := range testData {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", data.id)
			require.NoError(t, err)
			err = ts.SetString("name", data.name)
			require.NoError(t, err)
			err = ts.SetInt("age", data.age)
			require.NoError(t, err)
			err = ts.SetInt("score", data.score)
			require.NoError(t, err)
			err = ts.SetBool("active", data.active)
			require.NoError(t, err)
		}
	})

	planner := NewBasicQueryPlanner(md)

	t.Run("GreaterThan", func(t *testing.T) {
		// Test age > 25
		pred := query.NewPredicate(*query.NewTerm(
			*query.NewFieldNameExpression("age"),
			*query.NewConstantExpression(*query.NewIntConstant(25)),
			query.OpGT,
		))

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name", "age"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		ages := []int{}
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			age, err := scan.GetInt("age")
			require.NoError(t, err)
			assert.Greater(t, age, 25, "Age should be greater than 25")
			ages = append(ages, age)
		}
		// Should find: 30, 35, 40, 30 = 4 records
		assert.Equal(t, 4, count)
		assert.Contains(t, ages, 30)
		assert.Contains(t, ages, 35)
		assert.Contains(t, ages, 40)
	})

	t.Run("LessThan", func(t *testing.T) {
		// Test age < 30
		pred := query.NewPredicate(*query.NewTerm(
			*query.NewFieldNameExpression("age"),
			*query.NewConstantExpression(*query.NewIntConstant(30)),
			query.OpLT,
		))

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name", "age"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			age, err := scan.GetInt("age")
			require.NoError(t, err)
			assert.Less(t, age, 30, "Age should be less than 30")
		}
		// Should find: 20, 25, 25 = 3 records
		assert.Equal(t, 3, count)
	})

	t.Run("GreaterThanOrEqual", func(t *testing.T) {
		// Test score >= 90
		pred := query.NewPredicate(*query.NewTerm(
			*query.NewFieldNameExpression("score"),
			*query.NewConstantExpression(*query.NewIntConstant(90)),
			query.OpGE,
		))

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name", "score"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		scores := []int{}
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			score, err := scan.GetInt("score")
			require.NoError(t, err)
			assert.GreaterOrEqual(t, score, 90, "Score should be >= 90")
			scores = append(scores, score)
		}
		// Should find: 90, 95, 92 = 3 records
		assert.Equal(t, 3, count)
		assert.Contains(t, scores, 90)
		assert.Contains(t, scores, 95)
		assert.Contains(t, scores, 92)
	})

	t.Run("LessThanOrEqual", func(t *testing.T) {
		// Test score <= 85
		pred := query.NewPredicate(*query.NewTerm(
			*query.NewFieldNameExpression("score"),
			*query.NewConstantExpression(*query.NewIntConstant(85)),
			query.OpLE,
		))

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name", "score"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			score, err := scan.GetInt("score")
			require.NoError(t, err)
			assert.LessOrEqual(t, score, 85, "Score should be <= 85")
		}
		// Should find: 85, 75, 80 = 3 records
		assert.Equal(t, 3, count)
	})

	t.Run("NotEqual", func(t *testing.T) {
		// Test age != 25
		pred := query.NewPredicate(*query.NewTerm(
			*query.NewFieldNameExpression("age"),
			*query.NewConstantExpression(*query.NewIntConstant(25)),
			query.OpNE,
		))

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name", "age"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			age, err := scan.GetInt("age")
			require.NoError(t, err)
			assert.NotEqual(t, 25, age, "Age should not be 25")
		}
		// Should find all except age=25: 7 total - 2 with age=25 = 5 records
		assert.Equal(t, 5, count)
	})

	t.Run("MultipleComparisonOperators", func(t *testing.T) {
		// Test age > 25 AND score < 90
		term1 := query.NewTerm(
			*query.NewFieldNameExpression("age"),
			*query.NewConstantExpression(*query.NewIntConstant(25)),
			query.OpGT,
		)
		term2 := query.NewTerm(
			*query.NewFieldNameExpression("score"),
			*query.NewConstantExpression(*query.NewIntConstant(90)),
			query.OpLT,
		)
		pred := query.And(
			query.NewPredicate(*term1),
			query.NewPredicate(*term2),
		)

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name", "age", "score"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		names := []string{}
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			age, err := scan.GetInt("age")
			require.NoError(t, err)
			score, err := scan.GetInt("score")
			require.NoError(t, err)
			name, err := scan.GetString("name")
			require.NoError(t, err)
			assert.Greater(t, age, 25, "Age should be > 25")
			assert.Less(t, score, 90, "Score should be < 90")
			names = append(names, name)
		}
		// Should find: Charlie (30, 75) and Eve (40, 80) = 2 records
		assert.Equal(t, 2, count)
		assert.Contains(t, names, "Charlie")
		assert.Contains(t, names, "Eve")
	})

	t.Run("ComparisonWithString", func(t *testing.T) {
		// Test name > 'C' (string comparison)
		pred := query.NewPredicate(*query.NewTerm(
			*query.NewFieldNameExpression("name"),
			*query.NewConstantExpression(*query.NewStringConstant("C")),
			query.OpGT,
		))

		plan, err := planner.CreatePlan(parserdata.NewQueryData(
			[]string{"id", "name"}, []string{"students"}, pred,
		), tx, nil)
		require.NoError(t, err)

		scan, err := plan.Open()
		require.NoError(t, err)
		defer scan.Close()
		err = scan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		names := []string{}
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
			name, err := scan.GetString("name")
			require.NoError(t, err)
			assert.Greater(t, name, "C", "Name should be > 'C'")
			names = append(names, name)
		}
		// Should find names after 'C': Charlie, Diana, Eve, Frank, Grace
		assert.GreaterOrEqual(t, count, 5)
		assert.Contains(t, names, "Charlie")
		assert.Contains(t, names, "Diana")
	})

	t.Run("ComparisonWithBool", func(t *testing.T) {
		// Note: In this system, false < true (as per Constant.CompareTo implementation)
		// So comparisons work as: false < true

		t.Run("BoolEquality", func(t *testing.T) {
			// Test active = true
			pred := query.NewPredicate(*query.NewTerm(
				*query.NewFieldNameExpression("active"),
				*query.NewConstantExpression(*query.NewBoolConstant(true)),
				query.OpEQ,
			))

			plan, err := planner.CreatePlan(parserdata.NewQueryData(
				[]string{"id", "name", "active"}, []string{"students"}, pred,
			), tx, nil)
			require.NoError(t, err)

			scan, err := plan.Open()
			require.NoError(t, err)
			defer scan.Close()
			err = scan.BeforeFirst()
			require.NoError(t, err)

			count := 0
			for {
				hasNext, err := scan.Next()
				require.NoError(t, err)
				if !hasNext {
					break
				}
				count++
				active, err := scan.GetBool("active")
				require.NoError(t, err)
				assert.True(t, active, "Active should be true")
			}
			// Should find: Alice, Charlie, Diana, Frank = 4 records
			assert.Equal(t, 4, count)
		})

		t.Run("BoolNotEqual", func(t *testing.T) {
			// Test active != true (should find false values)
			pred := query.NewPredicate(*query.NewTerm(
				*query.NewFieldNameExpression("active"),
				*query.NewConstantExpression(*query.NewBoolConstant(true)),
				query.OpNE,
			))

			plan, err := planner.CreatePlan(parserdata.NewQueryData(
				[]string{"id", "name", "active"}, []string{"students"}, pred,
			), tx, nil)
			require.NoError(t, err)

			scan, err := plan.Open()
			require.NoError(t, err)
			defer scan.Close()
			err = scan.BeforeFirst()
			require.NoError(t, err)

			count := 0
			for {
				hasNext, err := scan.Next()
				require.NoError(t, err)
				if !hasNext {
					break
				}
				count++
				active, err := scan.GetBool("active")
				require.NoError(t, err)
				assert.False(t, active, "Active should be false")
			}
			// Should find: Bob, Eve, Grace = 3 records
			assert.Equal(t, 3, count)
		})

		t.Run("BoolGreaterThan", func(t *testing.T) {
			// Range operators on bool fields should fail at Open() time
			pred := query.NewPredicate(*query.NewTerm(
				*query.NewFieldNameExpression("active"),
				*query.NewConstantExpression(*query.NewBoolConstant(false)),
				query.OpGT,
			))

			plan, err := planner.CreatePlan(parserdata.NewQueryData(
				[]string{"id", "name", "active"}, []string{"students"}, pred,
			), tx, nil)
			require.NoError(t, err)

			// Open() should return an error before any buffers are pinned
			_, err = plan.Open()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot be used with boolean field")
		})

		t.Run("BoolRangeOperatorsShouldFail", func(t *testing.T) {
			// Test all range operators fail at Open() time
			operators := []query.Operator{query.OpGT, query.OpLT, query.OpGE, query.OpLE}
			for _, op := range operators {
				pred := query.NewPredicate(*query.NewTerm(
					*query.NewFieldNameExpression("active"),
					*query.NewConstantExpression(*query.NewBoolConstant(false)),
					op,
				))

				plan, err := planner.CreatePlan(parserdata.NewQueryData(
					[]string{"id", "name", "active"}, []string{"students"}, pred,
				), tx, nil)
				require.NoError(t, err, "Plan creation should succeed")

				// Open() should return an error before any buffers are pinned
				_, err = plan.Open()
				require.Error(t, err, "Range operator %s on bool field should return error", op.String())
				assert.Contains(t, err.Error(), "cannot be used with boolean field")
			}
		})
	})
}
