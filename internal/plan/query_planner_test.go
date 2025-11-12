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
	"github.com/yashagw/cranedb/internal/transaction"
)

// Helper to create and populate a table with test data
func createTableWithData(t *testing.T, tableName string, schema *record.Schema, md *metadata.Manager, tx *transaction.Transaction, dataFn func(*scan.TableScan)) {
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	if dataFn != nil {
		layout := record.NewLayoutFromSchema(schema)
		ts, err := scan.NewTableScan(tx, layout, tableName)
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

	createTableWithData(t, "students", schema, md, tx, func(ts *scan.TableScan) {
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

	createTableWithData(t, "products", schema, md, tx, func(ts *scan.TableScan) {
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

	createTableWithData(t, "students", s1, md, tx, func(ts *scan.TableScan) {
		err := ts.BeforeFirst()
		require.NoError(t, err)
		for i := 1; i <= 2; i++ {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("sid", i)
			require.NoError(t, err)
		}
	})
	createTableWithData(t, "courses", s2, md, tx, func(ts *scan.TableScan) {
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
	ts1, err := scan.NewTableScan(tx, record.NewLayoutFromSchema(s1), "students")
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
	ts2, err := scan.NewTableScan(tx, record.NewLayoutFromSchema(s2), "enrollments")
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
