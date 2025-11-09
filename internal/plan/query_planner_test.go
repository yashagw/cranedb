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
func createTableWithData(t *testing.T, tableName string, schema *record.Schema, md *metadata.Manager, tx *transaction.Transaction, dataFn func(*record.TableScan)) {
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	if dataFn != nil {
		layout := record.NewLayoutFromSchema(schema)
		ts := record.NewTableScan(tx, layout, tableName)
		dataFn(ts)
		ts.Close()
	}
}

// Helper to count scan results
func countScanResults(s scan.Scan) int {
	count := 0
	for s.Next() {
		count++
	}
	return count
}

func TestBasicQueryPlanner_SingleTableWithPredicate(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")

	createTableWithData(t, "students", schema, md, tx, func(ts *record.TableScan) {
		for i := 1; i <= 5; i++ {
			ts.Insert()
			ts.SetInt("id", i)
			ts.SetString("name", "Student")
			ts.SetInt("age", 20+i)
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
	scan := plan.Open()
	defer scan.Close()
	count := 0
	for scan.Next() {
		count++
		assert.Equal(t, 2, scan.GetInt("id"))
	}
	assert.Equal(t, 1, count)
}

func TestBasicQueryPlanner_NoPredicate(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	createTableWithData(t, "products", schema, md, tx, func(ts *record.TableScan) {
		for i := 1; i <= 3; i++ {
			ts.Insert()
			ts.SetInt("id", i)
			ts.SetString("name", "Product")
		}
	})

	planner := NewBasicQueryPlanner(md)
	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"id", "name"}, []string{"products"}, nil,
	), tx)
	require.NoError(t, err)

	scan := plan.Open()
	defer scan.Close()
	assert.Equal(t, 3, countScanResults(scan))
}

func TestBasicQueryPlanner_CartesianProduct(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	s1 := record.NewSchema()
	s1.AddIntField("sid")
	s2 := record.NewSchema()
	s2.AddIntField("cid")

	createTableWithData(t, "students", s1, md, tx, func(ts *record.TableScan) {
		for i := 1; i <= 2; i++ {
			ts.Insert()
			ts.SetInt("sid", i)
		}
	})
	createTableWithData(t, "courses", s2, md, tx, func(ts *record.TableScan) {
		for i := 1; i <= 2; i++ {
			ts.Insert()
			ts.SetInt("cid", i)
		}
	})

	planner := NewBasicQueryPlanner(md)
	plan, err := planner.CreatePlan(parserdata.NewQueryData(
		[]string{"sid", "cid"}, []string{"students", "courses"}, nil,
	), tx)
	require.NoError(t, err)

	scan := plan.Open()
	defer scan.Close()
	scan.BeforeFirst()
	assert.Equal(t, 4, countScanResults(scan)) // 2 * 2 = 4
}

func TestBasicQueryPlanner_JoinWithPredicate(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	// Create students table: id, name
	s1 := record.NewSchema()
	s1.AddIntField("id")
	s1.AddStringField("name", 20)
	md.CreateTable("students", s1, tx)
	ts1 := record.NewTableScan(tx, record.NewLayoutFromSchema(s1), "students")

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
	ts2 := record.NewTableScan(tx, record.NewLayoutFromSchema(s2), "enrollments")

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
	scan := plan.Open()
	defer scan.Close()
	scan.BeforeFirst()

	courses := []string{}
	for scan.Next() {
		assert.Equal(t, "Bob", scan.GetString("name"))
		courses = append(courses, scan.GetString("course"))
	}

	assert.Equal(t, 2, len(courses))
	assert.Contains(t, courses, "Physics")
	assert.Contains(t, courses, "Chemistry")
}
