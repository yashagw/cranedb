package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/record"
)

func TestPlanner_E2E(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	queryPlanner := NewBasicQueryPlanner(md)
	updatePlanner := NewBasicUpdatePlanner(md)
	planner := NewPlanner(queryPlanner, updatePlanner)

	// 1. CREATE TABLE
	createSQL := "CREATE TABLE students (id INT, name VARCHAR(20), age INT)"
	count, err := planner.ExecuteUpdate(createSQL, tx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// 2. INSERT records
	insertSQL1 := "INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)"
	count, err = planner.ExecuteUpdate(insertSQL1, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	insertSQL2 := "INSERT INTO students (id, name, age) VALUES (2, 'Bob', 22)"
	count, err = planner.ExecuteUpdate(insertSQL2, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	insertSQL3 := "INSERT INTO students (id, name, age) VALUES (3, 'Charlie', 21)"
	count, err = planner.ExecuteUpdate(insertSQL3, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// 3. QUERY all records
	querySQL := "SELECT id, name, age FROM students"
	plan, err := planner.CreatePlan(querySQL, tx)
	require.NoError(t, err)

	scan := plan.Open()
	defer scan.Close()

	records := []struct {
		id   int
		name string
		age  int
	}{}
	for scan.Next() {
		records = append(records, struct {
			id   int
			name string
			age  int
		}{
			id:   scan.GetInt("id"),
			name: scan.GetString("name"),
			age:  scan.GetInt("age"),
		})
	}
	assert.Equal(t, 3, len(records))
	assert.ElementsMatch(t, records, []struct {
		id   int
		name string
		age  int
	}{
		{1, "Alice", 20},
		{2, "Bob", 22},
		{3, "Charlie", 21},
	})

	// 4. QUERY with WHERE clause
	querySQL2 := "SELECT name FROM students WHERE id = 2"
	plan, err = planner.CreatePlan(querySQL2, tx)
	require.NoError(t, err)

	scan = plan.Open()
	defer scan.Close()

	found := false
	for scan.Next() {
		found = true
		assert.Equal(t, "Bob", scan.GetString("name"))
	}
	assert.True(t, found)

	// 5. UPDATE/MODIFY a record
	updateSQL := "UPDATE students SET age = 23 WHERE name = 'Bob'"
	count, err = planner.ExecuteUpdate(updateSQL, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the update
	plan, _ = planner.CreatePlan("SELECT age FROM students WHERE name = 'Bob'", tx)
	scan = plan.Open()
	defer scan.Close()

	for scan.Next() {
		assert.Equal(t, 23, scan.GetInt("age"))
	}

	// 6. DELETE a record
	deleteSQL := "DELETE FROM students WHERE id = 3"
	count, err = planner.ExecuteUpdate(deleteSQL, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify deletion - should have 2 records left
	plan, _ = planner.CreatePlan("SELECT id FROM students", tx)
	scan = plan.Open()
	defer scan.Close()

	remaining := 0
	for scan.Next() {
		remaining++
		id := scan.GetInt("id")
		assert.True(t, id == 1 || id == 2, "Only Alice and Bob should remain")
	}
	assert.Equal(t, 2, remaining)
}

func TestPlanner_CreateView(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	queryPlanner := NewBasicQueryPlanner(md)
	updatePlanner := NewBasicUpdatePlanner(md)
	planner := NewPlanner(queryPlanner, updatePlanner)

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("salary")
	md.CreateTable("employees", schema, tx)

	planner.ExecuteUpdate("INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 50000)", tx)
	planner.ExecuteUpdate("INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 60000)", tx)
	planner.ExecuteUpdate("INSERT INTO employees (id, name, salary) VALUES (3, 'Charlie', 70000)", tx)

	// Create view
	createViewSQL := "CREATE VIEW high_earners AS SELECT id, name FROM employees WHERE salary = 70000"
	count, err := planner.ExecuteUpdate(createViewSQL, tx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify view was created
	viewDef, err := md.GetViewDef("high_earners", tx)
	require.NoError(t, err)
	assert.Equal(t, "SELECT id, name FROM employees WHERE salary = 70000", viewDef)
}

func TestPlanner_ComplexJoinQuery(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	queryPlanner := NewBasicQueryPlanner(md)
	updatePlanner := NewBasicUpdatePlanner(md)
	planner := NewPlanner(queryPlanner, updatePlanner)

	planner.ExecuteUpdate("CREATE TABLE students (id INT, name VARCHAR(20))", tx)
	planner.ExecuteUpdate("CREATE TABLE courses (student_id INT, course VARCHAR(20))", tx)

	planner.ExecuteUpdate("INSERT INTO students (id, name) VALUES (1, 'Alice')", tx)
	planner.ExecuteUpdate("INSERT INTO students (id, name) VALUES (2, 'Bob')", tx)
	planner.ExecuteUpdate("INSERT INTO courses (student_id, course) VALUES (1, 'Math')", tx)
	planner.ExecuteUpdate("INSERT INTO courses (student_id, course) VALUES (1, 'Physics')", tx)
	planner.ExecuteUpdate("INSERT INTO courses (student_id, course) VALUES (2, 'Chemistry')", tx)

	// Query with join
	querySQL := "SELECT name, course FROM students, courses WHERE id = student_id AND name = 'Alice'"
	plan, err := planner.CreatePlan(querySQL, tx)
	require.NoError(t, err)

	scan := plan.Open()
	defer scan.Close()
	scan.BeforeFirst()

	courses := []string{}
	for scan.Next() {
		assert.Equal(t, "Alice", scan.GetString("name"))
		courses = append(courses, scan.GetString("course"))
	}

	assert.Equal(t, 2, len(courses))
	assert.Contains(t, courses, "Math")
	assert.Contains(t, courses, "Physics")
}
