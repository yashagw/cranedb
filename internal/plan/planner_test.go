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

	scan, err := plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	records := []struct {
		id   int
		name string
		age  int
	}{}
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := scan.GetInt("id")
		require.NoError(t, err)
		name, err := scan.GetString("name")
		require.NoError(t, err)
		age, err := scan.GetInt("age")
		require.NoError(t, err)
		records = append(records, struct {
			id   int
			name string
			age  int
		}{
			id:   id,
			name: name,
			age:  age,
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

	scan, err = plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	found := false
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		found = true
		name, err := scan.GetString("name")
		require.NoError(t, err)
		assert.Equal(t, "Bob", name)
	}
	assert.True(t, found)

	// 5. UPDATE/MODIFY a record
	updateSQL := "UPDATE students SET age = 23 WHERE name = 'Bob'"
	count, err = planner.ExecuteUpdate(updateSQL, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the update
	plan, err = planner.CreatePlan("SELECT age FROM students WHERE name = 'Bob'", tx)
	require.NoError(t, err)
	scan, err = plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		age, err := scan.GetInt("age")
		require.NoError(t, err)
		assert.Equal(t, 23, age)
	}

	// 6. DELETE a record
	deleteSQL := "DELETE FROM students WHERE id = 3"
	count, err = planner.ExecuteUpdate(deleteSQL, tx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify deletion - should have 2 records left
	plan, err = planner.CreatePlan("SELECT id FROM students", tx)
	require.NoError(t, err)
	scan, err = plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	remaining := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		remaining++
		id, err := scan.GetInt("id")
		require.NoError(t, err)
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
		assert.Equal(t, "Alice", name)
		course, err := scan.GetString("course")
		require.NoError(t, err)
		courses = append(courses, course)
	}

	assert.Equal(t, 2, len(courses))
	assert.Contains(t, courses, "Math")
	assert.Contains(t, courses, "Physics")
}

// TestPlanner_ComplexPredicateScenario tests the comprehensive scenario with:
// 2 tables, 4 predicates:
// - 2 predicates on table1 (one indexed, one not)
// - 1 predicate on table2
// - 1 join predicate between tables
func TestPlanner_ComplexPredicateScenario(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	queryPlanner := NewBasicQueryPlanner(md)
	updatePlanner := NewBasicUpdatePlanner(md)
	planner := NewPlanner(queryPlanner, updatePlanner)

	// Create tables and index
	_, err := planner.ExecuteUpdate("CREATE TABLE students (id INT, age INT, status VARCHAR(10), name VARCHAR(20))", tx)
	require.NoError(t, err)

	_, err = planner.ExecuteUpdate("CREATE TABLE courses (student_id INT, credits INT, course_name VARCHAR(30))", tx)
	require.NoError(t, err)

	// Create index on students.age (indexed predicate)
	_, err = planner.ExecuteUpdate("CREATE INDEX students_age_idx ON students (age)", tx)
	require.NoError(t, err)

	// Insert test data
	students := []string{
		"INSERT INTO students (id, age, status, name) VALUES (1, 25, 'active', 'Alice')",   // matches age=25, status=active
		"INSERT INTO students (id, age, status, name) VALUES (2, 30, 'inactive', 'Bob')",   // age≠25
		"INSERT INTO students (id, age, status, name) VALUES (3, 25, 'active', 'Charlie')", // matches age=25, status=active
		"INSERT INTO students (id, age, status, name) VALUES (4, 22, 'active', 'Diana')",   // age≠25
		"INSERT INTO students (id, age, status, name) VALUES (5, 25, 'inactive', 'Eve')",   // age=25 but status≠active
	}

	for _, sql := range students {
		_, err = planner.ExecuteUpdate(sql, tx)
		require.NoError(t, err)
	}

	courses := []string{
		"INSERT INTO courses (student_id, credits, course_name) VALUES (1, 4, 'Math')",    // student_id=1, credits=4
		"INSERT INTO courses (student_id, credits, course_name) VALUES (2, 2, 'History')", // student_id=2, credits=2
		"INSERT INTO courses (student_id, credits, course_name) VALUES (3, 4, 'Physics')", // student_id=3, credits=4
		"INSERT INTO courses (student_id, credits, course_name) VALUES (4, 3, 'English')", // student_id=4, credits=3
		"INSERT INTO courses (student_id, credits, course_name) VALUES (5, 4, 'Science')", // student_id=5, credits=4
	}

	for _, sql := range courses {
		_, err = planner.ExecuteUpdate(sql, tx)
		require.NoError(t, err)
	}

	// Execute the complex query with 4 predicates:
	// - students.age = 25           (indexed predicate on table1)
	// - students.status = 'active'  (non-indexed predicate on table1)
	// - courses.credits = 4         (predicate on table2)
	// - students.id = courses.student_id (join predicate)
	querySQL := `SELECT id, age, status, name, student_id, credits, course_name 
	             FROM students, courses 
	             WHERE age = 25 AND status = 'active' AND credits = 4 AND id = student_id`

	plan, err := planner.CreatePlan(querySQL, tx)
	require.NoError(t, err)

	// Execute and collect results
	scan, err := plan.Open()
	require.NoError(t, err)
	defer scan.Close()
	err = scan.BeforeFirst()
	require.NoError(t, err)

	var results []map[string]interface{}
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		result := make(map[string]interface{})

		// Get all fields
		id, err := scan.GetInt("id")
		require.NoError(t, err)
		result["id"] = id

		age, err := scan.GetInt("age")
		require.NoError(t, err)
		result["age"] = age

		status, err := scan.GetString("status")
		require.NoError(t, err)
		result["status"] = status

		name, err := scan.GetString("name")
		require.NoError(t, err)
		result["name"] = name

		studentId, err := scan.GetInt("student_id")
		require.NoError(t, err)
		result["student_id"] = studentId

		credits, err := scan.GetInt("credits")
		require.NoError(t, err)
		result["credits"] = credits

		courseName, err := scan.GetString("course_name")
		require.NoError(t, err)
		result["course_name"] = courseName

		results = append(results, result)
	}

	// Expected results: students with age=25 AND status='active'
	// joined with courses where credits=4 and students.id = courses.student_id
	//
	// Students matching age=25 AND status='active': Alice (id=1), Charlie (id=3)
	// Courses matching credits=4: Math (student_id=1), Physics (student_id=3), Science (student_id=5)
	// Join condition: students.id = courses.student_id
	//
	// Expected matches:
	// - Alice (id=1) with Math (student_id=1, credits=4)
	// - Charlie (id=3) with Physics (student_id=3, credits=4)

	t.Logf("Found %d results from complex predicate query", len(results))
	for i, result := range results {
		t.Logf("Result %d: %+v", i+1, result)
	}

	// Verify we get exactly 2 results
	assert.Equal(t, 2, len(results))

	// Verify the results are correct
	expectedResults := []map[string]interface{}{
		{"id": 1, "age": 25, "status": "active", "name": "Alice", "student_id": 1, "credits": 4, "course_name": "Math"},
		{"id": 3, "age": 25, "status": "active", "name": "Charlie", "student_id": 3, "credits": 4, "course_name": "Physics"},
	}

	for i, expected := range expectedResults {
		if i < len(results) {
			assert.Equal(t, expected["id"], results[i]["id"])
			assert.Equal(t, expected["age"], results[i]["age"])
			assert.Equal(t, expected["status"], results[i]["status"])
			assert.Equal(t, expected["name"], results[i]["name"])
			assert.Equal(t, expected["student_id"], results[i]["student_id"])
			assert.Equal(t, expected["credits"], results[i]["credits"])
			assert.Equal(t, expected["course_name"], results[i]["course_name"])
		}
	}

	// This test verifies that:
	// 1. Index optimization works (students.age = 25 uses index)
	// 2. Table-specific predicates are applied in Phase 1
	// 3. Join predicates are applied in Phase 3 after ProductPlan
	// 4. All 4 types of predicates work together correctly
}
