package query

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// setupProjectScanTest creates a test table with multiple fields
func setupProjectScanTest(t *testing.T, testDir string) (*transaction.Transaction, *record.TableScan) {
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	// Create table with multiple fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")
	schema.AddStringField("email", 30)
	schema.AddIntField("salary")

	layout := record.NewLayoutFromSchema(schema)
	ts := record.NewTableScan(tx, layout, "Employees")

	// Insert test data
	employees := []struct {
		id     int
		name   string
		age    int
		email  string
		salary int
	}{
		{1, "Alice", 30, "alice@example.com", 50000},
		{2, "Bob", 35, "bob@example.com", 60000},
		{3, "Charlie", 28, "charlie@example.com", 55000},
	}

	ts.BeforeFirst()
	for _, emp := range employees {
		ts.Insert()
		ts.SetInt("id", emp.id)
		ts.SetString("name", emp.name)
		ts.SetInt("age", emp.age)
		ts.SetString("email", emp.email)
		ts.SetInt("salary", emp.salary)
		t.Logf("Inserted: id=%d, name=%s, age=%d, email=%s, salary=%d",
			emp.id, emp.name, emp.age, emp.email, emp.salary)
	}

	return tx, ts
}

// TestProjectScanBasicProjection tests projecting a subset of fields
func TestProjectScanBasicProjection(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_basic"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	t.Run("ProjectTwoFields", func(t *testing.T) {
		ts.BeforeFirst()

		// Project only id and name
		fieldList := []string{"id", "name"}
		projectScan := NewProjectScan(ts, fieldList)
		require.NotNil(t, projectScan)

		projectScan.BeforeFirst()
		count := 0
		for projectScan.Next() {
			id := projectScan.GetInt("id")
			name := projectScan.GetString("name")
			t.Logf("Projected record: id=%d, name=%s", id, name)
			count++
		}

		assert.Equal(t, 3, count, "Should have 3 records")
		projectScan.Close()
	})

	t.Run("ProjectSingleField", func(t *testing.T) {
		ts.BeforeFirst()

		// Project only name
		fieldList := []string{"name"}
		projectScan := NewProjectScan(ts, fieldList)

		projectScan.BeforeFirst()
		names := []string{}
		for projectScan.Next() {
			name := projectScan.GetString("name")
			names = append(names, name)
			t.Logf("Projected name: %s", name)
		}

		require.Len(t, names, 3)
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Bob")
		assert.Contains(t, names, "Charlie")

		projectScan.Close()
	})

	t.Run("ProjectAllFields", func(t *testing.T) {
		ts.BeforeFirst()

		// Project all fields
		fieldList := []string{"id", "name", "age", "email", "salary"}
		projectScan := NewProjectScan(ts, fieldList)

		projectScan.BeforeFirst()
		if projectScan.Next() {
			id := projectScan.GetInt("id")
			name := projectScan.GetString("name")
			age := projectScan.GetInt("age")
			email := projectScan.GetString("email")
			salary := projectScan.GetInt("salary")

			t.Logf("All fields: id=%d, name=%s, age=%d, email=%s, salary=%d",
				id, name, age, email, salary)

			assert.Greater(t, id, 0)
			assert.NotEmpty(t, name)
			assert.Greater(t, age, 0)
			assert.NotEmpty(t, email)
			assert.Greater(t, salary, 0)
		}

		projectScan.Close()
	})
}

// TestProjectScanHasField tests field validation
func TestProjectScanHasField(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_hasfield"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	ts.BeforeFirst()

	// Project only id and name
	fieldList := []string{"id", "name"}
	projectScan := NewProjectScan(ts, fieldList)

	// Fields in projection
	assert.True(t, projectScan.HasField("id"), "Should have id field")
	assert.True(t, projectScan.HasField("name"), "Should have name field")
	t.Log("Projected fields (id, name) are accessible")

	// Fields not in projection (but exist in underlying scan)
	assert.False(t, projectScan.HasField("age"), "Should not have age field")
	assert.False(t, projectScan.HasField("email"), "Should not have email field")
	assert.False(t, projectScan.HasField("salary"), "Should not have salary field")
	t.Log("Non-projected fields (age, email, salary) are not accessible")

	// Non-existent field
	assert.False(t, projectScan.HasField("missing"), "Should not have missing field")

	projectScan.Close()
}

// TestProjectScanAccessNonProjectedField tests that accessing non-projected fields panics
func TestProjectScanAccessNonProjectedField(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_panic"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	ts.BeforeFirst()

	// Project only id and name
	fieldList := []string{"id", "name"}
	projectScan := NewProjectScan(ts, fieldList)

	projectScan.BeforeFirst()
	if projectScan.Next() {
		// Accessing projected fields should work
		id := projectScan.GetInt("id")
		name := projectScan.GetString("name")
		t.Logf("Accessed projected fields: id=%d, name=%s", id, name)

		// Accessing non-projected field should panic
		assert.Panics(t, func() {
			projectScan.GetInt("age")
		}, "Accessing non-projected int field should panic")
		t.Log("GetInt on non-projected field correctly panics")

		assert.Panics(t, func() {
			projectScan.GetString("email")
		}, "Accessing non-projected string field should panic")
		t.Log("GetString on non-projected field correctly panics")

		assert.Panics(t, func() {
			projectScan.GetValue("salary")
		}, "Accessing non-projected field via GetValue should panic")
		t.Log("GetValue on non-projected field correctly panics")
	}

	projectScan.Close()
}

// TestProjectScanNavigation tests navigation operations
func TestProjectScanNavigation(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_navigation"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	t.Run("NextIteration", func(t *testing.T) {
		ts.BeforeFirst()

		fieldList := []string{"id", "name"}
		projectScan := NewProjectScan(ts, fieldList)

		projectScan.BeforeFirst()
		count := 0
		for projectScan.Next() {
			count++
		}

		assert.Equal(t, 3, count, "Should iterate through all records")
		t.Logf("Iterated through %d records", count)

		projectScan.Close()
	})

	t.Run("ReIteration", func(t *testing.T) {
		ts.BeforeFirst()

		fieldList := []string{"id", "name"}
		projectScan := NewProjectScan(ts, fieldList)

		// First iteration
		projectScan.BeforeFirst()
		count1 := 0
		for projectScan.Next() {
			count1++
		}
		t.Logf("First iteration: %d records", count1)

		// Second iteration
		projectScan.BeforeFirst()
		count2 := 0
		for projectScan.Next() {
			count2++
		}
		t.Logf("Second iteration: %d records", count2)

		assert.Equal(t, count1, count2, "Should be able to re-iterate")
		assert.Equal(t, 3, count1)

		projectScan.Close()
	})
}

// TestProjectScanGetValue tests the GetValue method
func TestProjectScanGetValue(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_getvalue"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	ts.BeforeFirst()

	fieldList := []string{"id", "name", "age"}
	projectScan := NewProjectScan(ts, fieldList)

	projectScan.BeforeFirst()
	if projectScan.Next() {
		// GetValue on int field
		idVal := projectScan.GetValue("id")
		require.NotNil(t, idVal)
		t.Logf("GetValue(id) returned: %v (type: %T)", idVal, idVal)

		// GetValue on string field
		nameVal := projectScan.GetValue("name")
		require.NotNil(t, nameVal)
		t.Logf("GetValue(name) returned: %v (type: %T)", nameVal, nameVal)
	}

	projectScan.Close()
}

// TestProjectScanWithSelectScan tests combining ProjectScan with SelectScan
func TestProjectScanWithSelectScan(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_select"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	ts.BeforeFirst()

	// First apply projection
	fieldList := []string{"id", "name", "age"}
	projectScan := NewProjectScan(ts, fieldList)

	// Then apply filter: age = 30 (exact match)
	term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(30)))
	predicate := NewPredicate(*term)
	selectScan := NewSelectScan(projectScan, *predicate)

	selectScan.BeforeFirst()
	count := 0
	for selectScan.Next() {
		id := selectScan.GetInt("id")
		name := selectScan.GetString("name")
		age := selectScan.GetInt("age")

		assert.Equal(t, 30, age, "Age should be exactly 30")
		t.Logf("Projected and filtered: id=%d, name=%s, age=%d", id, name, age)
		count++
	}

	// Alice (age=30) is the only match
	assert.Equal(t, 1, count, "Should have 1 record with age = 30")
	t.Log("ProjectScan combined with SelectScan works correctly")

	selectScan.Close()
}

// TestProjectScanEmptyFieldList tests projection with empty field list
func TestProjectScanEmptyFieldList(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_empty"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	ts.BeforeFirst()

	// Empty field list (edge case)
	fieldList := []string{}
	projectScan := NewProjectScan(ts, fieldList)

	// Should have no fields
	assert.False(t, projectScan.HasField("id"))
	assert.False(t, projectScan.HasField("name"))
	t.Log("Empty field list results in no accessible fields")

	// Should still iterate through records
	projectScan.BeforeFirst()
	count := 0
	for projectScan.Next() {
		count++
	}
	assert.Equal(t, 3, count, "Should still iterate through all records")

	projectScan.Close()
}

// TestProjectScanOrdering tests that projection maintains record order
func TestProjectScanOrdering(t *testing.T) {
	testDir := "/tmp/testdb_projectscan_order"
	defer os.RemoveAll(testDir)

	tx, ts := setupProjectScanTest(t, testDir)
	defer tx.Commit()

	ts.BeforeFirst()

	fieldList := []string{"id", "name"}
	projectScan := NewProjectScan(ts, fieldList)

	// Collect IDs in order
	projectScan.BeforeFirst()
	var ids []int
	for projectScan.Next() {
		id := projectScan.GetInt("id")
		ids = append(ids, id)
	}

	// IDs should be in the same order as inserted
	require.Len(t, ids, 3)
	assert.Equal(t, 1, ids[0])
	assert.Equal(t, 2, ids[1])
	assert.Equal(t, 3, ids[2])
	t.Logf("Record order preserved: %v", ids)

	projectScan.Close()
}
