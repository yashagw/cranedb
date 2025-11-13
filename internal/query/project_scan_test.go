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
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// setupProjectScanTest creates a test table with multiple fields
func setupProjectScanTest(t *testing.T, testDir string) (*transaction.Transaction, *table.TableScan) {
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
	ts, err := table.NewTableScan(tx, layout, "Employees")
	require.NoError(t, err)

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

	err = ts.BeforeFirst()
	require.NoError(t, err)
	for _, emp := range employees {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", emp.id)

		require.NoError(t, err)
		err = ts.SetString("name", emp.name)

		require.NoError(t, err)
		err = ts.SetInt("age", emp.age)

		require.NoError(t, err)
		err = ts.SetString("email", emp.email)

		require.NoError(t, err)
		err = ts.SetInt("salary", emp.salary)

		require.NoError(t, err)
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
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Project only id and name
		fieldList := []string{"id", "name"}
		projectScan := NewProjectScan(ts, fieldList)
		require.NotNil(t, projectScan)

		err = projectScan.BeforeFirst()
		require.NoError(t, err)
		count := 0
		for {
			hasNext, err := projectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := projectScan.GetInt("id")

			require.NoError(t, err)
			name, err := projectScan.GetString("name")

			require.NoError(t, err)
			t.Logf("Projected record: id=%d, name=%s", id, name)
			count++
		}

		assert.Equal(t, 3, count, "Should have 3 records")
		projectScan.Close()
	})

	t.Run("ProjectSingleField", func(t *testing.T) {
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Project only name
		fieldList := []string{"name"}
		projectScan := NewProjectScan(ts, fieldList)

		err = projectScan.BeforeFirst()
		require.NoError(t, err)
		names := []string{}
		for {
			hasNext, err := projectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			name, err := projectScan.GetString("name")

			require.NoError(t, err)
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
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Project all fields
		fieldList := []string{"id", "name", "age", "email", "salary"}
		projectScan := NewProjectScan(ts, fieldList)

		err = projectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := projectScan.Next()
		require.NoError(t, err)
		if hasNext {
			id, err := projectScan.GetInt("id")

			require.NoError(t, err)
			name, err := projectScan.GetString("name")

			require.NoError(t, err)
			age, err := projectScan.GetInt("age")

			require.NoError(t, err)
			email, err := projectScan.GetString("email")

			require.NoError(t, err)
			salary, err := projectScan.GetInt("salary")

			require.NoError(t, err)

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

	err := ts.BeforeFirst()
	require.NoError(t, err)

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

	err := ts.BeforeFirst()
	require.NoError(t, err)

	// Project only id and name
	fieldList := []string{"id", "name"}
	projectScan := NewProjectScan(ts, fieldList)

	err = projectScan.BeforeFirst()

	require.NoError(t, err)
	hasNext, err := projectScan.Next()
	require.NoError(t, err)
	if hasNext {
		// Accessing projected fields should work
		id, err := projectScan.GetInt("id")

		require.NoError(t, err)
		name, err := projectScan.GetString("name")

		require.NoError(t, err)
		t.Logf("Accessed projected fields: id=%d, name=%s", id, name)

		// Accessing non-projected field should return error
		_, err = projectScan.GetInt("age")
		assert.Error(t, err, "Accessing non-projected int field should return error")
		t.Log("GetInt on non-projected field correctly returns error")

		_, err = projectScan.GetString("email")
		assert.Error(t, err, "Accessing non-projected string field should return error")
		t.Log("GetString on non-projected field correctly returns error")

		_, err = projectScan.GetValue("salary")
		assert.Error(t, err, "Accessing non-projected field via GetValue should return error")
		t.Log("GetValue on non-projected field correctly returns error")
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
		err := ts.BeforeFirst()
		require.NoError(t, err)

		fieldList := []string{"id", "name"}
		projectScan := NewProjectScan(ts, fieldList)

		err = projectScan.BeforeFirst()
		require.NoError(t, err)
		count := 0
		for {
			hasNext, err := projectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 3, count, "Should iterate through all records")
		t.Logf("Iterated through %d records", count)

		projectScan.Close()
	})

	t.Run("ReIteration", func(t *testing.T) {
		err := ts.BeforeFirst()
		require.NoError(t, err)

		fieldList := []string{"id", "name"}
		projectScan := NewProjectScan(ts, fieldList)

		// First iteration
		err = projectScan.BeforeFirst()

		require.NoError(t, err)
		count1 := 0
		for {
			hasNext, err := projectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count1++
		}
		t.Logf("First iteration: %d records", count1)

		// Second iteration
		err = projectScan.BeforeFirst()

		require.NoError(t, err)
		count2 := 0
		for {
			hasNext, err := projectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
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

	err := ts.BeforeFirst()
	require.NoError(t, err)

	fieldList := []string{"id", "name", "age"}
	projectScan := NewProjectScan(ts, fieldList)

	err = projectScan.BeforeFirst()

	require.NoError(t, err)
	hasNext, err := projectScan.Next()
	require.NoError(t, err)
	if hasNext {
		// GetValue on int field
		idVal, err := projectScan.GetValue("id")

		require.NoError(t, err)
		require.NotNil(t, idVal)
		t.Logf("GetValue(id) returned: %v (type: %T)", idVal, idVal)

		// GetValue on string field
		nameVal, err := projectScan.GetValue("name")

		require.NoError(t, err)
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

	err := ts.BeforeFirst()
	require.NoError(t, err)

	// First apply projection
	fieldList := []string{"id", "name", "age"}
	projectScan := NewProjectScan(ts, fieldList)

	// Then apply filter: age = 30 (exact match)
	predicate := createEqualsPredicate("age", 30)
	selectScan := NewSelectScan(projectScan, *predicate)

	err = selectScan.BeforeFirst()

	require.NoError(t, err)
	count := 0
	for {
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := selectScan.GetInt("id")

		require.NoError(t, err)
		name, err := selectScan.GetString("name")

		require.NoError(t, err)
		age, err := selectScan.GetInt("age")

		require.NoError(t, err)

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

	err := ts.BeforeFirst()
	require.NoError(t, err)

	// Empty field list (edge case)
	fieldList := []string{}
	projectScan := NewProjectScan(ts, fieldList)

	// Should have no fields
	assert.False(t, projectScan.HasField("id"))
	assert.False(t, projectScan.HasField("name"))
	t.Log("Empty field list results in no accessible fields")

	// Should still iterate through records
	err = projectScan.BeforeFirst()

	require.NoError(t, err)
	count := 0
	for {
		hasNext, err := projectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
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

	err := ts.BeforeFirst()
	require.NoError(t, err)

	fieldList := []string{"id", "name"}
	projectScan := NewProjectScan(ts, fieldList)

	// Collect IDs in order
	err = projectScan.BeforeFirst()

	require.NoError(t, err)
	var ids []int
	for {
		hasNext, err := projectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := projectScan.GetInt("id")

		require.NoError(t, err)
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
