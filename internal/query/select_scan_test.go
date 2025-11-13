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

// setupTestDB creates a test database with sample data
func setupTestDB(t *testing.T, testDir string) (*transaction.Transaction, *table.TableScan) {
	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	// Create schema with int and string fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("age")
	schema.AddStringField("name", 20)

	layout := record.NewLayoutFromSchema(schema)
	require.NotNil(t, layout)

	// Create TableScan and insert test data
	ts, err := table.NewTableScan(tx, layout, "TestTable")
	require.NoError(t, err)
	require.NotNil(t, ts)

	// Insert test records
	testData := []struct {
		id   int
		age  int
		name string
	}{
		{1, 25, "Alice"},
		{2, 30, "Bob"},
		{3, 25, "Charlie"},
		{4, 35, "David"},
		{5, 25, "Eve"},
		{6, 40, "Frank"},
		{7, 30, "Grace"},
		{8, 45, "Henry"},
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)
	for _, data := range testData {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", data.id)
		require.NoError(t, err)
		err = ts.SetInt("age", data.age)
		require.NoError(t, err)
		err = ts.SetString("name", data.name)
		require.NoError(t, err)
		t.Logf("Inserted: id=%d, age=%d, name=%s", data.id, data.age, data.name)
	}

	return tx, ts
}

// TestSelectScanFiltering tests all filtering operations with predicates
func TestSelectScanFiltering(t *testing.T) {
	testDir := "/tmp/testdb_selectscan_filter"
	defer os.RemoveAll(testDir)

	tx, ts := setupTestDB(t, testDir)
	defer tx.Commit()

	t.Run("FilterByIntField", func(t *testing.T) {
		ts.BeforeFirst()

		// Create predicate: age = 25
		predicate := createEqualsPredicate("age", 25)

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Collect results
		selectScan.BeforeFirst()
		var results []struct {
			id   int
			age  int
			name string
		}

		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := selectScan.GetInt("id")

			require.NoError(t, err)
			age, err := selectScan.GetInt("age")

			require.NoError(t, err)
			name, err := selectScan.GetString("name")

			require.NoError(t, err)
			results = append(results, struct {
				id   int
				age  int
				name string
			}{id, age, name})
			t.Logf("Found: id=%d, age=%d, name=%s", id, age, name)
		}

		// Verify results
		require.Len(t, results, 3, "Should find 3 records with age=25")
		for _, r := range results {
			assert.Equal(t, 25, r.age, "All results should have age=25")
		}

		// Verify specific records
		expectedNames := map[string]bool{"Alice": true, "Charlie": true, "Eve": true}
		for _, r := range results {
			assert.True(t, expectedNames[r.name], "Name should be one of Alice, Charlie, or Eve")
		}

		selectScan.Close()
	})

	t.Run("FilterByStringField", func(t *testing.T) {
		ts.BeforeFirst()

		// Create predicate: name = "Bob"
		predicate := createEqualsPredicate("name", "Bob")

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Collect results
		selectScan.BeforeFirst()
		count := 0
		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := selectScan.GetInt("id")

			require.NoError(t, err)
			age, err := selectScan.GetInt("age")

			require.NoError(t, err)
			name, err := selectScan.GetString("name")

			require.NoError(t, err)

			assert.Equal(t, 2, id)
			assert.Equal(t, 30, age)
			assert.Equal(t, "Bob", name)
			t.Logf("Found: id=%d, age=%d, name=%s", id, age, name)
			count++
		}

		assert.Equal(t, 1, count, "Should find exactly 1 record with name=Bob")
		selectScan.Close()
	})

	t.Run("FilterWithCompoundPredicate", func(t *testing.T) {
		ts.BeforeFirst()

		// Create compound predicate: age = 30 AND name = "Grace"
		predicate := createCompoundPredicate([]struct {
			fieldName string
			value     interface{}
		}{
			{"age", 30},
			{"name", "Grace"},
		})

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Collect results
		selectScan.BeforeFirst()
		count := 0
		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := selectScan.GetInt("id")

			require.NoError(t, err)
			age, err := selectScan.GetInt("age")

			require.NoError(t, err)
			name, err := selectScan.GetString("name")

			require.NoError(t, err)

			assert.Equal(t, 7, id)
			assert.Equal(t, 30, age)
			assert.Equal(t, "Grace", name)
			t.Logf("Found: id=%d, age=%d, name=%s", id, age, name)
			count++
		}

		assert.Equal(t, 1, count, "Should find exactly 1 record matching both conditions")
		selectScan.Close()
	})

	t.Run("FilterMatchesNoRecords", func(t *testing.T) {
		ts.BeforeFirst()

		// Create predicate: age = 100 (no records should match)
		predicate := createEqualsPredicate("age", 100)

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Verify no results
		selectScan.BeforeFirst()
		count := 0
		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 0, count, "Should find no records with age=100")
		t.Log("Correctly returned 0 records for non-matching predicate")
		selectScan.Close()
	})
}

// TestSelectScanReadOperations tests read-only operations like GetInt, GetString, GetValue, HasField
func TestSelectScanReadOperations(t *testing.T) {
	testDir := "/tmp/testdb_selectscan_read"
	defer os.RemoveAll(testDir)

	tx, ts := setupTestDB(t, testDir)
	defer tx.Commit()

	t.Run("HasField", func(t *testing.T) {
		ts.BeforeFirst()

		predicate := createEqualsPredicate("age", 25)
		selectScan := NewSelectScan(ts, *predicate)

		assert.True(t, selectScan.HasField("id"))
		assert.True(t, selectScan.HasField("age"))
		assert.True(t, selectScan.HasField("name"))
		assert.False(t, selectScan.HasField("missing"))

		selectScan.Close()
	})

	t.Run("GetValue", func(t *testing.T) {
		ts.BeforeFirst()

		predicate := createEqualsPredicate("age", 25)
		selectScan := NewSelectScan(ts, *predicate)

		err := selectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if hasNext {
			ageVal, err := selectScan.GetValue("age")

			require.NoError(t, err)
			require.NotNil(t, ageVal)

			// GetValue returns a value (int or string)
			ageInt, ok := ageVal.(int)
			require.True(t, ok, "GetValue should return int")
			assert.Equal(t, 25, ageInt)
			t.Logf("GetValue for age returned value: %d", ageInt)

			nameVal, err := selectScan.GetValue("name")

			require.NoError(t, err)
			require.NotNil(t, nameVal)
			nameStr, ok := nameVal.(string)
			require.True(t, ok, "GetValue should return string")
			assert.NotEmpty(t, nameStr)
			t.Logf("GetValue for name returned value: %s", nameStr)
		}

		selectScan.Close()
	})

	t.Run("GetIntAndGetString", func(t *testing.T) {
		ts.BeforeFirst()

		predicate := createEqualsPredicate("id", 2)
		selectScan := NewSelectScan(ts, *predicate)

		err := selectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if hasNext {
			id, err := selectScan.GetInt("id")

			require.NoError(t, err)
			age, err := selectScan.GetInt("age")

			require.NoError(t, err)
			name, err := selectScan.GetString("name")

			require.NoError(t, err)

			assert.Equal(t, 2, id)
			assert.Equal(t, 30, age)
			assert.Equal(t, "Bob", name)
			t.Logf("Read record: id=%d, age=%d, name=%s", id, age, name)
		}

		selectScan.Close()
	})
}

// TestSelectScanUpdateOperations tests update operations like SetInt, SetString
func TestSelectScanUpdateOperations(t *testing.T) {
	testDir := "/tmp/testdb_selectscan_update"
	defer os.RemoveAll(testDir)

	tx, ts := setupTestDB(t, testDir)
	defer tx.Commit()

	t.Run("SetInt", func(t *testing.T) {
		ts.BeforeFirst()

		// Find Bob and update his age
		predicate := createEqualsPredicate("name", "Bob")
		selectScan := NewSelectScan(ts, *predicate)

		err := selectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if hasNext {
			originalAge, err := selectScan.GetInt("age")

			require.NoError(t, err)
			t.Logf("Bob's original age: %d", originalAge)

			// Update Bob's age
			newAge := 31
			err = selectScan.SetInt("age", newAge)
			require.NoError(t, err)
			checkAge, err := selectScan.GetInt("age")
			require.NoError(t, err)
			assert.Equal(t, newAge, checkAge)
			t.Logf("Updated Bob's age to %d", newAge)
		}
		selectScan.Close()

		// Verify the update persisted
		ts.BeforeFirst()
		predicate2 := createEqualsPredicate("name", "Bob")
		selectScan2 := NewSelectScan(ts, *predicate2)

		err = selectScan2.BeforeFirst()
		require.NoError(t, err)
		hasNext, err = selectScan2.Next()
		require.NoError(t, err)
		if hasNext {
			checkAge, err := selectScan2.GetInt("age")
			require.NoError(t, err)
			assert.Equal(t, 31, checkAge, "Bob's age should be updated to 31")
			t.Log("Verified: Bob's age was successfully updated")
		}
		selectScan2.Close()
	})

	t.Run("SetString", func(t *testing.T) {
		ts.BeforeFirst()

		// Find Alice and update her name
		predicate := createEqualsPredicate("id", 1)
		selectScan := NewSelectScan(ts, *predicate)

		err := selectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if hasNext {
			originalName, err := selectScan.GetString("name")

			require.NoError(t, err)
			t.Logf("Original name: %s", originalName)

			// Update name
			newName := "Alicia"
			err = selectScan.SetString("name", newName)
			require.NoError(t, err)
			checkName, err := selectScan.GetString("name")
			require.NoError(t, err)
			assert.Equal(t, newName, checkName)
			t.Logf("Updated name to %s", newName)
		}
		selectScan.Close()

		// Verify the update persisted
		ts.BeforeFirst()
		predicate2 := createEqualsPredicate("id", 1)
		selectScan2 := NewSelectScan(ts, *predicate2)

		err = selectScan2.BeforeFirst()
		require.NoError(t, err)
		hasNext, err = selectScan2.Next()
		require.NoError(t, err)
		if hasNext {
			checkName, err := selectScan2.GetString("name")
			require.NoError(t, err)
			assert.Equal(t, "Alicia", checkName)
			t.Log("Verified: Name was successfully updated")
		}
		selectScan2.Close()
	})
}

// TestSelectScanInsertOperation tests insert operations
func TestSelectScanInsertOperation(t *testing.T) {
	testDir := "/tmp/testdb_selectscan_insert"
	defer os.RemoveAll(testDir)

	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)
	defer tx.Commit()

	// Create schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, "TestTable")
	require.NoError(t, err)

	// Create SelectScan with a predicate
	predicate := createEqualsPredicate("id", 1)
	selectScan := NewSelectScan(ts, *predicate)

	// Test Insert through SelectScan
	t.Log("Inserting new record through SelectScan")
	err = selectScan.BeforeFirst()
	require.NoError(t, err)
	err = selectScan.Insert()
	require.NoError(t, err)
	err = selectScan.SetInt("id", 1)

	require.NoError(t, err)
	err = selectScan.SetString("name", "TestUser")

	require.NoError(t, err)
	t.Log("Inserted: id=1, name=TestUser")

	selectScan.Close()

	// Verify the insert
	ts.BeforeFirst()
	count := 0
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
		assert.Equal(t, 1, id)
		assert.Equal(t, "TestUser", name)
		t.Logf("Verified inserted record: id=%d, name=%s", id, name)
		count++
	}
	assert.Equal(t, 1, count, "Should have exactly 1 record")
}

// TestSelectScanDeleteOperation tests delete operations
func TestSelectScanDeleteOperation(t *testing.T) {
	testDir := "/tmp/testdb_selectscan_delete"
	defer os.RemoveAll(testDir)

	tx, ts := setupTestDB(t, testDir)
	defer tx.Commit()

	// Count records before deletion
	initialCount := 0
	ts.BeforeFirst()
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		initialCount++
	}
	t.Logf("Initial record count: %d", initialCount)

	// Delete all records with age = 25
	ts.BeforeFirst()
	predicate := createEqualsPredicate("age", 25)
	selectScan := NewSelectScan(ts, *predicate)

	deletedCount := 0
	selectScan.BeforeFirst()
	for {
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := selectScan.GetString("name")

		require.NoError(t, err)
		t.Logf("Deleting: %s (age=25)", name)
		err = selectScan.Delete()
		require.NoError(t, err)
		deletedCount++
	}
	assert.Equal(t, 3, deletedCount, "Should delete 3 records with age=25")
	t.Logf("Deleted %d records", deletedCount)
	selectScan.Close()

	// Verify deletion
	ts.BeforeFirst()
	remainingCount := 0
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		age, err := ts.GetInt("age")

		require.NoError(t, err)
		name, err := ts.GetString("name")

		require.NoError(t, err)
		assert.NotEqual(t, 25, age, "No records with age=25 should remain")
		t.Logf("Remaining record: %s (age=%d)", name, age)
		remainingCount++
	}
	t.Logf("Remaining record count: %d", remainingCount)
	assert.Equal(t, initialCount-deletedCount, remainingCount)
}

// TestSelectScanNavigationOperations tests RID-based navigation
func TestSelectScanNavigationOperations(t *testing.T) {
	testDir := "/tmp/testdb_selectscan_navigation"
	defer os.RemoveAll(testDir)

	tx, ts := setupTestDB(t, testDir)
	defer tx.Commit()

	t.Run("GetRIDAndMoveToRID", func(t *testing.T) {
		ts.BeforeFirst()

		predicate := createEqualsPredicate("age", 30)
		selectScan := NewSelectScan(ts, *predicate)

		err := selectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if hasNext {
			rid, err := selectScan.GetRID()
			require.NoError(t, err)
			require.NotNil(t, rid)
			originalName, err := selectScan.GetString("name")
			require.NoError(t, err)
			t.Logf("Got RID for %s: block=%d, slot=%d", originalName, rid.Block(), rid.Slot())

			// Move to next record
			hasNext, err = selectScan.Next()
			require.NoError(t, err)
			if hasNext {
				differentName, err := selectScan.GetString("name")
				require.NoError(t, err)
				assert.NotEqual(t, originalName, differentName)
				t.Logf("Moved to next record: %s", differentName)

				// Move back to original RID
				err = selectScan.MoveToRID(rid)
				require.NoError(t, err)
				movedBackName, err := selectScan.GetString("name")
				require.NoError(t, err)
				assert.Equal(t, originalName, movedBackName, "Should move back to original record")
				t.Logf("Moved back to original record: %s", movedBackName)
			}
		}

		selectScan.Close()
	})

	t.Run("BeforeFirstAndNext", func(t *testing.T) {
		ts.BeforeFirst()

		predicate := createEqualsPredicate("age", 25)
		selectScan := NewSelectScan(ts, *predicate)

		// First iteration
		selectScan.BeforeFirst()
		count1 := 0
		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count1++
		}
		t.Logf("First iteration found %d records", count1)

		// Second iteration - should work again
		selectScan.BeforeFirst()
		count2 := 0
		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count2++
		}
		t.Logf("Second iteration found %d records", count2)

		assert.Equal(t, count1, count2, "BeforeFirst should reset scan for re-iteration")

		selectScan.Close()
	})
}
