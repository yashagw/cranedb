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

// setupTestDB creates a test database with sample data
func setupTestDB(t *testing.T, testDir string) (*transaction.Transaction, *record.TableScan) {
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
	ts := record.NewTableScan(tx, layout, "TestTable")
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

	ts.BeforeFirst()
	for _, data := range testData {
		ts.Insert()
		ts.SetInt("id", data.id)
		ts.SetInt("age", data.age)
		ts.SetString("name", data.name)
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
		term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
		predicate := NewPredicate(*term)

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

		for selectScan.Next() {
			id := selectScan.GetInt("id")
			age := selectScan.GetInt("age")
			name := selectScan.GetString("name")
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
		term := NewTerm(*NewFieldNameExpression("name"), *NewConstantExpression(*NewStringConstant("Bob")))
		predicate := NewPredicate(*term)

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Collect results
		selectScan.BeforeFirst()
		count := 0
		for selectScan.Next() {
			id := selectScan.GetInt("id")
			age := selectScan.GetInt("age")
			name := selectScan.GetString("name")

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
		term1 := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(30)))
		term2 := NewTerm(*NewFieldNameExpression("name"), *NewConstantExpression(*NewStringConstant("Grace")))
		predicate := NewPredicate(*term1)
		predicate.ConjunctWith(*NewPredicate(*term2))

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Collect results
		selectScan.BeforeFirst()
		count := 0
		for selectScan.Next() {
			id := selectScan.GetInt("id")
			age := selectScan.GetInt("age")
			name := selectScan.GetString("name")

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
		term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(100)))
		predicate := NewPredicate(*term)

		// Create SelectScan
		selectScan := NewSelectScan(ts, *predicate)
		require.NotNil(t, selectScan)

		// Verify no results
		selectScan.BeforeFirst()
		count := 0
		for selectScan.Next() {
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

		term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		assert.True(t, selectScan.HasField("id"))
		assert.True(t, selectScan.HasField("age"))
		assert.True(t, selectScan.HasField("name"))
		assert.False(t, selectScan.HasField("missing"))

		selectScan.Close()
	})

	t.Run("GetValue", func(t *testing.T) {
		ts.BeforeFirst()

		term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		selectScan.BeforeFirst()
		if selectScan.Next() {
			ageVal := selectScan.GetValue("age")
			require.NotNil(t, ageVal)

			// GetValue returns a Constant
			constant, ok := ageVal.(Constant)
			require.True(t, ok, "GetValue should return a Constant")
			assert.Equal(t, 25, constant.AsInt())
			t.Logf("GetValue for age returned Constant with value: %d", constant.AsInt())

			nameVal := selectScan.GetValue("name")
			require.NotNil(t, nameVal)
			nameConstant, ok := nameVal.(Constant)
			require.True(t, ok, "GetValue should return a Constant")
			assert.NotEmpty(t, nameConstant.AsString())
			t.Logf("GetValue for name returned Constant with value: %s", nameConstant.AsString())
		}

		selectScan.Close()
	})

	t.Run("GetIntAndGetString", func(t *testing.T) {
		ts.BeforeFirst()

		term := NewTerm(*NewFieldNameExpression("id"), *NewConstantExpression(*NewIntConstant(2)))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		selectScan.BeforeFirst()
		if selectScan.Next() {
			id := selectScan.GetInt("id")
			age := selectScan.GetInt("age")
			name := selectScan.GetString("name")

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
		term := NewTerm(*NewFieldNameExpression("name"), *NewConstantExpression(*NewStringConstant("Bob")))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		selectScan.BeforeFirst()
		if selectScan.Next() {
			originalAge := selectScan.GetInt("age")
			t.Logf("Bob's original age: %d", originalAge)

			// Update Bob's age
			newAge := 31
			selectScan.SetInt("age", newAge)
			assert.Equal(t, newAge, selectScan.GetInt("age"))
			t.Logf("Updated Bob's age to %d", newAge)
		}
		selectScan.Close()

		// Verify the update persisted
		ts.BeforeFirst()
		term2 := NewTerm(*NewFieldNameExpression("name"), *NewConstantExpression(*NewStringConstant("Bob")))
		predicate2 := NewPredicate(*term2)
		selectScan2 := NewSelectScan(ts, *predicate2)

		selectScan2.BeforeFirst()
		if selectScan2.Next() {
			assert.Equal(t, 31, selectScan2.GetInt("age"), "Bob's age should be updated to 31")
			t.Log("Verified: Bob's age was successfully updated")
		}
		selectScan2.Close()
	})

	t.Run("SetString", func(t *testing.T) {
		ts.BeforeFirst()

		// Find Alice and update her name
		term := NewTerm(*NewFieldNameExpression("id"), *NewConstantExpression(*NewIntConstant(1)))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		selectScan.BeforeFirst()
		if selectScan.Next() {
			originalName := selectScan.GetString("name")
			t.Logf("Original name: %s", originalName)

			// Update name
			newName := "Alicia"
			selectScan.SetString("name", newName)
			assert.Equal(t, newName, selectScan.GetString("name"))
			t.Logf("Updated name to %s", newName)
		}
		selectScan.Close()

		// Verify the update persisted
		ts.BeforeFirst()
		term2 := NewTerm(*NewFieldNameExpression("id"), *NewConstantExpression(*NewIntConstant(1)))
		predicate2 := NewPredicate(*term2)
		selectScan2 := NewSelectScan(ts, *predicate2)

		selectScan2.BeforeFirst()
		if selectScan2.Next() {
			assert.Equal(t, "Alicia", selectScan2.GetString("name"))
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
	ts := record.NewTableScan(tx, layout, "TestTable")

	// Create SelectScan with a predicate
	term := NewTerm(*NewFieldNameExpression("id"), *NewConstantExpression(*NewIntConstant(1)))
	predicate := NewPredicate(*term)
	selectScan := NewSelectScan(ts, *predicate)

	// Test Insert through SelectScan
	t.Log("Inserting new record through SelectScan")
	selectScan.BeforeFirst()
	selectScan.Insert()
	selectScan.SetInt("id", 1)
	selectScan.SetString("name", "TestUser")
	t.Log("Inserted: id=1, name=TestUser")

	selectScan.Close()

	// Verify the insert
	ts.BeforeFirst()
	count := 0
	for ts.Next() {
		id := ts.GetInt("id")
		name := ts.GetString("name")
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
	for ts.Next() {
		initialCount++
	}
	t.Logf("Initial record count: %d", initialCount)

	// Delete all records with age = 25
	ts.BeforeFirst()
	term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
	predicate := NewPredicate(*term)
	selectScan := NewSelectScan(ts, *predicate)

	deletedCount := 0
	selectScan.BeforeFirst()
	for selectScan.Next() {
		name := selectScan.GetString("name")
		t.Logf("Deleting: %s (age=25)", name)
		selectScan.Delete()
		deletedCount++
	}
	assert.Equal(t, 3, deletedCount, "Should delete 3 records with age=25")
	t.Logf("Deleted %d records", deletedCount)
	selectScan.Close()

	// Verify deletion
	ts.BeforeFirst()
	remainingCount := 0
	for ts.Next() {
		age := ts.GetInt("age")
		name := ts.GetString("name")
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

		term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(30)))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		selectScan.BeforeFirst()
		if selectScan.Next() {
			rid := selectScan.GetRID()
			require.NotNil(t, rid)
			originalName := selectScan.GetString("name")
			t.Logf("Got RID for %s: block=%d, slot=%d", originalName, rid.Block(), rid.Slot())

			// Move to next record
			if selectScan.Next() {
				differentName := selectScan.GetString("name")
				assert.NotEqual(t, originalName, differentName)
				t.Logf("Moved to next record: %s", differentName)

				// Move back to original RID
				selectScan.MoveToRID(rid)
				movedBackName := selectScan.GetString("name")
				assert.Equal(t, originalName, movedBackName, "Should move back to original record")
				t.Logf("Moved back to original record: %s", movedBackName)
			}
		}

		selectScan.Close()
	})

	t.Run("BeforeFirstAndNext", func(t *testing.T) {
		ts.BeforeFirst()

		term := NewTerm(*NewFieldNameExpression("age"), *NewConstantExpression(*NewIntConstant(25)))
		predicate := NewPredicate(*term)
		selectScan := NewSelectScan(ts, *predicate)

		// First iteration
		selectScan.BeforeFirst()
		count1 := 0
		for selectScan.Next() {
			count1++
		}
		t.Logf("First iteration found %d records", count1)

		// Second iteration - should work again
		selectScan.BeforeFirst()
		count2 := 0
		for selectScan.Next() {
			count2++
		}
		t.Logf("Second iteration found %d records", count2)

		assert.Equal(t, count1, count2, "BeforeFirst should reset scan for re-iteration")

		selectScan.Close()
	})
}
