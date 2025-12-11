package query

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/index"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// setupIndexSelectScanTest creates a test database with a table and hash index
func setupIndexSelectScanTest(t *testing.T, testDir string) (*transaction.Transaction, *table.TableScan, index.Index, *metadata.TableManager, *metadata.IndexManager) {
	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	require.NotNil(t, tx)

	// Create metadata managers
	tableManager := metadata.NewTableManager(true, tx)
	statsManager := metadata.NewStatsManager(tableManager, tx)
	indexManager := metadata.NewIndexManager(true, tableManager, statsManager, tx)

	// Create test table schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("age")
	schema.AddStringField("department", 15)
	schema.AddBoolField("active")

	// Create table using metadata manager
	err = tableManager.CreateTable("Students", schema, tx)
	require.NoError(t, err)

	// Get table layout and create table scan
	layout, err := tableManager.GetLayout("Students", tx)
	require.NoError(t, err)
	ts, err := table.NewTableScan(tx, layout, "Students")
	require.NoError(t, err)

	// Insert test data
	students := []struct {
		id         int
		name       string
		age        int
		department string
		active     bool
	}{
		{1, "Alice", 20, "CS", true},
		{2, "Bob", 22, "Math", false},
		{3, "Charlie", 20, "CS", true},
		{4, "David", 21, "Physics", true},
		{5, "Eve", 20, "Math", false},
		{6, "Frank", 23, "CS", true},
		{7, "Grace", 22, "Physics", true},
		{8, "Henry", 20, "CS", false},
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)
	for _, student := range students {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", student.id)
		require.NoError(t, err)
		err = ts.SetString("name", student.name)
		require.NoError(t, err)
		err = ts.SetInt("age", student.age)
		require.NoError(t, err)
		err = ts.SetString("department", student.department)
		require.NoError(t, err)
		err = ts.SetBool("active", student.active)
		require.NoError(t, err)
	}

	// Create index on age field using metadata manager
	indexName := "age_index"
	err = indexManager.CreateIndex(indexName, "Students", "age", tx)
	require.NoError(t, err)

	// Create hash index layout (block, id, dataval)
	indexSchema := record.NewSchema()
	indexSchema.AddIntField("block")
	indexSchema.AddIntField("id")
	indexSchema.AddIntField("dataval")
	indexLayout := record.NewLayoutFromSchema(indexSchema)

	// Create hash index
	hashIndex, err := index.NewBTreeIndex(tx, indexName, indexLayout)
	require.NoError(t, err)

	// Populate the index with data from the table
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		age, err := ts.GetInt("age")
		require.NoError(t, err)
		rid, err := ts.GetRID()
		require.NoError(t, err)
		err = hashIndex.Insert(age, rid)
		require.NoError(t, err)
	}

	return tx, ts, hashIndex, tableManager, indexManager
}

// TestIndexSelectScanBasicFunctionality tests basic IndexSelectScan operations
func TestIndexSelectScanBasicFunctionality(t *testing.T) {
	testDir := "/tmp/testdb_indexselectscan_basic"
	defer os.RemoveAll(testDir)

	tx, ts, hashIndex, _, _ := setupIndexSelectScanTest(t, testDir)
	defer tx.Commit()

	t.Run("FilterByAge20", func(t *testing.T) {
		// Reset table scan
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Create IndexSelectScan for age = 20
		indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 20)
		require.NoError(t, err)
		require.NotNil(t, indexSelectScan)

		// Collect results
		var results []struct {
			id         int
			name       string
			age        int
			department string
		}

		for {
			hasNext, err := indexSelectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := indexSelectScan.GetInt("id")
			require.NoError(t, err)
			name, err := indexSelectScan.GetString("name")
			require.NoError(t, err)
			age, err := indexSelectScan.GetInt("age")
			require.NoError(t, err)
			department, err := indexSelectScan.GetString("department")
			require.NoError(t, err)

			results = append(results, struct {
				id         int
				name       string
				age        int
				department string
			}{id, name, age, department})
		}

		// Verify results - should find 4 students with age=20 (Alice, Charlie, Eve, Henry)
		require.Len(t, results, 4, "Should find 4 students with age=20")
		for _, r := range results {
			assert.Equal(t, 20, r.age, "All results should have age=20")
		}

		// Verify specific students
		expectedNames := map[string]bool{"Alice": false, "Charlie": false, "Eve": false, "Henry": false}
		for _, r := range results {
			if _, exists := expectedNames[r.name]; exists {
				expectedNames[r.name] = true
			}
		}
		for name, found := range expectedNames {
			assert.True(t, found, "Student %s should be found", name)
		}

		indexSelectScan.Close()
	})

	t.Run("FilterByAge22", func(t *testing.T) {
		// Reset table scan
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Create IndexSelectScan for age = 22
		indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 22)
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := indexSelectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			age, err := indexSelectScan.GetInt("age")
			require.NoError(t, err)
			assert.Equal(t, 22, age)
			count++
		}

		// Should find 2 students with age=22 (Bob, Grace)
		assert.Equal(t, 2, count, "Should find 2 students with age=22")
		indexSelectScan.Close()
	})

	t.Run("FilterByNonExistentAge", func(t *testing.T) {
		// Reset table scan
		err := ts.BeforeFirst()
		require.NoError(t, err)

		// Create IndexSelectScan for age = 99 (doesn't exist)
		indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 99)
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := indexSelectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 0, count, "Should find no students with age=99")
		indexSelectScan.Close()
	})
}

// TestIndexSelectScanFieldAccess tests field access methods
func TestIndexSelectScanFieldAccess(t *testing.T) {
	testDir := "/tmp/testdb_indexselectscan_fields"
	defer os.RemoveAll(testDir)

	tx, ts, hashIndex, _, _ := setupIndexSelectScanTest(t, testDir)
	defer tx.Commit()

	err := ts.BeforeFirst()
	require.NoError(t, err)
	indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 21)
	require.NoError(t, err)

	t.Run("HasField", func(t *testing.T) {
		// Should have all table fields
		assert.True(t, indexSelectScan.HasField("id"))
		assert.True(t, indexSelectScan.HasField("name"))
		assert.True(t, indexSelectScan.HasField("age"))
		assert.True(t, indexSelectScan.HasField("department"))
		assert.True(t, indexSelectScan.HasField("active"))

		// Should not have non-existent field
		assert.False(t, indexSelectScan.HasField("missing"))
	})

	t.Run("GetIntAndGetString", func(t *testing.T) {
		hasNext, err := indexSelectScan.Next()
		require.NoError(t, err)
		if hasNext {
			// Test GetInt
			id, err := indexSelectScan.GetInt("id")
			require.NoError(t, err)
			assert.Greater(t, id, 0)

			age, err := indexSelectScan.GetInt("age")
			require.NoError(t, err)
			assert.Equal(t, 21, age)

			// Test GetString
			name, err := indexSelectScan.GetString("name")
			require.NoError(t, err)
			assert.NotEmpty(t, name)
			department, err := indexSelectScan.GetString("department")
			require.NoError(t, err)
			assert.NotEmpty(t, department)

			// Test GetBool
			active, err := indexSelectScan.GetBool("active")
			require.NoError(t, err)
			assert.IsType(t, true, active)
		}
	})

	t.Run("GetValue", func(t *testing.T) {
		// Reset to first record
		err := indexSelectScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := indexSelectScan.Next()
		require.NoError(t, err)
		if hasNext {
			// GetValue for int field
			idVal, err := indexSelectScan.GetValue("id")
			require.NoError(t, err)
			require.NotNil(t, idVal)
			idInt, ok := idVal.(int)
			require.True(t, ok)
			assert.Greater(t, idInt, 0)

			// GetValue for string field
			nameVal, err := indexSelectScan.GetValue("name")
			require.NoError(t, err)
			require.NotNil(t, nameVal)
			nameStr, ok := nameVal.(string)
			require.True(t, ok)
			assert.NotEmpty(t, nameStr)

			// GetValue for bool field
			activeVal, err := indexSelectScan.GetValue("active")
			require.NoError(t, err)
			require.NotNil(t, activeVal)
			activeBool, ok := activeVal.(bool)
			require.True(t, ok)
			assert.IsType(t, true, activeBool)
		}
	})

	indexSelectScan.Close()
}

// TestIndexSelectScanWithEmptyTable tests behavior with empty table
func TestIndexSelectScanWithEmptyTable(t *testing.T) {
	testDir := "/tmp/testdb_indexselectscan_empty"
	defer os.RemoveAll(testDir)

	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	require.NotNil(t, tx)
	defer tx.Commit()

	// Create metadata managers
	tableManager := metadata.NewTableManager(true, tx)
	statsManager := metadata.NewStatsManager(tableManager, tx)
	indexManager := metadata.NewIndexManager(true, tableManager, statsManager, tx)

	// Create empty table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("value")

	err = tableManager.CreateTable("EmptyTable", schema, tx)
	require.NoError(t, err)

	layout, err := tableManager.GetLayout("EmptyTable", tx)
	require.NoError(t, err)
	ts, err := table.NewTableScan(tx, layout, "EmptyTable")
	require.NoError(t, err)

	// Create index
	indexName := "value_index"
	err = indexManager.CreateIndex(indexName, "EmptyTable", "value", tx)
	require.NoError(t, err)

	indexSchema := record.NewSchema()
	indexSchema.AddIntField("block")
	indexSchema.AddIntField("id")
	indexSchema.AddIntField("dataval")
	indexLayout := record.NewLayoutFromSchema(indexSchema)

	hashIndex, err := index.NewBTreeIndex(tx, indexName, indexLayout)
	require.NoError(t, err)

	// Test IndexSelectScan on empty table
	err = ts.BeforeFirst()
	require.NoError(t, err)
	indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 42)
	require.NoError(t, err)

	count := 0
	for {
		hasNext, err := indexSelectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}

	assert.Equal(t, 0, count, "Empty table should return no records")
	indexSelectScan.Close()
}

// TestIndexSelectScanCombinedWithOtherScans tests combining IndexSelectScan with other scan types
func TestIndexSelectScanCombinedWithOtherScans(t *testing.T) {
	testDir := "/tmp/testdb_indexselectscan_combined"
	defer os.RemoveAll(testDir)

	tx, ts, hashIndex, _, _ := setupIndexSelectScanTest(t, testDir)
	defer tx.Commit()

	t.Run("IndexSelectScanWithSelectScan", func(t *testing.T) {
		// First use IndexSelectScan to filter by age = 20
		err := ts.BeforeFirst()
		require.NoError(t, err)
		indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 20)
		require.NoError(t, err)

		// Then use SelectScan to further filter by department = "CS"
		predicate := createEqualsPredicate("department", "CS")
		selectScan := NewSelectScan(indexSelectScan, *predicate)

		count := 0
		for {
			hasNext, err := selectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			age, err := selectScan.GetInt("age")
			require.NoError(t, err)
			department, err := selectScan.GetString("department")
			require.NoError(t, err)

			assert.Equal(t, 20, age)
			assert.Equal(t, "CS", department)
			count++
		}

		// Should find 3 students: Alice, Charlie, Henry (all age=20 and department=CS)
		assert.Equal(t, 3, count, "Should find 3 CS students with age=20")
		selectScan.Close()
	})

	t.Run("IndexSelectScanWithProjectScan", func(t *testing.T) {
		// Use IndexSelectScan to filter by age = 22
		err := ts.BeforeFirst()
		require.NoError(t, err)
		indexSelectScan, err := NewIndexSelectScan(ts, hashIndex, 22)
		require.NoError(t, err)

		// Then use ProjectScan to project only name and department
		fieldList := []string{"name", "department"}
		projectScan := NewProjectScan(indexSelectScan, fieldList)

		var results []struct {
			name       string
			department string
		}

		for {
			hasNext, err := projectScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			name, err := projectScan.GetString("name")
			require.NoError(t, err)
			department, err := projectScan.GetString("department")
			require.NoError(t, err)

			results = append(results, struct {
				name       string
				department string
			}{name, department})
		}

		// Should find 2 students with age=22: Bob (Math), Grace (Physics)
		require.Len(t, results, 2)
		expectedResults := map[string]string{"Bob": "Math", "Grace": "Physics"}
		for _, r := range results {
			expectedDept, exists := expectedResults[r.name]
			assert.True(t, exists, "Student %s should be expected", r.name)
			assert.Equal(t, expectedDept, r.department, "Department should match for %s", r.name)
		}

		projectScan.Close()
	})
}
