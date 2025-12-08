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

// setupIndexJoinScanTest creates two tables for index join testing.
// Table1 (Students): id, name, dept_id
// Table2 (Departments): dept_id, dept_name
// Index on Departments.dept_id
func setupIndexJoinScanTest(t *testing.T, testDir string) (*transaction.Transaction, *table.TableScan, *table.TableScan, index.Index, *metadata.TableManager, *metadata.IndexManager) {
	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := transaction.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	require.NotNil(t, tx)

	// Create metadata managers
	tableManager := metadata.NewTableManager(true, tx)
	statsManager := metadata.NewStatsManager(tableManager, tx)
	indexManager := metadata.NewIndexManager(true, tableManager, statsManager, tx)

	// Create Students table schema
	studentsSchema := record.NewSchema()
	studentsSchema.AddIntField("id")
	studentsSchema.AddStringField("name", 20)
	studentsSchema.AddIntField("dept_id")
	studentsSchema.AddBoolField("active")

	// Create Departments table schema
	deptSchema := record.NewSchema()
	deptSchema.AddIntField("dept_id")
	deptSchema.AddStringField("dept_name", 20)

	// Create tables
	err = tableManager.CreateTable("Students", studentsSchema, tx)
	require.NoError(t, err)
	err = tableManager.CreateTable("Departments", deptSchema, tx)
	require.NoError(t, err)

	// Get layouts
	studentsLayout, err := tableManager.GetLayout("Students", tx)
	require.NoError(t, err)
	deptLayout, err := tableManager.GetLayout("Departments", tx)
	require.NoError(t, err)

	// Create table scans
	studentsTS, err := table.NewTableScan(tx, studentsLayout, "Students")
	require.NoError(t, err)
	deptTS, err := table.NewTableScan(tx, deptLayout, "Departments")
	require.NoError(t, err)

	// Insert departments first
	departments := []struct {
		deptID   int
		deptName string
	}{
		{1, "CS"},
		{2, "Math"},
		{3, "Physics"},
	}

	err = deptTS.BeforeFirst()
	require.NoError(t, err)
	for _, dept := range departments {
		err = deptTS.Insert()
		require.NoError(t, err)
		err = deptTS.SetInt("dept_id", dept.deptID)
		require.NoError(t, err)
		err = deptTS.SetString("dept_name", dept.deptName)
		require.NoError(t, err)
	}

	// Insert students
	students := []struct {
		id     int
		name   string
		deptID int
		active bool
	}{
		{1, "Alice", 1, true},   // CS
		{2, "Bob", 2, false},    // Math
		{3, "Charlie", 1, true}, // CS
		{4, "David", 3, true},   // Physics
		{5, "Eve", 2, false},    // Math
	}

	err = studentsTS.BeforeFirst()
	require.NoError(t, err)
	for _, student := range students {
		err = studentsTS.Insert()
		require.NoError(t, err)
		err = studentsTS.SetInt("id", student.id)
		require.NoError(t, err)
		err = studentsTS.SetString("name", student.name)
		require.NoError(t, err)
		err = studentsTS.SetInt("dept_id", student.deptID)
		require.NoError(t, err)
		err = studentsTS.SetBool("active", student.active)
		require.NoError(t, err)
	}

	// Create index on Departments.dept_id
	indexName := "dept_id_index"
	err = indexManager.CreateIndex(indexName, "Departments", "dept_id", tx)
	require.NoError(t, err)

	// Create index layout
	indexSchema := record.NewSchema()
	indexSchema.AddIntField("block")
	indexSchema.AddIntField("id")
	indexSchema.AddIntField("dataval")
	indexLayout := record.NewLayoutFromSchema(indexSchema)

	// Create and populate index
	btreeIndex, err := index.NewBTreeIndex(tx, indexName, indexLayout)
	require.NoError(t, err)

	// Populate index with department data
	err = deptTS.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := deptTS.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		deptID, err := deptTS.GetInt("dept_id")
		require.NoError(t, err)
		rid, err := deptTS.GetRID()
		require.NoError(t, err)
		err = btreeIndex.Insert(deptID, rid)
		require.NoError(t, err)
	}

	return tx, studentsTS, deptTS, btreeIndex, tableManager, indexManager
}

// TestIndexJoinScanBasicJoin tests basic index join functionality
func TestIndexJoinScanBasicJoin(t *testing.T) {
	testDir := "/tmp/testdb_indexjoinscan_basic"
	defer os.RemoveAll(testDir)

	tx, studentsTS, deptTS, btreeIndex, _, _ := setupIndexJoinScanTest(t, testDir)
	defer tx.Commit()

	// Create index join scan: join Students with Departments on dept_id
	err := studentsTS.BeforeFirst()
	require.NoError(t, err)
	err = deptTS.BeforeFirst()
	require.NoError(t, err)

	indexJoinScan, err := NewIndexJoinScan(studentsTS, btreeIndex, "dept_id", deptTS)
	require.NoError(t, err)
	require.NotNil(t, indexJoinScan)

	// Collect join results
	type JoinResult struct {
		studentID   int
		studentName string
		deptID      int
		deptName    string
	}
	var results []JoinResult

	for {
		hasNext, err := indexJoinScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		studentID, err := indexJoinScan.GetInt("id")
		require.NoError(t, err)
		studentName, err := indexJoinScan.GetString("name")
		require.NoError(t, err)
		deptID, err := indexJoinScan.GetInt("dept_id")
		require.NoError(t, err)
		deptName, err := indexJoinScan.GetString("dept_name")
		require.NoError(t, err)

		results = append(results, JoinResult{
			studentID:   studentID,
			studentName: studentName,
			deptID:      deptID,
			deptName:    deptName,
		})
		t.Logf("Join result: %s (id=%d) -> %s (dept_id=%d)", studentName, studentID, deptName, deptID)
	}

	// Verify join results
	// Expected: 5 students joined with their departments
	require.Len(t, results, 5, "Should have 5 join results")

	// Verify specific joins
	expectedJoins := map[string]string{
		"Alice":   "CS",
		"Bob":     "Math",
		"Charlie": "CS",
		"David":   "Physics",
		"Eve":     "Math",
	}

	for _, r := range results {
		expectedDept, exists := expectedJoins[r.studentName]
		require.True(t, exists, "Student %s should exist", r.studentName)
		assert.Equal(t, expectedDept, r.deptName, "Student %s should be in department %s", r.studentName, expectedDept)
	}

	indexJoinScan.Close()
}

// TestIndexJoinScanFieldAccess tests field access from both sides of the join
func TestIndexJoinScanFieldAccess(t *testing.T) {
	testDir := "/tmp/testdb_indexjoinscan_fields"
	defer os.RemoveAll(testDir)

	tx, studentsTS, deptTS, btreeIndex, _, _ := setupIndexJoinScanTest(t, testDir)
	defer tx.Commit()

	err := studentsTS.BeforeFirst()
	require.NoError(t, err)
	err = deptTS.BeforeFirst()
	require.NoError(t, err)

	indexJoinScan, err := NewIndexJoinScan(studentsTS, btreeIndex, "dept_id", deptTS)
	require.NoError(t, err)

	t.Run("HasField", func(t *testing.T) {
		// Fields from lhs (Students)
		assert.True(t, indexJoinScan.HasField("id"))
		assert.True(t, indexJoinScan.HasField("name"))
		assert.True(t, indexJoinScan.HasField("dept_id"))
		assert.True(t, indexJoinScan.HasField("active"))

		// Fields from rhs (Departments)
		assert.True(t, indexJoinScan.HasField("dept_name"))

		// Non-existent field
		assert.False(t, indexJoinScan.HasField("missing"))
	})

	t.Run("GetIntFromBothSides", func(t *testing.T) {
		hasNext, err := indexJoinScan.Next()
		require.NoError(t, err)
		if hasNext {
			// Get int from lhs
			studentID, err := indexJoinScan.GetInt("id")
			require.NoError(t, err)
			assert.Greater(t, studentID, 0)

			// Get int from rhs (dept_id exists in both, but we check rhs first)
			// Actually, dept_id is in both, so it will get from rhs
			deptID, err := indexJoinScan.GetInt("dept_id")
			require.NoError(t, err)
			assert.Greater(t, deptID, 0)
		}
	})

	t.Run("GetStringFromBothSides", func(t *testing.T) {
		// Reset to first
		err := indexJoinScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := indexJoinScan.Next()
		require.NoError(t, err)
		if hasNext {
			// Get string from lhs
			studentName, err := indexJoinScan.GetString("name")
			require.NoError(t, err)
			assert.NotEmpty(t, studentName)

			// Get string from rhs
			deptName, err := indexJoinScan.GetString("dept_name")
			require.NoError(t, err)
			assert.NotEmpty(t, deptName)

			// Get bool from lhs
			active, err := indexJoinScan.GetBool("active")
			require.NoError(t, err)
			assert.IsType(t, true, active)
		}
	})

	t.Run("GetValue", func(t *testing.T) {
		err := indexJoinScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := indexJoinScan.Next()
		require.NoError(t, err)
		if hasNext {
			// GetValue from lhs
			studentIDVal, err := indexJoinScan.GetValue("id")
			require.NoError(t, err)
			require.NotNil(t, studentIDVal)

			// GetValue from rhs
			deptNameVal, err := indexJoinScan.GetValue("dept_name")
			require.NoError(t, err)
			require.NotNil(t, deptNameVal)
		}
	})

	indexJoinScan.Close()
}

// TestIndexJoinScanNavigation tests BeforeFirst and Next operations
func TestIndexJoinScanNavigation(t *testing.T) {
	testDir := "/tmp/testdb_indexjoinscan_navigation"
	defer os.RemoveAll(testDir)

	tx, studentsTS, deptTS, btreeIndex, _, _ := setupIndexJoinScanTest(t, testDir)
	defer tx.Commit()

	err := studentsTS.BeforeFirst()
	require.NoError(t, err)
	err = deptTS.BeforeFirst()
	require.NoError(t, err)

	indexJoinScan, err := NewIndexJoinScan(studentsTS, btreeIndex, "dept_id", deptTS)
	require.NoError(t, err)

	t.Run("ReIteration", func(t *testing.T) {
		// First iteration
		count1 := 0
		for {
			hasNext, err := indexJoinScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count1++
		}
		t.Logf("First iteration: %d records", count1)

		// Second iteration
		err := indexJoinScan.BeforeFirst()
		require.NoError(t, err)
		count2 := 0
		for {
			hasNext, err := indexJoinScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count2++
		}
		t.Logf("Second iteration: %d records", count2)

		assert.Equal(t, count1, count2, "Should be able to re-iterate")
		assert.Equal(t, 5, count1, "Should have 5 join results")
	})

	indexJoinScan.Close()
}

// TestIndexJoinScanEmptyLHS tests behavior with empty left-hand side
func TestIndexJoinScanEmptyLHS(t *testing.T) {
	testDir := "/tmp/testdb_indexjoinscan_empty_lhs"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := transaction.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	defer tx.Commit()

	// Create metadata managers
	tableManager := metadata.NewTableManager(true, tx)
	statsManager := metadata.NewStatsManager(tableManager, tx)
	indexManager := metadata.NewIndexManager(true, tableManager, statsManager, tx)

	// Create empty Students table
	studentsSchema := record.NewSchema()
	studentsSchema.AddIntField("id")
	studentsSchema.AddIntField("dept_id")
	err = tableManager.CreateTable("Students", studentsSchema, tx)
	require.NoError(t, err)

	// Create Departments table with one department
	deptSchema := record.NewSchema()
	deptSchema.AddIntField("dept_id")
	deptSchema.AddStringField("dept_name", 20)
	err = tableManager.CreateTable("Departments", deptSchema, tx)
	require.NoError(t, err)

	studentsLayout, err := tableManager.GetLayout("Students", tx)
	require.NoError(t, err)
	deptLayout, err := tableManager.GetLayout("Departments", tx)
	require.NoError(t, err)

	studentsTS, err := table.NewTableScan(tx, studentsLayout, "Students")
	require.NoError(t, err)
	deptTS, err := table.NewTableScan(tx, deptLayout, "Departments")
	require.NoError(t, err)

	// Insert one department
	err = deptTS.BeforeFirst()
	require.NoError(t, err)
	err = deptTS.Insert()
	require.NoError(t, err)
	err = deptTS.SetInt("dept_id", 1)
	require.NoError(t, err)
	err = deptTS.SetString("dept_name", "CS")
	require.NoError(t, err)

	// Create index
	indexName := "dept_id_index"
	err = indexManager.CreateIndex(indexName, "Departments", "dept_id", tx)
	require.NoError(t, err)

	indexSchema := record.NewSchema()
	indexSchema.AddIntField("block")
	indexSchema.AddIntField("id")
	indexSchema.AddIntField("dataval")
	indexLayout := record.NewLayoutFromSchema(indexSchema)

	btreeIndex, err := index.NewBTreeIndex(tx, indexName, indexLayout)
	require.NoError(t, err)

	// Populate index
	err = deptTS.BeforeFirst()
	require.NoError(t, err)
	hasNext, err := deptTS.Next()
	require.NoError(t, err)
	if hasNext {
		deptID, err := deptTS.GetInt("dept_id")
		require.NoError(t, err)
		rid, err := deptTS.GetRID()
		require.NoError(t, err)
		err = btreeIndex.Insert(deptID, rid)
		require.NoError(t, err)
	}

	// Create join scan with empty lhs
	err = studentsTS.BeforeFirst()
	require.NoError(t, err)
	err = deptTS.BeforeFirst()
	require.NoError(t, err)

	indexJoinScan, err := NewIndexJoinScan(studentsTS, btreeIndex, "dept_id", deptTS)
	require.NoError(t, err)

	// Should have no results
	count := 0
	for {
		hasNext, err := indexJoinScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}

	assert.Equal(t, 0, count, "Join with empty lhs should produce no results")
	indexJoinScan.Close()
}
