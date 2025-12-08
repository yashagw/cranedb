package query

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/temptable"
	"github.com/yashagw/cranedb/internal/transaction"
)

func setupSortScanTestDB(t *testing.T) (string, *transaction.Transaction, func()) {
	tempDir, err := os.MkdirTemp("", "sort_scan_test_*")
	require.NoError(t, err)

	dbPath := filepath.Join(tempDir, "testdb")

	fm, err := file.NewManager(dbPath, 400)
	require.NoError(t, err)
	lm, err := log.NewManager(fm, "testlog")
	require.NoError(t, err)
	bm, err := buffer.NewManager(fm, lm, 20)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := transaction.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()

	tx := transaction.NewTransaction(fm, lm, bm, lockTable, dirtyPageTable, transactionTable)

	cleanup := func() {
		tx.Commit()
		os.RemoveAll(tempDir)
	}

	return dbPath, tx, cleanup
}

func createTempTableWithData(t *testing.T, tx *transaction.Transaction, sch *record.Schema, data []map[string]interface{}) *temptable.TempTable {
	temp := temptable.NewTempTable(tx, sch)
	scan, err := temp.Open()
	require.NoError(t, err)

	for _, record := range data {
		err = scan.Insert()
		require.NoError(t, err)
		for fldname, val := range record {
			switch v := val.(type) {
			case int:
				err = scan.SetInt(fldname, v)
			case string:
				err = scan.SetString(fldname, v)
			case bool:
				err = scan.SetBool(fldname, v)
			}
			require.NoError(t, err)
		}
	}
	scan.Close()
	return temp
}

func TestSortScan(t *testing.T) {
	t.Run("NoRuns", func(t *testing.T) {
		_, _, cleanup := setupSortScanTestDB(t)
		defer cleanup()

		comp := NewRecordComparator([]string{"id"})
		runs := []*temptable.TempTable{}
		_, err := NewSortScan(runs, comp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one run is required")
	})

	t.Run("SingleRun", func(t *testing.T) {
		_, tx, cleanup := setupSortScanTestDB(t)
		defer cleanup()

		// Create schema
		schema := record.NewSchema()
		schema.AddIntField("id")
		schema.AddStringField("name", 20)
		schema.AddBoolField("active")

		// Create a single sorted run
		data := []map[string]interface{}{
			{"id": 1, "name": "Alice", "active": true},
			{"id": 2, "name": "Bob", "active": false},
			{"id": 3, "name": "Charlie", "active": true},
		}
		temp := createTempTableWithData(t, tx, schema, data)

		// Create SortScan with single run
		comp := NewRecordComparator([]string{"id"})
		runs := []*temptable.TempTable{temp}
		sortScan, err := NewSortScan(runs, comp)
		require.NoError(t, err)
		require.NotNil(t, sortScan)

		// Test reading records
		err = sortScan.BeforeFirst()
		require.NoError(t, err)

		expectedIds := []int{1, 2, 3}
		idx := 0
		for {
			hasNext, err := sortScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := sortScan.GetInt("id")
			require.NoError(t, err)
			assert.Equal(t, expectedIds[idx], id)
			active, err := sortScan.GetBool("active")
			require.NoError(t, err)
			assert.IsType(t, true, active)
			idx++
		}
		assert.Equal(t, len(expectedIds), idx)

		sortScan.Close()
	})

	t.Run("TwoRuns", func(t *testing.T) {
		_, tx, cleanup := setupSortScanTestDB(t)
		defer cleanup()

		// Create schema
		schema := record.NewSchema()
		schema.AddIntField("id")
		schema.AddStringField("name", 20)
		schema.AddBoolField("active")

		// Create two sorted runs
		run1Data := []map[string]interface{}{
			{"id": 1, "name": "Alice", "active": true},
			{"id": 3, "name": "Charlie", "active": true},
			{"id": 5, "name": "Eve", "active": false},
		}
		run2Data := []map[string]interface{}{
			{"id": 2, "name": "Bob", "active": false},
			{"id": 4, "name": "David", "active": true},
			{"id": 6, "name": "Frank", "active": true},
		}

		run1 := createTempTableWithData(t, tx, schema, run1Data)
		run2 := createTempTableWithData(t, tx, schema, run2Data)

		// Create SortScan with two runs
		comp := NewRecordComparator([]string{"id"})
		runs := []*temptable.TempTable{run1, run2}
		sortScan, err := NewSortScan(runs, comp)
		require.NoError(t, err)
		require.NotNil(t, sortScan)

		// Test reading merged records
		err = sortScan.BeforeFirst()
		require.NoError(t, err)

		expectedIds := []int{1, 2, 3, 4, 5, 6}
		idx := 0
		for {
			hasNext, err := sortScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := sortScan.GetInt("id")
			require.NoError(t, err)
			assert.Equal(t, expectedIds[idx], id, "Records should be merged in sorted order")
			active, err := sortScan.GetBool("active")
			require.NoError(t, err)
			assert.IsType(t, true, active)
			idx++
		}
		assert.Equal(t, len(expectedIds), idx)

		sortScan.Close()
	})

	t.Run("EmptyRun", func(t *testing.T) {
		_, tx, cleanup := setupSortScanTestDB(t)
		defer cleanup()

		// Create schema
		schema := record.NewSchema()
		schema.AddIntField("id")

		// Create one run with data, one empty run
		run1Data := []map[string]interface{}{
			{"id": 1},
			{"id": 2},
		}
		run1 := createTempTableWithData(t, tx, schema, run1Data)
		run2 := createTempTableWithData(t, tx, schema, []map[string]interface{}{})

		// Create SortScan
		comp := NewRecordComparator([]string{"id"})
		runs := []*temptable.TempTable{run1, run2}
		sortScan, err := NewSortScan(runs, comp)
		require.NoError(t, err)
		require.NotNil(t, sortScan)

		// Test reading - should only get records from run1
		err = sortScan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := sortScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}
		assert.Equal(t, 2, count)

		sortScan.Close()
	})

	t.Run("MultipleFields", func(t *testing.T) {
		_, tx, cleanup := setupSortScanTestDB(t)
		defer cleanup()

		// Create schema
		schema := record.NewSchema()
		schema.AddIntField("age")
		schema.AddStringField("name", 20)

		// Create two sorted runs (sorted by age, then name)
		run1Data := []map[string]interface{}{
			{"age": 25, "name": "Alice"},
			{"age": 25, "name": "Charlie"},
			{"age": 30, "name": "Bob"},
		}
		run2Data := []map[string]interface{}{
			{"age": 25, "name": "Bob"},
			{"age": 30, "name": "Alice"},
			{"age": 35, "name": "David"},
		}

		run1 := createTempTableWithData(t, tx, schema, run1Data)
		run2 := createTempTableWithData(t, tx, schema, run2Data)

		// Create SortScan with two runs, sorting by age then name
		comp := NewRecordComparator([]string{"age", "name"})
		runs := []*temptable.TempTable{run1, run2}
		sortScan, err := NewSortScan(runs, comp)
		require.NoError(t, err)
		require.NotNil(t, sortScan)

		// Test reading merged records
		err = sortScan.BeforeFirst()
		require.NoError(t, err)

		expectedRecords := []struct {
			age  int
			name string
		}{
			{25, "Alice"},
			{25, "Bob"},
			{25, "Charlie"},
			{30, "Alice"},
			{30, "Bob"},
			{35, "David"},
		}

		idx := 0
		for {
			hasNext, err := sortScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			age, err := sortScan.GetInt("age")
			require.NoError(t, err)
			name, err := sortScan.GetString("name")
			require.NoError(t, err)
			assert.Equal(t, expectedRecords[idx].age, age)
			assert.Equal(t, expectedRecords[idx].name, name)
			idx++
		}
		assert.Equal(t, len(expectedRecords), idx)

		sortScan.Close()
	})
}

func TestSortScanSaveRestorePosition(t *testing.T) {
	_, tx, cleanup := setupSortScanTestDB(t)
	defer cleanup()

	// Create schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	// Create two runs
	run1Data := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 3, "name": "Charlie"},
	}
	run2Data := []map[string]interface{}{
		{"id": 2, "name": "Bob"},
		{"id": 4, "name": "David"},
	}

	run1 := createTempTableWithData(t, tx, schema, run1Data)
	run2 := createTempTableWithData(t, tx, schema, run2Data)

	comp := NewRecordComparator([]string{"id"})
	runs := []*temptable.TempTable{run1, run2}
	sortScan, err := NewSortScan(runs, comp)
	require.NoError(t, err)

	err = sortScan.BeforeFirst()
	require.NoError(t, err)

	// Move to first record
	hasNext, err := sortScan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// Get current values
	id1, err := sortScan.GetInt("id")
	require.NoError(t, err)
	name1, err := sortScan.GetString("name")
	require.NoError(t, err)
	active1, err := sortScan.GetBool("active")
	require.NoError(t, err)

	// Save position
	err = sortScan.SavePosition()
	require.NoError(t, err)

	// Move forward
	hasNext, err = sortScan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	id2, err := sortScan.GetInt("id")
	require.NoError(t, err)
	assert.NotEqual(t, id1, id2)

	// Restore position
	err = sortScan.RestorePosition()
	require.NoError(t, err)

	// Should be back at first record
	id3, err := sortScan.GetInt("id")
	require.NoError(t, err)
	name3, err := sortScan.GetString("name")
	require.NoError(t, err)
	active3, err := sortScan.GetBool("active")
	require.NoError(t, err)
	assert.Equal(t, id1, id3)
	assert.Equal(t, name1, name3)
	assert.Equal(t, active1, active3)

	sortScan.Close()
}
