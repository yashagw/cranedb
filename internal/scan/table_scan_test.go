package scan

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

func TestTableScan(t *testing.T) {
	testDir := "/tmp/testdb_tablescan"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	assert.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	// Create schema with int and string fields
	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	layout := record.NewLayoutFromSchema(schema)
	require.NotNil(t, layout)

	for _, fieldName := range layout.GetSchema().Fields() {
		offset := layout.GetOffset(fieldName)
		t.Logf("%s has offset %d", fieldName, offset)
	}

	// Create TableScan
	ts, err := NewTableScan(tx, layout, "TestTable")
	require.NoError(t, err)
	require.NotNil(t, ts)
	assert.Equal(t, tx, ts.transaction)
	assert.Equal(t, layout, ts.layout)
	assert.Equal(t, "TestTable.tbl", ts.fileName)
	assert.NotNil(t, ts.currentRecordPage)
	assert.Equal(t, -1, ts.currentSlot)

	// Test 1: Fill the table with 50 random records
	st := map[int]map[int]int{}
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		n := (i * 7) % 50 // Simulate random values
		err = ts.SetInt("A", n)
		require.NoError(t, err)
		err = ts.SetString("B", "rec")
		require.NoError(t, err)
		rid, err := ts.GetRID()
		require.NoError(t, err)
		if _, ok := st[rid.Block()]; !ok {
			st[rid.Block()] = map[int]int{}
		}
		st[rid.Block()][rid.Slot()] = n
		t.Logf("inserting into block %d, slot %d: {%d, %s}", rid.Block(), rid.Slot(), n, "rec")
	}

	// Test 2: Verify we can iterate through all records
	err = ts.BeforeFirst()
	require.NoError(t, err)
	recordCount := 0
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		if ts.currentSlot != -1 {
			a, err := ts.GetInt("A")
			require.NoError(t, err)
			b, err := ts.GetString("B")
			require.NoError(t, err)
			rid, err := ts.GetRID()
			require.NoError(t, err)
			assert.Equal(t, st[rid.Block()][rid.Slot()], a)
			t.Logf("block %d, slot %d: {%d, %s}", rid.Block(), rid.Slot(), a, b)
			recordCount++
		}
	}
	assert.Equal(t, 50, recordCount)

	// Test 3: Delete records with A-values < 25
	t.Log("Deleting records with A-values < 25.")
	count := 0
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		if ts.currentSlot != -1 {
			a, err := ts.GetInt("A")
			require.NoError(t, err)
			b, err := ts.GetString("B")
			require.NoError(t, err)
			if a < 25 {
				count++
				rid, err := ts.GetRID()
				require.NoError(t, err)
				t.Logf("slot %d: {%d, %s}", rid.Slot(), a, b)
				err = ts.Delete()
				require.NoError(t, err)
			}
		}
	}
	t.Logf("%d values under 25 were deleted.", count)
	assert.Greater(t, count, 0)

	// Test 4: Verify remaining records
	t.Log("Here are the remaining records.")
	remainingCount := 0
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		if ts.currentSlot != -1 { // Only count valid records
			a, err := ts.GetInt("A")
			require.NoError(t, err)
			b, err := ts.GetString("B")
			require.NoError(t, err)
			rid, err := ts.GetRID()
			require.NoError(t, err)
			t.Logf("slot %d: {%d, %s}", rid.Slot(), a, b)
			assert.GreaterOrEqual(t, a, 25)
			remainingCount++
		}
	}
	t.Logf("Remaining records: %d", remainingCount)
	assert.Equal(t, 50-count, remainingCount)

	// Test 5: Test MoveToRID functionality
	t.Log("Testing MoveToRID functionality.")
	err = ts.BeforeFirst()
	require.NoError(t, err)
	hasNext, err := ts.Next()
	require.NoError(t, err)
	if hasNext && ts.currentSlot != -1 {
		rid, err := ts.GetRID()
		require.NoError(t, err)
		originalA, err := ts.GetInt("A")
		require.NoError(t, err)

		err = ts.BeforeFirst()
		require.NoError(t, err)
		hasNext, err = ts.Next()
		require.NoError(t, err)
		if hasNext {
			hasNext, err = ts.Next()
			require.NoError(t, err)
		}
		if hasNext && ts.currentSlot != -1 {
			err = ts.MoveToRID(rid)
			require.NoError(t, err)
			assert.Equal(t, rid.Block(), ts.currentRecordPage.Block().Number())
			assert.Equal(t, rid.Slot(), ts.currentSlot)
			checkA, err := ts.GetInt("A")
			require.NoError(t, err)
			assert.Equal(t, originalA, checkA)
		}
	}

	// Test 6: Test block operations
	t.Log("Testing block operations.")
	initialBlock := ts.currentRecordPage.Block().Number()

	// Test MoveToNewBlock
	err = ts.MoveToNewBlock()
	require.NoError(t, err)
	newBlock := ts.currentRecordPage.Block().Number()
	assert.Greater(t, newBlock, initialBlock)
	assert.Equal(t, -1, ts.currentSlot)

	// Test AtLastBlock
	atLast, err := ts.AtLastBlock()
	require.NoError(t, err)
	assert.True(t, atLast)

	// Test MoveToBlock
	err = ts.MoveToBlock(0)
	require.NoError(t, err)
	assert.Equal(t, 0, ts.currentRecordPage.Block().Number())
	atLast, err = ts.AtLastBlock()
	require.NoError(t, err)
	assert.False(t, atLast)

	// Test 7: Test BeforeFirst
	err = ts.MoveToBlock(1)
	require.NoError(t, err)
	assert.Equal(t, 1, ts.currentRecordPage.Block().Number())
	err = ts.BeforeFirst()
	require.NoError(t, err)
	assert.Equal(t, 0, ts.currentRecordPage.Block().Number())
	assert.Equal(t, -1, ts.currentSlot)

	// Test 8: Test Get/Set operations
	t.Log("Testing Get/Set operations.")
	err = ts.BeforeFirst()
	require.NoError(t, err)
	hasNext, err = ts.Next()
	require.NoError(t, err)
	if hasNext && ts.currentSlot != -1 {
		// Test setting and getting values
		err = ts.SetInt("A", 999)
		require.NoError(t, err)
		err = ts.SetString("B", "updated")
		require.NoError(t, err)
		checkA, err := ts.GetInt("A")
		require.NoError(t, err)
		checkB, err := ts.GetString("B")
		require.NoError(t, err)
		assert.Equal(t, 999, checkA)
		assert.Equal(t, "updated", checkB)
	}

	// Test 9: Test Close (should not panic)
	ts.Close()

	// Cleanup
	err = tx.Commit()
	require.NoError(t, err)
}
