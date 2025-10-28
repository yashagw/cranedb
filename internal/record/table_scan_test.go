package record

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestTableScan(t *testing.T) {
	testDir := "/tmp/testdb_tablescan"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	assert.NoError(t, err)
	bufferManager := buffer.NewManager(fileManager, logManager, 10)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	// Create schema with int and string fields
	schema := NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	layout := NewLayoutFromSchema(schema)
	require.NotNil(t, layout)

	for _, fieldName := range layout.schema.Fields() {
		offset := layout.GetOffset(fieldName)
		t.Logf("%s has offset %d", fieldName, offset)
	}

	// Create TableScan
	ts := NewTableScan(tx, layout, "TestTable")
	require.NotNil(t, ts)
	assert.Equal(t, tx, ts.transaction)
	assert.Equal(t, layout, ts.layout)
	assert.Equal(t, "TestTable.tbl", ts.fileName)
	assert.NotNil(t, ts.currentRecordPage)
	assert.Equal(t, -1, ts.currentSlot)

	// Test 1: Fill the table with 50 random records
	st := map[int]map[int]int{}
	ts.BeforeFirst()
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := (i * 7) % 50 // Simulate random values
		ts.SetInt("A", n)
		ts.SetString("B", "rec")
		rid := ts.GetRID()
		if _, ok := st[rid.Block()]; !ok {
			st[rid.Block()] = map[int]int{}
		}
		st[rid.Block()][rid.Slot()] = n
		t.Logf("inserting into block %d, slot %d: {%d, %s}", rid.Block(), rid.Slot(), n, "rec")
	}

	// Test 2: Verify we can iterate through all records
	ts.BeforeFirst()
	recordCount := 0
	for ts.Next() {
		if ts.currentSlot != -1 {
			a := ts.GetInt("A")
			b := ts.GetString("B")
			rid := ts.GetRID()
			assert.Equal(t, st[rid.Block()][rid.Slot()], a)
			t.Logf("block %d, slot %d: {%d, %s}", rid.Block(), rid.Slot(), a, b)
			recordCount++
		}
	}
	assert.Equal(t, 50, recordCount)

	// Test 3: Delete records with A-values < 25
	t.Log("Deleting records with A-values < 25.")
	count := 0
	ts.BeforeFirst()
	for ts.Next() {
		if ts.currentSlot != -1 {
			a := ts.GetInt("A")
			b := ts.GetString("B")
			if a < 25 {
				count++
				rid := ts.GetRID()
				t.Logf("slot %d: {%d, %s}", rid.Slot(), a, b)
				ts.Delete()
			}
		}
	}
	t.Logf("%d values under 25 were deleted.", count)
	assert.Greater(t, count, 0)

	// Test 4: Verify remaining records
	t.Log("Here are the remaining records.")
	remainingCount := 0
	ts.BeforeFirst()
	for ts.Next() {
		if ts.currentSlot != -1 { // Only count valid records
			a := ts.GetInt("A")
			b := ts.GetString("B")
			rid := ts.GetRID()
			t.Logf("slot %d: {%d, %s}", rid.Slot(), a, b)
			assert.GreaterOrEqual(t, a, 25)
			remainingCount++
		}
	}
	t.Logf("Remaining records: %d", remainingCount)
	assert.Equal(t, 50-count, remainingCount)

	// Test 5: Test MoveToRID functionality
	t.Log("Testing MoveToRID functionality.")
	ts.BeforeFirst()
	ts.Next()
	if ts.currentSlot != -1 {
		rid := ts.GetRID()
		originalA := ts.GetInt("A")

		ts.BeforeFirst()
		ts.Next()
		ts.Next()
		if ts.currentSlot != -1 {
			ts.MoveToRID(rid)
			assert.Equal(t, rid.Block(), ts.currentRecordPage.Block().Number())
			assert.Equal(t, rid.Slot(), ts.currentSlot)
			assert.Equal(t, originalA, ts.GetInt("A"))
		}
	}

	// Test 6: Test block operations
	t.Log("Testing block operations.")
	initialBlock := ts.currentRecordPage.Block().Number()

	// Test MoveToNewBlock
	ts.MoveToNewBlock()
	newBlock := ts.currentRecordPage.Block().Number()
	assert.Greater(t, newBlock, initialBlock)
	assert.Equal(t, -1, ts.currentSlot)

	// Test AtLastBlock
	assert.True(t, ts.AtLastBlock())

	// Test MoveToBlock
	ts.MoveToBlock(0)
	assert.Equal(t, 0, ts.currentRecordPage.Block().Number())
	assert.False(t, ts.AtLastBlock())

	// Test 7: Test BeforeFirst
	ts.MoveToBlock(1)
	assert.Equal(t, 1, ts.currentRecordPage.Block().Number())
	ts.BeforeFirst()
	assert.Equal(t, 0, ts.currentRecordPage.Block().Number())
	assert.Equal(t, -1, ts.currentSlot)

	// Test 8: Test Get/Set operations
	t.Log("Testing Get/Set operations.")
	ts.BeforeFirst()
	ts.Next()
	if ts.currentSlot != -1 {
		// Test setting and getting values
		ts.SetInt("A", 999)
		ts.SetString("B", "updated")
		assert.Equal(t, 999, ts.GetInt("A"))
		assert.Equal(t, "updated", ts.GetString("B"))
	}

	// Test 9: Test Close (should not panic)
	ts.Close()

	// Cleanup
	tx.Commit()
}
