package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

func setupBTPageTest(t *testing.T) (*transaction.Transaction, *record.Layout, func()) {
	t.Helper()

	tempDir := t.TempDir()

	fileManager, err := file.NewManager(tempDir, 400)
	require.NoError(t, err)

	logManager, err := log.NewManager(fileManager, "btpage_test.log")
	require.NoError(t, err)

	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)

	lockTable := transaction.NewLockTable()
	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)

	// Create a layout for B-tree index records
	schema := record.NewSchema()
	schema.AddIntField("block")   // RID block number
	schema.AddIntField("id")      // RID slot number
	schema.AddIntField("dataval") // The indexed value
	layout := record.NewLayoutFromSchema(schema)

	cleanup := func() {
		if err := tx.Commit(); err != nil {
			t.Errorf("failed to commit transaction: %v", err)
		}
		fileManager.Close()
	}

	return tx, layout, cleanup
}

func TestBTPage_BasicOperations(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	// Create a new block for our B-tree page
	blk, err := tx.Append("btree_test.tbl")
	require.NoError(t, err)

	// Create a new BTPage
	btPage, err := NewBTPage(tx, blk, layout)
	require.NoError(t, err)
	defer btPage.Close()

	// Test 1: Format the page as a leaf node (flag = -1)
	err = btPage.Format(-1)
	require.NoError(t, err)

	// Test 2: Verify the flag was set correctly
	// Note: -1 is stored as 0xFFFFFFFF, which when read as uint32 and converted to int
	// becomes 4294967295 on 64-bit systems, but comparisons still work correctly
	flag, err := btPage.GetFlag()
	require.NoError(t, err)
	// Check that flag < 0 (which indicates regular leaf, no overflow)
	assert.Less(t, flag, 0, "Expected leaf node flag to be < 0 (regular leaf)")

	// Test 3: Verify initial record count is 0
	numRecs, err := btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 0, numRecs, "Expected 0 initial records")

	// Test 4: Update record count
	err = btPage.SetNumRecs(5)
	require.NoError(t, err)

	numRecs, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 5, numRecs, "Expected 5 records after update")

	// Test 5: Change to internal node
	err = btPage.SetFlag(1)
	require.NoError(t, err)

	flag, err = btPage.GetFlag()
	require.NoError(t, err)
	assert.Equal(t, 1, flag, "Expected internal node flag (1)")
}

func TestBTPage_FormatWithDifferentFlags(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	// Test leaf node formatting
	leafBlk, err := tx.Append("btree_leaf_test.tbl")
	require.NoError(t, err)

	leafPage, err := NewBTPage(tx, leafBlk, layout)
	require.NoError(t, err)
	defer leafPage.Close()

	err = leafPage.Format(-1) // -1 = regular leaf (no overflow)
	require.NoError(t, err)

	flag, err := leafPage.GetFlag()
	require.NoError(t, err)
	// Check that flag < 0 (which indicates regular leaf, no overflow)
	assert.Less(t, flag, 0, "Expected leaf node flag to be < 0")

	// Test internal node formatting
	internalBlk, err := tx.Append("btree_internal_test.tbl")
	require.NoError(t, err)

	internalPage, err := NewBTPage(tx, internalBlk, layout)
	require.NoError(t, err)
	defer internalPage.Close()

	err = internalPage.Format(1) // 1 = internal
	require.NoError(t, err)

	flag, err = internalPage.GetFlag()
	require.NoError(t, err)
	assert.Equal(t, 1, flag)
}

func TestBTPage_SlotPositionCalculation(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	blk, err := tx.Append("btree_slot_test.tbl")
	require.NoError(t, err)

	btPage, err := NewBTPage(tx, blk, layout)
	require.NoError(t, err)
	defer btPage.Close()

	// Test slot position calculations
	// Header is 8 bytes (flag + numRecords)
	// Each slot size depends on the layout
	slotSize := layout.GetSlotSize()

	// Test slot 0 position
	slot0Pos := btPage.GetSlotPosition(0)
	expectedSlot0 := 8 // Right after header
	assert.Equal(t, expectedSlot0, slot0Pos, "Slot 0 should start at byte 8")

	// Test slot 1 position
	slot1Pos := btPage.GetSlotPosition(1)
	expectedSlot1 := 8 + slotSize
	assert.Equal(t, expectedSlot1, slot1Pos, "Slot 1 should start at byte %d", expectedSlot1)

	// Test slot 2 position
	slot2Pos := btPage.GetSlotPosition(2)
	expectedSlot2 := 8 + (2 * slotSize)
	assert.Equal(t, expectedSlot2, slot2Pos, "Slot 2 should start at byte %d", expectedSlot2)

	// Test field position calculations
	// Test "block" field in slot 0
	blockFieldPos := btPage.GetFieldPosition(0, "block")
	expectedBlockPos := slot0Pos + layout.GetOffset("block")
	assert.Equal(t, expectedBlockPos, blockFieldPos, "Block field in slot 0 should be at correct position")

	// Test "dataval" field in slot 1
	datavalFieldPos := btPage.GetFieldPosition(1, "dataval")
	expectedDatavalPos := slot1Pos + layout.GetOffset("dataval")
	assert.Equal(t, expectedDatavalPos, datavalFieldPos, "Dataval field in slot 1 should be at correct position")

	// Verify the positions are different and increasing
	assert.Less(t, slot0Pos, slot1Pos, "Slot positions should increase")
	assert.Less(t, slot1Pos, slot2Pos, "Slot positions should increase")
}

func TestBTPage_FieldAccess(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	blk, err := tx.Append("btree_field_test.tbl")
	require.NoError(t, err)

	btPage, err := NewBTPage(tx, blk, layout)
	require.NoError(t, err)
	defer btPage.Close()

	// Format the page first
	err = btPage.Format(-1)
	require.NoError(t, err)

	// Test 1: Basic integer field access
	err = btPage.SetInt(0, "block", 42)
	require.NoError(t, err)

	blockVal, err := btPage.GetInt(0, "block")
	require.NoError(t, err)
	assert.Equal(t, 42, blockVal, "Should read back the same integer value")

	// Test 2: Different slots
	err = btPage.SetInt(1, "id", 99)
	require.NoError(t, err)

	idVal, err := btPage.GetInt(1, "id")
	require.NoError(t, err)
	assert.Equal(t, 99, idVal, "Should read back integer from different slot")

	// Test 3: Different fields in same slot
	err = btPage.SetInt(0, "dataval", 123)
	require.NoError(t, err)

	dataVal, err := btPage.GetInt(0, "dataval")
	require.NoError(t, err)
	assert.Equal(t, 123, dataVal, "Should read back integer from different field")

	// Verify that slot 0 "block" value is still intact
	blockVal, err = btPage.GetInt(0, "block")
	require.NoError(t, err)
	assert.Equal(t, 42, blockVal, "Original value should be preserved")

	// Test 4: Generic SetVal/GetVal with integers
	err = btPage.SetVal(0, "block", 100)
	require.NoError(t, err)

	val, err := btPage.GetVal(0, "block")
	require.NoError(t, err)
	assert.Equal(t, 100, val, "Should read back integer using generic methods")

	// Test 5: Multiple slots and fields with generic methods
	testData := []struct {
		slot     int
		field    string
		value    any
		expected any
	}{
		{0, "block", 10, 10},
		{0, "id", 20, 20},
		{0, "dataval", 30, 30},
		{1, "block", 40, 40},
		{1, "id", 50, 50},
		{1, "dataval", 60, 60},
		{2, "block", 70, 70},
	}

	// Write all test data
	for _, td := range testData {
		err = btPage.SetVal(td.slot, td.field, td.value)
		require.NoError(t, err, "Failed to set value for slot %d, field %s", td.slot, td.field)
	}

	// Read back and verify all test data
	for _, td := range testData {
		val, err := btPage.GetVal(td.slot, td.field)
		require.NoError(t, err, "Failed to get value for slot %d, field %s", td.slot, td.field)
		assert.Equal(t, td.expected, val, "Mismatch for slot %d, field %s", td.slot, td.field)
	}

	// Test 6: Error handling - type mismatch
	err = btPage.SetVal(0, "block", "not an integer")
	assert.Error(t, err, "Should error when setting string to int field")
	assert.Contains(t, err.Error(), "expected int", "Error should mention type mismatch")

	// Test 7: Non-existent field behavior
	val, err = btPage.GetVal(0, "nonexistent")
	if err != nil {
		t.Logf("Getting non-existent field returned error: %v", err)
	} else {
		t.Logf("Getting non-existent field returned value: %v", val)
	}
}

func TestBTPage_RecordManagement(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	blk, err := tx.Append("btree_record_test.tbl")
	require.NoError(t, err)

	btPage, err := NewBTPage(tx, blk, layout)
	require.NoError(t, err)
	defer btPage.Close()

	// Format the page first
	err = btPage.Format(-1)
	require.NoError(t, err)

	// Test 1: Insert records and verify record count
	initialCount, err := btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 0, initialCount, "Should start with 0 records")

	// Insert at slot 0
	err = btPage.Insert(0)
	require.NoError(t, err)

	count, err := btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Should have 1 record after first insert")

	// Add some data to the first record
	err = btPage.SetVal(0, "block", 100)
	require.NoError(t, err)
	err = btPage.SetVal(0, "id", 200)
	require.NoError(t, err)
	err = btPage.SetVal(0, "dataval", 300)
	require.NoError(t, err)

	// Insert another record at slot 0 (should shift the first record to slot 1)
	err = btPage.Insert(0)
	require.NoError(t, err)

	count, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 2, count, "Should have 2 records after second insert")

	// Verify the original record was shifted to slot 1
	val, err := btPage.GetVal(1, "block")
	require.NoError(t, err)
	assert.Equal(t, 100, val, "Original record should be shifted to slot 1")

	val, err = btPage.GetVal(1, "id")
	require.NoError(t, err)
	assert.Equal(t, 200, val, "Original record should be shifted to slot 1")

	val, err = btPage.GetVal(1, "dataval")
	require.NoError(t, err)
	assert.Equal(t, 300, val, "Original record should be shifted to slot 1")

	// Test 2: Add data to the new record at slot 0
	err = btPage.SetVal(0, "block", 400)
	require.NoError(t, err)
	err = btPage.SetVal(0, "id", 500)
	require.NoError(t, err)
	err = btPage.SetVal(0, "dataval", 600)
	require.NoError(t, err)

	// Insert at slot 1 (between the two existing records)
	err = btPage.Insert(1)
	require.NoError(t, err)

	count, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 3, count, "Should have 3 records after third insert")

	// Verify record positions after middle insertion
	// Slot 0 should still have the new record
	val, err = btPage.GetVal(0, "block")
	require.NoError(t, err)
	assert.Equal(t, 400, val, "Slot 0 should be unchanged")

	// Slot 2 should now have the original record (shifted from slot 1)
	val, err = btPage.GetVal(2, "block")
	require.NoError(t, err)
	assert.Equal(t, 100, val, "Original record should be shifted to slot 2")

	// Test 3: CopyRecord functionality
	// Add data to slot 1
	err = btPage.SetVal(1, "block", 700)
	require.NoError(t, err)
	err = btPage.SetVal(1, "id", 800)
	require.NoError(t, err)
	err = btPage.SetVal(1, "dataval", 900)
	require.NoError(t, err)

	// Insert a new record at slot 3
	err = btPage.Insert(3)
	require.NoError(t, err)

	// Copy record from slot 1 to slot 3
	err = btPage.CopyRecord(1, 3)
	require.NoError(t, err)

	// Verify the copy
	val, err = btPage.GetVal(3, "block")
	require.NoError(t, err)
	assert.Equal(t, 700, val, "Copied record should match source")

	val, err = btPage.GetVal(3, "id")
	require.NoError(t, err)
	assert.Equal(t, 800, val, "Copied record should match source")

	val, err = btPage.GetVal(3, "dataval")
	require.NoError(t, err)
	assert.Equal(t, 900, val, "Copied record should match source")

	// Test 4: Delete records
	count, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 4, count, "Should have 4 records before deletion")

	// Delete record at slot 1
	err = btPage.Delete(1)
	require.NoError(t, err)

	count, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 3, count, "Should have 3 records after deletion")

	// Verify that slot 1 now contains what was previously in slot 2
	val, err = btPage.GetVal(1, "block")
	require.NoError(t, err)
	assert.Equal(t, 100, val, "Slot 1 should now contain the shifted record")

	// Delete record at slot 0
	err = btPage.Delete(0)
	require.NoError(t, err)

	count, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 2, count, "Should have 2 records after second deletion")

	// Verify that slot 0 now contains what was previously in slot 1
	val, err = btPage.GetVal(0, "block")
	require.NoError(t, err)
	assert.Equal(t, 100, val, "Slot 0 should now contain the shifted record")
}

func TestBTPage_SpecializedOperations(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	blk, err := tx.Append("btree_specialized_test.tbl")
	require.NoError(t, err)

	btPage, err := NewBTPage(tx, blk, layout)
	require.NoError(t, err)
	defer btPage.Close()

	// Format the page first
	err = btPage.Format(-1)
	require.NoError(t, err)

	// Test 1: FindSlotBefore with empty page
	slot, err := btPage.FindSlotBefore(50)
	require.NoError(t, err)
	assert.Equal(t, -1, slot, "Empty page should return -1")

	// Test 2: Add some sorted records
	testRecords := []struct {
		dataval int
		block   int
		id      int
	}{
		{10, 100, 1},
		{20, 200, 2},
		{30, 300, 3},
		{50, 500, 5},
		{70, 700, 7},
	}

	for i, record := range testRecords {
		err = btPage.Insert(i)
		require.NoError(t, err)
		err = btPage.SetVal(i, "dataval", record.dataval)
		require.NoError(t, err)
		err = btPage.SetVal(i, "block", record.block)
		require.NoError(t, err)
		err = btPage.SetVal(i, "id", record.id)
		require.NoError(t, err)
	}

	// Test 3: FindSlotBefore with various search keys
	testCases := []struct {
		searchKey    int
		expectedSlot int
		description  string
	}{
		{5, -1, "Key smaller than all records"},
		{10, -1, "Key equal to first record"},
		{15, 0, "Key between first and second"},
		{30, 1, "Key equal to middle record"},
		{40, 2, "Key between middle records"},
		{70, 3, "Key equal to last record"},
		{80, 4, "Key larger than all records"},
	}

	for _, tc := range testCases {
		slot, err := btPage.FindSlotBefore(tc.searchKey)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedSlot, slot, tc.description)
	}

	// Test 4: IsFull check
	isFull, err := btPage.IsFull()
	require.NoError(t, err)
	assert.False(t, isFull, "Page should not be full with 5 records")

	// Test 5: Fill up the page to test IsFull
	numRecs, err := btPage.GetNumRecs()
	require.NoError(t, err)

	// Calculate how many more records we can fit
	blockSize := tx.BlockSize()
	slotSize := layout.GetSlotSize()
	headerSize := 8 // flag + numRecs
	maxRecords := (blockSize - headerSize) / slotSize

	// Add records until we're close to full
	for numRecs < maxRecords-1 {
		err = btPage.Insert(numRecs)
		require.NoError(t, err)
		err = btPage.SetVal(numRecs, "dataval", 999)
		require.NoError(t, err)
		numRecs++
	}

	// Should still have room for one more
	isFull, err = btPage.IsFull()
	require.NoError(t, err)
	assert.False(t, isFull, "Page should not be full yet")

	// Add one more record
	err = btPage.Insert(numRecs)
	require.NoError(t, err)

	// Now it should be full
	isFull, err = btPage.IsFull()
	require.NoError(t, err)
	assert.True(t, isFull, "Page should be full now")
}

func TestBTPage_SplitAndTransfer(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	blk, err := tx.Append("btree_split_test.tbl")
	require.NoError(t, err)

	btPage, err := NewBTPage(tx, blk, layout)
	require.NoError(t, err)
	defer btPage.Close()

	// Format the page first
	err = btPage.Format(-1)
	require.NoError(t, err)

	// Add some test records
	testData := []struct {
		dataval int
		block   int
		id      int
	}{
		{10, 100, 1},
		{20, 200, 2},
		{30, 300, 3},
		{40, 400, 4},
		{50, 500, 5},
		{60, 600, 6},
	}

	for i, data := range testData {
		err = btPage.Insert(i)
		require.NoError(t, err)
		err = btPage.SetVal(i, "dataval", data.dataval)
		require.NoError(t, err)
		err = btPage.SetVal(i, "block", data.block)
		require.NoError(t, err)
		err = btPage.SetVal(i, "id", data.id)
		require.NoError(t, err)
	}

	// Verify we have 6 records
	numRecs, err := btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 6, numRecs, "Should have 6 records before split")

	// Test Split at position 3 (records 3, 4, 5 should move to new page)
	newBlk, err := btPage.Split(3, -1) // -1 = leaf flag (no overflow)
	require.NoError(t, err)
	require.NotNil(t, newBlk)

	// Check original page now has 3 records
	numRecs, err = btPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 3, numRecs, "Original page should have 3 records after split")

	// Verify original page contains records 0, 1, 2
	for i := 0; i < 3; i++ {
		val, err := btPage.GetVal(i, "dataval")
		require.NoError(t, err)
		assert.Equal(t, testData[i].dataval, val, "Original page should contain first 3 records")
	}

	// Check new page
	newPage, err := NewBTPage(tx, newBlk, layout)
	require.NoError(t, err)
	defer newPage.Close()

	newNumRecs, err := newPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 3, newNumRecs, "New page should have 3 records after split")

	// Verify new page contains records 3, 4, 5 (but at slots 0, 1, 2)
	for i := 0; i < 3; i++ {
		val, err := newPage.GetVal(i, "dataval")
		require.NoError(t, err)
		expectedVal := testData[i+3].dataval // Records 3, 4, 5 from original
		assert.Equal(t, expectedVal, val, "New page should contain last 3 records")
	}

	// Verify new page has correct flag
	flag, err := newPage.GetFlag()
	require.NoError(t, err)
	// Check that flag < 0 (which indicates regular leaf, no overflow)
	assert.Less(t, flag, 0, "New page should have leaf flag < 0 (regular leaf)")
}

func TestBTPage_TransferRecs(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	// Create source page
	srcBlk, err := tx.Append("btree_transfer_src.tbl")
	require.NoError(t, err)

	srcPage, err := NewBTPage(tx, srcBlk, layout)
	require.NoError(t, err)
	defer srcPage.Close()

	err = srcPage.Format(-1)
	require.NoError(t, err)

	// Create destination page
	destBlk, err := tx.Append("btree_transfer_dest.tbl")
	require.NoError(t, err)

	destPage, err := NewBTPage(tx, destBlk, layout)
	require.NoError(t, err)
	defer destPage.Close()

	err = destPage.Format(-1)
	require.NoError(t, err)

	// Add records to source page
	for i := 0; i < 5; i++ {
		err = srcPage.Insert(i)
		require.NoError(t, err)
		err = srcPage.SetVal(i, "dataval", (i+1)*10)
		require.NoError(t, err)
		err = srcPage.SetVal(i, "block", (i+1)*100)
		require.NoError(t, err)
		err = srcPage.SetVal(i, "id", i+1)
		require.NoError(t, err)
	}

	// Transfer records starting from slot 2
	err = srcPage.TransferRecs(2, destPage)
	require.NoError(t, err)

	// Check source page now has 2 records
	srcNumRecs, err := srcPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 2, srcNumRecs, "Source should have 2 records after transfer")

	// Check destination page has 3 records
	destNumRecs, err := destPage.GetNumRecs()
	require.NoError(t, err)
	assert.Equal(t, 3, destNumRecs, "Destination should have 3 records after transfer")

	// Verify source page contains first 2 records
	for i := 0; i < 2; i++ {
		val, err := srcPage.GetVal(i, "dataval")
		require.NoError(t, err)
		assert.Equal(t, (i+1)*10, val, "Source should contain first 2 records")
	}

	// Verify destination page contains last 3 records
	for i := 0; i < 3; i++ {
		val, err := destPage.GetVal(i, "dataval")
		require.NoError(t, err)
		expectedVal := (i + 3) * 10 // Records that were at slots 2, 3, 4 in source
		assert.Equal(t, expectedVal, val, "Destination should contain transferred records")
	}
}

func TestBTPage_NodeTypeSpecificMethods(t *testing.T) {
	tx, layout, cleanup := setupBTPageTest(t)
	defer cleanup()

	// Test Internal Node methods
	t.Run("InternalNodeMethods", func(t *testing.T) {
		blk, err := tx.Append("btree_internal_test.tbl")
		require.NoError(t, err)

		btPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		defer btPage.Close()

		// Format as internal node (flag = 1)
		err = btPage.Format(1)
		require.NoError(t, err)

		// Test InsertInternalNode
		err = btPage.InsertInternalNode(0, 20, 5)
		require.NoError(t, err)

		err = btPage.InsertInternalNode(0, 10, 3)
		require.NoError(t, err)

		err = btPage.InsertInternalNode(2, 30, 8)
		require.NoError(t, err)

		// Verify the entries
		val, err := btPage.GetVal(0, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 10, val, "First entry should have key 10")

		childNum, err := btPage.GetChildBlockNum(0)
		require.NoError(t, err)
		assert.Equal(t, 3, childNum, "First entry should point to block 3")

		val, err = btPage.GetVal(1, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 20, val, "Second entry should have key 20")

		childNum, err = btPage.GetChildBlockNum(1)
		require.NoError(t, err)
		assert.Equal(t, 5, childNum, "Second entry should point to block 5")

		val, err = btPage.GetVal(2, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 30, val, "Third entry should have key 30")

		childNum, err = btPage.GetChildBlockNum(2)
		require.NoError(t, err)
		assert.Equal(t, 8, childNum, "Third entry should point to block 8")
	})

	// Test Leaf Node methods
	t.Run("LeafNodeMethods", func(t *testing.T) {
		blk, err := tx.Append("btree_leaf_test.tbl")
		require.NoError(t, err)

		btPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		defer btPage.Close()

		// Format as leaf node (flag = -1)
		err = btPage.Format(-1)
		require.NoError(t, err)

		// Test InsertLeaf
		rid1 := record.NewRID(100, 5)
		err = btPage.InsertLeaf(0, 20, rid1)
		require.NoError(t, err)

		rid2 := record.NewRID(100, 8)
		err = btPage.InsertLeaf(0, 10, rid2)
		require.NoError(t, err)

		rid3 := record.NewRID(101, 2)
		err = btPage.InsertLeaf(2, 30, rid3)
		require.NoError(t, err)

		// Verify the entries
		val, err := btPage.GetVal(0, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 10, val, "First entry should have key 10")

		dataRid, err := btPage.GetDataRid(0)
		require.NoError(t, err)
		assert.Equal(t, rid2.Block(), dataRid.Block(), "First entry should have correct block")
		assert.Equal(t, rid2.Slot(), dataRid.Slot(), "First entry should have correct slot")

		val, err = btPage.GetVal(1, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 20, val, "Second entry should have key 20")

		dataRid, err = btPage.GetDataRid(1)
		require.NoError(t, err)
		assert.Equal(t, rid1.Block(), dataRid.Block(), "Second entry should have correct block")
		assert.Equal(t, rid1.Slot(), dataRid.Slot(), "Second entry should have correct slot")

		val, err = btPage.GetVal(2, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 30, val, "Third entry should have key 30")

		dataRid, err = btPage.GetDataRid(2)
		require.NoError(t, err)
		assert.Equal(t, rid3.Block(), dataRid.Block(), "Third entry should have correct block")
		assert.Equal(t, rid3.Slot(), dataRid.Slot(), "Third entry should have correct slot")
	})
}
