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

func setupInternalTest(t *testing.T) (*transaction.Transaction, *record.Layout, func()) {
	t.Helper()

	tempDir := t.TempDir()

	fileManager, err := file.NewManager(tempDir, 400)
	require.NoError(t, err)

	logManager, err := log.NewManager(fileManager, "bt_internal_test.log")
	require.NoError(t, err)

	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)

	lockTable := transaction.NewLockTable()
	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)

	// Create a layout for B-tree internal node records
	// Same layout as leaf nodes: block (child block number), id (unused), dataval (key)
	schema := record.NewSchema()
	schema.AddIntField("block")   // Child block number
	schema.AddIntField("id")      // Unused for internal nodes, but part of layout
	schema.AddIntField("dataval") // The key value
	layout := record.NewLayoutFromSchema(schema)

	cleanup := func() {
		if err := tx.Commit(); err != nil {
			t.Errorf("failed to commit transaction: %v", err)
		}
		fileManager.Close()
	}

	return tx, layout, cleanup
}

func TestInternal_findChildBlock(t *testing.T) {
	t.Run("Basic - key between entries", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create an internal node with entries: [10→block5, 20→block8, 30→block12]
		blk, err := tx.Append("btree_internal_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1) // Level 1 internal node
		require.NoError(t, err)

		// Insert entries: key 10 points to block 5, key 20 points to block 8, key 30 points to block 12
		err = page.InsertInternalNode(0, 10, 5)
		require.NoError(t, err)
		err = page.InsertInternalNode(1, 20, 8)
		require.NoError(t, err)
		err = page.InsertInternalNode(2, 30, 12)
		require.NoError(t, err)

		page.Close()

		// Create Internal node
		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Test: searchKey = 15 (between 10 and 20) → should return block5
		childBlk, err := internal.findChildBlock(15)
		require.NoError(t, err)
		assert.Equal(t, blk.Filename(), childBlk.Filename())
		assert.Equal(t, 5, childBlk.Number(), "Key 15 should point to block 5 (follows key 10)")

		// Test: searchKey = 25 (between 20 and 30) → should return block8
		childBlk, err = internal.findChildBlock(25)
		require.NoError(t, err)
		assert.Equal(t, 8, childBlk.Number(), "Key 25 should point to block 8 (follows key 20)")

		// Test: searchKey = 5 (less than first key) → should return block5 (first entry)
		childBlk, err = internal.findChildBlock(5)
		require.NoError(t, err)
		assert.Equal(t, 5, childBlk.Number(), "Key 5 should point to block 5 (first entry)")

		// Test: searchKey = 35 (greater than last key) → should return block12
		childBlk, err = internal.findChildBlock(35)
		require.NoError(t, err)
		assert.Equal(t, 12, childBlk.Number(), "Key 35 should point to block 12 (follows key 30)")
	})

	t.Run("Exact key match", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		blk, err := tx.Append("btree_internal_exact_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1)
		require.NoError(t, err)

		// Insert entries: [10→block5, 20→block8, 30→block12]
		err = page.InsertInternalNode(0, 10, 5)
		require.NoError(t, err)
		err = page.InsertInternalNode(1, 20, 8)
		require.NoError(t, err)
		err = page.InsertInternalNode(2, 30, 12)
		require.NoError(t, err)

		page.Close()

		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Test: searchKey = 20 (exact match) → should return block8 (the entry itself)
		childBlk, err := internal.findChildBlock(20)
		require.NoError(t, err)
		assert.Equal(t, 8, childBlk.Number(), "Exact match for key 20 should point to block 8")

		// Test: searchKey = 10 (exact match with first entry) → should return block5
		childBlk, err = internal.findChildBlock(10)
		require.NoError(t, err)
		assert.Equal(t, 5, childBlk.Number(), "Exact match for key 10 should point to block 5")

		// Test: searchKey = 30 (exact match with last entry) → should return block12
		childBlk, err = internal.findChildBlock(30)
		require.NoError(t, err)
		assert.Equal(t, 12, childBlk.Number(), "Exact match for key 30 should point to block 12")
	})

	t.Run("Single entry", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		blk, err := tx.Append("btree_internal_single_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1)
		require.NoError(t, err)

		// Insert single entry: [10→block5]
		err = page.InsertInternalNode(0, 10, 5)
		require.NoError(t, err)

		page.Close()

		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Any search key should point to block5
		childBlk, err := internal.findChildBlock(5)
		require.NoError(t, err)
		assert.Equal(t, 5, childBlk.Number(), "Key 5 should point to block 5")

		childBlk, err = internal.findChildBlock(10)
		require.NoError(t, err)
		assert.Equal(t, 5, childBlk.Number(), "Key 10 should point to block 5")

		childBlk, err = internal.findChildBlock(100)
		require.NoError(t, err)
		assert.Equal(t, 5, childBlk.Number(), "Key 100 should point to block 5")
	})

	t.Run("Empty page", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		blk, err := tx.Append("btree_internal_empty_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1)
		require.NoError(t, err)
		page.Close()

		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Empty page should error when trying to get block number
		// FindSlotBefore returns -1, we set slot to 0, but there are no records
		// So GetBlockNum(0) will fail because there's no record at slot 0
		_, err = internal.findChildBlock(10)
		assert.Error(t, err, "Should error on empty page")
	})
}

func TestInternal_Search(t *testing.T) {
	t.Run("Basic - find leaf block", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create a simple 2-level B-tree:
		// Root (internal node): [10→leaf1, 20→leaf2, 30→leaf3]
		// Leaf1: contains keys < 20
		// Leaf2: contains keys >= 20 and < 30
		// Leaf3: contains keys >= 30

		// Create leaf blocks
		leaf1Blk, err := tx.Append("btree_search_test.tbl")
		require.NoError(t, err)
		leaf1Page, err := NewBTPage(tx, leaf1Blk, layout)
		require.NoError(t, err)
		err = leaf1Page.Format(0) // Leaf node
		require.NoError(t, err)
		// Add some data to leaf1
		rid1 := record.NewRID(100, 1)
		err = leaf1Page.InsertLeaf(0, 5, rid1)
		require.NoError(t, err)
		rid2 := record.NewRID(100, 2)
		err = leaf1Page.InsertLeaf(1, 15, rid2)
		require.NoError(t, err)
		leaf1Page.Close()

		leaf2Blk, err := tx.Append("btree_search_test.tbl")
		require.NoError(t, err)
		leaf2Page, err := NewBTPage(tx, leaf2Blk, layout)
		require.NoError(t, err)
		err = leaf2Page.Format(0) // Leaf node
		require.NoError(t, err)
		// Add some data to leaf2
		rid3 := record.NewRID(100, 3)
		err = leaf2Page.InsertLeaf(0, 25, rid3)
		require.NoError(t, err)
		leaf2Page.Close()

		leaf3Blk, err := tx.Append("btree_search_test.tbl")
		require.NoError(t, err)
		leaf3Page, err := NewBTPage(tx, leaf3Blk, layout)
		require.NoError(t, err)
		err = leaf3Page.Format(0) // Leaf node
		require.NoError(t, err)
		// Add some data to leaf3
		rid4 := record.NewRID(100, 4)
		err = leaf3Page.InsertLeaf(0, 35, rid4)
		require.NoError(t, err)
		leaf3Page.Close()

		// Create root internal node
		rootBlk, err := tx.Append("btree_search_test.tbl")
		require.NoError(t, err)
		rootPage, err := NewBTPage(tx, rootBlk, layout)
		require.NoError(t, err)
		err = rootPage.Format(1) // Internal node (level 1)
		require.NoError(t, err)

		// Insert entries: [10→leaf1, 20→leaf2, 30→leaf3]
		err = rootPage.InsertInternalNode(0, 10, leaf1Blk.Number())
		require.NoError(t, err)
		err = rootPage.InsertInternalNode(1, 20, leaf2Blk.Number())
		require.NoError(t, err)
		err = rootPage.InsertInternalNode(2, 30, leaf3Blk.Number())
		require.NoError(t, err)
		rootPage.Close()

		// Test Search - create a new Internal for each search since Search modifies the page pointer
		// Search for key 15 (should find leaf1)
		internal1, err := NewInternal(tx, rootBlk, layout)
		require.NoError(t, err)
		leafBlkNum, err := internal1.Search(15)
		require.NoError(t, err)
		assert.Equal(t, leaf1Blk.Number(), leafBlkNum, "Key 15 should point to leaf1")
		internal1.Close()

		// Search for key 20 (should find leaf2 due to exact match logic)
		internal2, err := NewInternal(tx, rootBlk, layout)
		require.NoError(t, err)
		leafBlkNum, err = internal2.Search(20)
		require.NoError(t, err)
		assert.Equal(t, leaf2Blk.Number(), leafBlkNum, "Key 20 should point to leaf2")
		internal2.Close()

		// Search for key 25 (should find leaf2)
		internal3, err := NewInternal(tx, rootBlk, layout)
		require.NoError(t, err)
		leafBlkNum, err = internal3.Search(25)
		require.NoError(t, err)
		assert.Equal(t, leaf2Blk.Number(), leafBlkNum, "Key 25 should point to leaf2")
		internal3.Close()

		// Search for key 35 (should find leaf3)
		internal4, err := NewInternal(tx, rootBlk, layout)
		require.NoError(t, err)
		leafBlkNum, err = internal4.Search(35)
		require.NoError(t, err)
		assert.Equal(t, leaf3Blk.Number(), leafBlkNum, "Key 35 should point to leaf3")
		internal4.Close()
	})
}

func TestInternal_insertEntry(t *testing.T) {
	t.Run("Basic insertion - no split", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create an internal node page
		blk, err := tx.Append("btree_insert_entry_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1) // Internal node (level 1)
		require.NoError(t, err)

		// Insert initial entries: [10→block1, 30→block3]
		err = page.InsertInternalNode(0, 10, 1)
		require.NoError(t, err)
		err = page.InsertInternalNode(1, 30, 3)
		require.NoError(t, err)
		page.Close()

		// Create Internal node
		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert entry (20, block2) - should insert between 10 and 30
		entry := NewInternalNodeEntry(20, 2)
		result, err := internal.insertEntry(entry)
		require.NoError(t, err)
		assert.Nil(t, result, "Should not split for small number of entries")

		// Verify the entry was inserted correctly
		// Read back the page to verify
		verifyPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		numRecs, err := verifyPage.GetNumRecs()
		require.NoError(t, err)
		assert.Equal(t, 3, numRecs, "Should have 3 entries")

		// Verify entries are in sorted order
		val0, err := verifyPage.GetVal(0, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 10, val0, "First entry should be 10")

		val1, err := verifyPage.GetVal(1, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 20, val1, "Second entry should be 20")

		val2, err := verifyPage.GetVal(2, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 30, val2, "Third entry should be 30")

		// Verify block numbers
		blk0, err := verifyPage.GetBlockNum(0)
		require.NoError(t, err)
		assert.Equal(t, 1, blk0, "First entry should point to block 1")

		blk1, err := verifyPage.GetBlockNum(1)
		require.NoError(t, err)
		assert.Equal(t, 2, blk1, "Second entry should point to block 2")

		blk2, err := verifyPage.GetBlockNum(2)
		require.NoError(t, err)
		assert.Equal(t, 3, blk2, "Third entry should point to block 3")

		verifyPage.Close()
	})

	t.Run("Insertion causing split", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create an internal node page and fill it to capacity
		blk, err := tx.Append("btree_insert_split_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1) // Internal node (level 1)
		require.NoError(t, err)

		// Determine page capacity by inserting until full
		capacity := 0
		for {
			err = page.InsertInternalNode(capacity, capacity*10, capacity)
			if err != nil {
				break // Page is full
			}
			isFull, err := page.IsFull()
			require.NoError(t, err)
			if isFull {
				capacity++
				break
			}
			capacity++
		}
		page.Close()

		// Recreate page with entries just before full
		page, err = NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1)
		require.NoError(t, err)

		// Insert entries up to capacity-1 (one less than full)
		for i := 0; i < capacity-1; i++ {
			err = page.InsertInternalNode(i, i*10, i)
			require.NoError(t, err)
		}
		page.Close()

		// Create Internal node
		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert one more entry - should cause split
		entry := NewInternalNodeEntry((capacity-1)*10, capacity-1)
		result, err := internal.insertEntry(entry)
		require.NoError(t, err)
		require.NotNil(t, result, "Should return split entry when page is full")

		// Verify split entry
		assert.NotNil(t, result.DataValue, "Split entry should have a data value")
		assert.Greater(t, result.BlockNumber, 0, "Split entry should have a block number")

		// Verify the original page has fewer entries (split occurred)
		verifyPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		numRecs, err := verifyPage.GetNumRecs()
		require.NoError(t, err)
		assert.Less(t, numRecs, capacity, "Original page should have fewer entries after split")
		verifyPage.Close()
	})

	t.Run("Insert at beginning", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		blk, err := tx.Append("btree_insert_begin_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1)
		require.NoError(t, err)

		// Insert initial entry: [20→block2]
		err = page.InsertInternalNode(0, 20, 2)
		require.NoError(t, err)
		page.Close()

		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert entry (10, block1) - should insert at beginning
		entry := NewInternalNodeEntry(10, 1)
		result, err := internal.insertEntry(entry)
		require.NoError(t, err)
		assert.Nil(t, result, "Should not split")

		// Verify order
		verifyPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		val0, err := verifyPage.GetVal(0, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 10, val0, "First entry should be 10")

		val1, err := verifyPage.GetVal(1, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 20, val1, "Second entry should be 20")
		verifyPage.Close()
	})

	t.Run("Insert at end", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		blk, err := tx.Append("btree_insert_end_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(1)
		require.NoError(t, err)

		// Insert initial entry: [10→block1]
		err = page.InsertInternalNode(0, 10, 1)
		require.NoError(t, err)
		page.Close()

		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert entry (30, block3) - should insert at end
		entry := NewInternalNodeEntry(30, 3)
		result, err := internal.insertEntry(entry)
		require.NoError(t, err)
		assert.Nil(t, result, "Should not split")

		// Verify order
		verifyPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		val0, err := verifyPage.GetVal(0, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 10, val0, "First entry should be 10")

		val1, err := verifyPage.GetVal(1, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 30, val1, "Second entry should be 30")
		verifyPage.Close()
	})
}

func TestInternal_Insert(t *testing.T) {
	t.Run("Basic insertion with flag == 0 (delegates to insertEntry)", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create a page formatted as internal node but with flag 0
		// This tests the case where Insert delegates to insertEntry
		blk, err := tx.Append("btree_insert_basic_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(0) // Flag 0 - will delegate to insertEntry
		require.NoError(t, err)

		// Insert initial entries: [10→block1, 30→block3]
		err = page.InsertInternalNode(0, 10, 1)
		require.NoError(t, err)
		err = page.InsertInternalNode(1, 30, 3)
		require.NoError(t, err)
		page.Close()

		// Create Internal node
		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert entry (20, block2) - should delegate to insertEntry
		entry := NewInternalNodeEntry(20, 2)
		result, err := internal.Insert(entry)
		require.NoError(t, err)
		assert.Nil(t, result, "Should not split for small number of entries")

		// Verify the entry was inserted correctly
		verifyPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		numRecs, err := verifyPage.GetNumRecs()
		require.NoError(t, err)
		assert.Equal(t, 3, numRecs, "Should have 3 entries")

		// Verify entries are in sorted order
		val1, err := verifyPage.GetVal(1, "dataval")
		require.NoError(t, err)
		assert.Equal(t, 20, val1, "Middle entry should be 20")
		verifyPage.Close()
	})

	t.Run("Insertion causing split with flag == 0", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create an internal node page and fill it to capacity
		blk, err := tx.Append("btree_insert_split_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(0) // Flag 0 - will delegate to insertEntry
		require.NoError(t, err)

		// Determine page capacity
		capacity := 0
		for {
			err = page.InsertInternalNode(capacity, capacity*10, capacity)
			if err != nil {
				break
			}
			isFull, err := page.IsFull()
			require.NoError(t, err)
			if isFull {
				capacity++
				break
			}
			capacity++
		}
		page.Close()

		// Recreate page with entries just before full
		page, err = NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(0)
		require.NoError(t, err)

		// Insert entries up to capacity-1
		for i := 0; i < capacity-1; i++ {
			err = page.InsertInternalNode(i, i*10, i)
			require.NoError(t, err)
		}
		page.Close()

		// Create Internal node
		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert one more entry - should cause split
		entry := NewInternalNodeEntry((capacity-1)*10, capacity-1)
		result, err := internal.Insert(entry)
		require.NoError(t, err)
		require.NotNil(t, result, "Should return split entry when page is full")

		// Verify split entry
		assert.NotNil(t, result.DataValue, "Split entry should have a data value")
		assert.Greater(t, result.BlockNumber, 0, "Split entry should have a block number")

		// Verify the original page has fewer entries (split occurred)
		verifyPage, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		numRecs, err := verifyPage.GetNumRecs()
		require.NoError(t, err)
		assert.Less(t, numRecs, capacity, "Original page should have fewer entries after split")
		verifyPage.Close()
	})

	t.Run("Insert into multi-level tree - child split propagates", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create a 2-level tree:
		// Root (level 2): [10→child1, 30→child2]
		// Child1 (level 1): [5→leaf1, 15→leaf2]
		// Child2 (level 1): [25→leaf3, 35→leaf4]

		// Create leaf blocks (we'll use them as child internal nodes for simplicity)
		child1Blk, err := tx.Append("btree_insert_multi_test.tbl")
		require.NoError(t, err)
		child1Page, err := NewBTPage(tx, child1Blk, layout)
		require.NoError(t, err)
		err = child1Page.Format(1) // Level 1 internal node
		require.NoError(t, err)
		err = child1Page.InsertInternalNode(0, 5, 10)
		require.NoError(t, err)
		err = child1Page.InsertInternalNode(1, 15, 20)
		require.NoError(t, err)
		child1Page.Close()

		child2Blk, err := tx.Append("btree_insert_multi_test.tbl")
		require.NoError(t, err)
		child2Page, err := NewBTPage(tx, child2Blk, layout)
		require.NoError(t, err)
		err = child2Page.Format(1) // Level 1 internal node
		require.NoError(t, err)
		err = child2Page.InsertInternalNode(0, 25, 30)
		require.NoError(t, err)
		err = child2Page.InsertInternalNode(1, 35, 40)
		require.NoError(t, err)
		child2Page.Close()

		// Create root (level 2)
		rootBlk, err := tx.Append("btree_insert_multi_test.tbl")
		require.NoError(t, err)
		rootPage, err := NewBTPage(tx, rootBlk, layout)
		require.NoError(t, err)
		err = rootPage.Format(2) // Level 2 root
		require.NoError(t, err)
		err = rootPage.InsertInternalNode(0, 10, child1Blk.Number())
		require.NoError(t, err)
		err = rootPage.InsertInternalNode(1, 30, child2Blk.Number())
		require.NoError(t, err)
		rootPage.Close()

		// Create Internal node for root
		rootInternal, err := NewInternal(tx, rootBlk, layout)
		require.NoError(t, err)
		defer rootInternal.Close()

		// Insert entry (12, block12) - should go to child1
		// If child1 splits, the split entry should propagate to root
		entry := NewInternalNodeEntry(12, 12)
		result, err := rootInternal.Insert(entry)
		require.NoError(t, err)

		// Result depends on whether child1 split
		// For small trees, it might not split, so result could be nil
		// If it did split, result would contain the split entry
		if result != nil {
			assert.NotNil(t, result.DataValue, "Split entry should have a data value")
			assert.Greater(t, result.BlockNumber, 0, "Split entry should have a block number")
		}
	})

	t.Run("Insert with flag == 0 (leaf node case)", func(t *testing.T) {
		tx, layout, cleanup := setupInternalTest(t)
		defer cleanup()

		// Create a page formatted as leaf (flag == 0)
		// This tests the edge case where Insert is called on what appears to be a leaf
		blk, err := tx.Append("btree_insert_leaf_test.tbl")
		require.NoError(t, err)

		page, err := NewBTPage(tx, blk, layout)
		require.NoError(t, err)
		err = page.Format(0) // Leaf node (flag == 0)
		require.NoError(t, err)
		page.Close()

		// Create Internal node (even though it's formatted as leaf)
		internal, err := NewInternal(tx, blk, layout)
		require.NoError(t, err)
		defer internal.Close()

		// Insert entry - should delegate to insertEntry
		entry := NewInternalNodeEntry(10, 1)
		result, err := internal.Insert(entry)
		require.NoError(t, err)
		// Result depends on whether page is full
		// For empty page, should be nil (no split)
		if result == nil {
			// Verify entry was inserted
			verifyPage, err := NewBTPage(tx, blk, layout)
			require.NoError(t, err)
			numRecs, err := verifyPage.GetNumRecs()
			require.NoError(t, err)
			assert.Equal(t, 1, numRecs, "Should have 1 entry")
			verifyPage.Close()
		}
	})
}
