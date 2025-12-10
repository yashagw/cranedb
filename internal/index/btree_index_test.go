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

func setupBTreeIndexTest(t *testing.T, layout *record.Layout) (*BTreeIndex, *transaction.Transaction, func()) {
	t.Helper()

	tempDir := t.TempDir()

	fileManager, err := file.NewManager(tempDir, 400)
	require.NoError(t, err)

	logManager, err := log.NewManager(fileManager, "btree_index_test.log")
	require.NoError(t, err)

	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)

	lockTable := transaction.NewLockTable()
	dirtyPageTable := transaction.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()

	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()

	btreeIndex, err := NewBTreeIndex(tx, "test_btree_index", layout)
	require.NoError(t, err)

	cleanup := func() {
		if err := btreeIndex.Close(); err != nil {
			t.Errorf("failed to close btree index: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Errorf("failed to commit transaction: %v", err)
		}
		fileManager.Close()
	}

	return btreeIndex, tx, cleanup
}

func TestBTreeIndex_InsertAndSearch(t *testing.T) {
	layout := intIndexLayout()
	btreeIndex, _, cleanup := setupBTreeIndexTest(t, layout)
	defer cleanup()

	// Insert records with different keys
	rid1 := record.NewRID(1, 1)
	rid2 := record.NewRID(2, 2)
	rid3 := record.NewRID(3, 3)

	require.NoError(t, btreeIndex.Insert(10, rid1))
	require.NoError(t, btreeIndex.Insert(20, rid2))
	require.NoError(t, btreeIndex.Insert(30, rid3))

	// Search for key 20
	require.NoError(t, btreeIndex.BeforeFirst(20))
	hasNext, err := btreeIndex.Next()
	require.NoError(t, err)
	assert.True(t, hasNext, "Should find record with key 20")

	rid, err := btreeIndex.GetDataRid()
	require.NoError(t, err)
	assert.Equal(t, ridKey(rid2), ridKey(rid), "Should return correct RID for key 20")

	// Verify no more records with key 20
	hasNext, err = btreeIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "Should not have more records with key 20")
}

func TestBTreeIndex_MultipleRecordsSameKey(t *testing.T) {
	layout := intIndexLayout()
	btreeIndex, _, cleanup := setupBTreeIndexTest(t, layout)
	defer cleanup()

	searchKey := 42
	rid1 := record.NewRID(1, 1)
	rid2 := record.NewRID(2, 2)
	rid3 := record.NewRID(3, 3)
	otherRID := record.NewRID(9, 9)

	// Insert multiple records with same key
	require.NoError(t, btreeIndex.Insert(searchKey, rid1))
	require.NoError(t, btreeIndex.Insert(searchKey, rid2))
	require.NoError(t, btreeIndex.Insert(searchKey, rid3))
	require.NoError(t, btreeIndex.Insert(99, otherRID))

	// Search for the key
	require.NoError(t, btreeIndex.BeforeFirst(searchKey))

	var collected []string
	for {
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rid, err := btreeIndex.GetDataRid()
		require.NoError(t, err)
		collected = append(collected, ridKey(rid))
	}

	expected := []string{
		ridKey(rid1),
		ridKey(rid2),
		ridKey(rid3),
	}
	assert.ElementsMatch(t, expected, collected, "Should find all records with same key")
}

func TestBTreeIndex_Delete(t *testing.T) {
	layout := intIndexLayout()
	btreeIndex, _, cleanup := setupBTreeIndexTest(t, layout)
	defer cleanup()

	searchKey := 25
	rid1 := record.NewRID(1, 1)
	rid2 := record.NewRID(2, 2)
	rid3 := record.NewRID(3, 3)

	// Insert records
	require.NoError(t, btreeIndex.Insert(searchKey, rid1))
	require.NoError(t, btreeIndex.Insert(searchKey, rid2))
	require.NoError(t, btreeIndex.Insert(searchKey, rid3))

	// Delete middle record
	require.NoError(t, btreeIndex.Delete(searchKey, rid2))

	// Verify deletion
	require.NoError(t, btreeIndex.BeforeFirst(searchKey))

	var collected []string
	for {
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rid, err := btreeIndex.GetDataRid()
		require.NoError(t, err)
		collected = append(collected, ridKey(rid))
	}

	expected := []string{
		ridKey(rid1),
		ridKey(rid3),
	}
	assert.ElementsMatch(t, expected, collected, "Should not find deleted record")
}

func TestBTreeIndex_StringKeys(t *testing.T) {
	layout := stringIndexLayout(20)
	btreeIndex, _, cleanup := setupBTreeIndexTest(t, layout)
	defer cleanup()

	searchKey := "alpha"
	otherKey := "beta"

	rid1 := record.NewRID(4, 1)
	rid2 := record.NewRID(4, 2)
	otherRID := record.NewRID(10, 0)

	require.NoError(t, btreeIndex.Insert(searchKey, rid1))
	require.NoError(t, btreeIndex.Insert(searchKey, rid2))
	require.NoError(t, btreeIndex.Insert(otherKey, otherRID))

	// Search for searchKey
	require.NoError(t, btreeIndex.BeforeFirst(searchKey))

	var collected []string
	for {
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rid, err := btreeIndex.GetDataRid()
		require.NoError(t, err)
		collected = append(collected, ridKey(rid))
	}

	expected := []string{
		ridKey(rid1),
		ridKey(rid2),
	}
	assert.ElementsMatch(t, expected, collected, "Should find all records with string key")

	// Search for otherKey
	require.NoError(t, btreeIndex.BeforeFirst(otherKey))
	hasNext, err := btreeIndex.Next()
	require.NoError(t, err)
	assert.True(t, hasNext, "Should find record with otherKey")

	foundRID, err := btreeIndex.GetDataRid()
	require.NoError(t, err)
	assert.Equal(t, ridKey(otherRID), ridKey(foundRID), "Should return correct RID")
}

func TestBTreeIndex_EmptyIndex(t *testing.T) {
	layout := intIndexLayout()
	btreeIndex, _, cleanup := setupBTreeIndexTest(t, layout)
	defer cleanup()

	// Search for non-existent key
	require.NoError(t, btreeIndex.BeforeFirst(999))
	hasNext, err := btreeIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "Should not find any records in empty index")
}

func TestBTreeIndex_ReopenIndex(t *testing.T) {
	layout := intIndexLayout()
	tempDir := t.TempDir()

	fileManager, err := file.NewManager(tempDir, 400)
	require.NoError(t, err)
	defer fileManager.Close()

	logManager, err := log.NewManager(fileManager, "btree_reopen_test.log")
	require.NoError(t, err)

	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)

	lockTable := transaction.NewLockTable()
	dirtyPageTable := transaction.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()

	// First transaction: create index and insert data
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx1 := transactionManager.BeginTransaction()
	btreeIndex1, err := NewBTreeIndex(tx1, "test_reopen", layout)
	require.NoError(t, err)

	rid1 := record.NewRID(1, 1)
	require.NoError(t, btreeIndex1.Insert(10, rid1))
	require.NoError(t, btreeIndex1.Close())
	require.NoError(t, tx1.Commit())

	// Second transaction: reopen index and verify data
	tx2 := transactionManager.BeginTransaction()
	btreeIndex2, err := NewBTreeIndex(tx2, "test_reopen", layout)
	require.NoError(t, err)
	defer btreeIndex2.Close()

	require.NoError(t, btreeIndex2.BeforeFirst(10))
	hasNext, err := btreeIndex2.Next()
	require.NoError(t, err)
	assert.True(t, hasNext, "Should find record after reopening index")

	rid, err := btreeIndex2.GetDataRid()
	require.NoError(t, err)
	assert.Equal(t, ridKey(rid1), ridKey(rid), "Should return correct RID after reopen")

	require.NoError(t, tx2.Commit())
}

func TestBTreeIndex_SortedOrder(t *testing.T) {
	layout := intIndexLayout()
	btreeIndex, _, cleanup := setupBTreeIndexTest(t, layout)
	defer cleanup()

	// Insert records in random order
	require.NoError(t, btreeIndex.Insert(30, record.NewRID(3, 0)))
	require.NoError(t, btreeIndex.Insert(10, record.NewRID(1, 0)))
	require.NoError(t, btreeIndex.Insert(50, record.NewRID(5, 0)))
	require.NoError(t, btreeIndex.Insert(20, record.NewRID(2, 0)))
	require.NoError(t, btreeIndex.Insert(40, record.NewRID(4, 0)))

	// Verify records are returned in sorted order
	keys := []int{10, 20, 30, 40, 50}
	keyIndex := 0

	for _, key := range keys {
		require.NoError(t, btreeIndex.BeforeFirst(key))
		hasNext, err := btreeIndex.Next()
		require.NoError(t, err)
		assert.True(t, hasNext, "Should find record with key %d", key)

		rid, err := btreeIndex.GetDataRid()
		require.NoError(t, err)
		expectedBlock := keyIndex + 1
		assert.Equal(t, expectedBlock, rid.Block(), "Record for key %d should be in correct block", key)
		keyIndex++
	}
}

func TestBTreeIndex_SearchCost(t *testing.T) {
	// Test SearchCost function
	cost1 := SearchCost(100, 10)
	assert.Greater(t, cost1, 0, "Search cost should be positive")

	cost2 := SearchCost(1000, 10)
	assert.Equal(t, cost2, cost1, "Larger index should have same cost")

	cost3 := SearchCost(100, 20)
	assert.Less(t, cost3, cost1, "Higher fanout should have lower cost")
}
