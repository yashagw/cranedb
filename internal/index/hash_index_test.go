package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

func setupHashIndexTest(t *testing.T, layout *record.Layout) (*HashIndex, func()) {
	t.Helper()

	tempDir := t.TempDir()

	fileManager, err := file.NewManager(tempDir, 400)
	require.NoError(t, err)

	logManager, err := log.NewManager(fileManager, "hash_index_test.log")
	require.NoError(t, err)

	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)

	lockTable := transaction.NewLockTable()
	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)

	hashIndex, err := NewHashIndex(tx, "test_hash_index", layout)
	require.NoError(t, err)

	cleanup := func() {
		if err := hashIndex.Close(); err != nil {
			t.Errorf("failed to close hash index: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Errorf("failed to commit transaction: %v", err)
		}
		fileManager.Close()
	}

	return hashIndex, cleanup
}

func intIndexLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	schema.AddIntField("dataval")
	return record.NewLayoutFromSchema(schema)
}

func stringIndexLayout(length int) *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	schema.AddStringField("dataval", length)
	return record.NewLayoutFromSchema(schema)
}

func ridKey(rid *record.RID) string {
	return fmt.Sprintf("%d-%d", rid.Block(), rid.Slot())
}

func TestHashIndex_InsertSearchAndDelete(t *testing.T) {
	layout := intIndexLayout()
	hashIndex, cleanup := setupHashIndexTest(t, layout)
	defer cleanup()

	searchKey := 42
	otherKey := 99

	rid1 := record.NewRID(1, 3)
	rid2 := record.NewRID(2, 5)
	rid3 := record.NewRID(3, 7)
	otherRID := record.NewRID(9, 9)

	require.NoError(t, hashIndex.Insert(searchKey, rid1))
	require.NoError(t, hashIndex.Insert(searchKey, rid2))
	require.NoError(t, hashIndex.Insert(searchKey, rid3))
	require.NoError(t, hashIndex.Insert(otherKey, otherRID))

	require.NoError(t, hashIndex.BeforeFirst(searchKey))

	var collected []string
	for {
		hasNext, err := hashIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rid, err := hashIndex.GetDataRid()
		require.NoError(t, err)
		collected = append(collected, ridKey(rid))
	}

	expected := []string{
		ridKey(rid1),
		ridKey(rid2),
		ridKey(rid3),
	}
	assert.ElementsMatch(t, expected, collected)

	require.NoError(t, hashIndex.Delete(searchKey, rid2))

	require.NoError(t, hashIndex.BeforeFirst(searchKey))

	collected = collected[:0]
	for {
		hasNext, err := hashIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rid, err := hashIndex.GetDataRid()
		require.NoError(t, err)
		collected = append(collected, ridKey(rid))
	}

	expectedAfterDelete := []string{
		ridKey(rid1),
		ridKey(rid3),
	}
	assert.ElementsMatch(t, expectedAfterDelete, collected)

	require.NoError(t, hashIndex.BeforeFirst(otherKey))
	hasNext, err := hashIndex.Next()
	require.NoError(t, err)
	require.True(t, hasNext)
	foundRID, err := hashIndex.GetDataRid()
	require.NoError(t, err)
	assert.Equal(t, ridKey(otherRID), ridKey(foundRID))

	hasNext, err = hashIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestHashIndex_StringKeys(t *testing.T) {
	layout := stringIndexLayout(20)
	hashIndex, cleanup := setupHashIndexTest(t, layout)
	defer cleanup()

	searchKey := "alpha"
	otherKey := "beta"

	rid1 := record.NewRID(4, 1)
	rid2 := record.NewRID(4, 2)
	otherRID := record.NewRID(10, 0)

	require.NoError(t, hashIndex.Insert(searchKey, rid1))
	require.NoError(t, hashIndex.Insert(searchKey, rid2))
	require.NoError(t, hashIndex.Insert(otherKey, otherRID))

	require.NoError(t, hashIndex.BeforeFirst(searchKey))

	var collected []string
	for {
		hasNext, err := hashIndex.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rid, err := hashIndex.GetDataRid()
		require.NoError(t, err)
		collected = append(collected, ridKey(rid))
	}

	expected := []string{
		ridKey(rid1),
		ridKey(rid2),
	}
	assert.ElementsMatch(t, expected, collected)

	require.NoError(t, hashIndex.BeforeFirst(otherKey))
	hasNext, err := hashIndex.Next()
	require.NoError(t, err)
	require.True(t, hasNext)
	foundRID, err := hashIndex.GetDataRid()
	require.NoError(t, err)
	assert.Equal(t, ridKey(otherRID), ridKey(foundRID))
}
