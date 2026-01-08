package buffer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

func TestManager_BasicOperations(t *testing.T) {
	dbDir := "testdata"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm, err := log.NewManager(fm, "testlog")
	assert.NoError(t, err)
	defer lm.Close()

	dpt := NewDirtyPageTable()

	bm, err := NewManager(fm, lm, dpt, 3)
	require.NoError(t, err)
	assert.Equal(t, 3, bm.Available(), "Should have 3 available buffers initially")

	blk1 := file.NewBlockID("testfile", 0)
	blk2 := file.NewBlockID("testfile", 1)

	// Pin first buffer
	buff1, err := bm.Pin(blk1)
	require.NoError(t, err, "Should pin buffer successfully")
	assert.Equal(t, 2, bm.Available(), "Should have 2 available buffers after pinning")
	assert.True(t, buff1.IsPinned(), "Buffer should be pinned")
	assert.Equal(t, blk1, buff1.Block(), "Buffer should be assigned to blk1")

	// Pin same block again - should return same buffer
	buff1Again, err := bm.Pin(blk1)
	require.NoError(t, err, "Should pin same buffer again")
	assert.Same(t, buff1, buff1Again, "Pinning same block should return same buffer")
	assert.Equal(t, 2, bm.Available(), "Available count should remain 2")

	// Pin different block
	buff2, err := bm.Pin(blk2)
	require.NoError(t, err, "Should pin second buffer")
	assert.Equal(t, 1, bm.Available(), "Should have 1 available buffer")

	// Unpin first buffer once - should still be pinned (pinned twice)
	bm.Unpin(buff1)
	assert.Equal(t, 1, bm.Available(), "Buffer should still be pinned, available should be 1")
	// Unpin first buffer again - should become available
	bm.Unpin(buff1Again)
	assert.Equal(t, 2, bm.Available(), "Should have 2 available buffers after unpinning")
	assert.False(t, buff1.IsPinned(), "Buffer should not be pinned after unpinning twice")

	// Test modification tracking
	buff1.SetModifiedTx(123)
	assert.Equal(t, int64(123), buff1.ModifyingTx(), "Should track modifying transaction")

	buff1.SetModifiedLSN(456)
	assert.Equal(t, int64(456), buff1.ModifyingLSN(), "Should track LSN")

	// Clean up
	bm.Unpin(buff2)
}

func TestManager_LRUEvictionOrder(t *testing.T) {
	dbDir := "testdata_lru"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	require.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm, err := log.NewManager(fm, "testlog")
	require.NoError(t, err)
	defer lm.Close()

	dpt := NewDirtyPageTable()

	// Create a buffer pool with only 3 buffers
	bm, err := NewManager(fm, lm, dpt, 3)
	require.NoError(t, err)

	blk0 := file.NewBlockID("testfile", 0)
	blk1 := file.NewBlockID("testfile", 1)
	blk2 := file.NewBlockID("testfile", 2)
	blk3 := file.NewBlockID("testfile", 3)

	// Pin 3 blocks - fills the buffer pool
	buff0, err := bm.Pin(blk0)
	require.NoError(t, err)
	buff1, err := bm.Pin(blk1)
	require.NoError(t, err)
	buff2, err := bm.Pin(blk2)
	require.NoError(t, err)

	// Unpin all - LRU order should be: blk0 (oldest) -> blk1 -> blk2 (most recent)
	bm.Unpin(buff0)
	bm.Unpin(buff1)
	bm.Unpin(buff2)

	// Access blk0 again - this should move it to front of LRU
	// LRU order now: blk1 (oldest) -> blk2 -> blk0 (most recent)
	buff0Again, err := bm.Pin(blk0)
	require.NoError(t, err)
	assert.Same(t, buff0, buff0Again, "Should return same buffer for same block")
	bm.Unpin(buff0Again)

	// Pin a new block (blk3) - should evict blk1 (least recently used)
	buff3, err := bm.Pin(blk3)
	require.NoError(t, err)
	assert.Same(t, buff1, buff3, "Block 3 should be in the buffer that previously held block 1 (LRU eviction)")
	assert.Equal(t, blk3, buff3.Block(), "Buffer should now contain block 3")

	bm.Unpin(buff3)
}
