package buffermanager

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/filemanager"
	"github.com/yashagw/cranedb/internal/logmanager"
)

func TestBufferMgr_BasicOperations(t *testing.T) {
	dbDir := "testdata"
	blockSize := 400

	fm := filemanager.NewFileMgr(dbDir, blockSize)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm := logmanager.NewLogMgr(fm, "testlog")
	defer lm.Close()

	bm := NewBufferMgr(fm, lm, 3)
	assert.Equal(t, 3, bm.Available(), "Should have 3 available buffers initially")

	blk1 := filemanager.NewBlockID("testfile", 0)
	blk2 := filemanager.NewBlockID("testfile", 1)

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
	buff1.SetModified(123, 456)
	assert.Equal(t, 123, buff1.ModifyingTx(), "Should track modifying transaction")

	// Test flush all
	bm.FlushAll(123)

	// Clean up
	bm.Unpin(buff2)
}
