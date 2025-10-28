package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

func TestBufferList_PinAndUnpin(t *testing.T) {
	fileManager, err := file.NewManager("/tmp/testdb", 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	assert.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10) // 10 buffers
	assert.NoError(t, err)
	bufferList := NewBufferList(bufferManager)
	block := file.NewBlockID("testfile", 1)

	// Test 1: Pin a buffer for the first time
	buff1 := bufferList.Pin(block)
	require.NotNil(t, buff1)
	assert.Equal(t, 1, bufferList.pins[makeKey(block)])

	// Test 2: Pin the same buffer again (should increment pin count)
	buff2 := bufferList.Pin(block)
	require.NotNil(t, buff2)
	assert.Equal(t, buff1, buff2) // Should return the same buffer
	assert.Equal(t, 2, bufferList.pins[makeKey(block)])

	// Test 3: Unpin once (should decrement but not remove)
	bufferList.Unpin(block)
	assert.Equal(t, 1, bufferList.pins[makeKey(block)])
	assert.NotNil(t, bufferList.GetBuffer(block)) // Buffer should still exist

	// Test 4: Unpin again (should remove buffer completely)
	bufferList.Unpin(block)
	_, exists := bufferList.pins[makeKey(block)]
	assert.False(t, exists)
	assert.Nil(t, bufferList.GetBuffer(block)) // Buffer should be removed

	// Test 5: UnpinAll should work even with no buffers
	bufferList.UnpinAll()
	assert.Empty(t, bufferList.pins)
	assert.Empty(t, bufferList.buffers)
}
