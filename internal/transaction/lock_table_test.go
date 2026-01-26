package transaction

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/file"
)

func TestLockTable_ConcurrentLocking(t *testing.T) {
	lt := NewLockTable()
	block := file.NewBlockID("testfile", 1)

	// Test 1: Acquire exclusive lock
	err := lt.xLock(block)
	require.NoError(t, err)
	assert.True(t, lt.HasXLock(block))

	// Test 2: Another exclusive lock must wait
	exclusiveDone := make(chan error, 1)
	go func() {
		exclusiveDone <- lt.xLock(block)
	}()

	// Give it time to start waiting
	time.Sleep(100 * time.Millisecond)

	// Exclusive lock should still be waiting
	select {
	case <-exclusiveDone:
		t.Fatal("Exclusive lock acquired while another exclusive lock still held")
	default:
		// Expected: still waiting
	}

	// Release the first exclusive lock
	err = lt.unlock(block)
	require.NoError(t, err)
	assert.False(t, lt.HasXLock(block))

	// Now the waiting exclusive lock should be acquired
	err = <-exclusiveDone
	require.NoError(t, err)
	assert.True(t, lt.HasXLock(block))

	// Release the lock before Test 3
	err = lt.unlock(block)
	require.NoError(t, err)
	assert.False(t, lt.HasXLock(block))

	// Test 3: Multiple concurrent exclusive lock attempts - only one at a time
	const numConcurrent = 3
	var wg sync.WaitGroup
	acquired := make([]bool, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := lt.xLock(block)
			if err == nil {
				acquired[idx] = true
				// Hold the lock briefly, then release
				time.Sleep(50 * time.Millisecond)
				lt.unlock(block)
			}
		}(i)
	}

	wg.Wait()

	// All should eventually succeed (they acquire and release sequentially)
	successCount := 0
	for _, acq := range acquired {
		if acq {
			successCount++
		}
	}
	assert.Equal(t, numConcurrent, successCount, "All exclusive locks should eventually be acquired")
	assert.False(t, lt.HasXLock(block), "No lock should remain after all releases")
}
