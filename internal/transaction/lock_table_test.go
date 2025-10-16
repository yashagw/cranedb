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

	// Test 1: Multiple shared locks can be acquired simultaneously
	var wg sync.WaitGroup
	const numSharedLocks = 5

	for i := 0; i < numSharedLocks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := lt.sLock(block)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	assert.True(t, lt.HasSLock(block))
	assert.False(t, lt.HasXLock(block))

	// Test 2: Exclusive lock must wait for all shared locks to release
	exclusiveDone := make(chan error, 1)
	go func() {
		exclusiveDone <- lt.xLock(block)
	}()

	// Give it time to start waiting
	time.Sleep(100 * time.Millisecond)

	// Exclusive lock should still be waiting
	select {
	case <-exclusiveDone:
		t.Fatal("Exclusive lock acquired while shared locks still held")
	default:
		// Expected: still waiting
	}

	// Release all shared locks
	for i := 0; i < numSharedLocks; i++ {
		err := lt.unlock(block)
		require.NoError(t, err)
	}

	// Now exclusive lock should be acquired
	err := <-exclusiveDone
	require.NoError(t, err)
	assert.True(t, lt.HasXLock(block))
	assert.False(t, lt.HasSLock(block))

	// Test 3: Shared lock must wait for exclusive lock
	sharedDone := make(chan error, 1)
	go func() {
		sharedDone <- lt.sLock(block)
	}()

	time.Sleep(100 * time.Millisecond)

	// Shared lock should be waiting
	select {
	case <-sharedDone:
		t.Fatal("Shared lock acquired while exclusive lock still held")
	default:
		// Expected: still waiting
	}

	// Release exclusive lock
	err = lt.unlock(block)
	require.NoError(t, err)

	// Now shared lock should be acquired
	err = <-sharedDone
	require.NoError(t, err)
	assert.True(t, lt.HasSLock(block))

	// Clean up
	err = lt.unlock(block)
	require.NoError(t, err)
}
