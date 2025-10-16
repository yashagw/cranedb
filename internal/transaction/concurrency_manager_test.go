package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/file"
)

func TestConcurrencyManager_LockingAndUpgrade(t *testing.T) {
	lockTable := NewLockTable()
	cm1 := NewConcurrencyManager(lockTable)
	cm2 := NewConcurrencyManager(lockTable)
	block := file.NewBlockID("testfile", 1)

	// Test 1: Acquire shared lock
	err := cm1.sLock(block)
	require.NoError(t, err)
	assert.True(t, lockTable.HasSLock(block))

	// Test 2: Idempotent - acquiring same shared lock again should work
	err = cm1.sLock(block)
	require.NoError(t, err)

	// Test 3: Another manager can also acquire shared lock
	err = cm2.sLock(block)
	require.NoError(t, err)

	// Test 4: Exclusive lock from cm2 should wait (test in goroutine)
	done := make(chan error, 1)
	go func() {
		done <- cm2.xLock(block)
	}()

	// Give cm2 time to start waiting
	// Note: Since cm2 already has S lock, it will upgrade (unlock S, lock X)
	// But cm1 still has S lock, so it should wait

	// Release cm1's shared lock
	err = cm1.release()
	require.NoError(t, err)

	// Now cm2 should be able to acquire exclusive lock
	err = <-done
	require.NoError(t, err)
	assert.True(t, lockTable.HasXLock(block))

	// Test 5: cm1 trying to get shared lock should wait for cm2's exclusive
	done2 := make(chan error, 1)
	go func() {
		done2 <- cm1.sLock(block)
	}()

	// Release cm2's exclusive lock
	err = cm2.release()
	require.NoError(t, err)

	// cm1 should now get the shared lock
	err = <-done2
	require.NoError(t, err)
	assert.True(t, lockTable.HasSLock(block))

	// Test 6: Upgrade from shared to exclusive within same manager
	err = cm1.xLock(block)
	require.NoError(t, err)
	assert.True(t, lockTable.HasXLock(block))
	assert.False(t, lockTable.HasSLock(block))

	// Test 7: Release all locks
	err = cm1.release()
	require.NoError(t, err)
	assert.False(t, lockTable.HasXLock(block))
	assert.False(t, lockTable.HasSLock(block))
}
