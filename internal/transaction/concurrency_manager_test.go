package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/file"
)

func TestConcurrencyManager_MVCCLocking(t *testing.T) {
	lockTable := NewLockTable()
	cm1 := NewConcurrencyManager(lockTable)
	cm2 := NewConcurrencyManager(lockTable)
	block := file.NewBlockID("testfile", 1)

	// Test 1: sLock is a no-op under MVCC — no lock is actually acquired
	err := cm1.sLock(block)
	require.NoError(t, err)
	assert.False(t, lockTable.HasSLock(block), "sLock should be a no-op under MVCC")

	// Test 2: xLock still works for write protection
	err = cm1.xLock(block)
	require.NoError(t, err)
	assert.True(t, lockTable.HasXLock(block))

	// Test 3: Idempotent — acquiring same exclusive lock again should work
	err = cm1.xLock(block)
	require.NoError(t, err)
	assert.True(t, lockTable.HasXLock(block))

	// Test 4: Another manager's xLock should block while cm1 holds it
	done := make(chan error, 1)
	go func() {
		done <- cm2.xLock(block)
	}()

	// Release cm1's exclusive lock
	err = cm1.release()
	require.NoError(t, err)
	assert.False(t, lockTable.HasXLock(block))

	// Now cm2 should be able to acquire exclusive lock
	err = <-done
	require.NoError(t, err)
	assert.True(t, lockTable.HasXLock(block))

	// Test 5: Release cm2's lock
	err = cm2.release()
	require.NoError(t, err)
	assert.False(t, lockTable.HasXLock(block))
}
