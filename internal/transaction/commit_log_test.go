package transaction

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommitLog_MarkAndQuery(t *testing.T) {
	cl := NewCommitLog()

	// Initially nothing is committed
	assert.False(t, cl.IsCommitted(1))
	assert.False(t, cl.IsCommitted(2))

	// Mark transaction 1 as committed
	cl.MarkCommitted(1)
	assert.True(t, cl.IsCommitted(1))
	assert.False(t, cl.IsCommitted(2))

	// Mark transaction 2 as committed
	cl.MarkCommitted(2)
	assert.True(t, cl.IsCommitted(1))
	assert.True(t, cl.IsCommitted(2))
}

func TestCommitLog_Cleanup(t *testing.T) {
	cl := NewCommitLog()

	cl.MarkCommitted(1)
	cl.MarkCommitted(5)
	cl.MarkCommitted(10)
	cl.MarkCommitted(15)

	// Cleanup entries older than oldestActiveTx=10
	cl.Cleanup(10)

	// Transactions < 10 should be removed
	assert.False(t, cl.IsCommitted(1))
	assert.False(t, cl.IsCommitted(5))

	// Transactions >= 10 should remain
	assert.True(t, cl.IsCommitted(10))
	assert.True(t, cl.IsCommitted(15))
}

func TestCommitLog_ConcurrentAccess(t *testing.T) {
	cl := NewCommitLog()
	var wg sync.WaitGroup

	// Concurrently mark and query transactions
	for i := int64(1); i <= 100; i++ {
		wg.Add(1)
		go func(txNum int64) {
			defer wg.Done()
			cl.MarkCommitted(txNum)
			assert.True(t, cl.IsCommitted(txNum))
		}(i)
	}

	wg.Wait()

	// All should be committed
	for i := int64(1); i <= 100; i++ {
		assert.True(t, cl.IsCommitted(i))
	}
}

func TestCommitLog_IdempotentMark(t *testing.T) {
	cl := NewCommitLog()

	cl.MarkCommitted(5)
	cl.MarkCommitted(5)
	cl.MarkCommitted(5)

	assert.True(t, cl.IsCommitted(5))
}
