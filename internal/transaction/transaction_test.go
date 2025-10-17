package transaction

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

func TestTransaction_BasicOperations(t *testing.T) {
	fileManager := file.NewManager("/tmp/testdb", 400)
	logManager := log.NewManager(fileManager, "test.log")
	bufferManager := buffer.NewManager(fileManager, logManager, 10)
	lockTable := NewLockTable()

	// Test 1: Create transaction
	tx1 := NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx1)
	assert.Equal(t, 0, tx1.txNum) // First transaction should be 0

	// Test 2: Create another transaction (should get unique number)
	tx2 := NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx2)
	assert.Equal(t, 1, tx2.txNum) // Second transaction should be 1

	// Test 3: Pin and unpin buffer
	block := file.NewBlockID("testfile", 1)
	buff := tx1.Pin(block)
	require.NotNil(t, buff)
	assert.NotNil(t, tx1.bufferList.GetBuffer(block))
	tx1.Unpin(block)
	_, exists := tx1.bufferList.pins[makeKey(block)]
	assert.False(t, exists)

	// Test 4: Get file size
	size, err := tx1.Size("testfile")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, size, 0)

	// Test 5: Append new block
	newBlock, err := tx1.Append("testfile")
	require.NoError(t, err)
	require.NotNil(t, newBlock)
	assert.Equal(t, "testfile", newBlock.Filename())

	// Test 6: Commit transaction
	tx1.Commit()
	// After commit, all buffers should be unpinned
	assert.Empty(t, tx1.bufferList.pins)
	assert.Empty(t, tx1.bufferList.buffers)

	// Test 7: Rollback transaction
	tx2.Pin(block)
	tx2.Rollback()
	// After rollback, all buffers should be unpinned
	assert.Empty(t, tx2.bufferList.pins)
	assert.Empty(t, tx2.bufferList.buffers)
}

func TestTransaction_DataOperation(t *testing.T) {
	fileManager := file.NewManager("/tmp/testdb", 400)
	logManager := log.NewManager(fileManager, "test.log")
	bufferManager := buffer.NewManager(fileManager, logManager, 10)
	lockTable := NewLockTable()

	tx := NewTransaction(fileManager, logManager, bufferManager, lockTable)
	block := file.NewBlockID("testfile", 1)

	// Pin the buffer first
	buff := tx.Pin(block)
	require.NotNil(t, buff)

	// Test 1: Set and get integer
	tx.SetInt(block, 0, 42, true)
	val := tx.GetInt(block, 0)
	assert.Equal(t, 42, val)

	// Test 2: Set and get string
	tx.SetString(block, 4, "hello", true)
	str := tx.GetString(block, 4)
	assert.Equal(t, "hello", str)

	// Test 3: Multiple operations on same block
	tx.SetInt(block, 8, 100, true)
	tx.SetString(block, 12, "world", true)
	intVal := tx.GetInt(block, 8)
	strVal := tx.GetString(block, 12)
	assert.Equal(t, 100, intVal)
	assert.Equal(t, "world", strVal)

	tx.Commit()
}

func TestTransaction_ConcurrencyOperations(t *testing.T) {
	fileManager := file.NewManager("/tmp/testdb", 400)
	logManager := log.NewManager(fileManager, "test.log")
	bufferManager := buffer.NewManager(fileManager, logManager, 10)
	lockTable := NewLockTable()

	block := file.NewBlockID("testfile", 1)
	var wg sync.WaitGroup
	results := make([]int, 3)

	// Test concurrent transactions accessing the same block
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			tx := NewTransaction(fileManager, logManager, bufferManager, lockTable)
			buff := tx.Pin(block)
			require.NotNil(t, buff)

			// Each transaction writes a different value
			expectedValue := 100 + index
			tx.SetInt(block, 0, expectedValue, true) // Enable logging

			// Small delay to ensure concurrency
			time.Sleep(10 * time.Millisecond)

			// Read the value back - should see its own value
			val := tx.GetInt(block, 0)
			results[index] = val

			tx.Commit()
		}(i)
	}

	wg.Wait()

	// Each transaction should see its own written value
	for i := 0; i < 3; i++ {
		expectedValue := 100 + i
		assert.Equal(t, expectedValue, results[i], "Transaction %d should see its own value %d, but got %d", i, expectedValue, results[i])
	}
}

func TestTransaction_ReadWriteConcurrency(t *testing.T) {
	fileManager := file.NewManager("/tmp/testdb", 400)
	logManager := log.NewManager(fileManager, "test.log")
	bufferManager := buffer.NewManager(fileManager, logManager, 10)
	lockTable := NewLockTable()

	block := file.NewBlockID("testfile", 1)
	var wg sync.WaitGroup
	readResults := make([]int, 2)
	writeResults := make([]bool, 1)

	// Writer transaction
	wg.Add(1)
	go func() {
		defer wg.Done()
		tx := NewTransaction(fileManager, logManager, bufferManager, lockTable)
		buff := tx.Pin(block)
		require.NotNil(t, buff)

		// Write operation with exclusive lock
		tx.SetInt(block, 0, 999, true)
		writeResults[0] = true

		// Small delay to test locking
		time.Sleep(50 * time.Millisecond)

		tx.Commit()
	}()

	// Reader transactions (should wait for writer to complete)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// Small delay to ensure writer starts first
			time.Sleep(10 * time.Millisecond)

			tx := NewTransaction(fileManager, logManager, bufferManager, lockTable)
			buff := tx.Pin(block)
			require.NotNil(t, buff)

			// Read operation with shared lock
			val := tx.GetInt(block, 0)
			readResults[index] = val

			tx.Commit()
		}(i)
	}

	wg.Wait()

	// Verify writer completed
	assert.True(t, writeResults[0])

	// Verify readers got the written value (999)
	for i, val := range readResults {
		assert.Equal(t, 999, val, "Reader %d should have read the written value", i)
	}
}
