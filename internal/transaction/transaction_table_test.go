package transaction

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionTable_BasicOperations(t *testing.T) {
	// Test NewTransactionTable
	tt := NewTransactionTable()
	require.NotNil(t, tt)
	all := tt.GetAll()
	assert.Empty(t, all)

	// Test Add - add new transaction
	txNum1 := int64(1)
	tt.Add(txNum1, TransactionStatusRunning, 100)
	entry, exists := tt.Get(txNum1)
	require.True(t, exists)
	assert.Equal(t, TransactionStatusRunning, entry.Status)
	assert.Equal(t, int64(100), entry.LastLSN)

	// Test Add - add multiple transactions
	tt.Add(2, TransactionStatusCommitted, 200)
	tt.Add(3, TransactionStatusAborted, 300)
	entry2, exists2 := tt.Get(2)
	require.True(t, exists2)
	assert.Equal(t, TransactionStatusCommitted, entry2.Status)
	assert.Equal(t, int64(200), entry2.LastLSN)

	// Test Add - overwrite existing transaction
	tt.Add(txNum1, TransactionStatusCommitted, 150)
	entry, exists = tt.Get(txNum1)
	require.True(t, exists)
	assert.Equal(t, TransactionStatusCommitted, entry.Status)
	assert.Equal(t, int64(150), entry.LastLSN)

	// Test UpdateStatus - update existing transaction
	err := tt.UpdateStatus(txNum1, TransactionStatusAborted)
	require.NoError(t, err)
	entry, exists = tt.Get(txNum1)
	require.True(t, exists)
	assert.Equal(t, TransactionStatusAborted, entry.Status)
	assert.Equal(t, int64(150), entry.LastLSN) // LSN should remain unchanged

	// Test UpdateStatus - update non-existent transaction
	err = tt.UpdateStatus(999, TransactionStatusCommitted)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction 999 not found")

	// Test UpdateLastLSN - update existing transaction
	err = tt.UpdateLastLSN(txNum1, 200)
	require.NoError(t, err)
	entry, exists = tt.Get(txNum1)
	require.True(t, exists)
	assert.Equal(t, int64(200), entry.LastLSN)
	assert.Equal(t, TransactionStatusAborted, entry.Status) // Status should remain unchanged

	// Test UpdateLastLSN - update multiple times
	err = tt.UpdateLastLSN(txNum1, 300)
	require.NoError(t, err)
	entry, exists = tt.Get(txNum1)
	require.True(t, exists)
	assert.Equal(t, int64(300), entry.LastLSN)

	// Test UpdateLastLSN - update non-existent transaction
	err = tt.UpdateLastLSN(999, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction 999 not found")

	// Test Get - get non-existent transaction
	_, exists = tt.Get(999)
	assert.False(t, exists)

	// Test Get - verify returns a copy (immutability)
	entry.Status = TransactionStatusCommitted
	entry.LastLSN = 999
	entry2, exists = tt.Get(txNum1)
	require.True(t, exists)
	assert.Equal(t, TransactionStatusAborted, entry2.Status)
	assert.Equal(t, int64(300), entry2.LastLSN)

	// Test GetAll - on empty table
	tt2 := NewTransactionTable()
	all = tt2.GetAll()
	assert.NotNil(t, all)
	assert.Empty(t, all)

	// Test GetAll - with multiple transactions
	all = tt.GetAll()
	require.Len(t, all, 3)

	entry1, exists := all[1]
	require.True(t, exists)
	assert.Equal(t, TransactionStatusAborted, entry1.Status)
	assert.Equal(t, int64(300), entry1.LastLSN)

	entry2, exists = all[2]
	require.True(t, exists)
	assert.Equal(t, TransactionStatusCommitted, entry2.Status)
	assert.Equal(t, int64(200), entry2.LastLSN)

	entry3, exists := all[3]
	require.True(t, exists)
	assert.Equal(t, TransactionStatusAborted, entry3.Status)
	assert.Equal(t, int64(300), entry3.LastLSN)

	// Test GetAll - verify returns a snapshot (immutability)
	all[1].Status = TransactionStatusCommitted
	all[1].LastLSN = 999
	entry, exists = tt.Get(1)
	require.True(t, exists)
	assert.Equal(t, TransactionStatusAborted, entry.Status)
	assert.Equal(t, int64(300), entry.LastLSN)

	// Test GetAll - verify returns a new snapshot each time
	tt.Add(4, TransactionStatusRunning, 400)
	all2 := tt.GetAll()
	assert.Len(t, all2, 4)
	assert.Len(t, all, 3) // Original snapshot should be unchanged

	// Test Remove - remove existing transaction
	err = tt.Remove(2)
	require.NoError(t, err)
	_, exists = tt.Get(2)
	assert.False(t, exists)

	// Test Remove - remove non-existent transaction
	err = tt.Remove(999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction 999 not found")

	// Test Remove - remove multiple transactions
	tt.Add(5, TransactionStatusRunning, 500)
	tt.Add(6, TransactionStatusCommitted, 600)
	err = tt.Remove(5)
	require.NoError(t, err)
	_, exists = tt.Get(5)
	assert.False(t, exists)
	_, exists = tt.Get(6)
	assert.True(t, exists)

	// Test status transitions
	tt3 := NewTransactionTable()
	txNum3 := int64(10)
	tt3.Add(txNum3, TransactionStatusRunning, 100)
	err = tt3.UpdateStatus(txNum3, TransactionStatusCommitted)
	require.NoError(t, err)
	entry, exists = tt3.Get(txNum3)
	require.True(t, exists)
	assert.Equal(t, TransactionStatusCommitted, entry.Status)

	// Test status constants
	assert.Equal(t, TransactionStatus(0), TransactionStatusRunning)
	assert.Equal(t, TransactionStatus(1), TransactionStatusCommitted)
	assert.Equal(t, TransactionStatus(2), TransactionStatusAborted)
}

func TestTransactionTable_ConcurrentAccess(t *testing.T) {
	tt := NewTransactionTable()
	const numGoroutines = 10
	const numTransactions = 100

	var wg sync.WaitGroup

	// Test concurrent Add operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numTransactions; j++ {
				txNum := int64(id*numTransactions + j)
				tt.Add(txNum, TransactionStatusRunning, int64(txNum*10))
			}
		}(i)
	}
	wg.Wait()

	// Verify all transactions were added
	all := tt.GetAll()
	assert.Len(t, all, numGoroutines*numTransactions)

	// Test concurrent Get operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numTransactions; j++ {
				txNum := int64(id*numTransactions + j)
				entry, exists := tt.Get(txNum)
				require.True(t, exists)
				assert.Equal(t, TransactionStatusRunning, entry.Status)
			}
		}(i)
	}
	wg.Wait()

	// Test concurrent UpdateStatus operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numTransactions; j++ {
				txNum := int64(id*numTransactions + j)
				err := tt.UpdateStatus(txNum, TransactionStatusCommitted)
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all transactions were updated
	for i := 0; i < numGoroutines*numTransactions; i++ {
		entry, exists := tt.Get(int64(i))
		require.True(t, exists)
		assert.Equal(t, TransactionStatusCommitted, entry.Status)
	}

	// Test concurrent UpdateLastLSN operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numTransactions; j++ {
				txNum := int64(id*numTransactions + j)
				err := tt.UpdateLastLSN(txNum, int64(txNum*20))
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all LSNs were updated
	for i := 0; i < numGoroutines*numTransactions; i++ {
		entry, exists := tt.Get(int64(i))
		require.True(t, exists)
		assert.Equal(t, int64(i*20), entry.LastLSN)
	}

	// Test concurrent Remove operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numTransactions; j++ {
				txNum := int64(id*numTransactions + j)
				err := tt.Remove(txNum)
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all transactions were removed
	all = tt.GetAll()
	assert.Empty(t, all)
}
