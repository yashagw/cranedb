package transaction

import (
	"fmt"
	"sync"
)

// TransactionStatus represents the current state of a transaction
type TransactionStatus int

const (
	TransactionStatusRunning TransactionStatus = iota
	TransactionStatusCommitted
	TransactionStatusAborted
)

// TransactionEntry represents an entry in the Transaction Table
type TransactionEntry struct {
	Status  TransactionStatus
	LastLSN int64
}

// TransactionTable tracks all active transactions
// Key: Transaction number (int64)
// Value: Transaction entry containing status and lastLSN
type TransactionTable struct {
	mu    sync.RWMutex
	table map[int64]*TransactionEntry
}

// NewTransactionTable creates a new empty Transaction Table
func NewTransactionTable() *TransactionTable {
	return &TransactionTable{
		table: make(map[int64]*TransactionEntry),
	}
}

// Add adds a new transaction to the table
func (tt *TransactionTable) Add(txNum int64, status TransactionStatus, lsn int64) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.table[txNum] = &TransactionEntry{
		Status:  status,
		LastLSN: lsn,
	}
}

// UpdateStatus changes the transaction status
func (tt *TransactionTable) UpdateStatus(txNum int64, status TransactionStatus) error {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if entry, exists := tt.table[txNum]; exists {
		entry.Status = status
		return nil
	} else {
		return fmt.Errorf("transaction %d not found in transaction table", txNum)
	}
}

// UpdateLastLSN updates the last LSN for a transaction
func (tt *TransactionTable) UpdateLastLSN(txNum int64, lsn int64) error {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if entry, exists := tt.table[txNum]; exists {
		entry.LastLSN = lsn
		return nil
	} else {
		return fmt.Errorf("transaction %d not found in transaction table", txNum)
	}
}

// Remove removes a transaction from the table
func (tt *TransactionTable) Remove(txNum int64) error {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if _, exists := tt.table[txNum]; !exists {
		return fmt.Errorf("transaction %d not found in transaction table", txNum)
	}

	delete(tt.table, txNum)
	return nil
}

// Get retrieves a transaction entry
// Returns the entry and true if found, nil and false otherwise
func (tt *TransactionTable) Get(txNum int64) (*TransactionEntry, bool) {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	entry, exists := tt.table[txNum]
	if !exists {
		return nil, false
	}
	// Return a copy to prevent external modification
	return &TransactionEntry{
		Status:  entry.Status,
		LastLSN: entry.LastLSN,
	}, true
}

// GetAll returns a snapshot of all entries in the table
// This is used for checkpoint operations
func (tt *TransactionTable) GetAll() map[int64]*TransactionEntry {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	// Create a deep copy to prevent external modification
	snapshot := make(map[int64]*TransactionEntry, len(tt.table))
	for txNum, entry := range tt.table {
		snapshot[txNum] = &TransactionEntry{
			Status:  entry.Status,
			LastLSN: entry.LastLSN,
		}
	}
	return snapshot
}
