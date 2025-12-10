package transaction

import (
	"sync"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
)

// TransactionManager manages all active transactions and handles checkpoints.
type TransactionManager struct {
	fileManager      *file.Manager
	logManager       *dblog.Manager
	bufferManager    *buffer.Manager
	lockTable        *LockTable
	dirtyPageTable   *DirtyPageTable
	transactionTable *TransactionTable

	// Tracks all active transactions
	activeTransactions map[int64]*Transaction
	mu                 sync.RWMutex
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(fileManager *file.Manager, logManager *dblog.Manager, bufferManager *buffer.Manager, lockTable *LockTable, dirtyPageTable *DirtyPageTable, transactionTable *TransactionTable) *TransactionManager {
	return &TransactionManager{
		fileManager:        fileManager,
		logManager:         logManager,
		bufferManager:      bufferManager,
		lockTable:          lockTable,
		dirtyPageTable:     dirtyPageTable,
		transactionTable:   transactionTable,
		activeTransactions: make(map[int64]*Transaction),
	}
}

// BeginTransaction creates and starts a new transaction
func (tm *TransactionManager) BeginTransaction() *Transaction {
	txNum := getNextTxNum()

	concurrencyManager := NewConcurrencyManager(tm.lockTable)
	bufferList := NewBufferList(tm.bufferManager)

	transaction := &Transaction{
		fileManager:        tm.fileManager,
		logManager:         tm.logManager,
		bufferManager:      tm.bufferManager,
		concurrencyManager: concurrencyManager,
		txNum:              txNum,
		prevTxLSN:          -1,
		bufferList:         bufferList,
		dirtyPageTable:     tm.dirtyPageTable,
		transactionManager: tm,
	}

	recoveryManager := NewRecoveryManager(txNum, transaction, tm.logManager, tm.bufferManager, tm.dirtyPageTable, tm.transactionTable)
	transaction.recoveryManager = recoveryManager

	// 1. Write log record first (WAL principle)
	lsn := tm.logManager.GetNextLatestLSN()
	err := WriteStartLogRecord(tm.logManager, txNum, lsn, -1)
	if err != nil {
		return nil
	}
	transaction.prevTxLSN = lsn

	// 2. Update TransactionTable
	tm.transactionTable.Add(txNum, TransactionStatusRunning, lsn)

	// 3. Track active transaction
	tm.mu.Lock()
	tm.activeTransactions[txNum] = transaction
	tm.mu.Unlock()

	return transaction
}

// EndTransaction is called when a transaction commits or rolls back
// Note: TransactionTable status is handled by RecoveryManager, this only manages active transactions
func (tm *TransactionManager) EndTransaction(txNum int64, status TransactionStatus) {
	tm.mu.Lock()
	tx, exists := tm.activeTransactions[txNum]
	if exists {
		lsn := tx.prevTxLSN
		tm.transactionTable.UpdateStatus(txNum, status)
		tm.transactionTable.UpdateLastLSN(txNum, lsn)
		tm.transactionTable.Remove(txNum)
		delete(tm.activeTransactions, txNum)
	}
	tm.mu.Unlock()
}

// GetActiveTransactions returns a copy of active transaction numbers for checkpoint
func (tm *TransactionManager) GetActiveTransactions() []int64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	txNums := make([]int64, 0, len(tm.activeTransactions))
	for txNum := range tm.activeTransactions {
		txNums = append(txNums, txNum)
	}
	return txNums
}

// PerformCheckpoint performs a fuzzy checkpoint.
// This should be called periodically during normal operation.
// The TransactionTable is updated here with current transaction states.
func (tm *TransactionManager) PerformCheckpoint() error {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// Update TransactionTable with current active transactions
	for txNum, tx := range tm.activeTransactions {
		err := tm.transactionTable.UpdateStatus(txNum, TransactionStatusRunning)
		if err != nil {
			return err
		}
		err = tm.transactionTable.UpdateLastLSN(txNum, tx.prevTxLSN)
		if err != nil {
			return err
		}
	}

	// Gather snapshots
	txTableSnapshot := tm.transactionTable.GetAll()
	dptSnapshot := tm.dirtyPageTable.GetAll()

	tempTx := tm.BeginTransaction()
	if tempTx == nil {
		return nil
	}
	defer tempTx.Commit()

	return tempTx.recoveryManager.Checkpoint(txTableSnapshot, dptSnapshot)
}

// PerformDBRecovery performs database-wide ARIES recovery.
// This should be called during database startup to recover from crashes.
func (tm *TransactionManager) PerformDBRecovery() error {
	// Create a temporary transaction for recovery operations
	tempTx := tm.BeginTransaction()
	if tempTx == nil {
		return nil
	}
	defer tempTx.Commit()

	return tempTx.recoveryManager.DBRecovery()
}
