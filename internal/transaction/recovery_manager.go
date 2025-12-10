package transaction

import (
	"log/slog"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// RecoveryManager handles logging and recovery operations for a transaction.
// Each Transaction has a RecoveryManager.
// All RecoveryManagers share a single log manager and buffer manager.
type RecoveryManager struct {
	txNum            int64
	transaction      *Transaction
	logManager       *log.Manager
	bufferManager    *buffer.Manager
	dirtyPageTable   *DirtyPageTable
	transactionTable *TransactionTable
}

func NewRecoveryManager(txNum int64, transaction *Transaction, logManager *log.Manager, bufferManager *buffer.Manager, dirtyPageTable *DirtyPageTable, transactionTable *TransactionTable) *RecoveryManager {
	return &RecoveryManager{
		txNum:            txNum,
		transaction:      transaction,
		logManager:       logManager,
		bufferManager:    bufferManager,
		dirtyPageTable:   dirtyPageTable,
		transactionTable: transactionTable,
	}
}

func (rm *RecoveryManager) Commit() error {
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN

	// Write commit log record (WAL principle)
	err := WriteCommitLogRecord(rm.logManager, rm.txNum, lsn, prevLSN)
	if err != nil {
		return err
	}
	rm.transaction.prevTxLSN = lsn

	// Flush log up to commit record LSN (ARIES: no need to flush data buffers)
	err = rm.logManager.Flush(lsn)
	if err != nil {
		return err
	}

	return nil
}

func (rm *RecoveryManager) Rollback() error {
	// Perform undo operations first
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		if record.TxNumber() == rm.txNum {
			// Stop at Start log record
			if record.Op() == LogRecordStart {
				break
			}
			err := record.Undo(rm.transaction)
			if err != nil {
				return err
			}
		}
	}

	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN

	// Write rollback log record (WAL principle)
	err = WriteRollbackLogRecord(rm.logManager, rm.txNum, lsn, prevLSN)
	if err != nil {
		return err
	}
	rm.transaction.prevTxLSN = lsn

	// Flush log up to rollback record LSN (ARIES: no need to flush data buffers)
	err = rm.logManager.Flush(lsn)
	if err != nil {
		return err
	}

	return nil
}

// SetInt logs an integer modification operation before it occurs.
// It reads the current value from the buffer at the specified offset,
// writes a SetInt log record with the old value for potential rollback,
// and returns the LSN of the log record.
func (rm *RecoveryManager) SetInt(buf *buffer.Buffer, offset int, newValue int) (int64, error) {
	lsn := rm.logManager.GetNextLatestLSN()
	oldVal := buf.Contents().GetInt(offset)
	prevLSN := rm.transaction.prevTxLSN

	// 1. Write log record first (WAL principle)
	err := WriteSetIntLogRecord(rm.logManager, rm.txNum, lsn, prevLSN, buf.Block(), offset, oldVal, newValue)
	if err != nil {
		return 0, err
	}
	rm.transaction.prevTxLSN = lsn

	return lsn, nil
}

// SetString logs a string modification operation before it occurs.
// It reads the current value from the buffer at the specified offset,
// writes a SetString log record with the old value for potential rollback,
// and returns the LSN of the log record.
func (rm *RecoveryManager) SetString(buf *buffer.Buffer, offset int, newValue string) (int64, error) {
	lsn := rm.logManager.GetNextLatestLSN()
	oldVal := buf.Contents().GetString(offset)
	prevLSN := rm.transaction.prevTxLSN

	// 1. Write log record first (WAL principle)
	err := WriteSetStringLogRecord(rm.logManager, rm.txNum, lsn, prevLSN, buf.Block(), offset, oldVal, newValue)
	if err != nil {
		return 0, err
	}
	rm.transaction.prevTxLSN = lsn

	return lsn, nil
}

// SetBool logs a boolean modification operation before it occurs.
// It reads the current value from the buffer at the specified offset,
// writes a SetBool log record with the old value for potential rollback,
// and returns the LSN of the log record.
func (rm *RecoveryManager) SetBool(buf *buffer.Buffer, offset int, newValue bool) (int64, error) {
	lsn := rm.logManager.GetNextLatestLSN()
	oldVal := buf.Contents().GetBool(offset)
	prevLSN := rm.transaction.prevTxLSN

	// 1. Write log record first (WAL principle)
	err := WriteSetBoolLogRecord(rm.logManager, rm.txNum, lsn, prevLSN, buf.Block(), offset, oldVal, newValue)
	if err != nil {
		return 0, err
	}
	rm.transaction.prevTxLSN = lsn

	return lsn, nil
}

// Checkpoint writes a fuzzy checkpoint log record for recovery purposes.
// This should be called periodically (e.g., every N seconds or after N transactions).
// Checkpoint writes a fuzzy checkpoint log record for recovery purposes.
// Accepts snapshots of TransactionTable and DirtyPageTable as input.
func (rm *RecoveryManager) Checkpoint(txTableSnapshot map[int64]*TransactionEntry, dptSnapshot map[file.BlockID]*DirtyPageEntry) error {
	slog.Info("Saving fuzzy checkpoint...")
	lsn := rm.logManager.GetNextLatestLSN()

	// Write fuzzy checkpoint log record (WAL principle)
	err := WriteCheckpointLogRecord(rm.logManager, lsn, txTableSnapshot, dptSnapshot)
	if err != nil {
		return err
	}

	// Flush log up to checkpoint LSN
	err = rm.logManager.Flush(lsn)
	if err != nil {
		return err
	}

	return nil
}

// DBRecovery performs ARIES recovery:
// 1. Find the most recent checkpoint
// 2. Restore TransactionTable and DirtyPageTable from checkpoint
// 3. Perform analysis pass (identify winner/loser transactions)
// 4. Perform redo pass (redo all operations from minRecLSN)
// 5. Perform undo pass (undo loser transactions)
func (rm *RecoveryManager) DBRecovery() error {
	// Step 1: Find the most recent checkpoint and restore tables
	checkpointLSN, err := rm.findAndRestoreCheckpoint()
	if err != nil {
		return err
	}

	// Step 2: Analysis pass - identify winner and loser transactions
	// Winner: transactions that committed after checkpoint
	// Loser: transactions that were active at checkpoint or started after but didn't commit
	finishedTXs := make(map[int64]bool)
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	// Find all committed/rolled back transactions after checkpoint
	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		// Stop if we've gone past the checkpoint
		if record.Op() == LogRecordCheckpoint {
			if checkpointRecord, ok := record.(*CheckpointLogRecord); ok {
				if checkpointRecord.LSN() == checkpointLSN {
					break
				}
			}
		}

		if record.Op() == LogRecordCommit || record.Op() == LogRecordRollback {
			finishedTXs[record.TxNumber()] = true
		}
	}

	// Step 3: Redo pass - redo all operations from minRecLSN (minimum recLSN in DPT)
	// Calculate minRecLSN from DirtyPageTable (minimum recLSN of all dirty pages)
	minRecLSN := int64(-1)
	dptSnapshot := rm.dirtyPageTable.GetAll()
	for _, entry := range dptSnapshot {
		if minRecLSN < 0 || entry.RecLSN < minRecLSN {
			minRecLSN = entry.RecLSN
		}
	}
	// If no dirty pages, start from checkpoint LSN (or 0 if no checkpoint)
	if minRecLSN < 0 {
		if checkpointLSN >= 0 {
			minRecLSN = checkpointLSN
		} else {
			minRecLSN = 0
		}
	}

	// Perform redo pass: redo all log records from minRecLSN forward
	// Skip redo if PageLSN >= record LSN (ARIES optimization)
	lmIterator, err = rm.logManager.Iterator()
	if err != nil {
		return err
	}

	// Collect all log records first (we need to process them forward, but iterator goes backward)
	var records []LogRecord
	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		// Stop at checkpoint
		if record.Op() == LogRecordCheckpoint {
			if checkpointRecord, ok := record.(*CheckpointLogRecord); ok {
				if checkpointRecord.LSN() == checkpointLSN {
					break
				}
			}
		}

		// Only redo data modification records (SetInt, SetString, SetBool)
		if record.Op() == LogRecordSetInt || record.Op() == LogRecordSetString || record.Op() == LogRecordSetBool {
			var recordLSN int64
			switch r := record.(type) {
			case *SetIntLogRecord:
				recordLSN = r.LSN()
			case *SetStringLogRecord:
				recordLSN = r.LSN()
			case *SetBoolLogRecord:
				recordLSN = r.LSN()
			default:
				continue
			}

			// Only redo if record LSN >= minRecLSN
			if recordLSN >= minRecLSN {
				records = append(records, record)
			}
		}
	}

	// Process records in forward order (reverse the list since iterator goes backward)
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		redone, err := record.Redo(rm.transaction)
		if err != nil {
			return err
		}
		if redone {
			// Unpin the buffer after redo
			switch r := record.(type) {
			case *SetIntLogRecord:
				rm.transaction.Unpin(r.Block())
			case *SetStringLogRecord:
				rm.transaction.Unpin(r.Block())
			case *SetBoolLogRecord:
				rm.transaction.Unpin(r.Block())
			}
		}
	}

	// Step 4: Undo pass - undo all loser transactions
	// Loser transactions are those in TransactionTable that are not in finishedTXs
	lmIterator, err = rm.logManager.Iterator()
	if err != nil {
		return err
	}

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		// Stop at checkpoint
		if record.Op() == LogRecordCheckpoint {
			if checkpointRecord, ok := record.(*CheckpointLogRecord); ok {
				if checkpointRecord.LSN() == checkpointLSN {
					break
				}
			}
		}

		txNum := record.TxNumber()
		// Undo if this is a loser transaction (not finished)
		if txNum >= 0 && !finishedTXs[txNum] {
			err := record.Undo(rm.transaction)
			if err != nil {
				return err
			}
			// Unpin the buffer after undo to prevent buffer pool exhaustion during recovery
			if record.Op() == LogRecordSetInt {
				if setIntRecord, ok := record.(*SetIntLogRecord); ok {
					rm.transaction.Unpin(setIntRecord.block)
				}
			} else if record.Op() == LogRecordSetString {
				if setStringRecord, ok := record.(*SetStringLogRecord); ok {
					rm.transaction.Unpin(setStringRecord.block)
				}
			} else if record.Op() == LogRecordSetBool {
				if setBoolRecord, ok := record.(*SetBoolLogRecord); ok {
					rm.transaction.Unpin(setBoolRecord.block)
				}
			}
		}
	}

	return nil
}

// findAndRestoreCheckpoint finds the most recent checkpoint and restores
// TransactionTable and DirtyPageTable from it.
// Returns the checkpoint LSN, or -1 if no checkpoint found.
func (rm *RecoveryManager) findAndRestoreCheckpoint() (int64, error) {
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return -1, err
	}

	var checkpointLSN int64 = -1
	var checkpointRecord *CheckpointLogRecord

	// Find the most recent checkpoint
	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		if record.Op() == LogRecordCheckpoint {
			if cr, ok := record.(*CheckpointLogRecord); ok {
				checkpointLSN = cr.LSN()
				checkpointRecord = cr
				break
			}
		}
	}

	// If we found a checkpoint, restore the tables
	if checkpointRecord != nil {
		// Restore TransactionTable
		txTableSnapshot := checkpointRecord.TransactionTable()
		for txNum, entry := range txTableSnapshot {
			rm.transactionTable.Add(txNum, entry.Status, entry.LastLSN)
		}

		// Restore DirtyPageTable
		dptSnapshot := checkpointRecord.DirtyPageTable()
		for blockID, entry := range dptSnapshot {
			// Reconstruct BlockID from the key
			block := file.NewBlockID(blockID.Filename(), blockID.Number())
			rm.dirtyPageTable.Add(block, entry.RecLSN)
		}
	}

	return checkpointLSN, nil
}
