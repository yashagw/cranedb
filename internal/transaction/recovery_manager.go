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
	dirtyPageTable   *buffer.DirtyPageTable
	transactionTable *TransactionTable
}

func NewRecoveryManager(txNum int64, transaction *Transaction, logManager *log.Manager, bufferManager *buffer.Manager, dirtyPageTable *buffer.DirtyPageTable, transactionTable *TransactionTable) *RecoveryManager {
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
	// TODO: PERFORMANCE BOTTLENECK
	// Currently, we scan the entire log file to find records for this transaction.
	// This is inefficient (O(N) of total log size) and memory intensive.
	// FUTURE OPTIMIZATION:
	// 1. Implement random access in LogManager (ReadLogRecord(lsn)).
	//    The LSN should map to a physical offset in the log file.
	// 2. Instead of building this map, we should traverse the log chain backwards
	//    one by one using the PrevLSN pointer in each record:
	//    CurrentLSN -> Record.PrevLSN -> ... -> StartRecord.

	// Build a map of log records for this transaction
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	txLogRecords := make(map[int64]LogRecord)
	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		if record.TxNumber() == rm.txNum {
			txLogRecords[record.LSN()] = record
		}
	}

	// Perform ARIES undo with CLR generation
	err = rm.undoTransaction(rm.txNum, rm.transaction.prevTxLSN, txLogRecords)
	if err != nil {
		return err
	}

	// Write rollback log record
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN
	err = WriteRollbackLogRecord(rm.logManager, rm.txNum, lsn, prevLSN)
	if err != nil {
		return err
	}
	rm.transaction.prevTxLSN = lsn

	// Flush log up to rollback record LSN
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
func (rm *RecoveryManager) Checkpoint(txTableSnapshot map[int64]*TransactionEntry, dptSnapshot map[file.BlockID]*buffer.DirtyPageEntry) error {
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

	// Step 2: Analysis pass - rebuild Transaction Table and Dirty Page Table
	// Scan from checkpoint to end of log, updating tables based on log records
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	// Collect all log records from end of log back to checkpoint
	var logRecords []LogRecord
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
		logRecords = append(logRecords, record)
	}

	// Process records in forward order (reverse since iterator goes backward)
	finishedTXs := make(map[int64]bool)
	for i := len(logRecords) - 1; i >= 0; i-- {
		record := logRecords[i]
		txNum := record.TxNumber()

		switch record.Op() {
		case LogRecordStart:
			// Add new transaction to Transaction Table
			if startRecord, ok := record.(*StartLogRecord); ok {
				rm.transactionTable.Add(txNum, TransactionStatusRunning, startRecord.LSN())
			}

		case LogRecordCommit:
			// Mark transaction as committed and update lastLSN
			finishedTXs[txNum] = true
			if commitRecord, ok := record.(*CommitLogRecord); ok {
				rm.transactionTable.UpdateStatus(txNum, TransactionStatusCommitted)
				rm.transactionTable.UpdateLastLSN(txNum, commitRecord.LSN())
			}

		case LogRecordRollback:
			// Mark transaction as aborted and update lastLSN
			finishedTXs[txNum] = true
			if rollbackRecord, ok := record.(*RollbackLogRecord); ok {
				rm.transactionTable.UpdateStatus(txNum, TransactionStatusAborted)
				rm.transactionTable.UpdateLastLSN(txNum, rollbackRecord.LSN())
			}

		case LogRecordSetInt, LogRecordSetString, LogRecordSetBool:
			// Update Transaction Table lastLSN for data modification records
			if dataRecord, ok := record.(DataModificationRecord); ok {
				recordLSN := record.LSN()
				blockID := dataRecord.Block()

				// Update transaction's lastLSN
				if entry, exists := rm.transactionTable.Get(txNum); exists {
					if recordLSN > entry.LastLSN {
						rm.transactionTable.UpdateLastLSN(txNum, recordLSN)
					}
				} else {
					// Transaction not in table - add it as running
					rm.transactionTable.Add(txNum, TransactionStatusRunning, recordLSN)
				}

				// Add page to Dirty Page Table if not already present
				rm.dirtyPageTable.Add(blockID, recordLSN)
			}

		case LogRecordCLR:
			// Update Transaction Table lastLSN for CLR records
			if dataRecord, ok := record.(DataModificationRecord); ok {
				recordLSN := record.LSN()
				blockID := dataRecord.Block()

				// Update transaction's lastLSN
				if entry, exists := rm.transactionTable.Get(txNum); exists {
					if recordLSN > entry.LastLSN {
						rm.transactionTable.UpdateLastLSN(txNum, recordLSN)
					}
				} else {
					// Transaction not in table - add it as running
					rm.transactionTable.Add(txNum, TransactionStatusRunning, recordLSN)
				}

				// Add page to Dirty Page Table if not already present
				rm.dirtyPageTable.Add(blockID, recordLSN)
			}
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

		// Only redo data modification records (SetInt, SetString, SetBool, CLR)
		if record.Op() == LogRecordSetInt || record.Op() == LogRecordSetString || record.Op() == LogRecordSetBool || record.Op() == LogRecordCLR {
			recordLSN := record.LSN()

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
			case *CLRLogRecord:
				rm.transaction.Unpin(r.Block())
			}
		}
	}

	// Step 4: Undo pass - undo all loser transactions using CLRs
	// Loser transactions are those in TransactionTable that are not in finishedTXs

	// Get all loser transactions with their lastLSN values
	loserTxs := make(map[int64]int64) // txNum -> lastLSN
	txTableSnapshot := rm.transactionTable.GetAll()
	for txNum, entry := range txTableSnapshot {
		if !finishedTXs[txNum] {
			loserTxs[txNum] = entry.LastLSN
		}
	}

	// Build a map of all log records for efficient lookup by LSN
	allLogRecords := make(map[int64]LogRecord)
	lmIterator, err = rm.logManager.Iterator()
	if err != nil {
		return err
	}

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		// All log records now have LSN() method from the interface
		allLogRecords[record.LSN()] = record
	}

	// Undo each loser transaction by following its LSN chain backwards
	for txNum, lastLSN := range loserTxs {
		err = rm.undoTransaction(txNum, lastLSN, allLogRecords)
		if err != nil {
			return err
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

// undoTransaction performs ARIES undo for a single transaction
// Follows the transaction's LSN chain backwards, undoing operations and generating CLRs
func (rm *RecoveryManager) undoTransaction(txNum int64, lastLSN int64, allLogRecords map[int64]LogRecord) error {
	currentLSN := lastLSN

undoLoop:
	for currentLSN >= 0 {
		record, exists := allLogRecords[currentLSN]
		if !exists {
			// LSN not found - this shouldn't happen in a correct log
			slog.Warn("LSN not found in log records during undo", "txNum", txNum, "LSN", currentLSN)
			break undoLoop
		}

		// Only process records for this transaction
		if record.TxNumber() != txNum {
			// Mismatched transaction - this shouldn't happen in a correct log
			slog.Warn("Mismatched transaction in log records during undo", "expectedTxNum", txNum, "foundTxNum", record.TxNumber(), "LSN", currentLSN)
			break undoLoop
		}

		var nextLSN int64 = -1

		switch record.Op() {
		case LogRecordSetInt, LogRecordSetString, LogRecordSetBool:
			// Undo the operation and generate a CLR
			err := rm.undoDataModification(record)
			if err != nil {
				return err
			}
			nextLSN = record.PrevLSN()
		case LogRecordCLR:
			// For CLR, follow undoNextLSN instead of prevLSN
			if clrRecord, ok := record.(*CLRLogRecord); ok {
				nextLSN = clrRecord.UndoNextLSN()
			}
		case LogRecordStart:
			// Reached start of transaction - we're done
			break undoLoop
		case LogRecordRollback:
			nextLSN = record.PrevLSN()
		case LogRecordCommit:
			// It should not happen to encounter a commit record during undo
			slog.Error("Encountered commit record during undo", "txNum", txNum, "LSN", currentLSN)
			break undoLoop
		default:
			// Unexpected record type
			slog.Error("Unexpected log record type during undo", "txNum", txNum, "LSN", currentLSN, "Op", record.Op())
			break undoLoop
		}

		currentLSN = nextLSN
	}

	return nil
}

// undoDataModification undoes a data modification operation and generates a CLR
func (rm *RecoveryManager) undoDataModification(record LogRecord) error {
	switch r := record.(type) {
	case *SetIntLogRecord:
		// Perform the undo operation (restore old value)
		err := rm.transaction.SetInt(r.Block(), r.Offset(), r.OldValue(), false)
		if err != nil {
			return err
		}

		// Generate CLR
		clrLSN := rm.logManager.GetNextLatestLSN()
		prevLSN := rm.transaction.prevTxLSN
		undoNextLSN := r.PrevLSN()

		err = WriteCLRLogRecord(rm.logManager, rm.txNum, clrLSN, prevLSN, undoNextLSN,
			LogRecordSetInt, r.Block(), r.Offset(), r.OldValue(), "", false)
		if err != nil {
			return err
		}
		rm.transaction.prevTxLSN = clrLSN

		// Update LSN on the page after undo
		err = rm.transaction.ForceUpdatePageLSN(r.Block(), clrLSN)
		if err != nil {
			return err
		}

		// Unpin the buffer after undo
		rm.transaction.Unpin(r.Block())
	case *SetStringLogRecord:
		// Perform the undo operation (restore old value)
		err := rm.transaction.SetString(r.Block(), r.Offset(), r.OldValue(), false)
		if err != nil {
			return err
		}

		// Generate CLR
		clrLSN := rm.logManager.GetNextLatestLSN()
		prevLSN := rm.transaction.prevTxLSN
		undoNextLSN := r.PrevLSN()

		err = WriteCLRLogRecord(rm.logManager, rm.txNum, clrLSN, prevLSN, undoNextLSN,
			LogRecordSetString, r.Block(), r.Offset(), 0, r.OldValue(), false)
		if err != nil {
			return err
		}
		rm.transaction.prevTxLSN = clrLSN

		err = rm.transaction.ForceUpdatePageLSN(r.Block(), clrLSN)
		if err != nil {
			return err
		}

		// Unpin the buffer after undo
		rm.transaction.Unpin(r.Block())
	case *SetBoolLogRecord:
		// Perform the undo operation (restore old value)
		err := rm.transaction.SetBool(r.Block(), r.Offset(), r.OldValue(), false)
		if err != nil {
			return err
		}

		// Generate CLR
		clrLSN := rm.logManager.GetNextLatestLSN()
		prevLSN := rm.transaction.prevTxLSN
		undoNextLSN := r.PrevLSN()

		err = WriteCLRLogRecord(rm.logManager, rm.txNum, clrLSN, prevLSN, undoNextLSN,
			LogRecordSetBool, r.Block(), r.Offset(), 0, "", r.OldValue())
		if err != nil {
			return err
		}
		rm.transaction.prevTxLSN = clrLSN

		err = rm.transaction.ForceUpdatePageLSN(r.Block(), clrLSN)
		if err != nil {
			return err
		}

		// Unpin the buffer after undo
		rm.transaction.Unpin(r.Block())
	}

	return nil
}
