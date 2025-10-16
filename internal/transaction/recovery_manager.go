package transaction

import (
	"slices"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/log"
)

// RecoveryManager implements the undo-only algorithm
// Each Transaction has a RecoveryManager
// All RecoveryManager shares a single log manager and buffer manager
// DB Server Itself also has a RecoveryManager used for recovery after a crash
type RecoveryManager struct {
	txNum         int
	transaction   *Transaction
	logManager    *log.Manager
	bufferManager *buffer.Manager
}

func NewRecoveryManager(txNum int, transaction *Transaction, logManager *log.Manager, bufferManager *buffer.Manager) *RecoveryManager {
	return &RecoveryManager{
		txNum:         txNum,
		transaction:   transaction,
		logManager:    logManager,
		bufferManager: bufferManager,
	}
}

func (rm *RecoveryManager) Commit() {
	rm.bufferManager.FlushAll(rm.txNum)
	lsn := WriteCommitLogRecord(rm.logManager, rm.txNum)
	rm.logManager.Flush(lsn)
}

func (rm *RecoveryManager) Rollback() {
	rm.doRollback()
	rm.bufferManager.FlushAll(rm.txNum)
	lsn := WriteCommitLogRecord(rm.logManager, rm.txNum)
	rm.logManager.Flush(lsn)
}

func (rm *RecoveryManager) Recover() {
	rm.doRecovery()
	rm.bufferManager.FlushAll(rm.txNum)
	lsn := WriteCheckpointLogRecord(rm.logManager)
	rm.logManager.Flush(lsn)
}

// SetInt logs an integer modification operation before it occurs.
// It reads the current value from the buffer at the specified offset,
// writes a SetInt log record with the old value for potential rollback,
// and returns the LSN of the log record.
func (rm *RecoveryManager) SetInt(buf *buffer.Buffer, offset int) int {
	oldVal := buf.Contents().GetInt(offset)
	return WriteSetIntLogRecord(rm.logManager, rm.txNum, buf.Block(), offset, oldVal)
}

// SetString logs a string modification operation before it occurs.
// It reads the current value from the buffer at the specified offset,
// writes a SetString log record with the old value for potential rollback,
// and returns the LSN of the log record.
func (rm *RecoveryManager) SetString(buf *buffer.Buffer, offset int) int {
	oldVal := buf.Contents().GetString(offset)
	return WriteSetStringLogRecord(rm.logManager, rm.txNum, buf.Block(), offset, oldVal)
}

// doRollback undoes all operations for the current transaction by scanning the log records
// backwards. For each log record belonging to this transaction, it performs the corresponding
// undo operation, stopping when it reaches the transaction's Start record.
func (rm *RecoveryManager) doRollback() {
	lmIterator := rm.logManager.Iterator()

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		if record.TxNumber() == rm.txNum {
			// If reached Start then we can stop
			if record.Op() == LogRecordStart {
				break
			}
			record.Undo(rm.transaction)
		}
	}
}

// doRecovery performs database recovery by reading the log records backward
// and undoes any uncompleted transactions.
// Recovery stops if it reaches the start of the log or a checkpoint record.
func (rm *RecoveryManager) doRecovery() {
	finishedTXs := []int{}
	lmIterator := rm.logManager.Iterator()

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		// If reached Checkpoint then it means
		// above this logs everything is committed and we can stop
		if record.Op() == LogRecordCheckpoint {
			return
		}

		if record.Op() == LogRecordCommit || record.Op() == LogRecordRollback {
			finishedTXs = append(finishedTXs, record.TxNumber())
		}

		if !slices.Contains(finishedTXs, record.TxNumber()) {
			record.Undo(rm.transaction)
		}
	}
}
