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
	txNum         int64
	transaction   *Transaction
	logManager    *log.Manager
	bufferManager *buffer.Manager
}

func NewRecoveryManager(txNum int64, transaction *Transaction, logManager *log.Manager, bufferManager *buffer.Manager) *RecoveryManager {
	return &RecoveryManager{
		txNum:         txNum,
		transaction:   transaction,
		logManager:    logManager,
		bufferManager: bufferManager,
	}
}

func (rm *RecoveryManager) Commit() error {
	err := rm.bufferManager.FlushAll(rm.txNum)
	if err != nil {
		return err
	}
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN
	err = WriteCommitLogRecord(rm.logManager, rm.txNum, lsn, prevLSN)
	if err != nil {
		return err
	}
	rm.transaction.prevTxLSN = lsn
	return rm.logManager.Flush(lsn)
}

func (rm *RecoveryManager) Rollback() error {
	err := rm.doRollback()
	if err != nil {
		return err
	}
	err = rm.bufferManager.FlushAll(rm.txNum)
	if err != nil {
		return err
	}
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN
	err = WriteRollbackLogRecord(rm.logManager, rm.txNum, lsn, prevLSN)
	if err != nil {
		return err
	}
	rm.transaction.prevTxLSN = lsn
	return rm.logManager.Flush(lsn)
}

func (rm *RecoveryManager) Recover() error {
	err := rm.doRecovery()
	if err != nil {
		return err
	}
	err = rm.bufferManager.FlushAll(rm.txNum)
	if err != nil {
		return err
	}
	lsn := rm.logManager.GetNextLatestLSN()
	err = WriteCheckpointLogRecord(rm.logManager, lsn)
	if err != nil {
		return err
	}
	return rm.logManager.Flush(lsn)
}

// SetInt logs an integer modification operation before it occurs.
// It reads the current value from the buffer at the specified offset,
// writes a SetInt log record with the old value for potential rollback,
// and returns the LSN of the log record.
func (rm *RecoveryManager) SetInt(buf *buffer.Buffer, offset int, newValue int) (int64, error) {
	oldVal := buf.Contents().GetInt(offset)
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN
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
	oldVal := buf.Contents().GetString(offset)
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN
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
	oldVal := buf.Contents().GetBool(offset)
	lsn := rm.logManager.GetNextLatestLSN()
	prevLSN := rm.transaction.prevTxLSN
	err := WriteSetBoolLogRecord(rm.logManager, rm.txNum, lsn, prevLSN, buf.Block(), offset, oldVal, newValue)
	if err != nil {
		return 0, err
	}
	rm.transaction.prevTxLSN = lsn
	return lsn, nil
}

// doRollback undoes all operations for the current transaction by scanning the log records
// backwards. For each log record belonging to this transaction, it performs the corresponding
// undo operation, stopping when it reaches the transaction's Start record.
func (rm *RecoveryManager) doRollback() error {
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		if record.TxNumber() == rm.txNum {
			// If reached Start then we can stop
			if record.Op() == LogRecordStart {
				break
			}
			err := record.Undo(rm.transaction)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// doRecovery performs database recovery by reading the log records backward
// and undoes any uncompleted transactions.
// Recovery stops if it reaches the start of the log or a checkpoint record.
func (rm *RecoveryManager) doRecovery() error {
	finishedTXs := []int64{}
	lmIterator, err := rm.logManager.Iterator()
	if err != nil {
		return err
	}

	for lmIterator.HasNext() {
		logBytes := lmIterator.Next()
		record := CreateLogRecord(logBytes)

		// If reached Checkpoint then it means
		// above this logs everything is committed and we can stop
		if record.Op() == LogRecordCheckpoint {
			return nil
		}

		if record.Op() == LogRecordCommit || record.Op() == LogRecordRollback {
			finishedTXs = append(finishedTXs, record.TxNumber())
		}

		if !slices.Contains(finishedTXs, record.TxNumber()) {
			err := record.Undo(rm.transaction)
			if err != nil {
				return err
			}
			// Unpin the buffer after undo to prevent buffer pool exhaustion during recovery
			// Only unpin if this is a SetInt/SetString/SetBool record (they have block info)
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
