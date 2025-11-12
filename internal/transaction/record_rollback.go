package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type RollbackLogRecord struct {
	LogRecord
	txNum int
}

// NewRollbackLogRecord creates a new RollbackLogRecord
// Page format: [op(4)] [txNum(4)]
func NewRollbackLogRecord(page *file.Page) *RollbackLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt(txNumPos)

	return &RollbackLogRecord{
		txNum: txNum,
	}
}

// Op returns the operation type for this log record
func (s *RollbackLogRecord) Op() LogRecordType {
	return LogRecordRollback
}

// TxNumber returns the transaction number associated with this log record
func (s *RollbackLogRecord) TxNumber() int {
	return s.txNum
}

// Undo performs the undo operation for this log record
func (s *RollbackLogRecord) Undo(tx *Transaction) error {
	// No need to undo anything for Rollback Record
	return nil
}

// WriteRollbackLogRecord writes a RollbackLogRecord to the log manager
func WriteRollbackLogRecord(lm *log.Manager, txNum int) (int, error) {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	finalLen := txNumPos + 4

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordRollback))
	page.SetInt(txNumPos, txNum)

	return lm.Append(page.Bytes())
}
