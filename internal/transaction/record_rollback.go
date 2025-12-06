package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type RollbackLogRecord struct {
	LogRecord
	txNum   int64
	lsn     int64
	prevLSN int64
}

// NewRollbackLogRecord creates a new RollbackLogRecord
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)]
func NewRollbackLogRecord(page *file.Page) *RollbackLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt64Raw(txNumPos)

	lsnPos := txNumPos + 8
	lsn := page.GetInt64Raw(lsnPos)

	prevLSNPos := lsnPos + 8
	prevLSN := page.GetInt64Raw(prevLSNPos)

	return &RollbackLogRecord{
		txNum:   txNum,
		lsn:     lsn,
		prevLSN: prevLSN,
	}
}

// Op returns the operation type for this log record
func (s *RollbackLogRecord) Op() LogRecordType {
	return LogRecordRollback
}

// TxNumber returns the transaction number associated with this log record
func (s *RollbackLogRecord) TxNumber() int64 {
	return s.txNum
}

// LSN returns the log sequence number for this record
func (s *RollbackLogRecord) LSN() int64 {
	return s.lsn
}

// PrevLSN returns the previous log sequence number for this record
func (s *RollbackLogRecord) PrevLSN() int64 {
	return s.prevLSN
}

// Undo performs the undo operation for this log record
func (s *RollbackLogRecord) Undo(tx *Transaction) error {
	// No need to undo anything for Rollback Record
	return nil
}

// WriteRollbackLogRecord writes a RollbackLogRecord to the log manager
func WriteRollbackLogRecord(lm *log.Manager, txNum int64, lsn int64, prevLSN int64) error {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	finalLen := prevLSNPos + 8

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordRollback))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)

	return lm.Append(page.Bytes(), lsn)
}
