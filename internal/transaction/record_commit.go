package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type CommitLogRecord struct {
	LogRecord
	txNum   int64
	lsn     int64
	prevLSN int64
}

// NewCommitLogRecord creates a new CommitLogRecord
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)]
func NewCommitLogRecord(page *file.Page) *CommitLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt64Raw(txNumPos)

	lsnPos := txNumPos + 8
	lsn := page.GetInt64Raw(lsnPos)

	prevLSNPos := lsnPos + 8
	prevLSN := page.GetInt64Raw(prevLSNPos)

	return &CommitLogRecord{
		txNum:   txNum,
		lsn:     lsn,
		prevLSN: prevLSN,
	}
}

// Op returns the operation type for this log record
func (s *CommitLogRecord) Op() LogRecordType {
	return LogRecordCommit
}

// TxNumber returns the transaction number associated with this log record
func (s *CommitLogRecord) TxNumber() int64 {
	return s.txNum
}

// LSN returns the log sequence number for this record
func (s *CommitLogRecord) LSN() int64 {
	return s.lsn
}

// PrevLSN returns the previous log sequence number for this record
func (s *CommitLogRecord) PrevLSN() int64 {
	return s.prevLSN
}

// Undo performs the undo operation for this log record
func (s *CommitLogRecord) Undo(tx *Transaction) error {
	// No need to undo anything for Commit Record
	return nil
}

// Redo performs the redo operation for this log record.
// Commit records don't need redo (they're metadata).
func (s *CommitLogRecord) Redo(tx *Transaction) (bool, error) {
	return false, nil
}

// WriteCommitLogRecord writes a CommitLogRecord to the log manager
func WriteCommitLogRecord(lm *log.Manager, txNum int64, lsn int64, prevLSN int64) error {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	finalLen := prevLSNPos + 8

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordCommit))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)

	return lm.Append(page.Bytes(), lsn)
}
