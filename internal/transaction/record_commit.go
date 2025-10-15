package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type CommitLogRecord struct {
	LogRecord
	txNum int
}

// NewCommitLogRecord creates a new CommitLogRecord
// Page format: [op(4)] [txNum(4)]
func NewCommitLogRecord(page *file.Page) *CommitLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt(txNumPos)

	return &CommitLogRecord{
		txNum: txNum,
	}
}

// Op returns the operation type for this log record
func (s *CommitLogRecord) Op() LogRecordType {
	return LogRecordCommit
}

// TxNumber returns the transaction number associated with this log record
func (s *CommitLogRecord) TxNumber() int {
	return s.txNum
}

// Undo performs the undo operation for this log record
func (s *CommitLogRecord) Undo(tx *Transaction) {
	// No need to undo anything for Commit Record
}

// WriteCommitLogRecord writes a CommitLogRecord to the log manager
func WriteCommitLogRecord(lm *log.Manager, txNum int) int {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	finalLen := txNumPos + 4

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordCommit))
	page.SetInt(txNumPos, txNum)

	return lm.Append(page.Bytes())
}
