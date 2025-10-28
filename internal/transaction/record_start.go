package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type StartLogRecord struct {
	LogRecord
	txNum int
}

// NewStartLogRecord creates a new StartLogRecord
// Page format: [op(4)] [txNum(4)]
func NewStartLogRecord(page *file.Page) *StartLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt(txNumPos)

	return &StartLogRecord{
		txNum: txNum,
	}
}

// Op returns the operation type for this log record
func (s *StartLogRecord) Op() LogRecordType {
	return LogRecordStart
}

// TxNumber returns the transaction number associated with this log record
func (s *StartLogRecord) TxNumber() int {
	return s.txNum
}

// Undo performs the undo operation for this log record
func (s *StartLogRecord) Undo(tx *Transaction) {
	// No need to undo anything for Start Record
}

// WriteStartLogRecord writes a StartLogRecord to the log manager
func WriteStartLogRecord(lm *log.Manager, txNum int) (int, error) {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	finalLen := txNumPos + 4

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordStart))
	page.SetInt(txNumPos, txNum)

	return lm.Append(page.Bytes())
}
