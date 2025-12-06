package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type CheckpointLogRecord struct {
	LogRecord
	lsn int64
}

// NewCheckpointLogRecord creates a new CheckpointLogRecord
// Page format: [op(4)] [lsn(8)]
func NewCheckpointLogRecord(page *file.Page) *CheckpointLogRecord {
	opPos := 0
	lsnPos := opPos + LogRecordTypeSize()
	lsn := page.GetInt64Raw(lsnPos)

	return &CheckpointLogRecord{
		lsn: lsn,
	}
}

// Op returns the operation type for this log record
func (s *CheckpointLogRecord) Op() LogRecordType {
	return LogRecordCheckpoint
}

// TxNumber returns the transaction number associated with this log record
func (s *CheckpointLogRecord) TxNumber() int64 {
	// Checkpoint record is not associated with any transaction
	return -1
}

// LSN returns the log sequence number for this record
func (s *CheckpointLogRecord) LSN() int64 {
	return s.lsn
}

// Undo performs the undo operation for this log record
func (s *CheckpointLogRecord) Undo(tx *Transaction) error {
	// No need to undo anything for Checkpoint Record
	return nil
}

// WriteCheckpointLogRecord writes a CheckpointLogRecord to the log manager
func WriteCheckpointLogRecord(lm *log.Manager, lsn int64) error {
	opPos := 0
	lsnPos := opPos + LogRecordTypeSize()
	finalLen := lsnPos + 8

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordCheckpoint))
	page.SetInt64Raw(lsnPos, lsn)

	return lm.Append(page.Bytes(), lsn)
}
