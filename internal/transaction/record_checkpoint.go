package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type CheckpointLogRecord struct {
	LogRecord
}

// NewCheckpointLogRecord creates a new StartLogRecord
// Page format: [op(4)]
func NewCheckpointLogRecord(page *file.Page) *CheckpointLogRecord {
	return &CheckpointLogRecord{}
}

// Op returns the operation type for this log record
func (s *CheckpointLogRecord) Op() LogRecordType {
	return LogRecordCheckpoint
}

// TxNumber returns the transaction number associated with this log record
func (s *CheckpointLogRecord) TxNumber() int {
	// Checkpoint record is not associated with any transaction
	return -1
}

// Undo performs the undo operation for this log record
func (s *CheckpointLogRecord) Undo(tx *Transaction) error {
	// No need to undo anything for Checkpoint Record
	return nil
}

// WriteCheckpointLogRecord writes a CheckpointLogRecord to the log manager
func WriteCheckpointLogRecord(lm *log.Manager) (int, error) {
	opPos := 0
	finalLen := opPos + LogRecordTypeSize()

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordCheckpoint))

	return lm.Append(page.Bytes())
}
