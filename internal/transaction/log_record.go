package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
)

type LogRecordType int

func LogRecordTypeSize() int {
	return 4
}

// Log operation type constants
const (
	LogRecordCheckpoint LogRecordType = 0
	LogRecordStart      LogRecordType = 1
	LogRecordCommit     LogRecordType = 2
	LogRecordRollback   LogRecordType = 3
	LogRecordSetInt     LogRecordType = 4
	LogRecordSetString  LogRecordType = 5
)

// LogRecord interface
type LogRecord interface {
	Op() LogRecordType
	TxNumber() int
	Undo(tx *Transaction)
}

// CreateLogRecord returns the correct LogRecord based on the operation type
func CreateLogRecord(bytes []byte) LogRecord {
	page := file.NewPage(len(bytes))

	// First 4 bytes is the operation type
	op := page.GetInt(0)
	switch LogRecordType(op) {
	case LogRecordCheckpoint:
		return NewCheckpointLogRecord(page)
	case LogRecordStart:
		return NewStartLogRecord(page)
	case LogRecordCommit:
		return NewCommitLogRecord(page)
	case LogRecordRollback:
		return NewRollbackLogRecord(page)
	case LogRecordSetInt:
		return NewSetIntLogRecord(page)
	case LogRecordSetString:
		return NewSetStringLogRecord(page)
	default:
		panic("invalid operation type")
	}
}
