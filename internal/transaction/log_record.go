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
	LogRecordSetBool    LogRecordType = 6
)

// LogRecord interface
type LogRecord interface {
	Op() LogRecordType
	TxNumber() int64
	Undo(tx *Transaction) error
	Redo(tx *Transaction) (bool, error)
}

// CreateLogRecord returns the correct LogRecord based on the operation type
func CreateLogRecord(bytes []byte) LogRecord {
	page := file.NewPageFromBytes(bytes)

	// First 4 bytes is the operation type
	// Use Raw method since log records don't have pageLSN header
	op := page.GetIntRaw(0)
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
	case LogRecordSetBool:
		return NewSetBoolLogRecord(page)
	default:
		panic("invalid operation type")
	}
}
