package transaction

import (
	"fmt"
	"hash/crc32"

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
	LogRecordCLR        LogRecordType = 7
)

// LogRecord interface
type LogRecord interface {
	Op() LogRecordType
	TxNumber() int64
	LSN() int64
	PrevLSN() int64
	Undo(tx *Transaction) error
	Redo(tx *Transaction) (bool, error)
}

// DataModificationRecord interface for records that modify data blocks
type DataModificationRecord interface {
	LogRecord
	Block() *file.BlockID
}

var _ LogRecord = (*StartLogRecord)(nil)
var _ LogRecord = (*CommitLogRecord)(nil)
var _ LogRecord = (*RollbackLogRecord)(nil)
var _ LogRecord = (*CheckpointLogRecord)(nil)
var _ DataModificationRecord = (*SetIntLogRecord)(nil)
var _ DataModificationRecord = (*SetStringLogRecord)(nil)
var _ DataModificationRecord = (*SetBoolLogRecord)(nil)
var _ DataModificationRecord = (*CLRLogRecord)(nil)

// CRC32ChecksumSize returns the size of the CRC32 checksum in bytes
func CRC32ChecksumSize() int {
	return 4
}

// calculateCRC32 calculates the CRC32 checksum for the given data using IEEE polynomial
func calculateCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// verifyCRC32 verifies that the data matches the expected CRC32 checksum
func verifyCRC32(data []byte, expectedCRC uint32) error {
	actualCRC := calculateCRC32(data)
	if actualCRC != expectedCRC {
		return fmt.Errorf("CRC32 checksum mismatch: expected 0x%08x, got 0x%08x - log record is corrupted", expectedCRC, actualCRC)
	}
	return nil
}

// appendCRC32 appends the CRC32 checksum to the page and returns the total length
func appendCRC32(page *file.Page, dataLen int) int {
	crc := calculateCRC32(page.Bytes()[:dataLen])
	page.SetIntRaw(dataLen, int(crc))
	return dataLen + CRC32ChecksumSize()
}

// CreateLogRecord returns the correct LogRecord based on the operation type
func CreateLogRecord(bytes []byte) (LogRecord, error) {
	if len(bytes) < CRC32ChecksumSize() {
		return nil, fmt.Errorf("log record too short to contain CRC32 checksum: got %d bytes", len(bytes))
	}

	fullPage := file.NewPageFromBytes(bytes)

	dataLen := len(bytes) - CRC32ChecksumSize()
	recordData := bytes[:dataLen]
	storedCRC := uint32(fullPage.GetIntRaw(dataLen))
	if err := verifyCRC32(recordData, storedCRC); err != nil {
		return nil, err
	}

	dataPage := file.NewPageFromBytes(recordData)
	op := dataPage.GetIntRaw(0)

	switch LogRecordType(op) {
	case LogRecordCheckpoint:
		return NewCheckpointLogRecord(dataPage), nil
	case LogRecordStart:
		return NewStartLogRecord(dataPage), nil
	case LogRecordCommit:
		return NewCommitLogRecord(dataPage), nil
	case LogRecordRollback:
		return NewRollbackLogRecord(dataPage), nil
	case LogRecordSetInt:
		return NewSetIntLogRecord(dataPage), nil
	case LogRecordSetString:
		return NewSetStringLogRecord(dataPage), nil
	case LogRecordSetBool:
		return NewSetBoolLogRecord(dataPage), nil
	case LogRecordCLR:
		return NewCLRLogRecord(dataPage), nil
	default:
		return nil, fmt.Errorf("invalid log record operation type: %d", op)
	}
}
