package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

func TestSetIntLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	// Test data
	fileName := "test_file"
	blockNum := 5
	blockID := file.NewBlockID(fileName, blockNum)

	txNum := int64(42)
	offset := 100
	oldValue := 12345
	newValue := 67890
	lsn := logManager.GetNextLatestLSN()
	prevLSN := int64(-1)

	err = WriteSetIntLogRecord(logManager, txNum, lsn, prevLSN, blockID, offset, oldValue, newValue)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewSetIntLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, txNum, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, prevLSN, decodedRecord.prevLSN, "PrevLSN mismatch")
	assert.Equal(t, offset, decodedRecord.offset, "Offset mismatch")
	assert.Equal(t, oldValue, decodedRecord.oldValue, "Old value mismatch")
	assert.Equal(t, newValue, decodedRecord.newValue, "New value mismatch")
	assert.Equal(t, fileName, decodedRecord.block.Filename(), "Filename mismatch")
	assert.Equal(t, blockNum, decodedRecord.block.Number(), "Block number mismatch")
	assert.Equal(t, LogRecordSetInt, decodedRecord.Op())
}

func TestSetStringLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	// Test data
	fileName := "test_file"
	blockNum := 5
	blockID := file.NewBlockID(fileName, blockNum)

	txNum := int64(42)
	offset := 100
	oldValue := "old_test_value"
	newValue := "new_test_value"
	lsn := logManager.GetNextLatestLSN()
	prevLSN := int64(-1)

	err = WriteSetStringLogRecord(logManager, txNum, lsn, prevLSN, blockID, offset, oldValue, newValue)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewSetStringLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, txNum, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, prevLSN, decodedRecord.prevLSN, "PrevLSN mismatch")
	assert.Equal(t, offset, decodedRecord.offset, "Offset mismatch")
	assert.Equal(t, oldValue, decodedRecord.oldValue, "Old value mismatch")
	assert.Equal(t, newValue, decodedRecord.newValue, "New value mismatch")
	assert.Equal(t, fileName, decodedRecord.block.Filename(), "Filename mismatch")
	assert.Equal(t, blockNum, decodedRecord.block.Number(), "Block number mismatch")
	assert.Equal(t, LogRecordSetString, decodedRecord.Op())
}

func TestStartLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	txNum := int64(42)
	lsn := logManager.GetNextLatestLSN()
	prevLSN := int64(-1)

	err = WriteStartLogRecord(logManager, txNum, lsn, prevLSN)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewStartLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, txNum, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, prevLSN, decodedRecord.prevLSN, "PrevLSN mismatch")
	assert.Equal(t, LogRecordStart, decodedRecord.Op())
}

func TestCommitLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	txNum := int64(42)
	lsn := logManager.GetNextLatestLSN()
	prevLSN := int64(-1)

	err = WriteCommitLogRecord(logManager, txNum, lsn, prevLSN)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewCommitLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, txNum, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, prevLSN, decodedRecord.prevLSN, "PrevLSN mismatch")
	assert.Equal(t, LogRecordCommit, decodedRecord.Op())
}

func TestRollbackLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	txNum := int64(42)
	lsn := logManager.GetNextLatestLSN()
	prevLSN := int64(-1)

	err = WriteRollbackLogRecord(logManager, txNum, lsn, prevLSN)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewRollbackLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, txNum, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, prevLSN, decodedRecord.prevLSN, "PrevLSN mismatch")
	assert.Equal(t, LogRecordRollback, decodedRecord.Op())
}

func TestCheckpointLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	lsn := logManager.GetNextLatestLSN()
	err = WriteCheckpointLogRecord(logManager, lsn)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewCheckpointLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, int64(-1), decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, LogRecordCheckpoint, decodedRecord.Op())
}

func TestSetBoolLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	// Test data
	fileName := "test_file"
	blockNum := 5
	blockID := file.NewBlockID(fileName, blockNum)

	txNum := int64(42)
	offset := 100
	oldValue := true
	newValue := false
	lsn := logManager.GetNextLatestLSN()
	prevLSN := int64(-1)

	err = WriteSetBoolLogRecord(logManager, txNum, lsn, prevLSN, blockID, offset, oldValue, newValue)
	assert.NoError(t, err)

	// Get the last log record
	iterator, err := logManager.Iterator()
	assert.NoError(t, err)
	var lastRecord []byte
	for iterator.HasNext() {
		lastRecord = iterator.Next()
	}

	// Make sure we got a record
	require.NotNil(t, lastRecord, "No log record was written")

	// Create a page from the log record
	page := file.NewPageFromBytes(lastRecord)

	// Decode the log record
	decodedRecord := NewSetBoolLogRecord(page)

	// Verify the decoded record matches the original
	assert.Equal(t, txNum, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, lsn, decodedRecord.lsn, "LSN mismatch")
	assert.Equal(t, prevLSN, decodedRecord.prevLSN, "PrevLSN mismatch")
	assert.Equal(t, offset, decodedRecord.offset, "Offset mismatch")
	assert.Equal(t, oldValue, decodedRecord.oldValue, "Old value mismatch")
	assert.Equal(t, newValue, decodedRecord.newValue, "New value mismatch")
	assert.Equal(t, fileName, decodedRecord.block.Filename(), "Filename mismatch")
	assert.Equal(t, blockNum, decodedRecord.block.Number(), "Block number mismatch")
	assert.Equal(t, LogRecordSetBool, decodedRecord.Op())
}
