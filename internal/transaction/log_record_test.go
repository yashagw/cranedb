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

	txNum := 42
	offset := 100
	oldValue := 12345

	_, err = WriteSetIntLogRecord(logManager, txNum, blockID, offset, oldValue)
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
	assert.Equal(t, offset, decodedRecord.offset, "Offset mismatch")
	assert.Equal(t, oldValue, decodedRecord.oldValue, "Value mismatch")
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

	txNum := 42
	offset := 100
	oldValue := "old_test_value"

	_, err = WriteSetStringLogRecord(logManager, txNum, blockID, offset, oldValue)
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
	assert.Equal(t, offset, decodedRecord.offset, "Offset mismatch")
	assert.Equal(t, oldValue, decodedRecord.oldValue, "Value mismatch")
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

	txNum := 42

	_, err = WriteStartLogRecord(logManager, txNum)
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
	assert.Equal(t, LogRecordStart, decodedRecord.Op())
}

func TestCommitLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	txNum := 42

	_, err = WriteCommitLogRecord(logManager, txNum)
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
	assert.Equal(t, LogRecordCommit, decodedRecord.Op())
}

func TestRollbackLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	txNum := 42

	_, err = WriteRollbackLogRecord(logManager, txNum)
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
	assert.Equal(t, LogRecordRollback, decodedRecord.Op())
}

func TestCheckpointLogRecord_EncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	fileManager, err := file.NewManager(tempDir, 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "log_test")
	assert.NoError(t, err)

	_, err = WriteCheckpointLogRecord(logManager)
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
	assert.Equal(t, -1, decodedRecord.TxNumber(), "Transaction number mismatch")
	assert.Equal(t, LogRecordCheckpoint, decodedRecord.Op())
}
