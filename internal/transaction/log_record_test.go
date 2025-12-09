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

	txTable := make(map[int64]*TransactionEntry)
	txTable[1] = &TransactionEntry{
		Status:  TransactionStatusRunning,
		LastLSN: 100,
	}
	txTable[2] = &TransactionEntry{
		Status:  TransactionStatusCommitted,
		LastLSN: 200,
	}
	txTable[3] = &TransactionEntry{
		Status:  TransactionStatusAborted,
		LastLSN: 300,
	}

	dpt := make(map[file.BlockID]*DirtyPageEntry)
	block1 := file.NewBlockID("students.tbl", 0)
	block2 := file.NewBlockID("courses.tbl", 5)
	block3 := file.NewBlockID("enrollments.tbl", 10)
	dpt[file.MakeBlockKey(block1)] = &DirtyPageEntry{RecLSN: 50}
	dpt[file.MakeBlockKey(block2)] = &DirtyPageEntry{RecLSN: 150}
	dpt[file.MakeBlockKey(block3)] = &DirtyPageEntry{RecLSN: 250}

	err = WriteCheckpointLogRecord(logManager, lsn, txTable, dpt)
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
	assert.Equal(t, lsn, decodedRecord.LSN(), "LSN mismatch")
	assert.Equal(t, LogRecordCheckpoint, decodedRecord.Op())

	// Verify transaction table was encoded and decoded correctly
	decodedTxTable := decodedRecord.TransactionTable()
	require.Equal(t, len(txTable), len(decodedTxTable), "Transaction table size mismatch")

	for txNum, expectedEntry := range txTable {
		actualEntry, exists := decodedTxTable[txNum]
		require.True(t, exists, "Transaction %d not found in decoded table", txNum)
		assert.Equal(t, expectedEntry.Status, actualEntry.Status, "Status mismatch for transaction %d", txNum)
		assert.Equal(t, expectedEntry.LastLSN, actualEntry.LastLSN, "LastLSN mismatch for transaction %d", txNum)
	}

	// Verify dirty page table was encoded and decoded correctly
	decodedDPT := decodedRecord.DirtyPageTable()
	require.Equal(t, len(dpt), len(decodedDPT), "Dirty page table size mismatch")

	for blockKey, expectedEntry := range dpt {
		actualEntry, exists := decodedDPT[blockKey]
		require.True(t, exists, "Block %s not found in decoded DPT", blockKey.String())
		assert.Equal(t, expectedEntry.RecLSN, actualEntry.RecLSN, "RecLSN mismatch for block %s", blockKey.String())
	}
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
