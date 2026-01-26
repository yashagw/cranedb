package transaction

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

func TestSetInt64LogRecord_WriteAndParse(t *testing.T) {
	tempDir := "/tmp/testdb_setint64"
	defer os.RemoveAll(tempDir)

	fileManager, err := file.NewManager(tempDir, 4096)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)

	block := file.NewBlockID("testfile.tbl", 3)

	// Write a SetInt64 log record
	err = WriteSetInt64LogRecord(logManager, 7, 100, 50, block, 24, 999, 1234)
	require.NoError(t, err)

	// Read it back via log iterator
	iter, err := logManager.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	logBytes := iter.Next()
	record, err := CreateLogRecord(logBytes)
	require.NoError(t, err)

	setInt64Record, ok := record.(*SetInt64LogRecord)
	require.True(t, ok, "expected SetInt64LogRecord")

	assert.Equal(t, LogRecordSetInt64, setInt64Record.Op())
	assert.Equal(t, int64(7), setInt64Record.TxNumber())
	assert.Equal(t, int64(100), setInt64Record.LSN())
	assert.Equal(t, int64(50), setInt64Record.PrevLSN())
	assert.Equal(t, "testfile.tbl", setInt64Record.Block().Filename())
	assert.Equal(t, 3, setInt64Record.Block().Number())
	assert.Equal(t, 24, setInt64Record.Offset())
	assert.Equal(t, int64(999), setInt64Record.OldValue())
	assert.Equal(t, int64(1234), setInt64Record.NewValue())
}

func TestSetInt64LogRecord_UndoRedo(t *testing.T) {
	tempDir := "/tmp/testdb_setint64_undoredo"
	defer os.RemoveAll(tempDir)

	fileManager, err := file.NewManager(tempDir, 4096)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := NewTransactionTable()
	tm := NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)

	// Write initial value
	tx1 := tm.BeginTransaction()
	block, err := tx1.Append("testdata.tbl")
	require.NoError(t, err)
	err = tx1.SetInt64(block, 100, 42, true)
	require.NoError(t, err)

	// Verify written value
	val, err := tx1.GetInt64(block, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(42), val)

	// Write new value
	err = tx1.SetInt64(block, 100, 99, true)
	require.NoError(t, err)
	val, err = tx1.GetInt64(block, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(99), val)

	err = tx1.Commit()
	require.NoError(t, err)

	// Verify value persists in new transaction
	tx2 := tm.BeginTransaction()
	val, err = tx2.GetInt64(block, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(99), val)
	err = tx2.Commit()
	require.NoError(t, err)
}
