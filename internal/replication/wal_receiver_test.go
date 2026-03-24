package replication

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/transaction"
)

// buildSetIntRecordBytes builds raw bytes for a SetIntLogRecord with CRC32.
// Format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [filename(4+len)] [blkNum(4)] [offset(4)] [oldVal(4)] [newVal(4)] [crc(4)]
func buildSetIntRecordBytes(txNum, lsn, prevLSN int64, filename string, blkNum, offset, oldVal, newVal int) []byte {
	opPos := 0
	txNumPos := opPos + 4
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	fileNamePos := prevLSNPos + 8
	blockNumPos := fileNamePos + 4 + len(filename)
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	newValuePos := oldValuePos + 4
	dataLen := newValuePos + 4
	finalLen := dataLen + 4 // +CRC

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(transaction.LogRecordSetInt))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)
	page.SetStringRaw(fileNamePos, filename)
	page.SetIntRaw(blockNumPos, blkNum)
	page.SetIntRaw(offsetPos, offset)
	page.SetIntRaw(oldValuePos, oldVal)
	page.SetIntRaw(newValuePos, newVal)

	// Append CRC32 checksum
	data := page.Bytes()[:dataLen]
	crc := crc32.ChecksumIEEE(data)
	page.SetIntRaw(dataLen, int(crc))

	return page.Bytes()
}

// newTestManagers creates all managers needed for a WALReceiver test.
func newTestManagers(t *testing.T) (*file.Manager, *dblog.Manager, *buffer.Manager, *buffer.DirtyPageTable, *transaction.TransactionManager) {
	t.Helper()
	tempDir := t.TempDir()

	fm, err := file.NewManager(tempDir, 4096)
	require.NoError(t, err)

	lm, err := dblog.NewManager(fm, "test.log")
	require.NoError(t, err)

	dpt := buffer.NewDirtyPageTable()
	bm, err := buffer.NewManager(fm, lm, dpt, 20)
	require.NoError(t, err)

	lt := transaction.NewLockTable()
	tt := transaction.NewTransactionTable()
	tm := transaction.NewTransactionManager(fm, lm, bm, lt, dpt, tt)

	return fm, lm, bm, dpt, tm
}

func TestWALReceiverReplaysSetIntRecord(t *testing.T) {
	fm, lm, bm, dpt, tm := newTestManagers(t)
	_ = lm

	// Create the target data file with one block
	dataFile := "test_data.tbl"
	_, err := fm.Append(dataFile)
	require.NoError(t, err)

	wr := NewWALReceiver("unused", fm, lm, bm, dpt, tm)

	// Build a SetInt record: write value 42 at offset 100 in block 0
	recordBytes := buildSetIntRecordBytes(1, 10, 0, dataFile, 0, 100, 0, 42)

	// Replay the record directly
	err = wr.replayRecord(10, recordBytes)
	require.NoError(t, err)

	// Flush buffers to ensure data is on disk
	err = bm.FlushAllDirtyBuffers()
	require.NoError(t, err)

	// Verify the value was written to the data page
	blk := file.NewBlockID(dataFile, 0)
	page := file.NewPage(fm.BlockSize())
	err = fm.Read(blk, page)
	require.NoError(t, err)

	val := page.GetInt(100)
	assert.Equal(t, 42, val)
}

func TestEnsureBlockExists(t *testing.T) {
	fm, _, _, _, _ := newTestManagers(t)

	dataFile := "test_ensure.tbl"

	// Block 2 should not exist yet
	blk := file.NewBlockID(dataFile, 2)
	err := fm.EnsureBlockExists(blk)
	require.NoError(t, err)

	// File should now have at least 3 blocks (0, 1, 2)
	total, err := fm.GetTotalBlocks(dataFile)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, 3)
}

func TestWALReceiverReplayLSNUpdates(t *testing.T) {
	fm, lm, bm, dpt, tm := newTestManagers(t)

	wr := NewWALReceiver("unused", fm, lm, bm, dpt, tm)
	assert.Equal(t, int64(0), wr.ReplayLSN())

	// Create target file
	dataFile := "test_lsn.tbl"
	_, err := fm.Append(dataFile)
	require.NoError(t, err)

	// Replay a SetInt record with LSN 5
	recordBytes := buildSetIntRecordBytes(1, 5, 0, dataFile, 0, 100, 0, 10)
	err = wr.replayRecord(5, recordBytes)
	require.NoError(t, err)
	assert.Equal(t, int64(5), wr.ReplayLSN())

	// Replay another with LSN 10
	recordBytes = buildSetIntRecordBytes(2, 10, 0, dataFile, 0, 200, 0, 20)
	err = wr.replayRecord(10, recordBytes)
	require.NoError(t, err)
	assert.Equal(t, int64(10), wr.ReplayLSN())
}

func TestWALReceiverSkipsNonDataRecords(t *testing.T) {
	fm, lm, bm, dpt, tm := newTestManagers(t)

	wr := NewWALReceiver("unused", fm, lm, bm, dpt, tm)

	// Build a StartLogRecord (op=1) — should be skipped without error
	recSize := 4 + 8 + 8 + 8 + 4 // op + txNum + lsn + prevLSN + crc
	page := file.NewPage(recSize)
	page.SetIntRaw(0, int(transaction.LogRecordStart))
	page.SetInt64Raw(4, 1)  // txNum
	page.SetInt64Raw(12, 7) // lsn
	page.SetInt64Raw(20, 0) // prevLSN
	// CRC
	data := page.Bytes()[:recSize-4]
	crc := crc32.ChecksumIEEE(data)
	page.SetIntRaw(recSize-4, int(crc))

	err := wr.replayRecord(7, page.Bytes())
	require.NoError(t, err)
	assert.Equal(t, int64(7), wr.ReplayLSN())
}

func TestWALReceiverCreatesBlocksForNewFiles(t *testing.T) {
	fm, lm, bm, dpt, tm := newTestManagers(t)

	wr := NewWALReceiver("unused", fm, lm, bm, dpt, tm)

	// Replay a SetInt record targeting a file that doesn't exist yet (block 0)
	dataFile := "new_file.tbl"
	recordBytes := buildSetIntRecordBytes(1, 15, 0, dataFile, 0, 50, 0, 99)

	err := wr.replayRecord(15, recordBytes)
	require.NoError(t, err)

	// Flush and verify
	err = bm.FlushAllDirtyBuffers()
	require.NoError(t, err)

	blk := file.NewBlockID(dataFile, 0)
	page := file.NewPage(fm.BlockSize())
	err = fm.Read(blk, page)
	require.NoError(t, err)

	val := page.GetInt(50)
	assert.Equal(t, 99, val)
}
