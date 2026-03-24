package replication

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/transaction"
)

// writeLogRecord creates a proper log record (StartLogRecord format) and appends it to the log.
// Format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] = 28 bytes minimum
func writeLogRecord(t *testing.T, fm *file.Manager, lm *dblog.Manager, txNum int64) int64 {
	t.Helper()
	lsn := lm.GetNextLatestLSN()

	// Build a StartLogRecord: [op=1(4)] [txNum(8)] [lsn(8)] [prevLSN=0(8)]
	recSize := 4 + 8 + 8 + 8
	page := file.NewPage(recSize)
	page.SetIntRaw(0, int(transaction.LogRecordStart))
	page.SetInt64Raw(4, txNum)
	page.SetInt64Raw(12, lsn)
	page.SetInt64Raw(20, 0) // prevLSN

	err := lm.Append(page.Bytes(), lsn)
	assert.NoError(t, err)
	return lsn
}

func TestWALSenderStreamsRecords(t *testing.T) {
	tempDir := t.TempDir()

	fm, err := file.NewManager(tempDir, 4096)
	assert.NoError(t, err)

	logFile := "test_wal_sender.log"
	lm, err := dblog.NewManager(fm, logFile)
	assert.NoError(t, err)

	// Write some log records
	var lsns []int64
	for i := int64(1); i <= 5; i++ {
		lsn := writeLogRecord(t, fm, lm, i)
		lsns = append(lsns, lsn)
	}

	// Flush to disk
	err = lm.Flush(lm.LatestLSN())
	assert.NoError(t, err)

	// Create WAL sender with net.Pipe
	serverConn, clientConn := net.Pipe()
	ws := NewWALSender(fm, lm, logFile, serverConn)

	// Run the sender in background
	senderDone := make(chan error, 1)
	go func() {
		senderDone <- ws.Run()
	}()

	// Simulate follower: send handshake with startLSN=1 (want all records)
	handshakePayload := EncodeStandbyStatusUpdate(1)
	err = WriteMessage(clientConn, MsgStandbyStatusUpdate, handshakePayload)
	assert.NoError(t, err)

	// Read WALData messages from the sender
	var receivedLSNs []int64
	for i := 0; i < 5; i++ {
		msgType, payload, err := ReadMessage(clientConn)
		assert.NoError(t, err)
		assert.Equal(t, MsgWALData, msgType)

		lsn, _, err := DecodeWALData(payload)
		assert.NoError(t, err)
		receivedLSNs = append(receivedLSNs, lsn)
	}

	// Verify we got all LSNs in order
	assert.Equal(t, lsns, receivedLSNs)

	// Clean up
	ws.Stop()
	clientConn.Close()
	serverConn.Close()
}

func TestWALSenderSkipsOldRecords(t *testing.T) {
	tempDir := t.TempDir()

	fm, err := file.NewManager(tempDir, 4096)
	assert.NoError(t, err)

	logFile := "test_wal_sender_skip.log"
	lm, err := dblog.NewManager(fm, logFile)
	assert.NoError(t, err)

	// Write 5 records
	var lsns []int64
	for i := int64(1); i <= 5; i++ {
		lsn := writeLogRecord(t, fm, lm, i)
		lsns = append(lsns, lsn)
	}
	err = lm.Flush(lm.LatestLSN())
	assert.NoError(t, err)

	serverConn, clientConn := net.Pipe()
	ws := NewWALSender(fm, lm, logFile, serverConn)

	go func() {
		ws.Run()
	}()

	// Handshake with startLSN=3 (skip first 2 records)
	handshakePayload := EncodeStandbyStatusUpdate(3)
	err = WriteMessage(clientConn, MsgStandbyStatusUpdate, handshakePayload)
	assert.NoError(t, err)

	// Should receive records with LSN >= 3
	var receivedLSNs []int64
	for i := 0; i < 3; i++ {
		msgType, payload, err := ReadMessage(clientConn)
		assert.NoError(t, err)
		assert.Equal(t, MsgWALData, msgType)

		lsn, _, err := DecodeWALData(payload)
		assert.NoError(t, err)
		receivedLSNs = append(receivedLSNs, lsn)
	}

	assert.Equal(t, lsns[2:], receivedLSNs)

	ws.Stop()
	clientConn.Close()
	serverConn.Close()
}
