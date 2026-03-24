package replication

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/transaction"
)

// WALReceiver connects to a primary server and replays received WAL records
// into local storage, keeping the follower in sync with the primary.
type WALReceiver struct {
	primaryAddr   string
	conn          net.Conn
	writer        *bufio.Writer
	reader        *bufio.Reader

	fileManager    *file.Manager
	logManager     *dblog.Manager
	bufferManager  *buffer.Manager
	dirtyPageTable *buffer.DirtyPageTable
	txManager      *transaction.TransactionManager

	replayLSN int64
	mu        sync.Mutex
	stopCh    chan struct{}
}

// NewWALReceiver creates a new WAL receiver that will connect to the given primary address.
func NewWALReceiver(
	primaryAddr string,
	fm *file.Manager,
	lm *dblog.Manager,
	bm *buffer.Manager,
	dpt *buffer.DirtyPageTable,
	tm *transaction.TransactionManager,
) *WALReceiver {
	return &WALReceiver{
		primaryAddr:    primaryAddr,
		fileManager:    fm,
		logManager:     lm,
		bufferManager:  bm,
		dirtyPageTable: dpt,
		txManager:      tm,
		stopCh:         make(chan struct{}),
	}
}

// Run connects to the primary and starts receiving and replaying WAL records.
// Blocks until the connection is closed or Stop() is called.
func (wr *WALReceiver) Run() error {
	conn, err := net.Dial("tcp", wr.primaryAddr)
	if err != nil {
		return fmt.Errorf("dial primary: %w", err)
	}
	wr.conn = conn
	wr.writer = bufio.NewWriter(conn)
	wr.reader = bufio.NewReader(conn)

	// Send handshake with our current replay position
	startLSN := wr.ReplayLSN()
	payload := EncodeStandbyStatusUpdate(startLSN)
	if err := WriteMessage(wr.writer, MsgStandbyStatusUpdate, payload); err != nil {
		return fmt.Errorf("send handshake: %w", err)
	}
	if err := wr.writer.Flush(); err != nil {
		return fmt.Errorf("flush handshake: %w", err)
	}

	slog.Info("WAL receiver connected", "primaryAddr", wr.primaryAddr, "startLSN", startLSN)

	// Start background goroutine for periodic status updates
	go wr.sendPeriodicStatus()

	// Receive loop
	for {
		select {
		case <-wr.stopCh:
			return nil
		default:
		}

		msgType, payload, err := ReadMessage(wr.reader)
		if err != nil {
			select {
			case <-wr.stopCh:
				return nil
			default:
			}
			return fmt.Errorf("read message: %w", err)
		}

		switch msgType {
		case MsgWALData:
			lsn, recordBytes, err := DecodeWALData(payload)
			if err != nil {
				slog.Warn("WAL receiver: bad WALData", "err", err)
				continue
			}
			if err := wr.replayRecord(lsn, recordBytes); err != nil {
				slog.Warn("WAL receiver: replay error", "lsn", lsn, "err", err)
				continue
			}

		case MsgPrimaryKeepAlive:
			_, replyRequired, err := DecodePrimaryKeepAlive(payload)
			if err != nil {
				slog.Warn("WAL receiver: bad keepalive", "err", err)
				continue
			}
			if replyRequired {
				if err := wr.sendStatusUpdate(); err != nil {
					slog.Warn("WAL receiver: failed to send status", "err", err)
				}
			}

		default:
			slog.Warn("WAL receiver: unknown message type", "type", msgType)
		}
	}
}

// replayRecord parses and applies a single WAL record to local storage.
func (wr *WALReceiver) replayRecord(lsn int64, recordBytes []byte) (retErr error) {
	record, err := transaction.CreateLogRecord(recordBytes)
	if err != nil {
		return fmt.Errorf("parse log record: %w", err)
	}

	op := record.Op()

	// For Commit records, mark the primary's transaction as committed
	// in the follower's CommitLog so MVCC visibility checks pass.
	if op == transaction.LogRecordCommit {
		commitLog := wr.txManager.GetCommitLog()
		commitLog.MarkCommitted(record.TxNumber())
		wr.mu.Lock()
		wr.replayLSN = lsn
		wr.mu.Unlock()
		return nil
	}

	// Only replay data modification records
	if op != transaction.LogRecordSetInt &&
		op != transaction.LogRecordSetString &&
		op != transaction.LogRecordSetBool &&
		op != transaction.LogRecordSetInt64 &&
		op != transaction.LogRecordCLR {
		// Skip Start/Rollback/Checkpoint — update replay position
		wr.mu.Lock()
		wr.replayLSN = lsn
		wr.mu.Unlock()
		return nil
	}

	// Get the target block from the data modification record
	dmr, ok := record.(transaction.DataModificationRecord)
	if !ok {
		return fmt.Errorf("record op %d does not implement DataModificationRecord", op)
	}

	blk := dmr.Block()

	// Ensure the target file and block exist on the follower
	if err := wr.fileManager.EnsureBlockExists(blk); err != nil {
		return fmt.Errorf("ensure block %s: %w", blk, err)
	}

	// Create a short-lived transaction for this replay operation
	tx := wr.txManager.BeginTransaction()
	defer func() {
		if err := tx.ReleaseNoCommit(); err != nil && retErr == nil {
			retErr = fmt.Errorf("release tx: %w", err)
		}
	}()

	if err := transaction.RedoAndTrack(dmr, tx, wr.dirtyPageTable); err != nil {
		return fmt.Errorf("redo record at lsn %d: %w", lsn, err)
	}

	wr.mu.Lock()
	wr.replayLSN = lsn
	wr.mu.Unlock()

	return nil
}

// sendStatusUpdate sends a StandbyStatusUpdate with the current replay LSN.
func (wr *WALReceiver) sendStatusUpdate() error {
	replayLSN := wr.ReplayLSN()
	payload := EncodeStandbyStatusUpdate(replayLSN)
	if err := WriteMessage(wr.writer, MsgStandbyStatusUpdate, payload); err != nil {
		return err
	}
	return wr.writer.Flush()
}

// sendPeriodicStatus sends status updates to the primary every 5 seconds.
func (wr *WALReceiver) sendPeriodicStatus() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wr.stopCh:
			return
		case <-ticker.C:
			if err := wr.sendStatusUpdate(); err != nil {
				slog.Debug("WAL receiver periodic status error", "err", err)
				return
			}
		}
	}
}

// ReplayLSN returns the last successfully replayed LSN in a thread-safe manner.
func (wr *WALReceiver) ReplayLSN() int64 {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	return wr.replayLSN
}

// Stop signals the WAL receiver to stop and closes the connection.
func (wr *WALReceiver) Stop() {
	close(wr.stopCh)
	if wr.conn != nil {
		wr.conn.Close()
	}
}
