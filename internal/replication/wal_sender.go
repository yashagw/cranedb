package replication

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
)

// WALSender streams WAL records from a given LSN to a follower over a connection.
type WALSender struct {
	fileManager *file.Manager
	logManager  *dblog.Manager
	logFilename string
	writer      *bufio.Writer
	reader      *bufio.Reader

	followerReplayLSN int64
	mu                sync.Mutex
	stopCh            chan struct{}
}

// NewWALSender creates a new WAL sender for the given connection.
func NewWALSender(fm *file.Manager, lm *dblog.Manager, logFilename string, rw io.ReadWriter) *WALSender {
	return &WALSender{
		fileManager: fm,
		logManager:  lm,
		logFilename: logFilename,
		writer:      bufio.NewWriter(rw),
		reader:      bufio.NewReader(rw),
		stopCh:      make(chan struct{}),
	}
}

// Run performs the handshake and starts streaming WAL records.
// Blocks until the connection is closed or Stop() is called.
func (ws *WALSender) Run() error {
	// Step 1: Handshake — read StandbyStatusUpdate to get startLSN
	msgType, payload, err := ReadMessage(ws.reader)
	if err != nil {
		return fmt.Errorf("handshake read: %w", err)
	}
	if msgType != MsgStandbyStatusUpdate {
		return fmt.Errorf("expected StandbyStatusUpdate for handshake, got type %d", msgType)
	}
	startLSN, err := DecodeStandbyStatusUpdate(payload)
	if err != nil {
		return fmt.Errorf("decode handshake: %w", err)
	}

	ws.mu.Lock()
	ws.followerReplayLSN = startLSN
	ws.mu.Unlock()

	slog.Info("WAL sender handshake complete", "startLSN", startLSN)

	// Step 2: Flush WAL to ensure all records on disk
	if err := ws.logManager.ForceFlush(); err != nil {
		return fmt.Errorf("flush WAL: %w", err)
	}

	// Step 3: Create ForwardIterator at block 0
	iter, err := dblog.NewForwardIterator(ws.fileManager, ws.logFilename, 0)
	if err != nil {
		return fmt.Errorf("create forward iterator: %w", err)
	}

	// Step 4: Start goroutine to read status updates from follower
	go ws.readStatusUpdates()

	// Step 5: Stream loop
	lastKeepAlive := time.Now()
	for {
		select {
		case <-ws.stopCh:
			return nil
		default:
		}

		if iter.HasNext() {
			rec := iter.Next()
			if rec == nil {
				continue
			}

			lsn, err := dblog.GetLSNFromRecord(rec)
			if err != nil {
				// Skip unparseable records
				slog.Error("WAL sender: bad record", "err", err)
				continue
			}

			if lsn < startLSN {
				continue
			}

			payload := EncodeWALData(lsn, rec)
			if err := WriteMessage(ws.writer, MsgWALData, payload); err != nil {
				return fmt.Errorf("write WALData: %w", err)
			}
			if err := ws.writer.Flush(); err != nil {
				return fmt.Errorf("flush WALData: %w", err)
			}
		} else {
			// No records available — send keepalive every 10s and poll for new records
			if time.Since(lastKeepAlive) >= 10*time.Second {
				currentLSN := ws.logManager.LatestLSN()
				payload := EncodePrimaryKeepAlive(currentLSN, false)
				if err := WriteMessage(ws.writer, MsgPrimaryKeepAlive, payload); err != nil {
					return fmt.Errorf("write keepalive: %w", err)
				}
				if err := ws.writer.Flush(); err != nil {
					return fmt.Errorf("flush keepalive: %w", err)
				}
				lastKeepAlive = time.Now()
			}

			// Flush WAL and poll for new records
			if err := ws.logManager.ForceFlush(); err != nil {
				slog.Warn("WAL sender flush error during poll", "err", err)
			}

			if _, err := iter.Refresh(); err != nil {
				slog.Warn("WAL sender refresh error", "err", err)
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Stop signals the WAL sender to stop streaming.
func (ws *WALSender) Stop() {
	close(ws.stopCh)
}

// readStatusUpdates reads StandbyStatusUpdate messages from the follower
// and updates followerReplayLSN. Runs until the connection is closed or stopCh is signaled.
func (ws *WALSender) readStatusUpdates() {
	for {
		select {
		case <-ws.stopCh:
			return
		default:
		}

		msgType, payload, err := ReadMessage(ws.reader)
		if err != nil {
			slog.Debug("WAL sender status reader stopped", "err", err)
			return
		}

		if msgType == MsgStandbyStatusUpdate {
			replayLSN, err := DecodeStandbyStatusUpdate(payload)
			if err != nil {
				slog.Warn("WAL sender: bad status update", "err", err)
				continue
			}
			ws.mu.Lock()
			ws.followerReplayLSN = replayLSN
			ws.mu.Unlock()
			slog.Debug("Follower replay LSN updated", "replayLSN", replayLSN)
		}
	}
}
