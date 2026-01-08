package buffer

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// Manager manages a pool of buffers.
type Manager struct {
	bufferpool   []*Buffer
	lruList      *LRUList
	numAvailable int
	maxTime      time.Duration
	mu           sync.Mutex
	cond         *sync.Cond
	flushStopCh  chan struct{}
}

func NewManager(fileManager *file.Manager, logManager *log.Manager, dirtyPageTable *DirtyPageTable, numOfBuffer int) (*Manager, error) {
	if numOfBuffer <= 0 {
		return nil, errors.New("number of buffers must be positive")
	}

	lruList := NewLRUList()
	bufferpool := make([]*Buffer, 0, numOfBuffer)
	for i := 0; i < numOfBuffer; i++ {
		buf := NewBuffer(i, fileManager, logManager, dirtyPageTable)
		bufferpool = append(bufferpool, buf)
		lruList.Add(buf) // Add all buffers to LRU list
	}

	bm := &Manager{
		bufferpool:   bufferpool,
		lruList:      lruList,
		numAvailable: numOfBuffer,
		maxTime:      10 * time.Second,
		flushStopCh:  make(chan struct{}),
	}
	bm.cond = sync.NewCond(&bm.mu)
	return bm, nil
}

// StartBackgroundFlush launches a goroutine that periodically flushes dirty, unpinned buffers.
func (bm *Manager) StartBackgroundFlush(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				slog.Info("Background flush started")
				flushed := 0
				bm.mu.Lock()
				for _, buff := range bm.bufferpool {
					// Only flush unpinned buffers
					if !buff.IsPinned() {
						// TODO: do not ignore errors here
						_ = buff.flush()
						flushed++
					}
				}
				bm.mu.Unlock()
			case <-bm.flushStopCh:
				return
			}
		}
	}()
}

// StopBackgroundFlush signals the background flush goroutine to stop.
func (bm *Manager) StopBackgroundFlush() {
	close(bm.flushStopCh)
}

// CountDirtyBuffers returns the number of dirty, unpinned buffers.
// A buffer is dirty if it has txNum >= 0 and lsn >= 0.
func (bm *Manager) CountDirtyBuffers() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	count := 0
	for _, buff := range bm.bufferpool {
		if !buff.IsPinned() && buff.ModifyingTx() >= 0 && buff.ModifyingLSN() >= 0 {
			count++
		}
	}
	return count
}

// FlushAllDirtyBuffers flushes all dirty, unpinned buffers synchronously.
// This is useful for testing or forcing immediate persistence.
func (bm *Manager) FlushAllDirtyBuffers() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buff := range bm.bufferpool {
		// Only flush unpinned buffers
		if !buff.IsPinned() && buff.ModifyingTx() >= 0 && buff.ModifyingLSN() >= 0 {
			err := buff.flush()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bm *Manager) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.numAvailable
}

func (bm *Manager) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	prevPinned := buff.IsPinned()
	buff.unpin()
	slog.Info("Buffer unpinned from block", slog.Group("buffer",
		slog.Int("id", buff.number),
		slog.String("block", func() string {
			if buff.Block() != nil {
				return buff.Block().String()
			}
			return "nil"
		}()),
		slog.Bool("wasPinned", prevPinned),
		slog.Bool("nowPinned", buff.IsPinned()),
		slog.Int64("txNum", buff.ModifyingTx()),
		slog.Int64("lsn", buff.lsn),
	))
	if !buff.IsPinned() && prevPinned {
		bm.numAvailable++
		slog.Info("Buffer available after unpin", slog.Int("buffer", buff.number), slog.Int("numAvailable", bm.numAvailable))
		bm.cond.Broadcast()
	}
}

// Pin pins a buffer to the specified block.
// If the block is already in a buffer, that buffer is returned.
// Otherwise, an unpinned buffer is chosen and assigned to the block.
// Returns an error if no buffer becomes available within the timeout period.
func (bm *Manager) Pin(blk *file.BlockID) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	startTime := time.Now()
	buff, err := bm.tryToPin(blk)
	if err != nil {
		return nil, err
	}
	if buff != nil {
		slog.Info("Buffer pinned to block", slog.Group("buffer",
			slog.Int("id", buff.number),
			slog.String("block", func() string {
				if buff.Block() != nil {
					return buff.Block().String()
				}
				return "nil"
			}()),
			slog.Int64("txNum", buff.ModifyingTx()),
			slog.Int64("lsn", buff.lsn),
		))
	}

	// If no buffer available, wait with timeout
	for buff == nil && time.Since(startTime) < bm.maxTime {
		// Start a goroutine to wake us up after 100ms if no one else does
		go func() {
			time.Sleep(100 * time.Millisecond)
			bm.cond.Broadcast()
		}()

		// Sleep until someone calls Broadcast()
		bm.cond.Wait()
		buff, err = bm.tryToPin(blk)
		if err != nil {
			return nil, err
		}
	}

	if buff == nil {
		return nil, errors.New("empty buffer not found")
	}
	return buff, nil
}

// tryToPin attempts to pin a buffer to the specified block.
// Returns nil if no buffer is available.
func (bm *Manager) tryToPin(blk *file.BlockID) (*Buffer, error) {
	var buff *Buffer

	// 1. Check if the block is already in a buffer
	for _, b := range bm.bufferpool {
		block := b.Block()
		if block != nil && block.Filename() == blk.Filename() && block.Number() == blk.Number() {
			buff = b
			slog.Info("Buffer selected for block (already loaded)", slog.Group("buffer",
				slog.Int("id", b.number),
				slog.String("block", block.String()),
				slog.Int64("txNum", b.ModifyingTx()),
				slog.Int64("lsn", b.lsn),
			))
			break
		}
	}

	if buff == nil {
		// Use LRU to find the least recently used unpinned buffer
		buff = bm.lruList.GetLRUUnpinned()

		// If still no buffer, return nil
		if buff == nil {
			return nil, nil
		}

		slog.Info("Buffer reallocated for block", slog.Group("buffer",
			slog.Int("id", buff.number),
			slog.String("oldBlock", func() string {
				if buff.Block() != nil {
					return buff.Block().String()
				}
				return "nil"
			}()),
			slog.String("newBlock", blk.String()),
		))

		// Assign the buffer to the block
		err := buff.loadBlock(blk)
		if err != nil {
			return nil, err
		}
	}

	// Move to front of LRU list (most recently used)
	bm.lruList.MoveToFront(buff)

	// If the buffer wasn't already pinned, decrease available count
	if !buff.IsPinned() {
		bm.numAvailable--
	}

	buff.pin()

	return buff, nil
}
