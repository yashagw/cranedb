package buffer

import (
	"errors"
	"sync"
	"time"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

var ErrBufferAbort = errors.New("buffer request aborted")

// Manager manages a pool of buffers.
type Manager struct {
	bufferpool   []*Buffer
	numAvailable int
	maxTime      time.Duration
	mu           sync.Mutex
	cond         *sync.Cond
}

func NewManager(fm *file.Manager, lm *log.Manager, numbuffs int) *Manager {
	bufferpool := make([]*Buffer, 0, numbuffs)
	for range numbuffs {
		bufferpool = append(bufferpool, NewBuffer(fm, lm))
	}

	bm := &Manager{
		bufferpool:   bufferpool,
		numAvailable: numbuffs,
		maxTime:      10 * time.Second,
	}
	bm.cond = sync.NewCond(&bm.mu)
	return bm
}

func (bm *Manager) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.numAvailable
}

func (bm *Manager) FlushAll(txnum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buff := range bm.bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.flush()
		}
	}
}

func (bm *Manager) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buff.unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		// Wake up all waiting goroutines
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
	buff := bm.tryToPin(blk)

	// If no buffer available, wait with timeout
	for buff == nil && time.Since(startTime) < bm.maxTime {
		// Start a goroutine to wake us up after 100ms if no one else does
		go func() {
			time.Sleep(100 * time.Millisecond)
			bm.cond.Broadcast()
		}()

		// Sleep until someone calls Broadcast()
		bm.cond.Wait()
		buff = bm.tryToPin(blk)
	}

	if buff == nil {
		return nil, ErrBufferAbort
	}
	return buff, nil
}

// tryToPin attempts to pin a buffer to the specified block.
// Returns nil if no buffer is available.
func (bm *Manager) tryToPin(blk *file.BlockID) *Buffer {
	var buff *Buffer

	// 1. Check if the block is already in a buffer
	for _, b := range bm.bufferpool {
		block := b.Block()
		if block != nil && block.Filename() == blk.Filename() && block.Number() == blk.Number() {
			buff = b
			break
		}
	}

	// 2. If not, choose an unpinned buffer
	if buff == nil {
		for _, b := range bm.bufferpool {
			if !b.IsPinned() {
				buff = b
				break
			}
		}

		// 3. If no unpinned buffer is available, return nil
		if buff == nil {
			return nil
		}

		// 4. Assign the buffer to the block
		buff.assignToBlock(blk)
	}

	// 5. If the buffer wasn't already pinned, decrease available count
	if !buff.IsPinned() {
		bm.numAvailable--
	}

	buff.pin()

	return buff
}
