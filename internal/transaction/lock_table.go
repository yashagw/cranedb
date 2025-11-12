package transaction

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/yashagw/cranedb/internal/file"
)

var ErrLockAbort = errors.New("lock abort")
var ErrLockDoNotExist = errors.New("lock does not exist")

const (
	MAX_WAITING_TIME = 10 * time.Second
)

type blockKey struct {
	filename string
	blkNum   int
}

func makeKey(block *file.BlockID) blockKey {
	return blockKey{
		filename: block.Filename(),
		blkNum:   block.Number(),
	}
}

type LockTable struct {
	locks   map[blockKey]int
	mu      sync.Mutex
	waiters map[blockKey]chan struct{} // Block-specific notification channels
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks:   make(map[blockKey]int),
		waiters: make(map[blockKey]chan struct{}),
	}
}

func (lt *LockTable) sLock(block *file.BlockID) error {
	key := makeKey(block)
	deadline := time.Now().Add(MAX_WAITING_TIME)

	for {
		lt.mu.Lock()
		// Check if there's an exclusive lock
		if lt.locks[key] != -1 {
			// No exclusive lock, we can acquire shared lock
			lt.locks[key]++
			lt.mu.Unlock()
			return nil
		}

		// There's an exclusive lock, need to wait
		if lt.waiters[key] == nil {
			lt.waiters[key] = make(chan struct{}, 1)
		}
		waiter := lt.waiters[key]
		lt.mu.Unlock()

		timeout := time.Until(deadline)
		if timeout <= 0 {
			return ErrLockAbort
		}
		timer := time.NewTimer(timeout)

		select {
		case <-waiter:
			timer.Stop()
		case <-timer.C:
			return ErrLockAbort
		}
	}
}

func (lt *LockTable) xLock(block *file.BlockID) error {
	key := makeKey(block)
	deadline := time.Now().Add(MAX_WAITING_TIME)

	for {
		lt.mu.Lock()
		// Check if there are any locks (shared or exclusive locks)
		if lt.locks[key] == 0 {
			// No locks, we can acquire exclusive lock
			lt.locks[key] = -1
			lt.mu.Unlock()
			log.Printf("[LOCK] Acquired X lock on %s:%d", key.filename, key.blkNum)
			return nil
		}

		log.Printf("[LOCK] Waiting for X lock on %s:%d (current locks: %d)", key.filename, key.blkNum, lt.locks[key])
		if lt.waiters[key] == nil {
			lt.waiters[key] = make(chan struct{}, 1)
		}
		waiter := lt.waiters[key]
		lt.mu.Unlock()

		timeout := time.Until(deadline)
		if timeout <= 0 {
			log.Printf("[LOCK] X lock timeout on %s:%d", key.filename, key.blkNum)
			return ErrLockAbort
		}
		timer := time.NewTimer(timeout)

		select {
		case <-waiter:
			timer.Stop()
			log.Printf("[LOCK] Woken up for X lock on %s:%d, retrying", key.filename, key.blkNum)
		case <-timer.C:
			log.Printf("[LOCK] X lock wait timeout on %s:%d", key.filename, key.blkNum)
			return ErrLockAbort
		}
	}
}

func (lt *LockTable) unlock(block *file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	key := makeKey(block)
	val, exists := lt.locks[key]

	if !exists {
		return ErrLockDoNotExist
	}

	if val == -1 {
		delete(lt.locks, key)
		log.Printf("[LOCK] Released X lock on %s:%d", key.filename, key.blkNum)
	} else if val > 0 {
		lt.locks[key]--
		if lt.locks[key] == 0 {
			delete(lt.locks, key)
			log.Printf("[LOCK] Released last S lock on %s:%d", key.filename, key.blkNum)
		} else {
			log.Printf("[LOCK] Released S lock on %s:%d (remaining: %d)", key.filename, key.blkNum, lt.locks[key])
		}
	} else {
		return ErrLockDoNotExist
	}

	// Notify waiting goroutines for this specific block
	if waiter, exists := lt.waiters[key]; exists {
		select {
		case waiter <- struct{}{}:
			log.Printf("[LOCK] Notified waiters for %s:%d", key.filename, key.blkNum)
		default:
		}
	}

	return nil
}

// HasXLock returns true if the block has an exclusive lock
func (lt *LockTable) HasXLock(block *file.BlockID) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	key := makeKey(block)
	return lt.locks[key] == -1
}

// HasSLock returns true if the block has one or more shared locks
func (lt *LockTable) HasSLock(block *file.BlockID) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	key := makeKey(block)
	return lt.locks[key] > 0
}
