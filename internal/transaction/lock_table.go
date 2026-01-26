package transaction

import (
	"errors"
	"sync"
	"time"

	"github.com/yashagw/cranedb/internal/file"
)

var ErrLockAbort = errors.New("lock abort")
var ErrLockDoNotExist = errors.New("lock does not exist")

const (
	MAX_WAITING_TIME = 10 * time.Second
)

type LockTable struct {
	locks   map[file.BlockID]struct{} // tracks blocks with exclusive locks
	mu      sync.Mutex
	waiters map[file.BlockID]chan struct{} // Block-specific notification channels
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks:   make(map[file.BlockID]struct{}),
		waiters: make(map[file.BlockID]chan struct{}),
	}
}

func (lt *LockTable) xLock(block *file.BlockID) error {
	key := file.MakeBlockKey(block)
	deadline := time.Now().Add(MAX_WAITING_TIME)

	for {
		lt.mu.Lock()
		// Check if there's already an exclusive lock
		if _, exists := lt.locks[key]; !exists {
			// No lock, we can acquire exclusive lock
			lt.locks[key] = struct{}{}
			lt.mu.Unlock()
			return nil
		}

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

func (lt *LockTable) unlock(block *file.BlockID) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	key := file.MakeBlockKey(block)
	if _, exists := lt.locks[key]; !exists {
		return ErrLockDoNotExist
	}

	delete(lt.locks, key)

	// Notify waiting goroutines for this specific block
	if waiter, exists := lt.waiters[key]; exists {
		select {
		case waiter <- struct{}{}:
		default:
		}
	}

	return nil
}

// HasXLock returns true if the block has an exclusive lock
func (lt *LockTable) HasXLock(block *file.BlockID) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	key := file.MakeBlockKey(block)
	_, exists := lt.locks[key]
	return exists
}
