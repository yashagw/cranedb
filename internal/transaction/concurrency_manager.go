package transaction

import (
	"sync"

	"github.com/yashagw/cranedb/internal/file"
)

// Each Transaction has a ConcurrencyManager
// All Concurrency Manager shares a single LockTable
type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[blockKey]string // "S" for shared, "X" for exclusive
	mu        sync.Mutex
}

func NewConcurrencyManager(lockTable *LockTable) *ConcurrencyManager {
	return &ConcurrencyManager{
		lockTable: lockTable,
		locks:     make(map[blockKey]string),
		mu:        sync.Mutex{},
	}
}

func (cm *ConcurrencyManager) sLock(block *file.BlockID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := makeKey(block)

	// We already have a lock on this block, nothing to do
	if _, exists := cm.locks[key]; exists {
		return nil
	}

	err := cm.lockTable.sLock(block)
	if err != nil {
		return err
	}

	cm.locks[key] = "S"
	return nil
}

func (cm *ConcurrencyManager) xLock(block *file.BlockID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := makeKey(block)

	if lockType, exists := cm.locks[key]; exists {
		// We already have an exclusive lock, nothing to do
		if lockType == "X" {
			return nil
		}

		// We have a shared lock
		// Release the shared lock first, then acquire exclusive lock
		err := cm.lockTable.unlock(block)
		if err != nil {
			return err
		}

		err = cm.lockTable.xLock(block)
		if err != nil {
			return err
		}

		cm.locks[key] = "X"
		return nil
	}

	// We don't have any lock, acquire exclusive lock from lock table
	err := cm.lockTable.xLock(block)
	if err != nil {
		return err
	}

	cm.locks[key] = "X"
	return nil
}

func (cm *ConcurrencyManager) release() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for key := range cm.locks {
		block := file.NewBlockID(key.filename, key.blkNum)

		err := cm.lockTable.unlock(block)
		if err != nil {
			return err
		}
	}

	cm.locks = make(map[blockKey]string)

	return nil
}
