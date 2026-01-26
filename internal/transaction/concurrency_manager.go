package transaction

import (
	"sync"

	"github.com/yashagw/cranedb/internal/file"
)

// Each Transaction has a ConcurrencyManager
// All Concurrency Manager shares a single LockTable
type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[file.BlockID]string // "S" for shared, "X" for exclusive
	mu        sync.Mutex
}

func NewConcurrencyManager(lockTable *LockTable) *ConcurrencyManager {
	return &ConcurrencyManager{
		lockTable: lockTable,
		locks:     make(map[file.BlockID]string),
		mu:        sync.Mutex{},
	}
}

func (cm *ConcurrencyManager) sLock(block *file.BlockID) error {
	return nil
}

func (cm *ConcurrencyManager) xLock(block *file.BlockID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := file.MakeBlockKey(block)

	// Already have exclusive lock
	if lockType, exists := cm.locks[key]; exists && lockType == "X" {
		return nil
	}

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
		block := file.NewBlockID(key.Filename(), key.Number())

		err := cm.lockTable.unlock(block)
		if err != nil {
			return err
		}
	}

	cm.locks = make(map[file.BlockID]string)

	return nil
}
