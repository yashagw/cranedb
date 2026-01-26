package transaction

import "sync"

// CommitLog is a global registry of committed transaction IDs.
type CommitLog struct {
	mu        sync.RWMutex
	committed map[int64]bool
}

func NewCommitLog() *CommitLog {
	return &CommitLog{
		committed: make(map[int64]bool),
	}
}

// MarkCommitted records that txNum has committed.
func (cl *CommitLog) MarkCommitted(txNum int64) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.committed[txNum] = true
}

// IsCommitted returns true if txNum has committed.
func (cl *CommitLog) IsCommitted(txNum int64) bool {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.committed[txNum]
}

// Cleanup removes entries for transactions older than oldestActiveTx.
func (cl *CommitLog) Cleanup(oldestActiveTx int64) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for txNum := range cl.committed {
		if txNum < oldestActiveTx {
			delete(cl.committed, txNum)
		}
	}
}
