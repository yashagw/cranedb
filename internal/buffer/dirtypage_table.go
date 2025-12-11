package buffer

import (
	"fmt"
	"sync"

	"github.com/yashagw/cranedb/internal/file"
)

// DirtyPageEntry represents an entry in the Dirty Page Table
type DirtyPageEntry struct {
	RecLSN int64
}

// DirtyPageTable tracks which pages have been modified but not yet flushed to disk
// Key: file.BlockID (filename + block number) - identifies the page
// Value: Dirty page entry containing recLSN
type DirtyPageTable struct {
	mu    sync.RWMutex
	table map[file.BlockID]*DirtyPageEntry
}

// NewDirtyPageTable creates a new empty Dirty Page Table
func NewDirtyPageTable() *DirtyPageTable {
	return &DirtyPageTable{
		table: make(map[file.BlockID]*DirtyPageEntry),
	}
}

// Add adds a page to the DPT with its first modification LSN
// If the page already exists, this does nothing (recLSN is set once and never updated)
func (dpt *DirtyPageTable) Add(block *file.BlockID, lsn int64) {
	dpt.mu.Lock()
	defer dpt.mu.Unlock()

	if lsn < 0 {
		// invalid LSN, do not add
		return
	}

	key := file.MakeBlockKey(block)
	// Only add if not already present (recLSN is set once when page becomes dirty)
	if _, exists := dpt.table[key]; !exists {
		dpt.table[key] = &DirtyPageEntry{
			RecLSN: lsn,
		}
	}
}

// Remove removes a page from the DPT when it's flushed to disk
func (dpt *DirtyPageTable) Remove(block *file.BlockID) error {
	dpt.mu.Lock()
	defer dpt.mu.Unlock()
	key := file.MakeBlockKey(block)
	if _, exists := dpt.table[key]; !exists {
		return fmt.Errorf("block %s not found in dirty page table", block.String())
	}
	delete(dpt.table, key)
	return nil
}

// Get checks if a page is dirty and returns its recLSN
// Returns the entry and true if found, nil and false otherwise
func (dpt *DirtyPageTable) Get(block *file.BlockID) (*DirtyPageEntry, bool) {
	dpt.mu.RLock()
	defer dpt.mu.RUnlock()
	key := file.MakeBlockKey(block)
	entry, exists := dpt.table[key]
	if !exists {
		return nil, false
	}
	// Return a copy to prevent external modification
	return &DirtyPageEntry{
		RecLSN: entry.RecLSN,
	}, true
}

// GetAll returns a snapshot of all entries in the table
// This is used for checkpoint operations
func (dpt *DirtyPageTable) GetAll() map[file.BlockID]*DirtyPageEntry {
	dpt.mu.RLock()
	defer dpt.mu.RUnlock()

	// Create a deep copy to prevent external modification
	snapshot := make(map[file.BlockID]*DirtyPageEntry, len(dpt.table))
	for key, entry := range dpt.table {
		snapshot[key] = &DirtyPageEntry{
			RecLSN: entry.RecLSN,
		}
	}
	return snapshot
}
