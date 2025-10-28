package log

import (
	"sync"

	"github.com/yashagw/cranedb/internal/file"
)

// Manager manages the log file for the database.
// It provides methods to append log records and iterate over them.
type Manager struct {
	fm           *file.Manager
	logfile      string
	logpage      *file.Page
	currentBlk   *file.BlockID
	latestLSN    int
	lastSavedLSN int
	mu           sync.Mutex
}

// NewManager creates a new log manager
// The log manager maintains a single "current block" where new records are appended.
// If the log file is empty, it creates and initializes the first block.
// If the log file exists, it uses the last block as the current block.
//
// Block initialization:
//   - New blocks have boundary set to blockSize (indicating completely empty)
//   - Existing blocks are read to get their current state (boundary + existing records)
func NewManager(fm *file.Manager, logfile string) *Manager {
	// Create a new page with the block size from file manager
	logpage := file.NewPage(fm.BlockSize())

	var currentblk *file.BlockID

	numOfBlocks, err := fm.GetTotalBlocks(logfile)
	if err != nil {
		panic("not able to determine blocks in log file")
	}
	if numOfBlocks == 0 {
		// Create and initialize new block
		// Set boundary to blockSize, this indicates the block is completely empty
		currentblk, err = fm.Append(logfile)
		if err != nil {
			panic("not able to append block to log file")
		}
		logpage.SetInt(0, fm.BlockSize())
		fm.Write(currentblk, logpage)
	} else {
		// Use the last block
		// (zero-based: if numOfBlocks=3, last block is index 2)
		currentblk = file.NewBlockID(logfile, numOfBlocks-1)
		fm.Read(currentblk, logpage)
	}

	return &Manager{
		fm:           fm,
		logfile:      logfile,
		logpage:      logpage,
		currentBlk:   currentblk,
		latestLSN:    0,
		lastSavedLSN: 0,
	}
}

// Append adds a new log record to the log file.
// It returns the LSN assigned to this record.
//
// Block Layout:
//
//	[0-3]: boundary pointer (4 bytes) - points to start of used space (where records begin)
//	[4 to boundary-1]: free space
//	[boundary to blockSize-1]: log records (newest at boundary, oldest at end)
//
// Example of a block with records:
//
//	Block size: 100 bytes
//	Boundary: 60 (stored at offset 0-3)
//
//	Layout:
//	[0-3]:   boundary = 60
//	[4-59]:  free space (56 bytes available)
//	[60-69]: record3 (10 bytes: 4-byte length + 6-byte data)
//	[70-79]: record2 (10 bytes: 4-byte length + 6-byte data)
//	[80-99]: record1 (20 bytes: 4-byte length + 16-byte data)
//
//	When appending record4 (8 bytes data):
//	- Need 12 bytes total (4 for length + 8 for data)
//	- New position: 60 - 12 = 48
//	- Check: 48 - 4 >= 4? Yes (44 >= 4), so it fits
//	- Write record at position 48-59
//	- Update boundary to 48
func (lm *Manager) Append(logrec []byte) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.logpage.GetInt(0)
	bytesneeded := len(logrec) + 4

	// The record should fit entirely within [4, boundary] in the current block.
	// If not, we should create a new block and use that.
	//
	// Example with blockSize=100:
	// - Valid positions: 0 to 99
	// - Position 0-3: boundary pointer
	// - Position 4 to boundary-1: available for records
	// - If boundary=100 (empty block): availableSpace = 100-4 = 96 bytes
	// - If boundary=60: availableSpace = 60-4 = 56 bytes
	availableSpace := boundary - 4 // Space between position 4 and boundary
	if bytesneeded > availableSpace {
		// Record doesn't fit, need to move to a new block
		lm.flush()

		// Create and initialize new block
		// Set boundary to blockSize, this indicates the block is completely empty
		var err error
		lm.currentBlk, err = lm.fm.Append(lm.logfile)
		if err != nil {
			panic("not able to append block to log file")
		}
		lm.logpage.SetInt(0, lm.fm.BlockSize())
		lm.fm.Write(lm.currentBlk, lm.logpage)

		boundary = lm.logpage.GetInt(0)
	}

	// Calculate position where record will be written
	// Records grow downward from the boundary
	recpos := boundary - bytesneeded
	lm.logpage.SetBytesArray(recpos, logrec)

	// Write the boundary to mark the start of used space
	lm.logpage.SetInt(0, recpos)

	lm.latestLSN++

	return lm.latestLSN
}

// Flush writes the current log page to disk if there are any unsaved changes.
func (lm *Manager) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

// flush is an internal method that writes the current log page to disk.
func (lm *Manager) flush() {
	lm.fm.Write(lm.currentBlk, lm.logpage)
	lm.lastSavedLSN = lm.latestLSN
}

// Iterator returns an iterator that can be used to iterate over the log records
// from most recent to oldest.
func (lm *Manager) Iterator() *LogIterator {
	lm.flush()
	return NewLogIterator(lm.fm, lm.currentBlk)
}

// LatestLSN returns the most recently assigned LSN.
func (lm *Manager) LatestLSN() int {
	return lm.latestLSN
}

// LastSavedLSN returns the most recently saved LSN.
func (lm *Manager) LastSavedLSN() int {
	return lm.lastSavedLSN
}

// Close flushes the log and closes any open resources.
func (lm *Manager) Close() error {
	lm.flush()
	return nil
}
