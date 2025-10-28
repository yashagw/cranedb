package log

import (
	"errors"
	"sync"

	"github.com/yashagw/cranedb/internal/file"
)

// Manager manages the log file for the database.
// It provides methods to append log records and iterate over them.
type Manager struct {
	fileManager  *file.Manager
	logFilename  string
	logPage      *file.Page
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
func NewManager(fm *file.Manager, logFilename string) (*Manager, error) {
	logPage := file.NewPage(fm.BlockSize())

	totalBlocks, err := fm.GetTotalBlocks(logFilename)
	if err != nil {
		return nil, errors.New("not able to get total blocks in log file: " + err.Error())
	}

	var currentBlk *file.BlockID

	if totalBlocks == 0 {
		// Create and initialize new block
		// Set boundary to blockSize, this indicates the block is completely empty
		currentBlk, err = fm.Append(logFilename)
		if err != nil {
			return nil, errors.New("not able to append first block to log file: " + err.Error())
		}
		logPage.SetInt(0, fm.BlockSize())
		err = fm.Write(currentBlk, logPage)
		if err != nil {
			return nil, errors.New("not able to write first block to log file: " + err.Error())
		}
	} else {
		// Use the last block (blocks are zero-indexed, so the last block is totalBlocks - 1)
		// This makes the last existing block the current log block for appending new records.
		currentBlk = file.NewBlockID(logFilename, totalBlocks-1)
		err = fm.Read(currentBlk, logPage)
		if err != nil {
			return nil, errors.New("not able to read last block from log file: " + err.Error())
		}
	}

	return &Manager{
		fileManager:  fm,
		logFilename:  logFilename,
		logPage:      logPage,
		currentBlk:   currentBlk,
		latestLSN:    0,
		lastSavedLSN: 0,
	}, nil
}

// Close flushes the log and closes any open resources.
func (lm *Manager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.flush()
}

// Flush writes the current log page to disk if there are any unsaved changes.
func (lm *Manager) Flush(lsn int) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lsn > lm.lastSavedLSN {
		return lm.flush()
	}
	return nil
}

// Iterator returns an iterator that can be used to iterate over the log records
// from most recent to oldest.
func (lm *Manager) Iterator() (*LogIterator, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	err := lm.flush()
	if err != nil {
		return nil, errors.New("not able to flush log page to disk: " + err.Error())
	}
	return NewLogIterator(lm.fileManager, lm.currentBlk), nil
}

// flush is an internal method that writes the current log page to disk.
// It assumes that the mutex is already locked.
func (lm *Manager) flush() error {
	err := lm.fileManager.Write(lm.currentBlk, lm.logPage)
	if err != nil {
		return errors.New("not able to write log page to disk: " + err.Error())
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
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
func (lm *Manager) Append(logrec []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.logPage.GetInt(0)
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
		var err error

		// Record doesn't fit, need to move to a new block
		err = lm.flush()
		if err != nil {
			return 0, err
		}

		// Create and initialize new block
		// Set boundary to blockSize, this indicates the block is completely empty
		lm.currentBlk, err = lm.fileManager.Append(lm.logFilename)
		if err != nil {
			return 0, errors.New("not able to append block to log file: " + err.Error())
		}
		lm.logPage.SetInt(0, lm.fileManager.BlockSize())
		err = lm.fileManager.Write(lm.currentBlk, lm.logPage)
		if err != nil {
			return 0, errors.New("not able to write block to log file: " + err.Error())
		}

		boundary = lm.logPage.GetInt(0)
	}

	// Calculate position where record will be written
	// Records grow downward from the boundary
	recpos := boundary - bytesneeded
	lm.logPage.SetBytesArray(recpos, logrec)

	// Write the boundary to mark the start of used space
	lm.logPage.SetInt(0, recpos)
	lm.latestLSN++

	return lm.latestLSN, nil
}
