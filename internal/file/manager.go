package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Manager manages disk files as fixed-size blocks.
// Each block is the same size as a Page.
// Page is the in-memory representation of a block
// - Read: BlockID → load block from disk → store in Page
// - Modify: change data in Page
// - Write: Page → write back to disk at BlockID location
type Manager struct {
	blockSize   int
	dbDir       string
	openedFiles map[string]*os.File
	mu          sync.Mutex
}

// NewManager creates a new file manager for the specified directory
func NewManager(dbDir string, blockSize int) (*Manager, error) {
	_, err := os.Stat(dbDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dbDir, 0755)
		if err != nil {
			return nil, errors.New("failed to create database directory: " + err.Error())
		}
	}

	return &Manager{
		blockSize:   blockSize,
		dbDir:       dbDir,
		openedFiles: make(map[string]*os.File),
	}, nil
}

// BlockSize returns the block size
func (fm *Manager) BlockSize() int {
	return fm.blockSize
}

// Read reads the contents of the specified block into the provided page.
// Can only read blocks that exist (0 to numBlocks-1).
func (fm *Manager) Read(blk *BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if blk.Number() < 0 {
		return errors.New("negative block number not allowed")
	}

	f, err := fm.getFile(blk.Filename())
	if err != nil {
		return errors.New("failed to get file: " + err.Error())
	}

	numBlocks, err := fm.GetTotalBlocks(blk.Filename())
	if err != nil {
		return errors.New("failed to get number of blocks: " + err.Error())
	}

	// Can only read blocks that actually exist in the file
	if blk.Number() >= numBlocks {
		return errors.New("cannot read block: file only has " + strconv.Itoa(numBlocks) + " blocks")
	}

	_, err = f.ReadAt(p.Bytes(), int64(blk.Number()*fm.blockSize))
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.New("failed to read file: " + err.Error())
	}

	return nil
}

// Write writes the contents of the provided page to the specified block.
func (fm *Manager) Write(blk *BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if blk.Number() < 0 {
		return errors.New("negative block number not allowed")
	}

	f, err := fm.getFile(blk.Filename())
	if err != nil {
		return errors.New("failed to get file: " + err.Error())
	}

	_, err = f.WriteAt(p.Bytes(), int64(blk.Number()*fm.blockSize))
	if err != nil {
		return errors.New("failed to write file: " + err.Error())
	}

	return nil
}

// Append adds a new block to the end of the specified file and returns its BlockID.
// The new block is initialized with zeros.
func (fm *Manager) Append(filename string) (*BlockID, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Get the next block number
	numBlocks, err := fm.GetTotalBlocks(filename)
	if err != nil {
		return nil, errors.New("failed to get number of blocks: " + err.Error())
	}

	// Create the new block ID
	blk := NewBlockID(filename, numBlocks)

	emptyBytes := make([]byte, fm.blockSize)

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, errors.New("failed to get file: " + err.Error())
	}

	_, err = f.WriteAt(emptyBytes, int64(blk.Number()*fm.blockSize))
	if err != nil {
		return nil, errors.New("cannot append block: " + blk.String() + ": " + err.Error())
	}

	return blk, nil
}

// Close closes all opened files
func (fm *Manager) Close() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for name, f := range fm.openedFiles {
		err := f.Close()
		if err != nil {
			panic(fmt.Errorf("failed to close file %s: %w", name, err))
		}
		delete(fm.openedFiles, name)
	}
}

// GetTotalBlocks returns the number of blocks in the specified file
// Blocks are 0-indexed, so a file with blocks 0,1,2,3,4 has count 5.
func (fm *Manager) GetTotalBlocks(filename string) (int, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}

	return int(fi.Size() / int64(fm.blockSize)), nil
}

// getFile returns the file with the specified filename, creating it if it does not exist
func (fm *Manager) getFile(filename string) (*os.File, error) {
	f, ok := fm.openedFiles[filename]
	if ok {
		return f, nil
	}

	f, err := os.OpenFile(filepath.Join(fm.dbDir, filename), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	fm.openedFiles[filename] = f

	return f, nil
}
