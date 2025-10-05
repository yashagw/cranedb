package filemanager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	// ErrNegativeBlock is returned when trying to access a negative block number
	ErrNegativeBlock = errors.New("negative block number not allowed")
)

// FileMgr manages disk files as fixed-size blocks.
// Each block is the same size as a Page.
// Think of Page as the in-memory version of a block
// - we Read a block into a Page,
// - Modify the Page in memory then Write it back to the block on disk.
type FileMgr struct {
	blockSize   int
	dbDir       string
	openedFiles map[string]*os.File
	mu          sync.Mutex
}

// NewFileMgr creates a new file manager for the specified directory
func NewFileMgr(dbDir string, blockSize int) *FileMgr {
	_, err := os.Stat(dbDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dbDir, 0755)
		if err != nil {
			panic(fmt.Errorf("failed to create database directory: %w", err))
		}
	}

	return &FileMgr{
		blockSize:   blockSize,
		dbDir:       dbDir,
		openedFiles: make(map[string]*os.File),
	}
}

// Read reads the contents of the specified block into the provided page.
// Can only read blocks that exist (0 to numBlocks-1).
func (fm *FileMgr) Read(blk *BlockID, p *Page) {
	if blk.Number() < 0 {
		panic(ErrNegativeBlock)
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.GetFile(blk.Filename())
	if err != nil {
		panic(fmt.Errorf("failed to get file: %w", err))
	}

	numBlocks, err := fm.GetNumBlocks(blk.Filename())
	if err != nil {
		panic(fmt.Errorf("failed to get number of blocks: %w", err))
	}

	// Can only read blocks that actually exist in the file
	if blk.Number() >= numBlocks {
		panic(fmt.Errorf("cannot read block %d: file only has %d blocks", blk.Number(), numBlocks))
	}

	_, err = f.ReadAt(p.Bytes(), int64(blk.Number()*fm.blockSize))
	if err != nil && !errors.Is(err, io.EOF) {
		panic(fmt.Errorf("failed to read file: %w", err))
	}
}

// Write writes the contents of the provided page to the specified block.
func (fm *FileMgr) Write(blk *BlockID, p *Page) {
	if blk.Number() < 0 {
		panic(ErrNegativeBlock)
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.GetFile(blk.Filename())
	if err != nil {
		panic(fmt.Errorf("failed to get file: %w", err))
	}

	_, err = f.WriteAt(p.Bytes(), int64(blk.Number()*fm.blockSize))
	if err != nil {
		panic(fmt.Errorf("failed to write file: %w", err))
	}
}

// Append adds a new block to the end of the specified file and returns its BlockID.
// The new block is initialized with zeros.
func (fm *FileMgr) Append(filename string) *BlockID {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Get the next block number
	numBlocks, err := fm.GetNumBlocks(filename)
	if err != nil {
		panic(fmt.Errorf("failed to get number of blocks: %w", err))
	}

	// Create the new block ID
	blk := NewBlockID(filename, numBlocks)

	emptyBytes := make([]byte, fm.blockSize)

	f, err := fm.GetFile(filename)
	if err != nil {
		panic(fmt.Errorf("failed to get file: %w", err))
	}

	_, err = f.WriteAt(emptyBytes, int64(blk.Number()*fm.blockSize))
	if err != nil {
		panic(fmt.Errorf("cannot append block %v: %w", blk, err))
	}

	return blk
}

// BlockSize returns the block size used by this file manager
func (fm *FileMgr) BlockSize() int {
	return fm.blockSize
}

// Close closes all opened files
func (fm *FileMgr) Close() {
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

// GetNumBlocks returns the number of blocks in the specified file
func (fm *FileMgr) GetNumBlocks(filename string) (int, error) {
	f, err := fm.GetFile(filename)
	if err != nil {
		return 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}

	return int(fi.Size() / int64(fm.blockSize)), nil
}

// GetFile returns the file with the specified filename, creating it if it does not exist
func (fm *FileMgr) GetFile(filename string) (*os.File, error) {
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
