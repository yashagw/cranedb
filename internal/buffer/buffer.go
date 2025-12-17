package buffer

import (
	"log/slog"

	"github.com/yashagw/cranedb/internal/failpoint"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// Buffer represents a buffer in the buffer pool.
type Buffer struct {
	number         int // unique buffer number
	fileManager    *file.Manager
	logManager     *log.Manager
	dirtyPageTable *DirtyPageTable
	contents       *file.Page
	blk            *file.BlockID
	pins           int
	txNum          int64
	lsn            int64
}

func NewBuffer(number int, fm *file.Manager, lm *log.Manager, dirtyPageTable *DirtyPageTable) *Buffer {
	// number will be set by Manager
	return &Buffer{
		number:         number,
		fileManager:    fm,
		logManager:     lm,
		dirtyPageTable: dirtyPageTable,
		contents:       file.NewPage(fm.BlockSize()),
		blk:            nil,
		pins:           0,
		txNum:          -1,
		lsn:            -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.BlockID {
	return b.blk
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}

// SetModifiedTx marks this buffer as modified by the specified transaction.
func (b *Buffer) SetModifiedTx(txnum int64) {
	b.txNum = txnum
}

// SetModifiedLSN marks this buffer as modified with the specified LSN.
func (b *Buffer) SetModifiedLSN(lsn int64) {
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// SetBufferPageLSN sets the pageLSN of the buffer's contents.
func (b *Buffer) SetBufferPageLSN(lsn int64) {
	b.contents.SetPageLSN(lsn)
}

// ModifyingTx returns the transaction number that modified this buffer.
func (b *Buffer) ModifyingTx() int64 {
	return b.txNum
}

// ModifyingLSN returns the log sequence number associated with the last modification.
func (b *Buffer) ModifyingLSN() int64 {
	return b.lsn
}

// loadBlock assigns this buffer to the specified block.
func (b *Buffer) loadBlock(blk *file.BlockID) error {
	err := b.flush()
	if err != nil {
		return err
	}
	b.blk = blk

	numBlocks, err := b.fileManager.GetTotalBlocks(blk.Filename())
	if err != nil {
		return err
	}

	// If the block number is beyond the current file size, extend the file
	// until block is created
	for numBlocks <= blk.Number() {
		_, err = b.fileManager.Append(blk.Filename())
		if err != nil {
			return err
		}
		numBlocks++
	}

	err = b.fileManager.Read(blk, b.contents)
	if err != nil {
		return err
	}

	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	// Only write data to disk if the buffer has been modified
	if b.txNum >= 0 && b.lsn >= 0 {
		failpoint.InjectPanic("before-buffer-log-flush", failpoint.With("file", b.blk.Filename()))

		// 1. Flush log records up to the buffer's LSN
		err := b.logManager.Flush(b.lsn)
		if err != nil {
			return err
		}

		// 2. Update the pageLSN in the page before writing to disk
		b.contents.SetPageLSN(b.lsn)

		failpoint.InjectPanic("before-buffer-write-to-disk", failpoint.With("file", b.blk.Filename()))

		// 3. Write the buffer's contents to its assigned block on disk
		err = b.fileManager.Write(b.blk, b.contents)
		if err != nil {
			return err
		}

		failpoint.InjectPanic("before-buffer-remove-from-dirty-page-table", failpoint.With("file", b.blk.Filename()))

		// 4. Remove from dirty page table
		err = b.dirtyPageTable.Remove(b.blk)
		if err != nil {
			return err
		}

		// 5. Reset modification info
		b.txNum = -1
		b.lsn = -1

		slog.Info("Buffer flushed", "buffer", b.number, "block", b.blk, "lsn", b.lsn)
	}

	return nil
}
