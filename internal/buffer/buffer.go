package buffer

import (
	"log/slog"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// Buffer represents a buffer in the buffer pool.
type Buffer struct {
	number      int // unique buffer number
	fileManager *file.Manager
	logManager  *log.Manager
	contents    *file.Page
	blk         *file.BlockID
	pins        int
	txNum       int64
	lsn         int64
}

func NewBuffer(fm *file.Manager, lm *log.Manager, number int) *Buffer {
	// number will be set by Manager
	return &Buffer{
		number:      number,
		fileManager: fm,
		logManager:  lm,
		contents:    file.NewPage(fm.BlockSize()),
		blk:         nil,
		pins:        0,
		txNum:       -1,
		lsn:         -1,
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

// SetModified marks this buffer as modified by the specified transaction.
// If lsn is non-negative, it also sets the log sequence number.
// Note: pageLSN is only updated when the buffer is flushed to disk, not here.
func (b *Buffer) SetModified(txnum int64, lsn int64) {
	b.txNum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
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
	if b.txNum >= 0 && b.lsn >= 0 {
		err := b.logManager.Flush(b.lsn)
		if err != nil {
			return err
		}

		b.contents.SetPageLSN(b.lsn)

		err = b.fileManager.Write(b.blk, b.contents)
		if err != nil {
			return err
		}

		b.txNum = -1
		b.lsn = -1

		slog.Info("Buffer flushed", "buffer", b.number, "block", b.blk, "lsn", b.lsn)
	}

	return nil
}
