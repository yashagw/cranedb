package buffer

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// Buffer represents a buffer in the buffer pool.
type Buffer struct {
	fileManager *file.Manager
	logManager  *log.Manager
	contents    *file.Page
	blk         *file.BlockID
	pins        int
	txNum       int
	lsn         int
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
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

// SetModified marks this buffer as modified by the specified transaction.
// If lsn is non-negative, it also sets the log sequence number.
func (b *Buffer) SetModified(txnum int, lsn int) {
	b.txNum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// ModifyingTx returns the transaction number that modified this buffer.
func (b *Buffer) ModifyingTx() int {
	return b.txNum
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
	if b.txNum >= 0 {
		err := b.logManager.Flush(b.lsn)
		if err != nil {
			return err
		}
		err = b.fileManager.Write(b.blk, b.contents)
		if err != nil {
			return err
		}
		b.txNum = -1
	}

	return nil
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
