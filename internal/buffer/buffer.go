package buffer

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// Buffer represents a buffer in the buffer pool.
type Buffer struct {
	fm       *file.Manager
	lm       *log.Manager
	contents *file.Page
	blk      *file.BlockID
	pins     int
	txnum    int
	lsn      int
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(fm.BlockSize()),
		blk:      nil,
		pins:     0,
		txnum:    -1,
		lsn:      -1,
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
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// ModifyingTx returns the transaction number that modified this buffer.
func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

// assignToBlock assigns this buffer to the specified block.
func (b *Buffer) assignToBlock(blk *file.BlockID) {
	b.flush()
	b.blk = blk

	numBlocks, err := b.fm.GetNumBlocks(blk.Filename())
	if err != nil {
		panic(err)
	}

	// If the block number is beyond the current file size, extend the file
	// until block is created
	for numBlocks <= blk.Number() {
		b.fm.Append(blk.Filename())
		numBlocks++
	}

	b.fm.Read(blk, b.contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.blk, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
