package log

import "github.com/yashagw/cranedb/internal/file"

// LogIterator provides a way to iterate over log records.
// ITERATION STRATEGY:
// - Start at the current block's boundary (newest record in that block)
// - Read records moving toward blockSize (newest to oldest within block)
// - When block is exhausted, move to previous block and repeat
type LogIterator struct {
	fm         *file.Manager
	blk        *file.BlockID
	page       *file.Page
	currentpos int
	boundary   int
}

// NewLogIterator creates a new iterator for the log file, starting at the given block.
func NewLogIterator(fm *file.Manager, blk *file.BlockID) *LogIterator {
	it := &LogIterator{
		fm:   fm,
		blk:  blk,
		page: file.NewPage(fm.BlockSize()),
	}
	it.moveToBlock(blk)
	return it
}

// HasNext returns true if there are more log records to read.
func (it *LogIterator) HasNext() bool {
	return it.currentpos < it.fm.BlockSize() || it.blk.Number() > 0
}

// Next returns the next log record.
func (it *LogIterator) Next() []byte {
	// If we've read all records in current block, move to previous block
	if it.currentpos >= it.fm.BlockSize() {
		if it.blk.Number() == 0 {
			return nil
		}
		it.blk = file.NewBlockID(it.blk.Filename(), it.blk.Number()-1)
		it.moveToBlock(it.blk)
	}

	// Read current record and advance position
	rec := it.page.GetBytes(it.currentpos)
	it.currentpos += 4 + len(rec) // Move past this record (4 bytes length + data)
	return rec
}

// moveToBlock moves the iterator to the specified block and reads its contents.
func (it *LogIterator) moveToBlock(blk *file.BlockID) {
	it.fm.Read(blk, it.page)
	it.boundary = it.page.GetInt(0)
	// Start at the boundary (newest record)
	it.currentpos = it.boundary
}
