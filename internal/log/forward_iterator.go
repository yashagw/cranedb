package log

import "github.com/yashagw/cranedb/internal/file"

// ForwardIterator iterates over log records oldest-to-newest (forward order).
type ForwardIterator struct {
	fm         *file.Manager
	filename   string
	currentBlk int
	totalBlks  int
	records    [][]byte
	recordIdx  int
	page       *file.Page
}

// NewForwardIterator creates a forward iterator starting at the given block.
func NewForwardIterator(fm *file.Manager, filename string, startBlk int) (*ForwardIterator, error) {
	totalBlks, err := fm.GetTotalBlocks(filename)
	if err != nil {
		return nil, err
	}

	fi := &ForwardIterator{
		fm:         fm,
		filename:   filename,
		currentBlk: startBlk,
		totalBlks:  totalBlks,
		page:       file.NewPage(fm.BlockSize()),
	}

	if startBlk < totalBlks {
		if err := fi.loadBlock(startBlk); err != nil {
			return nil, err
		}
	}

	return fi, nil
}

// loadBlock reads the given block and collects its records in forward (oldest-first) order.
func (fi *ForwardIterator) loadBlock(blkNum int) error {
	blk := file.NewBlockID(fi.filename, blkNum)
	if err := fi.fm.Read(blk, fi.page); err != nil {
		return err
	}

	boundary := fi.page.GetIntRaw(0)
	blockSize := fi.fm.BlockSize()

	var reversed [][]byte
	pos := boundary
	for pos < blockSize {
		rec := fi.page.GetBytesArrayRaw(pos)
		if len(rec) == 0 {
			break
		}
		reversed = append(reversed, rec)
		pos += 4 + len(rec)
	}

	fi.records = make([][]byte, len(reversed))
	for i, r := range reversed {
		fi.records[len(reversed)-1-i] = r
	}
	fi.recordIdx = 0
	fi.currentBlk = blkNum

	return nil
}

// HasNext returns true if there are more records in the current block or more blocks to read.
func (fi *ForwardIterator) HasNext() bool {
	if fi.recordIdx < len(fi.records) {
		return true
	}
	return fi.currentBlk+1 < fi.totalBlks
}

// Next returns the next record in forward order.
func (fi *ForwardIterator) Next() []byte {
	if fi.recordIdx >= len(fi.records) {
		nextBlk := fi.currentBlk + 1
		if nextBlk >= fi.totalBlks {
			return nil
		}
		if err := fi.loadBlock(nextBlk); err != nil {
			return nil
		}
	}

	if fi.recordIdx >= len(fi.records) {
		return nil
	}

	rec := fi.records[fi.recordIdx]
	fi.recordIdx++
	return rec
}

// Refresh re-reads the total block count and current block to pick up new records.
func (fi *ForwardIterator) Refresh() (bool, error) {
	oldTotal := fi.totalBlks
	oldRecCount := len(fi.records)

	newTotal, err := fi.fm.GetTotalBlocks(fi.filename)
	if err != nil {
		return false, err
	}
	fi.totalBlks = newTotal

	if fi.currentBlk < fi.totalBlks {
		savedIdx := fi.recordIdx
		if err := fi.loadBlock(fi.currentBlk); err != nil {
			return false, err
		}
		fi.recordIdx = savedIdx
	}

	hasNew := fi.totalBlks > oldTotal ||
		len(fi.records) > oldRecCount ||
		fi.recordIdx < len(fi.records)

	return hasNew, nil
}
