package file

// BlockID represents a block in a file
type BlockID struct {
	filename string
	blkNum   int
}

// NewBlockID creates a new BlockID instance
func NewBlockID(filename string, blkNum int) *BlockID {
	return &BlockID{
		filename: filename,
		blkNum:   blkNum,
	}
}

// Filename returns the name of the file containing this block
func (b *BlockID) Filename() string {
	return b.filename
}

// Number returns the block number
func (b *BlockID) Number() int {
	return b.blkNum
}
