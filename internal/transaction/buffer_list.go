package transaction

import (
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
)

type BufferList struct {
	bufferManager *buffer.Manager

	buffers map[file.BlockID]*buffer.Buffer
	pins    map[file.BlockID]int // Track pin count for each block
}

func NewBufferList(bufferManager *buffer.Manager) *BufferList {
	return &BufferList{
		bufferManager: bufferManager,
		buffers:       make(map[file.BlockID]*buffer.Buffer),
		pins:          make(map[file.BlockID]int),
	}
}

func (bl *BufferList) GetBuffer(blk *file.BlockID) *buffer.Buffer {
	return bl.buffers[file.MakeBlockKey(blk)]
}

func (bl *BufferList) Pin(blk *file.BlockID) (*buffer.Buffer, error) {
	key := file.MakeBlockKey(blk)

	// If buffer is already pinned, just increment pin count
	if pinCount, exists := bl.pins[key]; exists {
		bl.pins[key] = pinCount + 1
		return bl.buffers[key], nil
	}

	// First time pinning this buffer
	buff, err := bl.bufferManager.Pin(blk)
	if err != nil {
		return nil, err
	}
	bl.buffers[key] = buff
	bl.pins[key] = 1
	return buff, nil
}

func (bl *BufferList) Unpin(blk *file.BlockID) {
	key := file.MakeBlockKey(blk)
	if pinCount, exists := bl.pins[key]; exists {
		bl.pins[key] = pinCount - 1

		// If pin count reaches zero, unpin from buffer manager and remove from maps
		if bl.pins[key] == 0 {
			bl.bufferManager.Unpin(bl.buffers[key])
			delete(bl.buffers, key)
			delete(bl.pins, key)
		}
	}
}

func (bl *BufferList) UnpinAll() {
	for _, buff := range bl.buffers {
		bl.bufferManager.Unpin(buff)
	}
	bl.buffers = make(map[file.BlockID]*buffer.Buffer)
	bl.pins = make(map[file.BlockID]int)
}
