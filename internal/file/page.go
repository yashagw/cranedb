package file

import (
	"encoding/binary"
)

// Page represents a block of data in memory
type Page struct {
	bytes []byte
}

// NewPage creates a new page with the specified block size
func NewPage(blockSize int) *Page {
	return &Page{
		bytes: make([]byte, blockSize),
	}
}

// NewPageFromBytes creates a new page from an existing byte array
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		bytes: b,
	}
}

// Bytes returns the underlying byte array
func (p *Page) Bytes() []byte {
	return p.bytes
}

// GetInt reads an integer from the specified offset
func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.bytes[offset : offset+4]))
}

// SetInt writes an integer at the specified offset
func (p *Page) SetInt(offset int, val int) {
	binary.BigEndian.PutUint32(p.bytes[offset:offset+4], uint32(val))
}

// GetBytesArray reads a byte array from the specified offset.
// The format is:
//   - First 4 bytes: length of the array
//   - Next N bytes: the actual array data
func (p *Page) GetBytesArray(offset int) []byte {
	length := p.GetInt(offset)

	// Validate length to prevent slice bounds errors from garbage data
	if length < 0 || offset+4+length > len(p.bytes) {
		return []byte{}
	}

	return p.bytes[offset+4 : offset+4+length]
}

// SetBytesArray writes a byte array at the specified offset.
// The format is:
//   - First 4 bytes: length of the array
//   - Next N bytes: the actual array data
func (p *Page) SetBytesArray(offset int, val []byte) {
	p.SetInt(offset, len(val))
	copy(p.bytes[offset+4:], val)
}

// GetString reads a string from the specified offset
func (p *Page) GetString(offset int) string {
	return string(p.GetBytesArray(offset))
}

// SetString writes a string at the specified offset
func (p *Page) SetString(offset int, val string) {
	p.SetBytesArray(offset, []byte(val))
}
