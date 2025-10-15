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

// GetInt reads an integer from the specified offset
func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.bytes[offset : offset+4]))
}

// SetInt writes an integer at the specified offset
func (p *Page) SetInt(offset int, val int) {
	binary.BigEndian.PutUint32(p.bytes[offset:offset+4], uint32(val))
}

// GetBytes reads a byte array from the specified offset.
// The format is:
//   - First 4 bytes: length of the array (N)
//   - Next N bytes: the actual array data
func (p *Page) GetBytes(offset int) []byte {
	// First read the length (stored as a 4-byte integer)
	length := p.GetInt(offset)

	// Then return a slice of the actual data
	// Starting at: offset + 4 (skipping the length)
	// Ending at: offset + 4 + length (including all data)
	return p.bytes[offset+4 : offset+4+length]
}

// SetBytes writes a byte array at the specified offset.
// The format is:
//   - First 4 bytes: length of the array
//   - Next N bytes: the actual array data
func (p *Page) SetBytes(offset int, val []byte) {
	// First write the length
	p.SetInt(offset, len(val))

	// Copy the actual data starting after the length
	copy(p.bytes[offset+4:], val)
}

// GetString reads a string from the specified offset
// Strings are stored as byte arrays
func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

// SetString writes a string at the specified offset
// Strings are stored as byte arrays
func (p *Page) SetString(offset int, val string) {
	p.SetBytes(offset, []byte(val))
}

// Bytes returns the underlying byte array
func (p *Page) Bytes() []byte {
	return p.bytes
}
