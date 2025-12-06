package file

import (
	"encoding/binary"
)

const (
	// PageHeaderSize is the size of the page header in bytes.
	// The header contains the pageLSN (8 bytes).
	PageHeaderSize = 8

	// PageLSNOffset is the offset where pageLSN is stored in the page header.
	// PageLSN is stored as int64 (8 bytes) at the beginning of the page.
	PageLSNOffset = 0
)

// Page represents a block of data in memory.
// Pages have a header containing the pageLSN, followed by the actual data.
type Page struct {
	bytes []byte
}

// NewPage creates a new page with the specified block size.
// The page is initialized with zeros.
func NewPage(blockSize int) *Page {
	return &Page{
		bytes: make([]byte, blockSize),
	}
}

// NewPageFromBytes creates a new page from an existing byte array.
// The page uses the provided byte array directly (no copy is made).
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		bytes: b,
	}
}

// Bytes returns the underlying byte array of the page.
func (p *Page) Bytes() []byte {
	return p.bytes
}

// GetPageLSN reads the pageLSN from the page header.
// PageLSN is the LSN (Log Sequence Number) of the last log record that modified this page.
// Returns -1 if the page is too small to contain a pageLSN.
func (p *Page) GetPageLSN() int64 {
	if len(p.bytes) < PageLSNOffset+8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(p.bytes[PageLSNOffset : PageLSNOffset+8]))
}

// SetPageLSN writes the pageLSN to the page header.
// PageLSN is the LSN (Log Sequence Number) of the last log record that modified this page.
// Does nothing if the page is too small to contain a pageLSN.
func (p *Page) SetPageLSN(lsn int64) {
	if len(p.bytes) < PageLSNOffset+8 {
		return
	}
	binary.BigEndian.PutUint64(p.bytes[PageLSNOffset:PageLSNOffset+8], uint64(lsn))
}

// Raw methods - operate directly on byte offsets without adding page header offset.
// These are used for log pages and other special cases.

// GetIntRaw reads a 32-bit signed integer from the specified offset.
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) GetIntRaw(offset int) int {
	if offset+4 > len(p.bytes) {
		panic("GetIntRaw: offset out of bounds")
	}
	return int(int32(binary.BigEndian.Uint32(p.bytes[offset : offset+4])))
}

// SetIntRaw writes a 32-bit signed integer at the specified offset.
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) SetIntRaw(offset int, val int) {
	if offset+4 > len(p.bytes) {
		panic("SetIntRaw: offset out of bounds")
	}
	binary.BigEndian.PutUint32(p.bytes[offset:offset+4], uint32(val))
}

// GetInt64Raw reads a 64-bit signed integer from the specified offset.
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) GetInt64Raw(offset int) int64 {
	if offset+8 > len(p.bytes) {
		panic("GetInt64Raw: offset out of bounds")
	}
	return int64(binary.BigEndian.Uint64(p.bytes[offset : offset+8]))
}

// SetInt64Raw writes a 64-bit signed integer at the specified offset.
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) SetInt64Raw(offset int, val int64) {
	if offset+8 > len(p.bytes) {
		panic("SetInt64Raw: offset out of bounds")
	}
	binary.BigEndian.PutUint64(p.bytes[offset:offset+8], uint64(val))
}

// GetBytesArrayRaw reads a byte array from the specified offset.
// The format is: first 4 bytes contain the length (as int32), followed by the actual data.
// The offset is absolute (does not account for page header).
// Returns an empty byte slice if the offset is out of bounds or the data is invalid.
func (p *Page) GetBytesArrayRaw(offset int) []byte {
	if offset+4 > len(p.bytes) {
		return []byte{}
	}
	length := int(int32(binary.BigEndian.Uint32(p.bytes[offset : offset+4])))

	// Validate length to prevent slice bounds errors from garbage data
	if length < 0 || offset+4+length > len(p.bytes) {
		return []byte{}
	}

	return p.bytes[offset+4 : offset+4+length]
}

// SetBytesArrayRaw writes a byte array at the specified offset.
// The format is: first 4 bytes contain the length (as int32), followed by the actual data.
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) SetBytesArrayRaw(offset int, val []byte) {
	if offset < 0 || offset+4+len(val) > len(p.bytes) {
		panic("SetBytesArrayRaw: offset out of bounds")
	}
	binary.BigEndian.PutUint32(p.bytes[offset:offset+4], uint32(len(val)))
	copy(p.bytes[offset+4:], val)
}

// GetStringRaw reads a string from the specified offset.
// The string is stored as a byte array (see GetBytesArrayRaw for format).
// The offset is absolute (does not account for page header).
func (p *Page) GetStringRaw(offset int) string {
	return string(p.GetBytesArrayRaw(offset))
}

// SetStringRaw writes a string at the specified offset.
// The string is stored as a byte array (see SetBytesArrayRaw for format).
// The offset is absolute (does not account for page header).
func (p *Page) SetStringRaw(offset int, val string) {
	p.SetBytesArrayRaw(offset, []byte(val))
}

// GetBoolRaw reads a boolean from the specified offset (1 byte).
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) GetBoolRaw(offset int) bool {
	if offset >= len(p.bytes) {
		panic("GetBoolRaw: offset out of bounds")
	}
	return p.bytes[offset] == 1
}

// SetBoolRaw writes a boolean at the specified offset (1 byte).
// The offset is absolute (does not account for page header).
// Panics if the offset is out of bounds.
func (p *Page) SetBoolRaw(offset int, val bool) {
	if offset >= len(p.bytes) {
		panic("SetBoolRaw: offset out of bounds")
	}
	if val {
		p.bytes[offset] = 1
	} else {
		p.bytes[offset] = 0
	}
}

// Regular methods - automatically account for page header offset.
// These are used for regular data pages.

// GetInt reads a 32-bit signed integer from the specified offset.
// The offset is relative to the start of the data (after the page header).
// Properly handles signed integers by converting through int32 to preserve sign.
func (p *Page) GetInt(offset int) int {
	return p.GetIntRaw(PageHeaderSize + offset)
}

// SetInt writes a 32-bit signed integer at the specified offset.
// The offset is relative to the start of the data (after the page header).
func (p *Page) SetInt(offset int, val int) {
	p.SetIntRaw(PageHeaderSize+offset, val)
}

// GetInt64 reads a 64-bit signed integer from the specified offset.
// The offset is relative to the start of the data (after the page header).
func (p *Page) GetInt64(offset int) int64 {
	return p.GetInt64Raw(PageHeaderSize + offset)
}

// SetInt64 writes a 64-bit signed integer at the specified offset.
// The offset is relative to the start of the data (after the page header).
func (p *Page) SetInt64(offset int, val int64) {
	p.SetInt64Raw(PageHeaderSize+offset, val)
}

// GetBytesArray reads a byte array from the specified offset.
// The format is: first 4 bytes contain the length (as int32), followed by the actual data.
// The offset is relative to the start of the data (after the page header).
func (p *Page) GetBytesArray(offset int) []byte {
	return p.GetBytesArrayRaw(PageHeaderSize + offset)
}

// SetBytesArray writes a byte array at the specified offset.
// The format is: first 4 bytes contain the length (as int32), followed by the actual data.
// The offset is relative to the start of the data (after the page header).
func (p *Page) SetBytesArray(offset int, val []byte) {
	p.SetBytesArrayRaw(PageHeaderSize+offset, val)
}

// GetString reads a string from the specified offset.
// The string is stored as a byte array (see GetBytesArray for format).
// The offset is relative to the start of the data (after the page header).
func (p *Page) GetString(offset int) string {
	return string(p.GetBytesArray(offset))
}

// SetString writes a string at the specified offset.
// The string is stored as a byte array (see SetBytesArray for format).
// The offset is relative to the start of the data (after the page header).
func (p *Page) SetString(offset int, val string) {
	p.SetBytesArray(offset, []byte(val))
}

// GetBool reads a boolean from the specified offset (1 byte).
// The offset is relative to the start of the data (after the page header).
func (p *Page) GetBool(offset int) bool {
	return p.GetBoolRaw(PageHeaderSize + offset)
}

// SetBool writes a boolean at the specified offset (1 byte).
// The offset is relative to the start of the data (after the page header).
func (p *Page) SetBool(offset int, val bool) {
	p.SetBoolRaw(PageHeaderSize+offset, val)
}
