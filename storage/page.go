package storage

import (
	"encoding/binary"

	"github.com/nayyara-airlangga/vonairdb/internal/runtime"
)

type PageSize uint

// Offset is the offset of a field within a page
// Because pages are fixed 8KB, we can use a uint16 to address any offset within a page
type Offset uint16

const (
	PageSize8K PageSize = 8 * 1024

	IntOffset Offset = 4
)

type (
	Int  uint32
	Long uint64
)

// A database page with a fixed size of 8KB
type Page struct {
	buf [PageSize8K]byte
}

func NewPage() *Page {

	return &Page{}
}

func NewPageFromBuf(buf [PageSize8K]byte) *Page {
	return &Page{buf}
}

// Gets a 4 byte integer from an offset in the page. Bytes are stored in big endian format
func (p Page) GetInt(offset Offset) Int {
	data := binary.BigEndian.Uint32(p.buf[offset : offset+IntOffset])

	return Int(data)
}

// Sets a 4 byte integer starting from an offset in the page. Bytes are stored in big endian format
func (p *Page) SetInt(offset Offset, val Int) {
	from := offset
	to := offset + IntOffset

	runtime.Assert(to <= Offset(PageSize8K), "SetInt offset out of bounds (from: %d, to: %d)", from, to)

	binary.BigEndian.PutUint32(p.buf[from:to], uint32(val))
}

// Gets an array of bytes from an offset in the page. The first 4 bytes stores
// the length of bytes to fetch
func (p Page) GetBytes(offset Offset) []byte {
	length := p.GetInt(offset)
	from := offset + IntOffset
	to := from + Offset(length)

	return p.buf[from:to]
}

// Sets an array of bytes starting from an offset in the page. The first 4 bytes are reserved for the length of the bytes. Bytes are stored in big endian format
func (p *Page) SetBytes(offset Offset, b []byte) {
	length := Int(len(b))
	from := offset
	to := offset + IntOffset + Offset(length)

	runtime.Assert(to <= Offset(PageSize8K), "SetBytes offset out of bounds (from: %d, to: %d)", from, to)

	p.SetInt(offset, length)

	copy(p.buf[from+IntOffset:to], b)
}

// Gets a string from an offset in the page. The first 4 bytes stores the length of the string. Strings are UTF-8 encoded
func (p Page) GetString(offset Offset) string {
	b := p.GetBytes(offset)

	return string(b)
}

// Sets a string starting from an offset in the page. The first 4 bytes are reserved for the length of the string as bytes. Strings are UTF-8 encoded and the bytes are stored in big endian format
func (p *Page) SetString(offset Offset, s string) {
	b := []byte(s)

	p.SetBytes(offset, b)
}

// Retuns the maximum amount of bytes needed to store a string. For now, assume
// all strings are UTF-8 encoded where the maximum bytes to store one character is 4 bytes
func (p Page) MaxStrLen(strlen Int) Int {
	return Int(IntOffset) + 4*strlen
}

// Returns all the contents of a page as a reference for writing/reading
func (p *Page) Contents() []byte {
	return p.buf[:]
}
