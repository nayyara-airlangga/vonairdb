package storage

import (
	"encoding/binary"

	"github.com/nayyara-airlangga/chevondb/internal/runtime"
)

type PageSize uint

// Offset is the offset of a field within a page
// Because pages are fixed 8KB, we can use a uint16 to address any offset within a page
type Offset uint16

const (
	PageSize8K PageSize = 8 * 1024

	IntOffset Offset = 4
)

type Int uint32

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
