package wal

import (
	"sync"

	"github.com/nayyara-airlangga/vonairdb/file"
	"github.com/nayyara-airlangga/vonairdb/storage"
)

var iteratorPagePool = sync.Pool{
	New: func() any {
		return storage.NewPage()
	},
}

// An iterator for the log records written in a WAL file on disk.
// It reads each page in the file starting from the newest page and iterates
// through the records in the page from left to right (latest to oldest)
type WalIterator struct {
	fm           *file.FileManager
	block        storage.Block
	page         *storage.Page
	currentPos   storage.Offset
	pageBoundary storage.Offset
}

func newWalIterator(fm *file.FileManager, block storage.Block) *WalIterator {
	page := iteratorPagePool.Get().(*storage.Page)

	it := &WalIterator{
		fm:    fm,
		block: block,
		page:  page,
	}

	return it
}

func (it *WalIterator) HasNext() bool {
	return it.currentPos < storage.Offset(it.fm.PageSize()) || it.block.BlockNum() > 0
}

func (it *WalIterator) Next() []byte {
	if it.currentPos == storage.Offset(it.fm.PageSize()) {
		if it.block.BlockNum() == 0 {
			return nil
		}

		prevBlock := storage.NewBlock(it.block.Filename(), it.block.BlockNum()-1)

		it.moveToBlock(prevBlock)
	}

	// After retrieving the log record from the page, we update the current
	// position of the iterator by summing it with the number of bytes to store
	// the record length (4 in this case) and the length of the record itself
	// because log records are always stored alongside its length
	record := it.page.GetBytes(it.currentPos)
	it.currentPos += storage.IntOffset + storage.Offset(len(record))

	return record
}

func (it *WalIterator) moveToBlock(block storage.Block) {
	it.fm.Read(block, it.page)

	// The first 4 bytes in a log page contains the position of the latest
	// log record for that page. This value is set as the page boundary
	// when moving to a new page so we know where to start reading in a new page.
	//
	// The current position is also set to the page boundary upon reading a new
	// page
	it.pageBoundary = storage.Offset(it.page.GetInt(0))
	it.currentPos = it.pageBoundary

	it.block = block
}
