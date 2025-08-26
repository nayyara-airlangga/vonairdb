package wal

import (
	"sync"

	"github.com/nayyara-airlangga/vonairdb/file"
	"github.com/nayyara-airlangga/vonairdb/storage"
)

// Manages the writing and flushing of WAL pages to disk
type WalWriter struct {
	fm           *file.FileManager
	logFile      string
	logPage      *storage.Page
	currentBlock storage.Block
	latestLsn    int
	lastSavedLsn int
	sync.Mutex
}

func NewWalWriter(fm *file.FileManager, logFile string) *WalWriter {
	logSize := fm.FileSizeInBlocks(logFile)

	logPage := storage.NewPage()

	w := &WalWriter{
		fm:           fm,
		logFile:      logFile,
		logPage:      logPage,
		latestLsn:    0,
		lastSavedLsn: 0,
	}

	if logSize == 0 {
		w.currentBlock = w.appendNewBlock()
	} else {
		w.currentBlock = storage.NewBlock(logFile, uint(logSize-1))
		fm.Read(w.currentBlock, w.logPage)
	}

	return w
}

// Checks if the requested LSN is newer than the last saved
// LSN. If the requested LSN is newer, then flush the current WAL page to disk.
// Requested LSN doesn't have to be latestLsn in memory to trigger a flush as
// long as it's still newer than what was saved in disk
func (w *WalWriter) Flush(lsn int) {
	if lsn >= w.lastSavedLsn {
		w.flush()
	}
}

func (w *WalWriter) Iterator() *WalIterator {
	w.flush()
	return newWalIterator(w.fm, w.currentBlock)
}

// Writes the contents of the current WAL page in memory to the page in disk
// while updating the last saved LSN to latest
func (w *WalWriter) flush() {
	w.fm.Write(w.currentBlock, w.logPage)
	w.lastSavedLsn = w.latestLsn
}

// Appends a log record to the log page
//
// If the record doesn't fit in the current log page, the contents of the
// current log page will be flushed to disk and we will create a new log page
// to write the record in.
//
// Log records are written from the end to the beginning of the page. The
// first 4 bytes of the page tracks the current position of the latest log
// record in the page. This allows the log iterator to parse through the logs
// starting from the latest to oldest
func (w *WalWriter) Append(record []byte) int {
	w.Lock()
	defer w.Unlock()

	// The current position in the page is stored in the first few bytes
	logPos := storage.Offset(w.logPage.GetInt(0))
	recSize := len(record)
	// The required size to storage a log record is the bytes for the record
	// length + the length of the record itself
	requiredSize := storage.Offset(recSize) + storage.IntOffset

	// If the required size to store a record + the space to store the log
	// position is greater than the current log position, then we don't
	// have enough space in the WAL page tos tore the record. We need to flush
	// the current page to disk first and initialize a new page for the record
	if requiredSize+storage.IntOffset > logPos {
		w.flush()
		w.currentBlock = w.appendNewBlock()
		logPos = storage.Offset(w.logPage.GetInt(0))
	}

	recPos := logPos - requiredSize

	w.logPage.SetBytes(recPos, record)
	w.logPage.SetInt(0, storage.Int(recPos))

	w.latestLsn++

	return w.latestLsn
}

// Appnds a new block at the end of a log file using the `FileManager`.
// The block size is inserted at the beginning of the page to help keep track
// of the position when appending new log records
func (w *WalWriter) appendNewBlock() storage.Block {
	blockNum := w.fm.FileSizeInBlocks(w.logFile)
	block := storage.NewBlock(w.logFile, uint(blockNum))

	w.logPage = storage.NewPage()

	w.logPage.SetInt(0, storage.Int(w.fm.PageSize()))

	w.fm.Write(block, w.logPage)

	return block
}
