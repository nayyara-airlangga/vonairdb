package wal

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/nayyara-airlangga/vonairdb/file"
	"github.com/nayyara-airlangga/vonairdb/storage"
)

func TestAppend(t *testing.T) {
	t.Run("increments latestLSN to 10", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_0"

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)

		printLogRecords(t, w, 1, 10)

		if w.latestLsn != 10 {
			t.Errorf("got %d as the latestLsn, expected %d", w.latestLsn, 10)
		}
	})

	t.Run("returns a correct LSN from append", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_1"

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)

		printLogRecords(t, w, 1, 10)

		record := createLogRecordString(t, 11)

		lsn := w.Append([]byte(record))

		if lsn != 11 {
			t.Errorf("got %d as the lsn, expected %d", lsn, 11)
		}
	})

	t.Run("returns the correct LSN after a flush", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_2"

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)

		printLogRecords(t, w, 1, 11)
		w.Flush(11)

		lsn := w.Append([]byte("record"))
		if lsn != 12 {
			t.Errorf("got %d as the lsn, expected %d", lsn, 12)
		}
	})

	t.Run("appends a new page to the file if the current one is full", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_3"

		// Remove log file before every execution
		if err := os.Remove(path.Join(dir, logFile)); err != nil {
			t.Fatalf("failed to remove log file for preparation: %v", err)
		}

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)

		if w.currentBlock.BlockNum() != 0 {
			t.Errorf("got %d for the block number, expected %d", w.currentBlock.BlockNum(), 0)
		}

		printLogRecords(t, w, 1, 1152)
		w.Flush(1024)

		if w.currentBlock.BlockNum() != 1 {
			t.Fatalf("got %d for the block number, expected %d", w.currentBlock.BlockNum(), 1)
		}
	})
}

func TestIterator(t *testing.T) {
	t.Run("returns an empty iterator if the log is empty", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_4"

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)
		iter := w.Iterator()

		val := iter.Next()

		if val != nil {
			t.Errorf("got %v, expected nil", val)
		}
	})

	t.Run("returns 10 records", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_5"

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)

		printLogRecords(t, w, 1, 10)

		iter := w.Iterator()

		for i := 10; i >= 1; i-- {
			got := iter.Next()
			expected := []byte(createLogRecordString(t, i))

			if !bytes.Equal(got, expected) {
				t.Errorf("got %d, expected %d", got, expected)
			}
		}
	})
	t.Run("returns 20 records after flush", func(t *testing.T) {
		dir := "waldir"
		logFile := "walfile_6"

		// Remove log file before every execution
		if err := os.Remove(path.Join(dir, logFile)); err != nil {
			t.Fatalf("failed to remove log file for preparation: %v", err)
		}

		fm := file.NewFileManager(dir, storage.PageSize8K)
		w := NewWalWriter(fm, logFile)

		printLogRecords(t, w, 1, 10)
		printLogRecords(t, w, 11, 20)

		w.Flush(15)

		iter := w.Iterator()

		for i := 20; i >= 1; i-- {
			got := iter.Next()
			expected := []byte(createLogRecordString(t, i))

			if !bytes.Equal(got, expected) {
				t.Errorf("got %d, expected %d", got, expected)
			}
		}
	})
}

func createLogRecordString(t *testing.T, i int) string {
	t.Helper()

	return fmt.Sprintf("record_%d", i)
}

func printLogRecords(t *testing.T, w *WalWriter, start, end int) {
	t.Helper()

	for i := start; i <= end; i++ {
		recordStr := createLogRecordString(t, i)
		_ = w.Append([]byte(recordStr))
	}
}
