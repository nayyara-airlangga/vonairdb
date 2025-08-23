package file_test

import (
	"testing"

	"github.com/nayyara-airlangga/vonairdb/file"
	"github.com/nayyara-airlangga/vonairdb/storage"
)

func TestFileManager(t *testing.T) {
	dir := "testdir"
	filename := "testfile"
	blockNum := 2

	fm := file.NewFileManager(dir, storage.PageSize8K)

	block := storage.NewBlock(filename, uint(blockNum))
	p1 := storage.NewPage()

	offset1 := storage.Offset(88)
	strval := "abcdefghijklm"

	p1.SetString(offset1, strval)

	size := p1.MaxStrLen(storage.Int(len(strval)))
	intv := 345
	offset2 := offset1 + storage.Offset(size)

	p1.SetInt(offset2, storage.Int(intv))
	fm.Write(block, p1)

	p2 := storage.NewPage()

	fm.Read(block, p2)

	intGot := p2.GetInt(offset2)
	if intGot != storage.Int(intv) {
		t.Errorf("expected %d at offset %d, got %d", intv, offset2, intGot)
	}

	strGot := p2.GetString(offset1)
	if strGot != strval {
		t.Errorf("expected %q at offset %d, got %q", strval, offset1, strGot)
	}
}
