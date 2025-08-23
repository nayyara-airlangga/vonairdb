package file

import (
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/nayyara-airlangga/vonairdb/storage"
)

const (
	tmpTablePrefix string = "__tmp_"

	walDir  string = "wal"
	WalPath string = "wal/log"
)

// Manages read and write operations for pages to disk blocks
type FileManager struct {
	dir         string
	pageSize    storage.PageSize
	isNew       bool
	openedFiles map[string]*os.File
	sync.Mutex
}

func NewFileManager(dir string, pageSize storage.PageSize) *FileManager {
	_, err := os.Stat(dir)

	isNew := os.IsNotExist(err)
	if isNew {
		if err = os.MkdirAll(dir, os.ModeSticky|os.ModePerm); err != nil {
			log.Fatalf("failed to create new database directory: %v", err)
		}

		walP := path.Join(dir, walDir)
		if err = os.MkdirAll(walP, os.ModeSticky|os.ModePerm); err != nil {
			log.Fatalf("failed to create directory for wal: %v", err)
		}
	}

	if !isNew && err != nil {
		log.Fatalf("failed to open database directory: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("failed to read database directory entries: %v", err)
	}

	for _, v := range entries {
		if strings.HasPrefix(v.Name(), tmpTablePrefix) {
			tmpFile := path.Join(dir, v.Name())
			if err = os.Remove(tmpFile); err != nil {
				log.Fatalf("failed to remove temp file: %v", tmpFile)
			}
		}
	}

	return &FileManager{
		dir:         dir,
		pageSize:    pageSize,
		isNew:       isNew,
		openedFiles: make(map[string]*os.File),
	}
}

func (m *FileManager) openFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0o755)
}

func (m *FileManager) getFile(filename string) *os.File {
	f, exists := m.openedFiles[filename]

	if !exists {
		p := path.Join(m.dir, filename)
		f, err := m.openFile(p)
		if err != nil {
			log.Fatalf("failed to open new file: %v", err)
		}

		m.openedFiles[filename] = f

		return f
	}

	return f
}

// Read the contents of a block to a page
func (m *FileManager) Read(b storage.Block, p *storage.Page) {
	m.Lock()
	defer m.Unlock()

	f := m.getFile(b.Filename())

	if _, err := f.ReadAt(p.Contents(), int64(b.BlockNum())*int64(m.pageSize)); err != io.EOF && err != nil {
		log.Fatalf("failed to read contents of %s to page: %v", b.String(), err)
	}
}

// Writes the contents of a page to a block
func (m *FileManager) Write(b storage.Block, p *storage.Page) {
	m.Lock()
	defer m.Unlock()

	f := m.getFile(b.Filename())
	if _, err := f.WriteAt(p.Contents(), int64(b.BlockNum())*int64(m.pageSize)); err != nil {
		log.Fatalf("failed to write contents of a page to %s: %v", b.String(), err)
	}
}

// Returns the size of a file in number of blocks
func (m *FileManager) FileSizeInBlocks(filename string) uint64 {
	f := m.getFile(filename)

	fInfo, err := f.Stat()
	if err != nil {
		log.Fatalf("failed to get file stat for file %q: %v", filename, err)
	}

	return uint64(fInfo.Size()) / uint64(m.pageSize)
}

func (m *FileManager) IsNew() bool {
	return m.isNew
}

func (m *FileManager) PageSize() storage.PageSize {
	return m.pageSize
}
