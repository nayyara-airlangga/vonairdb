package storage

import "fmt"

// A disk block represented by the file name and the logical block
// number
type Block struct {
	filename string
	blockNum uint
}

func NewBlock(filename string, blockNum uint) Block {
	return Block{filename, blockNum}
}

func (b Block) String() string {
	return fmt.Sprintf("file %q block %d", b.filename, b.blockNum)
}

func (b Block) Filename() string {
	return b.filename
}

func (b Block) BlockNum() uint {
	return b.blockNum
}

func (b Block) Id() string {
	return fmt.Sprintf("f:%s:%d", b.filename, b.blockNum)
}
