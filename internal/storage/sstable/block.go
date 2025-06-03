package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Block, basic unit of sstable, in sstable, it can be index, data or filter
type Block struct {
	record     *bytes.Buffer
	entriesCnt int
}

func NewBlock() *Block {
	return &Block{
		record: bytes.NewBuffer([]byte{}),
	}
}

func (b *Block) Append(key, value []byte) error {
	defer func() {
		b.entriesCnt++
	}()
	if err := binary.Write(b.record, binary.LittleEndian, uint16(len(key))); err != nil {
		return err
	}
	if err := binary.Write(b.record, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := b.record.Write(key); err != nil {
		return err
	}
	if _, err := b.record.Write(value); err != nil {
		return err
	}
	return nil
}

func (b *Block) Size() uint64 {
	return uint64(b.record.Len())
}

func (b *Block) FlushTo(dest io.Writer) (uint64, error) {
	defer b.clear()
	n, err := dest.Write(b.record.Bytes())
	return uint64(n), err
}

func (b *Block) clear() {
	b.entriesCnt = 0
	b.record.Reset()
}
