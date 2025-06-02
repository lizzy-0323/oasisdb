package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"oasisdb/internal/config"
	"os"
	"path"
	"sort"
)

// IndexEntry 表示索引块中的一个条目
type IndexEntry struct {
	MaxKey []byte
	Offset uint64
	Size   uint64
}
type SSTableWriter struct {
	conf          *config.Config    // config
	dest          *os.File          // ssTable file
	dataBuf       *bytes.Buffer     // data block buffer
	filterBuf     *bytes.Buffer     // filter block buffer
	indexBuf      *bytes.Buffer     // index block buffer
	blockToFilter map[uint64][]byte // block offset to filter
	assistBuf     [20]byte          // assist buffer using in index block
	indexEntries  []*IndexEntry

	dataBlock   *Block
	filterBlock *Block
	indexBlock  *Block
	writer      *bufio.Writer

	prevBlockOffset uint64
	prevBlockSize   uint64
}

func NewSSTableWriter(file string, conf *config.Config) (*SSTableWriter, error) {
	dest, err := os.OpenFile(path.Join(conf.Dir, file), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTableWriter{
		conf:            conf,
		dest:            dest,
		writer:          bufio.NewWriter(dest),
		dataBuf:         bytes.NewBuffer(nil),
		filterBuf:       bytes.NewBuffer(nil),
		indexBuf:        bytes.NewBuffer(nil),
		indexEntries:    make([]*IndexEntry, 0),
		blockToFilter:   make(map[uint64][]byte),
		dataBlock:       NewBlock(),
		filterBlock:     NewBlock(),
		indexBlock:      NewBlock(),
		prevBlockOffset: 0,
		prevBlockSize:   0,
	}, nil
}

// Append a key-value pair to the block
func (s *SSTableWriter) Append(key, value []byte) error {
	// If no data block, insert index first
	if s.dataBlock.entriesCnt == 0 {
		s.insertIndex(key)
	}

	// append to data block
	if err := s.dataBlock.Append(key, value); err != nil {
		return err
	}
	// add key to bloom filter
	s.conf.Filter.Add(key)

	// if dataBlock size is greater than SSTDataBlockSize, refresh block
	if s.dataBlock.Size() >= s.conf.SSTDataBlockSize {
		s.refreshBlock()
	}

	return nil
}

func (s *SSTableWriter) insertIndex(key []byte) {
	n := binary.PutUvarint(s.assistBuf[0:], s.prevBlockOffset)
	n += binary.PutUvarint(s.assistBuf[n:], s.prevBlockSize)
	// key: indexKey value: offset and size
	s.indexBlock.Append(key, s.assistBuf[:n])
	s.indexEntries = append(s.indexEntries, &IndexEntry{
		MaxKey: key,
		Offset: s.prevBlockOffset,
		Size:   s.prevBlockSize,
	})
}

// Finish all the process, and write all data to disk
func (s *SSTableWriter) Finish() error {
	// 1. 排序索引条目
	sort.Slice(s.indexEntries, func(i, j int) bool {
		return string(s.indexEntries[i].MaxKey) < string(s.indexEntries[j].MaxKey)
	})

	// 2. 写入布隆过滤器
	_, _ = s.filterBlock.FlushTo(s.filterBuf)

	// 3. 写入索引块
	_, _ = s.indexBlock.FlushTo(s.indexBuf)

	// 4. 写入 footer
	// TODO: implement footer

	// 5. write all data to disk
	_, _ = s.writer.Write(s.dataBuf.Bytes())
	_, _ = s.writer.Write(s.filterBuf.Bytes())
	_, _ = s.writer.Write(s.indexBuf.Bytes())

	return s.dest.Close()
}

// If block size is greater than SSTDataBlockSize, refresh block
func (s *SSTableWriter) refreshBlock() {
	if s.conf.Filter.KeyLen() == 0 {
		return
	}
	s.prevBlockOffset += uint64(s.dataBuf.Len())

	// TODO: Add bloom filter

	// reset bloom filter
	s.conf.Filter.Reset()

	// flush data block
	s.prevBlockSize, _ = s.dataBlock.FlushTo(s.dataBuf)
}

func (s *SSTableWriter) Size() uint64 {
	return uint64(s.dataBuf.Len())
}

func (s *SSTableWriter) Close() error {
	s.dataBuf.Reset()
	s.filterBuf.Reset()
	s.indexBuf.Reset()
	return s.dest.Close()
}
