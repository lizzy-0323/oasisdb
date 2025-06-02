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

type SSTableWriter struct {
	conf          *config.Config
	dest          *os.File // ssTable file
	dataBuf       *bytes.Buffer
	filterBuf     *bytes.Buffer
	indexBuf      *bytes.Buffer
	blockToFilter map[uint64][]byte
	indexEntries  []*IndexEntry

	dataBlock   *Block
	filterBlock *Block
	indexBlock  *Block
	writer      *bufio.Writer

	blockOffset uint64
}

func NewSSTableWriter(file string, conf *config.Config) (*SSTableWriter, error) {
	f, err := os.OpenFile(path.Join(conf.Dir, file), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTableWriter{
		conf:          conf,
		dest:          f,
		writer:        bufio.NewWriter(f),
		dataBuf:       bytes.NewBuffer(nil),
		filterBuf:     bytes.NewBuffer(nil),
		indexBuf:      bytes.NewBuffer(nil),
		indexEntries:  make([]*IndexEntry, 0),
		blockToFilter: make(map[uint64][]byte),
		dataBlock:     NewBlock(),
		filterBlock:   NewBlock(),
		indexBlock:    NewBlock(),
		blockOffset:   0,
	}, nil
}

// Append a key-value pair to the block
func (s *SSTableWriter) Append(key, value []byte) error {
	// write key size (2 bytes)
	if err := binary.Write(s.writer, binary.LittleEndian, uint16(len(key))); err != nil {
		return err
	}

	// 写入 value size (4 bytes)
	if err := binary.Write(s.writer, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	// 写入 key
	if _, err := s.writer.Write(key); err != nil {
		return err
	}

	// 写入 value
	if _, err := s.writer.Write(value); err != nil {
		return err
	}

	// 更新索引
	s.indexEntries = append(s.indexEntries, &IndexEntry{
		MaxKey: key,
		Offset: s.blockOffset,
		Size:   uint64(6 + len(key) + len(value)), // 6 = key size(2) + value size(4)
	})

	// 更新偏移量
	s.blockOffset += uint64(6 + len(key) + len(value))

	return nil
}

// Finish all the process, and write all data to disk
func (s *SSTableWriter) Finish() error {
	// 1. 排序索引条目
	sort.Slice(s.indexEntries, func(i, j int) bool {
		return string(s.indexEntries[i].MaxKey) < string(s.indexEntries[j].MaxKey)
	})

	// 2. TODO: 写入布隆过滤器
	filterOffset := s.blockOffset
	filterSize := uint64(0) // 暂时为空

	// 3. 写入索引块
	indexOffset := s.blockOffset + filterSize
	var indexSize uint64

	for _, entry := range s.indexEntries {
		// 写入 key size
		if err := binary.Write(s.writer, binary.LittleEndian, uint16(len(entry.MaxKey))); err != nil {
			return err
		}
		indexSize += 2

		// 写入 key
		if _, err := s.writer.Write(entry.MaxKey); err != nil {
			return err
		}
		indexSize += uint64(len(entry.MaxKey))

		// 写入 offset
		if err := binary.Write(s.writer, binary.LittleEndian, entry.Offset); err != nil {
			return err
		}
		indexSize += 8

		// 写入 size
		if err := binary.Write(s.writer, binary.LittleEndian, entry.Size); err != nil {
			return err
		}
		indexSize += 8
	}

	// 4. 写入 footer
	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], filterOffset)
	binary.LittleEndian.PutUint64(footer[8:16], filterSize)
	binary.LittleEndian.PutUint64(footer[16:24], indexOffset)
	binary.LittleEndian.PutUint64(footer[24:32], indexSize)
	binary.LittleEndian.PutUint32(footer[36:40], magicNumber)

	if _, err := s.writer.Write(footer); err != nil {
		return err
	}

	// 5. 刷新缓冲区并关闭文件
	if err := s.writer.Flush(); err != nil {
		return err
	}

	return s.dest.Close()
}

func (s *SSTableWriter) refreshBlock() {

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
