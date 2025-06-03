package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"oasisdb/internal/config"
	"os"
	"path"
)

var (
	ErrInvalidFile = errors.New("invalid sstable file")
	ErrKeyNotFound = errors.New("key not found")
)

type KV struct {
	Key   []byte
	Value []byte
}
type SSTableReader struct {
	conf         *config.Config
	src          *os.File
	reader       *bufio.Reader
	filterOffset uint64 // bloom filter offset
	filterSize   uint64 // bloom filter size
	indexOffset  uint64 // index block offset
	indexSize    uint64 // index block size
}

func NewSSTableReader(file string, conf *config.Config) (*SSTableReader, error) {
	src, err := os.OpenFile(path.Join(conf.Dir, file), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	// read footer
	stat, err := src.Stat()
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	if size < int64(conf.SSTFooterSize) {
		return nil, ErrInvalidFile
	}

	footer := make([]byte, conf.SSTFooterSize)
	if _, err := src.ReadAt(footer, size-int64(conf.SSTFooterSize)); err != nil {
		return nil, err
	}
	// Create reader with correct footer offsets
	ss := &SSTableReader{
		conf:   conf,
		src:    src,
		reader: bufio.NewReader(src),
	}

	// Read footer values in same order as writer
	ss.filterOffset = binary.LittleEndian.Uint64(footer[0:8])
	ss.filterSize = binary.LittleEndian.Uint64(footer[8:16])
	ss.indexOffset = binary.LittleEndian.Uint64(footer[16:24])
	ss.indexSize = binary.LittleEndian.Uint64(footer[24:32])

	return ss, nil
}

func (s *SSTableReader) ReadBlock(offset, size uint64) ([]byte, error) {
	if _, err := s.src.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	s.reader.Reset(s.src)

	buf := make([]byte, size)
	if _, err := io.ReadFull(s.src, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadIndex read index block to memory
func (s *SSTableReader) ReadIndex() ([]*IndexEntry, error) {
	// Reader footer first
	if s.indexOffset == 0 || s.indexSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	indexBlock, err := s.ReadBlock(s.indexOffset, s.indexSize)
	// fmt.Println("indexBlock: ", indexBlock)
	if err != nil {
		return nil, err
	}
	// parse index entries
	var indexEntries []*IndexEntry
	var pos uint64
	for pos < uint64(len(indexBlock)) {
		// Read key length (uint16)
		keyLen := binary.LittleEndian.Uint16(indexBlock[pos:])
		pos += 2
		// fmt.Println("keyLen: ", keyLen)

		// Read value length (uint32)
		valueLen := binary.LittleEndian.Uint32(indexBlock[pos:])
		pos += 4
		// fmt.Println("valueLen: ", valueLen)

		// Read key
		key := make([]byte, keyLen)
		copy(key, indexBlock[pos:pos+uint64(keyLen)])
		pos += uint64(keyLen)

		// Read value
		value := indexBlock[pos : pos+uint64(valueLen)]
		pos += uint64(valueLen)

		// Parse offset and size from value using varint
		offset, n := binary.Uvarint(value[0:])
		// fmt.Println("offset: ", offset)
		if n <= 0 {
			return nil, fmt.Errorf("failed to read offset from value")
		}
		size, m := binary.Uvarint(value[n:])
		// fmt.Println("size: ", size)
		if m <= 0 {
			return nil, fmt.Errorf("failed to read size from value")
		}

		indexEntries = append(indexEntries, &IndexEntry{
			Key:        key,
			PrevOffset: offset,
			PrevSize:   size,
		})
	}

	return indexEntries, nil
}

func (s *SSTableReader) ReadFilter() (map[uint64][]byte, error) {
	// Reader footer first
	if s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}
	filterBlock, err := s.ReadBlock(s.filterOffset, s.filterSize)
	if err != nil {
		return nil, err
	}

	blockToFilter := make(map[uint64][]byte)
	for i := uint64(0); i < uint64(len(filterBlock)); {
		keyLen := binary.LittleEndian.Uint16(filterBlock[i:])
		i += 2

		valueLen := binary.LittleEndian.Uint32(filterBlock[i:])
		i += 4

		key := filterBlock[i : i+uint64(keyLen)]
		offset, n := binary.Uvarint(key)
		if n <= 0 {
			return nil, fmt.Errorf("failed to read offset from key")
		}
		i += uint64(keyLen)

		blockToFilter[offset] = filterBlock[i : i+uint64(valueLen)]
		i += uint64(valueLen)
	}

	return blockToFilter, nil
}

func (s *SSTableReader) Close() error {
	s.reader.Reset(s.src)
	return s.src.Close()
}

func (s *SSTableReader) Size() (uint64, error) {
	stat, err := s.src.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (s *SSTableReader) ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {
	if buf.Len() < 6 { // 2 bytes for keyLen + 4 bytes for valueLen
		return nil, nil, io.EOF
	}

	// Read key length (2 bytes)
	keyLenBytes := buf.Next(2)
	keyLen := binary.LittleEndian.Uint16(keyLenBytes)

	// Read value length (4 bytes)
	valueLenBytes := buf.Next(4)
	valueLen := binary.LittleEndian.Uint32(valueLenBytes)

	if buf.Len() < int(keyLen)+int(valueLen) {
		return nil, nil, io.EOF
	}

	// Read key and value
	key := buf.Next(int(keyLen))
	value := buf.Next(int(valueLen))

	return key, value, nil
}

func (s *SSTableReader) ParseDataBlock(block []byte) ([]*KV, error) {
	var data []*KV
	var prevKey []byte
	buf := bytes.NewBuffer(block)
	for {
		key, value, err := s.ReadRecord(prevKey, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		data = append(data, &KV{
			Key:   key,
			Value: value,
		})
		prevKey = key
	}
	return data, nil
}

func (s *SSTableReader) ReadData() ([]*KV, error) {
	if s.indexOffset == 0 || s.indexSize == 0 || s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	// fetch data block from disk
	dataBlock, err := s.ReadBlock(0, s.filterOffset)
	if err != nil {
		return nil, err
	}

	// parse all data block content
	return s.ParseDataBlock(dataBlock)
}

func (s *SSTableReader) ReadFooter() error {
	// find the footer position
	if _, err := s.src.Seek(-int64(s.conf.SSTFooterSize), io.SeekEnd); err != nil {
		return err
	}

	// read footer
	footer := make([]byte, s.conf.SSTFooterSize)
	if _, err := s.src.Read(footer); err != nil {
		return err
	}

	// Read footer values in same order as writer
	s.filterOffset = binary.LittleEndian.Uint64(footer[0:8])
	s.filterSize = binary.LittleEndian.Uint64(footer[8:16])
	s.indexOffset = binary.LittleEndian.Uint64(footer[16:24])
	s.indexSize = binary.LittleEndian.Uint64(footer[24:32])
	return nil
}
