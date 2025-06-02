package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"oasisdb/internal/config"
	"os"
	"path"
)

const (
	blockSize      = 4 * 1024 // 4KB
	footerSize     = 40       // 固定大小的 footer
	magicNumber    = 0xDB0023DB
	indexEntrySize = 24 // key length(2) + key + offset(8) + size(8)
)

var (
	ErrInvalidFile = errors.New("invalid sstable file")
	ErrKeyNotFound = errors.New("key not found")
)

// IndexEntry 表示索引块中的一个条目
type IndexEntry struct {
	MaxKey []byte
	Offset uint64
	Size   uint64
}

type SSTableReader struct {
	src          *os.File
	reader       *bufio.Reader
	indexEntries []IndexEntry // 缓存的索引条目
	filterOffset uint64       // 布隆过滤器的偏移量
	filterSize   uint64       // 布隆过滤器的大小
	indexOffset  uint64       // 索引块的偏移量
	indexSize    uint64       // 索引块的大小
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
	if size < int64(footerSize) {
		return nil, ErrInvalidFile
	}

	footer := make([]byte, footerSize)
	if _, err := src.ReadAt(footer, size-int64(footerSize)); err != nil {
		return nil, err
	}
	magic := binary.LittleEndian.Uint32(footer[36:])
	if magic != magicNumber {
		return nil, ErrInvalidFile
	}

	ss := &SSTableReader{
		src:          src,
		reader:       bufio.NewReader(src),
		filterOffset: binary.LittleEndian.Uint64(footer[0:8]),
		filterSize:   binary.LittleEndian.Uint64(footer[8:16]),
		indexOffset:  binary.LittleEndian.Uint64(footer[16:24]),
		indexSize:    binary.LittleEndian.Uint64(footer[24:32]),
	}

	if err := ss.ReadIndex(); err != nil {
		return nil, err
	}

	return ss, nil
}

func (s *SSTableReader) ReadBlock(offset, size uint64) ([]byte, error) {
	if _, err := s.src.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(s.src, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadIndex read index block to memory
func (s *SSTableReader) ReadIndex() error {
	// Reader footer first
	if s.indexOffset == 0 || s.indexSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return err
		}
	}

	indexBlock, err := s.ReadBlock(s.indexOffset, s.indexSize)
	if err != nil {
		return err
	}

	// parse index entries
	s.indexEntries = make([]IndexEntry, 0)
	for i := uint64(0); i < s.indexSize; {
		// 读取 key 长度
		keySize := binary.LittleEndian.Uint16(indexBlock[i:])
		i += 2

		// 读取 key
		key := make([]byte, keySize)
		copy(key, indexBlock[i:i+uint64(keySize)])
		i += uint64(keySize)

		// 读取 offset 和 size
		offset := binary.LittleEndian.Uint64(indexBlock[i:])
		i += 8
		size := binary.LittleEndian.Uint64(indexBlock[i:])
		i += 8

		s.indexEntries = append(s.indexEntries, IndexEntry{
			MaxKey: key,
			Offset: offset,
			Size:   size,
		})
	}

	return nil
}

func (s *SSTableReader) ReadFilter() (map[uint64][]byte, error) {
	if s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}
	blockToFilter := make(map[uint64][]byte)

	// implement
	return blockToFilter, nil
}

// findBlock using binary search to find Blocks
func (s *SSTableReader) findBlock(key []byte) (offset, size uint64) {
	left, right := 0, len(s.indexEntries)-1

	for left <= right {
		mid := (left + right) / 2
		entry := s.indexEntries[mid]

		if bytes.Compare(key, entry.MaxKey) <= 0 {
			// key <= entry.MaxKey，可能在这个块或之前的块中
			right = mid - 1
			offset, size = entry.Offset, entry.Size
		} else {
			// key > entry.MaxKey，一定在后面的块中
			left = mid + 1
		}
	}

	return offset, size
}

// searchBlock 在数据块中搜索 key
func (s *SSTableReader) searchBlock(key []byte, offset, size uint64) ([]byte, error) {
	// 读取数据块
	block := make([]byte, size)
	if _, err := s.src.ReadAt(block, int64(offset)); err != nil {
		return nil, err
	}

	// 遍历块中的条目
	var pos uint64
	for pos < size {
		// 读取 key size 和 value size
		keySize := binary.LittleEndian.Uint16(block[pos:])
		pos += 2
		valueSize := binary.LittleEndian.Uint32(block[pos:])
		pos += 4

		// 读取 key
		entryKey := block[pos : pos+uint64(keySize)]
		pos += uint64(keySize)

		// 比较 key
		cmp := bytes.Compare(key, entryKey)
		if cmp == 0 {
			// 找到了 key，返回对应的 value
			value := make([]byte, valueSize)
			copy(value, block[pos:pos+uint64(valueSize)])
			return value, nil
		} else if cmp < 0 {
			// key 比当前条目的 key 小，由于条目是排序的，所以 key 不在这个块中
			break
		}

		// 移动到下一个条目
		pos += uint64(valueSize)
	}

	return nil, ErrKeyNotFound
}

// mayContain 检查 key 是否可能存在（布隆过滤器）
func (s *SSTableReader) mayContain(key []byte) bool {
	// TODO: 实现布隆过滤器
	return true // 暂时总是返回 true
}

func (s *SSTableReader) Close() error {
	s.reader.Reset(s.src)
	return s.src.Close()
}

func (s *SSTableReader) Size() (uint64, error) {
	if s.indexOffset == 0 {
		if err := s.ReadFooter(); err != nil {
			return 0, err
		}
	}
	return s.indexOffset + s.indexSize, nil
}

func (s *SSTableReader) ReadFooter() error {
	// find the footer position
	if _, err := s.src.Seek(-footerSize, io.SeekEnd); err != nil {
		return err
	}

	// read footer
	footer := make([]byte, footerSize)
	if _, err := s.src.Read(footer); err != nil {
		return err
	}

	// parse footer
	s.filterOffset = binary.LittleEndian.Uint64(footer[0:8])
	s.filterSize = binary.LittleEndian.Uint64(footer[8:16])
	s.indexOffset = binary.LittleEndian.Uint64(footer[16:24])
	s.indexSize = binary.LittleEndian.Uint64(footer[24:32])
	return nil
}

// Get 根据 key 获取对应的值
func (s *SSTableReader) Get(key []byte) ([]byte, error) {
	// 1. 使用布隆过滤器快速判断 key 是否可能存在
	if !s.mayContain(key) {
		return nil, ErrKeyNotFound
	}

	// 2. 二分查找定位数据块
	blockOffset, blockSize := s.findBlock(key)
	if blockOffset == 0 {
		return nil, ErrKeyNotFound
	}

	// 3. 读取并搜索数据块
	return s.searchBlock(key, blockOffset, blockSize)
}
