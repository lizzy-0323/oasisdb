package sstable

import (
	"bufio"
	"encoding/binary"
	"os"
	"sort"
)

// NewBuilder 创建一个新的 SSTable 构建器
func NewBuilder(file string) (*Builder, error) {
	f, err := os.Create(file)
	if err != nil {
		return nil, err
	}

	return &Builder{
		file:        f,
		writer:      bufio.NewWriter(f),
		indexBlock:  make([]IndexEntry, 0),
		blockOffset: 0,
	}, nil
}

// Add 添加一个键值对到 SSTable
func (b *Builder) Add(key, value []byte) error {
	// 写入 key size (2 bytes)
	if err := binary.Write(b.writer, binary.LittleEndian, uint16(len(key))); err != nil {
		return err
	}

	// 写入 value size (4 bytes)
	if err := binary.Write(b.writer, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	// 写入 key
	if _, err := b.writer.Write(key); err != nil {
		return err
	}

	// 写入 value
	if _, err := b.writer.Write(value); err != nil {
		return err
	}

	// 更新索引
	b.indexBlock = append(b.indexBlock, IndexEntry{
		MaxKey: key,
		Offset: b.blockOffset,
		Size:   uint64(6 + len(key) + len(value)), // 6 = key size(2) + value size(4)
	})

	// 更新偏移量
	b.blockOffset += uint64(6 + len(key) + len(value))

	return nil
}

// Finish 完成 SSTable 的构建
func (b *Builder) Finish() error {
	// 1. 排序索引条目
	sort.Slice(b.indexBlock, func(i, j int) bool {
		return string(b.indexBlock[i].MaxKey) < string(b.indexBlock[j].MaxKey)
	})

	// 2. 写入布隆过滤器（TODO）
	filterOffset := b.blockOffset
	filterSize := uint64(0) // 暂时为空

	// 3. 写入索引块
	indexOffset := b.blockOffset + filterSize
	var indexSize uint64

	for _, entry := range b.indexBlock {
		// 写入 key size
		if err := binary.Write(b.writer, binary.LittleEndian, uint16(len(entry.MaxKey))); err != nil {
			return err
		}
		indexSize += 2

		// 写入 key
		if _, err := b.writer.Write(entry.MaxKey); err != nil {
			return err
		}
		indexSize += uint64(len(entry.MaxKey))

		// 写入 offset
		if err := binary.Write(b.writer, binary.LittleEndian, entry.Offset); err != nil {
			return err
		}
		indexSize += 8

		// 写入 size
		if err := binary.Write(b.writer, binary.LittleEndian, entry.Size); err != nil {
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

	if _, err := b.writer.Write(footer); err != nil {
		return err
	}

	// 5. 刷新缓冲区并关闭文件
	if err := b.writer.Flush(); err != nil {
		return err
	}

	return b.file.Close()
}

// Close 关闭构建器
func (b *Builder) Close() error {
	return b.file.Close()
}
