package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"oasisdb/internal/config"
	"oasisdb/pkg/logger"
	"oasisdb/pkg/utils"
	"os"
	"path"
)

type IndexEntry struct {
	Key        []byte
	PrevOffset uint64
	PrevSize   uint64
}

type SSTableWriter struct {
	conf          *config.Config    // config
	dest          *os.File          // ssTable file
	dataBuf       *bytes.Buffer     // data block buffer
	filterBuf     *bytes.Buffer     // filter block buffer
	indexBuf      *bytes.Buffer     // index block buffer
	blockToFilter map[uint64][]byte // block offset to filter
	assistBuf     [20]byte          // assist buffer
	indexEntries  []*IndexEntry

	dataBlock   *Block
	filterBlock *Block
	indexBlock  *Block
	writer      *bufio.Writer

	prevKey         []byte
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
		prevKey:         []byte{},
		prevBlockOffset: 0,
		prevBlockSize:   0,
	}, nil
}

// Append a key-value pair to the block
func (s *SSTableWriter) Append(key, value []byte) error {
	// If open a new data block, insert index first
	if s.dataBlock.entriesCnt == 0 {
		if err := s.writeIndex(key); err != nil {
			return err
		}
	}

	// append to data block
	if err := s.dataBlock.Append(key, value); err != nil {
		return err
	}
	// add key to bloom filter
	s.conf.Filter.Add(key)
	// update prevKey
	s.prevKey = key

	// if dataBlock size is greater than SSTDataBlockSize, refresh block
	if s.dataBlock.Size() >= s.conf.SSTDataBlockSize {
		if err := s.refreshBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (s *SSTableWriter) writeIndex(key []byte) error {
	logger.Debug("Writing index", "prev_key", string(s.prevKey), "key", string(key))
	indexKey := utils.GetSeparatorBetween(s.prevKey, key)
	// Using assistBuf to store offset and size
	n := binary.PutUvarint(s.assistBuf[0:], s.prevBlockOffset)
	n += binary.PutUvarint(s.assistBuf[n:], s.prevBlockSize)

	// { key: indexKey value: offset and size }
	if err := s.indexBlock.Append(indexKey, s.assistBuf[:n]); err != nil {
		return err
	}

	s.indexEntries = append(s.indexEntries, &IndexEntry{
		Key:        indexKey,
		PrevOffset: s.prevBlockOffset,
		PrevSize:   s.prevBlockSize,
	})

	return nil
}

// complete sstable process by sstable writer, and return meta data for lsm tree to use
func (s *SSTableWriter) Finish() (uint64, map[uint64][]byte, []*IndexEntry, error) {
	// 1. Handle the last data block if it's not empty
	if s.dataBlock.entriesCnt > 0 {
		if err := s.refreshBlock(); err != nil {
			return 0, nil, nil, err
		}
		s.writeIndex(s.prevKey)
	}

	// 2. Write bloom filter block
	filterSize, err := s.filterBlock.FlushTo(s.filterBuf)
	if err != nil {
		return 0, nil, nil, err
	}

	// 3. Write index block
	indexSize, err := s.indexBlock.FlushTo(s.indexBuf)
	if err != nil {
		return 0, nil, nil, err
	}

	// 4. Create and write footer
	footer := make([]byte, s.conf.SSTFooterSize)
	filterOffset := s.Size()
	indexOffset := filterOffset + uint64(filterSize)

	// Write offsets and sizes in footer
	binary.LittleEndian.PutUint64(footer[0:], filterOffset)
	binary.LittleEndian.PutUint64(footer[8:], uint64(filterSize))
	binary.LittleEndian.PutUint64(footer[16:], indexOffset)
	binary.LittleEndian.PutUint64(footer[24:], uint64(indexSize))

	// 5. Write all data to disk
	if _, err := s.writer.Write(s.dataBuf.Bytes()); err != nil {
		return 0, nil, nil, err
	}
	if _, err := s.writer.Write(s.filterBuf.Bytes()); err != nil {
		return 0, nil, nil, err
	}
	if _, err := s.writer.Write(s.indexBuf.Bytes()); err != nil {
		return 0, nil, nil, err
	}
	if _, err := s.writer.Write(footer); err != nil {
		return 0, nil, nil, err
	}

	// 6. Flush writer buffer and close file
	if err := s.writer.Flush(); err != nil {
		return 0, nil, nil, err
	}
	if err := s.dest.Close(); err != nil {
		return 0, nil, nil, err
	}
	return s.Size(), s.blockToFilter, s.indexEntries, nil
}

// If block size is greater than SSTDataBlockSize, refresh block
func (s *SSTableWriter) refreshBlock() error {
	if s.conf.Filter.KeyLen() == 0 {
		return nil
	}

	s.prevBlockOffset = s.Size()
	// get bitmap for bloom filter
	filterBitmap := s.conf.Filter.Hash()
	s.blockToFilter[s.prevBlockOffset] = filterBitmap
	logger.Debug("Created Bloom Filter for block", "offset", s.prevBlockOffset, "keys_count", s.conf.Filter.KeyLen(), "bitmap_size", len(filterBitmap))
	n := binary.PutUvarint(s.assistBuf[0:], s.prevBlockOffset)
	if err := s.filterBlock.Append(s.assistBuf[:n], filterBitmap); err != nil {
		return err
	}
	// reset bloom filter
	s.conf.Filter.Reset()

	// flush data block, all data blocks are contiguous
	var err error
	s.prevBlockSize, err = s.dataBlock.FlushTo(s.dataBuf)
	if err != nil {
		return err
	}

	// Reset the data block for next use
	s.dataBlock = NewBlock()
	return nil
}

func (s *SSTableWriter) Size() uint64 {
	return uint64(s.dataBuf.Len())
}

// Close flushes and closes the underlying file
func (s *SSTableWriter) Close() error {
	s.dataBuf.Reset()
	s.filterBuf.Reset()
	s.indexBuf.Reset()
	return s.dest.Close()
}
