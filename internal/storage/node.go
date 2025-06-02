package storage

import (
	"bytes"
	"oasisdb/internal/config"
	"oasisdb/internal/storage/sstable"
)

type NodeOption func(*Node)

// Node in LSM Tree equals a sstable
type Node struct {
	conf          *config.Config
	file          string
	level         int
	seq           int32
	size          uint64
	startKey      []byte
	endKey        []byte
	blockToFilter map[uint64][]byte
	sstReader     *sstable.SSTableReader
	indexEntries  []*sstable.IndexEntry
}

func NewNode(conf *config.Config, opts ...NodeOption) *Node {
	n := &Node{
		conf: conf,
	}
	for _, opt := range opts {
		opt(n)
	}

	if err := Repair(n); err != nil {
		panic(err)
	}
	return n
}

func Repair(n *Node) error {
	if n.sstReader == nil {
		reader, err := sstable.NewSSTableReader(n.file, n.conf)
		if err != nil {
			return err
		}
		n.sstReader = reader
	}

	if n.indexEntries == nil {
		// Read index entries
		indexEntries, err := n.sstReader.ReadIndex()
		if err != nil {
			return err
		}
		n.indexEntries = indexEntries
	}

	// Set start and end keys from index entries
	if n.startKey == nil || n.endKey == nil {
		n.startKey = n.indexEntries[0].Key
		n.endKey = n.indexEntries[len(n.indexEntries)-1].Key
	}

	// Read filters
	if n.blockToFilter == nil {
		filters, err := n.sstReader.ReadFilter()
		if err != nil {
			return err
		}
		n.blockToFilter = filters
	}

	return nil
}

func (n *Node) WithFile(file string) *Node {
	n.file = file
	return n
}

func (n *Node) WithLevel(level int) *Node {
	n.level = level
	return n
}

func (n *Node) WithSeq(seq int32) *Node {
	n.seq = seq
	return n
}

func (n *Node) WithSize(size uint64) *Node {
	n.size = size
	return n
}

func (n *Node) WithStartKey(startKey []byte) *Node {
	n.startKey = startKey
	return n
}
func (n *Node) WithEndKey(endKey []byte) *Node {
	n.endKey = endKey
	return n
}

func (n *Node) WithBlockToFilter(blockToFilter map[uint64][]byte) *Node {
	n.blockToFilter = blockToFilter
	return n
}

func (n *Node) WithSSTableReader(sstReader *sstable.SSTableReader) *Node {
	n.sstReader = sstReader
	return n
}

func (n *Node) WithIndexEntries(indexEntries []*sstable.IndexEntry) *Node {
	n.indexEntries = indexEntries
	return n
}
func (n *Node) Size() uint64 {
	return n.size
}

func (n *Node) Start() []byte {
	return n.startKey
}

func (n *Node) End() []byte {
	return n.endKey
}

func (n *Node) Index() (level int, seq int32) {
	level, seq = n.level, n.seq
	return
}

// mayContain using bloom filter to judge whether the key exists
func (n *Node) mayContain(key []byte) bool {
	bitmap := n.blockToFilter[n.indexEntries[0].PrevOffset]
	return n.conf.Filter.MayContain(bitmap, key)
}

func (n *Node) Get(key []byte) ([]byte, bool, error) {
	// 1. search index block by binary search
	indexEntry, ok := n.searchIndex(key, 0, len(n.indexEntries)-1)
	if !ok {
		return nil, false, nil
	}
	// 2. using bloom filter to judge whether the key exists
	if !n.mayContain(key) {
		return nil, false, nil
	}
	// 3. fetch data block from disk
	dataBlock, err := n.sstReader.ReadBlock(indexEntry.PrevOffset, indexEntry.PrevSize)
	if err != nil {
		return nil, false, err
	}
	// 4. parse data block
	data, err := n.sstReader.ParseBlockData(dataBlock)
	if err != nil {
		return nil, false, err
	}
	// 5. find the key
	for _, kv := range data {
		if bytes.Equal(kv.Key, key) {
			return kv.Value, true, nil
		}
	}
	return nil, false, nil
}

func (n *Node) searchIndex(key []byte, start, end int) (*sstable.IndexEntry, bool) {
	if start == end {
		return n.indexEntries[start], bytes.Compare(n.indexEntries[start].Key, key) >= 0
	}
	mid := start + (end-start)>>1
	if bytes.Compare(n.indexEntries[mid].Key, key) < 0 {
		return n.searchIndex(key, mid+1, end)
	}
	return n.searchIndex(key, start, mid)
}
