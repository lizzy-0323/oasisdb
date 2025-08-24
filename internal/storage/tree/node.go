package tree

import (
	"bytes"
	"errors"
	"oasisdb/internal/config"
	"oasisdb/internal/storage/sstable"
	"os"
	"path"
)

type NodeOption func(*Node)

// Node in LSM Tree equals a sstable
type Node struct {
	conf          *config.Config
	file          string            // file name of sstable
	level         int               // level of sstable
	seq           int32             // seq of sstable
	size          uint64            // size of sstable
	startKey      []byte            // start key of sstable
	endKey        []byte            // end key of sstable
	blockToFilter map[uint64][]byte // block offset to filter
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
	if n.file == "" {
		n.file = "tmp.sst"
	}
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

	return n.Check()
}

// Check if configs are valid
func (n *Node) Check() error {
	if n.file == "" {
		return errors.New("file is empty")
	}
	if n.level < 0 {
		return errors.New("level is less than 0")
	}
	if n.seq < 0 {
		return errors.New("seq is less than 0")
	}
	if n.startKey == nil {
		return errors.New("startKey is nil")
	}
	if n.endKey == nil {
		return errors.New("endKey is nil")
	}
	if n.blockToFilter == nil {
		return errors.New("blockToFilter is nil")
	}
	if n.sstReader == nil {
		return errors.New("sstReader is nil")
	}
	if n.indexEntries == nil {
		return errors.New("indexEntries is nil")
	}
	return nil
}

func WithFile(file string) NodeOption {
	return func(n *Node) {
		n.file = file
	}
}

func WithLevel(level int) NodeOption {
	return func(n *Node) {
		n.level = level
	}
}

func WithSeq(seq int32) NodeOption {
	return func(n *Node) {
		n.seq = seq
	}
}

func WithSize(size uint64) NodeOption {
	return func(n *Node) {
		n.size = size
	}
}

func WithStartKey(startKey []byte) NodeOption {
	return func(n *Node) {
		n.startKey = startKey
	}
}

func WithEndKey(endKey []byte) NodeOption {
	return func(n *Node) {
		n.endKey = endKey
	}
}

func WithBlockToFilter(blockToFilter map[uint64][]byte) NodeOption {
	return func(n *Node) {
		n.blockToFilter = blockToFilter
	}
}

func WithSSTableReader(sstReader *sstable.SSTableReader) NodeOption {
	return func(n *Node) {
		n.sstReader = sstReader
	}
}

func WithIndexEntries(indexEntries []*sstable.IndexEntry) NodeOption {
	return func(n *Node) {
		n.indexEntries = indexEntries
	}
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

func (n *Node) Destroy() {
	n.sstReader.Close()
	_ = os.Remove(path.Join(n.conf.Dir, n.file))
}

func (n *Node) Close() {
	n.sstReader.Close()
}

// mayContain using bloom filter to judge whether the key exists
func (n *Node) mayContain(indexEntry *sstable.IndexEntry, key []byte) bool {
	bitmap := n.blockToFilter[indexEntry.PrevOffset]
	return n.conf.Filter.MayContain(bitmap, key)
}

func (n *Node) Get(key []byte) ([]byte, bool, error) {
	// 1. search index block by binary search
	indexEntry, ok := n.binarySearchIndex(key, 0, len(n.indexEntries)-1)
	if !ok {
		return nil, false, nil
	}

	// 2. using bloom filter to judge whether the key exists
	if !n.mayContain(indexEntry, key) {
		return nil, false, nil
	}

	// 3. fetch data block from disk
	dataBlock, err := n.sstReader.ReadBlock(indexEntry.PrevOffset, indexEntry.PrevSize)
	if err != nil {
		return nil, false, err
	}

	// 4. parse data block
	data, err := n.sstReader.ParseDataBlock(dataBlock)
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

func (n *Node) GetAll() ([]*sstable.KV, error) {
	return n.sstReader.ReadData()
}

func (n *Node) binarySearchIndex(key []byte, start, end int) (*sstable.IndexEntry, bool) {
	if start == end {
		return n.indexEntries[start], bytes.Compare(n.indexEntries[start].Key, key) >= 0
	}
	mid := start + (end-start)>>1
	if bytes.Compare(n.indexEntries[mid].Key, key) < 0 {
		return n.binarySearchIndex(key, mid+1, end)
	}
	return n.binarySearchIndex(key, start, mid)
}
