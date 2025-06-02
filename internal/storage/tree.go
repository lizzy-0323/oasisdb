package storage

import (
	"oasisdb/internal/config"
	"oasisdb/internal/storage/memtable"
	"oasisdb/internal/storage/wal"
	"sync"
	"sync/atomic"
)

// LSM Tree Engine
type LSMTree struct {
	conf           *config.Config
	dataLock       sync.RWMutex
	memTable       memtable.MemTable
	walWriter      *wal.WALWriter
	nodes          [][]*Node
	destDir        string
	levelLocks     []sync.RWMutex // locks used in every level
	memCompactCh   chan struct{}  // when memtable size reach the limit, trigger compaction
	levelCompactCh chan struct{}  // when sst file size of one layer reach the limit, trigger compaction
	stopCh         chan struct{}  // stop all jobs
	memTableIndex  int            // memtable index , correspond to wal files
	levelToSeq     []atomic.Int32
}

func NewLSMTree(conf *config.Config) (*LSMTree, error) {
	// 1. build LSM Tree
	t := &LSMTree{
		conf:           conf,
		memTable:       memtable.NewSkipList(),
		walWriter:      wal.NewWALWriter(),
		stopCh:         make(chan struct{}),
		memTableIndex:  0,
		levelToSeq:     make([]atomic.Int32, conf.MaxLevel),
		nodes:          make([][]*Node, conf.MaxLevel),
		levelLocks:     make([]sync.RWMutex, conf.MaxLevel),
		memCompactCh:   make(chan struct{}, 1),
		levelCompactCh: make(chan struct{}, 1),
	}
	// 2. Read sst file, construct nodes
	if err := t.constructTree(); err != nil {
		return nil, err
	}
	// 3. Start lsm compaction
	go t.compact()

	// 4. read wal files to restore memtables
	if err := t.Restore(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *LSMTree) Put(key, value []byte) error {
	return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (t *LSMTree) constructTree() error {
	return nil
}

func (t *LSMTree) refreshMemTableLocked() error {
	return nil
}

func (t *LSMTree) compact() error {
	return nil
}

func (t *LSMTree) Restore() error {
	return nil
}
