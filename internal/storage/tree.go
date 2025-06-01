package storage

import (
	"oasisdb/internal/config"
	"oasisdb/internal/storage/memtable"
	"oasisdb/internal/storage/wal"
	"sync"
)

// LSM Tree Engine
type LSMTree struct {
	conf         *config.Config
	dataLock     sync.RWMutex
	memTable     memtable.MemTable
	walWriter    *wal.WALWriter
	nodes        [][]*Node
	destDir      string
	memCompactCh chan struct{}
	stopCh       chan struct{}
}

func NewLSMTree(destDir string) *LSMTree {
	return &LSMTree{
		destDir:   destDir,
		memTable:  memtable.NewSkipList(),
		walWriter: wal.NewWALWriter(),
	}
}

func (t *LSMTree) Put(key, value []byte) error {
	return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (t *LSMTree) refreshMemTableLocked() error {
	return nil
}

func (t *LSMTree) Compact() error {
	return nil
}

func (t *LSMTree) Restore() error {
	return nil
}
