package tree

import (
	"bytes"
	"fmt"
	"oasisdb/internal/config"
	"oasisdb/internal/storage/memtable"
	"oasisdb/internal/storage/wal"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// LSM Tree Engine
type LSMTree struct {
	conf           *config.Config
	dataLock       sync.RWMutex
	memTable       memtable.MemTable         // memtable
	rOnlyMemTables []*memTableCompactItem    // read only memtables
	walWriter      *wal.WALWriter            // WAL writer, using in memTable Put
	nodes          [][]*Node                 // tree data structure
	levelLocks     []sync.RWMutex            // locks used in every level
	memCompactCh   chan *memTableCompactItem // when memtable size reach the limit, trigger compaction
	levelCompactCh chan int                  // when sst file size of one layer reach the limit, trigger compaction
	stopCh         chan struct{}             // stop all jobs
	memTableIndex  int                       // memtable index , correspond to wal files
	levelToSeq     []atomic.Int32
}

func NewLSMTree(conf *config.Config) (*LSMTree, error) {
	// 1. build LSM Tree
	t := &LSMTree{
		conf:           conf,
		stopCh:         make(chan struct{}),
		memTableIndex:  0,
		levelToSeq:     make([]atomic.Int32, conf.MaxLevel),
		nodes:          make([][]*Node, conf.MaxLevel),
		levelLocks:     make([]sync.RWMutex, conf.MaxLevel),
		memCompactCh:   make(chan *memTableCompactItem, 1),
		levelCompactCh: make(chan int, 1),
	}
	// 2. Read sst file, construct nodes
	if err := t.constructTree(); err != nil {
		return nil, err
	}
	// 3. Start lsm compaction
	go t.compact()

	// 4. Read wal files to restore memtables
	if err := t.constructMemTables(); err != nil {
		return nil, err
	}
	return t, nil
}

// Add a pair of kv to lsm tree, directly write into memtable
func (t *LSMTree) Put(key, value []byte) error {
	// 1. get lock
	t.dataLock.Lock()
	defer t.dataLock.Unlock()

	// 2. write into WAL
	if err := t.walWriter.Write(key, value); err != nil {
		return err
	}

	// 3. write into memtable(skiplist)
	t.memTable.Put(key, value)

	// 4. refresh memtable if size reach the limit
	if uint64(t.memTable.Size()*5/4) <= t.conf.SSTSize {
		return nil
	}

	// 5. refresh memtable
	t.refreshMemTableLocked()
	return nil
}

func (t *LSMTree) Stop() {
	close(t.stopCh)
	for i := range t.nodes {
		for _, node := range t.nodes[i] {
			node.Close()
		}
	}
}

func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	t.dataLock.RLock()
	// 1. read active memtable
	value, ok := t.memTable.Get(key)
	if ok {
		t.dataLock.RUnlock()
		return value, true, nil
	}

	// 2. if not found in active memtable, check read only memtables
	for i := len(t.rOnlyMemTables) - 1; i >= 0; i-- {
		value, ok = t.rOnlyMemTables[i].memTable.Get(key)
		if ok {
			t.dataLock.RUnlock()
			return value, true, nil
		}
	}
	t.dataLock.RUnlock()

	// 3. search nodes in level 0
	var err error
	t.levelLocks[0].RLock()
	for i := len(t.nodes[0]) - 1; i >= 0; i-- {
		if value, ok, err = t.nodes[0][i].Get(key); err != nil {
			t.levelLocks[0].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[0].RUnlock()
			return value, true, nil
		}
	}
	t.levelLocks[0].RUnlock()

	// 4. search nodes in other levels
	for level := 1; level < len(t.nodes); level++ {
		t.levelLocks[level].RLock()
		node, ok := t.levelBinarySearch(level, key, 0, len(t.nodes[level])-1)
		if !ok {
			t.levelLocks[level].RUnlock()
			continue
		}
		if value, ok, err = node.Get(key); err != nil {
			t.levelLocks[level].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[level].RUnlock()
			return value, true, nil
		}
		t.levelLocks[level].RUnlock()
	}
	// 4. return if not found
	return nil, false, nil
}

func (t *LSMTree) levelBinarySearch(level int, key []byte, left, right int) (*Node, bool) {
	for left <= right {
		mid := left + (right-left)/2
		switch {
		case bytes.Compare(key, t.nodes[level][mid].startKey) < 0:
			right = mid - 1
		case bytes.Compare(key, t.nodes[level][mid].endKey) > 0:
			left = mid + 1
		default:
			return t.nodes[level][mid], true
		}
	}
	return nil, false
}

func (t *LSMTree) refreshMemTableLocked() {
	// 1. change to readOnly skiplist and add to slices
	// 2. send to compact go routine
	// 3. compact write to level 0 sstable
	oldItem := &memTableCompactItem{
		memTable: t.memTable,
		walFile:  t.newWalFile(),
	}
	t.rOnlyMemTables = append(t.rOnlyMemTables, oldItem)
	t.walWriter.Close()
	go func() {
		t.memCompactCh <- oldItem
	}()

	t.memTableIndex++
	t.memTable, _ = t.newMemTable()
}

func (t *LSMTree) newMemTable() (memtable.MemTable, error) {
	walWriter, err := wal.NewWALWriter(t.newWalFile())
	if err != nil {
		return nil, err
	}
	t.walWriter = walWriter
	memtable := t.conf.MemTableConstructor()
	return memtable, nil
}

func (t *LSMTree) newWalFile() string {
	return path.Join(t.conf.Dir, "walfile", "memtable", fmt.Sprintf("%d.wal", t.memTableIndex))
}

func (t *LSMTree) sstFile(level int, seq int32) string {
	return path.Join(t.conf.Dir, "sstfile", fmt.Sprintf("%d_%d.sst", level, seq))
}

func walFileToMemTableIndex(walFile string) int {
	rawIndex := strings.ReplaceAll(walFile, ".wal", "")
	index, _ := strconv.Atoi(rawIndex)
	return index
}
