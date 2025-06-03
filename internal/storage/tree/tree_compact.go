package tree

import (
	"bytes"
	"math"
	"oasisdb/internal/storage/memtable"
	"oasisdb/internal/storage/sstable"
	"os"
)

type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}

func (t *LSMTree) compact() {
	for {
		select {
		case <-t.stopCh:
			return
		case memTableCompactItem := <-t.memCompactCh:
			t.compactMemTable(memTableCompactItem)
		case level := <-t.levelCompactCh:
			t.compactLevel(level)
		}
	}
}

// compact in level i
func (t *LSMTree) compactLevel(level int) {
	// get nodes in level i, and compact them to level i + 1
	pickedNodes := t.pickCompactNodes(level)

	// insert to level i + 1 target sstWriter
	seq := t.levelToSeq[level+1].Load() + 1
	sstWriter, _ := sstable.NewSSTableWriter(t.sstFile(level+1, seq), t.conf)
	defer sstWriter.Close()

	// get level i + 1 sst file size limit
	sstLimit := t.conf.SSTSize * uint64(math.Pow10(level+1))
	// get all kv data of picked nodes
	pickedKVs := t.pickedNodesToKVs(pickedNodes)
	// traverse every kv data
	for i := 0; i < len(pickedKVs); i++ {
		// if new level + 1 sst file size reach the limit
		if sstWriter.Size() > sstLimit {
			// finish sst writer
			size, blockToFilter, index, err := sstWriter.Finish()
			if err != nil {
				panic(err)
			}
			// insert node into lsm tree
			t.insertNode(level+1, seq, size, blockToFilter, index)
			// update seq
			seq = t.levelToSeq[level+1].Load() + 1
			// construct new sst writer
			sstWriter, _ = sstable.NewSSTableWriter(t.sstFile(level+1, seq), t.conf)
			defer sstWriter.Close()
		}

		// append kv to sst writer in level i + 1
		sstWriter.Append(pickedKVs[i].Key, pickedKVs[i].Value)
		// if this is the last kv data, need to finish sst writer and insert node into lsm tree
		if i == len(pickedKVs)-1 {
			size, blockToFilter, index, err := sstWriter.Finish()
			if err != nil {
				panic(err)
			}
			t.insertNode(level+1, seq, size, blockToFilter, index)
		}
	}

	// remove picked nodes
	t.removeNodes(level, pickedNodes)

	// try trigger compact in next level
	t.tryTriggerCompact(level + 1)
}

func (t *LSMTree) pickedNodesToKVs(pickedNodes []*Node) []*sstable.KV {
	memtable := t.conf.MemTableConstructor()
	for _, node := range pickedNodes {
		kvs, _ := node.GetAll()
		for _, kv := range kvs {
			// put kv into memtable, here larger index means newer data, and this will be used to cover older data
			memtable.Put(kv.Key, kv.Value)
		}
	}

	// sort by memtable
	_kvs := memtable.All()
	kvs := make([]*sstable.KV, 0, len(_kvs))
	for _, kv := range _kvs {
		kvs = append(kvs, &sstable.KV{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return kvs
}

func (t *LSMTree) removeNodes(level int, nodes []*Node) {
	// remove nodes from memory
outer:
	for _, node := range nodes {
		for i := level + 1; i >= level; i-- {
			for j := range t.nodes[i] {
				if node != t.nodes[i][j] {
					continue
				}

				t.levelLocks[i].Lock()
				t.nodes[i] = append(t.nodes[i][:j], t.nodes[i][j+1:]...)
				t.levelLocks[i].Unlock()
				continue outer
			}
		}
	}

	go func() {
		// destroy old nodes, including closing sst reader and deleting sst files
		for _, node := range nodes {
			node.Destroy()
		}
	}()
}

// compact read only memtable to level 0 sstable
func (t *LSMTree) compactMemTable(memCompactItem *memTableCompactItem) {
	// 1. flush memtable to level 0 sstable
	t.flushMemTable(memCompactItem.memTable)

	// 2. remove memtable from rOnly slice
	t.dataLock.Lock()
	for i := 0; i < len(t.rOnlyMemTables); i++ {
		if t.rOnlyMemTables[i] != memCompactItem {
			continue
		}
		t.rOnlyMemTables = t.rOnlyMemTables[i+1:]
	}
	t.dataLock.Unlock()

	// 3. remove wal files, because memtable has been compacted, wal files are no longer needed
	_ = os.Remove(memCompactItem.walFile)
}

func (t *LSMTree) pickCompactNodes(level int) []*Node {
	// read half nodes
	startKey := t.nodes[level][0].Start()
	endKey := t.nodes[level][0].End()

	mid := len(t.nodes[level]) >> 1
	if bytes.Compare(t.nodes[level][mid].Start(), startKey) < 0 {
		startKey = t.nodes[level][mid].Start()
	}

	if bytes.Compare(t.nodes[level][mid].End(), endKey) > 0 {
		endKey = t.nodes[level][mid].End()
	}

	var pickedNodes []*Node
	// pick nodes in level and level + 1
	for i := level + 1; i >= level; i-- {
		for j := 0; j < len(t.nodes[i]); j++ {
			if bytes.Compare(endKey, t.nodes[i][j].Start()) < 0 || bytes.Compare(startKey, t.nodes[i][j].End()) > 0 {
				continue
			}

			pickedNodes = append(pickedNodes, t.nodes[i][j])
		}
	}

	return pickedNodes
}

func (t *LSMTree) insertNode(level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*sstable.IndexEntry) {
	file := t.sstFile(level, seq)
	sstReader, _ := sstable.NewSSTableReader(file, t.conf)

	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
}

func (t *LSMTree) flushMemTable(memTable memtable.MemTable) {
	// 1. generate seq in level 0
	seq := t.levelToSeq[0].Load() + 1

	// 2. create sst writer
	sstWriter, _ := sstable.NewSSTableWriter(t.sstFile(0, seq), t.conf)
	defer sstWriter.Close()

	// 3. traverse memtable and write data to sst writer
	for _, kv := range memTable.All() {
		sstWriter.Append(kv.Key, kv.Value)
	}

	// 4. sstable finish
	size, blockToFilter, index, err := sstWriter.Finish()
	if err != nil {
		panic(err)
	}

	// 5. insert node
	t.insertNode(0, seq, size, blockToFilter, index)

	// 6. try trigger compact
	t.tryTriggerCompact(0)
}

func (t *LSMTree) tryTriggerCompact(level int) {
	if level == len(t.nodes)-1 {
		return
	}

	var size uint64
	for _, node := range t.nodes[level] {
		size += node.size
	}

	if size <= t.conf.SSTSize*uint64(math.Pow10(level))*uint64(t.conf.SSTNumPerLevel) {
		return
	}

	go func() {
		t.levelCompactCh <- level
	}()
}
