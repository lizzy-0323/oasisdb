package tree

import (
	"bytes"
	"math"
	"oasisdb/internal/storage/memtable"
	"oasisdb/internal/storage/sstable"
	"oasisdb/pkg/logger"
	"os"
	"time"
)

type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}

func (t *LSMTree) compact() {
	logger.Info("LSM Tree compact goroutine started")
	for {
		select {
		case <-t.stopCh:
			logger.Info("LSM Tree compact goroutine stopping")
			return
		case memTableCompactItem := <-t.memCompactCh:
			logger.Debug("Received memtable compact request", "wal_file", memTableCompactItem.walFile)
			t.compactMemTable(memTableCompactItem)
		case level := <-t.levelCompactCh:
			logger.Debug("Received level compact request", "level", level)
			t.compactLevel(level)
		}
	}
}

// compact in level i
func (t *LSMTree) compactLevel(level int) {
	startTime := time.Now()
	logger.Info("Starting level compaction", "level", level, "target_level", level+1)

	// get nodes in level i, and compact them to level i + 1
	pickedNodes := t.pickCompactNodes(level)
	logger.Debug("Picked nodes for compaction", "level", level, "node_count", len(pickedNodes))

	// insert to level i + 1 target sstWriter
	seq := t.levelToSeq[level+1].Load() + 1
	sstWriter, _ := sstable.NewSSTableWriter(t.sstFile(level+1, seq), t.conf)
	defer sstWriter.Close()

	// get level i + 1 sst file size limit
	sstLimit := t.conf.SSTSize * uint64(math.Pow10(level+1))
	logger.Debug("Compaction parameters", "target_level", level+1, "seq", seq, "sst_limit", sstLimit)

	// get all kv data of picked nodes
	pickedKVs := t.pickedNodesToKVs(pickedNodes)
	logger.Debug("Collected KV pairs from picked nodes", "kv_count", len(pickedKVs))
	// traverse every kv data
	for i := 0; i < len(pickedKVs); i++ {
		// if new level + 1 sst file size reach the limit
		if sstWriter.Size() > sstLimit {
			logger.Debug("SST file size limit reached, creating new file",
				"current_size", sstWriter.Size(), "limit", sstLimit, "level", level+1, "seq", seq)
			// finish sst writer
			size, blockToFilter, index, err := sstWriter.Finish()
			if err != nil {
				logger.Error("Failed to finish SST writer", "error", err)
				panic(err)
			}
			// insert node into lsm tree
			t.insertNode(level+1, seq, size, blockToFilter, index)
			logger.Debug("Inserted new SST node", "level", level+1, "seq", seq, "size", size)

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
				logger.Error("Failed to finish final SST writer", "error", err)
				panic(err)
			}
			t.insertNode(level+1, seq, size, blockToFilter, index)
			logger.Debug("Inserted final SST node", "level", level+1, "seq", seq, "size", size)
		}
	}

	// remove picked nodes
	t.removeNodes(level, pickedNodes)
	logger.Debug("Removed old nodes after compaction", "level", level, "removed_count", len(pickedNodes))

	// try trigger compact in next level
	t.tryTriggerCompact(level + 1)

	duration := time.Since(startTime)
	logger.Info("Level compaction completed", "level", level, "target_level", level+1,
		"processed_kvs", len(pickedKVs), "duration", duration)
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
	startTime := time.Now()
	logger.Info("Starting memtable compaction", "wal_file", memCompactItem.walFile)

	// 1. flush memtable to level 0 sstable
	t.flushMemTable(memCompactItem.memTable)
	logger.Debug("Flushed memtable to level 0 SSTable")

	// 2. remove memtable from rOnly slice
	t.dataLock.Lock()
	originalCount := len(t.rOnlyMemTables)
	for i := 0; i < len(t.rOnlyMemTables); i++ {
		if t.rOnlyMemTables[i] != memCompactItem {
			continue
		}
		t.rOnlyMemTables = t.rOnlyMemTables[i+1:]
		break
	}
	newCount := len(t.rOnlyMemTables)
	t.dataLock.Unlock()
	logger.Debug("Removed memtable from readonly list", "before_count", originalCount, "after_count", newCount)

	// 3. remove wal files, because memtable has been compacted, wal files are no longer needed
	err := os.Remove(memCompactItem.walFile)
	if err != nil {
		logger.Warn("Failed to remove WAL file", "file", memCompactItem.walFile, "error", err)
	} else {
		logger.Debug("Removed WAL file", "file", memCompactItem.walFile)
	}

	duration := time.Since(startTime)
	logger.Info("Memtable compaction completed", "wal_file", memCompactItem.walFile, "duration", duration)
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
	logger.Debug("Flushing memtable to level 0", "seq", seq)

	// 2. create sst writer
	sstWriter, _ := sstable.NewSSTableWriter(t.sstFile(0, seq), t.conf)
	defer sstWriter.Close()

	// 3. traverse memtable and write data to sst writer
	kvCount := 0
	for _, kv := range memTable.All() {
		sstWriter.Append(kv.Key, kv.Value)
		kvCount++
	}
	logger.Debug("Wrote KV pairs to SST", "count", kvCount, "level", 0, "seq", seq)

	// 4. sstable finish
	size, blockToFilter, index, err := sstWriter.Finish()
	if err != nil {
		logger.Error("Failed to finish SST writer during memtable flush", "error", err)
		panic(err)
	}
	logger.Debug("Finished SST file", "level", 0, "seq", seq, "size", size)

	// 5. insert node
	t.insertNode(0, seq, size, blockToFilter, index)
	logger.Debug("Inserted SST node into level 0", "seq", seq)

	// 6. try trigger compact
	t.tryTriggerCompact(0)
}

func (t *LSMTree) tryTriggerCompact(level int) {
	if level == len(t.nodes)-1 {
		logger.Debug("Skip compaction trigger for max level", "level", level)
		return
	}

	var size uint64
	nodeCount := len(t.nodes[level])
	for _, node := range t.nodes[level] {
		size += node.size
	}

	threshold := t.conf.SSTSize * uint64(math.Pow10(level)) * uint64(t.conf.SSTNumPerLevel)
	logger.Debug("Checking compaction trigger", "level", level, "current_size", size,
		"threshold", threshold, "node_count", nodeCount)

	if size <= threshold {
		logger.Debug("No compaction needed", "level", level, "size", size, "threshold", threshold)
		return
	}

	logger.Info("Triggering level compaction", "level", level, "size", size, "threshold", threshold)
	go func() {
		t.levelCompactCh <- level
	}()
}
