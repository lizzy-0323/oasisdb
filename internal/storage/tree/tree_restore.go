package tree

import (
	"bytes"
	"io/fs"
	"oasisdb/internal/storage/sstable"
	"oasisdb/internal/storage/wal"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

func (t *LSMTree) restoreMemTables(wals []fs.DirEntry) error {
	// 1. restore memtable, and to memory
	for i := 0; i < len(wals); i++ {
		name := wals[i].Name()
		file := path.Join(t.conf.Dir, "walfile", name)
		walReader, err := wal.NewWALReader(file)
		if err != nil {
			return err
		}
		defer walReader.Close()
		memtable := t.conf.MemTableConstructor()
		if err := walReader.RestoreToMemtable(memtable); err != nil {
			return err
		}
		if i == len(wals)-1 { // if it is the last wal file, use this memtable as read-write memtable
			t.memTable = memtable
			t.memTableIndex = walFileToMemTableIndex(name)
			t.walWriter, _ = wal.NewWALWriter(file)
		} else { // other memtables as read-only memtables, need to append to read-only memtables and channel
			memTableCompactItem := &memTableCompactItem{
				walFile:  file,
				memTable: memtable,
			}

			t.rOnlyMemTables = append(t.rOnlyMemTables, memTableCompactItem)
			t.memCompactCh <- memTableCompactItem
		}
	}
	return nil
}

func (t *LSMTree) constructMemTables() error {
	// 1. read wal dir to get all the wal files
	rawFiles, err := os.ReadDir(path.Join(t.conf.Dir, "walfile"))
	if err != nil {
		return err
	}

	// 2. sort the wal files
	files := make([]string, 0, len(rawFiles))
	for _, rawFile := range rawFiles {
		files = append(files, rawFile.Name())
	}
	sort.Strings(files)
	// 3. ensure the files are in order
	var wals []fs.DirEntry
	for _, entry := range rawFiles {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		wals = append(wals, entry)
	}

	// 4. if wals is empty, return new memtable
	if len(wals) == 0 {
		t.memTable, err = t.newMemTable()
		return err
	}

	// 5. restore memtables by wals
	return t.restoreMemTables(wals)
}

// read sst files, construct tree
func (t *LSMTree) constructTree() error {
	sstEntries, err := t.getSortedSSTEntries()
	if err != nil {
		return err
	}

	// read sst files
	for _, sstEntry := range sstEntries {
		if err = t.loadNode(sstEntry); err != nil {
			return err
		}
	}

	return nil
}

func (t *LSMTree) loadNode(sstEntry fs.DirEntry) error {
	sstReader, err := sstable.NewSSTableReader(sstEntry.Name(), t.conf)
	if err != nil {
		return err
	}

	blockToFilter, err := sstReader.ReadFilter()
	if err != nil {
		return err
	}

	index, err := sstReader.ReadIndex()
	if err != nil {
		return err
	}

	size, err := sstReader.Size()
	if err != nil {
		return err
	}

	level, seq := getLevelSeqFromSSTFile(sstEntry.Name())
	// 将 sst 文件作为一个 node 插入到 lsm tree 中
	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
	return nil
}

func (t *LSMTree) insertNodeWithReader(sstReader *sstable.SSTableReader, level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*sstable.IndexEntry) {
	file := t.sstFile(level, seq)
	t.levelToSeq[level].Store(seq)

	newNode := NewNode(t.conf, WithFile(file), WithLevel(level), WithSeq(seq), WithSize(size), WithBlockToFilter(blockToFilter), WithIndexEntries(index))
	// for level 0, as it is not sorted, just append
	if level == 0 {
		t.levelLocks[0].Lock()
		t.nodes[level] = append(t.nodes[level], newNode)
		t.levelLocks[0].Unlock()
		return
	}

	// for level1~levelk, as it is sorted, insert in order
	for i := 0; i < len(t.nodes[level])-1; i++ {
		// find the first node that is greater than newNode
		if bytes.Compare(newNode.End(), t.nodes[level][i+1].Start()) < 0 {
			t.levelLocks[level].Lock()
			t.nodes[level] = append(t.nodes[level][:i+1], t.nodes[level][i:]...)
			t.nodes[level][i+1] = newNode
			t.levelLocks[level].Unlock()
			return
		}
	}

	// if traverse all nodes and haven't inserted newNode, it means newNode is the largest node in this level, append to the end
	t.levelLocks[level].Lock()
	t.nodes[level] = append(t.nodes[level], newNode)
	t.levelLocks[level].Unlock()
}

func getLevelSeqFromSSTFile(file string) (level int, seq int32) {
	file = strings.Replace(file, ".sst", "", -1)
	splitted := strings.Split(file, "_")
	level, _ = strconv.Atoi(splitted[0])
	_seq, _ := strconv.Atoi(splitted[1])
	return level, int32(_seq)
}

func (t *LSMTree) getSortedSSTEntries() ([]fs.DirEntry, error) {
	allEntries, err := os.ReadDir(t.conf.Dir)
	if err != nil {
		return nil, err
	}

	sstEntries := make([]fs.DirEntry, 0, len(allEntries))
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		sstEntries = append(sstEntries, entry)
	}

	sort.Slice(sstEntries, func(i, j int) bool {
		levelI, seqI := getLevelSeqFromSSTFile(sstEntries[i].Name())
		levelJ, seqJ := getLevelSeqFromSSTFile(sstEntries[j].Name())
		if levelI == levelJ {
			return seqI < seqJ
		}
		return levelI < levelJ
	})
	return sstEntries, nil
}
