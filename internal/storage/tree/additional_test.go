package tree

import (
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"

	"oasisdb/internal/config"
	"oasisdb/internal/storage/sstable"
	"oasisdb/internal/storage/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBareTree(conf *config.Config) *LSMTree {
	return &LSMTree{
		conf:           conf,
		memTable:       conf.MemTableConstructor(),
		nodes:          make([][]*Node, conf.MaxLevel),
		levelLocks:     make([]sync.RWMutex, conf.MaxLevel),
		memCompactCh:   make(chan *memTableCompactItem, 4),
		levelCompactCh: make(chan int, 4),
		levelToSeq:     make([]atomic.Int32, conf.MaxLevel),
	}
}

func writeTestWAL(t *testing.T, conf *config.Config, name string, entries map[string]string) {
	t.Helper()

	writer, err := wal.NewWALWriter(path.Join(conf.Dir, "walfile", "memtable", name))
	require.NoError(t, err)
	defer writer.Close()

	for key, value := range entries {
		require.NoError(t, writer.Write([]byte(key), []byte(value)))
	}
}

func writeTestSSTable(t *testing.T, conf *config.Config, name string, entries [][2]string) {
	t.Helper()

	writer, err := sstable.NewSSTableWriter(path.Join("sstfile", name), conf)
	require.NoError(t, err)
	defer writer.Close()

	for _, entry := range entries {
		require.NoError(t, writer.Append([]byte(entry[0]), []byte(entry[1])))
	}

	_, _, _, err = writer.Finish()
	require.NoError(t, err)
}

func closeTreeNodes(tree *LSMTree) {
	for level := range tree.nodes {
		for _, node := range tree.nodes[level] {
			node.Close()
		}
	}
}

func TestConstructMemTablesRestoresLatestWalAsActive(t *testing.T) {
	conf, err := config.NewConfig(t.TempDir())
	require.NoError(t, err)

	writeTestWAL(t, conf, "10.wal", map[string]string{"newer": "value-10"})
	writeTestWAL(t, conf, "2.wal", map[string]string{"older": "value-2"})

	tree := newBareTree(conf)
	require.NoError(t, tree.constructMemTables())
	t.Cleanup(func() {
		if tree.walWriter != nil {
			tree.walWriter.Close()
		}
	})

	value, exists := tree.memTable.Get([]byte("newer"))
	assert.True(t, exists)
	assert.Equal(t, []byte("value-10"), value)
	assert.Equal(t, 10, tree.memTableIndex)

	require.Len(t, tree.rOnlyMemTables, 1)
	value, exists = tree.rOnlyMemTables[0].memTable.Get([]byte("older"))
	assert.True(t, exists)
	assert.Equal(t, []byte("value-2"), value)

	select {
	case item := <-tree.memCompactCh:
		assert.Equal(t, "2.wal", path.Base(item.walFile))
	default:
		t.Fatal("expected restored readonly memtable to be queued for compaction")
	}
}

func TestConstructTreeLoadsSSTablesFromSSTDirectory(t *testing.T) {
	conf, err := config.NewConfig(t.TempDir())
	require.NoError(t, err)

	writeTestSSTable(t, conf, "1_2.sst", [][2]string{{"m", "13"}, {"z", "26"}})
	writeTestSSTable(t, conf, "0_3.sst", [][2]string{{"c", "3"}, {"d", "4"}})
	writeTestSSTable(t, conf, "0_1.sst", [][2]string{{"a", "1"}, {"b", "2"}})
	require.NoError(t, os.WriteFile(path.Join(conf.Dir, "sstfile", "ignore.txt"), []byte("skip"), 0644))

	tree := newBareTree(conf)
	t.Cleanup(func() { closeTreeNodes(tree) })

	entries, err := tree.getSortedSSTEntries()
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, "0_1.sst", entries[0].Name())
	assert.Equal(t, "0_3.sst", entries[1].Name())
	assert.Equal(t, "1_2.sst", entries[2].Name())
	assert.Equal(t, 3, walFileToMemTableIndex("3.wal"))

	require.NoError(t, tree.constructTree())
	require.Len(t, tree.nodes[0], 2)
	require.Len(t, tree.nodes[1], 1)
	assert.Equal(t, int32(3), tree.levelToSeq[0].Load())
	assert.Equal(t, int32(2), tree.levelToSeq[1].Load())

	value, exists, err := tree.Get([]byte("d"))
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("4"), value)

	value, exists, err = tree.Get([]byte("z"))
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("26"), value)

	_, found := tree.levelBinarySearch(1, []byte("m"), 0, len(tree.nodes[1])-1)
	assert.True(t, found)
	_, found = tree.levelBinarySearch(1, []byte("b"), 0, len(tree.nodes[1])-1)
	assert.False(t, found)

	level, seq := getLevelSeqFromSSTFile("1_2.sst")
	assert.Equal(t, 1, level)
	assert.Equal(t, int32(2), seq)
}

func TestNodeAccessorsAndCompactMemTable(t *testing.T) {
	conf, err := config.NewConfig(t.TempDir())
	require.NoError(t, err)

	writeTestSSTable(t, conf, "accessor.sst", [][2]string{{"a", "1"}, {"b", "2"}})

	reader, err := sstable.NewSSTableReader(path.Join("sstfile", "accessor.sst"), conf)
	require.NoError(t, err)

	filters, err := reader.ReadFilter()
	require.NoError(t, err)
	indexEntries, err := reader.ReadIndex()
	require.NoError(t, err)
	size, err := reader.Size()
	require.NoError(t, err)

	node := NewNode(conf,
		WithFile(path.Join("sstfile", "accessor.sst")),
		WithLevel(2),
		WithSeq(7),
		WithSize(size),
		WithStartKey(indexEntries[0].Key),
		WithEndKey(indexEntries[len(indexEntries)-1].Key),
		WithBlockToFilter(filters),
		WithSSTableReader(reader),
		WithIndexEntries(indexEntries),
	)
	t.Cleanup(func() { node.Close() })

	node.WithSSTableReader(reader).WithIndexEntries(indexEntries)

	assert.Equal(t, size, node.Size())
	assert.Equal(t, indexEntries[0].Key, node.Start())
	assert.Equal(t, indexEntries[len(indexEntries)-1].Key, node.End())
	level, seq := node.Index()
	assert.Equal(t, 2, level)
	assert.Equal(t, int32(7), seq)
	assert.NoError(t, node.Check())

	kvs, err := node.GetAll()
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	assert.Equal(t, []byte("a"), kvs[0].Key)
	assert.Equal(t, []byte("1"), kvs[0].Value)

	tree := newBareTree(conf)
	t.Cleanup(func() { closeTreeNodes(tree) })

	memTable := conf.MemTableConstructor()
	require.NoError(t, memTable.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, memTable.Put([]byte("k2"), []byte("v2")))

	walFile := path.Join(conf.Dir, "walfile", "memtable", "99.wal")
	writer, err := wal.NewWALWriter(walFile)
	require.NoError(t, err)
	writer.Close()

	item := &memTableCompactItem{
		walFile:  walFile,
		memTable: memTable,
	}
	tree.rOnlyMemTables = []*memTableCompactItem{item}

	tree.compactMemTable(item)

	assert.Empty(t, tree.rOnlyMemTables)
	require.Len(t, tree.nodes[0], 1)

	value, exists, err := tree.nodes[0][0].Get([]byte("k1"))
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("v1"), value)

	_, err = os.Stat(walFile)
	assert.True(t, os.IsNotExist(err))
}
