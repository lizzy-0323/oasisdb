package tree

import (
	"bytes"
	"oasisdb/internal/config"
	"oasisdb/internal/storage/sstable"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewNode(t *testing.T) {
	dir := "testdata"

	// Create config
	conf, err := config.NewConfig(dir)
	assert.NoError(t, err)

	// Create test directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create a test SSTable file
	testFile := "test.sst"
	writer, err := sstable.NewSSTableWriter(testFile, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Write some test data
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, data := range testData {
		if err := writer.Append(data.key, data.value); err != nil {
			t.Fatal(err)
		}
	}
	_, _, _, err = writer.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// Test creating a new node
	node := NewNode(conf, WithFile(testFile), WithLevel(0), WithSeq(1))

	// Test node properties
	if node.file != testFile {
		t.Errorf("Expected file %s, got %s", testFile, node.file)
	}
	if node.level != 0 {
		t.Errorf("Expected level 0, got %d", node.level)
	}
	if node.seq != 1 {
		t.Errorf("Expected seq 1, got %d", node.seq)
	}

	// Test Get operation
	for _, data := range testData {
		value, exists, err := node.Get(data.key)
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Println("value: ", value)
		// fmt.Println("exists: ", exists)
		// fmt.Println("err: ", err)
		if !exists {
			t.Errorf("Key %s should exist", data.key)
		}
		if !bytes.Equal(value, data.value) {
			t.Errorf("Expected value %s, got %s", data.value, value)
		}
	}

	// Test non-existent key
	value, exists, err := node.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("Key should not exist")
	}
	if value != nil {
		t.Errorf("Value should be nil for non-existent key")
	}

	// Test node destruction
	node.Destroy()
	if _, err := os.Stat(path.Join(conf.Dir, testFile)); !os.IsNotExist(err) {
		t.Error("File should be deleted after destruction")
	}
}
