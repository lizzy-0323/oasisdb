package wal

import (
	"oasisdb/internal/storage/memtable"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// MockMemTable is a simple implementation of MemTable for testing
type MockMemTable struct {
	data map[string][]byte
}

func NewMockMemTable() *MockMemTable {
	return &MockMemTable{
		data: make(map[string][]byte),
	}
}

func (m *MockMemTable) Put(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockMemTable) Get(key []byte) ([]byte, bool) {
	value, exists := m.data[string(key)]
	return value, exists
}

func (m *MockMemTable) All() []*memtable.KVPair {
	var pairs []*memtable.KVPair
	for k, v := range m.data {
		pairs = append(pairs, &memtable.KVPair{
			Key:   []byte(k),
			Value: v,
		})
	}
	return pairs
}

func (m *MockMemTable) Size() int {
	size := 0
	for k, v := range m.data {
		size += len(k) + len(v)
	}
	return size
}

func (m *MockMemTable) EntriesCnt() int {
	return len(m.data)
}

// TestWALReader_NewWALReader tests creating a new WAL reader
func TestWALReader_NewWALReader(t *testing.T) {
	// Create temporary directory and file
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test.wal")

	// Create an empty file
	file, err := os.Create(walFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	file.Close()

	// Test creating new WAL reader
	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Check reader fields
	if reader.file != walFile {
		t.Errorf("Expected file path '%s', got '%s'", walFile, reader.file)
	}
	if reader.src == nil {
		t.Error("File handle should not be nil")
	}
	if reader.reader == nil {
		t.Error("Buffer reader should not be nil")
	}
}

// TestWALReader_NewWALReader_InvalidPath tests creating WAL reader with invalid path
func TestWALReader_NewWALReader_InvalidPath(t *testing.T) {
	// Try to create WAL reader with non-existent file
	invalidPath := "/invalid/path/test.wal"
	reader, err := NewWALReader(invalidPath)
	if err == nil {
		reader.Close()
		t.Error("Expected error when creating WAL reader with invalid path")
	}
	if reader != nil {
		t.Error("Reader should be nil when creation fails")
	}
}

// TestWALReader_RestoreToMemtable tests restoring data to memtable
func TestWALReader_RestoreToMemtable(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test.wal")

	// First write some data using WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, td := range testData {
		err := writer.Write(td.key, td.value)
		if err != nil {
			t.Fatalf("Failed to write test data: %v", err)
		}
	}
	writer.Close()

	// Now read the data using WAL reader
	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Create mock memtable and restore data
	memTable := NewMockMemTable()
	err = reader.RestoreToMemtable(memTable)
	if err != nil {
		t.Fatalf("Failed to restore to memtable: %v", err)
	}

	// Verify all data was restored correctly
	for _, td := range testData {
		value, exists := memTable.Get(td.key)
		if !exists {
			t.Errorf("Key '%s' not found in memtable", td.key)
			continue
		}
		if !reflect.DeepEqual(value, td.value) {
			t.Errorf("Value mismatch for key '%s': expected '%s', got '%s'", td.key, td.value, value)
		}
	}

	// Check entry count
	if memTable.EntriesCnt() != len(testData) {
		t.Errorf("Expected %d entries, got %d", len(testData), memTable.EntriesCnt())
	}
}

// TestWALReader_RestoreToMemtable_EmptyFile tests restoring from empty file
func TestWALReader_RestoreToMemtable_EmptyFile(t *testing.T) {
	// Create temporary directory and empty file
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "empty.wal")

	file, err := os.Create(walFile)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}
	file.Close()

	// Create WAL reader
	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Create mock memtable and restore data
	memTable := NewMockMemTable()
	err = reader.RestoreToMemtable(memTable)
	if err != nil {
		t.Fatalf("Failed to restore from empty file: %v", err)
	}

	// Verify memtable is empty
	if memTable.EntriesCnt() != 0 {
		t.Errorf("Expected 0 entries from empty file, got %d", memTable.EntriesCnt())
	}
}

// TestWALReader_RestoreToMemtable_LargeData tests restoring large data
func TestWALReader_RestoreToMemtable_LargeData(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "large.wal")

	// Write large data using WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	// Create large key and value
	largeKey := make([]byte, 5000)
	largeValue := make([]byte, 50000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte((i + 100) % 256)
	}

	err = writer.Write(largeKey, largeValue)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}
	writer.Close()

	// Read the data using WAL reader
	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Restore to memtable
	memTable := NewMockMemTable()
	err = reader.RestoreToMemtable(memTable)
	if err != nil {
		t.Fatalf("Failed to restore large data: %v", err)
	}

	// Verify data
	value, exists := memTable.Get(largeKey)
	if !exists {
		t.Error("Large key not found in memtable")
	}
	if !reflect.DeepEqual(value, largeValue) {
		t.Error("Large value mismatch")
	}
}

// TestWALReader_Close tests closing the WAL reader
func TestWALReader_Close(t *testing.T) {
	// Create temporary directory and file
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test.wal")

	file, err := os.Create(walFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	file.Close()

	// Create WAL reader
	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}

	// Close the reader (should not panic)
	reader.Close()

	// Try to use reader after close (behavior is undefined but should not panic)
	memTable := NewMockMemTable()
	err = reader.RestoreToMemtable(memTable)
	// We don't check for specific error as behavior after close is not defined
}
