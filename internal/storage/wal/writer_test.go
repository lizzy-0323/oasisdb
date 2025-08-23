package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALWriter_NewWALWriter tests creating a new WAL writer
func TestWALWriter_NewWALWriter(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test.wal")

	// Test creating new WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer writer.Close()

	// Check that the file was created
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Error("WAL file was not created")
	}

	// Check writer fields
	if writer.file != walFile {
		t.Errorf("Expected file path '%s', got '%s'", walFile, writer.file)
	}
	if writer.dest == nil {
		t.Error("File handle should not be nil")
	}
}

// TestWALWriter_NewWALWriter_InvalidPath tests creating WAL writer with invalid path
func TestWALWriter_NewWALWriter_InvalidPath(t *testing.T) {
	// Try to create WAL writer with invalid path
	invalidPath := "/invalid/path/test.wal"
	writer, err := NewWALWriter(invalidPath)
	if err == nil {
		writer.Close()
		t.Error("Expected error when creating WAL writer with invalid path")
	}
	if writer != nil {
		t.Error("Writer should be nil when creation fails")
	}
}

// TestWALWriter_Write tests writing key-value pairs
func TestWALWriter_Write(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test.wal")

	// Create WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer writer.Close()

	// Test cases
	testCases := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte(""), []byte("empty_key")},
		{[]byte("empty_value"), []byte("")},
		{[]byte("long_key_with_many_characters"), []byte("long_value_with_many_characters_and_more_content")},
	}

	// Write test data
	for _, tc := range testCases {
		err := writer.Write(tc.key, tc.value)
		if err != nil {
			t.Errorf("Failed to write key-value pair (%s, %s): %v", tc.key, tc.value, err)
		}
	}

	// Check file size is greater than 0
	info, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("WAL file should not be empty after writing data")
	}
}

// TestWALWriter_Write_LargeData tests writing large key-value pairs
func TestWALWriter_Write_LargeData(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test_large.wal")

	// Create WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer writer.Close()

	// Create large key and value
	largeKey := make([]byte, 10000)
	largeValue := make([]byte, 100000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Write large data
	err = writer.Write(largeKey, largeValue)
	if err != nil {
		t.Errorf("Failed to write large data: %v", err)
	}

	// Check file size
	info, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	expectedMinSize := int64(len(largeKey) + len(largeValue))
	if info.Size() < expectedMinSize {
		t.Errorf("WAL file size %d is smaller than expected minimum %d", info.Size(), expectedMinSize)
	}
}

// TestWALWriter_Write_MultipleWrites tests multiple sequential writes
func TestWALWriter_Write_MultipleWrites(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test_multiple.wal")

	// Create WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer writer.Close()

	// Write multiple entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte("key" + string(rune('0'+i%10)))
		value := []byte("value" + string(rune('0'+i%10)))

		err := writer.Write(key, value)
		if err != nil {
			t.Errorf("Failed to write entry %d: %v", i, err)
		}
	}

	// Check file exists and has content
	info, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("WAL file should not be empty after writing multiple entries")
	}
}

// TestWALWriter_Close tests closing the WAL writer
func TestWALWriter_Close(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "test_close.wal")

	// Create WAL writer
	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	// Write some data
	err = writer.Write([]byte("test"), []byte("data"))
	if err != nil {
		t.Errorf("Failed to write test data: %v", err)
	}

	// Close the writer
	writer.Close()

	_ = writer.Write([]byte("after"), []byte("close"))

}
