package sstable

import (
	"os"
	"path"
	"testing"

	"oasisdb/internal/config"

	"github.com/stretchr/testify/assert"
)

func createTestSSTable(t *testing.T, conf *config.Config) string {
	// Create test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	// Create SSTable file
	fileName := "test.sst"
	writer, err := NewSSTableWriter(fileName, conf)
	assert.NoError(t, err)

	// Write test data
	for _, data := range testData {
		err := writer.Append([]byte(data.key), []byte(data.value))
		assert.NoError(t, err)
	}

	// Finish writing
	_, _, _, err = writer.Finish()
	assert.NoError(t, err)

	return fileName
}

func TestSSTableReader_Basic(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)

	// Create test SSTable
	fileName := createTestSSTable(t, conf)

	// Create reader
	reader, err := NewSSTableReader(fileName, conf)
	assert.NoError(t, err)
	defer reader.Close()

	// Test reading index entries
	indexEntries, err := reader.ReadIndex()
	assert.NoError(t, err)
	assert.NotEmpty(t, indexEntries)

	// Test reading filter block
	filters, err := reader.ReadFilter()
	assert.NoError(t, err)
	assert.NotEmpty(t, filters)
}

func TestSSTableReader_InvalidFile(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)

	// Test with non-existent file
	_, err = NewSSTableReader("nonexistent.sst", conf)
	assert.Error(t, err)

	// Test with empty file
	emptyFile := "empty.sst"
	_, err = os.Create(path.Join(tmpDir, emptyFile))
	assert.NoError(t, err)

	_, err = NewSSTableReader(emptyFile, conf)
	assert.ErrorIs(t, err, ErrInvalidFile)
}

func TestSSTableReader_ReadFilter(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)

	// Create test SSTable
	fileName := createTestSSTable(t, conf)

	// Create reader
	reader, err := NewSSTableReader(fileName, conf)
	assert.NoError(t, err)
	defer reader.Close()

	// Test reading filter block
	filters, err := reader.ReadFilter()
	assert.NoError(t, err)
	assert.NotEmpty(t, filters)
}
