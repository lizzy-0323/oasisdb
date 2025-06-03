package sstable

import (
	"encoding/binary"
	"os"
	"path"
	"testing"

	"oasisdb/internal/config"

	"github.com/stretchr/testify/assert"
)

func TestSSTableWriter_Basic(t *testing.T) {
	// Create temp dir for test
	tmpDir := t.TempDir()

	// Create config
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)

	// Create writer
	writer, err := NewSSTableWriter("test.sst", conf)
	assert.NoError(t, err)

	// Test basic operations
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	// Write test data
	for _, data := range testData {
		err := writer.Append([]byte(data.key), []byte(data.value))
		assert.NoError(t, err)
	}

	// Finish writing
	_, _, _, err = writer.Finish()
	assert.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(path.Join(tmpDir, "test.sst"))
	assert.NoError(t, err)
}

func TestSSTableWriter_BlockRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	conf, err := config.NewConfig(tmpDir, config.WithSSTDataBlockSize(32))
	assert.NoError(t, err)

	writer, err := NewSSTableWriter("test.sst", conf)
	assert.NoError(t, err)

	// Write data that should cause multiple block refreshes
	for i := 0; i < 10; i++ {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		err := writer.Append(key, value)
		assert.NoError(t, err)
	}

	_, _, _, err = writer.Finish()
	assert.NoError(t, err)

	// Read and verify the file structure
	data, err := os.ReadFile(path.Join(tmpDir, "test.sst"))
	assert.NoError(t, err)

	// Read footer
	footerStart := len(data) - int(conf.SSTFooterSize)
	footer := data[footerStart:]

	// Verify footer format
	filterOffset := binary.LittleEndian.Uint64(footer[0:8])
	filterSize := binary.LittleEndian.Uint64(footer[8:16])
	indexOffset := binary.LittleEndian.Uint64(footer[16:24])
	indexSize := binary.LittleEndian.Uint64(footer[24:])

	// Verify section boundaries
	assert.Equal(t, indexOffset, filterOffset+filterSize)
	assert.Equal(t, uint64(footerStart), indexOffset+indexSize)
}

func TestSSTableWriter_EmptyWrite(t *testing.T) {
	tmpDir := t.TempDir()
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)

	writer, err := NewSSTableWriter("empty.sst", conf)
	assert.NoError(t, err)

	// Finish without writing any data
	_, _, _, err = writer.Finish()
	assert.NoError(t, err)

	// Verify file exists and has at least footer size
	info, err := os.Stat(path.Join(tmpDir, "empty.sst"))
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, info.Size(), int64(conf.SSTFooterSize)) // Footer size
}
