package config

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromFile(t *testing.T) {
	// Create a temporary test config file
	tmpDir := t.TempDir()
	testConfigPath := path.Join(tmpDir, "test_config.yaml")

	testConfig := `
dir: ../../
max_level: 7
sst_size: 1048576 
sst_num_per_level: 4
sst_data_block_size: 16384
sst_footer_size: 32
cache_size: 10
`
	err := os.WriteFile(testConfigPath, []byte(testConfig), 0644)
	assert.NoError(t, err)

	// Test reading from file
	cfg, err := FromFile(testConfigPath)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Verify the values
	assert.Equal(t, "../../", cfg.Dir)
	assert.Equal(t, 7, cfg.MaxLevel)
	assert.Equal(t, uint64(1048576), cfg.SSTSize)
	assert.Equal(t, uint64(4), cfg.SSTNumPerLevel)
	assert.Equal(t, uint64(16384), cfg.SSTDataBlockSize)
	assert.Equal(t, uint64(32), cfg.SSTFooterSize)
	assert.Equal(t, 10, cfg.CacheSize)
	assert.NotNil(t, cfg.Filter)
	assert.NotNil(t, cfg.MemTableConstructor)

	// Test with non-existent file
	cfg, err = FromFile("non_existent_file.yaml")
	assert.Error(t, err)
	assert.Nil(t, cfg)
}
