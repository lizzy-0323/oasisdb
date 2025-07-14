package index

import (
	"encoding/json"
	"os"
	"path"
	"testing"

	"oasisdb/internal/config"
	"oasisdb/pkg/errors"

	"github.com/stretchr/testify/assert"
)

func setupTestManager(t *testing.T) (*Manager, func()) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "oasisdb_test_*")
	assert.NoError(t, err)

	// Create required subdirectories
	assert.NoError(t, os.MkdirAll(path.Join(tmpDir, "indexfile"), 0755))
	assert.NoError(t, os.MkdirAll(path.Join(tmpDir, "walfile", "index"), 0755))

	// Create manager
	conf := &config.Config{
		Dir: tmpDir,
	}
	manager, err := NewIndexManager(conf)
	assert.NoError(t, err)

	// Return cleanup function
	cleanup := func() {
		manager.Close()
		os.RemoveAll(tmpDir)
	}

	return manager, cleanup
}

func TestManagerCreateIndex(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	// Test create index
	config := &IndexConfig{
		IndexType: HNSWIndex,
		Dimension: 128,
		SpaceType: L2Space,
		Parameters: map[string]interface{}{
			"M":              16,
			"efConstruction": 200,
			"efSearch":       100,
		},
	}

	index, err := manager.CreateIndex("test_collection", config)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// Verify config file was created
	configPath := path.Join(manager.conf.Dir, "indexfile", "test_collection.conf")
	configData, err := os.ReadFile(configPath)
	assert.NoError(t, err)

	var savedConfig IndexConfig
	err = json.Unmarshal(configData, &savedConfig)
	assert.NoError(t, err)
	assert.Equal(t, config.IndexType, savedConfig.IndexType)
	assert.Equal(t, config.Dimension, savedConfig.Dimension)

	// Test duplicate creation
	_, err = manager.CreateIndex("test_collection", config)
	assert.Error(t, err)
}

func TestManagerGetIndex(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	// Create test index
	config := &IndexConfig{
		IndexType: HNSWIndex,
		Dimension: 128,
		SpaceType: L2Space,
	}

	_, err := manager.CreateIndex("test_collection", config)
	assert.NoError(t, err)

	// Test get existing index
	index, err := manager.GetIndex("test_collection")
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// Test get non-existing index
	_, err = manager.GetIndex("non_existing")
	assert.Error(t, err)
}

func TestManagerDeleteIndex(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	// Create test index
	config := &IndexConfig{
		IndexType: HNSWIndex,
		Dimension: 128,
		SpaceType: L2Space,
	}

	_, err := manager.CreateIndex("test_collection", config)
	assert.NoError(t, err)

	// Test delete existing index
	err = manager.DeleteIndex("test_collection")
	assert.NoError(t, err)

	// Verify index was removed
	_, err = manager.GetIndex("test_collection")
	assert.ErrorIs(t, err, errors.ErrIndexNotFound)

	// Test delete non-existing index
	err = manager.DeleteIndex("non_existing")
	assert.ErrorIs(t, err, errors.ErrIndexNotFound)
}

func TestManagerBuildIndexHNSW(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	// Create test index
	config := &IndexConfig{
		IndexType: HNSWIndex,
		Dimension: 4,
		SpaceType: L2Space,
	}
	_, err := manager.CreateIndex("test_collection", config)
	assert.NoError(t, err)

	// Build index with a batch of vectors
	ids, vectors := generateVectors(10, 4)
	err = manager.BuildIndex("test_collection", ids, vectors)
	assert.NoError(t, err)

	// Retrieve index and verify search results
	idx, err := manager.GetIndex("test_collection")
	assert.NoError(t, err)
	res, err := idx.Search(vectors[6], 3)
	assert.NoError(t, err)
	assert.Greater(t, len(res.IDs), 0)
	assert.Equal(t, ids[6], res.IDs[0])
}

func TestManagerBuildIndexIVF(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	dim := 4
	// Create test IVF index
	cfg := &IndexConfig{
		IndexType: IVFFLATIndex,
		Dimension: dim,
		SpaceType: L2Space,
		Parameters: map[string]interface{}{
			"nlist":  float64(4),
			"nprobe": float64(2),
		},
	}
	_, err := manager.CreateIndex("ivf_collection", cfg)
	assert.NoError(t, err)

	ids, vectors := generateVectors(20, dim)
	err = manager.BuildIndex("ivf_collection", ids, vectors)
	assert.NoError(t, err)

	idx, err := manager.GetIndex("ivf_collection")
	assert.NoError(t, err)

	res, err := idx.Search(vectors[10], 3)
	assert.NoError(t, err)
	assert.Len(t, res.IDs, 3)
	assert.Equal(t, ids[10], res.IDs[0])
}

func TestManagerVectorOperations(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	// Create test index
	config := &IndexConfig{
		IndexType: HNSWIndex,
		Dimension: 3,
		SpaceType: L2Space,
	}

	index, err := manager.CreateIndex("test_collection", config)
	assert.NoError(t, err)

	// Test add vector
	vector := []float32{1.0, 2.0, 3.0}
	err = manager.AddVector("test_collection", "1", vector)
	assert.NoError(t, err)

	// Test add batch
	ids := []string{"2", "3"}
	vectors := [][]float32{
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}
	err = manager.AddVectorBatch("test_collection", ids, vectors)
	assert.NoError(t, err)

	// Verify vectors were added by searching
	result, err := index.Search(vector, 3)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.IDs))

	// // Test delete vector
	// err = manager.DeleteVector("test_collection", "1")
	// assert.NoError(t, err)

	// // Verify vector was deleted
	// result, err = index.Search(vector, 3)
	// assert.NoError(t, err)
	// assert.Equal(t, 2, len(result.IDs))
}
