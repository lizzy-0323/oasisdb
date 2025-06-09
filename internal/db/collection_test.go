package db

import (
	"os"
	"oasisdb/internal/config"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollectionOperations(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "oasisdb_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create DB instance
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)
	db, err := New(conf)
	assert.NoError(t, err)
	err = db.Open()
	assert.NoError(t, err)
	defer db.Close()

	// Test CreateCollection
	collection, err := db.CreateCollection(&CreateCollectionOptions{
		Name:      "test_collection",
		Dimension: 128,
		IndexType: "hnsw",
		Parameters: map[string]string{
			"M":              "16",
			"efConstruction": "100",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, collection)
	assert.Equal(t, "test_collection", collection.Name)

	// Test GetCollection
	collection2, err := db.GetCollection("test_collection")
	assert.NoError(t, err)
	assert.NotNil(t, collection2)
	assert.Equal(t, "test_collection", collection2.Name)

	// Test GetCollection with non-existent collection
	collection3, err := db.GetCollection("non_existent")
	assert.Error(t, err)
	assert.Nil(t, collection3)

	// Test CreateCollection with existing name
	collection4, err := db.CreateCollection(&CreateCollectionOptions{
		Name: "test_collection",
	})
	assert.Error(t, err)
	assert.Nil(t, collection4)

	// Test DeleteCollection
	err = db.DeleteCollection("test_collection")
	assert.NoError(t, err)

	// Verify collection was deleted
	collection5, err := db.GetCollection("test_collection")
	assert.Error(t, err)
	assert.Nil(t, collection5)

	// Test DeleteCollection with non-existent collection
	err = db.DeleteCollection("non_existent")
	assert.Error(t, err)
}
