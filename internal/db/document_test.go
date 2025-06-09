package db

import (
	"oasisdb/internal/config"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDocumentOperations(t *testing.T) {
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

	// Create a collection
	collection, err := db.CreateCollection(&CreateCollectionOptions{
		Name:      "test_collection",
		Dimension: 3,
		IndexType: "hnsw",
		Parameters: map[string]string{
			"M":              "16",
			"efConstruction": "100",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, collection)
	assert.Equal(t, "test_collection", collection.Name)

	// Test document operations
	docID := "doc1"
	doc := &Document{
		ID:         docID,
		Vector:     []float32{1.0, 2.0, 3.0},
		Parameters: map[string]interface{}{"name": "test", "age": 30, "tags": []string{"tag1", "tag2"}},
		Dimension:  3,
	}

	// Test CreateDocument
	err = db.UpsertDocument(collection.Name, doc)
	assert.NoError(t, err)

	// Test GetDocument
	retrievedDoc, err := db.GetDocument(collection.Name, docID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedDoc)
	assert.Equal(t, doc.Parameters["name"], retrievedDoc.Parameters["name"])
	assert.Equal(t, float64(doc.Parameters["age"].(int)), retrievedDoc.Parameters["age"])

	// Test GetDocument with non-existent document
	retrievedDoc, err = db.GetDocument(collection.Name, "non_existent")
	assert.Error(t, err)
	assert.Nil(t, retrievedDoc)

	// Test UpdateDocument
	updatedDoc := &Document{
		ID:         docID,
		Vector:     []float32{1.0, 2.0, 3.0},
		Parameters: map[string]interface{}{"name": "updated", "age": 31, "tags": []string{"tag1", "tag2"}},
		Dimension:  3,
	}
	err = db.UpsertDocument(collection.Name, updatedDoc)
	assert.NoError(t, err)

	// Verify update
	retrievedDoc, err = db.GetDocument(collection.Name, docID)
	assert.NoError(t, err)
	assert.Equal(t, updatedDoc.Parameters["name"], retrievedDoc.Parameters["name"])
	assert.Equal(t, float64(updatedDoc.Parameters["age"].(int)), retrievedDoc.Parameters["age"])

	// Test DeleteDocument
	err = db.DeleteDocument(collection.Name, docID)
	assert.NoError(t, err)

	// Verify document was deleted
	retrievedDoc, err = db.GetDocument(collection.Name, docID)
	assert.Error(t, err)
	assert.Nil(t, retrievedDoc)

	// Test DeleteDocument with non-existent document
	err = db.DeleteDocument(collection.Name, "non_existent")
	assert.Error(t, err)
}
