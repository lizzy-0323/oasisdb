package db

import (
	"encoding/json"
	"fmt"
	"oasisdb/pkg/errors"
)

// Document represents a document
type Document struct {
	ID         string         `json:"id"`
	Vector     []float32      `json:"vector"`
	Parameters map[string]any `json:"parameters"`
	Dimension  int            `json:"dimension"`
}

// UpsertDocument inserts or updates a document
func (db *DB) UpsertDocument(collectionName string, doc *Document) error {
	// validate vector dimension
	if len(doc.Vector) != doc.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", doc.Dimension, len(doc.Vector))
	}

	// store document metadata
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, doc.ID)
	docData, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := db.Storage.PutScalar([]byte(docKey), docData); err != nil {
		return err
	}

	// upsert vector index
	if err := db.IndexManager.AddVector(collectionName, doc.ID, doc.Vector); err != nil {
		return err
	}

	return nil
}

// GetDocument gets a document
func (db *DB) GetDocument(collectionName string, id string) (*Document, error) {
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, id)
	data, exists, err := db.Storage.GetScalar([]byte(docKey))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.ErrDocumentNotFound
	}

	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// DeleteDocument deletes a document
func (db *DB) DeleteDocument(collectionName string, id string) error {
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, id)
	if err := db.Storage.DeleteScalar([]byte(docKey)); err != nil {
		return err
	}

	if err := db.IndexManager.DeleteVector(collectionName, id); err != nil {
		return err
	}
	return nil
}

// SearchVectors returns top-k vector ids and distances
func (db *DB) SearchVectors(collectionName string, queryVector []float32, k int) ([]string, []float32, error) {
	index, err := db.IndexManager.GetIndex(collectionName)
	if err != nil {
		return nil, nil, err
	}
	searchResult, err := index.Search(queryVector, k)
	if err != nil {
		return nil, nil, err
	}
	return searchResult.IDs, searchResult.Distances, nil
}

// SearchDocuments returns top-k documents and distances
func (db *DB) SearchDocuments(collectionName string, vector []float32, k int, filter map[string]interface{}) ([]*Document, []float32, error) {
	// 1. get index
	index, err := db.IndexManager.GetIndex(collectionName)
	if err != nil {
		return nil, nil, err
	}

	// 2. search using hnsw index
	searchResult, err := index.Search(vector, k)
	if err != nil {
		return nil, nil, err
	}

	// 3. get documents by ids
	docs := make([]*Document, len(searchResult.IDs))
	for i, id := range searchResult.IDs {
		doc, err := db.GetDocument(collectionName, id)
		if err != nil {
			return nil, nil, err
		}
		docs[i] = doc
	}

	// 4. return documents
	return docs, searchResult.Distances, nil
}

// BatchUpsertDocuments 批量插入或更新文档
func (db *DB) BatchUpsertDocuments(collectionName string, docs []*Document) error {
	// Get collection to validate dimension
	collection, err := db.GetCollection(collectionName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %v", err)
	}

	// Prepare batch data
	docKeys := make([][]byte, len(docs))
	docValues := make([][]byte, len(docs))
	ids := make([]string, len(docs))
	vectors := make([][]float32, len(docs))

	// Validate and prepare data
	for i, doc := range docs {
		// Validate vector dimension
		if len(doc.Vector) != collection.Dimension {
			return fmt.Errorf("vector dimension mismatch for document %s: expected %d, got %d",
				doc.ID, collection.Dimension, len(doc.Vector))
		}
		doc.Dimension = collection.Dimension

		// Prepare document key and value
		docKey := fmt.Sprintf("doc:%s:%s", collectionName, doc.ID)
		docData, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document %s: %v", doc.ID, err)
		}

		docKeys[i] = []byte(docKey)
		docValues[i] = docData
		ids[i] = doc.ID
		vectors[i] = doc.Vector
	}

	// Batch store document metadata
	if err := db.Storage.BatchPutScalar(docKeys, docValues); err != nil {
		return fmt.Errorf("failed to batch store documents: %v", err)
	}

	// Batch update vector index
	if err := db.IndexManager.AddVectorBatch(collectionName, ids, vectors); err != nil {
		return fmt.Errorf("failed to batch update vector index: %v", err)
	}

	return nil
}

// TODO: BatchDeleteDocuments 批量删除文档
func (db *DB) BatchDeleteDocuments(collectionName string, ids []string) error {
	// 1. 批量删除文档元数据
	for _, id := range ids {
		if err := db.DeleteDocument(collectionName, id); err != nil {
			return err
		}
	}
	return nil
}
