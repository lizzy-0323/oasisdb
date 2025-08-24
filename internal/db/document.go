package db

import (
	"encoding/json"
	"fmt"
	"oasisdb/pkg/errors"
	"oasisdb/pkg/logger"
	"time"
)

// Document represents a document (used for client API)
type Document struct {
	ID         string         `json:"id"`
	Vector     []float32      `json:"vector"`
	Parameters map[string]any `json:"parameters"`
	Dimension  int            `json:"dimension"`
}

// DocumentMetadata represents document metadata stored in scalar storage (without vector)
type DocumentMetadata struct {
	ID         string         `json:"id"`
	Parameters map[string]any `json:"parameters"`
	Dimension  int            `json:"dimension"`
}

type batchData struct {
	docKeys   [][]byte
	docValues [][]byte
	ids       []string
	vectors   [][]float32
}

// docToMetadata converts a Document to DocumentMetadata (without vector)
func docToMetadata(doc *Document) *DocumentMetadata {
	return &DocumentMetadata{
		ID:         doc.ID,
		Parameters: doc.Parameters,
		Dimension:  doc.Dimension,
	}
}

// metadataToDoc converts DocumentMetadata to Document (with vector from index)
func metadataToDoc(metadata *DocumentMetadata, vector []float32) *Document {
	return &Document{
		ID:         metadata.ID,
		Vector:     vector,
		Parameters: metadata.Parameters,
		Dimension:  metadata.Dimension,
	}
}

// UpsertDocument inserts or updates a document
func (db *DB) UpsertDocument(collectionName string, doc *Document) error {
	// handle automatic embedding generation if requested
	if doc.Parameters != nil {
		if flag, ok := doc.Parameters["embedding"].(bool); ok && flag && len(doc.Vector) == 0 {
			text, okText := doc.Parameters["text"].(string)
			if !okText {
				return fmt.Errorf("text parameter is required for embedding when vector is not provided")
			}
			vec64, err := db.conf.EmbeddingProvider.Embed(text)
			if err != nil {
				return fmt.Errorf("failed to generate embedding: %w", err)
			}
			doc.Vector = float64SliceTo32(vec64)
			doc.Dimension = len(doc.Vector)
		}
	}

	// validate vector dimension
	if len(doc.Vector) != doc.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", doc.Dimension, len(doc.Vector))
	}

	// store document metadata (without vector)
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, doc.ID)
	metadata := docToMetadata(doc)
	docData, err := json.Marshal(metadata)
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
	// Get document metadata from scalar storage
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, id)
	data, exists, err := db.Storage.GetScalar([]byte(docKey))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.ErrDocumentNotFound
	}

	var metadata DocumentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	// Get vector from vector index
	vector, err := db.IndexManager.GetVector(collectionName, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get vector: %w", err)
	}

	// Combine metadata and vector to create full document
	doc := metadataToDoc(&metadata, vector)
	return doc, nil
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
	startTime := time.Now()
	logger.Info("Starting vector search", "collection", collectionName, "k", k, "vector_dim", len(queryVector))

	// check if collection exists
	_, err := db.GetCollection(collectionName)
	if err != nil {
		logger.Error("Collection not found", "collection", collectionName, "error", err)
		return nil, nil, err
	}
	logger.Debug("Collection validated", "collection", collectionName)

	index, err := db.IndexManager.GetIndex(collectionName)
	if err != nil {
		logger.Error("Failed to get index", "collection", collectionName, "error", err)
		return nil, nil, err
	}
	logger.Debug("Retrieved index for collection", "collection", collectionName)

	searchStart := time.Now()
	searchResult, err := index.Search(queryVector, k)
	searchDuration := time.Since(searchStart)
	if err != nil {
		logger.Error("Vector search failed", "collection", collectionName, "error", err)
		return nil, nil, err
	}

	totalDuration := time.Since(startTime)
	logger.Info("Vector search completed", "collection", collectionName, "k", k,
		"results", len(searchResult.IDs), "search_duration", searchDuration, "total_duration", totalDuration)

	return searchResult.IDs, searchResult.Distances, nil
}

// SearchDocuments returns top-k documents and distances
func (db *DB) SearchDocuments(collectionName string, queryDoc *Document, k int, filter map[string]any) ([]*Document, []float32, error) {
	startTime := time.Now()
	logger.Info("Starting document search", "collection", collectionName, "k", k, "has_filter", filter != nil)

	// Handle automatic embedding generation if requested
	if queryDoc.Parameters != nil {
		if flag, ok := queryDoc.Parameters["embedding"].(bool); ok && flag && len(queryDoc.Vector) == 0 {
			text, okText := queryDoc.Parameters["text"].(string)
			if !okText {
				logger.Error("Text parameter missing for embedding generation")
				return nil, nil, fmt.Errorf("text parameter is required for embedding when vector is not provided")
			}
			logger.Debug("Generating embedding for text", "text_length", len(text))
			vec64, err := db.conf.EmbeddingProvider.Embed(text)
			if err != nil {
				logger.Error("Failed to generate embedding", "error", err)
				return nil, nil, fmt.Errorf("failed to generate embedding: %w", err)
			}
			queryDoc.Vector = float64SliceTo32(vec64)
			queryDoc.Dimension = len(queryDoc.Vector)
			logger.Debug("Generated embedding", "dimension", queryDoc.Dimension)
		}
	}

	// Validate that query document has a vector
	if len(queryDoc.Vector) == 0 {
		logger.Error("Query document missing vector")
		return nil, nil, fmt.Errorf("query document must have a vector or embedding parameters")
	}
	logger.Debug("Query vector validated", "dimension", len(queryDoc.Vector))

	// 1. get index
	index, err := db.IndexManager.GetIndex(collectionName)
	if err != nil {
		logger.Error("Failed to get index", "collection", collectionName, "error", err)
		return nil, nil, err
	}
	logger.Debug("Retrieved index for collection", "collection", collectionName)

	// 2. search using hnsw index
	searchStart := time.Now()
	searchResult, err := index.Search(queryDoc.Vector, k)
	searchDuration := time.Since(searchStart)
	if err != nil {
		logger.Error("Index search failed", "collection", collectionName, "error", err)
		return nil, nil, err
	}
	logger.Debug("Index search completed", "collection", collectionName, "k", k,
		"found_results", len(searchResult.IDs), "search_duration", searchDuration)

	// 3. check if any results found
	if len(searchResult.IDs) == 0 {
		logger.Info("No search results found", "collection", collectionName, "k", k)
		return nil, nil, errors.ErrNoResultsFound
	}

	// 4. get documents by ids
	docs := make([]*Document, len(searchResult.IDs))
	fetchStart := time.Now()
	for i, id := range searchResult.IDs {
		doc, err := db.GetDocument(collectionName, id)
		if err != nil {
			logger.Error("Failed to get document", "collection", collectionName, "id", id, "error", err)
			return nil, nil, err
		}
		docs[i] = doc
	}
	fetchDuration := time.Since(fetchStart)
	logger.Debug("Document fetch completed", "collection", collectionName, "count", len(docs), "fetch_duration", fetchDuration)

	totalDuration := time.Since(startTime)
	logger.Info("Document search completed", "collection", collectionName, "k", k,
		"results", len(docs), "total_duration", totalDuration)

	// 5. return documents
	return docs, searchResult.Distances, nil
}

func (db *DB) prepareBatchData(collectionName string, docs []*Document) (*batchData, error) {
	// Get collection to validate dimension
	collection, err := db.GetCollection(collectionName)
	if err != nil {
		return nil, fmt.Errorf("failed to get collection: %w", err)
	}

	// Prepare batch data
	docKeys := make([][]byte, len(docs))
	docValues := make([][]byte, len(docs))
	ids := make([]string, len(docs))
	vectors := make([][]float32, len(docs))

	// Validate and prepare data
	for i, doc := range docs {
		// Automatic embedding generation for batch docs
		if doc.Parameters != nil {
			if flag, ok := doc.Parameters["embedding"].(bool); ok && flag && len(doc.Vector) == 0 {
				text, okText := doc.Parameters["text"].(string)
				if !okText {
					return nil, fmt.Errorf("text parameter is required for embedding when vector is not provided for document %s", doc.ID)
				}
				vec64, err := db.conf.EmbeddingProvider.Embed(text)
				if err != nil {
					return nil, fmt.Errorf("failed to generate embedding for document %s: %w", doc.ID, err)
				}
				doc.Vector = float64SliceTo32(vec64)
				doc.Dimension = len(doc.Vector)
			}
		}

		// Validate vector dimension
		if len(doc.Vector) != collection.Dimension {
			return nil, fmt.Errorf("vector dimension mismatch for document %s: expected %d, got %d",
				doc.ID, collection.Dimension, len(doc.Vector))
		}
		doc.Dimension = collection.Dimension

		// Prepare document key and value (only metadata, without vector)
		docKey := fmt.Sprintf("doc:%s:%s", collectionName, doc.ID)
		metadata := docToMetadata(doc)
		docData, err := json.Marshal(metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document metadata %s: %w", doc.ID, err)
		}

		docKeys[i] = []byte(docKey)
		docValues[i] = docData
		ids[i] = doc.ID
		vectors[i] = doc.Vector
	}

	return &batchData{
		docKeys:   docKeys,
		docValues: docValues,
		ids:       ids,
		vectors:   vectors,
	}, nil
}

func (db *DB) BuildIndex(collectionName string, docs []*Document) error {
	// Prepare batch data
	batchData, err := db.prepareBatchData(collectionName, docs)
	if err != nil {
		return err
	}

	// Batch store document metadata (without vectors)
	if err := db.Storage.BatchPutScalar(batchData.docKeys, batchData.docValues); err != nil {
		return fmt.Errorf("failed to batch store document metadata: %w", err)
	}

	// Build vector index
	if err := db.IndexManager.BuildIndex(collectionName, batchData.ids, batchData.vectors); err != nil {
		return fmt.Errorf("failed to build vector index: %w", err)
	}

	return nil
}

func (db *DB) BatchUpsertDocuments(collectionName string, docs []*Document) error {
	// Prepare batch data
	batchData, err := db.prepareBatchData(collectionName, docs)
	if err != nil {
		return err
	}

	// Batch store document metadata (without vectors)
	if err := db.Storage.BatchPutScalar(batchData.docKeys, batchData.docValues); err != nil {
		return fmt.Errorf("failed to batch store document metadata: %w", err)
	}

	// Batch update vector index
	if err := db.IndexManager.AddVectorBatch(collectionName, batchData.ids, batchData.vectors); err != nil {
		return fmt.Errorf("failed to batch update vector index: %w", err)
	}

	return nil
}

// float64SliceTo32 converts a slice of float64 to float32
func float64SliceTo32(src []float64) []float32 {
	res := make([]float32, len(src))
	for i, v := range src {
		res[i] = float32(v)
	}
	return res
}
