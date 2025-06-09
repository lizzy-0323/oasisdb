package db

import (
	"encoding/json"
	"fmt"

	"oasisdb/internal/index"
	"oasisdb/pkg/errors"
)

// Collection represents a collection of vectors
type Collection struct {
	Name      string            `json:"name"`      // collection name
	Metadata  map[string]string `json:"metadata"`  // collection metadata
	Dimension int               `json:"dimension"` // vector dimension
	IndexType string            `json:"indexType"` // index type (e.g., "hnsw")
}

// CreateCollectionOptions represents options for creating a collection
type CreateCollectionOptions struct {
	Name       string            `json:"name"`
	Parameters map[string]string `json:"parameters"`
	Dimension  int               `json:"dimension"`
	IndexType  string            `json:"indexType"` // e.g., "hnsw"
}

func NewCollection(opts *CreateCollectionOptions) *Collection {
	return &Collection{
		Name:      opts.Name,
		Metadata:  opts.Parameters,
		Dimension: opts.Dimension,
		IndexType: opts.IndexType,
	}
}

// CreateCollection creates a new collection
func (db *DB) CreateCollection(opts *CreateCollectionOptions) (*Collection, error) {
	// Validate options
	if opts.Name == "" {
		return nil, fmt.Errorf("collection name is required")
	}
	if opts.Dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive")
	}
	if opts.IndexType == "" {
		opts.IndexType = "hnsw" // default to HNSW
	}

	// Check if collection exists
	key := fmt.Sprintf("collection:%s", opts.Name)
	result, exists, err := db.Storage.GetScalar([]byte(key))
	if err != nil {
		return nil, err
	}
	if exists && result != nil {
		return nil, errors.ErrCollectionExists
	}

	// Create index configuration
	indexConf := &index.IndexConfig{
		IndexType: opts.IndexType,
		Dimension: opts.Dimension,
		SpaceType: index.L2Space, // default to L2 distance
		Parameters: map[string]interface{}{
			"M":              opts.Parameters["M"],
			"efConstruction": opts.Parameters["efConstruction"],
		},
	}

	// Create index
	_, err = db.IndexManager.CreateIndex(opts.Name, indexConf)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	// Create collection
	collection := NewCollection(opts)

	// Save collection metadata
	data, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}

	if err := db.Storage.PutScalar([]byte(key), data); err != nil {
		return nil, err
	}

	return collection, nil
}

// TODO: GetCollection gets a collection
func (db *DB) GetCollection(name string) (*Collection, error) {
	key := fmt.Sprintf("collection:%s", name)
	data, exists, err := db.Storage.GetScalar([]byte(key))
	if err != nil {
		return nil, err
	}
	if !exists || data == nil {
		return nil, errors.ErrCollectionNotFound
	}

	var collection Collection
	if err := json.Unmarshal(data, &collection); err != nil {
		return nil, err
	}

	// Get index
	_, err = db.IndexManager.GetIndex(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get index: %w", err)
	}

	return &collection, nil
}

// DeleteCollection deletes a collection and its index
func (db *DB) DeleteCollection(name string) error {
	// Delete index first
	if err := db.IndexManager.DeleteIndex(name); err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}

	// Delete collection metadata
	key := fmt.Sprintf("collection:%s", name)
	result, exists, err := db.Storage.GetScalar([]byte(key))
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	if !exists || result == nil {
		return errors.ErrCollectionNotFound
	}
	if err := db.Storage.DeleteScalar([]byte(key)); err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	return nil
}

// TODO: ListCollections lists all collections
func (db *DB) ListCollections() ([]*Collection, error) {
	return nil, nil
}
