package db

import (
	"encoding/json"
	"fmt"
)

// Collection represents a collection of vectors
type Collection struct {
	Name     string            // collection name
	Metadata map[string]string // collection metadata
}


// CreateCollection creates a new collection
func (db *DB) CreateCollection(name string, metadata map[string]string) (*Collection, error) {
	key := fmt.Sprintf("collection:%s", name)
	if _, exists, err := db.Storage.GetScalar([]byte(key)); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("collection %s already exists", name)
	}

	collection := &Collection{
		Name:     name,
		Metadata: metadata,
	}

	data, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}

	if err := db.Storage.PutScalar([]byte(key), data); err != nil {
		return nil, err
	}

	return collection, nil
}

// GetCollection gets a collection
func (db *DB) GetCollection(name string) (*Collection, error) {
	key := fmt.Sprintf("collection:%s", name)
	data, exists, err := db.Storage.GetScalar([]byte(key))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("collection %s not found", name)
	}

	var collection Collection
	if err := json.Unmarshal(data, &collection); err != nil {
		return nil, err
	}
	return &collection, nil
}

// DeleteCollection deletes a collection
func (db *DB) DeleteCollection(name string) error {
	key := fmt.Sprintf("collection:%s", name)
	return db.Storage.DeleteScalar([]byte(key))
}

// ListCollections lists all collections
func (db *DB) ListCollections() ([]*Collection, error) {
	return nil, nil
}
