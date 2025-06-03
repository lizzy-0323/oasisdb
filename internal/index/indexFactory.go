package index

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"oasisdb/internal/config"
	"oasisdb/internal/storage/wal"
	"oasisdb/pkg/logger"
)

const (
	HNSWIndex = "hnsw"
	IVFIndex  = "ivf"
)

// Factory manages vector index instances
type Factory struct {
	mu        sync.RWMutex
	indexDir  string
	indices   map[string]VectorIndex // collection name -> index
	walReader *wal.WALReader
	walWriter *wal.WALWriter
}

// NewFactory creates a new index factory
func NewFactory(conf *config.Config) (*Factory, error) {
	// walReader, err := wal.NewWALReader(path.Join(conf.Dir, "walfile"))
	// if err != nil {
	// 	return nil, err
	// }
	// walWriter, err := wal.NewWALWriter(path.Join(conf.Dir, "walfile"))
	// if err != nil {
	// 	return nil, err
	// }
	return &Factory{
		indexDir: path.Join(conf.Dir, "indexfile"),
		indices:  make(map[string]VectorIndex),
		// walReader: walReader,
		// walWriter: walWriter,
	}, nil
}

// LoadIndexs loads all indexes from disk
func (f *Factory) LoadIndexs() error {
	// Read index directory
	entries, err := os.ReadDir(f.indexDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Create directory if not exists
			if err := os.MkdirAll(f.indexDir, 0755); err != nil {
				return fmt.Errorf("failed to create index directory: %v", err)
			}
			return nil
		}
		return fmt.Errorf("failed to read index directory: %v", err)
	}

	// Load each index file
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse filename to get collection name and config
		collectionName := strings.TrimSuffix(entry.Name(), ".idx")
		configPath := path.Join(f.indexDir, collectionName+".conf")

		// Read config file
		configData, err := os.ReadFile(configPath)
		if err != nil {
			logger.Error("Failed to read index config", "collection", collectionName, "error", err)
			continue
		}

		var config Config
		if err := json.Unmarshal(configData, &config); err != nil {
			logger.Error("Failed to parse index config", "collection", collectionName, "error", err)
			continue
		}

		// Create index
		var index VectorIndex
		switch config.IndexType {
		case HNSWIndex:
			index, err = newHNSWIndex(&config)
		case IVFIndex:
			// TODO: implement IVF index
			continue
		default:
			logger.Error("Unsupported index type", "collection", collectionName, "type", config.IndexType)
			continue
		}

		if err != nil {
			logger.Error("Failed to create index", "collection", collectionName, "error", err)
			continue
		}

		// Load index data
		indexPath := path.Join(f.indexDir, entry.Name())
		file, err := os.Open(indexPath)
		if err != nil {
			logger.Error("Failed to open index file", "collection", collectionName, "error", err)
			continue
		}
		defer file.Close()

		if err := index.Load(context.Background(), file); err != nil {
			logger.Error("Failed to load index data", "collection", collectionName, "error", err)
			continue
		}

		// Store index
		f.indices[collectionName] = index
		logger.Info("Loaded vector index", "collection", collectionName, "type", config.IndexType)
	}

	return nil
}

// CreateIndex creates a new vector index
func (f *Factory) CreateIndex(ctx context.Context, collectionName string, config *Config) (VectorIndex, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Check if index already exists
	if _, exists := f.indices[collectionName]; exists {
		return nil, fmt.Errorf("index already exists for collection %s", collectionName)
	}

	// Create index based on type
	var index VectorIndex
	var err error
	switch config.IndexType {
	case HNSWIndex:
		index, err = newHNSWIndex(config)
	case IVFIndex:
		// TODO: implement IVF index
	default:
		return nil, fmt.Errorf("unsupported index type: %s", config.IndexType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	// Store index
	f.indices[collectionName] = index
	logger.Info("Created vector index", "collection", collectionName, "type", config.IndexType)

	return index, nil
}

// GetIndex retrieves an existing vector index
func (f *Factory) GetIndex(collectionName string) (VectorIndex, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	index, exists := f.indices[collectionName]
	if !exists {
		return nil, fmt.Errorf("index not found for collection %s", collectionName)
	}

	return index, nil
}

// DeleteIndex removes a vector index
func (f *Factory) DeleteIndex(ctx context.Context, collectionName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	index, exists := f.indices[collectionName]
	if !exists {
		return fmt.Errorf("index not found for collection %s", collectionName)
	}

	// Close index
	if err := index.Close(); err != nil {
		return fmt.Errorf("failed to close index: %v", err)
	}

	// Remove from map
	delete(f.indices, collectionName)
	logger.Info("Deleted vector index", "collection", collectionName)

	return nil
}

// Close closes all indices
func (f *Factory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for name, index := range f.indices {
		if err := index.Close(); err != nil {
			logger.Error("Failed to close index", "collection", name, "error", err)
		}
	}

	f.indices = make(map[string]VectorIndex)
	return nil
}
