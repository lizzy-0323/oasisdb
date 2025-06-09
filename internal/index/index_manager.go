package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
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

// Manager manages vector index instances
type Manager struct {
	conf      *config.Config
	mu        sync.RWMutex
	indices   map[string]VectorIndex // collection name -> index
	indexCh   chan indexSaveItem
	stopCh    chan struct{}
	walWriter *wal.WALWriter
}

type indexSaveItem struct {
	collectionName string
	index          VectorIndex
}

// NewIndexManager creates a new index manager
func NewIndexManager(conf *config.Config) (*Manager, error) {
	m := &Manager{
		conf:    conf,
		indices: make(map[string]VectorIndex),
		indexCh: make(chan indexSaveItem),
		stopCh:  make(chan struct{}),
	}
	if err := m.LoadIndexs(); err != nil {
		return nil, err
	}
	go m.monitorIndexSave()
	return m, nil
}

func (m *Manager) reconstructIndex() error {
	// 1. Read WAL directory for index
	entries, err := os.ReadDir(path.Join(m.conf.Dir, "walfile", "index"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read WAL directory: %v", err)
	}

	// 2. Sort WAL files by sequence number
	sort.Slice(entries, func(i, j int) bool {
		seq1 := strings.TrimSuffix(entries[i].Name(), ".wal")
		seq2 := strings.TrimSuffix(entries[j].Name(), ".wal")
		num1, _ := strconv.Atoi(seq1)
		num2, _ := strconv.Atoi(seq2)
		return num1 < num2
	})

	// 3. Process each WAL file
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}

		walPath := path.Join(m.conf.Dir, "walfile", "index", entry.Name())
		walReader, err := wal.NewWALReader(walPath)
		if err != nil {
			logger.Error("Failed to create WAL reader", "file", entry.Name(), "error", err)
			continue
		}
		defer walReader.Close()

		data, err := os.ReadFile(walPath)
		if err != nil {
			logger.Error("Failed to read WAL file", "file", entry.Name(), "error", err)
			continue
		}

		walEntry, err := decodeWALEntry(data)
		if err != nil {
			logger.Error("Failed to decode WAL entry", "file", entry.Name(), "error", err)
			continue
		}

		// Handle operation
		if walEntry.OpType == WALOpCreateIndex {
			var createData CreateIndexData
			if err := json.Unmarshal(walEntry.Data, &createData); err != nil {
				logger.Error("Failed to unmarshal create index data", "file", entry.Name(), "error", err)
				continue
			}

			// Create index
			var index VectorIndex
			switch createData.Config.IndexType {
			case HNSWIndex:
				index, err = newHNSWIndex(createData.Config)
			case IVFIndex:
				// TODO: implement IVF index
				continue
			default:
				logger.Error("Unsupported index type", "file", entry.Name(), "type", createData.Config.IndexType)
				continue
			}

			if err != nil {
				logger.Error("Failed to create index", "file", entry.Name(), "error", err)
				continue
			}

			// Store index
			m.indices[walEntry.Collection] = index
			logger.Info("Reconstructed index from WAL", "collection", walEntry.Collection)
		} else {
			// Apply operation to existing index
			index, exists := m.indices[walEntry.Collection]
			if !exists {
				logger.Error("Index not found for WAL operation", "collection", walEntry.Collection)
				continue
			}

			if err := index.ApplyOpWithWal(walEntry); err != nil {
				logger.Error("Failed to apply WAL entry", "collection", walEntry.Collection, "error", err)
				continue
			}
		}
	}

	return nil
}

// LoadIndexs loads all indexes from disk
func (m *Manager) LoadIndexs() error {
	// 1. Read index directory
	entries, err := os.ReadDir(path.Join(m.conf.Dir, "indexfile"))
	if err != nil {
		if os.IsNotExist(err) {
			// Create directory if not exists
			if err := os.MkdirAll(path.Join(m.conf.Dir, "indexfile"), 0755); err != nil {
				return fmt.Errorf("failed to create index directory: %v", err)
			}
			return nil
		}
		return fmt.Errorf("failed to read index directory: %v", err)
	}

	// 2. Reconstruct index from WAL
	if err := m.reconstructIndex(); err != nil {
		return err
	}

	// 3. Load each index file
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse filename to get collection name and config
		collectionName := strings.TrimSuffix(entry.Name(), ".idx")
		configPath := path.Join(m.conf.Dir, "indexfile", collectionName+".conf")

		// Read config file
		configData, err := os.ReadFile(configPath)
		if err != nil {
			logger.Error("Failed to read index config", "collection", collectionName, "error", err)
			continue
		}

		var config IndexConfig
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
		indexPath := path.Join(m.conf.Dir, "indexfile", entry.Name())
		if err := index.Load(indexPath); err != nil {
			logger.Error("Failed to load index data", "collection", collectionName, "error", err)
			continue
		}

		// Store index in table
		m.indices[collectionName] = index
		logger.Info("Loaded vector index", "collection", collectionName, "type", config.IndexType)
	}

	return nil
}

// CreateIndex creates a new vector index
func (m *Manager) CreateIndex(collectionName string, config *IndexConfig) (VectorIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if index already exists
	if _, exists := m.indices[collectionName]; exists {
		return nil, fmt.Errorf("index already exists for collection %s", collectionName)
	}

	// Initialize WAL writer if not already initialized
	if m.walWriter == nil {
		walWriter, err := wal.NewWALWriter(m.newWalFile(stringToInt32(collectionName)))
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL writer: %v", err)
		}
		m.walWriter = walWriter
	}

	// Create WAL entry
	createData := CreateIndexData{Config: config}
	dataBytes, err := json.Marshal(createData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal create index data: %v", err)
	}

	entry := &WALEntry{
		OpType:     WALOpCreateIndex,
		Collection: collectionName,
		Data:       dataBytes,
	}

	// Write to WAL
	entryBytes, err := encodeWALEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to encode WAL entry: %v", err)
	}

	if err := m.walWriter.Write([]byte(collectionName), entryBytes); err != nil {
		return nil, fmt.Errorf("failed to write to WAL: %v", err)
	}

	// Create index based on type
	var index VectorIndex
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

	// Write index config to file
	configData, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index config: %v", err)
	}
	configPath := path.Join(m.conf.Dir, "indexfile", collectionName+".conf")
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write index config: %v", err)
	}

	// Store index
	m.indices[collectionName] = index
	m.indexCh <- indexSaveItem{
		collectionName: collectionName,
		index:          index,
	}
	logger.Info("Created vector index", "collection", collectionName, "type", config.IndexType)
	return index, nil
}

// GetIndex retrieves an existing vector index
func (m *Manager) GetIndex(collectionName string) (VectorIndex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	index, exists := m.indices[collectionName]
	if !exists {
		return nil, fmt.Errorf("index not found for collection %s", collectionName)
	}

	return index, nil
}

// DeleteIndex removes a vector index
func (m *Manager) DeleteIndex(collectionName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	index, exists := m.indices[collectionName]
	if !exists {
		return fmt.Errorf("index not found for collection %s", collectionName)
	}

	// Close index
	if err := index.Close(); err != nil {
		return fmt.Errorf("failed to close index: %v", err)
	}

	// Delete index file
	indexPath := m.indexFiles(stringToInt32(collectionName))
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to delete index file", "error", err)
	}

	// Delete config file
	configPath := path.Join(m.conf.Dir, "indexfile", collectionName+".conf")
	if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to delete config file", "error", err)
	}

	// Delete WAL file
	walPath := m.newWalFile(stringToInt32(collectionName))
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to delete WAL file", "error", err)
	}

	// Remove from map
	delete(m.indices, collectionName)
	logger.Info("Deleted vector index and related files", "collection", collectionName)

	return nil
}

// Close closes all indices
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, index := range m.indices {
		if err := index.Close(); err != nil {
			logger.Error("Failed to close index", "collection", name, "error", err)
		}
	}

	m.indices = make(map[string]VectorIndex)
	close(m.stopCh)
	return nil
}

// monitorIndexSave monitors the index channel and saves the index to disk
func (m *Manager) monitorIndexSave() error {
	for {
		select {
		case indexItem := <-m.indexCh:
			// Save index to disk
			if err := indexItem.index.Save(m.indexFiles(stringToInt32(indexItem.collectionName))); err != nil {
				logger.Error("Failed to save index", "error", err)
				continue
			}
			logger.Info("Saved index to disk", "collection", indexItem.collectionName)
			// delete WAL file
			walPath := m.newWalFile(stringToInt32(indexItem.collectionName))
			if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
				logger.Error("Failed to delete WAL file", "error", err)
			}
		case <-m.stopCh:
			return nil
		}
	}
}

// AddVector adds a vector to the specified index with WAL support
func (m *Manager) AddVector(collectionName string, id string, vector []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get index
	index, exists := m.indices[collectionName]
	if !exists {
		return fmt.Errorf("index not found for collection %s", collectionName)
	}

	// Initialize WAL writer if not already initialized
	if m.walWriter == nil {
		walWriter, err := wal.NewWALWriter(m.newWalFile(stringToInt32(collectionName)))
		if err != nil {
			return fmt.Errorf("failed to create WAL writer: %v", err)
		}
		m.walWriter = walWriter
	}

	// Create WAL entry
	addData := AddVectorData{
		ID:     id,
		Vector: vector,
	}
	dataBytes, err := json.Marshal(addData)
	if err != nil {
		return fmt.Errorf("failed to marshal add vector data: %v", err)
	}

	entry := &WALEntry{
		OpType:     WALOpAddVector,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := index.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %v", err)
	}
	return nil
}

// AddVectorBatch adds multiple vectors to the specified index with WAL support
func (m *Manager) AddVectorBatch(collectionName string, ids []string, vectors [][]float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get index
	index, exists := m.indices[collectionName]
	if !exists {
		return fmt.Errorf("index not found for collection %s", collectionName)
	}

	// Initialize WAL writer if not already initialized
	if m.walWriter == nil {
		walWriter, err := wal.NewWALWriter(m.newWalFile(stringToInt32(collectionName)))
		if err != nil {
			return fmt.Errorf("failed to create WAL writer: %v", err)
		}
		m.walWriter = walWriter
	}

	// Create WAL entry
	addData := AddBatchData{
		IDs:     ids,
		Vectors: vectors,
	}
	dataBytes, err := json.Marshal(addData)
	if err != nil {
		return fmt.Errorf("failed to marshal add batch data: %v", err)
	}

	entry := &WALEntry{
		OpType:     WALOpAddBatch,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := index.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %v", err)
	}
	return nil
}

// DeleteVector deletes a vector from the specified index with WAL support
func (m *Manager) DeleteVector(collectionName string, id string) error {
	// TODO: fix delete vector
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get index
	index, exists := m.indices[collectionName]
	if !exists {
		return fmt.Errorf("index not found for collection %s", collectionName)
	}

	// Initialize WAL writer if not already initialized
	if m.walWriter == nil {
		walWriter, err := wal.NewWALWriter(m.newWalFile(stringToInt32(collectionName)))
		if err != nil {
			return fmt.Errorf("failed to create WAL writer: %v", err)
		}
		m.walWriter = walWriter
	}

	// Create WAL entry
	deleteData := DeleteVectorData{ID: id}
	dataBytes, err := json.Marshal(deleteData)
	if err != nil {
		return fmt.Errorf("failed to marshal delete vector data: %v", err)
	}

	entry := &WALEntry{
		OpType:     WALOpDeleteVector,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := index.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %v", err)
	}
	return nil
}

func stringToInt32(str string) int32 {
	i, _ := strconv.Atoi(str)
	return int32(i)
}

func (m *Manager) newWalFile(seq int32) string {
	return path.Join(m.conf.Dir, "walfile", "index", fmt.Sprintf("%d.wal", seq))
}

func (m *Manager) indexFiles(seq int32) string {
	return path.Join(m.conf.Dir, "indexfile", fmt.Sprintf("index_%d.idx", seq))
}
