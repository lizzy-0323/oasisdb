package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"oasisdb/internal/config"
	"oasisdb/internal/storage/wal"
	"oasisdb/pkg/errors"
	"oasisdb/pkg/logger"
)

// Manager manages vector index instances
type Manager struct {
	conf       *config.Config
	mu         sync.RWMutex
	indices    map[string]VectorIndex // collection name -> index
	indexCh    chan indexSaveItem
	stopCh     chan struct{}
	doneCh     chan struct{} // signal when monitorIndexSave is done
	stopSaveCh map[string]chan struct{}
	walWriter  *wal.WALWriter
}

type indexSaveItem struct {
	collectionName string
	index          VectorIndex
}

// NewIndexManager creates a new index manager
func NewIndexManager(conf *config.Config) (*Manager, error) {
	m := &Manager{
		conf:       conf,
		indices:    make(map[string]VectorIndex),
		indexCh:    make(chan indexSaveItem, 100),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		stopSaveCh: make(map[string]chan struct{}),
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
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// 2. Sort WAL files by creation time
	sort.Slice(entries, func(i, j int) bool {
		info1, err1 := entries[i].Info()
		info2, err2 := entries[j].Info()
		if err1 != nil || err2 != nil {
			return entries[i].Name() < entries[j].Name()
		}
		return info1.ModTime().Before(info2.ModTime())
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
			case IVFFLATIndex:
				index, err = newIVFIndex(createData.Config)
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
			if err := m.ApplyOpWithWal(walEntry); err != nil {
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
			if err := os.MkdirAll(path.Join(m.conf.Dir, "indexfile"), 0755); err != nil {
				return fmt.Errorf("failed to create index directory: %w", err)
			}
			return nil
		}
		return errors.ErrFailedToLoadIndex
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
		case IVFFLATIndex:
			index, err = newIVFIndex(&config)
		case FLATIndex:
			index, err = newFlatIndex(&config)
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

	// Set WAL writer
	if err := m.setWalWriter(collectionName); err != nil {
		return nil, err
	}

	// Create WAL entry
	createData := CreateIndexData{Config: config}
	dataBytes, err := json.Marshal(createData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal create index data: %w", err)
	}

	entry := &WALEntry{
		OpType:     WALOpCreateIndex,
		Collection: collectionName,
		Data:       dataBytes,
	}

	// Write to WAL
	if err := m.ApplyOpWithWal(entry); err != nil {
		return nil, err
	}

	// Create index based on type
	var index VectorIndex
	switch config.IndexType {
	case HNSWIndex:
		index, err = newHNSWIndex(config)
	case IVFFLATIndex:
		index, err = newIVFIndex(config)
	default:
		return nil, errors.ErrUnsupportedIndexType
	}

	if err != nil {
		return nil, errors.ErrFailedToCreateIndex
	}

	// Write index config to file
	configData, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index config: %w", err)
	}
	configPath := path.Join(m.conf.Dir, "indexfile", collectionName+".conf")
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write index config: %w", err)
	}

	// Store index
	m.indices[collectionName] = index
	m.indexCh <- indexSaveItem{
		collectionName: collectionName,
		index:          index,
	}
	m.stopSaveCh[collectionName] = make(chan struct{})
	logger.Info("Created vector index", "collection", collectionName, "type", config.IndexType)
	return index, nil
}

// GetIndex retrieves an existing vector index
func (m *Manager) GetIndex(collectionName string) (VectorIndex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	index, exists := m.indices[collectionName]
	if !exists {
		return nil, errors.ErrIndexNotFound
	}

	return index, nil
}

// DeleteIndex removes a vector index
func (m *Manager) DeleteIndex(collectionName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get index instance
	index, exists := m.indices[collectionName]
	if !exists {
		return errors.ErrIndexNotFound
	}

	// First stop any ongoing save operations
	if ch, ok := m.stopSaveCh[collectionName]; ok {
		close(ch)
		delete(m.stopSaveCh, collectionName)
	}

	// Remove from map to prevent new operations
	delete(m.indices, collectionName)

	// Close index
	if err := index.Close(); err != nil {
		return fmt.Errorf("failed to close index: %w", err)
	}

	// Delete files
	indexPath := m.newIndexFile(stringToInt32(collectionName))
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to delete index file", "error", err)
	}

	walPath := m.newWalFile(stringToInt32(collectionName))
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to delete WAL file", "error", err)
	}

	logger.Info("Deleted vector index and related files", "collection", collectionName)
	return nil
}

// Close closes all indices
func (m *Manager) Close() error {
	// First signal monitor to stop and wait for it to finish current operations
	close(m.stopCh)
	<-m.doneCh

	// Now it's safe to close indices
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, index := range m.indices {
		if err := index.Close(); err != nil {
			logger.Error("Failed to close index", "collection", name, "error", err)
		}
	}

	m.indices = make(map[string]VectorIndex)
	return nil
}

// monitorIndexSave monitors the index channel and saves the index to disk
func (m *Manager) monitorIndexSave() error {
	defer close(m.doneCh)
	for {
		select {
		case indexItem := <-m.indexCh:
			m.mu.RLock()
			stopCh, hasStopCh := m.stopSaveCh[indexItem.collectionName]
			_, exists := m.indices[indexItem.collectionName]
			m.mu.RUnlock()

			// Skip if index is being deleted or doesn't exist
			if !exists {
				logger.Info("Skip saving deleted index", "collection", indexItem.collectionName)
				continue
			}

			// Check if save operation should be stopped
			if hasStopCh {
				select {
				case <-stopCh:
					logger.Info("Skip saving index due to deletion", "collection", indexItem.collectionName)
					continue
				default:
				}
			}

			// Save index to disk
			if err := indexItem.index.Save(m.newIndexFile(stringToInt32(indexItem.collectionName))); err != nil {
				logger.Error("Failed to save index", "error", err)
				continue
			}
			logger.Info("Saved index to disk", "collection", indexItem.collectionName)

			// Delete WAL file
			walPath := m.newWalFile(stringToInt32(indexItem.collectionName))
			if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
				logger.Error("Failed to delete WAL file", "error", err)
			}

		case <-m.stopCh:
			logger.Info("Stop saving index")
			return nil
		}
	}
}

// AddVector adds a vector to the specified index with WAL support
func (m *Manager) AddVector(collectionName string, id string, vector []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set WAL writer
	if err := m.setWalWriter(collectionName); err != nil {
		return err
	}

	// Create WAL entry
	addData := AddVectorData{
		ID:     id,
		Vector: vector,
	}
	dataBytes, err := json.Marshal(addData)
	if err != nil {
		return fmt.Errorf("failed to marshal add vector data: %w", err)
	}

	entry := &WALEntry{
		OpType:     WALOpAddVector,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := m.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %w", err)
	}
	return nil
}

// BuildIndex builds an index with WAL support
func (m *Manager) BuildIndex(collectionName string, ids []string, vectors [][]float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set WAL writer
	if err := m.setWalWriter(collectionName); err != nil {
		return err
	}

	// Create WAL entry
	buildData := BuildIndexData{
		IDs:     ids,
		Vectors: vectors,
	}
	dataBytes, err := json.Marshal(buildData)
	if err != nil {
		return fmt.Errorf("failed to marshal build index data: %w", err)
	}

	entry := &WALEntry{
		OpType:     WALOpBuildIndex,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := m.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %w", err)
	}
	return nil
}

// AddVectorBatch adds multiple vectors to the specified index with WAL support
func (m *Manager) AddVectorBatch(collectionName string, ids []string, vectors [][]float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set WAL writer
	if err := m.setWalWriter(collectionName); err != nil {
		return err
	}

	// Create WAL entry
	addData := AddBatchData{
		IDs:     ids,
		Vectors: vectors,
	}
	dataBytes, err := json.Marshal(addData)
	if err != nil {
		return fmt.Errorf("failed to marshal add batch data: %w", err)
	}

	entry := &WALEntry{
		OpType:     WALOpAddBatch,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := m.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %w", err)
	}
	return nil
}

// DeleteVector deletes a vector from the specified index with WAL support
func (m *Manager) DeleteVector(collectionName string, id string) error {
	// TODO: fix delete vector
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set WAL writer
	if err := m.setWalWriter(collectionName); err != nil {
		return err
	}

	// Create WAL entry
	deleteData := DeleteVectorData{ID: id}
	dataBytes, err := json.Marshal(deleteData)
	if err != nil {
		return fmt.Errorf("failed to marshal delete vector data: %w", err)
	}

	entry := &WALEntry{
		OpType:     WALOpDeleteVector,
		Collection: collectionName,
		Data:       dataBytes,
	}

	if err := m.ApplyOpWithWal(entry); err != nil {
		return fmt.Errorf("failed to apply WAL entry: %w", err)
	}
	return nil
}

func (m *Manager) ApplyOpWithWal(entry *WALEntry) error {
	entryBytes, err := encodeWALEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode WAL entry: %w", err)
	}

	if err := m.walWriter.Write([]byte(entry.Collection), entryBytes); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	if entry.OpType == WALOpCreateIndex {
		return nil
	}

	index, exists := m.indices[entry.Collection]
	if !exists {
		return fmt.Errorf("index not found for collection %s", entry.Collection)
	}

	switch entry.OpType {
	case WALOpBuildIndex:
		var data BuildIndexData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("failed to unmarshal build index data: %w", err)
		}
		return index.Build(data.IDs, data.Vectors)

	case WALOpAddVector:
		var data AddVectorData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("failed to unmarshal add vector data: %w", err)
		}
		return index.Add(data.ID, data.Vector)

	case WALOpAddBatch:
		var data AddBatchData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("failed to unmarshal add batch data: %w", err)
		}
		return index.AddBatch(data.IDs, data.Vectors)

	case WALOpDeleteVector:
		var data DeleteVectorData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("failed to unmarshal delete vector data: %w", err)
		}
		return index.Delete(data.ID)

	default:
		return fmt.Errorf("unsupported WAL operation type: %s", entry.OpType)
	}
}

func (m *Manager) setWalWriter(collectionName string) error {
	walWriter, err := wal.NewWALWriter(m.newWalFile(stringToInt32(collectionName)))
	if err != nil {
		return fmt.Errorf("failed to create WAL writer: %w", err)
	}
	m.walWriter = walWriter
	return nil
}

func (m *Manager) newWalFile(seq int32) string {
	return path.Join(m.conf.Dir, "walfile", "index", fmt.Sprintf("%d.wal", seq))
}

func (m *Manager) newIndexFile(seq int32) string {
	return path.Join(m.conf.Dir, "indexfile", fmt.Sprintf("index_%d.idx", seq))
}
