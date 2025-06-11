package index

import (
	"fmt"
	"strconv"

	"oasisdb/internal/engine/go_api/ivf"
)

type ivfIndex struct {
	config *IndexConfig
	index  *ivf.IVFIndex
}

func newIVFIndex(config *IndexConfig) (VectorIndex, error) {
	// Get IVF-specific parameters
	params := config.Parameters
	nlist, ok := params["nlist"].(float64)
	if !ok {
		nlist = 100 // default value
	}
	if config.Dimension <= 0 {
		return nil, fmt.Errorf("invalid dimension: %d", config.Dimension)
	}
	if nlist <= 0 {
		return nil, fmt.Errorf("invalid nlist: %f", nlist)
	}

	// Create IVF index with dimension and nlist from config
	index, err := ivf.NewIVFIndex(uint32(config.Dimension), uint32(nlist))
	if err != nil {
		return nil, fmt.Errorf("failed to create IVF index: %v", err)
	}

	return &ivfIndex{config: config, index: index}, nil
}

func (i *ivfIndex) Train(data []float32) error {
	return i.index.Train(data)
}

func (i *ivfIndex) Add(docID string, vector []float32) error {
	// Convert docID to int64
	id, err := strconv.ParseInt(docID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid document ID: %v", err)
	}

	// Add vector to index
	if err := i.index.Add(vector, id); err != nil {
		return fmt.Errorf("failed to add vector: %v", err)
	}

	return nil
}

func (i *ivfIndex) AddBatch(docIDs []string, vectors [][]float32) error {
	// Convert docIDs to int64s
	ids := make([]int64, len(docIDs))
	for j, docID := range docIDs {
		id, err := strconv.ParseInt(docID, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid document ID at index %d: %v", j, err)
		}
		ids[j] = id
	}

	// Get number of goroutines from parameters
	params := i.config.Parameters
	numGoroutines, ok := params["num_goroutines"].(float64)
	if !ok {
		numGoroutines = 4 // default value
	}

	// Add vectors to index
	return i.index.AddItems(vectors, ids, int(numGoroutines))
}

func (i *ivfIndex) Search(query []float32, k int) (*SearchResult, error) {
	// Search k nearest neighbors
	neighbors, distances, err := i.index.Search(query, uint32(k))
	if err != nil {
		return nil, fmt.Errorf("failed to search index: %v", err)
	}

	// Convert neighbors to string docIDs
	docIDs := make([]string, len(neighbors))
	for j, id := range neighbors {
		docIDs[j] = strconv.FormatInt(id, 10)
	}

	return &SearchResult{
		IDs:       docIDs,
		Distances: distances,
	}, nil
}

func (i *ivfIndex) Delete(docID string) error {
	// Convert docID to int64
	id, err := strconv.ParseInt(docID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid document ID: %v", err)
	}

	// Remove vector from index
	if err := i.index.Remove([]int64{id}); err != nil {
		return fmt.Errorf("failed to delete vector: %v", err)
	}

	return nil
}

func (i *ivfIndex) DeleteBatch(docIDs []string) error {
	// Convert docIDs to int64s
	ids := make([]int64, len(docIDs))
	for j, docID := range docIDs {
		id, err := strconv.ParseInt(docID, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid document ID at index %d: %v", j, err)
		}
		ids[j] = id
	}

	// Remove vectors from index
	if err := i.index.Remove(ids); err != nil {
		return fmt.Errorf("failed to delete vectors: %v", err)
	}

	return nil
}

func (i *ivfIndex) Save(indexPath string) error {
	// Save index to file
	return i.index.Save(indexPath)
}

func (i *ivfIndex) Load(indexPath string) error {
	// Load index from file
	index, err := ivf.LoadIVFIndex(indexPath)
	if err != nil {
		return err
	}

	i.index = index
	return nil
}

func (i *ivfIndex) Close() error {
	// Free index resources
	if i.index != nil {
		i.index.Free()
		i.index = nil
	}

	return nil
}
