package index

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"oasisdb/internal/engine/go_api/hnsw"
)

type hnswIndex struct {
	index     *hnsw.Index
	dimension int
	config    *Config
}

func newHNSWIndex(config *Config) (VectorIndex, error) {
	if config.Dimension <= 0 {
		return nil, fmt.Errorf("invalid dimension: %d", config.Dimension)
	}

	// Get HNSW specific parameters
	M := uint32(16)                // default M
	efConstruction := uint32(200)  // default efConstruction
	maxElements := uint32(1000000) // default maxElements

	if v, ok := config.Parameters["M"]; ok {
		if m, ok := v.(float64); ok {
			M = uint32(m)
		}
	}
	if v, ok := config.Parameters["efConstruction"]; ok {
		if ef, ok := v.(float64); ok {
			efConstruction = uint32(ef)
		}
	}
	if v, ok := config.Parameters["maxElements"]; ok {
		if max, ok := v.(float64); ok {
			maxElements = uint32(max)
		}
	}

	// Create HNSW index
	index := hnsw.NewIndex(
		uint32(config.Dimension),
		maxElements,
		M,
		efConstruction,
		string(config.SpaceType),
	)
	if index == nil {
		return nil, fmt.Errorf("failed to create HNSW index")
	}

	return &hnswIndex{
		index:     index,
		dimension: config.Dimension,
		config:    config,
	}, nil
}

func (h *hnswIndex) Add(ctx context.Context, id string, vector []float32) error {
	if len(vector) != h.dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.dimension, len(vector))
	}
	return h.index.AddPoint(vector, uint32(stringToID(id)))
}

func (h *hnswIndex) AddBatch(ctx context.Context, ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return fmt.Errorf("ids and vectors length mismatch")
	}

	// Convert string IDs to uint32
	uint32IDs := make([]uint32, len(ids))
	for i, id := range ids {
		uint32IDs[i] = uint32(stringToID(id))
	}

	return h.index.AddItems(vectors, uint32IDs, 4) // Use 4 goroutines for batch insert
}

func (h *hnswIndex) Delete(ctx context.Context, id string) error {
	return h.index.MarkDeleted(uint32(stringToID(id)))
}

func (h *hnswIndex) DeleteBatch(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := h.Delete(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (h *hnswIndex) Search(ctx context.Context, vector []float32, k int) (*SearchResult, error) {
	if len(vector) != h.dimension {
		return nil, fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.dimension, len(vector))
	}

	ids, distances, err := h.index.SearchKNN(vector, k)
	if err != nil {
		return nil, err
	}

	// Convert uint32 IDs back to strings
	strIDs := make([]string, len(ids))
	for i, id := range ids {
		strIDs[i] = idToString(int64(id))
	}

	return &SearchResult{
		IDs:       strIDs,
		Distances: distances,
	}, nil
}

func (h *hnswIndex) Load(ctx context.Context, reader io.Reader) error {
	// TODO: implement loading from reader
	return nil
}

func (h *hnswIndex) Save(ctx context.Context, writer io.Writer) error {
	// TODO: implement saving to writer
	return nil
}

func (h *hnswIndex) Close() error {
	h.index.Unload()
	return nil
}

// stringToID converts a string ID to int64
func stringToID(id string) int64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return int64(h.Sum64())
}

// idToString converts an int64 ID back to string
func idToString(id int64) string {
	return fmt.Sprintf("%d", id)
}
