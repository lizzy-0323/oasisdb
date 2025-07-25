package index

import (
	"fmt"
	"oasisdb/internal/engine/go_api/hnsw"
	"oasisdb/pkg/errors"
)

type hnswIndex struct {
	index  *hnsw.Index
	config *IndexConfig
}

func newHNSWIndex(config *IndexConfig) (VectorIndex, error) {
	if config.Dimension <= 0 {
		return nil, errors.ErrInvalidDimension
	}

	// Get HNSW specific parameters
	M := uint32(DEFAULT_M)                            // default M
	efConstruction := uint32(DEFAULT_EF_CONSTRUCTION) // default efConstruction
	maxElements := uint32(DEFAULT_MAX_ELEMENTS)       // default maxElements

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
		return nil, errors.ErrFailedToCreateIndex
	}

	return &hnswIndex{
		index:  index,
		config: config,
	}, nil
}

func (h *hnswIndex) Add(id string, vector []float32) error {
	if len(vector) != h.config.Dimension {
		return errors.ErrInvalidDimension
	}
	return h.index.AddPoint(vector, uint32(stringToID(id)))
}

func (h *hnswIndex) Build(ids []string, vectors [][]float32) error {
	return h.AddBatch(ids, vectors)
}

func (h *hnswIndex) AddBatch(ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return errors.ErrInvalidDimension
	}

	// Convert string IDs to uint32
	uint32IDs := make([]uint32, len(ids))
	for i, id := range ids {
		uint32IDs[i] = uint32(stringToID(id))
	}

	return h.index.AddItems(vectors, uint32IDs, DEFAULT_BUILD_THREADS) // Use 4 goroutines for batch insert
}

func (h *hnswIndex) Delete(id string) error {
	// 1. ensure id exists
	if h.index.GetVectorByLabel(uint32(stringToID(id)), int(h.config.Dimension)) == nil {
		return fmt.Errorf("id %s does not exist", id)
	}
	return h.index.MarkDeleted(uint32(stringToID(id)))
}

func (h *hnswIndex) Search(vector []float32, k int) (*SearchResult, error) {
	if len(vector) != h.config.Dimension {
		return nil, errors.ErrInvalidDimension
	}

	ids, distances, err := h.index.SearchKNN(vector, k)
	if err != nil {
		return nil, err
	}
	// Reverse the order of results
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
		distances[i], distances[j] = distances[j], distances[i]
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

func (h *hnswIndex) Load(filePath string) error {
	var spaceType string
	if h.config.SpaceType == IPSpace {
		spaceType = "ip"
	} else {
		spaceType = "l2"
	}

	index, err := hnsw.LoadIndex(filePath, int(h.config.Dimension), spaceType)
	if err != nil {
		return errors.ErrFailedToLoadIndex
	}

	if h.index != nil {
		h.index.Unload()
	}

	// Update index
	h.index = index
	return nil
}

func (h *hnswIndex) Save(filePath string) error {
	if h.index == nil {
		return fmt.Errorf("index is not initialized")
	}
	return h.index.SaveIndex(filePath)
}

func (h *hnswIndex) Close() error {
	if h.index == nil {
		return nil
	}
	h.index.Unload()
	return nil
}

func (h *hnswIndex) SetParams(params map[string]any) error {
	if len(params) == 0 {
		return errors.ErrEmptyParameter
	}

	for key, val := range params {
		switch key {
		case "efsearch":
			var ival int
			switch v := val.(type) {
			case int:
				ival = v
			case float64:
				ival = int(v)
			default:
				return errors.ErrInvalidParameter
			}
			if err := h.SetEfSearch(ival); err != nil {
				return err
			}
		default:
			// unknown parameter
			return errors.ErrInvalidParameter
		}
	}
	return nil
}

func (h *hnswIndex) SetEfSearch(ef int) error {
	if h.index == nil {
		return fmt.Errorf("index is not initialized")
	}
	return h.index.SetEf(ef)
}

// stringToID converts a string ID to int64
func stringToID(id string) int64 {
	var n int64
	_, err := fmt.Sscanf(id, "%d", &n)
	if err != nil {
		return 0
	}
	return n
}

// idToString converts an int64 ID back to string
func idToString(id int64) string {
	return fmt.Sprintf("%d", id)
}
