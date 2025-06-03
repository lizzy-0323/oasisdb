package index

import (
	"context"
	"io"
)

// SpaceType represents the distance metric type
type SpaceType string

const (
	L2Space  SpaceType = "l2"
	IPSpace  SpaceType = "ip"
	CosSpace SpaceType = "cos"
)

// Config represents index configuration
type Config struct {
	Dimension  int                    // vector dimension
	SpaceType  SpaceType              // distance metric type
	IndexType  string                 // index type (e.g., "hnsw", "ivf")
	Parameters map[string]interface{} // index-specific parameters
}

// SearchResult represents a search result
type SearchResult struct {
	IDs       []string  // document IDs
	Distances []float32 // distances to query vector
}

// VectorIndex represents a vector index
type VectorIndex interface {
	// Add adds a vector to the index
	Add(ctx context.Context, id string, vector []float32) error

	// AddBatch adds multiple vectors to the index
	AddBatch(ctx context.Context, ids []string, vectors [][]float32) error

	// Delete removes a vector from the index
	Delete(ctx context.Context, id string) error

	// Search performs a k-NN search
	Search(ctx context.Context, vector []float32, k int) (*SearchResult, error)

	// Load loads the index from disk
	Load(ctx context.Context, reader io.Reader) error

	// Save saves the index to disk
	Save(ctx context.Context, writer io.Writer) error

	// Close closes the index and releases resources
	Close() error
}
