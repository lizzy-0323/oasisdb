package index

// SpaceType represents the distance metric type
type SpaceType string

const (
	L2Space  SpaceType = "l2"
	IPSpace  SpaceType = "ip"
	CosSpace SpaceType = "cos"
)

// IndexConfig represents index configuration
type IndexConfig struct {
	SpaceType  SpaceType              // distance metric type
	Dimension  int                    // vector dimension
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
	Add(id string, vector []float32) error

	// AddBatch adds multiple vectors to the index
	AddBatch(ids []string, vectors [][]float32) error

	// Delete removes a vector from the index
	Delete(id string) error

	// Search performs a k-NN search
	Search(vector []float32, k int) (*SearchResult, error)

	// ToBytes converts the index to a byte slice
	ToBytes() []byte

	// Load loads the index from disk
	Load(filePath string) error

	// Save saves the index to disk
	Save(filePath string) error

	// Close closes the index and releases resources
	Close() error

	// ApplyOpWithWal applies a WAL entry to the index
	ApplyOpWithWal(entry *WALEntry) error
}
