package index

import (
	"encoding/json"
)

// WALOpType represents the type of operation in WAL
type WALOpType string

const (
	WALOpCreateIndex  WALOpType = "create_index"
	WALOpAddVector    WALOpType = "add_vector"
	WALOpAddBatch     WALOpType = "add_batch"
	WALOpDeleteVector WALOpType = "delete_vector"
	WALOpBuildIndex   WALOpType = "build_index"
)

// WALEntry represents a single WAL log entry
type WALEntry struct {
	OpType     WALOpType       `json:"op_type"`
	Collection string          `json:"collection"`
	Data       json.RawMessage `json:"data"`
}

// CreateIndexData represents the data for creating an index
type CreateIndexData struct {
	Config *IndexConfig `json:"config"`
}

// AddVectorData represents the data for adding a vector
type AddVectorData struct {
	ID     string    `json:"id"`
	Vector []float32 `json:"vector"`
}

// AddBatchData represents the data for adding multiple vectors
type AddBatchData struct {
	IDs     []string    `json:"ids"`
	Vectors [][]float32 `json:"vectors"`
}

type BuildIndexData struct {
	IDs     []string    `json:"ids"`
	Vectors [][]float32 `json:"vectors"`
}

// DeleteVectorData represents the data for deleting a vector
type DeleteVectorData struct {
	ID string `json:"id"`
}

// encodeWALEntry encodes a WAL entry to bytes
func encodeWALEntry(entry *WALEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// decodeWALEntry decodes bytes to a WAL entry
func decodeWALEntry(data []byte) (*WALEntry, error) {
	var entry WALEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}
