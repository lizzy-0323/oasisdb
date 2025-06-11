package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIVFIndex(t *testing.T) {
	// Create index configuration
	config := &IndexConfig{
		IndexType: IVFIndex,
		Dimension: 8,
		Parameters: map[string]interface{}{
			"nlist": float64(4), // 使用较小的nlist便于测试
		},
	}

	// Create index
	idx, err := newIVFIndex(config)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}
	defer idx.Close()

	// Prepare test data
	trainData := []float32{
		1, 0, 0, 0, 0, 0, 0, 0,
		0, 1, 0, 0, 0, 0, 0, 0,
		0, 0, 1, 0, 0, 0, 0, 0,
		0, 0, 0, 1, 0, 0, 0, 0,
	}

	// Train index
	if err := idx.Train(trainData); err != nil {
		t.Fatalf("Failed to train index: %v", err)
	}

	// Test adding single vector
	vector1 := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	if err := idx.Add("1", vector1); err != nil {
		t.Fatalf("Failed to add single vector: %v", err)
	}

	// Test adding multiple vectors
	vectors := [][]float32{
		{0, 1, 0, 0, 0, 0, 0, 0},
		{0, 0, 1, 0, 0, 0, 0, 0},
	}
	docIDs := []string{"2", "3"}
	if err := idx.AddBatch(docIDs, vectors); err != nil {
		t.Fatalf("Failed to add multiple vectors: %v", err)
	}

	// Test search
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	result, err := idx.Search(query, 2)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}

	// Verify search results
	if len(result.IDs) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result.IDs))
	}
	if result.IDs[0] != "1" { // 最近邻应该是ID为1的向量
		t.Errorf("Expected first result to be '1', got %s", result.IDs[0])
	}

	// Test delete single vector
	if err := idx.Delete("1"); err != nil {
		t.Fatalf("Failed to delete vector: %v", err)
	}

	// Test save and load
	tmpFile := "test_ivf_index.bin"
	defer os.Remove(tmpFile)

	// Save index
	if err := idx.Save(tmpFile); err != nil {
		t.Fatalf("Failed to save index: %v", err)
	}

	// Create new index for loading
	newIdx, err := newIVFIndex(config)
	if err != nil {
		t.Fatalf("Failed to create new IVF index: %v", err)
	}
	defer newIdx.Close()

	// Load index
	if err := newIdx.Load(tmpFile); err != nil {
		t.Fatalf("Failed to load index: %v", err)
	}

	// Verify loaded index by performing a search
	result, err = newIdx.Search(query, 1)
	if err != nil {
		t.Fatalf("Failed to search loaded index: %v", err)
	}
}

func TestIVFIndexInvalidConfig(t *testing.T) {
	// Test with invalid dimension
	config := &IndexConfig{
		IndexType: IVFIndex,
		Dimension: 0,
		Parameters: map[string]interface{}{
			"nlist": float64(4),
		},
	}

	_, err := newIVFIndex(config)
	assert.Error(t, err)

	// Test with missing nlist parameter
	config = &IndexConfig{
		IndexType:  IVFIndex,
		Dimension:  8,
		Parameters: map[string]interface{}{},
	}

	idx, err := newIVFIndex(config)
	if err != nil {
		t.Errorf("Expected success with default nlist, got error: %v", err)
	}
	if idx != nil {
		idx.Close()
	}
}

func TestIVFIndexInvalidOperations(t *testing.T) {
	config := &IndexConfig{
		IndexType: IVFIndex,
		Dimension: 8,
		Parameters: map[string]interface{}{
			"nlist": float64(4),
		},
	}

	idx, err := newIVFIndex(config)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}
	defer idx.Close()

	// Test adding vector with wrong dimension
	wrongDimVector := []float32{1, 0, 0} // 维度不正确
	err = idx.Add("1", wrongDimVector)
	assert.Error(t, err)

	// Test adding vector with invalid ID
	err = idx.Add("", []float32{1, 0, 0, 0, 0, 0, 0, 0})
	assert.Error(t, err)

	// Test searching before training
	_, err = idx.Search([]float32{1, 0, 0, 0, 0, 0, 0, 0}, 1)
	assert.Error(t, err)
}
