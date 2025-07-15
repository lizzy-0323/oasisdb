package index

import (
	"path/filepath"
	"testing"
)

func generateVectors(n, dim int) (ids []string, vecs [][]float32) {
	ids = make([]string, n)
	vecs = make([][]float32, n)
	for i := 0; i < n; i++ {
		ids[i] = string(rune('0' + i + 1))
		v := make([]float32, dim)
		v[0] = float32(i) // linearly separable along first dimension
		vecs[i] = v
	}
	return
}

func TestIVFIndex_BuildAndSearch(t *testing.T) {
	dim := 4
	ids, vectors := generateVectors(20, dim)
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFFLATIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(5),
			"nprobe": float64(2),
		},
	}
	vIdx, err := newIVFIndex(cfg)
	idx := vIdx.(*ivfIndex)
	if err != nil {
		t.Fatalf("failed to create IVF index: %v", err)
	}
	if err := idx.Build(ids, vectors); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	// search vector close to 7 (index 6)
	query := vectors[6]
	res, err := idx.Search(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != ids[6] {
		t.Fatalf("unexpected top result: %+v", res.IDs)
	}
}

func TestIVFIndex_SaveAndLoad(t *testing.T) {
	dim := 4
	ids, vectors := generateVectors(15, dim)
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFFLATIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(5),
			"nprobe": float64(2),
		},
	}
	vIdx, _ := newIVFIndex(cfg)
	idx := vIdx.(*ivfIndex)

	// build index with sample vectors
	if err := idx.Build(ids, vectors); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	// save the index to a temp file inside the test temp dir
	tmpPath := filepath.Join(t.TempDir(), "ivf.idx")
	if err := idx.Save(tmpPath); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// create a new empty index and load from disk
	vIdx2, _ := newIVFIndex(cfg)
	idx2 := vIdx2.(*ivfIndex)
	if err := idx2.Load(tmpPath); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// search using the second index and verify results match
	query := vectors[10]
	res, err := idx2.Search(query, 1)
	if err != nil {
		t.Fatalf("search after load failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != ids[10] {
		t.Fatalf("unexpected result after load, got %+v", res.IDs)
	}
}

func TestIVFIndex_AddAfterTrain(t *testing.T) {
	dim := 4
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFFLATIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(4),
			"nprobe": float64(2),
		},
	}
	vIdx, _ := newIVFIndex(cfg)
	idx := vIdx.(*ivfIndex)

	// train with 5 vectors
	_, vectors := generateVectors(5, dim)
	if err := idx.Train(vectors); err != nil {
		t.Fatalf("train failed: %v", err)
	}

	// add new vector
	id := "100"
	vec := make([]float32, dim)
	vec[0] = 100
	if err := idx.Add(id, vec); err != nil {
		t.Fatalf("add failed: %v", err)
	}
	res, err := idx.Search(vec, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != id {
		t.Fatalf("did not find added vector, got %v", res.IDs)
	}
}
