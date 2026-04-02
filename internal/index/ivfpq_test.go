package index

import (
	"path/filepath"
	"testing"
)

func generatePQVectors(n, dim int) (ids []string, vecs [][]float32) {
	ids = make([]string, n)
	vecs = make([][]float32, n)
	for i := 0; i < n; i++ {
		ids[i] = string(rune('0' + i + 1))
		v := make([]float32, dim)
		v[0] = float32(i)
		vecs[i] = v
	}
	return
}

func TestIVFPQIndex_BuildAndSearch(t *testing.T) {
	dim := 8
	ids, vectors := generatePQVectors(20, dim)
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFPQIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(5),
			"nprobe": float64(2),
			"m":      float64(4),
			"nbits":  float64(8),
		},
	}
	vIdx, err := newIVFPQIndex(cfg)
	idx := vIdx.(*ivfpqIndex)
	if err != nil {
		t.Fatalf("failed to create IVFPQ index: %v", err)
	}
	if err := idx.Build(ids, vectors); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	query := vectors[6]
	res, err := idx.Search(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != ids[6] {
		t.Fatalf("unexpected top result: %+v", res.IDs)
	}
}

func TestIVFPQIndex_SaveAndLoad(t *testing.T) {
	dim := 8
	ids, vectors := generatePQVectors(15, dim)
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFPQIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(5),
			"nprobe": float64(2),
			"m":      float64(4),
			"nbits":  float64(8),
		},
	}
	vIdx, _ := newIVFPQIndex(cfg)
	idx := vIdx.(*ivfpqIndex)

	if err := idx.Build(ids, vectors); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	tmpPath := filepath.Join(t.TempDir(), "ivfpq.idx")
	if err := idx.Save(tmpPath); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	vIdx2, _ := newIVFPQIndex(cfg)
	idx2 := vIdx2.(*ivfpqIndex)
	if err := idx2.Load(tmpPath); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	query := vectors[10]
	res, err := idx2.Search(query, 1)
	if err != nil {
		t.Fatalf("search after load failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != ids[10] {
		t.Fatalf("unexpected result after load, got %+v", res.IDs)
	}
}

func TestIVFPQIndex_AddAfterTrain(t *testing.T) {
	dim := 8
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFPQIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(4),
			"nprobe": float64(2),
			"m":      float64(4),
			"nbits":  float64(8),
		},
	}
	vIdx, _ := newIVFPQIndex(cfg)
	idx := vIdx.(*ivfpqIndex)

	_, vectors := generatePQVectors(5, dim)
	if err := idx.Train(vectors); err != nil {
		t.Fatalf("train failed: %v", err)
	}

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

func TestIVFPQIndex_AddAndDelete(t *testing.T) {
	dim := 8
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFPQIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(4),
			"nprobe": float64(2),
			"m":      float64(4),
			"nbits":  float64(8),
		},
	}
	vIdx, _ := newIVFPQIndex(cfg)
	idx := vIdx.(*ivfpqIndex)

	// train with a few vectors first
	_, baseVectors := generatePQVectors(10, dim)
	if err := idx.Train(baseVectors); err != nil {
		t.Fatalf("train failed: %v", err)
	}

	id := "test"
	vec := make([]float32, dim)
	vec[0] = 42
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

	if err := idx.Delete(id); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	res, err = idx.Search(vec, 1)
	if err != nil {
		t.Fatalf("search after delete failed: %v", err)
	}
	if len(res.IDs) > 0 && res.IDs[0] == id {
		t.Fatalf("vector was not deleted")
	}
}

func TestIVFPQIndex_ParamsValidation(t *testing.T) {
	dim := 8
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFPQIndex,
		Dimension: dim,
		Parameters: map[string]interface{}{
			"nlist":  float64(4),
			"nprobe": float64(2),
			"m":      float64(4),
			"nbits":  float64(8),
		},
	}
	vIdx, _ := newIVFPQIndex(cfg)
	idx := vIdx.(*ivfpqIndex)

	if err := idx.SetNProbe(0); err == nil {
		t.Fatalf("expected error for nprobe=0")
	}
	if err := idx.SetNProbe(5); err == nil {
		t.Fatalf("expected error for nprobe > nlist")
	}
	if err := idx.SetNProbe(3); err != nil {
		t.Fatalf("unexpected error for valid nprobe: %v", err)
	}
}

func TestIVFPQIndex_NonDivisibleDim(t *testing.T) {
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: IVFPQIndex,
		Dimension: 5,
		Parameters: map[string]interface{}{
			"m": float64(4),
		},
	}
	_, err := newIVFPQIndex(cfg)
	if err == nil {
		t.Fatalf("expected error when dimension is not divisible by m")
	}
}
