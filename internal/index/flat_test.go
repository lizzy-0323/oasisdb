package index

import (
	"path/filepath"
	"testing"
)

// 生成测试用向量和ID
func generateFlatVectors(n, dim int) (ids []string, vecs [][]float32) {
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

// 测试 FlatIndex 的构建和检索
func TestFlatIndex_BuildAndSearch(t *testing.T) {
	dim := 4
	ids, vectors := generateFlatVectors(20, dim)
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: FLATIndex, // 假设你定义了 FLATIndex 常量
		Dimension: dim,
	}
	vIdx, err := newFlatIndex(cfg)
	if err != nil {
		t.Fatalf("failed to create Flat index: %v", err)
	}
	idx := vIdx.(*FlatIndex)
	if err := idx.Build(ids, vectors); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	// 检索与第7个向量最接近的向量
	query := vectors[6]
	res, err := idx.Search(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != ids[6] {
		t.Fatalf("unexpected top result: %+v", res.IDs)
	}
}

// 测试 FlatIndex 的保存和加载
func TestFlatIndex_SaveAndLoad(t *testing.T) {
	dim := 4
	ids, vectors := generateFlatVectors(15, dim)
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: FLATIndex,
		Dimension: dim,
	}
	vIdx, _ := newFlatIndex(cfg)
	idx := vIdx.(*FlatIndex)

	// 构建索引
	if err := idx.Build(ids, vectors); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	// 保存索引到临时文件
	tmpPath := filepath.Join(t.TempDir(), "flat.idx")
	if err := idx.Save(tmpPath); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// 新建一个索引实例并从磁盘加载
	vIdx2, _ := newFlatIndex(cfg)
	idx2 := vIdx2.(*FlatIndex)
	if err := idx2.Load(tmpPath); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// 检索并验证结果
	query := vectors[10]
	res, err := idx2.Search(query, 1)
	if err != nil {
		t.Fatalf("search after load failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != ids[10] {
		t.Fatalf("unexpected result after load, got %+v", res.IDs)
	}
}

// 测试 FlatIndex 的添加和删除
func TestFlatIndex_AddAndDelete(t *testing.T) {
	dim := 4
	cfg := &IndexConfig{
		SpaceType: L2Space,
		IndexType: FLATIndex,
		Dimension: dim,
	}
	vIdx, _ := newFlatIndex(cfg)
	idx := vIdx.(*FlatIndex)

	// 添加一个向量
	id := "test"
	vec := make([]float32, dim)
	vec[0] = 42
	if err := idx.Add(id, vec); err != nil {
		t.Fatalf("add failed: %v", err)
	}
	// 检索刚添加的向量
	res, err := idx.Search(vec, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.IDs) == 0 || res.IDs[0] != id {
		t.Fatalf("did not find added vector, got %v", res.IDs)
	}

	// 删除该向量
	if err := idx.Delete(id); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	// 检查删除后是否还能检索到
	res, err = idx.Search(vec, 1)
	if err != nil {
		t.Fatalf("search after delete failed: %v", err)
	}
	if len(res.IDs) > 0 && res.IDs[0] == id {
		t.Fatalf("vector was not deleted")
	}
}

