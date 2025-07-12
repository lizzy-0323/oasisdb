package index

// Target:
// 1. delete the helper function
// 2. put config *IndexConfig(hnsw.go and ivf.go)

import (
	"encoding/gob"
	pkgerrors "oasisdb/pkg/errors"
	"os"
	"sort"
)

type FlatIndex struct {
	Dim     int
	Data    []float32
	Ids     []string
	IdToIdx map[string]int
	config  *IndexConfig
}

// 构造函数
func newFlatIndex(config *IndexConfig) (VectorIndex, error) {
	if config.Dimension <= 0 {
		return nil, pkgerrors.ErrInvalidDimension
	}
	return &FlatIndex{
		Dim:     config.Dimension,     // 设置向量维度
		Ids:     make([]string, 0),    // 初始化ID切片
		Data:    make([]float32, 0),   // 初始化向量连续内存
		IdToIdx: make(map[string]int), // 初始化ID到下标的映射
		config:  config,               // 保存配置
	}, nil
}

// Add 添加单个向量
func (f *FlatIndex) Add(id string, vector []float32) error {

	if len(vector) != f.Dim {
		return pkgerrors.ErrInvalidDimension
	}
	if _, exists := f.IdToIdx[id]; exists {
		return pkgerrors.ErrDocumentExists
	}
	f.Ids = append(f.Ids, id)
	f.Data = append(f.Data, vector...) // 将向量展开并添加到连续内存
	f.IdToIdx[id] = len(f.Ids) - 1
	return nil
}

// AddBatch 批量添加向量
func (f *FlatIndex) AddBatch(ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return pkgerrors.ErrMisMatchKeysAndValues
	}
	for i := range ids {
		if err := f.Add(ids[i], vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

// Build 构建索引（flat 实现直接批量添加）
func (f *FlatIndex) Build(ids []string, vectors [][]float32) error {

	if len(ids) != len(vectors) {
		return pkgerrors.ErrMisMatchKeysAndValues
	}
	f.Ids = make([]string, len(ids))
	f.Data = make([]float32, 0) // 清空连续内存
	f.IdToIdx = make(map[string]int)
	for i := range ids {
		if len(vectors[i]) != f.Dim {
			return pkgerrors.ErrInvalidDimension
		}
		f.Ids[i] = ids[i]
		f.Data = append(f.Data, vectors[i]...) // 将向量展开并添加到连续内存
		f.IdToIdx[ids[i]] = i
	}
	return nil
}

// Delete 删除指定ID的向量
func (f *FlatIndex) Delete(id string) error {

	idx, exists := f.IdToIdx[id]
	if !exists {
		return pkgerrors.ErrDocumentNotFound
	}
	// 删除向量和ID
	f.Ids = append(f.Ids[:idx], f.Ids[idx+1:]...)
	// 从连续内存中删除向量
	start := idx * f.Dim
	end := start + f.Dim
	f.Data = append(f.Data[:start], f.Data[end:]...)
	delete(f.IdToIdx, id)
	// 更新idToIdx
	for i := idx; i < len(f.Ids); i++ {
		f.IdToIdx[f.Ids[i]] = i
	}
	return nil
}

// Search 进行k近邻暴力检索
func (f *FlatIndex) Search(vector []float32, k int) (*SearchResult, error) {

	if len(vector) != f.Dim {
		return nil, pkgerrors.ErrInvalidDimension
	}
	type pair struct {
		id   string
		dist float32
	}
	var results []pair
	for i := 0; i < len(f.Ids); i++ {
		// 从连续内存中提取向量
		start := i * f.Dim
		end := start + f.Dim
		searchVector := f.Data[start:end]

		dist := distance(vector, searchVector, f.config.SpaceType)
		results = append(results, pair{f.Ids[i], dist})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].dist < results[j].dist })
	if k > len(results) {
		k = len(results)
	}
	ids := make([]string, k)
	dists := make([]float32, k)
	for i := 0; i < k; i++ {
		ids[i] = results[i].id
		dists[i] = results[i].dist
	}
	return &SearchResult{IDs: ids, Distances: dists}, nil
}

// SetParams 设置参数（flat实现可忽略）
func (f *FlatIndex) SetParams(params map[string]any) error {
	return nil
}

// Load 从磁盘加载索引
func (f *FlatIndex) Load(filePath string) error {

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	return dec.Decode(f)
}

// Save： save index into the disk
func (f *FlatIndex) Save(filePath string) error {

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	return enc.Encode(f)
}

// Close release resource 
func (f *FlatIndex) Close() error {
	return nil
}
