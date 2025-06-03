package db

import (
	"encoding/json"
	"fmt"
)

// Document represents a document
type Document struct {
	ID        string         `json:"id"`
	Vector    []float32      `json:"vector"`
	Metadata  map[string]any `json:"metadata"`
	Dimension int            `json:"dimension"`
}

// UpsertDocument inserts or updates a document
func (db *DB) UpsertDocument(collectionName string, doc *Document) error {
	// validate vector dimension
	if len(doc.Vector) != doc.Dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", doc.Dimension, len(doc.Vector))
	}

	// store document metadata
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, doc.ID)
	docData, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := db.Storage.PutScalar([]byte(docKey), docData); err != nil {
		return err
	}

	// update vector index
	// TODO: implement vector index update

	return nil
}

// GetDocument gets a document
func (db *DB) GetDocument(collectionName string, id string) (*Document, error) {
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, id)
	data, exists, err := db.Storage.GetScalar([]byte(docKey))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("document %s not found", id)
	}

	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// DeleteDocument deletes a document
func (db *DB) DeleteDocument(collectionName string, id string) error {
	docKey := fmt.Sprintf("doc:%s:%s", collectionName, id)
	if err := db.Storage.DeleteScalar([]byte(docKey)); err != nil {
		return err
	}

	// TODO: delete vector from index
	return nil
}

// SearchDocuments searches documents
func (db *DB) SearchDocuments(collectionName string, queryVector []float32, k int, filter map[string]interface{}) ([]*Document, []float32, error) {
	// TODO: 实现向量搜索
	// 1. 使用HNSW/IVF索引进行向量搜索
	// 2. 获取最近邻的文档ID和距离
	// 3. 根据文档ID获取完整的文档信息
	// 4. 应用过滤条件

	return nil, nil, nil
}

// BatchUpsertDocuments 批量插入或更新文档
func (db *DB) BatchUpsertDocuments(collectionName string, docs []*Document) error {
	// TODO: 实现批量插入
	// 1. 批量验证向量维度
	// 2. 批量存储文档元数据
	// 3. 批量更新向量索引
	return nil
}

// BatchDeleteDocuments 批量删除文档
func (db *DB) BatchDeleteDocuments(collectionName string, ids []string) error {
	// TODO: 实现批量删除
	// 1. 批量删除文档元数据
	// 2. 批量从向量索引中删除向量
	return nil
}
