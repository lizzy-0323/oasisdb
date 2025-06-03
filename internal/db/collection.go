package db

import (
	"encoding/json"
	"fmt"
)

// Collection 表示一个向量集合
type Collection struct {
	Name      string            // 集合名称
	Dimension int              // 向量维度
	Metadata  map[string]string // 集合元数据
	db        *DB              // 所属数据库
}

// Document 表示一个文档
type Document struct {
	ID       string                 `json:"id"`
	Vector   []float32             `json:"vector"`
	Metadata map[string]interface{} `json:"metadata"`
}

// CreateCollection 创建一个新的集合
func (db *DB) CreateCollection(name string, dimension int, metadata map[string]string) (*Collection, error) {
	// 检查集合是否已存在
	key := fmt.Sprintf("collection:%s", name)
	if _, exists, err := db.Storage.GetScalar([]byte(key)); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("collection %s already exists", name)
	}

	// 创建新集合
	collection := &Collection{
		Name:      name,
		Dimension: dimension,
		Metadata:  metadata,
		db:        db,
	}

	// 序列化并存储集合信息
	data, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}

	if err := db.Storage.PutScalar([]byte(key), data); err != nil {
		return nil, err
	}

	return collection, nil
}

// GetCollection 获取一个集合
func (db *DB) GetCollection(name string) (*Collection, error) {
	key := fmt.Sprintf("collection:%s", name)
	data, exists, err := db.Storage.GetScalar([]byte(key))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("collection %s not found", name)
	}

	var collection Collection
	if err := json.Unmarshal(data, &collection); err != nil {
		return nil, err
	}
	collection.db = db
	return &collection, nil
}

// DeleteCollection 删除一个集合
func (db *DB) DeleteCollection(name string) error {
	key := fmt.Sprintf("collection:%s", name)
	return db.Storage.DeleteScalar([]byte(key))
}

// ListCollections 列出所有集合
func (db *DB) ListCollections() ([]*Collection, error) {
	// TODO: 实现集合列表功能
	return nil, nil
}
