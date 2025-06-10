package db

import (
	"oasisdb/internal/cache"
	"oasisdb/internal/config"
	"oasisdb/internal/index"
	"oasisdb/internal/storage"
)

type DB struct {
	conf         *config.Config
	Storage      storage.ScalarStorage
	IndexManager *index.Manager
	Cache        *cache.LRUCache
}

func New(conf *config.Config) (*DB, error) {
	return &DB{
		conf: conf,
	}, nil
}

func (db *DB) Open() error {
	storage, err := storage.NewStorage(db.conf)
	if err != nil {
		return err
	}
	indexManager, err := index.NewIndexManager(db.conf)
	if err != nil {
		return err
	}
	// load indexs
	if err := indexManager.LoadIndexs(); err != nil {
		return err
	}
	db.Storage = storage
	db.IndexManager = indexManager
	db.Cache = cache.NewLRUCache(db.conf.CacheSize)
	return nil
}

func (db *DB) Close() {
	db.Storage.Stop()
	db.IndexManager.Close()
	db.Cache.Clear()
}
