package db

import (
	"oasisdb/internal/config"
	"oasisdb/internal/index"
	"oasisdb/internal/storage"
)

type DB struct {
	Storage      storage.ScalarStorage
	IndexFactory *index.Factory
}

func (db *DB) Open(conf *config.Config) error {
	storage, err := storage.NewStorage(conf)
	if err != nil {
		return err
	}
	db.Storage = storage
	indexFactory, err := index.NewFactory(conf)
	if err != nil {
		return err
	}
	// load indexs
	if err := indexFactory.LoadIndexs(); err != nil {
		return err
	}
	db.IndexFactory = indexFactory
	return nil
}

func (db *DB) Close() {
	db.Storage.Stop()
	db.IndexFactory.Close()
}
