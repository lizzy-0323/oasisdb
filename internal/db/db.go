package db

import (
	"oasisdb/internal/config"
	"oasisdb/internal/storage"
)

type VectorDB interface {
	Query()
	UpSert()
	GetScalar(key []byte) ([]byte, bool, error)
	PutScalar(key []byte, value []byte) error
	DeleteScalar(key []byte) error
}

type DB struct {
	Storage *storage.Storage
}

func (db *DB) Open(config *config.Config) error {
	storage, err := storage.NewStorage(config)
	if err != nil {
		return err
	}
	db.Storage = storage
	return nil
}

func init() {

}
