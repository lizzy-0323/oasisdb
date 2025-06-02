package db

import "oasisdb/internal/storage"

type Config struct {
}

type DB struct {
	LSMTree *storage.LSMTree
	Config  Config
}

func (db *DB) Open(config *Config) error {
	return nil
}

func init() {

}
