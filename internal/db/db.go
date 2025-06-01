package db

import "oasisdb/internal/storage"

type Config struct {
}

type DB struct {
	LSMTree *storage.LSMTree
	Config  Config
}
