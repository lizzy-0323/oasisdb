package config

import "oasisdb/internal/storage/memtable"

type Config struct {
	Dir      string
	MaxLevel int

	// SSTable Config
	SSTSize        uint64
	SSTNumPerLevel uint64
	SSTBlockSize   uint64
	SSTFilterType  string

	MemTableConstructor memtable.MemTableConstructor
}
