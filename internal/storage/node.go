package storage

import (
	"oasisdb/internal/config"
	"oasisdb/internal/storage/sstable"
)

// Node in LSM Tree equals a sstable
type Node struct {
	conf      *config.Config
	file      string
	level     int
	seq       int
	size      uint64
	sstReader *sstable.SSTableReader
}

func NewNode(conf *config.Config) *Node {
	return &Node{
		conf: conf,
	}
}
