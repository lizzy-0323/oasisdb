package config

import (
	"io/fs"
	"oasisdb/internal/storage/filter"
	"oasisdb/internal/storage/memtable"
	"os"
	"strings"
)

type Config struct {
	Dir      string // dir to save sst files
	MaxLevel int

	// SSTable Config
	SSTSize          uint64
	SSTNumPerLevel   uint64
	SSTDataBlockSize int
	SSTFooterSize    uint64

	Filter              filter.Filter
	memTableConstructor memtable.MemTableConstructor
}
type ConfigOption func(*Config)

const (
	DefaultMaxLevel         = 7
	DefaultSSTSize          = 1024 * 1024 // 1MB
	DefaultSSTNumPerLevel   = 10
	DefaultSSTDataBlockSize = 16 * 1024 // 16KB
	DefaultSSTFooterSize    = 32        // 32B
)

func NewConfig(dir string, opts ...ConfigOption) (*Config, error) {
	c := Config{
		Dir:           dir,
		SSTFooterSize: DefaultSSTFooterSize,
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.MaxLevel <= 1 {
		c.MaxLevel = DefaultMaxLevel
	}
	if c.SSTSize <= 0 {
		c.SSTSize = DefaultSSTSize
	}
	if c.SSTNumPerLevel <= 0 {
		c.SSTNumPerLevel = DefaultSSTNumPerLevel
	}
	if c.SSTDataBlockSize <= 0 {
		c.SSTDataBlockSize = DefaultSSTDataBlockSize
	}
	if c.SSTFooterSize <= 0 {
		c.SSTFooterSize = DefaultSSTFooterSize
	}
	if c.Filter == nil {
		c.Filter = filter.NewBloomFilter(1024)
	}
	if c.memTableConstructor == nil {
		c.memTableConstructor = memtable.NewSkipList
	}

	return &c, c.Check()
}

func (c *Config) Check() error {
	if _, err := os.ReadDir(c.Dir); err != nil {
		_, ok := err.(*fs.PathError)
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		if err = os.Mkdir(c.Dir, os.ModePerm); err != nil {
			return err
		}
	}
	// TODO: Add wal check
	return nil
}

// WithMaxLevel set max level of lsm tree
func WithMaxLevel(maxLevel int) ConfigOption {
	return func(c *Config) {
		c.MaxLevel = maxLevel
	}
}

// WithSSTSize set sstable size
func WithSSTSize(sstSize uint64) ConfigOption {
	return func(c *Config) {
		c.SSTSize = sstSize
	}
}

// WithSSTNumPerLevel set sstable num per level
func WithSSTNumPerLevel(sstNumPerLevel uint64) ConfigOption {
	return func(c *Config) {
		c.SSTNumPerLevel = sstNumPerLevel
	}
}

// WithSSTDataBlockSize set sstable data block size
func WithSSTDataBlockSize(sstDataBlockSize int) ConfigOption {
	return func(c *Config) {
		c.SSTDataBlockSize = sstDataBlockSize
	}
}

// WithFilter set sstable filter
func WithFilter(filter filter.Filter) ConfigOption {
	return func(c *Config) {
		c.Filter = filter
	}
}

// WithMemTableConstructor set memtable constructor
func WithMemTableConstructor(constructor memtable.MemTableConstructor) ConfigOption {
	return func(c *Config) {
		c.memTableConstructor = constructor
	}
}
