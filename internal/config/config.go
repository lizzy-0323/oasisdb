package config

import (
	"errors"
	"io/fs"
	"oasisdb/internal/storage/filter"
	"oasisdb/internal/storage/memtable"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Dir      string `yaml:"dir"` // dir to save sst files
	MaxLevel int    `yaml:"max_level"`

	// SSTable Config
	SSTSize          uint64 `yaml:"sst_size"`
	SSTNumPerLevel   uint64 `yaml:"sst_num_per_level"`
	SSTDataBlockSize uint64 `yaml:"sst_data_block_size"`
	SSTFooterSize    uint64 `yaml:"sst_footer_size"`

	// Cache Config
	CacheSize int `yaml:"cache_size"`

	Filter              filter.Filter
	MemTableConstructor memtable.MemTableConstructor
}
type ConfigOption func(*Config)

const (
	DefaultMaxLevel         = 7
	DefaultSSTSize          = 1024 * 1024 // 1MB
	DefaultSSTNumPerLevel   = 10
	DefaultSSTDataBlockSize = 16 * 1024 // 16KB
	DefaultSSTFooterSize    = 32        // 32B
	DefaultCacheSize        = 10
)

func NewConfig(dir string, opts ...ConfigOption) (*Config, error) {
	c := Config{
		Dir:           dir,
		SSTFooterSize: DefaultSSTFooterSize,
		CacheSize:     DefaultCacheSize,
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
	if c.CacheSize <= 0 {
		c.CacheSize = DefaultCacheSize
	}
	if c.Filter == nil {
		c.Filter = filter.NewBloomFilter(1024)
	}
	if c.MemTableConstructor == nil {
		c.MemTableConstructor = memtable.NewSkipList
	}

	return &c, c.Check()
}

func (c *Config) Check() error {
	if _, err := os.ReadDir(c.Dir); err != nil {
		var pathError *fs.PathError
		if !errors.As(err, &pathError) || !strings.HasSuffix(pathError.Err.Error(), "no such file or directory") {
			return err
		}
		if err = os.Mkdir(c.Dir, os.ModePerm); err != nil {
			return err
		}
	}

	// Create WAL directory if not exists
	if err := os.MkdirAll(path.Join(c.Dir, "walfile", "memtable"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(path.Join(c.Dir, "walfile", "index"), 0755); err != nil {
		return err
	}
	// Create index directory if not exists
	if err := os.MkdirAll(path.Join(c.Dir, "indexfile"), 0755); err != nil {
		return err
	}
	// Create SST directory if not exists
	if err := os.MkdirAll(path.Join(c.Dir, "sstfile"), 0755); err != nil {
		return err
	}

	return nil
}

// FromFile reads configuration from a YAML file
func FromFile(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// Create config with options from file
	opts := []ConfigOption{
		WithMaxLevel(config.MaxLevel),
		WithSSTSize(config.SSTSize),
		WithSSTNumPerLevel(config.SSTNumPerLevel),
		WithSSTDataBlockSize(config.SSTDataBlockSize),
		WithSSTFooterSize(config.SSTFooterSize),
		WithCacheSize(config.CacheSize),
	}

	return NewConfig(config.Dir, opts...)
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

// WithSSTFooterSize set sstable footer size
func WithSSTFooterSize(sstFooterSize uint64) ConfigOption {
	return func(c *Config) {
		c.SSTFooterSize = sstFooterSize
	}
}

// WithSSTNumPerLevel set sstable num per level
func WithSSTNumPerLevel(sstNumPerLevel uint64) ConfigOption {
	return func(c *Config) {
		c.SSTNumPerLevel = sstNumPerLevel
	}
}

// WithSSTDataBlockSize set sstable data block size
func WithSSTDataBlockSize(sstDataBlockSize uint64) ConfigOption {
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
		c.MemTableConstructor = constructor
	}
}

// WithCacheSize set cache size
func WithCacheSize(cacheSize int) ConfigOption {
	return func(c *Config) {
		c.CacheSize = cacheSize
	}
}
