package filter

import "github.com/twmb/murmur3"

type BloomFilter struct {
	m        int // len of bitmap
	hashKeys []uint32
}

const (
	DefaultBloomFilterM = 1024
)

func NewBloomFilter(m int) *BloomFilter {
	if m <= 0 {
		m = DefaultBloomFilterM
	}
	return &BloomFilter{
		m: m,
	}
}

func (b *BloomFilter) Add(key []byte) {
	b.hashKeys = append(b.hashKeys, murmur3.Sum32(key))
}

func (b *BloomFilter) MayContain(bitmap, key []byte) bool {
	return false
}

func (b *BloomFilter) GetBestK() int {
	return 0
}

func (b *BloomFilter) Hash() []uint32 {
	return nil
}

func (b *BloomFilter) KeyLen() int {
	return 0
}
