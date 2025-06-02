package filter

import (
	"math"

	"github.com/twmb/murmur3"
)

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

// judge if key may be in filter
func (b *BloomFilter) MayContain(bitmap, key []byte) bool {
	if bitmap == nil {
		bitmap = b.Hash()
	}
	k := bitmap[len(bitmap)-1]
	h1 := murmur3.Sum32(key)
	h2 := (h1 >> 17) | (h1 << 15)
	for i := uint32(0); i < uint32(k); i++ {
		// h_i = h1 + i*h2
		targetBit := (h1 + i*h2) % uint32(len(bitmap)<<3)
		if (bitmap[targetBit>>3] & (1 << (targetBit & 7))) == 0 {
			return false
		}
	}
	// This maybe misjudged, you can refer to bloom filter paper
	return true
}

// get best k for bloom filter, k is the number of hash functions
func (b *BloomFilter) GetBestK() uint8 {
	// formula: k = ln2 * m / n  m: bitmap size, n: key count
	k := uint8(math.Ln2 * float64(b.m) / float64(len(b.hashKeys)))
	return max(1, min(30, k))
}

// generate bitmap for filter, which contains k hash functions
func (b *BloomFilter) Hash() []byte {
	k := b.GetBestK()
	// generate empty bitmap
	bitmap := b.bitmap(k)
	// h1 = murmur3.Sum32(key)
	// h2 = (h1 >> 17) | (h1 << 15)
	// h_i = h1 + i*h2
	for _, hashedKey := range b.hashKeys {
		// hashedKey is h1
		// delta is h2
		delta := (hashedKey >> 17) | (hashedKey << 15)
		for i := uint32(0); i < uint32(k); i++ {
			// gi = h1 + i * h2
			// need to mark the bit
			targetBit := (hashedKey + i*delta) % uint32(len(bitmap)<<3)
			bitmap[targetBit>>3] |= (1 << (targetBit & 7))
		}
	}
	return bitmap
}

// get key len
func (b *BloomFilter) KeyLen() int {
	return len(b.hashKeys)
}

func (b *BloomFilter) bitmap(k uint8) []byte {
	bitmapLen := (b.m + 7) >> 3
	bitmap := make([]byte, bitmapLen+1)
	// last byte is k
	bitmap[bitmapLen] = k
	return bitmap
}

// reset filter
func (b *BloomFilter) Reset() {
	b.hashKeys = b.hashKeys[:0]
}

func min(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint8) uint8 {
	if a > b {
		return a
	}
	return b
}
