package filter

import (
	"math"
	"oasisdb/pkg/logger"

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
		logger.Debug("bitmap is nil, using Hash()", "key", string(key))
		bitmap = b.Hash()
	}

	if len(bitmap) == 0 {
		logger.Debug("Empty bitmap", "key", string(key))
		return true // if bitmap is empty, ignore
	}

	k := b.GetK(bitmap)

	// calculate h1 and h2
	h1 := murmur3.Sum32(key)
	h2 := (h1 >> 17) | (h1 << 15)

	availableBits := uint32((len(bitmap) - 1) << 3) // exclude the last byte which is k

	for i := uint32(0); i < uint32(k); i++ {
		// h_i = h1 + i*h2
		targetBit := (h1 + i*h2) % availableBits
		if (bitmap[targetBit>>3] & (1 << (targetBit & 7))) == 0 {
			logger.Debug("Bloom filter not contain key", "key", string(key), "target_bit", targetBit)
			return false
		}
	}
	// This maybe misjudged, you can refer to bloom filter paper
	logger.Debug("Bloom filter may contain key", "key", string(key))
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

	availableBits := uint32((len(bitmap) - 1) << 3) // exclude the last byte which is k
	logger.Debug("Bloom filter Hash", "k", k, "total_bitmap_len", len(bitmap), "available_bits", availableBits)

	for _, hashedKey := range b.hashKeys {
		// hashedKey is h1
		// delta is h2
		delta := (hashedKey >> 17) | (hashedKey << 15)
		for i := uint32(0); i < uint32(k); i++ {
			// gi = h1 + i * h2
			// need to mark the bit
			targetBit := (hashedKey + i*delta) % availableBits
			bitmap[targetBit>>3] |= (1 << (targetBit & 7))
		}
	}
	return bitmap
}

func (b *BloomFilter) GetK(bitmap []byte) uint8 {
	// k is the number of hash functions, which is the last byte of bitmap
	k := bitmap[len(bitmap)-1]

	// validate k value,Because the Bloom filter is an additional function and should not affect the main link, it is downgraded here for insurance.
	if k == 0 || k > 30 {
		logger.Debug("Invalid k value detected, using default", "invalid_k", k, "bitmap_len", len(bitmap))
		// use a reasonable default k value, like calculate by bitmap size
		// for 1024 bits bitmap, the reasonable k value is about 7-8
		k = 8
	}
	return k
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
