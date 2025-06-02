package filter

type BloomFilter struct {
}

func NewBloomFilter() *BloomFilter {
	return &BloomFilter{}
}

func (b *BloomFilter) Add(key []byte) {

}

func (b *BloomFilter) MayContain(key []byte) bool {
	return false
}
