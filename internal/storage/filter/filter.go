package filter

type Filter interface {
	Add(key []byte)                     // add key to filter
	MayContain(bitmap, key []byte) bool // check if key may be in filter
	Hash() []byte                       // generate bitmap for Filter
	KeyLen() int
	Reset() // reset filter
}
