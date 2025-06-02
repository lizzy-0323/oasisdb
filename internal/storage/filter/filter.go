package filter

type Filter interface {
	Add(key []byte)
	MayContain(key []byte) bool
}
