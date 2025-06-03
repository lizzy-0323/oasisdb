package memtable

type MemTableConstructor func() MemTable

type MemTable interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	All() []*KVPair  // return all key-value pairs
	Size() int       // data size
	EntriesCnt() int // num of entries
}

type KVPair struct {
	Key   []byte
	Value []byte
}
