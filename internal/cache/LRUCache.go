package cache

import "container/list"

type LRUCache struct {
	maxSize    int
	cache      map[string]*list.Element
	doubleList *list.List
}

func NewLRUCache(maxSize int) *LRUCache {
	return &LRUCache{
		maxSize:    maxSize,
		cache:      make(map[string]*list.Element),
		doubleList: list.New(),
	}
}

func (l *LRUCache) Set(key string, value interface{}) {
}

func (l *LRUCache) Get(key string) (interface{}, bool) {
	return nil, false
}
