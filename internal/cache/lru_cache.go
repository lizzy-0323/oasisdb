package cache

import "container/list"

// entry represents a key-value pair in the cache
type entry struct {
	key   string
	value interface{}
}

// LRUCache implements a Least Recently Used cache
type LRUCache struct {
	maxSize    int
	cache      map[string]*list.Element
	doubleList *list.List
}

// NewLRUCache creates a new LRU cache with the given maximum size
func NewLRUCache(maxSize int) *LRUCache {
	return &LRUCache{
		maxSize:    maxSize,
		cache:      make(map[string]*list.Element),
		doubleList: list.New(),
	}
}

// Set adds or updates a key-value pair in the cache
func (l *LRUCache) Set(key string, value interface{}) {
	// If key exists, update its value and move to front
	if element, exists := l.cache[key]; exists {
		l.doubleList.MoveToFront(element)
		element.Value.(*entry).value = value
		return
	}

	// Add new entry
	ele := l.doubleList.PushFront(&entry{key: key, value: value})
	l.cache[key] = ele

	// Remove oldest if cache is full
	if l.doubleList.Len() > l.maxSize {
		oldest := l.doubleList.Back()
		if oldest != nil {
			l.removeElement(oldest)
		}
	}
}

// Get retrieves a value from the cache by key
func (l *LRUCache) Get(key string) (interface{}, bool) {
	element, exists := l.cache[key]
	if !exists {
		return nil, false
	}

	// Move to front (most recently used)
	l.doubleList.MoveToFront(element)
	return element.Value.(*entry).value, true
}

// Delete removes a key-value pair from the cache
func (l *LRUCache) Delete(key string) {
	if element, exists := l.cache[key]; exists {
		l.removeElement(element)
	}
}

// DeleteWithPrefix removes all key-value pairs with the given prefix
func (l *LRUCache) DeleteWithPrefix(prefix string) {
	// Create a list of elements to remove to avoid modifying the map during iteration
	toRemove := make([]*list.Element, 0)

	// Find all elements with the prefix
	for key, element := range l.cache {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			toRemove = append(toRemove, element)
		}
	}

	// Remove all found elements
	for _, element := range toRemove {
		l.removeElement(element)
	}
}

func (l *LRUCache) Clear() {
	l.cache = make(map[string]*list.Element)
	l.doubleList = list.New()
}

func (l *LRUCache) Len() int {
	return l.doubleList.Len()
}

// removeElement removes an element from the cache
func (l *LRUCache) removeElement(element *list.Element) {
	l.doubleList.Remove(element)
	delete(l.cache, element.Value.(*entry).key)
}
