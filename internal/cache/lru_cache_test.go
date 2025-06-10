package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRUCache_Basic(t *testing.T) {
	cache := NewLRUCache(2)

	// Test Set and Get
	cache.Set("key1", []string{"doc1", "doc2"})
	value, exists := cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, []string{"doc1", "doc2"}, value)

	// Test non-existent key
	_, exists = cache.Get("non-existent")
	assert.False(t, exists)
}

func TestLRUCache_Capacity(t *testing.T) {
	cache := NewLRUCache(2)

	// Fill cache
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// Add one more item, should evict key1
	cache.Set("key3", "value3")

	// key1 should be evicted
	_, exists := cache.Get("key1")
	assert.False(t, exists)

	// key2 and key3 should exist
	value, exists := cache.Get("key2")
	assert.True(t, exists)
	assert.Equal(t, "value2", value)

	value, exists = cache.Get("key3")
	assert.True(t, exists)
	assert.Equal(t, "value3", value)
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	cache := NewLRUCache(2)

	// Set initial value
	cache.Set("key1", "value1")

	// Update value
	cache.Set("key1", "newvalue1")

	// Check updated value
	value, exists := cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "newvalue1", value)
}

func TestLRUCache_LRUOrder(t *testing.T) {
	cache := NewLRUCache(2)

	// Add two items
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// Access key1, making it most recently used
	cache.Get("key1")

	// Add new item, should evict key2 instead of key1
	cache.Set("key3", "value3")

	// key1 should still exist (most recently used)
	value, exists := cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", value)

	// key2 should be evicted
	_, exists = cache.Get("key2")
	assert.False(t, exists)

	// key3 should exist
	value, exists = cache.Get("key3")
	assert.True(t, exists)
	assert.Equal(t, "value3", value)
}
