package memtable_test

import (
	"bytes"
	"oasisdb/internal/storage/memtable"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_BasicOperations(t *testing.T) {
	sl := memtable.NewSkipList()

	// Test Put and Get
	key1 := []byte("key1")
	value1 := []byte("value1")
	err := sl.Put(key1, value1)
	assert.NoError(t, err)

	val, err := sl.Get(key1)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(value1, val))

	// Test update existing key
	value2 := []byte("value2")
	err = sl.Put(key1, value2)
	assert.NoError(t, err)

	val, err = sl.Get(key1)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(value2, val))

	// Test non-existent key
	nonExistKey := []byte("nonexist")
	val, err = sl.Get(nonExistKey)
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Test size and entries count
	assert.Equal(t, len(key1)+len(value2), sl.Size())
	assert.Equal(t, 1, sl.EntriesCnt())
}

func TestSkipList_ConcurrentOperations(t *testing.T) {
	sl := memtable.NewSkipList()
	const numGoroutines = 10
	const numOpsPerGoroutine = 100

	wg := sync.WaitGroup{}
	wg.Add(numGoroutines * 2) // Writers + Readers

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := []byte("key" + string(rune(id)) + string(rune(j)))
				value := []byte("value" + string(rune(id)) + string(rune(j)))
				err := sl.Put(key, value)
				assert.NoError(t, err)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := []byte("key" + string(rune(id)) + string(rune(j)))
				_, err := sl.Get(key)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	assert.Equal(t, numGoroutines*numOpsPerGoroutine, sl.EntriesCnt())
}
