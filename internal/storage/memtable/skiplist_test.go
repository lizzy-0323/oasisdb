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

	val, ok := sl.Get(key1)
	assert.True(t, ok)
	assert.True(t, bytes.Equal(value1, val))

	// Test update existing key
	value2 := []byte("value2")
	err = sl.Put(key1, value2)
	assert.NoError(t, err)

	val, ok = sl.Get(key1)
	assert.True(t, ok)
	assert.True(t, bytes.Equal(value2, val))

	// Test non-existent key
	nonExistKey := []byte("nonexist")
	val, ok = sl.Get(nonExistKey)
	assert.False(t, ok)
	assert.Nil(t, val)

	// Test size and entries count
	assert.Equal(t, len(key1)+len(value2), sl.Size())
	assert.Equal(t, 1, sl.EntriesCnt())
}

func TestSkipList_ConcurrentOperations(t *testing.T) {
	sl := memtable.NewSkipList()
	const numGoroutines = 10
	const numOpsPerGoroutine = 100

	var writerWg, readerWg sync.WaitGroup
	writerWg.Add(numGoroutines)
	readerWg.Add(numGoroutines)

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer writerWg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := []byte("key" + string(rune(id)) + string(rune(j)))
				value := []byte("value" + string(rune(id)) + string(rune(j)))
				err := sl.Put(key, value)
				assert.NoError(t, err)
			}
		}(i)
	}

	// Wait for writers to finish
	writerWg.Wait()

	// Readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer readerWg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := []byte("key" + string(rune(id)) + string(rune(j)))
				val, ok := sl.Get(key)
				assert.True(t, ok)
				assert.True(t, bytes.Equal(val, []byte("value"+string(rune(id))+string(rune(j)))))
			}
		}(i)
	}

	readerWg.Wait()

	// Verify final state
	assert.Equal(t, numGoroutines*numOpsPerGoroutine, sl.EntriesCnt())
}
