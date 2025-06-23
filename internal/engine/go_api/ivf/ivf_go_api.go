package ivf

import (
	"fmt"
	"sync"
	"unsafe"
)

/*
#cgo CFLAGS: -I${SRCDIR}/../../c_api/ivf
#cgo LDFLAGS: -L${SRCDIR}/../../build -livf -Wl,-rpath,${SRCDIR}/../../build
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include "ivf_c_api.h"
*/
import "C"

// Success is returned when an operation succeeds
const Success C.ivf_error_t = 0

// IVFIndex represents an IVF index
type IVFIndex struct {
	index *C.struct_IVFIndex
	mutex sync.RWMutex
	Dim   uint32
}

// NewIVFIndex creates a new IVF index
func NewIVFIndex(dim, nlist uint32) (*IVFIndex, error) {
	index := C.ivf_create_index(C.uint32_t(dim), C.uint32_t(nlist))
	if index == nil {
		return nil, fmt.Errorf("failed to create IVF index")
	}
	return &IVFIndex{index: index, Dim: dim}, nil
}

// Free releases resources associated with the index
func (idx *IVFIndex) Free() {
	if idx.index != nil {
		C.ivf_free_index(idx.index)
		idx.index = nil
	}
}

// Train trains the index with the given data
func (idx *IVFIndex) Train(data []float32) error {
	if len(data) == 0 {
		return fmt.Errorf("empty training data")
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	num := len(data) / int(idx.Dim)
	fmt.Println(num)
	ret := C.ivf_train(idx.index, C.uint(num), (*C.float)(&data[0]))
	if ret != Success {
		return fmt.Errorf("failed to train IVF index: %d", ret)
	}
	return nil
}

// Add adds a single vector to the index
func (idx *IVFIndex) Add(vector []float32, id int64) error {
	if len(vector) == 0 {
		return fmt.Errorf("empty vector")
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	ret := C.ivf_add(idx.index, C.uint(1),
		(*C.float)(&vector[0]), (*C.int64_t)(&id))
	if ret != Success {
		return fmt.Errorf("failed to add vector to IVF index: %d", ret)
	}
	return nil
}

// AddItems adds multiple vectors with their IDs to the index
func (idx *IVFIndex) AddItems(vectors [][]float32, ids []int64, numGoroutines int) error {
	if len(vectors) != len(ids) {
		return fmt.Errorf("vectors and ids must have the same length")
	}
	if numGoroutines <= 0 {
		numGoroutines = 1
	}
	if numGoroutines > len(vectors) {
		numGoroutines = len(vectors)
	}

	// For small batches, use single goroutine
	if len(vectors) <= 10000 || numGoroutines == 1 {
		return idx.addBatch(vectors, ids, 0, len(vectors))
	}

	// Split work among goroutines
	block := len(vectors) / numGoroutines
	var wg sync.WaitGroup
	var errChan = make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		start := i * block
		end := (i + 1) * block
		if i == numGoroutines-1 {
			end = len(vectors)
		}

		go func(start, end int) {
			defer wg.Done()
			if err := idx.addBatch(vectors, ids, start, end); err != nil {
				errChan <- err
			}
		}(start, end)
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return fmt.Errorf("failed to add items: %w", err)
	}
	return nil
}

// addBatch adds a batch of vectors to the index
func (idx *IVFIndex) addBatch(vectors [][]float32, ids []int64, start, end int) error {
	if start >= end || end > len(vectors) {
		return fmt.Errorf("invalid batch range")
	}

	batchSize := end - start
	// Flatten vectors for the batch
	dim := len(vectors[start])
	flatVectors := make([]float32, batchSize*dim)
	batchIds := make([]int64, batchSize)

	for i := 0; i < batchSize; i++ {
		copy(flatVectors[i*dim:], vectors[start+i])
		batchIds[i] = ids[start+i]
	}

	ret := C.ivf_add(idx.index, C.uint32_t(batchSize), (*C.float)(&flatVectors[0]), (*C.int64_t)(&batchIds[0]))
	if ret != Success {
		return fmt.Errorf("failed to add batch: %d", ret)
	}
	return nil
}

// Search searches for the k nearest neighbors of the query vector
func (idx *IVFIndex) Search(query []float32, k uint32) ([]int64, []float32, error) {
	if len(query) == 0 {
		return nil, nil, fmt.Errorf("empty query vector")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	neighbors := make([]int64, k)
	distances := make([]float32, k)

	// Get nprobe from parameters, default to 1
	nprobe := uint32(1)

	ret := C.ivf_search(idx.index, C.uint32_t(1), (*C.float)(&query[0]),
		C.uint32_t(k), C.uint32_t(nprobe), (*C.int64_t)(&neighbors[0]), (*C.float)(&distances[0]))
	if ret == 0 {
		return neighbors, distances, nil
	}
	return nil, nil, fmt.Errorf("failed to search IVF index: %d", ret)
}

// Remove removes vectors with the given IDs from the index
func (idx *IVFIndex) Remove(ids []int64) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if len(ids) == 0 {
		return nil
	}

	err := C.ivf_remove(idx.index, C.uint32_t(len(ids)), (*C.int64_t)(&ids[0]))
	if err != Success {
		return fmt.Errorf("failed to remove vectors from IVF index: %d", err)
	}
	return nil
}

// Size returns the number of vectors in the index
func (idx *IVFIndex) Size() uint64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	return uint64(C.ivf_size(idx.index))
}

// Save saves the index to a file
func (idx *IVFIndex) Save(path string) error {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	ret := C.ivf_save(idx.index, cPath)
	if ret != Success {
		return fmt.Errorf("failed to save IVF index: %d", ret)
	}

	return nil
}

// Load loads the index from a file
func LoadIVFIndex(path string) (*IVFIndex, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	index := C.ivf_load(cPath)
	if index == nil {
		return nil, fmt.Errorf("failed to load IVF index")
	}

	return &IVFIndex{index: index}, nil
}
