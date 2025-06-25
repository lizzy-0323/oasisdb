package hnsw

/*
#cgo CFLAGS: -I${SRCDIR}/../../c_api/hnsw
#cgo LDFLAGS: -L${SRCDIR}/../../build -lhnsw -Wl,-rpath,${SRCDIR}/../../build
#include "hnsw_c_api.h"
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"
)

type Index struct {
	index *C.HNSWIndex
}

func NewIndex(dim, maxElements, m, efConstruction uint32, spaceType string) *Index {
	var index *C.HNSWIndex
	switch spaceType {
	case "l2":
		index = C.hnsw_new(C.size_t(dim), C.size_t(maxElements), C.size_t(m), C.size_t(efConstruction), C.char('l'))
	case "ip":
		index = C.hnsw_new(C.size_t(dim), C.size_t(maxElements), C.size_t(m), C.size_t(efConstruction), C.char('i'))
	default:
		return nil
	}
	return &Index{index: index}
}

// Unload the index, free the memory, opposite to NewIndex
func (idx *Index) Unload() bool {
	if idx.index == nil {
		// already unloaded
		return false
	}
	idx.Free()
	idx.index = nil
	return true
}

func (idx *Index) Free() {
	C.hnsw_free(idx.index)
}

func (idx *Index) AddItems(points [][]float32, ids []uint32, numGoroutines int) error {
	if len(ids) != len(points) {
		return fmt.Errorf("ids and points must have the same length")
	}
	if numGoroutines <= 0 {
		numGoroutines = 1
	}
	if numGoroutines > len(points) {
		numGoroutines = len(points)
	}
	// If the number of points is less than 10000, use single goroutine
	// TODO: 需要确定合适的参数
	if len(points) <= 10000 || numGoroutines == 1 {
		for i := 0; i < len(points); i++ {
			err := idx.AddPoint(points[i], ids[i])
			if err != nil {
				return fmt.Errorf("failed to add point: %w", err)
			}
		}
		return nil
	}
	block := len(points) / numGoroutines
	var wg sync.WaitGroup

	for i := range numGoroutines {
		wg.Add(1)
		start := i * block
		end := (i + 1) * block
		if i == numGoroutines-1 && len(points) > end {
			end = len(points)
		}
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				err := idx.AddPoint(points[j], ids[j])
				if err != nil {
					fmt.Printf("failed to add point: %v\n", err)
				}
			}
		}(start, end)
	}
	wg.Wait()
	return nil
}

func (idx *Index) AddPoint(point []float32, id uint32) error {
	if len(point) == 0 {
		return fmt.Errorf("empty point data")
	}
	if idx.index == nil {
		return fmt.Errorf("index not initialized")
	}
	ret := C.hnsw_add_point(idx.index, (*C.float)(&point[0]), C.size_t(id))
	if ret != 0 {
		return fmt.Errorf("failed to add point")
	}
	return nil
}

func (idx *Index) SearchKNN(query []float32, k int) ([]uint32, []float32, error) {
	if len(query) == 0 {
		return nil, nil, fmt.Errorf("empty query data")
	}

	labels := make([]C.size_t, k)
	distances := make([]C.float, k)

	C.hnsw_search_knn(idx.index, (*C.float)(&query[0]), C.size_t(k),
		(*C.size_t)(&labels[0]), (*C.float)(&distances[0]))

	result_labels := make([]uint32, k)
	result_distances := make([]float32, k)

	for i := 0; i < k; i++ {
		result_labels[i] = uint32(labels[i])
		result_distances[i] = float32(distances[i])
	}

	return result_labels, result_distances, nil
}

func (idx *Index) BatchSearchKnn(queries [][]float32, k int, numGoroutines int) ([][]uint32, [][]float32, error) {
	if numGoroutines <= 1 {
		numGoroutines = 1
	}
	if numGoroutines > len(queries) {
		numGoroutines = len(queries)
	}
	labelList := make([][]uint32, len(queries))
	distList := make([][]float32, len(queries))

	// If the number of queries is less than 10000, use single goroutine
	// TODO: 需要确定合适的参数
	if len(queries) <= 10000 || numGoroutines == 1 {
		for i := 0; i < len(queries); i++ {
			labels, distances, err := idx.SearchKNN(queries[i], k)
			if err != nil {
				return nil, nil, err
			}
			labelList[i] = labels
			distList[i] = distances
		}
		return labelList, distList, nil
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < len(queries); j++ {
				labels, distances, err := idx.SearchKNN(queries[j], k)
				if err != nil {
					return
				}
				mu.Lock()
				labelList[j] = labels
				distList[j] = distances
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return labelList, distList, nil
}

func (idx *Index) SetEf(ef int) error {
	if idx.index == nil {
		return fmt.Errorf("index is not initialized")
	}
	C.hnsw_set_ef(idx.index, C.size_t(ef))
	return nil
}

func (idx *Index) SaveIndex(path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	ret := C.hnsw_save_index(idx.index, cPath)
	if ret != 0 {
		return fmt.Errorf("failed to save index")
	}
	return nil
}

func LoadIndex(path string, dim int, spaceType string) (*Index, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var index *C.HNSWIndex
	switch spaceType {
	case "l2":
		index = C.hnsw_load_index(cPath, C.size_t(dim), C.char('l'))
	case "ip":
		index = C.hnsw_load_index(cPath, C.size_t(dim), C.char('i'))
	default:
		return nil, fmt.Errorf("unsupported space type: %s", spaceType)
	}

	if index == nil {
		return nil, fmt.Errorf("failed to load index")
	}
	return &Index{index: index}, nil
}

func (idx *Index) MarkDeleted(label uint32) error {
	ret := C.hnsw_mark_deleted(idx.index, C.size_t(label))
	if ret != 0 {
		return fmt.Errorf("failed to mark element as deleted")
	}
	return nil
}

// GetVectorByLabel get index by label
func (idx *Index) GetVectorByLabel(label uint32, dim int) []float32 {
	var outDataPtr C.float
	ret := C.get_data_by_label(idx.index, C.size_t(label), &outDataPtr)
	if ret != 0 {
		return nil // label not found
	}
	outData := make([]float32, dim)
	for i := 0; i < dim; i++ {
		outData[i] = float32(*(*C.float)(unsafe.Pointer(uintptr(unsafe.Pointer(&outDataPtr)) + uintptr(i)*unsafe.Sizeof(C.float(0)))))
	}
	return outData
}

func (idx *Index) GetMaxElements() int {
	return int(C.get_max_elements(idx.index))
}

func (idx *Index) GetCurrentElementCount() int {
	return int(C.get_current_element_count(idx.index))
}

func (idx *Index) GetDeletedCount() int {
	return int(C.get_deleted_count(idx.index))
}

func (idx *Index) GetAvgHops() float32 {
	return float32(C.get_avg_hops(idx.index))
}

func (idx *Index) GetAvgDistComputations() float32 {
	return float32(C.get_avg_dist_computations(idx.index))
}

func (idx *Index) GetQueryCount() int {
	return int(C.get_query_count(idx.index))
}
