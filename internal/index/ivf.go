package index

import (
	"encoding/gob"
	"errors"
	pkgerrors "oasisdb/pkg/errors"
	"oasisdb/pkg/logger"
	"os"
	"sort"
)

// ivfItem stores an individual vector and its document id.
// We keep the full vector in memory for simplicity – this is fine for a
// pure-Go reference implementation.
type ivfItem struct {
	ID     string
	Vector []float32
}

// ivfIndex is a (very) simple Inverted File (IVF) implementation. It supports
// training with k-means, adding new vectors and searching the index.  The
// design trades performance for portability – it is intentionally written in
// pure Go and avoids any cgo / SIMD dependencies so that it compiles on every
// platform.  The implementation is **NOT** production-grade but is sufficient
// for functional tests and examples.
type ivfIndex struct {
	// Immutable after construction / training
	config    *IndexConfig
	nlist     int // number of clusters
	nprobe    int // number of clusters to search
	centroids [][]float32

	// Mutable – protected by the Manager’s external locking guarantees; the
	// surrounding code never accesses the same index from multiple goroutines
	// concurrently, so we do not add extra locks here.
	lists [][]ivfItem

	// pending vectors stored before training
	pendingIDs     []string
	pendingVectors [][]float32

	trained bool
}

// persistable snapshot with exported fields for gob
type ivfSnapshot struct {
	Config    *IndexConfig
	Nlist     int
	Nprobe    int
	Centroids [][]float32
	Lists     [][]ivfItem
	Trained   bool
}

// newIVFIndex creates an empty IVF index. Training **must** be performed by
// calling Train(...) (or Build(...)) before Search can be used.
func newIVFIndex(config *IndexConfig) (VectorIndex, error) {
	if config.Dimension <= 0 {
		return nil, pkgerrors.ErrInvalidDimension
	}

	// Retrieve IVF parameters (fall back to sensible defaults)
	nlist := DEFAULT_NLIST
	nprobe := DEFAULT_NPROBE
	if v, ok := config.Parameters["nlist"]; ok {
		if vv, ok := v.(float64); ok {
			nlist = int(vv)
		}
	}
	if v, ok := config.Parameters["nprobe"]; ok {
		if vv, ok := v.(float64); ok {
			nprobe = int(vv)
		}
	}
	if nprobe <= 0 {
		nprobe = 1
	}
	if nprobe > nlist {
		nprobe = nlist
	}

	idx := &ivfIndex{
		config:         config,
		nlist:          nlist,
		nprobe:         nprobe,
		centroids:      nil,
		lists:          make([][]ivfItem, nlist),
		pendingIDs:     nil,
		pendingVectors: nil,
		trained:        false,
	}
	return idx, nil
}

// /////////////////////// Public (extra) APIs /////////////////////////
// Train performs k-means clustering over the supplied vectors and initialises
// the inverted lists.  After training, any vectors passed earlier via Add or
// AddBatch are assigned to their closest centroid.
func (ivf *ivfIndex) Train(vectors [][]float32) error {
	if ivf.trained {
		return nil // already trained
	}
	if len(vectors) == 0 {
		return errors.New("no training data provided")
	}
	// dimension check
	for _, v := range vectors {
		if len(v) != ivf.config.Dimension {
			return pkgerrors.ErrInvalidDimension
		}
	}

	centroids := kMeans(vectors, ivf.nlist, ivf.config.Dimension, DEFAULT_MAX_KMEANS_ITER)
	if len(centroids) != ivf.nlist {
		return errors.New("failed to train k-means")
	}
	ivf.centroids = centroids
	ivf.trained = true

	// Re-insert any pending vectors gathered before Train was called
	if len(ivf.pendingIDs) > 0 {
		_ = ivf.AddBatch(ivf.pendingIDs, ivf.pendingVectors)
		ivf.pendingIDs = nil
		ivf.pendingVectors = nil
	}
	return nil
}

// Build is a convenience wrapper that trains on the supplied vectors and then
// immediately adds them to the index.
func (ivf *ivfIndex) Build(ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return pkgerrors.ErrMisMatchKeysAndValues
	}
	if err := ivf.Train(vectors); err != nil {
		return err
	}
	return ivf.AddBatch(ids, vectors)
}

///////////////////////// VectorIndex interface /////////////////////////

func (ivf *ivfIndex) Add(id string, vector []float32) error {
	if len(vector) != ivf.config.Dimension {
		return pkgerrors.ErrInvalidDimension
	}
	if !ivf.trained {
		// Buffer until training is complete
		ivf.pendingIDs = append(ivf.pendingIDs, id)
		ivf.pendingVectors = append(ivf.pendingVectors, vector)
		return nil
	}
	c := ivf.closestCentroid(vector)
	ivf.lists[c] = append(ivf.lists[c], ivfItem{ID: id, Vector: vector})
	return nil
}

func (ivf *ivfIndex) AddBatch(ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return pkgerrors.ErrMisMatchKeysAndValues
	}
	for i, id := range ids {
		if err := ivf.Add(id, vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

func (ivf *ivfIndex) Delete(id string) error {
	// TODO: implement delete
	return nil
}

func (ivf *ivfIndex) Search(vector []float32, k int) (*SearchResult, error) {
	if !ivf.trained {
		return nil, errors.New("index not trained")
	}
	if len(vector) != ivf.config.Dimension {
		return nil, pkgerrors.ErrInvalidDimension
	}
	if k <= 0 {
		return &SearchResult{}, nil
	}

	// 1. find nprobe nearest centroids
	type centroidDist struct {
		idx int
		d   float32
	}
	cds := make([]centroidDist, ivf.nlist)
	for i, c := range ivf.centroids {
		cds[i] = centroidDist{idx: i, d: distance(vector, c, ivf.config.SpaceType)}
	}
	sort.Slice(cds, func(i, j int) bool { return cds[i].d < cds[j].d })

	// 2. gather candidates from the selected lists
	type cand struct {
		id string
		d  float32
	}
	candidates := make([]cand, 0, k*ivf.nprobe)
	for i := 0; i < ivf.nprobe && i < len(cds); i++ {
		listIdx := cds[i].idx
		for _, it := range ivf.lists[listIdx] {
			d := distance(vector, it.Vector, ivf.config.SpaceType)
			candidates = append(candidates, cand{id: it.ID, d: d})
		}
	}

	// 3. select top-k from candidates
	if len(candidates) == 0 {
		return &SearchResult{}, nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].d < candidates[j].d })
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	ids := make([]string, len(candidates))
	dists := make([]float32, len(candidates))
	for i, c := range candidates {
		ids[i] = c.id
		dists[i] = c.d
	}
	logger.Debug("Search result", "ids", ids, "dists", dists)
	return &SearchResult{IDs: ids, Distances: dists}, nil
}

// GetVector get vector by id
func (ivf *ivfIndex) GetVector(id string) ([]float32, error) {
	if !ivf.trained {
		for i, pendingID := range ivf.pendingIDs {
			if pendingID == id {
				return append([]float32(nil), ivf.pendingVectors[i]...), nil
			}
		}
		return nil, pkgerrors.ErrDocumentNotFound
	}

	for _, list := range ivf.lists {
		for _, item := range list {
			if item.ID == id {
				return append([]float32(nil), item.Vector...), nil
			}
		}
	}

	return nil, pkgerrors.ErrDocumentNotFound
}

func (ivf *ivfIndex) Load(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var snap ivfSnapshot
	if err := dec.Decode(&snap); err != nil {
		return err
	}
	// restore fields
	ivf.config = snap.Config
	ivf.nlist = snap.Nlist
	ivf.nprobe = snap.Nprobe
	ivf.centroids = snap.Centroids
	ivf.lists = snap.Lists
	ivf.trained = snap.Trained
	// clear any pending buffers
	ivf.pendingIDs = nil
	ivf.pendingVectors = nil
	return nil
}

func (ivf *ivfIndex) Save(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	snap := ivfSnapshot{
		Config:    ivf.config,
		Nlist:     ivf.nlist,
		Nprobe:    ivf.nprobe,
		Centroids: ivf.centroids,
		Lists:     ivf.lists,
		Trained:   ivf.trained,
	}
	logger.Debug("Saving index to file", "file", filePath)
	return enc.Encode(&snap)
}

func (ivf *ivfIndex) Close() error {
	// nothing to do
	return nil
}

///////////////////////// helpers /////////////////////////

// closestCentroid returns the index of the centroid nearest to the provided
// vector.
func (ivf *ivfIndex) closestCentroid(v []float32) int {
	best := 0
	bestDist := distance(v, ivf.centroids[0], ivf.config.SpaceType)
	for i := 1; i < len(ivf.centroids); i++ {
		d := distance(v, ivf.centroids[i], ivf.config.SpaceType)
		if d < bestDist {
			bestDist = d
			best = i
		}
	}
	return best
}

// kMeans performs a very small-scale Lloyd-style k-means clustering. The
// implementation is intentionally simple and stops after a fixed number of
// iterations or when centroids stop moving.  It **does not** implement any
// advanced initialisation (like k-means++); instead it picks the first k
// vectors as initial centroids which is good enough for a demo.
func kMeans(data [][]float32, k, dim, maxIter int) [][]float32 {
	if len(data) < k {
		k = len(data)
	}
	centroids := make([][]float32, k)
	for i := 0; i < k; i++ {
		centroids[i] = append([]float32(nil), data[i]...)
	}

	assignments := make([]int, len(data))

	for iter := 0; iter < maxIter; iter++ {
		changed := false
		// assignment step
		for i, v := range data {
			best := 0
			bestDist := distance(v, centroids[0], L2Space)
			for c := 1; c < k; c++ {
				d := distance(v, centroids[c], L2Space)
				if d < bestDist {
					bestDist = d
					best = c
				}
			}
			if assignments[i] != best {
				changed = true
				assignments[i] = best
			}
		}
		// update step
		counts := make([]int, k)
		sums := make([][]float32, k)
		for i := 0; i < k; i++ {
			sums[i] = make([]float32, dim)
		}
		for i, v := range data {
			c := assignments[i]
			counts[c]++
			for d := 0; d < dim; d++ {
				sums[c][d] += v[d]
			}
		}
		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				continue // empty cluster – keep old centroid
			}
			for d := 0; d < dim; d++ {
				centroids[c][d] = sums[c][d] / float32(counts[c])
			}
		}
		if !changed {
			break
		}
	}
	return centroids
}

func (ivf *ivfIndex) SetParams(params map[string]any) error {
	if len(params) == 0 {
		return pkgerrors.ErrEmptyParameter
	}

	for key, val := range params {
		switch key {
		case "nprobe":
			var ival int
			switch v := val.(type) {
			case int:
				ival = v
			case float64:
				ival = int(v)
			default:
				return pkgerrors.ErrInvalidParameter
			}
			if err := ivf.SetNProbe(ival); err != nil {
				return err
			}
		default:
			return pkgerrors.ErrInvalidParameter
		}
	}

	return nil
}

func (ivf *ivfIndex) SetNProbe(nprobe int) error {
	if nprobe <= 0 || nprobe > ivf.nlist {
		return pkgerrors.ErrInvalidParameter
	}
	ivf.nprobe = nprobe
	return nil
}
