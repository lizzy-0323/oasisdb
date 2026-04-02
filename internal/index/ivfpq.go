package index

import (
	"encoding/gob"
	"errors"
	pkgerrors "oasisdb/pkg/errors"
	"oasisdb/pkg/logger"
	"os"
	"sort"
)

// ivfpqItem stores a quantized vector, its original vector and document id.
type ivfpqItem struct {
	ID     string
	Codes  []uint8
	Vector []float32
}

// ivfpqIndex is a pure-Go Inverted File with Product Quantization implementation.
// It builds on top of the IVF design by compressing each residual vector with
// Product Quantization, significantly reducing memory usage and speeding up
// distance computations at the cost of slightly lower recall.
type ivfpqIndex struct {
	config    *IndexConfig
	nlist     int
	nprobe    int
	m         int // number of subspaces
	nbits     int // bits per subspace (only 8 is supported)
	dim       int
	subDim    int // dimension per subspace = dim / m
	centroids [][]float32

	// pqCodebooks[j][c][d] : subspace j, code c, dimension d (d < subDim)
	pqCodebooks [][][]float32

	lists [][]ivfpqItem

	pendingIDs     []string
	pendingVectors [][]float32

	trained bool
}

// ivfpqSnapshot is the persistable representation used by gob.
type ivfpqSnapshot struct {
	Config      *IndexConfig
	Nlist       int
	Nprobe      int
	M           int
	Nbits       int
	Dim         int
	SubDim      int
	Centroids   [][]float32
	PQCodebooks [][][]float32
	Lists       [][]ivfpqItem
	Trained     bool
}

// newIVFPQIndex creates an empty IVFPQ index. Training must be performed
// before Search can be used.
func newIVFPQIndex(config *IndexConfig) (VectorIndex, error) {
	if config.Dimension <= 0 {
		return nil, pkgerrors.ErrInvalidDimension
	}

	nlist := DEFAULT_NLIST
	nprobe := DEFAULT_NPROBE
	m := DEFAULT_IVFPQ_M
	nbits := DEFAULT_IVFPQ_NBITS

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
	if v, ok := config.Parameters["m"]; ok {
		if vv, ok := v.(float64); ok {
			m = int(vv)
		}
	}
	if v, ok := config.Parameters["nbits"]; ok {
		if vv, ok := v.(float64); ok {
			nbits = int(vv)
		}
	}

	if nprobe <= 0 {
		nprobe = 1
	}
	if nprobe > nlist {
		nprobe = nlist
	}
	if m <= 0 {
		m = 1
	}
	if config.Dimension%m != 0 {
		return nil, errors.New("dimension must be divisible by m")
	}
	if nbits != 8 {
		return nil, errors.New("only nbits=8 is supported")
	}

	idx := &ivfpqIndex{
		config:         config,
		nlist:          nlist,
		nprobe:         nprobe,
		m:              m,
		nbits:          nbits,
		dim:            config.Dimension,
		subDim:         config.Dimension / m,
		centroids:      nil,
		pqCodebooks:    nil,
		lists:          make([][]ivfpqItem, nlist),
		pendingIDs:     nil,
		pendingVectors: nil,
		trained:        false,
	}
	return idx, nil
}

// /////////////////////// Public (extra) APIs /////////////////////////

// Train performs k-means clustering for coarse centroids and then trains
// Product Quantization codebooks on the residual vectors.
func (idx *ivfpqIndex) Train(vectors [][]float32) error {
	if idx.trained {
		return nil
	}
	if len(vectors) == 0 {
		return errors.New("no training data provided")
	}
	for _, v := range vectors {
		if len(v) != idx.dim {
			return pkgerrors.ErrInvalidDimension
		}
	}

	// 1. coarse k-means
	centroids := kMeans(vectors, idx.nlist, idx.dim, DEFAULT_MAX_KMEANS_ITER)
	if len(centroids) != idx.nlist {
		return errors.New("failed to train coarse k-means")
	}
	idx.centroids = centroids

	// 2. train PQ codebooks on residuals
	ksub := 1 << idx.nbits
	idx.pqCodebooks = make([][][]float32, idx.m)

	for j := 0; j < idx.m; j++ {
		subVectors := make([][]float32, len(vectors))
		for i, vec := range vectors {
			c := idx.closestCentroid(vec)
			residual := make([]float32, idx.subDim)
			offset := j * idx.subDim
			for d := 0; d < idx.subDim; d++ {
				residual[d] = vec[offset+d] - idx.centroids[c][offset+d]
			}
			subVectors[i] = residual
		}
		idx.pqCodebooks[j] = kMeans(subVectors, ksub, idx.subDim, DEFAULT_MAX_KMEANS_ITER)
	}

	idx.trained = true

	if len(idx.pendingIDs) > 0 {
		_ = idx.AddBatch(idx.pendingIDs, idx.pendingVectors)
		idx.pendingIDs = nil
		idx.pendingVectors = nil
	}
	return nil
}

// Build trains the index and immediately adds the supplied vectors.
func (idx *ivfpqIndex) Build(ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return pkgerrors.ErrMisMatchKeysAndValues
	}
	if err := idx.Train(vectors); err != nil {
		return err
	}
	return idx.AddBatch(ids, vectors)
}

///////////////////////// VectorIndex interface /////////////////////////

func (idx *ivfpqIndex) Add(id string, vector []float32) error {
	if len(vector) != idx.dim {
		return pkgerrors.ErrInvalidDimension
	}
	if !idx.trained {
		idx.pendingIDs = append(idx.pendingIDs, id)
		idx.pendingVectors = append(idx.pendingVectors, vector)
		return nil
	}
	c := idx.closestCentroid(vector)
	codes := idx.encodeVector(vector, c)
	idx.lists[c] = append(idx.lists[c], ivfpqItem{ID: id, Codes: codes, Vector: vector})
	return nil
}

func (idx *ivfpqIndex) AddBatch(ids []string, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return pkgerrors.ErrMisMatchKeysAndValues
	}
	for i, id := range ids {
		if err := idx.Add(id, vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

func (idx *ivfpqIndex) Delete(id string) error {
	for ci := range idx.lists {
		for i, item := range idx.lists[ci] {
			if item.ID == id {
				idx.lists[ci] = append(idx.lists[ci][:i], idx.lists[ci][i+1:]...)
				return nil
			}
		}
	}
	return nil
}

func (idx *ivfpqIndex) Search(vector []float32, k int) (*SearchResult, error) {
	if !idx.trained {
		return nil, errors.New("index not trained")
	}
	if len(vector) != idx.dim {
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
	cds := make([]centroidDist, idx.nlist)
	for i, c := range idx.centroids {
		cds[i] = centroidDist{idx: i, d: distance(vector, c, idx.config.SpaceType)}
	}
	sort.Slice(cds, func(i, j int) bool { return cds[i].d < cds[j].d })

	// 2. gather candidates using ADC
	type cand struct {
		id string
		d  float32
	}
	candidates := make([]cand, 0, k*idx.nprobe)

	for i := 0; i < idx.nprobe && i < len(cds); i++ {
		ci := cds[i].idx

		// precompute distance table for this centroid
		// use actual codebook size (may be smaller than 1<<nbits when training data is limited)
		actualKsub := len(idx.pqCodebooks[0])
		dtable := make([][]float32, idx.m)
		for j := 0; j < idx.m; j++ {
			dtable[j] = make([]float32, actualKsub)
			offset := j * idx.subDim
			for code := 0; code < actualKsub; code++ {
				var dist float32
				for d := 0; d < idx.subDim; d++ {
					diff := vector[offset+d] - idx.centroids[ci][offset+d] - idx.pqCodebooks[j][code][d]
					dist += diff * diff
				}
				dtable[j][code] = dist
			}
		}

		for _, item := range idx.lists[ci] {
			var approxDist float32
			for j := 0; j < idx.m; j++ {
				approxDist += dtable[j][item.Codes[j]]
			}
			candidates = append(candidates, cand{id: item.ID, d: approxDist})
		}
	}

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

func (idx *ivfpqIndex) GetVector(id string) ([]float32, error) {
	if !idx.trained {
		for i, pendingID := range idx.pendingIDs {
			if pendingID == id {
				return append([]float32(nil), idx.pendingVectors[i]...), nil
			}
		}
		return nil, pkgerrors.ErrDocumentNotFound
	}
	for _, list := range idx.lists {
		for _, item := range list {
			if item.ID == id {
				return append([]float32(nil), item.Vector...), nil
			}
		}
	}
	return nil, pkgerrors.ErrDocumentNotFound
}

func (idx *ivfpqIndex) Load(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var snap ivfpqSnapshot
	if err := dec.Decode(&snap); err != nil {
		return err
	}
	idx.config = snap.Config
	idx.nlist = snap.Nlist
	idx.nprobe = snap.Nprobe
	idx.m = snap.M
	idx.nbits = snap.Nbits
	idx.dim = snap.Dim
	idx.subDim = snap.SubDim
	idx.centroids = snap.Centroids
	idx.pqCodebooks = snap.PQCodebooks
	idx.lists = snap.Lists
	idx.trained = snap.Trained
	idx.pendingIDs = nil
	idx.pendingVectors = nil
	return nil
}

func (idx *ivfpqIndex) Save(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	snap := ivfpqSnapshot{
		Config:      idx.config,
		Nlist:       idx.nlist,
		Nprobe:      idx.nprobe,
		M:           idx.m,
		Nbits:       idx.nbits,
		Dim:         idx.dim,
		SubDim:      idx.subDim,
		Centroids:   idx.centroids,
		PQCodebooks: idx.pqCodebooks,
		Lists:       idx.lists,
		Trained:     idx.trained,
	}
	logger.Debug("Saving index to file", "file", filePath)
	return enc.Encode(&snap)
}

func (idx *ivfpqIndex) Close() error {
	return nil
}

func (idx *ivfpqIndex) SetParams(params map[string]any) error {
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
			if err := idx.SetNProbe(ival); err != nil {
				return err
			}
		default:
			return pkgerrors.ErrInvalidParameter
		}
	}
	return nil
}

func (idx *ivfpqIndex) SetNProbe(nprobe int) error {
	if nprobe <= 0 || nprobe > idx.nlist {
		return pkgerrors.ErrInvalidParameter
	}
	idx.nprobe = nprobe
	return nil
}

///////////////////////// helpers /////////////////////////

func (idx *ivfpqIndex) closestCentroid(v []float32) int {
	best := 0
	bestDist := distance(v, idx.centroids[0], idx.config.SpaceType)
	for i := 1; i < len(idx.centroids); i++ {
		d := distance(v, idx.centroids[i], idx.config.SpaceType)
		if d < bestDist {
			bestDist = d
			best = i
		}
	}
	return best
}

// encodeVector quantizes a full vector into PQ codes relative to the given centroid.
func (idx *ivfpqIndex) encodeVector(vector []float32, centroidIdx int) []uint8 {
	codes := make([]uint8, idx.m)
	for j := 0; j < idx.m; j++ {
		offset := j * idx.subDim
		bestCode := 0
		bestDist := float32(0)
		for d := 0; d < idx.subDim; d++ {
			diff := vector[offset+d] - idx.centroids[centroidIdx][offset+d] - idx.pqCodebooks[j][0][d]
			bestDist += diff * diff
		}
		for code := 1; code < len(idx.pqCodebooks[j]); code++ {
			var dist float32
			for d := 0; d < idx.subDim; d++ {
				diff := vector[offset+d] - idx.centroids[centroidIdx][offset+d] - idx.pqCodebooks[j][code][d]
				dist += diff * diff
			}
			if dist < bestDist {
				bestDist = dist
				bestCode = code
			}
		}
		codes[j] = uint8(bestCode)
	}
	return codes
}
