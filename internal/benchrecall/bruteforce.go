package benchrecall

import (
	"sort"
	"strconv"
)

// TopKL2 returns indices of the k smallest L2 distances from query to base (full scan).
func TopKL2(query []float32, base [][]float32, k int) []int {
	if k <= 0 || len(base) == 0 {
		return nil
	}
	if k > len(base) {
		k = len(base)
	}
	type item struct {
		idx int
		d   float32
	}
	dim := len(query)
	dists := make([]item, len(base))
	for i := range base {
		var s float64
		v := base[i]
		for j := 0; j < dim && j < len(v); j++ {
			d := float64(v[j]) - float64(query[j])
			s += d * d
		}
		dists[i] = item{idx: i, d: float32(s)}
	}
	sort.Slice(dists, func(i, j int) bool {
		if dists[i].d == dists[j].d {
			return dists[i].idx < dists[j].idx
		}
		return dists[i].d < dists[j].d
	})
	out := make([]int, k)
	for i := 0; i < k; i++ {
		out[i] = dists[i].idx
	}
	return out
}

// RecallAtK computes |intersection(pred, truth)| / k for the first k elements of pred vs truth set.
func RecallAtK(pred, truth []int, k int) float64 {
	if k <= 0 {
		return 0
	}
	set := make(map[int]struct{}, len(truth))
	for _, id := range truth {
		set[id] = struct{}{}
	}
	var hit int
	for i := 0; i < k && i < len(pred); i++ {
		if _, ok := set[pred[i]]; ok {
			hit++
		}
	}
	return float64(hit) / float64(k)
}

// RecallAtKIDs parses decimal document IDs from HNSW search and compares to ground-truth indices.
func RecallAtKIDs(predIDs []string, truthIdx []int, k int) float64 {
	if k <= 0 {
		return 0
	}
	set := make(map[int]struct{}, len(truthIdx))
	for _, id := range truthIdx {
		set[id] = struct{}{}
	}
	var hit int
	for i := 0; i < k && i < len(predIDs); i++ {
		n, err := strconv.Atoi(predIDs[i])
		if err != nil {
			continue
		}
		if _, ok := set[n]; ok {
			hit++
		}
	}
	return float64(hit) / float64(k)
}
