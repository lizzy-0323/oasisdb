package index

const (
	L2Space      SpaceType = "l2"
	IPSpace      SpaceType = "ip"
	CosSpace     SpaceType = "cos"
	HammingSpace SpaceType = "hamming"
)

const (
	HNSWIndex    IndexType = "hnsw"
	IVFFLATIndex IndexType = "ivf_flat"
	FLATIndex    IndexType = "flat"
)

// HNSW specific constants
const (
	DEFAULT_M               = 16
	DEFAULT_EF_CONSTRUCTION = 200
	DEFAULT_MAX_ELEMENTS    = 100000
	DEFAULT_BUILD_THREADS   = 4
)

// IVF specific constants
const (
	DEFAULT_MAX_KMEANS_ITER = 40
	DEFAULT_NLIST           = 100
	DEFAULT_NPROBE          = 10
)
