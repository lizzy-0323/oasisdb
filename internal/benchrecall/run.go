package benchrecall

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"oasisdb/internal/config"
	"oasisdb/internal/db"
)

// Row is one (ef_search, recall, qps) sample for recall–QPS curves.
type Row struct {
	EfSearch int
	Recall   float64
	QPS      float64
}

// Config drives the in-process OasisDB benchmark.
type Config struct {
	// Dataset is "fashion" (784-dim Fashion-MNIST) or "sift1m" (128-dim ANN_SIFT1M).
	Dataset        string
	DataDir        string
	Download       bool
	BaseN          int
	QueryN         int
	K              int
	EfSearchValues []int
	WarmupQueries  int
	Collection     string
}

// Run builds an HNSW collection, computes exact top-K neighbors by brute force on the base set,
// then sweeps efSearch (Fashion-MNIST or SIFT1M subset — see Dataset).
func Run(cfg Config) ([]Row, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	base, queries, dim, err := loadDataset(cfg)
	if err != nil {
		return nil, err
	}

	gt := make([][]int, len(queries))
	for i, q := range queries {
		gt[i] = TopKL2(q, base, cfg.K)
	}

	tmp, err := os.MkdirTemp("", "oasisdb-benchrecall-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.RemoveAll(tmp)
	}()

	conf, err := config.NewConfig(tmp, config.WithLogLevel("error"))
	if err != nil {
		return nil, err
	}
	database, err := db.New(conf)
	if err != nil {
		return nil, err
	}
	err = database.Open()
	if err != nil {
		return nil, err
	}
	defer database.Close() // close DB before temp dir cleanup (defer order: Close then RemoveAll)

	_, err = database.CreateCollection(&db.CreateCollectionOptions{
		Name:       cfg.Collection,
		Dimension:  dim,
		IndexType:  "hnsw",
		Parameters: map[string]string{},
	})
	if err != nil {
		return nil, err
	}

	docs := make([]*db.Document, cfg.BaseN)
	for i := 0; i < cfg.BaseN; i++ {
		id := strconv.Itoa(i)
		docs[i] = &db.Document{
			ID:        id,
			Vector:    base[i],
			Dimension: dim,
		}
	}
	err = database.BuildIndex(cfg.Collection, docs)
	if err != nil {
		return nil, err
	}

	idx, err := database.IndexManager.GetIndex(cfg.Collection)
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(cfg.EfSearchValues))
	for _, ef := range cfg.EfSearchValues {
		err = idx.SetParams(map[string]any{"efsearch": ef})
		if err != nil {
			return nil, fmt.Errorf("SetParams efsearch=%d: %w", ef, err)
		}
		for w := 0; w < cfg.WarmupQueries && w < len(queries); w++ {
			_, _, _ = database.SearchVectors(cfg.Collection, queries[w], cfg.K)
		}
		start := time.Now()
		recSum := 0.0
		for qi := 0; qi < len(queries); qi++ {
			ids, _, searchErr := database.SearchVectors(cfg.Collection, queries[qi], cfg.K)
			if searchErr != nil {
				return nil, searchErr
			}
			recSum += RecallAtKIDs(ids, gt[qi], cfg.K)
		}
		elapsed := time.Since(start).Seconds()
		qps := float64(len(queries)) / elapsed
		rows = append(rows, Row{
			EfSearch: ef,
			Recall:   recSum / float64(len(queries)),
			QPS:      qps,
		})
	}
	return rows, nil
}

func validateConfig(cfg *Config) error {
	if cfg.Collection == "" {
		cfg.Collection = "bench"
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("DataDir is required")
	}
	if cfg.BaseN <= 0 || cfg.QueryN <= 0 || cfg.K <= 0 {
		return fmt.Errorf("BaseN, QueryN, K must be positive")
	}
	if len(cfg.EfSearchValues) == 0 {
		return fmt.Errorf("EfSearchValues is required")
	}
	if cfg.Dataset == "" {
		cfg.Dataset = "fashion"
	}
	return nil
}

func loadDataset(cfg Config) (base, queries [][]float32, dim int, err error) {
	switch cfg.Dataset {
	case "fashion":
		return loadFashion(cfg)
	case "sift1m":
		return loadSIFT(cfg)
	default:
		return nil, nil, 0, fmt.Errorf("unknown dataset %q (use fashion or sift1m)", cfg.Dataset)
	}
}

func loadFashion(cfg Config) (base, queries [][]float32, dim int, err error) {
	gzPath := filepath.Join(cfg.DataDir, defaultTrainFile)
	if cfg.Download {
		gzPath, err = DownloadFashionTrainImages(cfg.DataDir)
		if err != nil {
			return nil, nil, 0, err
		}
	} else {
		st, statErr := os.Stat(gzPath)
		if statErr != nil || st.Size() == 0 {
			return nil, nil, 0, fmt.Errorf("dataset missing at %s (use -download)", gzPath)
		}
	}
	need := cfg.BaseN + cfg.QueryN
	vecs, err := LoadFashionVectors(gzPath, need)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(vecs) < need {
		return nil, nil, 0, fmt.Errorf("need %d vectors, file has %d", need, len(vecs))
	}
	return vecs[:cfg.BaseN], vecs[cfg.BaseN : cfg.BaseN+cfg.QueryN], len(vecs[0]), nil
}

func loadSIFT(cfg Config) (base, queries [][]float32, dim int, err error) {
	basePath, queryPath, err := EnsureSIFT1MFiles(cfg.DataDir, cfg.Download, cfg.BaseN, cfg.QueryN)
	if err != nil {
		return nil, nil, 0, err
	}
	base, queries, err = LoadSIFT1M(basePath, queryPath, cfg.BaseN, cfg.QueryN)
	if err != nil {
		return nil, nil, 0, err
	}
	return base, queries, SIFT1MDim, nil
}
