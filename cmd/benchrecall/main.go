// Command benchrecall reproduces the OasisDB recall–QPS workflow from issue #29 / #43 / #44:
// Fashion-MNIST (784-d) or SIFT1M subset (128-d), HNSW, sweep efSearch, CSV (recall vs QPS).
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"oasisdb/internal/benchrecall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	dataset := flag.String("dataset", "fashion", "dataset: fashion | sift1m")
	dataDir := flag.String("data", "", "data directory (default: benchmarks/data or benchmarks/data/sift1m)")
	download := flag.Bool("download", false, "download dataset files if missing")
	baseN := flag.Int("nbase", 0, "number of indexed vectors (default: 10000 fashion, 20000 sift1m)")
	queryN := flag.Int("nquery", 0, "number of query vectors (default: 500 fashion, 500 sift1m)")
	k := flag.Int("k", 10, "top-k for recall@k")
	efList := flag.String("ef", "16,32,64,128,256", "comma-separated efSearch values")
	warmup := flag.Int("warmup", 2, "warmup queries per efSearch value")
	out := flag.String("o", "", "optional CSV output path (default: stdout)")
	flag.Parse()

	data := *dataDir
	if data == "" {
		if *dataset == "sift1m" {
			data = "benchmarks/data/sift1m"
		} else {
			data = "benchmarks/data"
		}
	}
	nb, nq := *baseN, *queryN
	if nb == 0 {
		if *dataset == "sift1m" {
			nb = 20000
		} else {
			nb = 10000
		}
	}
	if nq == 0 {
		nq = 500
	}

	efs, err := parseInts(*efList)
	if err != nil {
		return fmt.Errorf("bad -ef: %w", err)
	}

	cfg := benchrecall.Config{
		Dataset:        *dataset,
		DataDir:        data,
		Download:       *download,
		BaseN:          nb,
		QueryN:         nq,
		K:              *k,
		EfSearchValues: efs,
		WarmupQueries:  *warmup,
	}

	rows, err := benchrecall.Run(cfg)
	if err != nil {
		return fmt.Errorf("benchrecall: %w", err)
	}

	var output *os.File
	w := csv.NewWriter(os.Stdout)
	if *out != "" {
		output, err = os.Create(*out)
		if err != nil {
			return fmt.Errorf("create %s: %w", *out, err)
		}
		defer output.Close()
		w = csv.NewWriter(output)
	}
	_ = w.Write([]string{"efsearch", "recall_at_k", "qps"})
	for _, r := range rows {
		_ = w.Write([]string{
			strconv.Itoa(r.EfSearch),
			strconv.FormatFloat(r.Recall, 'f', 6, 64),
			strconv.FormatFloat(r.QPS, 'f', 2, 64),
		})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("csv: %w", err)
	}
	return nil
}

func parseInts(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	var out []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
