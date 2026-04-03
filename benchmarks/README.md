# OasisDB benchmarks (issues #29, #43, #44)

Recall–QPS curves for **HNSW** inside a real in-process `oasisdb` `DB`: download data, build index, brute-force **L2 top-k** on the indexed subset as ground truth, sweep **`efSearch`**, emit CSV (`efsearch`, `recall_at_k`, `qps`). Plot **recall** on the x-axis and **QPS** on the y-axis.

## Datasets

| Flag | Dataset | Notes |
|------|---------|--------|
| `-dataset fashion` (default) | [Fashion-MNIST](https://github.com/zalandoresearch/fashion-mnist) 784-d | `train-images-idx3-ubyte.gz` (~25 MB). Default `-data benchmarks/data`. |
| `-dataset sift1m` | [ANN_SIFT1M](http://corpus-texmex.irisa.fr/) 128-d (`.fvecs`) | Base/query mirrored from [Hugging Face](https://huggingface.co/datasets/qbo-odp/sift1m). Default `-data benchmarks/data/sift1m`. By default only a **prefix** of `sift_base.fvecs` is downloaded (enough for `-nbase`). |

Brute-force ground truth is computed over the **indexed base vectors only** (first `nbase` from base file / Fashion train). This matches a clean local benchmark; it is not identical to full 1M Texmex leaderboard settings unless you set `nbase` to 1M and accept long GT time (or extend the tool with official `.ivecs` ground truth).

## Quick start (Fashion-MNIST)

```bash
(base) ➜  ~/MyPlaygroud/oasisdb git:(issue-29-add-benchmarks) ✗ mkdir -p benchmarks/data     
go run ./cmd/benchrecall \                                      
  -download \                                                 
  -dataset fashion \
  -data benchmarks/data/fashion_minist \
  -nbase 20000 \
  -nquery 1000 \
  -k 10 \
  -ef "8,16,32,64,128,256" \
  -o benchmarks/fashion_results.csv

python benchmarks/plot_recall_qps.py \
  benchmarks/fashion_results.csv \
  -o benchmarks/fashion_recall_qps.png \
  --title "OasisDB HNSW (Fashion-MNIST)"
```

## Quick start (SIFT1M subset)

```bash
mkdir -p benchmarks/data/sift1m                                 
go run ./cmd/benchrecall \                                      
  -download \                                                 
  -dataset sift1m \ 
  -data benchmarks/data/sift1m \        
  -nbase 20000 \
  -nquery 500 \ 
  -k 10 \
  -ef "8,16,32,64,128,256" \
  -o benchmarks/sift_results.csv   

python benchmarks/plot_recall_qps.py \
  benchmarks/sift_results.csv \       
  -o benchmarks/sift_recall_qps.png \
  --title "OasisDB HNSW (SIFT1M subset)"
```

or use full 1M base:
```bash
go run ./cmd/benchrecall -dataset sift1m -download \
  -nbase 1000000 -nquery 10000 -k 10 -ef "8,16,32,64,128,256" \
  -o benchmarks/sift_full_results.csv
```

Tune sweep:

```bash
go run ./cmd/benchrecall -dataset fashion -data benchmarks/data -nbase 20000 -nquery 1000 -k 10 -ef "8,16,32,64,128,256" -o benchmarks/fashion_results.csv
```

Build CLI:

```bash
make bench-recall
./bin/benchrecall -h
```

## Git

Downloaded `.gz` and `sift_*.fvecs` under `benchmarks/data/` are ignored; commit CSVs or figures only if you want to share results.
