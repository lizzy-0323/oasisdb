# Python benchmarks (`run_benchmarks.py`)

This script benchmarks OasisDB over HTTP using **Fashion-MNIST** or a **SIFT1M-style** subset. With the OasisDB server running, it downloads data on first run (cached under `--data-dir`), upserts vectors, sweeps HNSW **`efsearch`**, measures **QPS** and **recall@k** against a brute-force ground truth, writes a **CSV**, and saves a **PNG** plot (QPS and recall vs. `efsearch`).

## Requirements

- Python 3.10+
- Packages: `numpy`, `matplotlib` (e.g. `pip install numpy matplotlib`)
- Run commands from the **repository root**. The script adds `client-sdk/Python` to `sys.path` and uses `client.py` there.

## Quick start

1. Start OasisDB so the HTTP API responds (health check must succeed). Default URL is `http://localhost:8080`.

2. Fashion-MNIST (small, good for a first run):

   ```bash
   python benchmarks/run_benchmarks.py --dataset fashion
   ```

3. SIFT (larger download):

   ```bash
   python benchmarks/run_benchmarks.py --dataset sift
   ```

## Common options

| Option | Description |
|--------|-------------|
| `--dataset` | **Required.** `fashion` or `sift`. |
| `--base-url` | OasisDB base URL. Default: `http://localhost:8080`. |
| `--data-dir` | Cache directory for downloaded files. Default: `benchmarks/data`. |
| `--k` | Top-k for search and recall. Default: `10`. |
| `--efsearch` | Space-separated list of `efsearch` values. Default: `10 20 40 80 120 160`. |
| `--out` | Output CSV path. If omitted: `benchmarks/fashion_results_python.csv` or `benchmarks/sift_results_python.csv` depending on dataset. |

The plot is written next to the CSV with the same basename and a `.png` extension.

## Notes

- If the target collection already exists on the server, the script reuses it instead of rebuilding from scratch every time.
- Ground truth is brute-force nearest neighbors on the vectors loaded into this run; align this with your index and distance semantics when interpreting recall.
