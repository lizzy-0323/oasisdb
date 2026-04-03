#!/usr/bin/env python3
"""Plot recall@k (x) vs QPS (y) from benchrecall CSV output."""

import argparse
import csv
import sys


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("-o", "--output", default="recall_qps.png")
    p.add_argument("-t", "--title", default="OasisDB HNSW")
    args = p.parse_args()
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is required: pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    xs, ys, efs = [], [], []
    with open(args.csv_path, newline="") as f:
        for row in csv.DictReader(f):
            xs.append(float(row["recall_at_k"]))
            ys.append(float(row["qps"]))
            efs.append(int(row["efsearch"]))

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(xs, ys, "o-", linewidth=1.5, markersize=6)
    for x, y, ef in zip(xs, ys, efs):
        ax.annotate(str(ef), (x, y), textcoords="offset points", xytext=(4, 4), fontsize=8)
    ax.set_xlabel("Recall@k")
    ax.set_ylabel("QPS")
    ax.set_title(args.title)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
