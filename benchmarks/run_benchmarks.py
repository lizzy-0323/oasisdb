from __future__ import annotations

import argparse
import csv
import gzip
import struct
import time
from pathlib import Path
from typing import Mapping, Sequence
from urllib.request import urlretrieve

import matplotlib.pyplot as plt
import numpy as np

import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "client-sdk" / "Python"))

from client import OasisDBClient, OasisDBError


# -------------------------- Dataset download and preprocessing -------------------------- #

FASHION_URLS = {
    "images": "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/train-images-idx3-ubyte.gz",
    "labels": "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/train-labels-idx1-ubyte.gz",
    "test_images": "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/t10k-images-idx3-ubyte.gz",
    "test_labels": "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/t10k-labels-idx1-ubyte.gz",
}

SIFT_BASE_URLS = [
    "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_base.fvecs?download=true",
    "ftp://ftp.irisa.fr/local/texmex/corpus/sift_base.fvecs",
]
SIFT_QUERY_URLS = [
    "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_query.fvecs?download=true",
    "ftp://ftp.irisa.fr/local/texmex/corpus/sift_query.fvecs",
]


def _download_if_missing(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    urlretrieve(url, path.as_posix())


def _download_with_fallback(urls: Sequence[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    errors: list[str] = []
    for url in urls:
        try:
            urlretrieve(url, path.as_posix())
            return
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    details = "\n".join(errors)
    raise RuntimeError(f"Failed to download {path.name} from all sources:\n{details}")


def _read_idx_images(path: Path) -> np.ndarray:
    with gzip.open(path, "rb") as f:
        magic, num, rows, cols = struct.unpack(">IIII", f.read(16))
        if magic != 2051:
            raise ValueError(f"Invalid magic number {magic} in {path}")
        buf = f.read(rows * cols * num)
        data = np.frombuffer(buf, dtype=np.uint8)
        data = data.reshape(num, rows * cols).astype("float32") / 255.0
        return data


def _read_fvecs(path: Path) -> np.ndarray:
    # Standard fvecs format:
    # [int32 dim][float32 x dim] repeated for each vector.
    raw = np.fromfile(path, dtype=np.int32)
    if raw.size == 0:
        raise ValueError(f"Empty fvecs file: {path}")

    dim = int(raw[0])
    if dim <= 0:
        raise ValueError(f"Invalid vector dimension {dim} in {path}")

    row_width = dim + 1
    if raw.size % row_width != 0:
        raise ValueError(
            f"Corrupted or incomplete fvecs file: {path}. "
            f"int32_count={raw.size}, expected a multiple of {row_width}."
        )

    mat = raw.reshape(-1, row_width)
    # Validate that each row stores the same dimension.
    if not np.all(mat[:, 0] == dim):
        raise ValueError(f"Inconsistent dimension headers in {path}")

    # Reinterpret payload as float32 without extra copy.
    return mat[:, 1:].view(np.float32)


def prepare_fashion(data_dir: Path) -> tuple[np.ndarray, np.ndarray]:
    images_path = data_dir / "fashion-train-images.gz"
    test_images_path = data_dir / "fashion-test-images.gz"

    _download_if_missing(FASHION_URLS["images"], images_path)
    _download_if_missing(FASHION_URLS["test_images"], test_images_path)

    train = _read_idx_images(images_path)
    test = _read_idx_images(test_images_path)

    return train, test


def prepare_sift(data_dir: Path) -> tuple[np.ndarray, np.ndarray]:
    base_path = data_dir / "sift_base.fvecs"
    query_path = data_dir / "sift_query.fvecs"

    _download_with_fallback(SIFT_BASE_URLS, base_path)
    _download_with_fallback(SIFT_QUERY_URLS, query_path)

    base = _read_fvecs(base_path)
    query = _read_fvecs(query_path)
    return base.astype("float32"), query.astype("float32")


def brute_force_topk(base: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    x2 = np.sum(base * base, axis=1)  # [nb]
    q2 = np.sum(queries * queries, axis=1, keepdims=True)  # [nq, 1]
    distances = q2 + x2[None, :] - 2.0 * queries @ base.T  # [nq, nb]
    idx_partial = np.argpartition(distances, kth=k - 1, axis=1)[:, :k]
    dist_partial = np.take_along_axis(distances, idx_partial, axis=1)
    order = np.argsort(dist_partial, axis=1)
    return np.take_along_axis(idx_partial, order, axis=1)


def compute_recall_at_k(gt_idx: np.ndarray, ann_idx: np.ndarray) -> float:
    assert gt_idx.shape == ann_idx.shape
    nq, k = gt_idx.shape
    hits = 0
    for i in range(nq):
        hits += len(set(gt_idx[i]).intersection(ann_idx[i]))
    return hits / (nq * k)


def batch_documents(
    base: np.ndarray,
    id_offset: int = 0,
) -> list[Mapping[str, object]]:
    nb = base.shape[0]
    ids = np.arange(id_offset, id_offset + nb, dtype=np.int64)
    vectors = base.tolist()
    return [{"id": str(i), "vector": v} for i, v in zip(ids, vectors)]


def run_single_benchmark(
    client: OasisDBClient,
    collection: str,
    base: np.ndarray,
    query: np.ndarray,
    k: int,
    efsearch_values: Sequence[int],
    out_csv: Path,
) -> None:
    dim = int(base.shape[1])

    # Try to reuse existing collection if it is already present.
    try:
        client.get_collection(collection)
    except OasisDBError:
        client.create_collection(collection, dimension=dim, index_type="hnsw")
        docs = batch_documents(base)
        client.batch_upsert_documents(collection, docs)
        client.build_index(collection, docs)

    gt_idx = brute_force_topk(base, query, k=k)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["efsearch", "qps", f"recall@{k}"])

        for ef in efsearch_values:
            client.set_params(collection, {"efsearch": ef})

            t0 = time.perf_counter()
            ann_indices: list[list[int]] = []
            for q in query:
                resp = client.search_vectors(collection, q.tolist(), limit=k)
                # /vectors/search returns {"ids": [...], "distances": [...]}
                ids = resp.get("ids", [])
                idxs = [int(doc_id) for doc_id in ids]
                if len(idxs) < k:
                    idxs.extend([-1] * (k - len(idxs)))
                ann_indices.append(idxs)
            elapsed = time.perf_counter() - t0

            ann_idx = np.asarray(ann_indices, dtype=int)
            recall = compute_recall_at_k(gt_idx, ann_idx)
            qps = len(query) / elapsed if elapsed > 0.0 else 0.0
            writer.writerow([ef, qps, recall])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        choices=["fashion", "sift"],
        required=True,
        help="Which dataset to benchmark.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("benchmarks/data"),
        help="Directory containing *_base.npy and *_query.npy files.",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=10,
        help="Top-k for recall and search.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output CSV path. If omitted, a default is chosen per dataset.",
    )
    parser.add_argument(
        "--efsearch",
        type=int,
        nargs="+",
        default=[10, 20, 40, 80, 120, 160],
        help="List of efsearch values to sweep.",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://localhost:8080",
        help="OasisDB HTTP endpoint.",
    )

    args = parser.parse_args()

    if args.dataset == "fashion":
        base, query = prepare_fashion(args.data_dir)
        collection = "fashion784"
        default_out = Path("benchmarks/fashion_results_python.csv")
    else:
        base, query = prepare_sift(args.data_dir)
        collection = "sift1m_subset"
        default_out = Path("benchmarks/sift_results_python.csv")

    out_csv = args.out or default_out

    with OasisDBClient(base_url=args.base_url) as client:
        if not client.health_check():
            raise RuntimeError(f"Health check failed for {args.base_url}")

        run_single_benchmark(
            client=client,
            collection=collection,
            base=base,
            query=query,
            k=args.k,
            efsearch_values=args.efsearch,
            out_csv=out_csv,
        )

    # Read CSV back for plotting. Use positional columns to avoid issues with
    # characters like '@' in the header names.
    data = np.genfromtxt(out_csv, delimiter=",", skip_header=1)
    # data has shape [num_rows, 3]: [efsearch, qps, recall@k]
    ef = data[:, 0]
    qps = data[:, 1]
    recall = data[:, 2]

    fig, ax1 = plt.subplots()
    ax1.set_xlabel("efsearch")
    ax1.set_ylabel("QPS", color="tab:blue")
    ax1.plot(ef, qps, "-o", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    ax2.set_ylabel(f"recall@{args.k}", color="tab:red")
    ax2.plot(ef, recall, "-s", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")

    fig.tight_layout()
    png_path = out_csv.with_suffix(".png")
    png_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(png_path, dpi=150)



if __name__ == "__main__":
    main()

