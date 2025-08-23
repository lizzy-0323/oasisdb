"""
使用 Fashion-MNIST 数据集对 OasisDB 进行基准测试的脚本。

该脚本将执行以下操作：
  1. 下载 Fashion-MNIST 数据集（如果本地不存在）。
  2. 连接到 OasisDB 服务并执行健康检查。
  3. 创建一个名为 'fashion_mnist' 的集合。
  4. 将所有 60,000 个训练向量插入集合并构建索引。
  5. 使用 10,000 个测试向量进行基准测试。
  6. 计算并打印 QPS（每秒查询率）和 Recall@100（召回率）。
  7. 通过删除集合来清理资源。
"""
from __future__ import annotations

import os
import sys
import time
from typing import List, Dict, Any

import h5py
import numpy as np
import requests
from tqdm import tqdm

# 确保 client SDK 在 Python 的搜索路径中
# 你可能需要根据你的项目结构调整这个路径
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from client import OasisDBClient, OasisDBError

# --- 1. 配置信息 ---
# 数据集配置
DATASET_URL = "http://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5"
DATASET_FILENAME = "client-sdk/Python/data/fashion-mnist-784-euclidean.hdf5"

# OasisDB 配置
COLLECTION_NAME = "fashion_mnist"
VECTOR_DIMENSION = 784  # Fashion-MNIST 向量是 784 维
TOP_K = 100 # 我们希望检索最相似的 Top 100 个结果

# --- 2. 数据加载工具 ---

def download_file(url: str, fname: str):
    """带进度条的文件下载函数。"""
    if os.path.exists(fname):
        print(f"数据集 '{fname}' 已存在，跳过下载。")
        return

    print(f"正在从 {url} 下载数据集到 {fname}...")
    try:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        total_size = int(resp.headers.get('content-length', 0))
        with open(fname, 'wb') as f, tqdm(
            total=total_size, unit='iB', unit_scale=True, desc=fname
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))
    except Exception as e:
        print(f"\n下载文件时出错: {e}")
        # 清理下载不完整的文件
        if os.path.exists(fname):
            os.remove(fname)
        sys.exit(1)

def load_fashion_mnist_data() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """下载并加载 Fashion-MNIST HDF5 数据集。"""
    download_file(DATASET_URL, DATASET_FILENAME)
    
    print(f"正在从 '{DATASET_FILENAME}' 加载数据到内存...")
    with h5py.File(DATASET_FILENAME, 'r') as f:
        base_vectors = np.array(f['train'])
        query_vectors = np.array(f['test'])
        # 真实结果 (Ground truth) 存储在 'neighbors' 键中
        ground_truth = np.array(f['neighbors'])
        
    print(f"数据加载完毕: {len(base_vectors)} 个底库向量, {len(query_vectors)} 个查询向量。")
    return base_vectors, query_vectors, ground_truth

# --- 3. 主要评测逻辑 ---

def main() -> None:
    # 初始化 OasisDB 客户端 (默认连接 http://localhost:8080)
    client = OasisDBClient()

    try:
        # 1. 健康检查
        ok = client.health_check()
        print("健康检查:", "OK" if ok else "FAILED")
        if not ok:
            print("OasisDB 服务未运行或无法访问。")
            sys.exit(1)
            
        # 可选：清理上次运行失败时可能残留的集合
        try:
            client.delete_collection(COLLECTION_NAME)
            print(f"已删除之前存在的集合 '{COLLECTION_NAME}'。")
        except OasisDBError:
            pass # 集合不存在，这很正常

        # 2. 从磁盘加载数据集
        base_vectors, query_vectors, ground_truth = load_fashion_mnist_data()

        # 3. 创建一个新集合
        print(f"正在创建集合 '{COLLECTION_NAME}' (维度: {VECTOR_DIMENSION})...")
        coll = client.create_collection(COLLECTION_NAME, dimension=VECTOR_DIMENSION)
        print("集合创建成功。")

        # 4. 准备并插入数据
        # 示例代码显示使用 'build_index' 来插入数据。
        # 我们需要将 numpy 数组格式化为 SDK 所期望的字典列表格式。
        print(f"正在准备 {len(base_vectors)} 个文档用于插入...")
        docs_to_insert: List[Dict[str, Any]] = [
            {"id": str(i), "vector": vector.tolist()}
            for i, vector in enumerate(tqdm(base_vectors, desc="格式化向量中"))
        ]
        
        print("正在插入文档并构建索引... (这可能需要一些时间)")
        start_insert_time = time.time()
        client.build_index(COLLECTION_NAME, docs_to_insert)
        end_insert_time = time.time()
        print(f"插入和索引构建完成，耗时 {end_insert_time - start_insert_time:.2f} 秒。")

        # 5. 运行基准测试
        print("\n--- 开始基准测试 ---")
        num_queries = len(query_vectors)
        all_results = []
        
        start_benchmark_time = time.time()
        for vec in tqdm(query_vectors, desc="执行搜索查询"):
            # 注意: 示例 SDK 的调用是 `search_vectors`。
            # 我们需要将 numpy 向量转换为列表来进行搜索。
            search_results = client.search_vectors(COLLECTION_NAME, vec.tolist(), limit=TOP_K)
            all_results.append(search_results)
        end_benchmark_time = time.time()

        # 6. 计算并报告结果
        total_time = end_benchmark_time - start_benchmark_time
        qps = num_queries / total_time
        
        # 计算召回率
        total_recall = 0
        for i, result_docs in enumerate(all_results):
            # search_vectors 的返回结果可能是一个字典列表，例如: [{'id': '42', 'score': 0.8}, ...]
            # 我们需要提取 ID 并将其转换为整数，以便与 ground_truth进行比较。
            result_ids = {int(doc_id) for doc_id in result_docs['ids']}
            ground_truth_ids = set(ground_truth[i][:TOP_K])
            
            intersection = len(result_ids.intersection(ground_truth_ids))
            total_recall += intersection / TOP_K
            
        avg_recall = total_recall / num_queries

        print("\n--- 基准测试结果 ---")
        print(f"总查询次数: {num_queries}")
        print(f"总耗时: {total_time:.4f} 秒")
        print(f"QPS (每秒查询率): {qps:.4f}")
        print(f"平均召回率 @{TOP_K}: {avg_recall:.4f}")

    except OasisDBError as e:
        print(f"\n与 OasisDB 服务器交互时发生错误: {e}")
    except Exception as e:
        print(f"\n发生未知错误: {e}")
    finally:
        # 先查看collection是否还在
        client.get_collection(COLLECTION_NAME)
        print("collection目前还在")
        # 7. 通过删除集合来清理资源
        print(f"\n正在清理资源，删除集合 '{COLLECTION_NAME}'...")
        try:
            client.delete_collection(COLLECTION_NAME)
            print("集合删除成功。")
        except OasisDBError as e:
            print(f"无法删除集合 (可能已被删除): {e}")
        
        client.close()


if __name__ == "__main__":
    main()