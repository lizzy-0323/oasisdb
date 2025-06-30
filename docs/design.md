
# Design Document

## Introduction
Currently, there are many proprietary vector database components available for users to choose from, including but not limited to the following:

- Milvus
- Qdrant
- Pinecone
- Weaviate
- Chroma

Among them, Milvus is a relatively well-developed vector database with comprehensive functionality and a good community, therefore OasisDB has heavily referenced Milvus's design.

However, unlike OasisDB, Milvus is actually a distributed vector database architecture with node partitioning, which creates a certain learning curve for beginners.

Besides these specialized vector databases, some existing database vendors are providing vector search capabilities for their existing databases. I believe this might be a future trend because, for users, while vector search performance is important, maintainability and simplicity are equally important. With the development of AI, more and more users will prefer to build systems with fewer components, thereby reducing system maintenance costs. Products in this category include OceanBase, pgvector, Redis-Search, and others.

Given all of this, why build another vector database? The reason is simple: I want to use the simplest architecture to help beginners quickly understand the basic components of a vector database. OasisDB avoids any internal RPC communication to ensure the system is absolutely lightweight, and it doesn't provide any distributed capabilities to ensure system maintainability.

Therefore, OasisDB can be considered simple, and also "incomplete", but I believe this is precisely its value. If you want to try vector database functionality, understand its principles, or self-host a RAG system, then OasisDB is an excellent choice.

## Architecture Design

The overall architecture of OasisDB can be seen in this diagram: ![Architecture Design](./images/architecture.png)

In summary, it can be divided into the following parts:

1. Gateway
2. LRU Cache
3. Vector Storage
4. Scalar Storage
5. Embedding Service (Optional)

Among these, the essential parts are Vector Storage and Scalar Storage. We can simply consider a standalone vector database to be composed of these two parts.

The Gateway component is straightforward, primarily providing RESTful APIs for user interaction.

The LRU Cache is mainly for caching popular vector searches. This cache should not be set too large, or it will consume a lot of memory. Additionally, we need to identify cached results by the (vector, topk) combination. If a query requests a larger topk than what was previously cached, the cached results cannot be used as a substitute. The code for this is in the `internal/cache` directory.

Vector Storage is primarily for storing vector indexes and saving them. I've implemented both HNSW index based on hnswlib and IVF index for different scenarios. The IVF index has lower accuracy but works well for small-scale, high-dimensional data, while HNSW is the opposite. The HNSW index code is in the `internal/engine` directory, and the IVF index code is in the `internal/ivf` directory.

Scalar Storage is mainly for storing vector metadata, which can be implemented with a KV store. I've implemented a KV storage based on the LSM tree, with code in the `internal/storage` directory.

Embedding Service is mainly for providing vector embedding functionality. To make the user experience better, this is an optional feature, and I only support the vector embedding service provided by Alibaba Cloud. The code is in the `internal/embedding` directory.

## Technology Selection

The technology selection is kept as simple as possible, introducing only the hnswlib library while implementing everything else from scratch. This is to be friendly to Go developers and beginners, who don't need to refer to other library implementations, thus reducing the learning curve.

## Implementation Details

Here are several implementation details that should be noted:
1. First, all parts that interact with the disk should adopt a WAL (Write-Ahead Logging) mechanism to enable failure recovery. For vector storage, the `ApplyOpWithWAL` function implements the WAL mechanism for all operations. In addition, vector storage needs to add snapshot functionality. Currently, users can call this themselves, but we also need to add an automatic snapshot mechanism, and whether to immediately write to disk needs to be configurable. This part still needs improvement.

2. For scalar storage, a relatively standard LSM tree structure is used, similar to RocksDB's implementation. The advantage of the LSM tree is that it converts random writes to sequential writes, greatly improving write performance. For vectors, large batch writes are often needed, so this is very reasonable. The memtable architecture uses a Skip List implementation, which can be referenced in the code at `internal/storage/memtable.go`.

3. How to implement filtering queries? Currently, filtering queries are still in the design phase. There are three common approaches: pre-filtering, post-filtering, and in-memory filtering. The most challenging to implement is in-memory filtering, which requires specific data structures to complete the process during retrieval. Pre-filtering requires filtering all data first and then performing vector retrieval, which is costly. Post-filtering performs filtering after retrieval, but if the original topk is used for the query, the results after filtering may not be enough, so the topk for retrieval needs to be adjusted. A simple approach is to set it to twice the user-defined topk.
