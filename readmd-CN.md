# OasisDB

[English](readme.md) | 简体中文

OasisDB 是一个高性能的向量数据库，设计理念是简单易用。通过单机部署和 RESTful API 接口，为您的应用程序提供高效的向量相似度搜索。

## 功能

1. 多种向量索引：HNSW(hnswlib), IVF(faiss)
2. 简单易用：单机部署和 RESTful API

## 架构

![架构](./docs/images/architecture.png)

## 快速开始

```bash
make build
./bin/oasisdb
```

## 许可证

[MIT](LICENSE)
