# OasisDB

[![Coverage Status](https://coveralls.io/repos/github/lizzy-0323/oasisdb/badge.svg?branch=main)](https://coveralls.io/github/lizzy-0323/oasisdb?branch=main)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![logo](./docs/images/logo.png)
[English](readme.md) | 简体中文

OasisDB 是一个高性能的向量数据库，设计理念是简单易用。通过单机部署和 RESTful API 接口，为您的应用程序提供高效的向量相似度搜索。

## 功能

1. 多种向量索引：HNSW(hnswlib), IVF(faiss)
2. 简单易用：单机部署和 RESTful API
3. Embedding 支持：多种 embedding 模型
4. Python HTTP client：帮助您轻松地与 OasisDB 交互

## 架构

![架构](./docs/images/architecture.png)

## 快速开始

### 构建

```bash
make build
./bin/oasisdb
```

### 使用

您可以通过 HTTP client 发送请求到 OasisDB。

## 许可证

[MIT](LICENSE)
