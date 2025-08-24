# OasisDB

[![Coverage Status](https://coveralls.io/repos/github/lizzy-0323/oasisdb/badge.svg?branch=main)](https://coveralls.io/github/lizzy-0323/oasisdb?branch=main)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<!-- ![logo](./docs/images/logo.png) -->

[![Build](https://github.com/lizzy-0323/oasisdb/actions/workflows/push_pr.yml/badge.svg)](https://github.com/lizzy-0323/oasisdb/actions/workflows/push_pr.yml)

English | [简体中文](readme-CN.md)

## 🚀 What is OasisDB?

**OasisDB** is a high-performance vector database designed for simplicity and ease of use. Unlike other complex and heavy vector databases, OasisDB provides efficient vector similarity search through both standalone deployment and RESTful API interfaces.

### 🎯 Perfect for Beginners

This project was created to help developers learn vector search easily and effectively. You can explore detailed design ideas and key concepts in our [Design Documentation](docs/design.md).

## ✨ Features

### 🔍 **Multiple Vector Index Types**

- **HNSW** (Hierarchical Navigable Small World) - Fast approximate search
- **IVFFLAT** (Inverted File with Flat compression) - Balanced performance and accuracy
- **Flat** - Exact search with maximum accuracy
- And more index types for different use cases

### ⚡ **Ultra-Lightweight Architecture**

- **Standalone deployment** as a single process
- **Zero internal network communication** - no complex distributed setup
- **Minimal resource footprint** for easy deployment

### 🤖 **Embedding Service Integration**

- **Built-in embedding support** for seamless vector generation
- 📖 Learn more in our [Embedding Documentation](docs/embedding.md)

### 🛠️ **Developer-Friendly**

- **RESTful API** for easy HTTP integration
- **Multi-language SDKs**: Python, Go, etc(under development)

## 🏗️ Architecture

![Architecture](./docs/images/architecture.png)

## 🚀 Quick Start

### Prerequisites

- Go 1.22+
- CMake 3.22+
- Python 3.10+
- uv(optional for package dependencies)

### Build from source

```bash
make build
./bin/oasisdb

# or you can use the script to start oasisdb
chmod +x ./scripts/start.sh
./scripts/start.sh
```

### Usage

You can use HTTP client to send request to oasisdb, and we recommend [uv](https://docs.astral.sh/uv/) to install Python dependencies.

```python
from client import OasisDBClient
client = OasisDBClient()
client.health_check()
```

For more usage, please see [API Documentation](docs/api.md),
you can also use [example.py](client-sdk/python/example.py) to see how to use it. And now we also provide Go client SDK, you can see the example in [example.go](client-sdk/go/example.go).

## 🤝 Contribution

I welcome any contributions to this project. Before contributing, please open an issue to discuss the changes you want to make.

If you want to start a PR for code changes, please follow the steps below to ensure the code quality:

```bash
make test
make lint # Ensure golangci-lint is installed
```

The contributors of this project are listed below, thank you all for your contributions!

[![contributors](https://contrib.rocks/image?repo=lizzy-0323/oasisdb)](https://github.com/lizzy-0323/oasisdb/graphs/contributors)

## 📝 License

OasisDB is licensed under [Apache 2.0 License](LICENSE)
