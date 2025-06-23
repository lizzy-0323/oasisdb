# OasisDB

[![Coverage Status](https://coveralls.io/repos/github/lizzy-0323/oasisdb/badge.svg?branch=main)](https://coveralls.io/github/lizzy-0323/oasisdb?branch=main)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
<!-- ![logo](./docs/images/logo.png) -->

<!-- [![Build](https://github.com/lizzy-0323/oasisdb/actions/workflows/push_pr.yml/badge.svg)](https://github.com/lizzy-0323/oasisdb/actions/workflows/push_pr.yml) -->

English | [简体中文](readmd-CN.md)

OasisDB is a high-performance vector database designed for simplicity and ease of use. It enables efficient vector similarity search for your applications through both standalone deployment and RESTful API interfaces.

I start this project for all the beginners to learn vector search very easily, and you can see the detail design ideas and related key knowledge in [design](docs/design.md).

## Features

1. Multiple type of vector index: HNSW(hnswlib), IVF(faiss)
2. Lightweight: standalone deployment as one process, **do not have any internal network communication**.
3. Embedding support: multiple embedding models
4. Easy to use: RESTful API and Python SDK

## Architecture

![Architecture](./docs/images/architecture.png)

## Quick Start

### Prerequisites

- Go 1.22+
- CMake 3.22+
- Python 3.10+
- uv(optional)

### Build from source

```bash
make build
./bin/oasisdb
```

### Usage

you can use http client to send request to oasisdb, and we choose uv to install python dependencies.

```python
from client import OasisDBClient
client = OasisDBClient()
client.health_check()
```
For more usage, please see [apidoc](docs/api.md),
you can also use [example.py](example.py) to see how to use it.

## Contribution

I will be very happy if you can contribute to this project. before contributing, please open an issue to discuss the changes you want to make.

If you want to start a PR for code changes, please follow the steps below to ensure the code quality:

```bash
make lint
make test
```

## License

[MIT License](LICENSE)
