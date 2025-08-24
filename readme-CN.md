# OasisDB

[![Coverage Status](https://coveralls.io/repos/github/lizzy-0323/oasisdb/badge.svg?branch=main)](https://coveralls.io/github/lizzy-0323/oasisdb?branch=main)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<!-- ![logo](./docs/images/logo.png) -->
English | **简体中文**

## 🚀 什么是 OasisDB？

**OasisDB** 是一款以简洁性和易用性为核心的高性能向量数据库。与其他复杂的向量数据库不同，OasisDB 通过单进程部署和 RESTful API 接口提供高效的向量相似度检索功能。

### ✨ 核心特性

- **超轻量级**：**小于 20MB** 的体积
- **零依赖**：无需内部网络通信
- **易于部署**：简单的设置和配置，一键启动
- **初学者友好**：完美适合学习向量检索概念

### 🎯 专为初学者设计

该项目旨在帮助开发者轻松有效地学习向量检索。您可以在我们的[设计文档](docs/design-CN.md)中探索详细的设计思路和关键概念。

## ✨ 功能特点

### 🔍 **多种向量索引类型**

- **HNSW**（分层可导航小世界）- 快速近似搜索
- **IVFFLAT**（倒排文件与平面压缩）- 性能与精度的平衡
- **Flat** - 精确搜索，最高精度
- 更多索引类型适用于不同使用场景

### ⚡ **超轻量级架构**

- **单进程部署**，无需复杂配置
- **零内部网络通信** - 无需复杂的分布式设置
- **最小资源占用**，便于部署

### 🤖 **嵌入服务集成**

- **内置嵌入支持**，无缝生成向量
- 📖 更多详情请参阅[嵌入文档](docs/embedding.md)

### 🛠️ **开发者友好**

- **RESTful API** 便于 HTTP 集成
- **多语言 SDK**：Python, Go, etc(under development)

## 🏗️ 架构

![架构](./docs/images/architecture.png)

## 🚀 快速开始

### 前置条件

- Go 1.22+
- CMake 3.22+
- Python 3.10+
- uv（可选，用于依赖管理）

### 从源码构建

```bash
make build
./bin/oasisdb
```

### 使用示例

您可以使用 HTTP 请求或 Python 客户端与 OasisDB 交互。以下示例使用 `uv` 安装依赖，并展示最简单的健康检查：

更多用法请参阅 [apidoc](docs/api.md)，或查看示例脚本 [example.py](example.py)。

## 🤝 贡献指南

欢迎任何形式的贡献！在提交代码之前，请先通过 issue 讨论您的想法。

若要提交 PR，请确保通过以下步骤保证代码质量：

```bash
make test
make lint
```

## 📝 许可证

[Apache 2.0](LICENSE)
