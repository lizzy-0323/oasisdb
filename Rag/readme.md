# Rag 目录说明

本目录为 OasisDB 项目的 RAG（Retrieval-Augmented Generation，检索增强生成）系统实现，主要用于结合向量数据库与大语言模型（LLM）实现智能问答。

## 主要功能
- 支持知识库初始化与管理，将文本转为向量存入 OasisDB
- 基于用户问题进行向量检索，返回最相关的知识片段
- 集成 LLM（如 Gemini），基于检索到的上下文生成答案
- 提供自动化测试与示例代码

## 目录结构
- `main.go`：程序入口，演示 RAG 流程，包括知识库初始化、问题输入、检索与 LLM 调用
- `internal/`：核心实现代码
    - `rag.go`：RAG 主要逻辑，包括向量化、检索、上下文获取、LLM 调用等
    - 其他 embedding、client 等子模块
- `test/`：自动化测试代码，覆盖 RAG 初始化、检索、LLM 调用等流程

## 运行方法

1. **启动 OasisDB 服务**
   - 请确保 OasisDB 已在本地或指定服务器启动。例如：
     ```bash
     # 假设已编译好 oasisdb 可执行文件
     ./oasisdb 
     ```
   - 启动后默认监听 http://localhost:8080

2. **准备环境变量**
   - 需设置 `GCP_API_KEY` 环境变量，用于 Gemini LLM 调用：
     ```bash
     export GCP_API_KEY=你的APIKey
     ```
3. **运行示例**
   ```bash
   cd Rag
   go run .
   ```
   按提示输入问题，体验 RAG 智能问答流程。

4. **运行测试**
   ```bash
   cd Rag
   go test ./test/...
   ```

## 注意事项
- 需先启动 OasisDB 服务并保证可访问
- API Key 请勿写入代码，注意安全
- 支持自定义 embedding provider 和 LLM 接入


## 内置示例知识

本项目在初始化知识库时，内置了以下示例知识（可在 `Rag/internal/rag.go` 文件的 `InitializeKnowledgeBase` 方法中查找和修改）：

- 人工智能是计算机科学的一个分支，致力于创建能够执行通常需要人类智能的任务的系统。
- 机器学习是人工智能的一个子集，它使计算机能够在没有明确编程的情况下学习和改进。
- 深度学习是机器学习的一个分支，使用神经网络来模拟人脑的学习过程。
- 自然语言处理是人工智能的一个领域，专注于计算机理解和生成人类语言。
- 计算机视觉是人工智能的一个分支，使计算机能够从图像和视频中获取信息。


