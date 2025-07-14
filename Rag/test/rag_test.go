package test

import (
	"oasisdb/Rag/internal"
	"testing"
)

func TestRAG_InitializeAndSearch(t *testing.T) {
	// 创建 RAG 实例
	rag, err := internal.NewRAG("demo")
	if err != nil {
		t.Fatalf("RAG 初始化失败: %v", err)
	}

	// 初始化知识库
	err = rag.InitializeKnowledgeBase()
	if err != nil {
		t.Fatalf("知识库初始化失败: %v", err)
	}

	// 测试向量检索
	query := "什么是人工智能？"
	queryVec, err := rag.TextToVector(query)
	if err != nil {
		t.Fatalf("向量化失败: %v", err)
	}

	ids, err := rag.SearchSimilarContext(queryVec, 1)
	if err != nil {
		t.Fatalf("向量检索失败: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("未检索到相关文档")
	}

	// 检查能否获取上下文
	contexts, err := rag.GetContextByIDs(ids)
	if err != nil {
		t.Fatalf("获取上下文失败: %v", err)
	}
	if len(contexts) == 0 {
		t.Fatalf("未获取到上下文内容")
	}
}
