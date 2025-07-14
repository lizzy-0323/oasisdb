package internal

import (
	"bufio"
	"fmt"
	"log"
	"oasisdb/client-sdk/Go/client"
	"oasisdb/internal/embedding"
	"oasisdb/internal/embedding/provider"
	"os"
	"strings"
)

// RAG 结构体定义了一个RAG系统
type RAG struct {
	Embedder       embedding.EmbeddingProvider // 嵌入向量提供者接口
	Client         *client.OasisDBClient       // OasisDB 客户端
	CollectionName string                      // 集合名称
}

// NewRAG 创建一个新的RAG实例
func NewRAG(collectionName string) (*RAG, error) {
	embedder, err := provider.NewAliyunEmbeddingProvider()
	if err != nil {
		log.Fatalf("failed to create embedding provider: %v", err)
	}

	client := client.NewOasisDBClient("http://localhost:8080")

	// ping test
	check, err := client.HealthCheck()
	if check && err == nil {
		return &RAG{
			Embedder:       embedder,
			Client:         client,
			CollectionName: collectionName,
		}, nil
	}

	return nil, err
}

// TextToVector converts text to embedding vector
func (r *RAG) TextToVector(text string) ([]float32, error) {
	vector, err := r.Embedder.Embed(text)
	if err != nil {
		return nil, fmt.Errorf("embedding failed: %w", err)
	}
	// 将 []float64 转换为 []float32
	float32Vector := make([]float32, len(vector))
	for i, v := range vector {
		float32Vector[i] = float32(v)
	}
	return float32Vector, nil
}

// SearchSimilarContext 通过 OasisDB 客户端检索相似向量
func (r *RAG) SearchSimilarContext(queryVec []float32, k int) ([]string, error) {
	result, err := r.Client.SearchVectors(r.CollectionName, queryVec, k)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	idsRaw, ok := result["ids"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid search result format")
	}
	ids := make([]string, len(idsRaw))
	for i, v := range idsRaw {
		ids[i], _ = v.(string)
	}
	return ids, nil
}

// InitializeKnowledgeBase 初始化知识库
func (r *RAG) InitializeKnowledgeBase() error {
	// 创建集合前先删除
	_ = r.Client.DeleteCollection(r.CollectionName)

	vector, err := r.TextToVector("测试维度")
	if err != nil {
		return fmt.Errorf("failed to get embedding dimension: %w", err)
	}
	dim := len(vector)
	fmt.Printf("[INFO] 检测到 embedding 维度: %d\n", dim)

	_, err = r.Client.CreateCollection(r.CollectionName, dim, "hnsw", nil)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	// 检查集合实际维度
	coll, err := r.Client.GetCollection(r.CollectionName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}
	fmt.Printf("[INFO] 集合实际维度: %v\n", coll["dimension"])

	// 添加一些示例知识
	knowledgeData := []string{
		"人工智能是计算机科学的一个分支，致力于创建能够执行通常需要人类智能的任务的系统。",
		"机器学习是人工智能的一个子集，它使计算机能够在没有明确编程的情况下学习和改进。",
		"深度学习是机器学习的一个分支，使用神经网络来模拟人脑的学习过程。",
		"自然语言处理是人工智能的一个领域，专注于计算机理解和生成人类语言。",
		"计算机视觉是人工智能的一个分支，使计算机能够从图像和视频中获取信息。",
	}

	// 将知识转换为向量并存储
	for i, text := range knowledgeData {
		vector, err := r.TextToVector(text)
		if err != nil {
			return fmt.Errorf("failed to convert text to vector: %w", err)
		}
		fmt.Printf("upsert vector length: %d\n", len(vector))
		// 构造文档参数
		params := map[string]any{"text": text}
		_, err = r.Client.UpsertDocument(r.CollectionName, fmt.Sprintf("doc%d", i+1), vector, params)
		if err != nil {
			return fmt.Errorf("failed to upsert document: %w", err)
		}
	}

	log.Println("Knowledge base initialized successfully")
	return nil
}

// GetContextByIDs 根据ID获取上下文内容
func (r *RAG) GetContextByIDs(ids []string) ([]string, error) {
	var contexts []string
	for _, id := range ids {
		doc, err := r.Client.GetDocument(r.CollectionName, id)
		if err != nil {
			continue
		}
		params, ok := doc["parameters"].(map[string]any)
		if !ok || params == nil {
			continue
		}
		text, ok := params["text"].(string)
		if ok {
			contexts = append(contexts, text)
		}
	}
	return contexts, nil
}

// CallLLM 调用LLM获取答案（模拟实现）
func (r *RAG) CallLLM(question string, contexts []string) (string, error) {
	// 这里应该调用真实的LLM API，现在用模拟实现
	contextText := strings.Join(contexts, "\n")

	// 简单的模板回答
	answer := fmt.Sprintf("基于检索到的上下文信息：\n%s\n\n问题：%s\n\n回答：根据以上信息，这是一个关于AI相关技术的问题。",
		contextText, question)

	return answer, nil
}

// GetUserInput 获取用户输入
func GetUserInput(prompt string) string {
	fmt.Print(prompt)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}
