package main

import (
	"fmt"
	"log"
	"oasisdb/Rag/internal"
	"strings"
)

func main() {
	fmt.Println("=== RAG 系统启动 ===")

	// 初始化 RAG 系统
	rag ,err:= internal.NewRAG("demo")
	if err!=nil {
		panic("Can`t find ")
	}

	// 初始化知识库
	fmt.Println("Initialize Knowledge Base...")
	err = rag.InitializeKnowledgeBase()
	if err != nil {
		log.Fatalf("failed to initialize knowledge base: %v", err)
	}
	fmt.Println("Initialize Success ! ")

	// 交互式问答循环
	for {
		fmt.Println("\n" + strings.Repeat("=", 50))

		// Step 1: 获取用户问题
		question := internal.GetUserInput("请输入您的问题 (输入 'quit' 退出): ")
		if question == "quit" || question == "exit" {
			fmt.Println("感谢使用 RAG 系统，再见！")
			break
		}
		if question == "" {
			continue
		}

		fmt.Printf("您的问题: %s\n", question)

		// Step 2: 将问题转换为向量
		fmt.Println("正在将问题转换为向量...")
		questionVec, err := rag.TextToVector(question)
		if err != nil {
			log.Printf("text to vector failed: %v", err)
			continue
		}
		fmt.Printf("向量转换完成，维度: %d\n", len(questionVec))

		// Step 3: 检索相似上下文
		fmt.Println("正在检索相关知识...")
		similarIDs, err := rag.SearchSimilarContext(questionVec, 3)
		if err != nil {
			log.Printf("vector search failed: %v", err)
			continue
		}
		fmt.Printf("找到 %d 个相关文档\n", len(similarIDs))

		// Step 4: 获取上下文内容
		contexts, err := rag.GetContextByIDs(similarIDs)
		if err != nil {
			log.Printf("failed to get contexts: %v", err)
			continue
		}

		// Step 5: 调用 LLM 生成答案
		fmt.Println("正在生成答案...")
		answer, err := rag.CallLLM(question, contexts)
		if err != nil {
			log.Printf("LLM call failed: %v", err)
			continue
		}

		// 输出答案
		fmt.Println("\n=== 答案 ===")
		fmt.Println(answer)
		fmt.Println("===========")
	}
}

//Todo:
// helper funcion for user can input text
// func inputContext(){

// }
