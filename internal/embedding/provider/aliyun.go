package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"oasisdb/internal/embedding"
	"os"
	"time"
)

const (
	MODEL   = "text-embedding-v4"
	API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"
)

type AliyunEmbeddingProvider struct {
	apiKey string
	apiURL string
}

func NewAliyunEmbeddingProvider() (embedding.EmbeddingProvider, error) {
	apiKey, exists := os.LookupEnv("DASHSCOPE_API_KEY")
	if !exists {
		return nil, errors.New("DASHSCOPE_API_KEY not found")
	}
	return &AliyunEmbeddingProvider{
		apiKey: apiKey,
		apiURL: API_URL,
	}, nil
}

func (e *AliyunEmbeddingProvider) GetEmbeddings(input string) (*AliyunEmbeddingResponse, error) {
	requestPayload, err := e.buildRequest(input)
	if err != nil {
		return nil, err
	}

	bodyBytes, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest("POST", e.apiURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", e.apiKey))

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("DashScope embeddings API returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var embeddingResp AliyunEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embeddingResp); err != nil {
		return nil, err
	}

	return &embeddingResp, nil
}

func (e *AliyunEmbeddingProvider) buildRequest(input string) (*AliyunEmbeddingRequest, error) {
	return &AliyunEmbeddingRequest{
		Model:          MODEL,
		Input:          input,
		EncodingFormat: "float",
	}, nil
}

func (e *AliyunEmbeddingProvider) Embed(text string) ([]float64, error) {
	response, err := e.GetEmbeddings(text)
	if err != nil {
		return nil, err
	}

	if len(response.Data) == 0 {
		return nil, errors.New("no embeddings returned")
	}
	return response.Data[0].Embedding, nil
}

func (e *AliyunEmbeddingProvider) EmbedBatch(texts []string) ([][]float64, error) {
	// Build request with slice input
	req := &AliyunEmbeddingRequest{
		Model:          MODEL,
		Input:          texts,
		EncodingFormat: "float",
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest("POST", e.apiURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", e.apiKey))

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("DashScope embeddings API returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var embeddingResp AliyunEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embeddingResp); err != nil {
		return nil, err
	}

	if len(embeddingResp.Data) != len(texts) {
		return nil, fmt.Errorf("expected %d embeddings, got %d", len(texts), len(embeddingResp.Data))
	}

	embeddings := make([][]float64, len(embeddingResp.Data))
	for i, item := range embeddingResp.Data {
		embeddings[i] = item.Embedding
	}

	return embeddings, nil
}
