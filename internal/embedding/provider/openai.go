package provider

import (
	"errors"
	"fmt"
	"oasisdb/internal/embedding"
	"os"
)

const (
	MODEL   = "text-embedding-ada-002"
	API_URL = "https://api.openai.com/v1/embeddings"
)

type OpenAIEmbeddingProvider struct {
	apiKey string
	apiURL string
}

func NewEmbeddingProvider() (embedding.EmbeddingProvider, error) {
	apiKey, exists := os.LookupEnv("OPENAI_API_KEY")
	if !exists {
		return nil, errors.New("OPENAI_API_KEY not found")
	}
	return &OpenAIEmbeddingProvider{
		apiKey: apiKey,
		apiURL: API_URL,
	}, nil
}

func (e *OpenAIEmbeddingProvider) GetEmbeddings(input string) (*OpenAIEmbeddingResponse, error) {
	request, err := e.buildRequest(input)
	if err != nil {
		return nil, err
	}

	fmt.Println(request)
	return &OpenAIEmbeddingResponse{
		Data: []struct {
			Embedding []float64 `json:"embedding"`
		}{
			{
				Embedding: nil,
			},
		},
	}, nil
}

func (e *OpenAIEmbeddingProvider) buildRequest(input string) (*OpenAIEmbeddingRequest, error) {
	return &OpenAIEmbeddingRequest{
		Model: MODEL,
		Input: input,
	}, nil
}

func (e *OpenAIEmbeddingProvider) Embed(text string) ([]float64, error) {
	response, err := e.GetEmbeddings(text)
	if err != nil {
		return nil, err
	}

	if len(response.Data) == 0 {
		return nil, errors.New("no embeddings returned")
	}
	return response.Data[0].Embedding, nil
}

func (e *OpenAIEmbeddingProvider) EmbedBatch(texts []string) ([][]float64, error) {
	var embeddings [][]float64
	for _, text := range texts {
		embedding, err := e.Embed(text)
		if err != nil {
			return nil, err
		}
		embeddings = append(embeddings, embedding)
	}
	return embeddings, nil
}
