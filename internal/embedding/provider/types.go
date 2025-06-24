package provider

type OpenAIEmbeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type OpenAIEmbeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}
