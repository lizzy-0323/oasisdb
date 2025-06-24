package embedding

// EmbeddingProvider is an interface for embedding providers
type EmbeddingProvider interface {
	Embed(text string) ([]float64, error)
	EmbedBatch(texts []string) ([][]float64, error)
}
