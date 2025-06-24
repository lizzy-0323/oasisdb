package embedding

type EmbeddingProvider interface {
	Embed(text string) ([]float64, error)
	EmbedBatch(texts []string) ([][]float64, error)
}
