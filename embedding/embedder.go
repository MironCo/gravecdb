package embedding

// Embedder interface for generating text embeddings
type Embedder interface {
	Embed(text string) ([]float32, error)
	Model() string
}
