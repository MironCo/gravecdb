package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// OllamaEmbedder generates embeddings using Ollama's local API
type OllamaEmbedder struct {
	baseURL string
	model   string
	client  *http.Client
}

// NewOllamaEmbedder creates a new Ollama embedder with default settings
// Uses localhost:11434 and nomic-embed-text model
func NewOllamaEmbedder() *OllamaEmbedder {
	return &OllamaEmbedder{
		baseURL: "http://localhost:11434",
		model:   "nomic-embed-text",
		client:  &http.Client{},
	}
}

// NewOllamaEmbedderWithConfig creates an Ollama embedder with custom settings
func NewOllamaEmbedderWithConfig(baseURL, model string) *OllamaEmbedder {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	if model == "" {
		model = "nomic-embed-text"
	}
	return &OllamaEmbedder{
		baseURL: baseURL,
		model:   model,
		client:  &http.Client{},
	}
}

type ollamaEmbedRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbedResponse struct {
	Embedding []float64 `json:"embedding"`
}

// Embed generates an embedding for the given text
func (e *OllamaEmbedder) Embed(text string) ([]float32, error) {
	reqBody := ollamaEmbedRequest{
		Model:  e.model,
		Prompt: text,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := e.client.Post(
		e.baseURL+"/api/embeddings",
		"application/json",
		bytes.NewBuffer(jsonBody),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama API error (status %d): %s", resp.StatusCode, string(body))
	}

	var embResp ollamaEmbedResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(embResp.Embedding) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	// Convert float64 to float32
	result := make([]float32, len(embResp.Embedding))
	for i, v := range embResp.Embedding {
		result[i] = float32(v)
	}

	return result, nil
}

// Model returns the model name being used
func (e *OllamaEmbedder) Model() string {
	return e.model
}

// EmbedAndStore generates an embedding for text and stores it on a node
func (e *OllamaEmbedder) EmbedAndStore(g *Graph, nodeID string, text string) error {
	vector, err := e.Embed(text)
	if err != nil {
		return fmt.Errorf("failed to generate embedding: %w", err)
	}

	return g.SetNodeEmbedding(nodeID, vector, e.model)
}
