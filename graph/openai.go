package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// OpenAIEmbedder generates embeddings using OpenAI's API
type OpenAIEmbedder struct {
	apiKey string
	model  string
	client *http.Client
}

// NewOpenAIEmbedder creates a new OpenAI embedder
// If apiKey is empty, it will try to read from OPENAI_API_KEY environment variable
func NewOpenAIEmbedder(apiKey string) *OpenAIEmbedder {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	return &OpenAIEmbedder{
		apiKey: apiKey,
		model:  "text-embedding-3-small",
		client: &http.Client{},
	}
}

// NewOpenAIEmbedderWithModel creates an embedder with a specific model
func NewOpenAIEmbedderWithModel(apiKey, model string) *OpenAIEmbedder {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	return &OpenAIEmbedder{
		apiKey: apiKey,
		model:  model,
		client: &http.Client{},
	}
}

// embeddingRequest is the request body for OpenAI's embedding API
type embeddingRequest struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

// embeddingResponse is the response from OpenAI's embedding API
type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Model string `json:"model"`
	Usage struct {
		PromptTokens int `json:"prompt_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
}

// Embed generates an embedding for the given text
func (e *OpenAIEmbedder) Embed(text string) ([]float32, error) {
	if e.apiKey == "" {
		return nil, fmt.Errorf("OpenAI API key not set")
	}

	reqBody := embeddingRequest{
		Input: text,
		Model: e.model,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", "https://api.openai.com/v1/embeddings", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.apiKey)

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenAI API error (status %d): %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(embResp.Data) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	return embResp.Data[0].Embedding, nil
}

// Model returns the model name being used
func (e *OpenAIEmbedder) Model() string {
	return e.model
}

// EmbedAndStore generates an embedding for text and stores it on a node
func (e *OpenAIEmbedder) EmbedAndStore(g *Graph, nodeID string, text string) error {
	vector, err := e.Embed(text)
	if err != nil {
		return fmt.Errorf("failed to generate embedding: %w", err)
	}

	return g.SetNodeEmbedding(nodeID, vector, e.model)
}
