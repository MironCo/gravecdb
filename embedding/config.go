package embedding

import (
	"fmt"
	"os"
	"strings"
)

// Config holds configuration for creating an embedder
type Config struct {
	Provider string // "ollama", "openai"
	Model    string // model name (optional, uses defaults)
	BaseURL  string // API base URL (optional, uses defaults)
	APIKey   string // API key (for openai)
}

// NewFromConfig creates an embedder from config
func NewFromConfig(cfg Config) (Embedder, error) {
	switch strings.ToLower(cfg.Provider) {
	case "ollama":
		return NewOllamaEmbedderWithConfig(cfg.BaseURL, cfg.Model), nil
	case "openai":
		apiKey := cfg.APIKey
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI_API_KEY")
		}
		if cfg.Model != "" {
			return NewOpenAIEmbedderWithModel(apiKey, cfg.Model), nil
		}
		return NewOpenAIEmbedder(apiKey), nil
	default:
		return nil, fmt.Errorf("unknown embedder provider: %s (supported: ollama, openai)", cfg.Provider)
	}
}

// NewFromURL creates an embedder from a connection string
// Format: provider://[apikey@]host[:port][/model]
//
// Examples:
//
//	ollama://localhost:11434/nomic-embed-text
//	ollama://localhost/mxbai-embed-large
//	ollama://                                    (uses defaults)
//	openai://                                    (uses OPENAI_API_KEY env var)
//	openai://sk-xxx@api.openai.com/text-embedding-3-large
func NewFromURL(url string) (Embedder, error) {
	// Parse provider
	parts := strings.SplitN(url, "://", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid embedder URL format, expected: provider://[apikey@]host[:port][/model]")
	}

	provider := strings.ToLower(parts[0])
	rest := parts[1]

	cfg := Config{Provider: provider}

	// Parse apikey@host/model
	if strings.Contains(rest, "@") {
		atParts := strings.SplitN(rest, "@", 2)
		cfg.APIKey = atParts[0]
		rest = atParts[1]
	}

	// Parse host/model
	if strings.Contains(rest, "/") {
		slashIdx := strings.Index(rest, "/")
		host := rest[:slashIdx]
		cfg.Model = rest[slashIdx+1:]
		rest = host
	}

	// Parse host:port -> baseURL
	if rest != "" {
		switch provider {
		case "ollama":
			if !strings.Contains(rest, ":") {
				rest = rest + ":11434"
			}
			cfg.BaseURL = "http://" + rest
		case "openai":
			if rest != "" && rest != "api.openai.com" {
				cfg.BaseURL = "https://" + rest
			}
		}
	}

	return NewFromConfig(cfg)
}

// Default returns the default embedder based on environment
// Checks EMBEDDER_URL env var first, then falls back to Ollama localhost
func Default() (Embedder, error) {
	if url := os.Getenv("EMBEDDER_URL"); url != "" {
		return NewFromURL(url)
	}

	// Default to local Ollama
	return NewOllamaEmbedder(), nil
}
