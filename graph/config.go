package graph

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds configuration for creating a graph database connection
type Config struct {
	// Data directory for persistence (empty = in-memory only)
	DataDir string

	// Authentication
	Username string
	Password string

	// Embedder configuration (optional)
	EmbedderURL string // e.g., "ollama://localhost:11434/nomic-embed-text"
}

// ParseDSN parses a connection string into a Config
// Format: graphdb://[username:password@][host]/[datadir]?[options]
//
// Examples:
//
//	graphdb://                                    (in-memory, no auth)
//	graphdb:///data                               (persist to ./data, no auth)
//	graphdb://admin:secret@/data                  (persist to ./data, with auth)
//	graphdb://admin:secret@/data?embedder=ollama://localhost:11434
//	graphdb://:memory:                            (explicit in-memory)
//
// Environment variables:
//
//	GRAPHDB_USERNAME - default username
//	GRAPHDB_PASSWORD - default password
//	GRAPHDB_DATA_DIR - default data directory
//	EMBEDDER_URL     - default embedder URL
func ParseDSN(dsn string) (*Config, error) {
	cfg := &Config{}

	// Handle empty DSN
	if dsn == "" || dsn == "graphdb://" {
		return cfg, nil
	}

	// Parse scheme
	if !strings.HasPrefix(dsn, "graphdb://") {
		return nil, fmt.Errorf("invalid DSN: must start with graphdb://")
	}

	rest := strings.TrimPrefix(dsn, "graphdb://")

	// Check for explicit in-memory
	if rest == ":memory:" {
		return cfg, nil
	}

	// Parse credentials if present (username:password@)
	if strings.Contains(rest, "@") {
		atIdx := strings.Index(rest, "@")
		creds := rest[:atIdx]
		rest = rest[atIdx+1:]

		if strings.Contains(creds, ":") {
			parts := strings.SplitN(creds, ":", 2)
			cfg.Username = parts[0]
			cfg.Password = parts[1]
		} else {
			cfg.Username = creds
		}
	}

	// Parse query params if present
	if strings.Contains(rest, "?") {
		qIdx := strings.Index(rest, "?")
		queryStr := rest[qIdx+1:]
		rest = rest[:qIdx]

		// Parse query parameters
		for _, param := range strings.Split(queryStr, "&") {
			if strings.Contains(param, "=") {
				kv := strings.SplitN(param, "=", 2)
				switch kv[0] {
				case "embedder":
					cfg.EmbedderURL = kv[1]
				}
			}
		}
	}

	// Rest is the data directory (may start with /)
	if rest != "" && rest != "/" {
		cfg.DataDir = strings.TrimPrefix(rest, "/")
	}

	// Apply environment variable defaults
	if cfg.Username == "" {
		cfg.Username = os.Getenv("GRAPHDB_USERNAME")
	}
	if cfg.Password == "" {
		cfg.Password = os.Getenv("GRAPHDB_PASSWORD")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = os.Getenv("GRAPHDB_DATA_DIR")
	}
	if cfg.EmbedderURL == "" {
		cfg.EmbedderURL = os.Getenv("EMBEDDER_URL")
	}

	return cfg, nil
}

// Open creates a new graph database from the config
func (cfg *Config) Open() (*Graph, error) {
	if cfg.DataDir == "" {
		return NewGraph(), nil
	}
	return NewGraphWithPersistence(cfg.DataDir)
}

// RequiresAuth returns true if authentication is configured
func (cfg *Config) RequiresAuth() bool {
	return cfg.Username != "" || cfg.Password != ""
}

// ValidateCredentials checks if the provided credentials are valid
func (cfg *Config) ValidateCredentials(username, password string) bool {
	if !cfg.RequiresAuth() {
		return true
	}
	return cfg.Username == username && cfg.Password == password
}

// GetEmbedder creates an embedder from the config (if configured)
func (cfg *Config) GetEmbedder() (Embedder, error) {
	if cfg.EmbedderURL == "" {
		return nil, nil
	}
	return NewEmbedderFromURL(cfg.EmbedderURL)
}

// ServerConfig holds configuration for the HTTP server
type ServerConfig struct {
	*Config

	// Server settings
	Host string
	Port int

	// CORS settings
	AllowOrigins []string

	// TLS settings (optional)
	TLSCert string
	TLSKey  string
}

// ParseServerDSN parses a server connection string
// Format: graphdb://[username:password@][host][:port]/[datadir]?[options]
//
// Examples:
//
//	graphdb://localhost:8080/data
//	graphdb://admin:secret@0.0.0.0:8080/data
//	graphdb://admin:secret@:8080/data?embedder=ollama://localhost:11434
func ParseServerDSN(dsn string) (*ServerConfig, error) {
	cfg := &ServerConfig{
		Config: &Config{},
		Host:   "localhost",
		Port:   8080,
	}

	if dsn == "" || dsn == "graphdb://" {
		return cfg, nil
	}

	if !strings.HasPrefix(dsn, "graphdb://") {
		return nil, fmt.Errorf("invalid DSN: must start with graphdb://")
	}

	rest := strings.TrimPrefix(dsn, "graphdb://")

	// Parse credentials if present
	if strings.Contains(rest, "@") {
		atIdx := strings.Index(rest, "@")
		creds := rest[:atIdx]
		rest = rest[atIdx+1:]

		if strings.Contains(creds, ":") {
			parts := strings.SplitN(creds, ":", 2)
			cfg.Username = parts[0]
			cfg.Password = parts[1]
		} else {
			cfg.Username = creds
		}
	}

	// Parse query params
	if strings.Contains(rest, "?") {
		qIdx := strings.Index(rest, "?")
		queryStr := rest[qIdx+1:]
		rest = rest[:qIdx]

		for _, param := range strings.Split(queryStr, "&") {
			if strings.Contains(param, "=") {
				kv := strings.SplitN(param, "=", 2)
				switch kv[0] {
				case "embedder":
					cfg.EmbedderURL = kv[1]
				case "cors":
					cfg.AllowOrigins = strings.Split(kv[1], ",")
				case "tls_cert":
					cfg.TLSCert = kv[1]
				case "tls_key":
					cfg.TLSKey = kv[1]
				}
			}
		}
	}

	// Parse host:port/datadir
	if strings.Contains(rest, "/") {
		slashIdx := strings.Index(rest, "/")
		hostPort := rest[:slashIdx]
		cfg.DataDir = rest[slashIdx+1:]
		rest = hostPort
	}

	// Parse host:port
	if rest != "" {
		if strings.Contains(rest, ":") {
			parts := strings.SplitN(rest, ":", 2)
			if parts[0] != "" {
				cfg.Host = parts[0]
			}
			if parts[1] != "" {
				port, err := strconv.Atoi(parts[1])
				if err != nil {
					return nil, fmt.Errorf("invalid port: %s", parts[1])
				}
				cfg.Port = port
			}
		} else {
			cfg.Host = rest
		}
	}

	// Apply environment variable defaults
	if cfg.Username == "" {
		cfg.Username = os.Getenv("GRAPHDB_USERNAME")
	}
	if cfg.Password == "" {
		cfg.Password = os.Getenv("GRAPHDB_PASSWORD")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = os.Getenv("GRAPHDB_DATA_DIR")
	}
	if cfg.EmbedderURL == "" {
		cfg.EmbedderURL = os.Getenv("EMBEDDER_URL")
	}
	if cfg.Host == "localhost" {
		if h := os.Getenv("GRAPHDB_HOST"); h != "" {
			cfg.Host = h
		}
	}
	if cfg.Port == 8080 {
		if p := os.Getenv("GRAPHDB_PORT"); p != "" {
			port, err := strconv.Atoi(p)
			if err == nil {
				cfg.Port = port
			}
		}
	}

	return cfg, nil
}

// Address returns the server address (host:port)
func (cfg *ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}
