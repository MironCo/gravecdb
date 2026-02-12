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
// Format: gravecdb://[username:password@][host]/[datadir]?[options]
//
// Examples:
//
//	gravecdb://                                    (in-memory, no auth)
//	gravecdb:///data                               (persist to ./data, no auth)
//	gravecdb://admin:secret@/data                  (persist to ./data, with auth)
//	gravecdb://admin:secret@/data?embedder=ollama://localhost:11434
//	gravecdb://:memory:                            (explicit in-memory)
//
// Environment variables:
//
//	GRAVECDB_USERNAME - default username
//	GRAVECDB_PASSWORD - default password
//	GRAVECDB_DATA_DIR - default data directory
//	EMBEDDER_URL      - default embedder URL
func ParseDSN(dsn string) (*Config, error) {
	cfg := &Config{}

	// Handle empty DSN
	if dsn == "" || dsn == "gravecdb://" {
		return cfg, nil
	}

	// Parse scheme
	if !strings.HasPrefix(dsn, "gravecdb://") {
		return nil, fmt.Errorf("invalid DSN: must start with gravecdb://")
	}

	rest := strings.TrimPrefix(dsn, "gravecdb://")

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
		cfg.Username = os.Getenv("GRAVECDB_USERNAME")
	}
	if cfg.Password == "" {
		cfg.Password = os.Getenv("GRAVECDB_PASSWORD")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = os.Getenv("GRAVECDB_DATA_DIR")
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
// Format: gravecdb://[username:password@][host][:port]/[datadir]?[options]
//
// Examples:
//
//	gravecdb://localhost:8080/data
//	gravecdb://admin:secret@0.0.0.0:8080/data
//	gravecdb://admin:secret@:8080/data?embedder=ollama://localhost:11434
func ParseServerDSN(dsn string) (*ServerConfig, error) {
	cfg := &ServerConfig{
		Config: &Config{},
		Host:   "localhost",
		Port:   8080,
	}

	if dsn == "" || dsn == "gravecdb://" {
		return cfg, nil
	}

	if !strings.HasPrefix(dsn, "gravecdb://") {
		return nil, fmt.Errorf("invalid DSN: must start with gravecdb://")
	}

	rest := strings.TrimPrefix(dsn, "gravecdb://")

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
		cfg.Username = os.Getenv("GRAVECDB_USERNAME")
	}
	if cfg.Password == "" {
		cfg.Password = os.Getenv("GRAVECDB_PASSWORD")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = os.Getenv("GRAVECDB_DATA_DIR")
	}
	if cfg.EmbedderURL == "" {
		cfg.EmbedderURL = os.Getenv("EMBEDDER_URL")
	}
	if cfg.Host == "localhost" {
		if h := os.Getenv("GRAVECDB_HOST"); h != "" {
			cfg.Host = h
		}
	}
	if cfg.Port == 8080 {
		if p := os.Getenv("GRAVECDB_PORT"); p != "" {
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
