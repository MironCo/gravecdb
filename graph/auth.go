package graph

import (
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"
)

// AuthMiddleware provides HTTP Basic Auth middleware for the graph database server
type AuthMiddleware struct {
	username string
	password string
	realm    string
}

// NewAuthMiddleware creates a new auth middleware
// If username and password are empty, all requests are allowed
func NewAuthMiddleware(username, password string) *AuthMiddleware {
	return &AuthMiddleware{
		username: username,
		password: password,
		realm:    "gravecdb",
	}
}

// NewAuthMiddlewareFromConfig creates auth middleware from a Config
func NewAuthMiddlewareFromConfig(cfg *Config) *AuthMiddleware {
	return NewAuthMiddleware(cfg.Username, cfg.Password)
}

// IsEnabled returns true if authentication is enabled
func (m *AuthMiddleware) IsEnabled() bool {
	return m.username != "" || m.password != ""
}

// Authenticate checks the request for valid credentials
// Returns true if auth is disabled or credentials are valid
func (m *AuthMiddleware) Authenticate(r *http.Request) bool {
	if !m.IsEnabled() {
		return true
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}

	// Parse Basic auth
	if !strings.HasPrefix(auth, "Basic ") {
		return false
	}

	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
	if err != nil {
		return false
	}

	pair := strings.SplitN(string(payload), ":", 2)
	if len(pair) != 2 {
		return false
	}

	// Constant-time comparison to prevent timing attacks
	usernameMatch := subtle.ConstantTimeCompare([]byte(pair[0]), []byte(m.username)) == 1
	passwordMatch := subtle.ConstantTimeCompare([]byte(pair[1]), []byte(m.password)) == 1

	return usernameMatch && passwordMatch
}

// Handler returns an http.Handler that wraps another handler with auth
func (m *AuthMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.Authenticate(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// HandlerFunc returns a middleware function compatible with common routers
func (m *AuthMiddleware) HandlerFunc(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.Authenticate(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// GinMiddleware returns a middleware function for Gin router
// Usage: r.Use(auth.GinMiddleware())
func (m *AuthMiddleware) GinMiddleware() func(c interface{ Next(); AbortWithStatus(int); GetHeader(string) string; Header(string, string) }) {
	return func(c interface {
		Next()
		AbortWithStatus(int)
		GetHeader(string) string
		Header(string, string)
	}) {
		if !m.IsEnabled() {
			c.Next()
			return
		}

		auth := c.GetHeader("Authorization")
		if auth == "" {
			c.Header("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		if !strings.HasPrefix(auth, "Basic ") {
			c.Header("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
		if err != nil {
			c.Header("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		pair := strings.SplitN(string(payload), ":", 2)
		if len(pair) != 2 {
			c.Header("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		usernameMatch := subtle.ConstantTimeCompare([]byte(pair[0]), []byte(m.username)) == 1
		passwordMatch := subtle.ConstantTimeCompare([]byte(pair[1]), []byte(m.password)) == 1

		if !usernameMatch || !passwordMatch {
			c.Header("WWW-Authenticate", `Basic realm="`+m.realm+`"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		c.Next()
	}
}
