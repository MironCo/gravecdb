package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/miron/go-graph-database/graph"
)

// Client represents a connection to a graph database server
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// Connect creates a new client connection to a graph database server
// Example: client.Connect("http://localhost:8080")
func Connect(baseURL string) (*Client, error) {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

// Query executes a Cypher-like query and returns the result
func (c *Client) Query(queryStr string) (*graph.QueryResult, error) {
	reqBody := map[string]string{
		"query": queryStr,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/api/query",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed: %s - %s", resp.Status, string(body))
	}

	var result graph.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode result: %w", err)
	}

	return &result, nil
}

// CreateNode creates a new node with the given labels and properties
func (c *Client) CreateNode(labels []string, properties map[string]interface{}) (string, error) {
	reqBody := map[string]interface{}{
		"labels":     labels,
		"properties": properties,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/api/nodes",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create node: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create node failed: %s - %s", resp.Status, string(body))
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode result: %w", err)
	}

	return result["id"], nil
}

// CreateRelationship creates a new relationship between two nodes
func (c *Client) CreateRelationship(relType, fromID, toID string, properties map[string]interface{}) (string, error) {
	reqBody := map[string]interface{}{
		"type":       relType,
		"from":       fromID,
		"to":         toID,
		"properties": properties,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/api/relationships",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create relationship: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create relationship failed: %s - %s", resp.Status, string(body))
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode result: %w", err)
	}

	return result["id"], nil
}

// DeleteNode soft-deletes a node by ID
func (c *Client) DeleteNode(id string) error {
	req, err := http.NewRequest("DELETE", c.baseURL+"/api/nodes/"+id, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete node failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

// DeleteRelationship soft-deletes a relationship by ID
func (c *Client) DeleteRelationship(id string) error {
	req, err := http.NewRequest("DELETE", c.baseURL+"/api/relationships/"+id, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete relationship: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete relationship failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

// GetGraph returns the current graph state
func (c *Client) GetGraph() (map[string]interface{}, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/api/graph")
	if err != nil {
		return nil, fmt.Errorf("failed to get graph: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get graph failed: %s - %s", resp.Status, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode result: %w", err)
	}

	return result, nil
}

// GetGraphAsOf returns the graph state at a specific time
func (c *Client) GetGraphAsOf(t time.Time) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/graph/asof?t=%s", c.baseURL, t.Format(time.RFC3339))

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get graph: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get graph failed: %s - %s", resp.Status, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode result: %w", err)
	}

	return result, nil
}

// Close closes the client connection (cleanup if needed)
func (c *Client) Close() error {
	// No-op for HTTP client, but provided for API consistency
	return nil
}
