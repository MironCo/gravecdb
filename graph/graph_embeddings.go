package graph

import (
	"fmt"
	"time"
)

// SetNodeEmbedding stores a vector embedding for a node
// Previous embeddings are automatically versioned (ValidTo is set)
func (g *Graph) SetNodeEmbedding(nodeID string, vector []float32, model string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	node, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	// Create a deep copy of the node's current properties
	propertySnapshot := make(map[string]interface{})
	for k, v := range node.Properties {
		propertySnapshot[k] = v
	}

	g.embeddings.Add(nodeID, vector, model, propertySnapshot)

	// Persist to bbolt if enabled (save all embedding versions for this node)
	if g.boltStore != nil {
		// Get all versions from embedding store
		versions := g.embeddings.GetAll(nodeID)
		if err := g.boltStore.SaveEmbedding(nodeID, versions); err != nil {
			return fmt.Errorf("failed to persist embedding to bbolt: %w", err)
		}
	}

	return nil
}

// GetNodeEmbedding returns the current embedding for a node
func (g *Graph) GetNodeEmbedding(nodeID string) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetCurrent(nodeID)
}

// GetNodeEmbeddingAt returns the embedding that was valid at a specific time
func (g *Graph) GetNodeEmbeddingAt(nodeID string, t time.Time) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetAt(nodeID, t)
}

// SemanticSearch finds the k most similar nodes to a query vector
// Only searches nodes that are valid at the current time
func (g *Graph) SemanticSearch(query []float32, k int) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Build set of currently valid node IDs
	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsCurrentlyValid() {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, time.Now(), validNodeIDs)
}

// SemanticSearchAt finds the k most similar nodes to a query vector at a specific time
// Only searches nodes that were valid at the given time
func (g *Graph) SemanticSearchAt(query []float32, k int, asOf time.Time) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Build set of node IDs that were valid at the given time
	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsValidAt(asOf) {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, asOf, validNodeIDs)
}

// SemanticSearchAllVersions finds all historical versions of nodes similar to a query vector
// Returns all embedding versions across all time periods that match the query
func (g *Graph) SemanticSearchAllVersions(query []float32, k int, threshold float32, labelFilter []string, calculateDrift bool) []VersionedSearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Build set of valid node IDs (nodes that have ever existed)
	// Apply label filtering if specified
	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		// If label filter is specified, check if node has any of those labels
		if len(labelFilter) > 0 {
			hasLabel := false
			for _, label := range labelFilter {
				for _, nodeLabel := range node.Labels {
					if nodeLabel == label {
						hasLabel = true
						break
					}
				}
				if hasLabel {
					break
				}
			}
			if !hasLabel {
				continue
			}
		}
		validNodeIDs[id] = true
	}

	return g.embeddings.SearchAllVersions(query, k, validNodeIDs, threshold, calculateDrift)
}
