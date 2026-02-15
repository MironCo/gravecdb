package graph

import (
	"fmt"
	"time"
)

// setNodeEmbedding stores a vector embedding for a node
func (g *memGraph) setNodeEmbedding(nodeID string, vector []float32, model string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	node, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	propertySnapshot := make(map[string]interface{})
	for k, v := range node.Properties {
		propertySnapshot[k] = v
	}

	g.embeddings.Add(nodeID, vector, model, propertySnapshot)

	return nil
}

// getNodeEmbedding returns the current embedding for a node
func (g *memGraph) getNodeEmbedding(nodeID string) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetCurrent(nodeID)
}

// getNodeEmbeddingAt returns the embedding that was valid at a specific time
func (g *memGraph) getNodeEmbeddingAt(nodeID string, t time.Time) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetAt(nodeID, t)
}

// semanticSearch finds the k most similar nodes to a query vector
func (g *memGraph) semanticSearch(query []float32, k int) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsCurrentlyValid() {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, time.Now(), validNodeIDs)
}

// semanticSearchAt finds the k most similar nodes to a query vector at a specific time
func (g *memGraph) semanticSearchAt(query []float32, k int, asOf time.Time) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsValidAt(asOf) {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, asOf, validNodeIDs)
}

// semanticSearchAllVersions finds all historical versions of nodes similar to a query vector
func (g *memGraph) semanticSearchAllVersions(query []float32, k int, threshold float32, labelFilter []string, calculateDrift bool) []VersionedSearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
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
