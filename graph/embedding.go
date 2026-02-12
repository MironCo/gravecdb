package graph

import (
	"math"
	"time"
)

// Embedding represents a versioned vector embedding for a node
type Embedding struct {
	NodeID    string
	Vector    []float32
	Model     string     // e.g., "text-embedding-3-small"
	ValidFrom time.Time
	ValidTo   *time.Time
}

// NewEmbedding creates a new embedding for a node
func NewEmbedding(nodeID string, vector []float32, model string) *Embedding {
	return &Embedding{
		NodeID:    nodeID,
		Vector:    vector,
		Model:     model,
		ValidFrom: time.Now(),
		ValidTo:   nil,
	}
}

// IsValidAt checks if this embedding was valid at a specific point in time
func (e *Embedding) IsValidAt(t time.Time) bool {
	if t.Before(e.ValidFrom) {
		return false
	}
	if e.ValidTo != nil && !t.Before(*e.ValidTo) {
		return false
	}
	return true
}

// CosineSimilarity computes cosine similarity between two vectors
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// EmbeddingStore manages versioned embeddings for nodes
type EmbeddingStore struct {
	embeddings map[string][]*Embedding // nodeID -> list of embedding versions
}

// NewEmbeddingStore creates a new embedding store
func NewEmbeddingStore() *EmbeddingStore {
	return &EmbeddingStore{
		embeddings: make(map[string][]*Embedding),
	}
}

// Add stores a new embedding for a node, closing out any previous embedding
func (s *EmbeddingStore) Add(nodeID string, vector []float32, model string) *Embedding {
	now := time.Now()

	// Close out the current embedding if one exists
	if existing := s.GetCurrent(nodeID); existing != nil {
		existing.ValidTo = &now
	}

	// Create and store the new embedding
	emb := &Embedding{
		NodeID:    nodeID,
		Vector:    vector,
		Model:     model,
		ValidFrom: now,
		ValidTo:   nil,
	}

	s.embeddings[nodeID] = append(s.embeddings[nodeID], emb)
	return emb
}

// GetCurrent returns the currently valid embedding for a node
func (s *EmbeddingStore) GetCurrent(nodeID string) *Embedding {
	versions := s.embeddings[nodeID]
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].ValidTo == nil {
			return versions[i]
		}
	}
	return nil
}

// GetAt returns the embedding that was valid at a specific time
func (s *EmbeddingStore) GetAt(nodeID string, t time.Time) *Embedding {
	versions := s.embeddings[nodeID]
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].IsValidAt(t) {
			return versions[i]
		}
	}
	return nil
}

// SearchResult represents a node with its similarity score
type SearchResult struct {
	NodeID     string
	Similarity float32
}

// Search finds the k most similar nodes to a query vector at a specific time
func (s *EmbeddingStore) Search(query []float32, k int, asOf time.Time, validNodeIDs map[string]bool) []SearchResult {
	var results []SearchResult

	for nodeID, versions := range s.embeddings {
		// Skip nodes that aren't valid at the query time
		if validNodeIDs != nil && !validNodeIDs[nodeID] {
			continue
		}

		// Find the embedding valid at the query time
		var emb *Embedding
		for i := len(versions) - 1; i >= 0; i-- {
			if versions[i].IsValidAt(asOf) {
				emb = versions[i]
				break
			}
		}

		if emb == nil {
			continue
		}

		similarity := CosineSimilarity(query, emb.Vector)
		results = append(results, SearchResult{
			NodeID:     nodeID,
			Similarity: similarity,
		})
	}

	// Sort by similarity (descending)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Similarity > results[i].Similarity {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	// Return top k
	if k > 0 && len(results) > k {
		results = results[:k]
	}

	return results
}