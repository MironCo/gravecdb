package embedding

import (
	"math"
	"time"
)

// Embedding represents a versioned vector embedding for a node
type Embedding struct {
	NodeID           string
	Vector           []float32
	Model            string     // e.g., "text-embedding-3-small"
	ValidFrom        time.Time
	ValidTo          *time.Time
	PropertySnapshot map[string]interface{} // Snapshot of node properties when embedding was created
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
// The validity range is [ValidFrom, ValidTo) - inclusive start, exclusive end
func (e *Embedding) IsValidAt(t time.Time) bool {
	if t.Before(e.ValidFrom) {
		return false
	}
	// The validity range is [ValidFrom, ValidTo) - inclusive start, exclusive end
	if e.ValidTo != nil && (t.After(*e.ValidTo) || t.Equal(*e.ValidTo)) {
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

// CosineDistance computes cosine distance (1 - similarity) between two vectors
func CosineDistance(a, b []float32) float32 {
	return 1.0 - CosineSimilarity(a, b)
}

// Store manages versioned embeddings for nodes
type Store struct {
	embeddings map[string][]*Embedding // nodeID -> list of embedding versions
}

// NewStore creates a new embedding store
func NewStore() *Store {
	return &Store{
		embeddings: make(map[string][]*Embedding),
	}
}

// LoadEmbedding inserts an embedding directly, preserving its original ValidFrom/ValidTo.
// Use this when loading persisted embeddings from disk (not for new embeddings).
func (s *Store) LoadEmbedding(emb *Embedding) {
	s.embeddings[emb.NodeID] = append(s.embeddings[emb.NodeID], emb)
}

// Add stores a new embedding for a node, closing out any previous embedding
func (s *Store) Add(nodeID string, vector []float32, model string, propertySnapshot map[string]interface{}) *Embedding {
	now := time.Now()

	// Close out the current embedding if one exists
	if existing := s.GetCurrent(nodeID); existing != nil {
		existing.ValidTo = &now
	}

	// Create and store the new embedding
	emb := &Embedding{
		NodeID:           nodeID,
		Vector:           vector,
		Model:            model,
		ValidFrom:        now,
		ValidTo:          nil,
		PropertySnapshot: propertySnapshot,
	}

	s.embeddings[nodeID] = append(s.embeddings[nodeID], emb)
	return emb
}

// GetCurrent returns the currently valid embedding for a node
func (s *Store) GetCurrent(nodeID string) *Embedding {
	versions := s.embeddings[nodeID]
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].ValidTo == nil {
			return versions[i]
		}
	}
	return nil
}

// GetAt returns the embedding that was valid at a specific time
func (s *Store) GetAt(nodeID string, t time.Time) *Embedding {
	versions := s.embeddings[nodeID]
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].IsValidAt(t) {
			return versions[i]
		}
	}
	return nil
}

// GetAll returns all embedding versions for a node
func (s *Store) GetAll(nodeID string) []*Embedding {
	return s.embeddings[nodeID]
}

// SearchResult represents a node with its similarity score
type SearchResult struct {
	NodeID     string
	Similarity float32
}

// VersionedSearchResult represents a node version with its similarity score and temporal validity
type VersionedSearchResult struct {
	NodeID            string
	Similarity        float32
	ValidFrom         time.Time
	ValidTo           *time.Time
	Embedding         *Embedding
	DriftFromPrevious float32 // Cosine distance from previous embedding version
	DriftFromFirst    float32 // Cosine distance from first embedding version
}

// Search finds the k most similar nodes to a query vector at a specific time
func (s *Store) Search(query []float32, k int, asOf time.Time, validNodeIDs map[string]bool) []SearchResult {
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

// SearchAllVersions finds all historical versions of nodes similar to a query vector
// Returns all embeddings (across all time periods) that match, with their temporal validity
func (s *Store) SearchAllVersions(query []float32, k int, validNodeIDs map[string]bool, threshold float32, calculateDrift bool) []VersionedSearchResult {
	var results []VersionedSearchResult

	// Group results by nodeID for drift calculation
	nodeResults := make(map[string][]VersionedSearchResult)

	for nodeID, versions := range s.embeddings {
		// Skip nodes that aren't in the valid set (if provided)
		if validNodeIDs != nil && !validNodeIDs[nodeID] {
			continue
		}

		// Check all embedding versions for this node
		for _, emb := range versions {
			similarity := CosineSimilarity(query, emb.Vector)

			// Apply threshold filter
			if threshold > 0 && similarity < threshold {
				continue
			}

			result := VersionedSearchResult{
				NodeID:     nodeID,
				Similarity: similarity,
				ValidFrom:  emb.ValidFrom,
				ValidTo:    emb.ValidTo,
				Embedding:  emb,
			}

			nodeResults[nodeID] = append(nodeResults[nodeID], result)
		}
	}

	// Calculate drift if requested
	if calculateDrift {
		for nodeID, nodeVersions := range nodeResults {
			// Sort versions by ValidFrom (ascending) for chronological drift calculation
			for i := 0; i < len(nodeVersions)-1; i++ {
				for j := i + 1; j < len(nodeVersions); j++ {
					if nodeVersions[j].ValidFrom.Before(nodeVersions[i].ValidFrom) {
						nodeVersions[i], nodeVersions[j] = nodeVersions[j], nodeVersions[i]
					}
				}
			}

			// Calculate drift metrics
			for i := range nodeVersions {
				if i == 0 {
					// First version has no drift
					nodeVersions[i].DriftFromPrevious = 0
					nodeVersions[i].DriftFromFirst = 0
				} else {
					// Drift from previous version
					nodeVersions[i].DriftFromPrevious = CosineDistance(
						nodeVersions[i].Embedding.Vector,
						nodeVersions[i-1].Embedding.Vector,
					)
					// Drift from first version
					nodeVersions[i].DriftFromFirst = CosineDistance(
						nodeVersions[i].Embedding.Vector,
						nodeVersions[0].Embedding.Vector,
					)
				}
			}

			nodeResults[nodeID] = nodeVersions
		}
	}

	// Flatten results
	for _, nodeVersions := range nodeResults {
		results = append(results, nodeVersions...)
	}

	// Sort by similarity (descending), then by ValidFrom (descending) for ties
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Similarity > results[i].Similarity ||
				(results[j].Similarity == results[i].Similarity && results[j].ValidFrom.After(results[i].ValidFrom)) {
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
