package graph

import "time"

// GraphDB is the common interface for all graph database implementations
// Both Graph (in-memory) and DiskGraph (hybrid disk+cache) implement this
type GraphDB interface {
	// Node operations
	CreateNode(labels ...string) *Node
	GetNode(id string) (*Node, error)
	GetNodesByLabel(label string) []*Node
	SetNodeProperty(nodeID, key string, value interface{}) error
	DeleteNode(nodeID string) error

	// Relationship operations
	CreateRelationship(relType, fromID, toID string) (*Relationship, error)
	GetRelationshipsForNode(nodeID string) []*Relationship
	SetRelationshipProperty(relID, key string, value interface{}) error
	DeleteRelationship(relID string) error

	// Query operations
	ExecuteQueryWithEmbedder(query *Query, embedder Embedder) (*QueryResult, error)

	// Path finding
	ShortestPath(fromID, toID string) *Path
	AllPaths(fromID, toID string, maxDepth int) []*Path

	// Embeddings
	GetNodeEmbedding(nodeID string) *Embedding

	// Temporal operations
	AsOf(t time.Time) *TemporalView

	// Database management
	Close() error
}
