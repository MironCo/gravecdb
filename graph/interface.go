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

// TransactionalGraphDB extends GraphDB with transaction support
// DiskGraph implements this; in-memory Graph does not (transactions are no-ops)
type TransactionalGraphDB interface {
	GraphDB

	// BeginTransaction starts a new transaction
	// Returns a GraphTransaction that can be used for all operations within the transaction
	BeginTransaction() (GraphTransaction, error)
}

// GraphTransaction represents an active database transaction
// All operations within a transaction are atomic - either all succeed or all are rolled back
type GraphTransaction interface {
	// Node operations within transaction
	CreateNode(labels ...string) (*Node, error)
	GetNode(id string) (*Node, error)
	SetNodeProperty(nodeID, key string, value interface{}) error
	DeleteNode(nodeID string) error

	// Relationship operations within transaction
	CreateRelationship(relType, fromID, toID string) (*Relationship, error)
	GetRelationship(id string) (*Relationship, error)
	SetRelationshipProperty(relID, key string, value interface{}) error
	DeleteRelationship(relID string) error

	// Execute a full query within this transaction
	ExecuteQuery(query *Query, embedder Embedder) (*QueryResult, error)

	// Transaction control
	Commit() error
	Rollback() error
}
