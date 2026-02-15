package graph

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
