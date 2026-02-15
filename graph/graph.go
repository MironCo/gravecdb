package graph

import (
	"sync"

	"github.com/MironCo/gravecdb/core"
	"github.com/MironCo/gravecdb/embedding"
)

// Type aliases for core types
type Node = core.Node
type Relationship = core.Relationship

var NewNode = core.NewNode
var NewRelationship = core.NewRelationship

// Type aliases for embedding types
type Embedding = embedding.Embedding
type SearchResult = embedding.SearchResult
type VersionedSearchResult = embedding.VersionedSearchResult

// memGraph is an internal in-memory graph used by DiskGraph for complex operations
// like path finding and query execution. Use DiskGraph for all public APIs.
type memGraph struct {
	nodes                map[string]*Node
	nodeVersions         map[string][]*Node
	relationships        map[string]*Relationship
	relationshipVersions map[string][]*Relationship
	nodesByLabel         map[string]map[string]*Node
	embeddings           *embedding.Store
	mu                   sync.RWMutex
}

// newMemGraph creates a new in-memory graph for internal use
func newMemGraph() *memGraph {
	return &memGraph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           embedding.NewStore(),
	}
}
