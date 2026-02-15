package graph

import (
	"fmt"
	"sync"

	"github.com/MironCo/gravecdb/core"
	"github.com/MironCo/gravecdb/embedding"
	"github.com/MironCo/gravecdb/storage"
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

// Graph represents the in-memory graph database with optional disk persistence
type Graph struct {
	nodes                map[string]*Node
	nodeVersions         map[string][]*Node            // All versions of each node for temporal queries
	relationships        map[string]*Relationship
	relationshipVersions map[string][]*Relationship    // All versions of each relationship for temporal queries
	nodesByLabel         map[string]map[string]*Node   // label -> nodeID -> Node
	embeddings           *embedding.Store              // Vector embeddings for semantic search
	mu                   sync.RWMutex
	boltStore            *storage.BoltStore            // bbolt storage backend (nil if no persistence)
}

// NewGraph creates a new graph database instance without persistence
func NewGraph() *Graph {
	return &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           embedding.NewStore(),
	}
}

// NewGraphWithBolt creates a new graph database with bbolt persistence
// This is the recommended persistence layer - provides ACID transactions and MVCC
// dataDir: directory where gravecdb.db file will be stored
func NewGraphWithBolt(dataDir string) (*Graph, error) {
	// Create the bbolt storage backend
	store, err := storage.NewBoltStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}

	g := &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           embedding.NewStore(),
		boltStore:            store,
	}

	// Load existing data from bbolt into memory
	if err := g.loadFromBolt(); err != nil {
		return nil, fmt.Errorf("failed to load from bolt: %w", err)
	}

	return g, nil
}

// loadFromBolt loads all data from bbolt into memory
func (g *Graph) loadFromBolt() error {
	if g.boltStore == nil {
		return nil
	}

	// Load all nodes
	nodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return fmt.Errorf("failed to load nodes: %w", err)
	}

	for _, node := range nodes {
		g.nodes[node.ID] = node

		// Rebuild label index
		for _, label := range node.Labels {
			if g.nodesByLabel[label] == nil {
				g.nodesByLabel[label] = make(map[string]*Node)
			}
			g.nodesByLabel[label][node.ID] = node
		}
	}

	// Load all relationships
	rels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return fmt.Errorf("failed to load relationships: %w", err)
	}

	for _, rel := range rels {
		g.relationships[rel.ID] = rel
	}

	// Load all embeddings
	embeddingsMap, err := g.boltStore.GetAllEmbeddings()
	if err != nil {
		return fmt.Errorf("failed to load embeddings: %w", err)
	}

	// Directly set the embeddings map (matches EmbeddingStore's internal structure)
	for nodeID, embeddings := range embeddingsMap {
		for _, emb := range embeddings {
			// Re-add each embedding to maintain versioning
			g.embeddings.Add(nodeID, emb.Vector, emb.Model, emb.PropertySnapshot)
		}
	}

	return nil
}

// Close closes the database
// Should be called when shutting down to ensure all data is saved
func (g *Graph) Close() error {
	if g.boltStore != nil {
		return g.boltStore.Close()
	}
	return nil
}
