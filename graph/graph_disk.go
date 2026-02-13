package graph

import (
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/google/uuid"
)

// DiskGraph uses hybrid storage: indexes in RAM, data on disk with LRU cache
// Much lower RAM usage than full in-memory, but still fast for common queries
type DiskGraph struct {
	boltStore *BoltStore

	// In-memory indexes (small footprint - just IDs)
	labelIndex map[string][]string // label -> []nodeIDs (50 bytes per node)

	// LRU caches for hot data
	nodeCache *lru.Cache[string, *Node]         // Recently accessed nodes
	relCache  *lru.Cache[string, *Relationship] // Recently accessed relationships

	mu sync.RWMutex
}

// NewDiskGraph creates a disk-first graph with in-memory indexes and LRU cache
// cacheSize: number of nodes/relationships to keep in LRU cache (0 = use default 10000)
func NewDiskGraph(dataDir string, cacheSize int) (*DiskGraph, error) {
	store, err := NewBoltStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}

	if cacheSize <= 0 {
		cacheSize = 10000 // Default: cache 10k nodes
	}

	nodeCache, err := lru.New[string, *Node](cacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create node cache: %w", err)
	}

	relCache, err := lru.New[string, *Relationship](cacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create relationship cache: %w", err)
	}

	g := &DiskGraph{
		boltStore:  store,
		labelIndex: make(map[string][]string),
		nodeCache:  nodeCache,
		relCache:   relCache,
	}

	// Build label index from disk
	if err := g.rebuildLabelIndex(); err != nil {
		return nil, fmt.Errorf("failed to build label index: %w", err)
	}

	return g, nil
}

// rebuildLabelIndex scans all nodes and builds the in-memory label index
func (g *DiskGraph) rebuildLabelIndex() error {
	nodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return err
	}

	for _, node := range nodes {
		for _, label := range node.Labels {
			g.labelIndex[label] = append(g.labelIndex[label], node.ID)
		}
	}

	return nil
}

// GetNode retrieves a node (from cache if present, otherwise from disk)
func (g *DiskGraph) GetNode(id string) (*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check cache first
	if node, ok := g.nodeCache.Get(id); ok {
		return node, nil
	}

	// Cache miss - load from disk
	node, err := g.boltStore.GetNode(id)
	if err != nil {
		return nil, err
	}

	if node != nil {
		g.nodeCache.Add(id, node)
	}

	return node, nil
}

// GetNodesByLabel retrieves nodes by label using in-memory index
func (g *DiskGraph) GetNodesByLabel(label string) []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Use in-memory index to get node IDs
	nodeIDs := g.labelIndex[label]
	if nodeIDs == nil {
		return []*Node{}
	}

	// Fetch nodes (from cache or disk)
	nodes := make([]*Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		// Check cache
		if node, ok := g.nodeCache.Get(id); ok {
			nodes = append(nodes, node)
			continue
		}

		// Load from disk
		node, _ := g.boltStore.GetNode(id)
		if node != nil {
			g.nodeCache.Add(id, node)
			nodes = append(nodes, node)
		}
	}

	return nodes
}

// GetAllNodes retrieves all nodes from disk
func (g *DiskGraph) GetAllNodes() ([]*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.boltStore.GetAllNodes()
}

// GetRelationship retrieves a relationship (from cache if present, otherwise from disk)
func (g *DiskGraph) GetRelationship(id string) (*Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check cache first
	if rel, ok := g.relCache.Get(id); ok {
		return rel, nil
	}

	// Cache miss - load from disk
	rel, err := g.boltStore.GetRelationship(id)
	if err != nil {
		return nil, err
	}

	if rel != nil {
		g.relCache.Add(id, rel)
	}

	return rel, nil
}

// GetAllRelationships retrieves all relationships from disk
func (g *DiskGraph) GetAllRelationships() ([]*Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.boltStore.GetAllRelationships()
}

// CreateNode creates a new node and persists to disk
func (g *DiskGraph) CreateNode(labels ...string) *Node {
	g.mu.Lock()
	defer g.mu.Unlock()

	node := &Node{
		ID:         uuid.New().String(),
		Labels:     labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
		ValidTo:    nil,
	}

	if err := g.boltStore.SaveNode(node); err != nil {
		panic(fmt.Sprintf("failed to save node: %v", err))
	}

	// Update label index
	for _, label := range labels {
		g.labelIndex[label] = append(g.labelIndex[label], node.ID)
	}

	// Add to cache
	g.nodeCache.Add(node.ID, node)

	return node
}

// SetNodeProperty sets a property on a node
func (g *DiskGraph) SetNodeProperty(nodeID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Read from disk
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// Update property
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[key] = value

	// Write back to disk
	if err := g.boltStore.SaveNode(node); err != nil {
		return err
	}

	// Update cache
	g.nodeCache.Add(nodeID, node)

	return nil
}

// CreateRelationship creates a new relationship
func (g *DiskGraph) CreateRelationship(relType, fromID, toID string) (*Relationship, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Verify nodes exist
	from, err := g.boltStore.GetNode(fromID)
	if err != nil || from == nil {
		return nil, fmt.Errorf("from node not found: %s", fromID)
	}

	to, err := g.boltStore.GetNode(toID)
	if err != nil || to == nil {
		return nil, fmt.Errorf("to node not found: %s", toID)
	}

	rel := &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromID,
		ToNodeID:   toID,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
		ValidTo:    nil,
	}

	if err := g.boltStore.SaveRelationship(rel); err != nil {
		return nil, fmt.Errorf("failed to save relationship: %w", err)
	}

	// Add to cache
	g.relCache.Add(rel.ID, rel)

	return rel, nil
}

// SetRelationshipProperty sets a property on a relationship
func (g *DiskGraph) SetRelationshipProperty(relID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil {
		return err
	}
	if rel == nil {
		return fmt.Errorf("relationship not found: %s", relID)
	}

	if rel.Properties == nil {
		rel.Properties = make(map[string]interface{})
	}
	rel.Properties[key] = value

	return g.boltStore.SaveRelationship(rel)
}

// DeleteNode soft-deletes a node
func (g *DiskGraph) DeleteNode(nodeID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()

	// Soft delete all relationships involving this node
	rels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return fmt.Errorf("failed to get relationships: %w", err)
	}

	for _, rel := range rels {
		if rel.ValidTo == nil && (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) {
			if err := g.boltStore.DeleteRelationship(rel.ID, now); err != nil {
				return fmt.Errorf("failed to delete relationship: %w", err)
			}
		}
	}

	// Soft delete the node
	return g.boltStore.DeleteNode(nodeID, now)
}

// DeleteRelationship soft-deletes a relationship
func (g *DiskGraph) DeleteRelationship(relID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.boltStore.DeleteRelationship(relID, time.Now())
}

// AsOf returns a temporal view of the graph at a specific time
// For disk mode, we load the snapshot into memory (hybrid approach)
func (g *DiskGraph) AsOf(t time.Time) *TemporalView {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Create a new in-memory graph
	snapshot := NewGraph()

	// Load all nodes from disk and filter by time
	allNodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return snapshot.AsOf(t)
	}

	for _, node := range allNodes {
		if node.IsValidAt(t) {
			snapshot.nodes[node.ID] = node
			for _, label := range node.Labels {
				if snapshot.nodesByLabel[label] == nil {
					snapshot.nodesByLabel[label] = make(map[string]*Node)
				}
				snapshot.nodesByLabel[label][node.ID] = node
			}
		}
	}

	// Load all relationships from disk and filter by time
	allRels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return snapshot.AsOf(t)
	}

	for _, rel := range allRels {
		if rel.IsValidAt(t) {
			snapshot.relationships[rel.ID] = rel
		}
	}

	// Load embeddings
	allEmbs, err := g.boltStore.GetAllEmbeddings()
	if err != nil {
		return snapshot.AsOf(t)
	}

	for nodeID, embeddings := range allEmbs {
		for _, emb := range embeddings {
			snapshot.embeddings.Add(nodeID, emb.Vector, emb.Model)
		}
	}

	return snapshot.AsOf(t)
}

// GetRelationshipsForNode returns all relationships involving a node
func (g *DiskGraph) GetRelationshipsForNode(nodeID string) []*Relationship {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var result []*Relationship

	allRels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return result
	}

	for _, rel := range allRels {
		if rel.ValidTo == nil && (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) {
			result = append(result, rel)
		}
	}

	return result
}

// GetNodeEmbedding retrieves the current embedding for a node
func (g *DiskGraph) GetNodeEmbedding(nodeID string) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()

	embeddings, err := g.boltStore.GetEmbedding(nodeID)
	if err != nil || len(embeddings) == 0 {
		return nil
	}

	// Return most recent valid embedding
	for i := len(embeddings) - 1; i >= 0; i-- {
		if embeddings[i].ValidTo == nil {
			return embeddings[i]
		}
	}

	return nil
}

// ExecuteQueryWithEmbedder executes a Cypher-like query (delegates to in-memory graph)
func (g *DiskGraph) ExecuteQueryWithEmbedder(query *Query, embedder Embedder) (*QueryResult, error) {
	// Load into memory graph for query execution
	// This is a simplified approach - in production you'd want to optimize this
	g.mu.RLock()
	defer g.mu.RUnlock()

	memGraph := NewGraph()

	// Load all data into memory
	nodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n.ValidTo == nil {
			memGraph.nodes[n.ID] = n
			for _, label := range n.Labels {
				if memGraph.nodesByLabel[label] == nil {
					memGraph.nodesByLabel[label] = make(map[string]*Node)
				}
				memGraph.nodesByLabel[label][n.ID] = n
			}
		}
	}

	rels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return nil, err
	}
	for _, r := range rels {
		if r.ValidTo == nil {
			memGraph.relationships[r.ID] = r
		}
	}

	embeddings, err := g.boltStore.GetAllEmbeddings()
	if err != nil {
		return nil, err
	}
	for nodeID, embs := range embeddings {
		for _, emb := range embs {
			memGraph.embeddings.Add(nodeID, emb.Vector, emb.Model)
		}
	}

	return memGraph.ExecuteQueryWithEmbedder(query, embedder)
}

// loadIntoMemory creates an in-memory graph with current data (for complex operations)
func (g *DiskGraph) loadIntoMemory() *Graph {
	memGraph := NewGraph()

	nodes, _ := g.boltStore.GetAllNodes()
	for _, n := range nodes {
		if n.ValidTo == nil {
			memGraph.nodes[n.ID] = n
			for _, label := range n.Labels {
				if memGraph.nodesByLabel[label] == nil {
					memGraph.nodesByLabel[label] = make(map[string]*Node)
				}
				memGraph.nodesByLabel[label][n.ID] = n
			}
		}
	}

	rels, _ := g.boltStore.GetAllRelationships()
	for _, r := range rels {
		if r.ValidTo == nil {
			memGraph.relationships[r.ID] = r
		}
	}

	return memGraph
}

// ShortestPath finds the shortest path between two nodes
func (g *DiskGraph) ShortestPath(fromID, toID string) *Path {
	// Delegate to in-memory graph for path finding (complex traversal operation)
	memGraph := g.loadIntoMemory()
	return memGraph.ShortestPath(fromID, toID)
}

// AllPaths finds all paths between two nodes up to maxDepth
func (g *DiskGraph) AllPaths(fromID, toID string, maxDepth int) []*Path {
	// Delegate to in-memory graph for path finding (complex traversal operation)
	memGraph := g.loadIntoMemory()
	return memGraph.AllPaths(fromID, toID, maxDepth)
}

// Close closes the database
func (g *DiskGraph) Close() error {
	return g.boltStore.Close()
}

// Stats returns database statistics
func (g *DiskGraph) Stats() (map[string]interface{}, error) {
	nodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return nil, err
	}

	rels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return nil, err
	}

	boltStats, err := g.boltStore.Stats()
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"nodes":         len(nodes),
		"relationships": len(rels),
		"bolt_stats":    boltStats,
	}, nil
}
