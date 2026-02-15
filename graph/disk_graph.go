package graph

import (
	"fmt"
	"sync"
	"time"

	"github.com/MironCo/gravecdb/storage"
	lru "github.com/hashicorp/golang-lru/v2"
)

// DiskGraph uses hybrid storage: indexes in RAM, data on disk with LRU cache
// Much lower RAM usage than full in-memory, but still fast for common queries
type DiskGraph struct {
	boltStore *storage.BoltStore

	// In-memory indexes (small footprint - just IDs)
	labelIndex   map[string][]string // label -> []nodeIDs
	nodeRelIndex map[string][]string // nodeID -> []relIDs (for fast relationship lookups)

	// LRU caches for hot data
	nodeCache *lru.Cache[string, *Node]         // Recently accessed nodes
	relCache  *lru.Cache[string, *Relationship] // Recently accessed relationships

	mu sync.RWMutex
}

// NewDiskGraph creates a disk-first graph with in-memory indexes and LRU cache
// cacheSize: number of nodes/relationships to keep in LRU cache (0 = use default 10000)
func NewDiskGraph(dataDir string, cacheSize int) (*DiskGraph, error) {
	store, err := storage.NewBoltStore(dataDir)
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
		boltStore:    store,
		labelIndex:   make(map[string][]string),
		nodeRelIndex: make(map[string][]string),
		nodeCache:    nodeCache,
		relCache:     relCache,
	}

	// Build indexes from disk
	if err := g.rebuildLabelIndex(); err != nil {
		return nil, fmt.Errorf("failed to build label index: %w", err)
	}
	if err := g.rebuildRelIndex(); err != nil {
		return nil, fmt.Errorf("failed to build relationship index: %w", err)
	}

	return g, nil
}

// rebuildLabelIndex scans all nodes and builds the in-memory label index
func (g *DiskGraph) rebuildLabelIndex() error {
	nodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return err
	}

	// Only index current (non-deleted) nodes, and dedupe by ID
	// (GetAllNodes returns all versions, but we only want one entry per ID)
	seen := make(map[string]bool)
	for _, node := range nodes {
		if node.ValidTo != nil {
			continue // Skip historical/deleted versions
		}
		if seen[node.ID] {
			continue // Already indexed this ID
		}
		seen[node.ID] = true
		for _, label := range node.Labels {
			g.labelIndex[label] = append(g.labelIndex[label], node.ID)
		}
	}

	return nil
}

// rebuildRelIndex scans all relationships and builds the in-memory relationship index
func (g *DiskGraph) rebuildRelIndex() error {
	rels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return err
	}

	for _, rel := range rels {
		if rel.ValidTo == nil { // Only index active relationships
			g.nodeRelIndex[rel.FromNodeID] = append(g.nodeRelIndex[rel.FromNodeID], rel.ID)
			g.nodeRelIndex[rel.ToNodeID] = append(g.nodeRelIndex[rel.ToNodeID], rel.ID)
		}
	}

	return nil
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

	boltStats := g.boltStore.Stats()

	return map[string]interface{}{
		"nodes":         len(nodes),
		"relationships": len(rels),
		"bolt_stats":    boltStats,
	}, nil
}

// AsOf returns a temporal view of the graph at a specific time
// For disk mode, we load the snapshot into memory (hybrid approach)
func (g *DiskGraph) AsOf(t time.Time) *TemporalView {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Create a new in-memory graph
	snapshot := newMemGraph()

	// Load all nodes from disk and filter by time
	allNodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return snapshot.asOf(t)
	}

	for _, node := range allNodes {
		if node.IsValidAt(t) {
			snapshot.nodes[node.ID] = node
			// Also add to nodeVersions so TemporalView.GetAllNodes() can find them
			if snapshot.nodeVersions[node.ID] == nil {
				snapshot.nodeVersions[node.ID] = []*Node{}
			}
			snapshot.nodeVersions[node.ID] = append(snapshot.nodeVersions[node.ID], node)

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
		return snapshot.asOf(t)
	}

	for _, rel := range allRels {
		if rel.IsValidAt(t) {
			snapshot.relationships[rel.ID] = rel
			// Also add to relationshipVersions so TemporalView.GetAllRelationships() can find them
			if snapshot.relationshipVersions[rel.ID] == nil {
				snapshot.relationshipVersions[rel.ID] = []*Relationship{}
			}
			snapshot.relationshipVersions[rel.ID] = append(snapshot.relationshipVersions[rel.ID], rel)
		}
	}

	// Load embeddings
	allEmbs, err := g.boltStore.GetAllEmbeddings()
	if err != nil {
		return snapshot.asOf(t)
	}

	for nodeID, embeddings := range allEmbs {
		for _, emb := range embeddings {
			snapshot.embeddings.Add(nodeID, emb.Vector, emb.Model, emb.PropertySnapshot)
		}
	}

	return snapshot.asOf(t)
}

// GetAllNodeVersions returns all versions of all nodes (for building timelines)
// This includes historical versions that have been modified or deleted
func (g *DiskGraph) GetAllNodeVersions() []*Node {
	// BoltStore.GetAllNodes() already returns all versions
	// (both current and historical with ValidFrom/ValidTo timestamps)
	nodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return []*Node{}
	}
	return nodes
}

// GetAllRelationshipVersions returns all versions of all relationships (for building timelines)
// This includes historical versions that have been modified or deleted
func (g *DiskGraph) GetAllRelationshipVersions() []*Relationship {
	// BoltStore.GetAllRelationships() already returns all versions
	// (both current and historical with ValidFrom/ValidTo timestamps)
	rels, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return []*Relationship{}
	}
	return rels
}

// loadIntoMemory creates an in-memory graph with current data (for complex operations)
func (g *DiskGraph) loadIntoMemory() *memGraph {
	memGraph := newMemGraph()

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

// loadIntoMemoryUnlocked creates an in-memory graph (caller must hold lock)
func (g *DiskGraph) loadIntoMemoryUnlocked() *memGraph {
	memGraph := newMemGraph()

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

// PathExists checks if any path exists between two nodes
func (g *DiskGraph) PathExists(fromID, toID string) bool {
	return g.ShortestPath(fromID, toID) != nil
}

// removeFromRelIndex removes a relationship ID from a node's relationship index
func (g *DiskGraph) removeFromRelIndex(nodeID, relID string) {
	ids := g.nodeRelIndex[nodeID]
	for i, id := range ids {
		if id == relID {
			g.nodeRelIndex[nodeID] = append(ids[:i], ids[i+1:]...)
			return
		}
	}
}

// getNodeUnlocked gets a node from cache or disk (caller must hold lock)
func (g *DiskGraph) getNodeUnlocked(id string) *Node {
	if node, ok := g.nodeCache.Get(id); ok {
		return node
	}
	node, err := g.boltStore.GetNode(id)
	if err == nil && node != nil {
		g.nodeCache.Add(id, node)
	}
	return node
}
