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

	for _, node := range nodes {
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

// AsOf returns a temporal view of the graph at a specific point in time.
// Scans disk for all node/relationship versions valid at t.
func (g *DiskGraph) AsOf(t time.Time) *TemporalView {
	g.mu.RLock()
	defer g.mu.RUnlock()

	tv := &TemporalView{
		nodes:         make(map[string]*Node),
		relationships: make(map[string]*Relationship),
		nodesByLabel:  make(map[string][]string),
		nodeRelIndex:  make(map[string][]string),
		asOfTime:      t,
	}

	allNodes, _ := g.boltStore.GetAllNodes()
	for _, node := range allNodes {
		if node.IsValidAt(t) {
			tv.nodes[node.ID] = node
			for _, label := range node.Labels {
				tv.nodesByLabel[label] = append(tv.nodesByLabel[label], node.ID)
			}
		}
	}

	allRels, _ := g.boltStore.GetAllRelationships()
	for _, rel := range allRels {
		if rel.IsValidAt(t) {
			tv.relationships[rel.ID] = rel
			tv.nodeRelIndex[rel.FromNodeID] = append(tv.nodeRelIndex[rel.FromNodeID], rel.ID)
			tv.nodeRelIndex[rel.ToNodeID] = append(tv.nodeRelIndex[rel.ToNodeID], rel.ID)
		}
	}

	return tv
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

// diskPathStep is used during BFS/DFS path reconstruction
type diskPathStep struct {
	rel        *Relationship
	previousID string
}

// ShortestPath finds the shortest path between two nodes using BFS on the disk indexes.
func (g *DiskGraph) ShortestPath(fromID, toID string) *Path {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.shortestPathUnlocked(fromID, toID)
}

// shortestPathUnlocked performs BFS without acquiring the lock (caller must hold lock).
func (g *DiskGraph) shortestPathUnlocked(fromID, toID string) *Path {
	fromNode := g.getNodeUnlocked(fromID)
	toNode := g.getNodeUnlocked(toID)
	if fromNode == nil || !fromNode.IsCurrentlyValid() || toNode == nil || !toNode.IsCurrentlyValid() {
		return nil
	}

	queue := []string{fromID}
	visited := map[string]bool{fromID: true}
	parent := map[string]*diskPathStep{}

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		if currentID == toID {
			return g.reconstructPathUnlocked(fromID, toID, parent)
		}

		for _, rel := range g.getRelationshipsForNodeUnlocked(currentID) {
			if !rel.IsCurrentlyValid() {
				continue
			}
			var neighborID string
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if !visited[neighborID] {
				neighbor := g.getNodeUnlocked(neighborID)
				if neighbor == nil || !neighbor.IsCurrentlyValid() {
					continue
				}
				visited[neighborID] = true
				parent[neighborID] = &diskPathStep{rel: rel, previousID: currentID}
				queue = append(queue, neighborID)
			}
		}
	}
	return nil
}

func (g *DiskGraph) reconstructPathUnlocked(fromID, toID string, parent map[string]*diskPathStep) *Path {
	path := &Path{Nodes: []*Node{}, Relationships: []*Relationship{}}

	current := toID
	var steps []string
	var rels []*Relationship

	for current != fromID {
		steps = append(steps, current)
		step := parent[current]
		if step == nil {
			return nil
		}
		rels = append(rels, step.rel)
		current = step.previousID
	}
	steps = append(steps, fromID)

	for i := len(steps) - 1; i >= 0; i-- {
		if node := g.getNodeUnlocked(steps[i]); node != nil {
			path.Nodes = append(path.Nodes, node)
		}
	}
	for i := len(rels) - 1; i >= 0; i-- {
		path.Relationships = append(path.Relationships, rels[i])
	}
	path.Length = len(path.Relationships)
	return path
}

// AllPaths finds all simple paths between two nodes using DFS on the disk indexes.
func (g *DiskGraph) AllPaths(fromID, toID string, maxDepth int) []*Path {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.allPathsUnlocked(fromID, toID, maxDepth)
}

// allPathsUnlocked performs DFS without acquiring the lock (caller must hold lock).
func (g *DiskGraph) allPathsUnlocked(fromID, toID string, maxDepth int) []*Path {
	fromNode := g.getNodeUnlocked(fromID)
	toNode := g.getNodeUnlocked(toID)
	if fromNode == nil || !fromNode.IsCurrentlyValid() || toNode == nil || !toNode.IsCurrentlyValid() {
		return nil
	}

	var paths []*Path
	visited := make(map[string]bool)
	currentPath := &Path{Nodes: []*Node{}, Relationships: []*Relationship{}}
	g.dfsAllPathsUnlocked(fromID, toID, visited, currentPath, &paths, maxDepth, 0)
	return paths
}

func (g *DiskGraph) dfsAllPathsUnlocked(currentID, targetID string, visited map[string]bool, currentPath *Path, allPaths *[]*Path, maxDepth, depth int) {
	if maxDepth > 0 && depth >= maxDepth {
		return
	}
	visited[currentID] = true
	currentNode := g.getNodeUnlocked(currentID)
	if currentNode == nil || !currentNode.IsCurrentlyValid() {
		visited[currentID] = false
		return
	}
	currentPath.Nodes = append(currentPath.Nodes, currentNode)

	if currentID == targetID {
		pathCopy := &Path{
			Nodes:         make([]*Node, len(currentPath.Nodes)),
			Relationships: make([]*Relationship, len(currentPath.Relationships)),
			Length:        len(currentPath.Relationships),
		}
		copy(pathCopy.Nodes, currentPath.Nodes)
		copy(pathCopy.Relationships, currentPath.Relationships)
		*allPaths = append(*allPaths, pathCopy)
	} else {
		for _, rel := range g.getRelationshipsForNodeUnlocked(currentID) {
			if !rel.IsCurrentlyValid() {
				continue
			}
			var neighborID string
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if !visited[neighborID] {
				neighbor := g.getNodeUnlocked(neighborID)
				if neighbor == nil || !neighbor.IsCurrentlyValid() {
					continue
				}
				currentPath.Relationships = append(currentPath.Relationships, rel)
				g.dfsAllPathsUnlocked(neighborID, targetID, visited, currentPath, allPaths, maxDepth, depth+1)
				currentPath.Relationships = currentPath.Relationships[:len(currentPath.Relationships)-1]
			}
		}
	}

	currentPath.Nodes = currentPath.Nodes[:len(currentPath.Nodes)-1]
	visited[currentID] = false
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
