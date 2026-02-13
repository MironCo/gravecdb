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
	labelIndex   map[string][]string   // label -> []nodeIDs
	nodeRelIndex map[string][]string   // nodeID -> []relIDs (for fast relationship lookups)

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

	// Update relationship index
	g.nodeRelIndex[fromID] = append(g.nodeRelIndex[fromID], rel.ID)
	g.nodeRelIndex[toID] = append(g.nodeRelIndex[toID], rel.ID)

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

	// Soft delete all relationships involving this node using index
	relIDs := g.nodeRelIndex[nodeID]
	for _, relID := range relIDs {
		rel, err := g.boltStore.GetRelationship(relID)
		if err != nil || rel == nil || rel.ValidTo != nil {
			continue
		}
		if err := g.boltStore.DeleteRelationship(relID, now); err != nil {
			return fmt.Errorf("failed to delete relationship: %w", err)
		}
		// Remove from the other node's index
		otherNodeID := rel.FromNodeID
		if otherNodeID == nodeID {
			otherNodeID = rel.ToNodeID
		}
		g.removeFromRelIndex(otherNodeID, relID)
		g.relCache.Remove(relID)
	}
	// Clear this node's relationship index
	delete(g.nodeRelIndex, nodeID)

	// Soft delete the node
	return g.boltStore.DeleteNode(nodeID, now)
}

// DeleteRelationship soft-deletes a relationship
func (g *DiskGraph) DeleteRelationship(relID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Get relationship to update index
	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil {
		return err
	}
	if rel != nil && rel.ValidTo == nil {
		// Remove from relationship index
		g.removeFromRelIndex(rel.FromNodeID, relID)
		g.removeFromRelIndex(rel.ToNodeID, relID)
		g.relCache.Remove(relID)
	}

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

	relIDs := g.nodeRelIndex[nodeID]
	if len(relIDs) == 0 {
		return nil
	}

	result := make([]*Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		// Check cache first
		if rel, ok := g.relCache.Get(relID); ok {
			result = append(result, rel)
			continue
		}
		// Load from disk
		rel, err := g.boltStore.GetRelationship(relID)
		if err == nil && rel != nil && rel.ValidTo == nil {
			g.relCache.Add(relID, rel)
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

// ExecuteQueryWithEmbedder executes a Cypher-like query
func (g *DiskGraph) ExecuteQueryWithEmbedder(query *Query, embedder Embedder) (*QueryResult, error) {
	// For mutating queries (CREATE, SET, DELETE), use DiskGraph's own methods
	// For read queries (MATCH without mutations), use in-memory graph

	switch query.QueryType {
	case "CREATE":
		return g.executeCreateQuery(query)
	case "MATCH":
		// Check if this is a mutating MATCH query
		if query.CreateClause != nil {
			return g.executeMatchCreateQuery(query)
		}
		if query.SetClause != nil {
			return g.executeSetQuery(query)
		}
		if query.DeleteClause != nil {
			return g.executeDeleteQuery(query)
		}
		// Read-only MATCH query - use in-memory approach
		return g.executeReadQuery(query, embedder)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", query.QueryType)
	}
}

// executeCreateQuery handles CREATE queries directly on DiskGraph
func (g *DiskGraph) executeCreateQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	cc := query.CreateClause
	createdVars := make(map[string]interface{})
	createdCount := 0

	// Create nodes
	for _, nodeSpec := range cc.Nodes {
		node := g.createNodeUnlocked(nodeSpec.Labels...)

		// Set properties
		for key, value := range nodeSpec.Properties {
			if err := g.setNodePropertyUnlocked(node.ID, key, value); err != nil {
				return nil, err
			}
		}

		if nodeSpec.Variable != "" {
			createdVars[nodeSpec.Variable] = node
		}
		createdCount++
	}

	// Create relationships
	for _, relSpec := range cc.Relationships {
		fromNode, ok := createdVars[relSpec.FromVar].(*Node)
		if !ok {
			continue
		}
		toNode, ok := createdVars[relSpec.ToVar].(*Node)
		if !ok {
			continue
		}

		rel, err := g.createRelationshipUnlocked(relSpec.Type, fromNode.ID, toNode.ID)
		if err != nil {
			return nil, err
		}

		for key, value := range relSpec.Properties {
			if err := g.setRelPropertyUnlocked(rel.ID, key, value); err != nil {
				return nil, err
			}
		}

		if relSpec.Variable != "" {
			createdVars[relSpec.Variable] = rel
		}
		createdCount++
	}

	// Build result
	result := &QueryResult{
		Columns: []string{"created"},
		Rows: []map[string]interface{}{
			{"created": createdCount},
		},
	}

	return result, nil
}

// executeMatchCreateQuery handles MATCH...CREATE queries
func (g *DiskGraph) executeMatchCreateQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Use index-based matching instead of loading entire database
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	cc := query.CreateClause
	createdCount := 0

	for _, match := range matches {
		createdVars := make(map[string]interface{})
		for k, v := range match {
			createdVars[k] = v
		}

		// Create nodes (skip if already matched from MATCH clause)
		for _, nodeSpec := range cc.Nodes {
			// If this variable already exists from MATCH, don't create a new node
			if nodeSpec.Variable != "" {
				if _, exists := createdVars[nodeSpec.Variable]; exists {
					continue
				}
			}
			node := g.createNodeUnlocked(nodeSpec.Labels...)
			for key, value := range nodeSpec.Properties {
				g.setNodePropertyUnlocked(node.ID, key, value)
			}
			if nodeSpec.Variable != "" {
				createdVars[nodeSpec.Variable] = node
			}
			createdCount++
		}

		// Create relationships
		for _, relSpec := range cc.Relationships {
			var fromNode, toNode *Node
			if e, ok := createdVars[relSpec.FromVar]; ok {
				fromNode, _ = e.(*Node)
			}
			if e, ok := createdVars[relSpec.ToVar]; ok {
				toNode, _ = e.(*Node)
			}
			if fromNode == nil || toNode == nil {
				continue
			}

			rel, err := g.createRelationshipUnlocked(relSpec.Type, fromNode.ID, toNode.ID)
			if err != nil {
				continue
			}
			for key, value := range relSpec.Properties {
				g.setRelPropertyUnlocked(rel.ID, key, value)
			}
			createdCount++
		}
	}

	return &QueryResult{
		Columns: []string{"created"},
		Rows:    []map[string]interface{}{{"created": createdCount}},
	}, nil
}

// executeSetQuery handles MATCH...SET queries
func (g *DiskGraph) executeSetQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Use index-based matching
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	updatedCount := 0
	for _, match := range matches {
		for _, update := range query.SetClause.Updates {
			entity, ok := match[update.Variable]
			if !ok {
				continue
			}
			if node, ok := entity.(*Node); ok {
				g.setNodePropertyUnlocked(node.ID, update.Property, update.Value)
				updatedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				g.setRelPropertyUnlocked(rel.ID, update.Property, update.Value)
				updatedCount++
			}
		}
	}

	return &QueryResult{
		Columns: []string{"updated"},
		Rows:    []map[string]interface{}{{"updated": updatedCount}},
	}, nil
}

// executeDeleteQuery handles MATCH...DELETE queries
func (g *DiskGraph) executeDeleteQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Use index-based matching
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	deletedCount := 0
	for _, match := range matches {
		for _, varName := range query.DeleteClause.Variables {
			entity, ok := match[varName]
			if !ok {
				continue
			}
			if node, ok := entity.(*Node); ok {
				if query.DeleteClause.Detach {
					// Delete relationships first
					rels := g.getRelationshipsForNodeUnlocked(node.ID)
					for _, rel := range rels {
						g.deleteRelationshipUnlocked(rel.ID)
					}
				}
				g.deleteNodeUnlocked(node.ID)
				deletedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				g.deleteRelationshipUnlocked(rel.ID)
				deletedCount++
			}
		}
	}

	return &QueryResult{
		Columns: []string{"deleted"},
		Rows:    []map[string]interface{}{{"deleted": deletedCount}},
	}, nil
}

// executeReadQuery handles read-only MATCH queries using in-memory graph
func (g *DiskGraph) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	memGraph := g.loadIntoMemoryUnlocked()

	// Load embeddings
	embeddings, _ := g.boltStore.GetAllEmbeddings()
	for nodeID, embs := range embeddings {
		for _, emb := range embs {
			memGraph.embeddings.Add(nodeID, emb.Vector, emb.Model)
		}
	}

	return memGraph.ExecuteQueryWithEmbedder(query, embedder)
}

// loadIntoMemoryUnlocked creates an in-memory graph (caller must hold lock)
func (g *DiskGraph) loadIntoMemoryUnlocked() *Graph {
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

// createNodeUnlocked creates a node (caller must hold write lock)
func (g *DiskGraph) createNodeUnlocked(labels ...string) *Node {
	node := &Node{
		ID:         uuid.New().String(),
		Labels:     labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
	}

	g.boltStore.SaveNode(node)

	// Update in-memory label index
	for _, label := range labels {
		g.labelIndex[label] = append(g.labelIndex[label], node.ID)
	}

	// Add to cache
	g.nodeCache.Add(node.ID, node)

	return node
}

// setNodePropertyUnlocked sets a node property (caller must hold write lock)
func (g *DiskGraph) setNodePropertyUnlocked(nodeID, key string, value interface{}) error {
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil || node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	node.Properties[key] = value
	g.boltStore.SaveNode(node)
	g.nodeCache.Add(nodeID, node)

	return nil
}

// createRelationshipUnlocked creates a relationship (caller must hold write lock)
func (g *DiskGraph) createRelationshipUnlocked(relType, fromID, toID string) (*Relationship, error) {
	rel := &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromID,
		ToNodeID:   toID,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
	}

	if err := g.boltStore.SaveRelationship(rel); err != nil {
		return nil, err
	}

	// Update relationship index
	g.nodeRelIndex[fromID] = append(g.nodeRelIndex[fromID], rel.ID)
	g.nodeRelIndex[toID] = append(g.nodeRelIndex[toID], rel.ID)

	g.relCache.Add(rel.ID, rel)

	return rel, nil
}

// setRelPropertyUnlocked sets a relationship property (caller must hold write lock)
func (g *DiskGraph) setRelPropertyUnlocked(relID, key string, value interface{}) error {
	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil || rel == nil {
		return fmt.Errorf("relationship not found: %s", relID)
	}

	rel.Properties[key] = value
	g.boltStore.SaveRelationship(rel)
	g.relCache.Add(relID, rel)

	return nil
}

// getRelationshipsForNodeUnlocked gets relationships for a node (caller must hold lock)
func (g *DiskGraph) getRelationshipsForNodeUnlocked(nodeID string) []*Relationship {
	relIDs := g.nodeRelIndex[nodeID]
	if len(relIDs) == 0 {
		return nil
	}

	result := make([]*Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		// Check cache first
		if rel, ok := g.relCache.Get(relID); ok {
			result = append(result, rel)
			continue
		}
		// Load from disk
		rel, err := g.boltStore.GetRelationship(relID)
		if err == nil && rel != nil && rel.ValidTo == nil {
			g.relCache.Add(relID, rel)
			result = append(result, rel)
		}
	}
	return result
}

// deleteNodeUnlocked deletes a node (caller must hold write lock)
func (g *DiskGraph) deleteNodeUnlocked(nodeID string) error {
	node, _ := g.boltStore.GetNode(nodeID)
	if node != nil {
		now := time.Now()
		node.ValidTo = &now
		g.boltStore.SaveNode(node)

		// Remove from label index
		for _, label := range node.Labels {
			ids := g.labelIndex[label]
			for i, id := range ids {
				if id == nodeID {
					g.labelIndex[label] = append(ids[:i], ids[i+1:]...)
					break
				}
			}
		}

		g.nodeCache.Remove(nodeID)
	}
	return nil
}

// deleteRelationshipUnlocked deletes a relationship (caller must hold write lock)
func (g *DiskGraph) deleteRelationshipUnlocked(relID string) error {
	rel, _ := g.boltStore.GetRelationship(relID)
	if rel != nil {
		now := time.Now()
		rel.ValidTo = &now
		g.boltStore.SaveRelationship(rel)

		// Remove from relationship index
		g.removeFromRelIndex(rel.FromNodeID, relID)
		g.removeFromRelIndex(rel.ToNodeID, relID)

		g.relCache.Remove(relID)
	}
	return nil
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

// findMatchesUnlocked finds pattern matches using indexes (caller must hold lock)
func (g *DiskGraph) findMatchesUnlocked(pattern *MatchPattern, where *WhereClause) []map[string]interface{} {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	// Start with the first node pattern - use label index
	firstPattern := pattern.Nodes[0]
	var candidateNodes []*Node

	if len(firstPattern.Labels) > 0 {
		// Use label index for fast lookup
		nodeIDs := g.labelIndex[firstPattern.Labels[0]]
		candidateNodes = make([]*Node, 0, len(nodeIDs))
		for _, id := range nodeIDs {
			node := g.getNodeUnlocked(id)
			if node != nil && node.ValidTo == nil {
				candidateNodes = append(candidateNodes, node)
			}
		}
	} else {
		// No label specified - need all nodes (slower path)
		allNodes, _ := g.boltStore.GetAllNodes()
		for _, n := range allNodes {
			if n.ValidTo == nil {
				candidateNodes = append(candidateNodes, n)
			}
		}
	}

	// Filter by properties if specified
	if len(firstPattern.Properties) > 0 {
		filtered := make([]*Node, 0)
		for _, node := range candidateNodes {
			if matchesProperties(node.Properties, firstPattern.Properties) {
				filtered = append(filtered, node)
			}
		}
		candidateNodes = filtered
	}

	// Build initial matches
	var matches []map[string]interface{}
	for _, node := range candidateNodes {
		match := map[string]interface{}{}
		if firstPattern.Variable != "" {
			match[firstPattern.Variable] = node
		}
		matches = append(matches, match)
	}

	// For simple single-node patterns, apply WHERE and return
	if len(pattern.Nodes) == 1 && len(pattern.Relationships) == 0 {
		if where != nil {
			matches = g.filterMatchesUnlocked(matches, where)
		}
		return matches
	}

	// Handle multi-node patterns without relationships (e.g., MATCH (a:Person), (b:Company))
	// This computes a cartesian product of all matching nodes
	if len(pattern.Relationships) == 0 && len(pattern.Nodes) > 1 {
		// Process remaining node patterns (first one already processed)
		for i := 1; i < len(pattern.Nodes); i++ {
			nodePattern := pattern.Nodes[i]

			// Find candidate nodes for this pattern
			var nodeCandidates []*Node
			if len(nodePattern.Labels) > 0 {
				label := nodePattern.Labels[0]
				nodeIDs := g.labelIndex[label]
				for _, id := range nodeIDs {
					node := g.getNodeUnlocked(id)
					if node != nil && node.ValidTo == nil {
						nodeCandidates = append(nodeCandidates, node)
					}
				}
			} else {
				allNodes, _ := g.boltStore.GetAllNodes()
				for _, n := range allNodes {
					if n.ValidTo == nil {
						nodeCandidates = append(nodeCandidates, n)
					}
				}
			}

			// Filter by properties
			if len(nodePattern.Properties) > 0 {
				filtered := make([]*Node, 0)
				for _, node := range nodeCandidates {
					if matchesProperties(node.Properties, nodePattern.Properties) {
						filtered = append(filtered, node)
					}
				}
				nodeCandidates = filtered
			}

			// Compute cartesian product with existing matches
			var newMatches []map[string]interface{}
			for _, match := range matches {
				for _, node := range nodeCandidates {
					newMatch := make(map[string]interface{})
					for k, v := range match {
						newMatch[k] = v
					}
					if nodePattern.Variable != "" {
						newMatch[nodePattern.Variable] = node
					}
					newMatches = append(newMatches, newMatch)
				}
			}
			matches = newMatches
		}

		if where != nil {
			matches = g.filterMatchesUnlocked(matches, where)
		}
		return matches
	}

	// Handle relationship patterns
	for _, relPattern := range pattern.Relationships {
		var newMatches []map[string]interface{}

		for _, match := range matches {
			// Get the "from" node
			fromIdx := relPattern.FromIndex
			if fromIdx >= len(pattern.Nodes) {
				continue
			}
			fromVar := pattern.Nodes[fromIdx].Variable
			fromEntity, ok := match[fromVar]
			if !ok {
				continue
			}
			fromNode, ok := fromEntity.(*Node)
			if !ok {
				continue
			}

			// Get relationships for this node using index
			rels := g.getRelationshipsForNodeUnlocked(fromNode.ID)

			for _, rel := range rels {
				// Check relationship type
				if len(relPattern.Types) > 0 {
					typeMatch := false
					for _, t := range relPattern.Types {
						if rel.Type == t {
							typeMatch = true
							break
						}
					}
					if !typeMatch {
						continue
					}
				}

				// Determine target node based on direction
				var targetNodeID string
				if relPattern.Direction == "->" {
					if rel.FromNodeID != fromNode.ID {
						continue
					}
					targetNodeID = rel.ToNodeID
				} else if relPattern.Direction == "<-" {
					if rel.ToNodeID != fromNode.ID {
						continue
					}
					targetNodeID = rel.FromNodeID
				} else {
					// Undirected - either end
					if rel.FromNodeID == fromNode.ID {
						targetNodeID = rel.ToNodeID
					} else {
						targetNodeID = rel.FromNodeID
					}
				}

				// Get target node
				targetNode := g.getNodeUnlocked(targetNodeID)
				if targetNode == nil || targetNode.ValidTo != nil {
					continue
				}

				// Check target node labels
				toIdx := relPattern.ToIndex
				if toIdx < len(pattern.Nodes) {
					toPattern := pattern.Nodes[toIdx]
					if len(toPattern.Labels) > 0 {
						hasLabel := false
						for _, reqLabel := range toPattern.Labels {
							for _, nodeLabel := range targetNode.Labels {
								if reqLabel == nodeLabel {
									hasLabel = true
									break
								}
							}
							if hasLabel {
								break
							}
						}
						if !hasLabel {
							continue
						}
					}

					// Check target node properties
					if len(toPattern.Properties) > 0 {
						if !matchesProperties(targetNode.Properties, toPattern.Properties) {
							continue
						}
					}

					// Build new match
					newMatch := make(map[string]interface{})
					for k, v := range match {
						newMatch[k] = v
					}
					if toPattern.Variable != "" {
						newMatch[toPattern.Variable] = targetNode
					}
					if relPattern.Variable != "" {
						newMatch[relPattern.Variable] = rel
					}
					newMatches = append(newMatches, newMatch)
				}
			}
		}
		matches = newMatches
	}

	// Apply WHERE clause
	if where != nil {
		matches = g.filterMatchesUnlocked(matches, where)
	}

	return matches
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

// filterMatchesUnlocked applies WHERE conditions (caller must hold lock)
func (g *DiskGraph) filterMatchesUnlocked(matches []map[string]interface{}, where *WhereClause) []map[string]interface{} {
	if where == nil || len(where.Conditions) == 0 {
		return matches
	}

	var result []map[string]interface{}
	for _, match := range matches {
		allMatch := true
		for _, cond := range where.Conditions {
			entity, ok := match[cond.Variable]
			if !ok {
				allMatch = false
				break
			}

			var props map[string]interface{}
			if node, ok := entity.(*Node); ok {
				props = node.Properties
			} else if rel, ok := entity.(*Relationship); ok {
				props = rel.Properties
			} else {
				allMatch = false
				break
			}

			propVal, exists := props[cond.Property]
			if !exists {
				allMatch = false
				break
			}

			if !evaluateCondition(propVal, cond.Operator, cond.Value) {
				allMatch = false
				break
			}
		}
		if allMatch {
			result = append(result, match)
		}
	}
	return result
}

// matchesProperties checks if node properties match required properties
func matchesProperties(nodeProps, required map[string]interface{}) bool {
	for key, reqVal := range required {
		if nodeVal, ok := nodeProps[key]; !ok || nodeVal != reqVal {
			return false
		}
	}
	return true
}

// evaluateCondition evaluates a single WHERE condition
func evaluateCondition(propVal interface{}, operator string, condVal interface{}) bool {
	switch operator {
	case "=", "==":
		return fmt.Sprintf("%v", propVal) == fmt.Sprintf("%v", condVal)
	case "!=", "<>":
		return fmt.Sprintf("%v", propVal) != fmt.Sprintf("%v", condVal)
	case ">":
		return compareValues(propVal, condVal) > 0
	case ">=":
		return compareValues(propVal, condVal) >= 0
	case "<":
		return compareValues(propVal, condVal) < 0
	case "<=":
		return compareValues(propVal, condVal) <= 0
	default:
		return false
	}
}

// compareValues compares two values numerically if possible
func compareValues(a, b interface{}) int {
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}
	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// toFloat64 converts a value to float64 if possible
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case float32:
		return float64(val), true
	default:
		return 0, false
	}
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

// ========== Transaction Support ==========

// DiskGraphTransaction wraps a BoltStore transaction with graph operations
// All operations within the transaction are atomic
type DiskGraphTransaction struct {
	g         *DiskGraph
	tx        *Tx
	committed bool

	// Track changes for index updates on commit
	createdNodes []*Node
	createdRels  []*Relationship
	deletedNodes []string // node IDs
	deletedRels  []string // relationship IDs
}

// BeginTransaction starts a new ACID transaction
func (g *DiskGraph) BeginTransaction() (GraphTransaction, error) {
	// Acquire write lock for the duration of the transaction
	// This provides serializable isolation
	g.mu.Lock()

	tx, err := g.boltStore.BeginTx(true)
	if err != nil {
		g.mu.Unlock()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &DiskGraphTransaction{
		g:            g,
		tx:           tx,
		createdNodes: make([]*Node, 0),
		createdRels:  make([]*Relationship, 0),
		deletedNodes: make([]string, 0),
		deletedRels:  make([]string, 0),
	}, nil
}

// CreateNode creates a node within the transaction
func (t *DiskGraphTransaction) CreateNode(labels ...string) (*Node, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}

	node := &Node{
		ID:         uuid.New().String(),
		Labels:     labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
	}

	if err := t.tx.SaveNode(node); err != nil {
		return nil, err
	}

	t.createdNodes = append(t.createdNodes, node)
	return node, nil
}

// GetNode retrieves a node within the transaction
func (t *DiskGraphTransaction) GetNode(id string) (*Node, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}
	return t.tx.GetNode(id)
}

// SetNodeProperty sets a property on a node within the transaction
func (t *DiskGraphTransaction) SetNodeProperty(nodeID, key string, value interface{}) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	node, err := t.tx.GetNode(nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[key] = value

	return t.tx.SaveNode(node)
}

// DeleteNode marks a node as deleted within the transaction
func (t *DiskGraphTransaction) DeleteNode(nodeID string) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	// First delete all relationships involving this node
	rels, err := t.tx.GetAllRelationships()
	if err != nil {
		return err
	}

	now := time.Now()
	for _, rel := range rels {
		if rel.ValidTo == nil && (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) {
			if err := t.tx.DeleteRelationship(rel.ID, now); err != nil {
				return err
			}
			t.deletedRels = append(t.deletedRels, rel.ID)
		}
	}

	// Delete the node
	if err := t.tx.DeleteNode(nodeID, now); err != nil {
		return err
	}
	t.deletedNodes = append(t.deletedNodes, nodeID)

	return nil
}

// CreateRelationship creates a relationship within the transaction
func (t *DiskGraphTransaction) CreateRelationship(relType, fromID, toID string) (*Relationship, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}

	// Verify nodes exist
	from, err := t.tx.GetNode(fromID)
	if err != nil || from == nil {
		return nil, fmt.Errorf("from node not found: %s", fromID)
	}

	to, err := t.tx.GetNode(toID)
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
	}

	if err := t.tx.SaveRelationship(rel); err != nil {
		return nil, err
	}

	t.createdRels = append(t.createdRels, rel)
	return rel, nil
}

// GetRelationship retrieves a relationship within the transaction
func (t *DiskGraphTransaction) GetRelationship(id string) (*Relationship, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}
	return t.tx.GetRelationship(id)
}

// SetRelationshipProperty sets a property on a relationship within the transaction
func (t *DiskGraphTransaction) SetRelationshipProperty(relID, key string, value interface{}) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	rel, err := t.tx.GetRelationship(relID)
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

	return t.tx.SaveRelationship(rel)
}

// DeleteRelationship marks a relationship as deleted within the transaction
func (t *DiskGraphTransaction) DeleteRelationship(relID string) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	if err := t.tx.DeleteRelationship(relID, time.Now()); err != nil {
		return err
	}
	t.deletedRels = append(t.deletedRels, relID)

	return nil
}

// ExecuteQuery executes a full query within the transaction
// This is more complex - for now, we'll support CREATE queries
func (t *DiskGraphTransaction) ExecuteQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}

	switch query.QueryType {
	case "CREATE":
		return t.executeCreateQuery(query)
	case "MATCH":
		if query.CreateClause != nil {
			return t.executeMatchCreateQuery(query)
		}
		if query.SetClause != nil {
			return t.executeSetQuery(query)
		}
		if query.DeleteClause != nil {
			return t.executeDeleteQuery(query)
		}
		// Read-only MATCH - delegate to main graph (snapshot isolation)
		return t.executeReadQuery(query, embedder)
	default:
		return nil, fmt.Errorf("unsupported query type in transaction: %s", query.QueryType)
	}
}

// executeCreateQuery handles CREATE within a transaction
func (t *DiskGraphTransaction) executeCreateQuery(query *Query) (*QueryResult, error) {
	cc := query.CreateClause
	createdVars := make(map[string]interface{})
	createdCount := 0

	// Create nodes
	for _, nodeSpec := range cc.Nodes {
		node, err := t.CreateNode(nodeSpec.Labels...)
		if err != nil {
			return nil, err
		}

		for key, value := range nodeSpec.Properties {
			if err := t.SetNodeProperty(node.ID, key, value); err != nil {
				return nil, err
			}
		}

		if nodeSpec.Variable != "" {
			createdVars[nodeSpec.Variable] = node
		}
		createdCount++
	}

	// Create relationships
	for _, relSpec := range cc.Relationships {
		fromNode, ok := createdVars[relSpec.FromVar].(*Node)
		if !ok {
			continue
		}
		toNode, ok := createdVars[relSpec.ToVar].(*Node)
		if !ok {
			continue
		}

		rel, err := t.CreateRelationship(relSpec.Type, fromNode.ID, toNode.ID)
		if err != nil {
			return nil, err
		}

		for key, value := range relSpec.Properties {
			if err := t.SetRelationshipProperty(rel.ID, key, value); err != nil {
				return nil, err
			}
		}

		if relSpec.Variable != "" {
			createdVars[relSpec.Variable] = rel
		}
		createdCount++
	}

	return &QueryResult{
		Columns: []string{"created"},
		Rows:    []map[string]interface{}{{"created": createdCount}},
	}, nil
}

// executeMatchCreateQuery handles MATCH...CREATE within a transaction
func (t *DiskGraphTransaction) executeMatchCreateQuery(query *Query) (*QueryResult, error) {
	// Find matches using the underlying graph's read methods
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	createdCount := 0
	for _, match := range matches {
		for _, nodeSpec := range query.CreateClause.Nodes {
			node, err := t.CreateNode(nodeSpec.Labels...)
			if err != nil {
				return nil, err
			}

			for key, value := range nodeSpec.Properties {
				if err := t.SetNodeProperty(node.ID, key, value); err != nil {
					return nil, err
				}
			}

			match[nodeSpec.Variable] = node
			createdCount++
		}

		for _, relSpec := range query.CreateClause.Relationships {
			fromID := t.resolveNodeID(match, relSpec.FromVar)
			toID := t.resolveNodeID(match, relSpec.ToVar)

			if fromID == "" || toID == "" {
				continue
			}

			rel, err := t.CreateRelationship(relSpec.Type, fromID, toID)
			if err != nil {
				return nil, err
			}

			for key, value := range relSpec.Properties {
				if err := t.SetRelationshipProperty(rel.ID, key, value); err != nil {
					return nil, err
				}
			}
			createdCount++
		}
	}

	return &QueryResult{
		Columns: []string{"created"},
		Rows:    []map[string]interface{}{{"created": createdCount}},
	}, nil
}

// executeSetQuery handles MATCH...SET within a transaction
func (t *DiskGraphTransaction) executeSetQuery(query *Query) (*QueryResult, error) {
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	updatedCount := 0
	for _, match := range matches {
		for _, update := range query.SetClause.Updates {
			nodeID := t.resolveNodeID(match, update.Variable)
			if nodeID == "" {
				continue
			}

			if err := t.SetNodeProperty(nodeID, update.Property, update.Value); err != nil {
				return nil, err
			}
			updatedCount++
		}
	}

	return &QueryResult{
		Columns: []string{"updated"},
		Rows:    []map[string]interface{}{{"updated": updatedCount}},
	}, nil
}

// executeDeleteQuery handles MATCH...DELETE within a transaction
func (t *DiskGraphTransaction) executeDeleteQuery(query *Query) (*QueryResult, error) {
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	deletedCount := 0
	deletedIDs := make(map[string]bool)

	for _, match := range matches {
		for _, varName := range query.DeleteClause.Variables {
			switch v := match[varName].(type) {
			case *Node:
				if !deletedIDs[v.ID] {
					if err := t.DeleteNode(v.ID); err != nil {
						return nil, err
					}
					deletedIDs[v.ID] = true
					deletedCount++
				}
			case *Relationship:
				if !deletedIDs[v.ID] {
					if err := t.DeleteRelationship(v.ID); err != nil {
						return nil, err
					}
					deletedIDs[v.ID] = true
					deletedCount++
				}
			}
		}
	}

	return &QueryResult{
		Columns: []string{"deleted"},
		Rows:    []map[string]interface{}{{"deleted": deletedCount}},
	}, nil
}

// executeReadQuery handles read-only MATCH queries using a snapshot
func (t *DiskGraphTransaction) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	// For read queries within a transaction, use the current transaction's view
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	if len(matches) == 0 {
		return &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}, nil
	}

	// Build result based on RETURN clause
	return t.buildQueryResult(matches, query.ReturnClause)
}

// findMatches finds pattern matches within the transaction
func (t *DiskGraphTransaction) findMatches(pattern *MatchPattern, where *WhereClause) []map[string]interface{} {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	// Get all nodes from the transaction
	allNodes, err := t.tx.GetAllNodes()
	if err != nil {
		return nil
	}

	// Filter by first node pattern
	firstPattern := pattern.Nodes[0]
	var candidates []*Node

	for _, node := range allNodes {
		if node.ValidTo != nil {
			continue // Skip deleted nodes
		}
		if len(firstPattern.Labels) > 0 {
			hasLabel := false
			for _, label := range node.Labels {
				if label == firstPattern.Labels[0] {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}
		// Check properties
		if t.nodeMatchesProperties(node, firstPattern.Properties) {
			candidates = append(candidates, node)
		}
	}

	// Build matches
	var matches []map[string]interface{}
	for _, node := range candidates {
		match := map[string]interface{}{
			firstPattern.Variable: node,
		}
		matches = append(matches, match)
	}

	// Handle relationships if present
	if len(pattern.Relationships) > 0 {
		matches = t.expandRelationshipMatches(matches, pattern, allNodes)
	}

	// Apply WHERE clause
	if where != nil {
		matches = t.filterByWhere(matches, where)
	}

	return matches
}

// Helper methods for transaction query execution
func (t *DiskGraphTransaction) nodeMatchesProperties(node *Node, props map[string]interface{}) bool {
	for key, expected := range props {
		actual, ok := node.Properties[key]
		if !ok {
			return false
		}
		if !valuesEqual(actual, expected) {
			return false
		}
	}
	return true
}

func (t *DiskGraphTransaction) expandRelationshipMatches(matches []map[string]interface{}, pattern *MatchPattern, allNodes []*Node) []map[string]interface{} {
	allRels, err := t.tx.GetAllRelationships()
	if err != nil {
		return matches
	}

	var expanded []map[string]interface{}

	for _, match := range matches {
		for _, relPattern := range pattern.Relationships {
			// Get from/to node patterns using indices
			if relPattern.FromIndex >= len(pattern.Nodes) || relPattern.ToIndex >= len(pattern.Nodes) {
				continue
			}
			fromNodePattern := pattern.Nodes[relPattern.FromIndex]
			toNodePattern := pattern.Nodes[relPattern.ToIndex]

			fromNode, _ := match[fromNodePattern.Variable].(*Node)
			if fromNode == nil {
				continue
			}

			for _, rel := range allRels {
				if rel.ValidTo != nil {
					continue
				}
				// Check relationship type
				if len(relPattern.Types) > 0 {
					typeMatch := false
					for _, t := range relPattern.Types {
						if rel.Type == t {
							typeMatch = true
							break
						}
					}
					if !typeMatch {
						continue
					}
				}

				var targetNode *Node
				if rel.FromNodeID == fromNode.ID {
					// Find the target node
					for _, n := range allNodes {
						if n.ID == rel.ToNodeID && n.ValidTo == nil {
							targetNode = n
							break
						}
					}
				}

				if targetNode == nil {
					continue
				}

				// Check if target matches the pattern
				if len(toNodePattern.Labels) > 0 {
					hasLabel := false
					for _, l := range targetNode.Labels {
						if l == toNodePattern.Labels[0] {
							hasLabel = true
							break
						}
					}
					if !hasLabel {
						continue
					}
				}

				newMatch := make(map[string]interface{})
				for k, v := range match {
					newMatch[k] = v
				}
				if relPattern.Variable != "" {
					newMatch[relPattern.Variable] = rel
				}
				newMatch[toNodePattern.Variable] = targetNode
				expanded = append(expanded, newMatch)
			}
		}
	}

	if len(expanded) > 0 {
		return expanded
	}
	return matches
}

func (t *DiskGraphTransaction) filterByWhere(matches []map[string]interface{}, where *WhereClause) []map[string]interface{} {
	var filtered []map[string]interface{}
	for _, match := range matches {
		if t.evaluateWhereConditions(match, where.Conditions) {
			filtered = append(filtered, match)
		}
	}
	return filtered
}

// evaluateWhereConditions evaluates all conditions in a WHERE clause
func (t *DiskGraphTransaction) evaluateWhereConditions(match map[string]interface{}, conditions []Condition) bool {
	for _, cond := range conditions {
		entity, ok := match[cond.Variable]
		if !ok {
			return false
		}

		var propVal interface{}
		switch e := entity.(type) {
		case *Node:
			propVal = e.Properties[cond.Property]
		case *Relationship:
			propVal = e.Properties[cond.Property]
		default:
			return false
		}

		if !evaluateCondition(propVal, cond.Operator, cond.Value) {
			return false
		}
	}
	return true
}

func (t *DiskGraphTransaction) resolveNodeID(match map[string]interface{}, varName string) string {
	if v, ok := match[varName]; ok {
		if node, ok := v.(*Node); ok {
			return node.ID
		}
	}
	return ""
}

func (t *DiskGraphTransaction) buildQueryResult(matches []map[string]interface{}, returnClause *ReturnClause) (*QueryResult, error) {
	if returnClause == nil || len(returnClause.Items) == 0 {
		return &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}, nil
	}

	columns := make([]string, len(returnClause.Items))
	for i, item := range returnClause.Items {
		if item.Alias != "" {
			columns[i] = item.Alias
		} else if item.Property != "" {
			columns[i] = item.Variable + "." + item.Property
		} else {
			columns[i] = item.Variable
		}
	}

	rows := make([]map[string]interface{}, 0, len(matches))
	for _, match := range matches {
		row := make(map[string]interface{})
		for i, item := range returnClause.Items {
			col := columns[i]
			if v, ok := match[item.Variable]; ok {
				if item.Property != "" {
					if node, ok := v.(*Node); ok {
						row[col] = node.Properties[item.Property]
					} else if rel, ok := v.(*Relationship); ok {
						row[col] = rel.Properties[item.Property]
					}
				} else {
					row[col] = v
				}
			}
		}
		rows = append(rows, row)
	}

	// Apply ORDER BY and LIMIT if present (from ReturnClause)
	if len(returnClause.OrderBy) > 0 {
		sortRowsByOrderItems(rows, returnClause.OrderBy)
	}
	if returnClause.Limit > 0 && len(rows) > returnClause.Limit {
		rows = rows[:returnClause.Limit]
	}

	return &QueryResult{Columns: columns, Rows: rows}, nil
}

// sortRowsByOrderItems sorts rows by order items
func sortRowsByOrderItems(rows []map[string]interface{}, orderBy []OrderItem) {
	if len(orderBy) == 0 || len(rows) == 0 {
		return
	}

	// Simple sort by first order item
	item := orderBy[0]
	key := item.Variable
	if item.Property != "" {
		key = item.Variable + "." + item.Property
	}

	// Use sort.Slice for stable sorting
	for i := 0; i < len(rows)-1; i++ {
		for j := i + 1; j < len(rows); j++ {
			shouldSwap := false
			vi := rows[i][key]
			vj := rows[j][key]

			// Compare based on type
			switch a := vi.(type) {
			case int:
				if b, ok := vj.(int); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			case int64:
				if b, ok := vj.(int64); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			case float64:
				if b, ok := vj.(float64); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			case string:
				if b, ok := vj.(string); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			}

			if shouldSwap {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}

// Commit commits the transaction and updates in-memory indexes
func (t *DiskGraphTransaction) Commit() error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}
	t.committed = true

	// Commit the underlying bbolt transaction
	if err := t.tx.Commit(); err != nil {
		t.g.mu.Unlock()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update in-memory indexes for created nodes
	for _, node := range t.createdNodes {
		for _, label := range node.Labels {
			t.g.labelIndex[label] = append(t.g.labelIndex[label], node.ID)
		}
		t.g.nodeCache.Add(node.ID, node)
	}

	// Update in-memory indexes for created relationships
	for _, rel := range t.createdRels {
		t.g.nodeRelIndex[rel.FromNodeID] = append(t.g.nodeRelIndex[rel.FromNodeID], rel.ID)
		t.g.nodeRelIndex[rel.ToNodeID] = append(t.g.nodeRelIndex[rel.ToNodeID], rel.ID)
		t.g.relCache.Add(rel.ID, rel)
	}

	// Update indexes for deleted nodes
	for _, nodeID := range t.deletedNodes {
		// Get the node to find its labels
		node, _ := t.g.boltStore.GetNode(nodeID)
		if node != nil {
			for _, label := range node.Labels {
				ids := t.g.labelIndex[label]
				for i, id := range ids {
					if id == nodeID {
						t.g.labelIndex[label] = append(ids[:i], ids[i+1:]...)
						break
					}
				}
			}
		}
		delete(t.g.nodeRelIndex, nodeID)
		t.g.nodeCache.Remove(nodeID)
	}

	// Update indexes for deleted relationships
	for _, relID := range t.deletedRels {
		rel, _ := t.g.boltStore.GetRelationship(relID)
		if rel != nil {
			t.g.removeFromRelIndex(rel.FromNodeID, relID)
			t.g.removeFromRelIndex(rel.ToNodeID, relID)
		}
		t.g.relCache.Remove(relID)
	}

	// Release the write lock
	t.g.mu.Unlock()

	return nil
}

// Rollback aborts the transaction, discarding all changes
func (t *DiskGraphTransaction) Rollback() error {
	if t.committed {
		return nil // Already completed
	}
	t.committed = true

	// Rollback the underlying bbolt transaction
	err := t.tx.Rollback()

	// Release the write lock
	t.g.mu.Unlock()

	return err
}
