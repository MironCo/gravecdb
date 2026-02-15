package graph

import (
	"fmt"
	"sync"
	"time"
)

// Graph represents the in-memory graph database with optional disk persistence
type Graph struct {
	nodes              map[string]*Node
	nodeVersions       map[string][]*Node          // All versions of each node for temporal queries
	relationships      map[string]*Relationship
	relationshipVersions map[string][]*Relationship // All versions of each relationship for temporal queries
	nodesByLabel       map[string]map[string]*Node // label -> nodeID -> Node
	embeddings         *EmbeddingStore             // Vector embeddings for semantic search
	mu                 sync.RWMutex
	wal                *WAL       // Write-Ahead Log for persistence (nil if persistence disabled)
	boltStore          *BoltStore // bbolt storage backend (nil if using WAL or no persistence)
}

// NewGraph creates a new graph database instance without persistence
func NewGraph() *Graph {
	return &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           NewEmbeddingStore(),
	}
}

// NewGraphWithPersistence creates a new graph database with disk persistence
// dataDir: directory where the database files (WAL and snapshots) will be stored
// If a previous database exists in dataDir, it will be recovered automatically
// Uses default settings (balanced performance and durability)
func NewGraphWithPersistence(dataDir string) (*Graph, error) {
	// Create the WAL instance with default settings
	wal, err := NewWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	g := &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           NewEmbeddingStore(),
		wal:                  wal,
	}

	// Attempt to recover from disk
	if err := g.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover graph: %w", err)
	}

	return g, nil
}

// NewGraphWithMaxDurability creates a graph with maximum durability (slow writes)
// Every write is immediately synced to disk - safest but slowest option
func NewGraphWithMaxDurability(dataDir string) (*Graph, error) {
	wal, err := NewWALWithOptions(dataDir, WALOptions{
		BufferSize: 1,
		SyncMode:   SyncEveryWrite,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	g := &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           NewEmbeddingStore(),
		wal:                  wal,
	}

	if err := g.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover graph: %w", err)
	}

	return g, nil
}

// NewGraphWithMaxPerformance creates a graph optimized for write performance
// Batches writes and syncs periodically - fastest but may lose recent writes on crash
func NewGraphWithMaxPerformance(dataDir string) (*Graph, error) {
	wal, err := NewWALWithOptions(dataDir, WALOptions{
		BufferSize:    1000, // Large buffer
		SyncMode:      SyncPeriodic,
		FlushInterval: 1 * time.Second, // Sync every second
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	g := &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           NewEmbeddingStore(),
		wal:                  wal,
	}

	if err := g.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover graph: %w", err)
	}

	return g, nil
}

// NewGraphWithBolt creates a new graph database with bbolt persistence
// This is the recommended persistence layer - provides ACID transactions and MVCC
// dataDir: directory where gravecdb.db file will be stored
func NewGraphWithBolt(dataDir string) (*Graph, error) {
	// Create the bbolt storage backend
	store, err := NewBoltStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}

	g := &Graph{
		nodes:                make(map[string]*Node),
		nodeVersions:         make(map[string][]*Node),
		relationships:        make(map[string]*Relationship),
		relationshipVersions: make(map[string][]*Relationship),
		nodesByLabel:         make(map[string]map[string]*Node),
		embeddings:           NewEmbeddingStore(),
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

// recover rebuilds the graph state from disk
// 1. Load the latest snapshot (if exists)
// 2. Replay all operations from the WAL that occurred after the snapshot
func (g *Graph) recover() error {
	if g.wal == nil {
		return nil // No persistence enabled
	}

	// Step 1: Load the latest snapshot
	snapshot, err := g.wal.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	if snapshot != nil {
		// Restore the graph state from the snapshot
		g.nodes = snapshot.Nodes
		g.relationships = snapshot.Relationships
		g.nodesByLabel = snapshot.NodesByLabel
	}

	// Step 2: Replay operations from the WAL
	// These are operations that occurred after the snapshot was created
	operations, err := g.wal.ReadOperations()
	if err != nil {
		return fmt.Errorf("failed to read WAL operations: %w", err)
	}

	// Apply each operation in order to bring the graph to the latest state
	for _, op := range operations {
		if err := g.replayOperation(op); err != nil {
			return fmt.Errorf("failed to replay operation: %w", err)
		}
	}

	return nil
}

// replayOperation applies a logged operation to the graph
// Used during recovery to rebuild state from the WAL
func (g *Graph) replayOperation(op Operation) error {
	switch op.Type {
	case "CREATE_NODE":
		// Recreate a node with its original ID and labels
		nodeID := op.Data["id"].(string)
		labelsInterface := op.Data["labels"].([]interface{})
		labels := make([]string, len(labelsInterface))
		for i, l := range labelsInterface {
			labels[i] = l.(string)
		}

		node := &Node{
			ID:         nodeID,
			Labels:     labels,
			Properties: make(map[string]interface{}),
			ValidFrom:  op.Timestamp, // Use the operation timestamp
			ValidTo:    nil,
		}
		g.nodes[nodeID] = node

		// Rebuild label index
		for _, label := range labels {
			if g.nodesByLabel[label] == nil {
				g.nodesByLabel[label] = make(map[string]*Node)
			}
			g.nodesByLabel[label][nodeID] = node
		}

	case "CREATE_RELATIONSHIP":
		// Recreate a relationship with its original ID
		relID := op.Data["id"].(string)
		relType := op.Data["type"].(string)
		fromNodeID := op.Data["from"].(string)
		toNodeID := op.Data["to"].(string)

		rel := &Relationship{
			ID:         relID,
			Type:       relType,
			FromNodeID: fromNodeID,
			ToNodeID:   toNodeID,
			Properties: make(map[string]interface{}),
			ValidFrom:  op.Timestamp, // Use the operation timestamp
			ValidTo:    nil,
		}
		g.relationships[relID] = rel

	case "SET_NODE_PROPERTY":
		// Set a property on a node
		nodeID := op.Data["node_id"].(string)
		key := op.Data["key"].(string)
		value := op.Data["value"]

		if node, exists := g.nodes[nodeID]; exists {
			node.Properties[key] = value
		}

	case "SET_REL_PROPERTY":
		// Set a property on a relationship
		relID := op.Data["rel_id"].(string)
		key := op.Data["key"].(string)
		value := op.Data["value"]

		if rel, exists := g.relationships[relID]; exists {
			rel.Properties[key] = value
		}

	case "DELETE_NODE":
		// Soft delete a node by setting ValidTo timestamp
		nodeID := op.Data["node_id"].(string)
		if node, exists := g.nodes[nodeID]; exists {
			// Parse the deleted_at timestamp from the operation
			if deletedAtStr, ok := op.Data["deleted_at"].(string); ok {
				deletedAt, err := time.Parse(time.RFC3339Nano, deletedAtStr)
				if err == nil {
					node.ValidTo = &deletedAt
				}
			} else if deletedAt, ok := op.Data["deleted_at"].(time.Time); ok {
				node.ValidTo = &deletedAt
			}
		}

	case "DELETE_RELATIONSHIP":
		// Soft delete a relationship by setting ValidTo timestamp
		relID := op.Data["rel_id"].(string)
		if rel, exists := g.relationships[relID]; exists {
			// Parse the deleted_at timestamp from the operation
			if deletedAtStr, ok := op.Data["deleted_at"].(string); ok {
				deletedAt, err := time.Parse(time.RFC3339Nano, deletedAtStr)
				if err == nil {
					rel.ValidTo = &deletedAt
				}
			} else if deletedAt, ok := op.Data["deleted_at"].(time.Time); ok {
				rel.ValidTo = &deletedAt
			}
		}
	}

	return nil
}

// Snapshot creates a snapshot of the current graph state and truncates the WAL
// This should be called periodically to avoid the WAL growing too large
func (g *Graph) Snapshot() error {
	if g.wal == nil {
		return nil // No persistence enabled
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	// Create a snapshot of the current state
	snapshot := &Snapshot{
		Nodes:         g.nodes,
		Relationships: g.relationships,
		NodesByLabel:  g.nodesByLabel,
	}

	// Write the snapshot to disk
	if err := g.wal.CreateSnapshot(snapshot); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Clear the WAL since all operations are now in the snapshot
	if err := g.wal.TruncateLog(); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	return nil
}

// Close flushes any pending writes and closes the database
// Should be called when shutting down to ensure all data is saved
func (g *Graph) Close() error {
	if g.wal != nil {
		return g.wal.Close()
	}
	if g.boltStore != nil {
		return g.boltStore.Close()
	}
	return nil
}

// CreateNode adds a node to the graph
// If persistence is enabled, the operation is logged to the WAL before being applied
func (g *Graph) CreateNode(labels ...string) *Node {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.createNodeUnlocked(labels...)
}

// createNodeUnlocked creates a node without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
func (g *Graph) createNodeUnlocked(labels ...string) *Node {
	node := NewNode(labels...)

	// Log the operation to the WAL before applying it
	// This ensures durability - if we crash after writing to WAL but before
	// completing the in-memory update, we can replay the operation on recovery
	if g.wal != nil {
		op := Operation{
			Type: "CREATE_NODE",
			Data: map[string]interface{}{
				"id":     node.ID,
				"labels": labels,
			},
		}
		if err := g.wal.WriteOperation(op); err != nil {
			// In a production system, we'd handle this error more gracefully
			panic(fmt.Sprintf("failed to write operation to WAL: %v", err))
		}
	}

	// Apply the operation to the in-memory graph
	g.nodes[node.ID] = node

	// Add to version history
	g.nodeVersions[node.ID] = append(g.nodeVersions[node.ID], node)

	// Index by labels for fast lookup
	for _, label := range labels {
		if g.nodesByLabel[label] == nil {
			g.nodesByLabel[label] = make(map[string]*Node)
		}
		g.nodesByLabel[label][node.ID] = node
	}

	// Persist to bbolt if enabled
	if g.boltStore != nil {
		if err := g.boltStore.SaveNode(node); err != nil {
			// Rollback in-memory changes on persistence failure
			delete(g.nodes, node.ID)
			g.nodeVersions[node.ID] = g.nodeVersions[node.ID][:len(g.nodeVersions[node.ID])-1]
			for _, label := range labels {
				delete(g.nodesByLabel[label], node.ID)
			}
			panic(fmt.Sprintf("failed to persist node to bbolt: %v", err))
		}
	}

	return node
}

// GetNode retrieves a node by ID if it is currently valid (not deleted)
// For historical queries, use graph.AsOf(time).GetNode(id)
func (g *Graph) GetNode(id string) (*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, exists := g.nodes[id]
	if !exists {
		return nil, fmt.Errorf("node with ID %s not found", id)
	}

	// Only return the node if it's currently valid (not soft-deleted)
	if !node.IsCurrentlyValid() {
		return nil, fmt.Errorf("node with ID %s not found", id)
	}

	return node, nil
}

// CreateRelationship creates a relationship between two nodes
// If persistence is enabled, the operation is logged to the WAL before being applied
func (g *Graph) CreateRelationship(relType string, fromNodeID, toNodeID string) (*Relationship, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.createRelationshipUnlocked(relType, fromNodeID, toNodeID)
}

// createRelationshipUnlocked creates a relationship without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
func (g *Graph) createRelationshipUnlocked(relType string, fromNodeID, toNodeID string) (*Relationship, error) {
	// Verify both nodes exist before creating the relationship
	if _, exists := g.nodes[fromNodeID]; !exists {
		return nil, fmt.Errorf("from node with ID %s not found", fromNodeID)
	}
	if _, exists := g.nodes[toNodeID]; !exists {
		return nil, fmt.Errorf("to node with ID %s not found", toNodeID)
	}

	rel := NewRelationship(relType, fromNodeID, toNodeID)

	// Log the operation to the WAL
	if g.wal != nil {
		op := Operation{
			Type: "CREATE_RELATIONSHIP",
			Data: map[string]interface{}{
				"id":   rel.ID,
				"type": relType,
				"from": fromNodeID,
				"to":   toNodeID,
			},
		}
		if err := g.wal.WriteOperation(op); err != nil {
			return nil, fmt.Errorf("failed to write operation to WAL: %w", err)
		}
	}

	// Apply the operation to the in-memory graph
	g.relationships[rel.ID] = rel

	// Add to version history
	g.relationshipVersions[rel.ID] = append(g.relationshipVersions[rel.ID], rel)

	// Persist to bbolt if enabled
	if g.boltStore != nil {
		if err := g.boltStore.SaveRelationship(rel); err != nil {
			// Rollback in-memory changes
			delete(g.relationships, rel.ID)
			g.relationshipVersions[rel.ID] = g.relationshipVersions[rel.ID][:len(g.relationshipVersions[rel.ID])-1]
			return nil, fmt.Errorf("failed to persist relationship to bbolt: %w", err)
		}
	}

	return rel, nil
}

// GetRelationship retrieves a relationship by ID if it is currently valid (not deleted)
// For historical queries, use graph.AsOf(time).GetRelationship(id)
func (g *Graph) GetRelationship(id string) (*Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	rel, exists := g.relationships[id]
	if !exists {
		return nil, fmt.Errorf("relationship with ID %s not found", id)
	}

	// Only return the relationship if it's currently valid (not soft-deleted)
	if !rel.IsCurrentlyValid() {
		return nil, fmt.Errorf("relationship with ID %s not found", id)
	}

	return rel, nil
}

// GetNodesByLabel retrieves all currently valid nodes with a specific label
// For historical queries, use graph.AsOf(time).GetNodesByLabel(label)
func (g *Graph) GetNodesByLabel(label string) []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getNodesByLabelUnlocked(label)
}

// getNodesByLabelUnlocked gets nodes by label without acquiring a lock (internal use)
// Caller must already hold g.mu.RLock() or g.mu.Lock()
func (g *Graph) getNodesByLabelUnlocked(label string) []*Node {
	nodes := []*Node{}
	if nodeMap, exists := g.nodesByLabel[label]; exists {
		for _, node := range nodeMap {
			// Only include nodes that are currently valid (not soft-deleted)
			if node.IsCurrentlyValid() {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// GetRelationshipsForNode retrieves all currently valid relationships connected to a node
// For historical queries, use graph.AsOf(time).GetRelationshipsForNode(nodeID)
func (g *Graph) GetRelationshipsForNode(nodeID string) []*Relationship {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getRelationshipsForNodeUnlocked(nodeID)
}

// getRelationshipsForNodeUnlocked gets relationships without acquiring a lock (internal use)
// Caller must already hold g.mu.RLock() or g.mu.Lock()
func (g *Graph) getRelationshipsForNodeUnlocked(nodeID string) []*Relationship {
	rels := []*Relationship{}
	for _, rel := range g.relationships {
		// Only include relationships that are currently valid (not soft-deleted)
		if (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) && rel.IsCurrentlyValid() {
			rels = append(rels, rel)
		}
	}
	return rels
}

// DeleteNode marks a node as deleted by setting its ValidTo timestamp
// This is a soft delete - the node remains in the graph for historical queries
// All relationships connected to this node are also marked as deleted
// If persistence is enabled, the operation is logged to the WAL before being applied
func (g *Graph) DeleteNode(nodeID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteNodeUnlocked(nodeID)
}

// deleteNodeUnlocked deletes a node without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
func (g *Graph) deleteNodeUnlocked(nodeID string) error {
	node, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	// Check if already deleted
	if !node.IsCurrentlyValid() {
		return fmt.Errorf("node with ID %s is already deleted", nodeID)
	}

	now := time.Now()

	// Log the operation to the WAL
	if g.wal != nil {
		op := Operation{
			Type: "DELETE_NODE",
			Data: map[string]interface{}{
				"node_id":    nodeID,
				"deleted_at": now,
			},
		}
		if err := g.wal.WriteOperation(op); err != nil {
			return fmt.Errorf("failed to write operation to WAL: %w", err)
		}
	}

	// Soft delete: set ValidTo timestamp instead of removing from map
	node.ValidTo = &now

	// Also soft delete all connected relationships
	for _, rel := range g.relationships {
		if (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) && rel.IsCurrentlyValid() {
			rel.ValidTo = &now
			// Persist each relationship deletion to bbolt
			if g.boltStore != nil {
				if err := g.boltStore.SaveRelationship(rel); err != nil {
					return fmt.Errorf("failed to persist relationship deletion to bbolt: %w", err)
				}
			}
		}
	}

	// Persist node deletion to bbolt if enabled
	if g.boltStore != nil {
		if err := g.boltStore.DeleteNode(nodeID, now); err != nil {
			return fmt.Errorf("failed to persist node deletion to bbolt: %w", err)
		}
	}

	return nil
}

// DeleteRelationship marks a relationship as deleted by setting its ValidTo timestamp
// This is a soft delete - the relationship remains in the graph for historical queries
// If persistence is enabled, the operation is logged to the WAL before being applied
func (g *Graph) DeleteRelationship(relID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteRelationshipUnlocked(relID)
}

// deleteRelationshipUnlocked deletes a relationship without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
func (g *Graph) deleteRelationshipUnlocked(relID string) error {
	rel, exists := g.relationships[relID]
	if !exists {
		return fmt.Errorf("relationship with ID %s not found", relID)
	}

	// Check if already deleted
	if !rel.IsCurrentlyValid() {
		return fmt.Errorf("relationship with ID %s is already deleted", relID)
	}

	now := time.Now()

	// Log the operation to the WAL
	if g.wal != nil {
		op := Operation{
			Type: "DELETE_RELATIONSHIP",
			Data: map[string]interface{}{
				"rel_id":     relID,
				"deleted_at": now,
			},
		}
		if err := g.wal.WriteOperation(op); err != nil {
			return fmt.Errorf("failed to write operation to WAL: %w", err)
		}
	}

	// Soft delete: set ValidTo timestamp instead of removing from map
	rel.ValidTo = &now

	// Persist to bbolt if enabled
	if g.boltStore != nil {
		if err := g.boltStore.DeleteRelationship(relID, now); err != nil {
			return fmt.Errorf("failed to persist relationship deletion to bbolt: %w", err)
		}
	}

	return nil
}

// SetNodeProperty sets a property on a node
// This is a helper method that logs the operation to the WAL if persistence is enabled
// Use this instead of calling node.SetProperty directly when persistence is needed
func (g *Graph) SetNodeProperty(nodeID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.setNodePropertyUnlocked(nodeID, key, value)
}

// setNodePropertyUnlocked sets a property without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
// Creates a new version of the node to preserve temporal history
func (g *Graph) setNodePropertyUnlocked(nodeID, key string, value interface{}) error {
	currentNode, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	// Check if value is actually changing
	if currentVal, exists := currentNode.Properties[key]; exists && currentVal == value {
		return nil // No change needed
	}

	now := time.Now()

	// Close out the current version
	currentNode.ValidTo = &now

	// Create a new version with copied properties
	newNode := &Node{
		ID:         currentNode.ID,
		Labels:     currentNode.Labels, // Labels array is not modified, safe to share reference
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil, // Currently valid
	}

	// Deep copy properties
	for k, v := range currentNode.Properties {
		newNode.Properties[k] = v
	}

	// Apply the new value
	newNode.Properties[key] = value

	// Log the operation to the WAL
	if g.wal != nil {
		op := Operation{
			Type: "SET_NODE_PROPERTY",
			Data: map[string]interface{}{
				"node_id": nodeID,
				"key":     key,
				"value":   value,
			},
		}
		if err := g.wal.WriteOperation(op); err != nil {
			// Rollback the version change
			currentNode.ValidTo = nil
			return fmt.Errorf("failed to write operation to WAL: %w", err)
		}
	}

	// Update the current node pointer
	g.nodes[nodeID] = newNode

	// Add to version history
	g.nodeVersions[nodeID] = append(g.nodeVersions[nodeID], newNode)

	// Update label index
	for _, label := range newNode.Labels {
		g.nodesByLabel[label][nodeID] = newNode
	}

	// Persist to bbolt if enabled
	if g.boltStore != nil {
		if err := g.boltStore.SaveNode(newNode); err != nil {
			// Rollback changes
			currentNode.ValidTo = nil
			g.nodes[nodeID] = currentNode
			g.nodeVersions[nodeID] = g.nodeVersions[nodeID][:len(g.nodeVersions[nodeID])-1]
			for _, label := range newNode.Labels {
				g.nodesByLabel[label][nodeID] = currentNode
			}
			return fmt.Errorf("failed to persist node property to bbolt: %w", err)
		}
	}

	return nil
}

// SetRelationshipProperty sets a property on a relationship
// This is a helper method that logs the operation to the WAL if persistence is enabled
// Use this instead of calling relationship.SetProperty directly when persistence is needed
func (g *Graph) SetRelationshipProperty(relID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.setRelationshipPropertyUnlocked(relID, key, value)
}

// setRelationshipPropertyUnlocked sets a relationship property without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
func (g *Graph) setRelationshipPropertyUnlocked(relID, key string, value interface{}) error {
	currentRel, exists := g.relationships[relID]
	if !exists {
		return fmt.Errorf("relationship with ID %s not found", relID)
	}

	// Check if value is actually changing
	if currentVal, exists := currentRel.Properties[key]; exists && currentVal == value {
		return nil // No change needed
	}

	now := time.Now()

	// Close out the current version
	currentRel.ValidTo = &now

	// Create a new version with copied properties
	newRel := &Relationship{
		ID:         currentRel.ID,
		Type:       currentRel.Type,
		FromNodeID: currentRel.FromNodeID,
		ToNodeID:   currentRel.ToNodeID,
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil, // Currently valid
	}

	// Deep copy properties
	for k, v := range currentRel.Properties {
		newRel.Properties[k] = v
	}

	// Apply the new value
	newRel.Properties[key] = value

	// Log the operation to the WAL
	if g.wal != nil {
		op := Operation{
			Type: "SET_REL_PROPERTY",
			Data: map[string]interface{}{
				"rel_id": relID,
				"key":    key,
				"value":  value,
			},
		}
		if err := g.wal.WriteOperation(op); err != nil {
			// Rollback the version change
			currentRel.ValidTo = nil
			return fmt.Errorf("failed to write operation to WAL: %w", err)
		}
	}

	// Update the current relationship pointer
	g.relationships[relID] = newRel

	// Add to version history
	g.relationshipVersions[relID] = append(g.relationshipVersions[relID], newRel)

	// Persist to bbolt if enabled
	if g.boltStore != nil {
		if err := g.boltStore.SaveRelationship(newRel); err != nil {
			// Rollback changes
			currentRel.ValidTo = nil
			g.relationships[relID] = currentRel
			g.relationshipVersions[relID] = g.relationshipVersions[relID][:len(g.relationshipVersions[relID])-1]
			return fmt.Errorf("failed to persist relationship property to bbolt: %w", err)
		}
	}

	return nil
}

// SetNodeEmbedding stores a vector embedding for a node
// Previous embeddings are automatically versioned (ValidTo is set)
func (g *Graph) SetNodeEmbedding(nodeID string, vector []float32, model string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	node, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	// Create a deep copy of the node's current properties
	propertySnapshot := make(map[string]interface{})
	for k, v := range node.Properties {
		propertySnapshot[k] = v
	}

	g.embeddings.Add(nodeID, vector, model, propertySnapshot)

	// Persist to bbolt if enabled (save all embedding versions for this node)
	if g.boltStore != nil {
		// Get all versions from embedding store
		versions := g.embeddings.GetAll(nodeID)
		if err := g.boltStore.SaveEmbedding(nodeID, versions); err != nil {
			return fmt.Errorf("failed to persist embedding to bbolt: %w", err)
		}
	}

	return nil
}

// GetNodeEmbedding returns the current embedding for a node
func (g *Graph) GetNodeEmbedding(nodeID string) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetCurrent(nodeID)
}

// GetNodeEmbeddingAt returns the embedding that was valid at a specific time
func (g *Graph) GetNodeEmbeddingAt(nodeID string, t time.Time) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetAt(nodeID, t)
}

// SemanticSearch finds the k most similar nodes to a query vector
// Only searches nodes that are valid at the current time
func (g *Graph) SemanticSearch(query []float32, k int) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Build set of currently valid node IDs
	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsCurrentlyValid() {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, time.Now(), validNodeIDs)
}

// SemanticSearchAt finds the k most similar nodes to a query vector at a specific time
// Only searches nodes that were valid at the given time
func (g *Graph) SemanticSearchAt(query []float32, k int, asOf time.Time) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Build set of node IDs that were valid at the given time
	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsValidAt(asOf) {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, asOf, validNodeIDs)
}

// SemanticSearchAllVersions finds all historical versions of nodes similar to a query vector
// Returns all embedding versions across all time periods that match the query
func (g *Graph) SemanticSearchAllVersions(query []float32, k int, threshold float32, labelFilter []string, calculateDrift bool) []VersionedSearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Build set of valid node IDs (nodes that have ever existed)
	// Apply label filtering if specified
	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		// If label filter is specified, check if node has any of those labels
		if len(labelFilter) > 0 {
			hasLabel := false
			for _, label := range labelFilter {
				for _, nodeLabel := range node.Labels {
					if nodeLabel == label {
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
		validNodeIDs[id] = true
	}

	return g.embeddings.SearchAllVersions(query, k, validNodeIDs, threshold, calculateDrift)
}
