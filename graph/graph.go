package graph

import (
	"fmt"
	"sync"
	"time"

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

// CreateNode adds a node to the graph
// If persistence is enabled, the operation is persisted to bbolt
func (g *Graph) CreateNode(labels ...string) *Node {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.createNodeUnlocked(labels...)
}

// createNodeUnlocked creates a node without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
func (g *Graph) createNodeUnlocked(labels ...string) *Node {
	node := NewNode(labels...)

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
// If persistence is enabled, the operation is persisted to bbolt
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

// GetAllNodeVersions returns all versions of all nodes (for building timelines)
// This includes historical versions that have been modified or deleted
func (g *Graph) GetAllNodeVersions() []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := []*Node{}
	for _, versions := range g.nodeVersions {
		nodes = append(nodes, versions...)
	}
	return nodes
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

// GetAllRelationshipVersions returns all versions of all relationships (for building timelines)
// This includes historical versions that have been modified or deleted
func (g *Graph) GetAllRelationshipVersions() []*Relationship {
	g.mu.RLock()
	defer g.mu.RUnlock()

	rels := []*Relationship{}
	for _, versions := range g.relationshipVersions {
		rels = append(rels, versions...)
	}
	return rels
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
// If persistence is enabled, the operation is persisted to bbolt
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
// If persistence is enabled, the operation is persisted to bbolt
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
// This is a helper method that persists the operation to bbolt if enabled
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

// DeleteNodeProperty removes a property from a node
// Creates a new version of the node without the property to preserve temporal history
func (g *Graph) DeleteNodeProperty(nodeID, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteNodePropertyUnlocked(nodeID, key)
}

// deleteNodePropertyUnlocked removes a property without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
// Creates a new version of the node to preserve temporal history
func (g *Graph) deleteNodePropertyUnlocked(nodeID, key string) error {
	currentNode, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	// Check if property exists
	if _, exists := currentNode.Properties[key]; !exists {
		return nil // Property doesn't exist, nothing to delete
	}

	now := time.Now()

	// Close out the current version
	currentNode.ValidTo = &now

	// Create a new version with copied properties (excluding the deleted one)
	newNode := &Node{
		ID:         currentNode.ID,
		Labels:     currentNode.Labels, // Labels array is not modified, safe to share reference
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil, // Currently valid
	}

	// Deep copy properties except the one being deleted
	for k, v := range currentNode.Properties {
		if k != key {
			newNode.Properties[k] = v
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
			return fmt.Errorf("failed to persist node property deletion to bbolt: %w", err)
		}
	}

	return nil
}

// SetRelationshipProperty sets a property on a relationship
// This is a helper method that persists the operation to bbolt if enabled
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

// DeleteRelationshipProperty removes a property from a relationship
// Creates a new version of the relationship without the property to preserve temporal history
func (g *Graph) DeleteRelationshipProperty(relID, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteRelationshipPropertyUnlocked(relID, key)
}

// deleteRelationshipPropertyUnlocked removes a property without acquiring a lock (internal use)
// Caller must already hold g.mu.Lock()
// Creates a new version of the relationship to preserve temporal history
func (g *Graph) deleteRelationshipPropertyUnlocked(relID, key string) error {
	currentRel, exists := g.relationships[relID]
	if !exists {
		return fmt.Errorf("relationship with ID %s not found", relID)
	}

	// Check if property exists
	if _, exists := currentRel.Properties[key]; !exists {
		return nil // Property doesn't exist, nothing to delete
	}

	now := time.Now()

	// Close out the current version
	currentRel.ValidTo = &now

	// Create a new version with copied properties (excluding the deleted one)
	newRel := &Relationship{
		ID:         currentRel.ID,
		Type:       currentRel.Type,
		FromNodeID: currentRel.FromNodeID,
		ToNodeID:   currentRel.ToNodeID,
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil, // Currently valid
	}

	// Deep copy properties except the one being deleted
	for k, v := range currentRel.Properties {
		if k != key {
			newRel.Properties[k] = v
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
			return fmt.Errorf("failed to persist relationship property deletion to bbolt: %w", err)
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
