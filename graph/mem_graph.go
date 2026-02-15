package graph

import (
	"fmt"
	"sort"
	"sync"
	"time"

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

// ============================================================================
// Node Operations
// ============================================================================

// createNode adds a node to the in-memory graph
func (g *memGraph) createNode(labels ...string) *Node {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.createNodeUnlocked(labels...)
}

// createNodeUnlocked creates a node without acquiring a lock (internal use)
func (g *memGraph) createNodeUnlocked(labels ...string) *Node {
	node := NewNode(labels...)

	g.nodes[node.ID] = node
	g.nodeVersions[node.ID] = append(g.nodeVersions[node.ID], node)

	for _, label := range labels {
		if g.nodesByLabel[label] == nil {
			g.nodesByLabel[label] = make(map[string]*Node)
		}
		g.nodesByLabel[label][node.ID] = node
	}

	return node
}

// getNode retrieves a node by ID if it is currently valid
func (g *memGraph) getNode(id string) (*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, exists := g.nodes[id]
	if !exists {
		return nil, fmt.Errorf("node with ID %s not found", id)
	}

	if !node.IsCurrentlyValid() {
		return nil, fmt.Errorf("node with ID %s not found", id)
	}

	return node, nil
}

// getNodesByLabel retrieves all currently valid nodes with a specific label
func (g *memGraph) getNodesByLabel(label string) []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getNodesByLabelUnlocked(label)
}

// getAllNodeVersions returns all versions of all nodes
func (g *memGraph) getAllNodeVersions() []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := []*Node{}
	for _, versions := range g.nodeVersions {
		nodes = append(nodes, versions...)
	}
	return nodes
}

// getNodesByLabelUnlocked gets nodes by label without acquiring a lock
func (g *memGraph) getNodesByLabelUnlocked(label string) []*Node {
	nodes := []*Node{}
	if nodeMap, exists := g.nodesByLabel[label]; exists {
		for _, node := range nodeMap {
			if node.IsCurrentlyValid() {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// deleteNode marks a node as deleted
func (g *memGraph) deleteNode(nodeID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteNodeUnlocked(nodeID)
}

// deleteNodeUnlocked deletes a node without acquiring a lock
func (g *memGraph) deleteNodeUnlocked(nodeID string) error {
	node, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	if !node.IsCurrentlyValid() {
		return fmt.Errorf("node with ID %s is already deleted", nodeID)
	}

	now := time.Now()
	node.ValidTo = &now

	// Also soft delete all connected relationships
	for _, rel := range g.relationships {
		if (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) && rel.IsCurrentlyValid() {
			rel.ValidTo = &now
		}
	}

	return nil
}

// setNodeProperty sets a property on a node, creating a new version
func (g *memGraph) setNodeProperty(nodeID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.setNodePropertyUnlocked(nodeID, key, value)
}

// setNodePropertyUnlocked sets a property without acquiring a lock
func (g *memGraph) setNodePropertyUnlocked(nodeID, key string, value interface{}) error {
	currentNode, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	if currentVal, exists := currentNode.Properties[key]; exists && currentVal == value {
		return nil
	}

	now := time.Now()
	currentNode.ValidTo = &now

	newNode := &Node{
		ID:         currentNode.ID,
		Labels:     currentNode.Labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil,
	}

	for k, v := range currentNode.Properties {
		newNode.Properties[k] = v
	}
	newNode.Properties[key] = value

	g.nodes[nodeID] = newNode
	g.nodeVersions[nodeID] = append(g.nodeVersions[nodeID], newNode)

	for _, label := range newNode.Labels {
		g.nodesByLabel[label][nodeID] = newNode
	}

	return nil
}

// deleteNodeProperty removes a property from a node
func (g *memGraph) deleteNodeProperty(nodeID, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteNodePropertyUnlocked(nodeID, key)
}

// deleteNodePropertyUnlocked removes a property without acquiring a lock
func (g *memGraph) deleteNodePropertyUnlocked(nodeID, key string) error {
	currentNode, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	if _, exists := currentNode.Properties[key]; !exists {
		return nil
	}

	now := time.Now()
	currentNode.ValidTo = &now

	newNode := &Node{
		ID:         currentNode.ID,
		Labels:     currentNode.Labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil,
	}

	for k, v := range currentNode.Properties {
		if k != key {
			newNode.Properties[k] = v
		}
	}

	g.nodes[nodeID] = newNode
	g.nodeVersions[nodeID] = append(g.nodeVersions[nodeID], newNode)

	for _, label := range newNode.Labels {
		g.nodesByLabel[label][nodeID] = newNode
	}

	return nil
}

// ============================================================================
// Relationship Operations
// ============================================================================

// createRelationship creates a relationship between two nodes
func (g *memGraph) createRelationship(relType string, fromNodeID, toNodeID string) (*Relationship, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.createRelationshipUnlocked(relType, fromNodeID, toNodeID)
}

// createRelationshipUnlocked creates a relationship without acquiring a lock
func (g *memGraph) createRelationshipUnlocked(relType string, fromNodeID, toNodeID string) (*Relationship, error) {
	if _, exists := g.nodes[fromNodeID]; !exists {
		return nil, fmt.Errorf("from node with ID %s not found", fromNodeID)
	}
	if _, exists := g.nodes[toNodeID]; !exists {
		return nil, fmt.Errorf("to node with ID %s not found", toNodeID)
	}

	rel := NewRelationship(relType, fromNodeID, toNodeID)

	g.relationships[rel.ID] = rel
	g.relationshipVersions[rel.ID] = append(g.relationshipVersions[rel.ID], rel)

	return rel, nil
}

// getRelationship retrieves a relationship by ID if it is currently valid
func (g *memGraph) getRelationship(id string) (*Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	rel, exists := g.relationships[id]
	if !exists {
		return nil, fmt.Errorf("relationship with ID %s not found", id)
	}

	if !rel.IsCurrentlyValid() {
		return nil, fmt.Errorf("relationship with ID %s not found", id)
	}

	return rel, nil
}

// getRelationshipsForNode retrieves all currently valid relationships connected to a node
func (g *memGraph) getRelationshipsForNode(nodeID string) []*Relationship {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getRelationshipsForNodeUnlocked(nodeID)
}

// getAllRelationshipVersions returns all versions of all relationships
func (g *memGraph) getAllRelationshipVersions() []*Relationship {
	g.mu.RLock()
	defer g.mu.RUnlock()

	rels := []*Relationship{}
	for _, versions := range g.relationshipVersions {
		rels = append(rels, versions...)
	}
	return rels
}

// getRelationshipsForNodeUnlocked gets relationships without acquiring a lock
func (g *memGraph) getRelationshipsForNodeUnlocked(nodeID string) []*Relationship {
	rels := []*Relationship{}
	for _, rel := range g.relationships {
		if (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) && rel.IsCurrentlyValid() {
			rels = append(rels, rel)
		}
	}
	return rels
}

// deleteRelationship marks a relationship as deleted
func (g *memGraph) deleteRelationship(relID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteRelationshipUnlocked(relID)
}

// deleteRelationshipUnlocked deletes a relationship without acquiring a lock
func (g *memGraph) deleteRelationshipUnlocked(relID string) error {
	rel, exists := g.relationships[relID]
	if !exists {
		return fmt.Errorf("relationship with ID %s not found", relID)
	}

	if !rel.IsCurrentlyValid() {
		return fmt.Errorf("relationship with ID %s is already deleted", relID)
	}

	now := time.Now()
	rel.ValidTo = &now

	return nil
}

// setRelationshipProperty sets a property on a relationship
func (g *memGraph) setRelationshipProperty(relID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.setRelationshipPropertyUnlocked(relID, key, value)
}

// setRelationshipPropertyUnlocked sets a relationship property without acquiring a lock
func (g *memGraph) setRelationshipPropertyUnlocked(relID, key string, value interface{}) error {
	currentRel, exists := g.relationships[relID]
	if !exists {
		return fmt.Errorf("relationship with ID %s not found", relID)
	}

	if currentVal, exists := currentRel.Properties[key]; exists && currentVal == value {
		return nil
	}

	now := time.Now()
	currentRel.ValidTo = &now

	newRel := &Relationship{
		ID:         currentRel.ID,
		Type:       currentRel.Type,
		FromNodeID: currentRel.FromNodeID,
		ToNodeID:   currentRel.ToNodeID,
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil,
	}

	for k, v := range currentRel.Properties {
		newRel.Properties[k] = v
	}
	newRel.Properties[key] = value

	g.relationships[relID] = newRel
	g.relationshipVersions[relID] = append(g.relationshipVersions[relID], newRel)

	return nil
}

// deleteRelationshipProperty removes a property from a relationship
func (g *memGraph) deleteRelationshipProperty(relID, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteRelationshipPropertyUnlocked(relID, key)
}

// deleteRelationshipPropertyUnlocked removes a property without acquiring a lock
func (g *memGraph) deleteRelationshipPropertyUnlocked(relID, key string) error {
	currentRel, exists := g.relationships[relID]
	if !exists {
		return fmt.Errorf("relationship with ID %s not found", relID)
	}

	if _, exists := currentRel.Properties[key]; !exists {
		return nil
	}

	now := time.Now()
	currentRel.ValidTo = &now

	newRel := &Relationship{
		ID:         currentRel.ID,
		Type:       currentRel.Type,
		FromNodeID: currentRel.FromNodeID,
		ToNodeID:   currentRel.ToNodeID,
		Properties: make(map[string]interface{}),
		ValidFrom:  now,
		ValidTo:    nil,
	}

	for k, v := range currentRel.Properties {
		if k != key {
			newRel.Properties[k] = v
		}
	}

	g.relationships[relID] = newRel
	g.relationshipVersions[relID] = append(g.relationshipVersions[relID], newRel)

	return nil
}

// ============================================================================
// Embedding Operations
// ============================================================================

// setNodeEmbedding stores a vector embedding for a node
func (g *memGraph) setNodeEmbedding(nodeID string, vector []float32, model string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	node, exists := g.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	propertySnapshot := make(map[string]interface{})
	for k, v := range node.Properties {
		propertySnapshot[k] = v
	}

	g.embeddings.Add(nodeID, vector, model, propertySnapshot)

	return nil
}

// getNodeEmbedding returns the current embedding for a node
func (g *memGraph) getNodeEmbedding(nodeID string) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetCurrent(nodeID)
}

// getNodeEmbeddingAt returns the embedding that was valid at a specific time
func (g *memGraph) getNodeEmbeddingAt(nodeID string, t time.Time) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.embeddings.GetAt(nodeID, t)
}

// semanticSearch finds the k most similar nodes to a query vector
func (g *memGraph) semanticSearch(query []float32, k int) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsCurrentlyValid() {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, time.Now(), validNodeIDs)
}

// semanticSearchAt finds the k most similar nodes to a query vector at a specific time
func (g *memGraph) semanticSearchAt(query []float32, k int, asOf time.Time) []SearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
		if node.IsValidAt(asOf) {
			validNodeIDs[id] = true
		}
	}

	return g.embeddings.Search(query, k, asOf, validNodeIDs)
}

// semanticSearchAllVersions finds all historical versions of nodes similar to a query vector
func (g *memGraph) semanticSearchAllVersions(query []float32, k int, threshold float32, labelFilter []string, calculateDrift bool) []VersionedSearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	validNodeIDs := make(map[string]bool)
	for id, node := range g.nodes {
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

// ============================================================================
// Temporal View
// ============================================================================

// TemporalView represents a view of the graph at a specific point in time
type TemporalView struct {
	graph *memGraph
	asOf  time.Time
}

// asOf creates a temporal view of the graph at a specific point in time
func (g *memGraph) asOf(t time.Time) *TemporalView {
	return &TemporalView{
		graph: g,
		asOf:  t,
	}
}

// GetNode retrieves a node by ID if it was valid at the temporal view's time
func (tv *TemporalView) GetNode(id string) (*Node, error) {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	return tv.getNodeAtTime(id)
}

// GetNodesByLabel retrieves all nodes with a specific label that were valid at the temporal view's time
func (tv *TemporalView) GetNodesByLabel(label string) []*Node {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	nodes := []*Node{}
	if nodeMap, exists := tv.graph.nodesByLabel[label]; exists {
		for nodeID := range nodeMap {
			if node, _ := tv.getNodeAtTime(nodeID); node != nil {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// getNodeAtTime is a helper that finds the version of a node valid at tv.asOf
func (tv *TemporalView) getNodeAtTime(id string) (*Node, error) {
	versions, exists := tv.graph.nodeVersions[id]
	if !exists || len(versions) == 0 {
		return nil, nil
	}

	idx := sort.Search(len(versions), func(i int) bool {
		return versions[i].ValidFrom.After(tv.asOf)
	})

	if idx == 0 {
		return nil, nil
	}

	candidate := versions[idx-1]
	if candidate.IsValidAt(tv.asOf) {
		return candidate, nil
	}

	return nil, nil
}

// GetRelationshipsForNode retrieves all relationships connected to a node that were valid at the temporal view's time
func (tv *TemporalView) GetRelationshipsForNode(nodeID string) []*Relationship {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	rels := []*Relationship{}
	for relID := range tv.graph.relationshipVersions {
		if rel := tv.getRelationshipAtTime(relID); rel != nil {
			if rel.FromNodeID == nodeID || rel.ToNodeID == nodeID {
				rels = append(rels, rel)
			}
		}
	}
	return rels
}

// getRelationshipAtTime is a helper that finds the version of a relationship valid at tv.asOf
func (tv *TemporalView) getRelationshipAtTime(id string) *Relationship {
	versions, exists := tv.graph.relationshipVersions[id]
	if !exists || len(versions) == 0 {
		return nil
	}

	idx := sort.Search(len(versions), func(i int) bool {
		return versions[i].ValidFrom.After(tv.asOf)
	})

	if idx == 0 {
		return nil
	}

	candidate := versions[idx-1]
	if candidate.IsValidAt(tv.asOf) {
		return candidate
	}

	return nil
}

// GetRelationship retrieves a relationship by ID if it was valid at the temporal view's time
func (tv *TemporalView) GetRelationship(id string) (*Relationship, error) {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	rel := tv.getRelationshipAtTime(id)
	return rel, nil
}

// GetAllNodes returns all nodes that were valid at the temporal view's time
func (tv *TemporalView) GetAllNodes() []*Node {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	nodes := []*Node{}
	for nodeID := range tv.graph.nodeVersions {
		if node, _ := tv.getNodeAtTime(nodeID); node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetAllRelationships returns all relationships that were valid at the temporal view's time
func (tv *TemporalView) GetAllRelationships() []*Relationship {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	rels := []*Relationship{}
	for relID := range tv.graph.relationshipVersions {
		if rel := tv.getRelationshipAtTime(relID); rel != nil {
			rels = append(rels, rel)
		}
	}
	return rels
}
