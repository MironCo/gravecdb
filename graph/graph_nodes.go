package graph

import (
	"fmt"
	"time"
)

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
