package graph

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GetNode retrieves a node (from cache if present, otherwise from disk)
// Returns error if node doesn't exist or has been deleted
func (g *DiskGraph) GetNode(id string) (*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check cache first
	if node, ok := g.nodeCache.Get(id); ok {
		if node.ValidTo != nil {
			return nil, fmt.Errorf("node not found: %s", id)
		}
		return node, nil
	}

	// Cache miss - load from disk
	node, err := g.boltStore.GetNode(id)
	if err != nil {
		return nil, err
	}

	if node == nil || node.ValidTo != nil {
		return nil, fmt.Errorf("node not found: %s", id)
	}

	g.nodeCache.Add(id, node)
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
		node, err := g.boltStore.GetNode(id)
		if err != nil {
			continue
		}
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

// CreateNode creates a new node and persists to disk
func (g *DiskGraph) CreateNode(labels ...string) (*Node, error) {
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
		return nil, fmt.Errorf("failed to save node: %w", err)
	}

	// Update label index
	for _, label := range labels {
		g.labelIndex[label] = append(g.labelIndex[label], node.ID)
	}

	// Add to cache
	g.nodeCache.Add(node.ID, node)

	return node, nil
}

// createNodeUnlocked creates a node (caller must hold write lock)
func (g *DiskGraph) createNodeUnlocked(labels ...string) (*Node, error) {
	return g.createNodeAtTimeUnlocked(time.Now(), labels...)
}

// createNodeAtTimeUnlocked creates a node with a custom ValidFrom timestamp (caller must hold write lock)
func (g *DiskGraph) createNodeAtTimeUnlocked(validFrom time.Time, labels ...string) (*Node, error) {
	node := &Node{
		ID:         uuid.New().String(),
		Labels:     labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  validFrom,
	}

	if err := g.boltStore.SaveNode(node); err != nil {
		return nil, fmt.Errorf("failed to save node: %w", err)
	}

	// Update in-memory label index
	for _, label := range labels {
		g.labelIndex[label] = append(g.labelIndex[label], node.ID)
	}

	// Add to cache
	g.nodeCache.Add(node.ID, node)

	return node, nil
}

// SetNodeProperty sets a property on a node
// Creates a new version of the node to preserve temporal history
func (g *DiskGraph) SetNodeProperty(nodeID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Read current node from disk
	currentNode, err := g.boltStore.GetNode(nodeID)
	if err != nil {
		return err
	}
	if currentNode == nil {
		return fmt.Errorf("node not found: %s", nodeID)
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

	// Save both versions to disk (old version with ValidTo set, new version as current)
	if err := g.boltStore.SaveNode(currentNode); err != nil {
		return fmt.Errorf("failed to save old version: %w", err)
	}
	if err := g.boltStore.SaveNode(newNode); err != nil {
		return fmt.Errorf("failed to save new version: %w", err)
	}

	// Update cache with new version
	g.nodeCache.Add(nodeID, newNode)

	return nil
}

// setNodePropertyUnlocked sets a node property (caller must hold write lock)
func (g *DiskGraph) setNodePropertyUnlocked(nodeID, key string, value interface{}) error {
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil || node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	node.Properties[key] = value
	if err := g.boltStore.SaveNode(node); err != nil {
		return fmt.Errorf("failed to save node: %w", err)
	}
	g.nodeCache.Add(nodeID, node)

	return nil
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

	// Remove from node cache
	g.nodeCache.Remove(nodeID)

	// Remove from label index
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node for label cleanup: %w", err)
	}
	if node != nil {
		for _, label := range node.Labels {
			ids := g.labelIndex[label]
			for i, id := range ids {
				if id == nodeID {
					g.labelIndex[label] = append(ids[:i], ids[i+1:]...)
					break
				}
			}
		}
	}

	// Soft delete the node
	return g.boltStore.DeleteNode(nodeID, now)
}

// deleteNodeUnlocked deletes a node (caller must hold write lock)
func (g *DiskGraph) deleteNodeUnlocked(nodeID string) error {
	return g.deleteNodeAtTimeUnlocked(nodeID, time.Now())
}

// deleteNodeAtTimeUnlocked deletes a node with a custom ValidTo timestamp (caller must hold write lock)
func (g *DiskGraph) deleteNodeAtTimeUnlocked(nodeID string, validTo time.Time) error {
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}
	if node != nil {
		node.ValidTo = &validTo
		if err := g.boltStore.SaveNode(node); err != nil {
			return fmt.Errorf("failed to save node: %w", err)
		}

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

// DeleteNodeProperty removes a property from a node
// Creates a new version of the node without the property
func (g *DiskGraph) DeleteNodeProperty(nodeID, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	currentNode, err := g.boltStore.GetNode(nodeID)
	if err != nil {
		return err
	}
	if currentNode == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// Check if property exists
	if _, exists := currentNode.Properties[key]; !exists {
		return nil // Nothing to delete
	}

	now := time.Now()
	currentNode.ValidTo = &now

	// Create new version without the property
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

	if err := g.boltStore.SaveNode(currentNode); err != nil {
		return fmt.Errorf("failed to save old version: %w", err)
	}
	if err := g.boltStore.SaveNode(newNode); err != nil {
		return fmt.Errorf("failed to save new version: %w", err)
	}

	g.nodeCache.Add(nodeID, newNode)
	return nil
}

// deleteNodePropertyUnlocked removes a property from a node (caller must hold write lock)
func (g *DiskGraph) deleteNodePropertyUnlocked(nodeID, key string) error {
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil || node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}
	if _, exists := node.Properties[key]; !exists {
		return nil // nothing to do
	}
	delete(node.Properties, key)
	if err := g.boltStore.SaveNode(node); err != nil {
		return fmt.Errorf("failed to save node: %w", err)
	}
	g.nodeCache.Add(nodeID, node)
	return nil
}

// removeNodeLabelUnlocked removes a label from a node (caller must hold write lock)
func (g *DiskGraph) removeNodeLabelUnlocked(nodeID, label string) error {
	node, err := g.boltStore.GetNode(nodeID)
	if err != nil || node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}
	newLabels := make([]string, 0, len(node.Labels))
	found := false
	for _, l := range node.Labels {
		if l == label {
			found = true
		} else {
			newLabels = append(newLabels, l)
		}
	}
	if !found {
		return nil // nothing to do
	}
	node.Labels = newLabels
	if err := g.boltStore.SaveNode(node); err != nil {
		return fmt.Errorf("failed to save node: %w", err)
	}
	g.nodeCache.Add(nodeID, node)

	// Update label index
	ids := g.labelIndex[label]
	for i, id := range ids {
		if id == nodeID {
			g.labelIndex[label] = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	return nil
}
