package graph

import (
	"fmt"
	"time"
)

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
