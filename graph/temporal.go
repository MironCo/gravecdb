package graph

import "time"

// TemporalView represents a view of the graph at a specific point in time
// This allows querying the graph as it existed at any historical moment
type TemporalView struct {
	graph *Graph
	asOf  time.Time
}

// AsOf creates a temporal view of the graph at a specific point in time
// This allows you to query the graph as it existed at that moment
//
// Example:
//
//	// See the graph as it was on January 1, 2023
//	view := graph.AsOf(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
//	nodes := view.GetNodesByLabel("Person")
func (g *Graph) AsOf(t time.Time) *TemporalView {
	return &TemporalView{
		graph: g,
		asOf:  t,
	}
}

// GetNode retrieves a node by ID if it was valid at the temporal view's time
func (tv *TemporalView) GetNode(id string) (*Node, error) {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	node, exists := tv.graph.nodes[id]
	if !exists {
		return nil, nil // Node never existed
	}

	// Check if the node was valid at the query time
	if !node.IsValidAt(tv.asOf) {
		return nil, nil // Node existed but wasn't valid at this time
	}

	return node, nil
}

// GetNodesByLabel retrieves all nodes with a specific label that were valid at the temporal view's time
func (tv *TemporalView) GetNodesByLabel(label string) []*Node {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	nodes := []*Node{}
	if nodeMap, exists := tv.graph.nodesByLabel[label]; exists {
		for _, node := range nodeMap {
			// Only include nodes that were valid at the query time
			if node.IsValidAt(tv.asOf) {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// GetRelationshipsForNode retrieves all relationships connected to a node that were valid at the temporal view's time
func (tv *TemporalView) GetRelationshipsForNode(nodeID string) []*Relationship {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	rels := []*Relationship{}
	for _, rel := range tv.graph.relationships {
		// Check if the relationship involves this node AND was valid at the query time
		if (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) && rel.IsValidAt(tv.asOf) {
			rels = append(rels, rel)
		}
	}
	return rels
}

// GetRelationship retrieves a relationship by ID if it was valid at the temporal view's time
func (tv *TemporalView) GetRelationship(id string) (*Relationship, error) {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	rel, exists := tv.graph.relationships[id]
	if !exists {
		return nil, nil // Relationship never existed
	}

	// Check if the relationship was valid at the query time
	if !rel.IsValidAt(tv.asOf) {
		return nil, nil // Relationship existed but wasn't valid at this time
	}

	return rel, nil
}

// GetAllNodes returns all nodes that were valid at the temporal view's time
func (tv *TemporalView) GetAllNodes() []*Node {
	tv.graph.mu.RLock()
	defer tv.graph.mu.RUnlock()

	nodes := []*Node{}
	for _, node := range tv.graph.nodes {
		if node.IsValidAt(tv.asOf) {
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
	for _, rel := range tv.graph.relationships {
		if rel.IsValidAt(tv.asOf) {
			rels = append(rels, rel)
		}
	}
	return rels
}
