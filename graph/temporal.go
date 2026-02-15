package graph

import (
	"sort"
	"time"
)

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
// Uses binary search on the version history for O(log n) lookup
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
		// For each unique node ID, find the version valid at tv.asOf
		for nodeID := range nodeMap {
			if node, _ := tv.getNodeAtTime(nodeID); node != nil {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// getNodeAtTime is a helper that finds the version of a node valid at tv.asOf
// Assumes the caller already holds the read lock
func (tv *TemporalView) getNodeAtTime(id string) (*Node, error) {
	versions, exists := tv.graph.nodeVersions[id]
	if !exists || len(versions) == 0 {
		return nil, nil
	}

	// Binary search to find the version valid at tv.asOf
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
	// Iterate through all relationship IDs and get their version at tv.asOf
	for relID := range tv.graph.relationshipVersions {
		if rel := tv.getRelationshipAtTime(relID); rel != nil {
			// Check if the relationship involves this node
			if rel.FromNodeID == nodeID || rel.ToNodeID == nodeID {
				rels = append(rels, rel)
			}
		}
	}
	return rels
}

// getRelationshipAtTime is a helper that finds the version of a relationship valid at tv.asOf
// Assumes the caller already holds the read lock
func (tv *TemporalView) getRelationshipAtTime(id string) *Relationship {
	versions, exists := tv.graph.relationshipVersions[id]
	if !exists || len(versions) == 0 {
		return nil
	}

	// Binary search to find the version valid at tv.asOf
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
	// Iterate through all node IDs and get their version at tv.asOf
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
	// Iterate through all relationship IDs and get their version at tv.asOf
	for relID := range tv.graph.relationshipVersions {
		if rel := tv.getRelationshipAtTime(relID); rel != nil {
			rels = append(rels, rel)
		}
	}
	return rels
}
