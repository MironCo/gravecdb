package graph

import (
	"sort"
	"time"
)

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
