package graph

import (
	"fmt"
	"time"
)

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
