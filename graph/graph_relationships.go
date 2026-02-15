package graph

import (
	"fmt"
	"time"
)

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
