package graph

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GetRelationship retrieves a relationship (from cache if present, otherwise from disk)
func (g *DiskGraph) GetRelationship(id string) (*Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check cache first
	if rel, ok := g.relCache.Get(id); ok {
		return rel, nil
	}

	// Cache miss - load from disk
	rel, err := g.boltStore.GetRelationship(id)
	if err != nil {
		return nil, err
	}

	if rel != nil {
		g.relCache.Add(id, rel)
	}

	return rel, nil
}

// GetAllRelationships retrieves all relationships from disk
func (g *DiskGraph) GetAllRelationships() ([]*Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.boltStore.GetAllRelationships()
}

// CreateRelationship creates a new relationship
func (g *DiskGraph) CreateRelationship(relType, fromID, toID string) (*Relationship, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Verify nodes exist
	from, err := g.boltStore.GetNode(fromID)
	if err != nil || from == nil {
		return nil, fmt.Errorf("from node not found: %s", fromID)
	}

	to, err := g.boltStore.GetNode(toID)
	if err != nil || to == nil {
		return nil, fmt.Errorf("to node not found: %s", toID)
	}

	rel := &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromID,
		ToNodeID:   toID,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
		ValidTo:    nil,
	}

	if err := g.boltStore.SaveRelationship(rel); err != nil {
		return nil, fmt.Errorf("failed to save relationship: %w", err)
	}

	// Update relationship index
	g.nodeRelIndex[fromID] = append(g.nodeRelIndex[fromID], rel.ID)
	g.nodeRelIndex[toID] = append(g.nodeRelIndex[toID], rel.ID)

	// Add to cache
	g.relCache.Add(rel.ID, rel)

	return rel, nil
}

// SetRelationshipProperty sets a property on a relationship
func (g *DiskGraph) SetRelationshipProperty(relID, key string, value interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Read current relationship from disk
	currentRel, err := g.boltStore.GetRelationship(relID)
	if err != nil {
		return err
	}
	if currentRel == nil {
		return fmt.Errorf("relationship not found: %s", relID)
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

	// Save both versions to disk
	if err := g.boltStore.SaveRelationship(currentRel); err != nil {
		return fmt.Errorf("failed to save old version: %w", err)
	}
	if err := g.boltStore.SaveRelationship(newRel); err != nil {
		return fmt.Errorf("failed to save new version: %w", err)
	}

	// Update cache with new version
	g.relCache.Add(relID, newRel)

	return nil
}

// DeleteRelationship soft-deletes a relationship
func (g *DiskGraph) DeleteRelationship(relID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Get relationship to update index
	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil {
		return err
	}
	if rel != nil && rel.ValidTo == nil {
		// Remove from relationship index
		g.removeFromRelIndex(rel.FromNodeID, relID)
		g.removeFromRelIndex(rel.ToNodeID, relID)
		g.relCache.Remove(relID)
	}

	return g.boltStore.DeleteRelationship(relID, time.Now())
}

// GetRelationshipsForNode returns all relationships involving a node
func (g *DiskGraph) GetRelationshipsForNode(nodeID string) []*Relationship {
	g.mu.RLock()
	defer g.mu.RUnlock()

	relIDs := g.nodeRelIndex[nodeID]
	if len(relIDs) == 0 {
		return nil
	}

	result := make([]*Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		// Check cache first
		if rel, ok := g.relCache.Get(relID); ok {
			result = append(result, rel)
			continue
		}
		// Load from disk
		rel, err := g.boltStore.GetRelationship(relID)
		if err == nil && rel != nil && rel.ValidTo == nil {
			g.relCache.Add(relID, rel)
			result = append(result, rel)
		}
	}

	return result
}

// GetNodeEmbedding retrieves the current embedding for a node
func (g *DiskGraph) GetNodeEmbedding(nodeID string) *Embedding {
	g.mu.RLock()
	defer g.mu.RUnlock()

	embeddings, err := g.boltStore.GetEmbedding(nodeID)
	if err != nil || len(embeddings) == 0 {
		return nil
	}

	// Return most recent valid embedding
	for i := len(embeddings) - 1; i >= 0; i-- {
		if embeddings[i].ValidTo == nil {
			return embeddings[i]
		}
	}

	return nil
}

// createRelationshipUnlocked creates a relationship (caller must hold write lock)
func (g *DiskGraph) createRelationshipUnlocked(relType, fromID, toID string) (*Relationship, error) {
	rel := &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromID,
		ToNodeID:   toID,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
	}

	if err := g.boltStore.SaveRelationship(rel); err != nil {
		return nil, err
	}

	// Update relationship index
	g.nodeRelIndex[fromID] = append(g.nodeRelIndex[fromID], rel.ID)
	g.nodeRelIndex[toID] = append(g.nodeRelIndex[toID], rel.ID)

	g.relCache.Add(rel.ID, rel)

	return rel, nil
}

// setRelPropertyUnlocked sets a relationship property (caller must hold write lock)
func (g *DiskGraph) setRelPropertyUnlocked(relID, key string, value interface{}) error {
	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil || rel == nil {
		return fmt.Errorf("relationship not found: %s", relID)
	}

	rel.Properties[key] = value
	if err := g.boltStore.SaveRelationship(rel); err != nil {
		return fmt.Errorf("failed to save relationship: %w", err)
	}
	g.relCache.Add(relID, rel)

	return nil
}

// getRelationshipsForNodeUnlocked gets relationships for a node (caller must hold lock)
func (g *DiskGraph) getRelationshipsForNodeUnlocked(nodeID string) []*Relationship {
	relIDs := g.nodeRelIndex[nodeID]
	if len(relIDs) == 0 {
		return nil
	}

	result := make([]*Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		// Check cache first
		if rel, ok := g.relCache.Get(relID); ok {
			result = append(result, rel)
			continue
		}
		// Load from disk
		rel, err := g.boltStore.GetRelationship(relID)
		if err == nil && rel != nil && rel.ValidTo == nil {
			g.relCache.Add(relID, rel)
			result = append(result, rel)
		}
	}
	return result
}

// deleteRelationshipUnlocked deletes a relationship (caller must hold write lock)
func (g *DiskGraph) deleteRelationshipUnlocked(relID string) error {
	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil {
		return fmt.Errorf("failed to get relationship: %w", err)
	}
	if rel != nil {
		now := time.Now()
		rel.ValidTo = &now
		if err := g.boltStore.SaveRelationship(rel); err != nil {
			return fmt.Errorf("failed to save relationship: %w", err)
		}

		// Remove from relationship index
		g.removeFromRelIndex(rel.FromNodeID, relID)
		g.removeFromRelIndex(rel.ToNodeID, relID)

		g.relCache.Remove(relID)
	}
	return nil
}
