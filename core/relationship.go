package core

import (
	"time"

	"github.com/google/uuid"
)

// Relationship represents an edge in the graph connecting two nodes
// Supports temporal queries through ValidFrom/ValidTo timestamps
type Relationship struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	FromNodeID string                 `json:"fromNodeId"`
	ToNodeID   string                 `json:"toNodeId"`
	Properties map[string]interface{} `json:"properties"`
	ValidFrom  time.Time              `json:"validFrom"`
	ValidTo    *time.Time             `json:"validTo"`
}

// NewRelationship creates a new relationship between two nodes
func NewRelationship(relType string, fromNodeID, toNodeID string) *Relationship {
	return &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromNodeID,
		ToNodeID:   toNodeID,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
		ValidTo:    nil,
	}
}

// SetProperty sets a property on the relationship
func (r *Relationship) SetProperty(key string, value interface{}) {
	r.Properties[key] = value
}

// GetProperty retrieves a property from the relationship
func (r *Relationship) GetProperty(key string) (interface{}, bool) {
	val, exists := r.Properties[key]
	return val, exists
}

// IsValidAt checks if the relationship was valid at a specific point in time
func (r *Relationship) IsValidAt(t time.Time) bool {
	if t.Before(r.ValidFrom) {
		return false
	}
	if r.ValidTo != nil && (t.After(*r.ValidTo) || t.Equal(*r.ValidTo)) {
		return false
	}
	return true
}

// IsCurrentlyValid checks if the relationship is currently valid (not deleted)
func (r *Relationship) IsCurrentlyValid() bool {
	return r.ValidTo == nil
}
