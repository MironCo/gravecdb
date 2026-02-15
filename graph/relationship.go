package graph

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
	ValidFrom  time.Time              `json:"validFrom"`  // When this relationship became valid/active
	ValidTo    *time.Time             `json:"validTo"`    // When this relationship became invalid/deleted (nil = still valid)
}

// NewRelationship creates a new relationship between two nodes
// The relationship is marked as valid starting from the current time
func NewRelationship(relType string, fromNodeID, toNodeID string) *Relationship {
	return &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromNodeID,
		ToNodeID:   toNodeID,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
		ValidTo:    nil, // nil indicates the relationship is currently valid
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
// A relationship is valid at time t if: ValidFrom <= t AND (ValidTo is nil OR ValidTo > t)
// The validity range is [ValidFrom, ValidTo) - inclusive start, exclusive end
func (r *Relationship) IsValidAt(t time.Time) bool {
	// Check if the relationship had been created by time t
	if t.Before(r.ValidFrom) {
		return false
	}

	// Check if the relationship was still valid at time t
	// ValidTo == nil means the relationship is still valid (never deleted)
	// The validity range is [ValidFrom, ValidTo) - inclusive start, exclusive end
	if r.ValidTo != nil && (t.After(*r.ValidTo) || t.Equal(*r.ValidTo)) {
		return false
	}

	return true
}

// IsCurrentlyValid checks if the relationship is currently valid (not deleted)
func (r *Relationship) IsCurrentlyValid() bool {
	return r.ValidTo == nil
}
