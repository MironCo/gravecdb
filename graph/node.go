package graph

import (
	"time"

	"github.com/google/uuid"
)

// Node represents a vertex in the graph with labels and properties
// Supports temporal queries through ValidFrom/ValidTo timestamps
type Node struct {
	ID         string                 `json:"id"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
	ValidFrom  time.Time              `json:"validFrom"`  // When this node became valid/active
	ValidTo    *time.Time             `json:"validTo"`    // When this node became invalid/deleted (nil = still valid)
}

// NewNode creates a new node with the given labels
// The node is marked as valid starting from the current time
func NewNode(labels ...string) *Node {
	return &Node{
		ID:         uuid.New().String(),
		Labels:     labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  time.Now(),
		ValidTo:    nil, // nil indicates the node is currently valid
	}
}

// SetProperty sets a property on the node
func (n *Node) SetProperty(key string, value interface{}) {
	n.Properties[key] = value
}

// GetProperty retrieves a property from the node
func (n *Node) GetProperty(key string) (interface{}, bool) {
	val, exists := n.Properties[key]
	return val, exists
}

// HasLabel checks if the node has a specific label
func (n *Node) HasLabel(label string) bool {
	for _, l := range n.Labels {
		if l == label {
			return true
		}
	}
	return false
}

// IsValidAt checks if the node was valid at a specific point in time
// A node is valid at time t if: ValidFrom <= t AND (ValidTo is nil OR ValidTo > t)
func (n *Node) IsValidAt(t time.Time) bool {
	// Check if the node had been created by time t
	if t.Before(n.ValidFrom) {
		return false
	}

	// Check if the node was still valid at time t
	// ValidTo == nil means the node is still valid (never deleted)
	// The validity range is [ValidFrom, ValidTo) - inclusive start, exclusive end
	if n.ValidTo != nil && (t.After(*n.ValidTo) || t.Equal(*n.ValidTo)) {
		return false
	}

	return true
}

// IsCurrentlyValid checks if the node is currently valid (not deleted)
func (n *Node) IsCurrentlyValid() bool {
	return n.ValidTo == nil
}
