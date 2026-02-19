package graph

import (
	"fmt"
	"time"

	"github.com/MironCo/gravecdb/core"
	"github.com/MironCo/gravecdb/cypher"
	"github.com/MironCo/gravecdb/embedding"
)

// ============================================================================
// Core type aliases
// ============================================================================

type Node = core.Node
type Relationship = core.Relationship

var NewNode = core.NewNode
var NewRelationship = core.NewRelationship

type Embedding = embedding.Embedding
type SearchResult = embedding.SearchResult
type VersionedSearchResult = embedding.VersionedSearchResult

// Embedder is the interface for generating embeddings
type Embedder = embedding.Embedder

// ============================================================================
// Cypher type aliases
// ============================================================================

type Query = cypher.GraphQuery
type MatchPattern = cypher.GraphMatchPattern
type NodePattern = cypher.GraphNodePattern
type RelPattern = cypher.GraphRelPattern
type PathFunction = cypher.GraphPathFunction
type TimeClause = cypher.GraphTimeClause
type WhereClause = cypher.GraphWhereClause
type Condition = cypher.GraphCondition
type ReturnClause = cypher.GraphReturnClause
type ReturnItem = cypher.GraphReturnItem
type OrderItem = cypher.GraphOrderItem
type CreateClause = cypher.GraphCreateClause
type CreateNode = cypher.GraphCreateNode
type CreateRelationship = cypher.GraphCreateRelationship
type SetClause = cypher.GraphSetClause
type PropertyUpdate = cypher.GraphPropertyUpdate
type DeleteClause = cypher.GraphDeleteClause
type EmbedClause = cypher.GraphEmbedClause
type SimilarToClause = cypher.GraphSimilarToClause
type MergeClause = cypher.GraphMergeClause
type RemoveClause = cypher.GraphRemoveClause
type RemoveItem = cypher.GraphRemoveItem
type UnwindClause = cypher.GraphUnwindClause

// ParseQuery parses a Cypher query string and returns a Query
func ParseQuery(queryStr string) (*Query, error) {
	return cypher.ParseToGraph(queryStr)
}

// ============================================================================
// Query result types
// ============================================================================

// QueryResult represents the result of executing a query
type QueryResult struct {
	Columns []string
	Rows    []map[string]interface{}
}

// Match represents a single match of the pattern (variable -> node or relationship)
type Match map[string]interface{}

// Path represents a path through the graph
type Path struct {
	Nodes         []*Node
	Relationships []*Relationship
	Length        int
}

// ============================================================================
// TemporalView — a snapshot of the graph at a specific point in time
// ============================================================================

// TemporalView holds a pre-filtered snapshot of nodes and relationships
// valid at a specific point in time. Build one via DiskGraph.AsOf(t).
type TemporalView struct {
	asOf          time.Time
	nodes         map[string]*Node         // nodeID -> node valid at asOf
	nodesByLabel  map[string][]*Node       // label  -> nodes valid at asOf
	relationships map[string]*Relationship // relID  -> rel valid at asOf
	relsByNode    map[string][]*Relationship // nodeID -> rels touching that node
}

// GetNode retrieves a node by ID if it was valid at the snapshot time.
func (tv *TemporalView) GetNode(id string) (*Node, error) {
	n, ok := tv.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node %s not found at %v", id, tv.asOf)
	}
	return n, nil
}

// GetNodesByLabel returns all nodes with the given label valid at the snapshot time.
func (tv *TemporalView) GetNodesByLabel(label string) []*Node {
	return tv.nodesByLabel[label]
}

// GetAllNodes returns all nodes valid at the snapshot time.
func (tv *TemporalView) GetAllNodes() []*Node {
	out := make([]*Node, 0, len(tv.nodes))
	for _, n := range tv.nodes {
		out = append(out, n)
	}
	return out
}

// GetRelationshipsForNode returns all relationships touching nodeID valid at the snapshot time.
func (tv *TemporalView) GetRelationshipsForNode(nodeID string) []*Relationship {
	return tv.relsByNode[nodeID]
}

// GetRelationship retrieves a relationship by ID if it was valid at the snapshot time.
func (tv *TemporalView) GetRelationship(id string) (*Relationship, error) {
	r, ok := tv.relationships[id]
	if !ok {
		return nil, fmt.Errorf("relationship %s not found at %v", id, tv.asOf)
	}
	return r, nil
}

// GetAllRelationships returns all relationships valid at the snapshot time.
func (tv *TemporalView) GetAllRelationships() []*Relationship {
	out := make([]*Relationship, 0, len(tv.relationships))
	for _, r := range tv.relationships {
		out = append(out, r)
	}
	return out
}

// ============================================================================
// Comparison helpers (used by query evaluation)
// ============================================================================

// valuesEqual compares two values for equality, handling type differences.
func valuesEqual(a, b interface{}) bool {
	if a == b {
		return true
	}

	aNum, aIsNum := toFloat64ForCompare(a)
	bNum, bIsNum := toFloat64ForCompare(b)
	if aIsNum && bIsNum {
		return aNum == bNum
	}

	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return aStr == bStr
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// toFloat64ForCompare converts numeric types to float64 for comparison.
func toFloat64ForCompare(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
