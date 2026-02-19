package graph

import (
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
// Cypher / query type aliases
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
type GraphSemanticCondition = cypher.GraphSemanticCondition

// valuesEqual compares two values for equality, handling type differences
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
	return false
}

// toFloat64ForCompare converts numeric types to float64 for comparison
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

// ParseQuery parses a Cypher query string and returns a Query
func ParseQuery(queryStr string) (*Query, error) {
	return cypher.ParseToGraph(queryStr)
}

// QueryResult represents the result of executing a query
type QueryResult struct {
	Columns []string
	Rows    []map[string]interface{}
}

// Match represents a single match of the query pattern.
// Using a type alias (=) keeps it fully compatible with map[string]interface{}.
type Match = map[string]interface{}

// Path represents a path through the graph
type Path struct {
	Nodes         []*Node
	Relationships []*Relationship
	Length        int
}

// ============================================================================
// TemporalView — self-contained snapshot of the graph at a point in time
// ============================================================================

// TemporalView represents a view of the graph at a specific point in time.
// It is a plain data snapshot and does not depend on memGraph.
type TemporalView struct {
	nodes         map[string]*Node
	relationships map[string]*Relationship
	nodesByLabel  map[string][]string // label -> []nodeIDs
	nodeRelIndex  map[string][]string // nodeID -> []relIDs
	asOfTime      time.Time
}

// GetNode retrieves a node by ID if it was valid at the temporal view's time.
func (tv *TemporalView) GetNode(id string) (*Node, error) {
	if n, ok := tv.nodes[id]; ok {
		return n, nil
	}
	return nil, nil
}

// GetNodesByLabel retrieves all nodes with a specific label valid at the temporal view's time.
func (tv *TemporalView) GetNodesByLabel(label string) []*Node {
	ids := tv.nodesByLabel[label]
	result := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := tv.nodes[id]; ok {
			result = append(result, n)
		}
	}
	return result
}

// GetRelationshipsForNode retrieves all relationships connected to a node valid at the temporal view's time.
func (tv *TemporalView) GetRelationshipsForNode(nodeID string) []*Relationship {
	ids := tv.nodeRelIndex[nodeID]
	result := make([]*Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := tv.relationships[id]; ok {
			result = append(result, r)
		}
	}
	return result
}

// GetRelationship retrieves a relationship by ID if it was valid at the temporal view's time.
func (tv *TemporalView) GetRelationship(id string) (*Relationship, error) {
	if r, ok := tv.relationships[id]; ok {
		return r, nil
	}
	return nil, nil
}

// GetAllNodes returns all nodes that were valid at the temporal view's time.
func (tv *TemporalView) GetAllNodes() []*Node {
	result := make([]*Node, 0, len(tv.nodes))
	for _, n := range tv.nodes {
		result = append(result, n)
	}
	return result
}

// GetAllRelationships returns all relationships that were valid at the temporal view's time.
func (tv *TemporalView) GetAllRelationships() []*Relationship {
	result := make([]*Relationship, 0, len(tv.relationships))
	for _, r := range tv.relationships {
		result = append(result, r)
	}
	return result
}
