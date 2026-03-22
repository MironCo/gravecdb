package graph

import (
	"fmt"
	"time"

	"github.com/MironCo/gravecdb/storage"
	"github.com/google/uuid"
)

// DiskGraphTransaction wraps a BoltStore transaction with graph operations
// All operations within the transaction are atomic
type DiskGraphTransaction struct {
	g         *DiskGraph
	tx        *storage.Tx
	committed bool

	// Track changes for index updates on commit
	createdNodes []*Node
	createdRels  []*Relationship
	deletedNodes []string // node IDs
	deletedRels  []string // relationship IDs
}

// BeginTransaction starts a new ACID transaction
func (g *DiskGraph) BeginTransaction() (GraphTransaction, error) {
	// Acquire write lock for the duration of the transaction
	// This provides serializable isolation
	g.mu.Lock()

	tx, err := g.boltStore.BeginTx(true)
	if err != nil {
		g.mu.Unlock()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &DiskGraphTransaction{
		g:            g,
		tx:           tx,
		createdNodes: make([]*Node, 0),
		createdRels:  make([]*Relationship, 0),
		deletedNodes: make([]string, 0),
		deletedRels:  make([]string, 0),
	}, nil
}

// CreateNode creates a node within the transaction
func (t *DiskGraphTransaction) CreateNode(labels ...string) (*Node, error) {
	return t.CreateNodeAtTime(time.Now(), labels...)
}

// CreateNodeAtTime creates a node with a custom ValidFrom timestamp within the transaction
func (t *DiskGraphTransaction) CreateNodeAtTime(validFrom time.Time, labels ...string) (*Node, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}

	node := &Node{
		ID:         uuid.New().String(),
		Labels:     labels,
		Properties: make(map[string]interface{}),
		ValidFrom:  validFrom,
	}

	if err := t.tx.SaveNode(node); err != nil {
		return nil, err
	}

	t.createdNodes = append(t.createdNodes, node)
	return node, nil
}

// GetNode retrieves a node within the transaction
func (t *DiskGraphTransaction) GetNode(id string) (*Node, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}
	return t.tx.GetNode(id)
}

// SetNodeProperty sets a property on a node within the transaction
func (t *DiskGraphTransaction) SetNodeProperty(nodeID, key string, value interface{}) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	node, err := t.tx.GetNode(nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[key] = value

	return t.tx.SaveNode(node)
}

// DeleteNode marks a node as deleted within the transaction
func (t *DiskGraphTransaction) DeleteNode(nodeID string) error {
	return t.DeleteNodeAtTime(nodeID, time.Now())
}

// DeleteNodeAtTime soft-deletes a node with a custom ValidTo timestamp within the transaction
func (t *DiskGraphTransaction) DeleteNodeAtTime(nodeID string, validTo time.Time) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	// First delete all relationships involving this node
	rels, err := t.tx.GetAllRelationships()
	if err != nil {
		return err
	}

	for _, rel := range rels {
		if rel.ValidTo == nil && (rel.FromNodeID == nodeID || rel.ToNodeID == nodeID) {
			if err := t.tx.DeleteRelationship(rel.ID, validTo); err != nil {
				return err
			}
			t.deletedRels = append(t.deletedRels, rel.ID)
		}
	}

	// Delete the node
	if err := t.tx.DeleteNode(nodeID, validTo); err != nil {
		return err
	}
	t.deletedNodes = append(t.deletedNodes, nodeID)

	return nil
}

// CreateRelationship creates a relationship within the transaction
func (t *DiskGraphTransaction) CreateRelationship(relType, fromID, toID string) (*Relationship, error) {
	return t.CreateRelationshipAtTime(time.Now(), relType, fromID, toID)
}

// CreateRelationshipAtTime creates a relationship with a custom ValidFrom timestamp within the transaction
func (t *DiskGraphTransaction) CreateRelationshipAtTime(validFrom time.Time, relType, fromID, toID string) (*Relationship, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}

	// Verify nodes exist
	from, err := t.tx.GetNode(fromID)
	if err != nil || from == nil {
		return nil, fmt.Errorf("from node not found: %s", fromID)
	}

	to, err := t.tx.GetNode(toID)
	if err != nil || to == nil {
		return nil, fmt.Errorf("to node not found: %s", toID)
	}

	rel := &Relationship{
		ID:         uuid.New().String(),
		Type:       relType,
		FromNodeID: fromID,
		ToNodeID:   toID,
		Properties: make(map[string]interface{}),
		ValidFrom:  validFrom,
	}

	if err := t.tx.SaveRelationship(rel); err != nil {
		return nil, err
	}

	t.createdRels = append(t.createdRels, rel)
	return rel, nil
}

// GetRelationship retrieves a relationship within the transaction
func (t *DiskGraphTransaction) GetRelationship(id string) (*Relationship, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}
	return t.tx.GetRelationship(id)
}

// SetRelationshipProperty sets a property on a relationship within the transaction
func (t *DiskGraphTransaction) SetRelationshipProperty(relID, key string, value interface{}) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	rel, err := t.tx.GetRelationship(relID)
	if err != nil {
		return err
	}
	if rel == nil {
		return fmt.Errorf("relationship not found: %s", relID)
	}

	if rel.Properties == nil {
		rel.Properties = make(map[string]interface{})
	}
	rel.Properties[key] = value

	return t.tx.SaveRelationship(rel)
}

// DeleteRelationship marks a relationship as deleted within the transaction
func (t *DiskGraphTransaction) DeleteRelationship(relID string) error {
	return t.DeleteRelationshipAtTime(relID, time.Now())
}

// DeleteRelationshipAtTime marks a relationship as deleted with a custom ValidTo timestamp within the transaction
func (t *DiskGraphTransaction) DeleteRelationshipAtTime(relID string, validTo time.Time) error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}

	if err := t.tx.DeleteRelationship(relID, validTo); err != nil {
		return err
	}
	t.deletedRels = append(t.deletedRels, relID)

	return nil
}

// ExecuteQuery executes a full query within the transaction
// This is more complex - for now, we'll support CREATE queries
func (t *DiskGraphTransaction) ExecuteQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	if t.committed {
		return nil, fmt.Errorf("transaction already completed")
	}

	switch query.QueryType {
	case "CREATE":
		return t.executeCreateQuery(query)
	case "MATCH":
		if query.CreateClause != nil {
			return t.executeMatchCreateQuery(query)
		}
		if query.SetClause != nil {
			return t.executeSetQuery(query)
		}
		if query.DeleteClause != nil {
			return t.executeDeleteQuery(query)
		}
		// Read-only MATCH - delegate to main graph (snapshot isolation)
		return t.executeReadQuery(query, embedder)
	default:
		return nil, fmt.Errorf("unsupported query type in transaction: %s", query.QueryType)
	}
}

// executeCreateQuery handles CREATE within a transaction
func (t *DiskGraphTransaction) executeCreateQuery(query *Query) (*QueryResult, error) {
	// Determine creation timestamp (AT TIME or now)
	createTime := time.Now()
	if query.TimeClause != nil && query.TimeClause.Timestamp > 0 {
		createTime = time.Unix(query.TimeClause.Timestamp, 0)
	}

	cc := query.CreateClause
	createdVars := make(map[string]interface{})
	createdCount := 0

	// Create nodes
	for _, nodeSpec := range cc.Nodes {
		node, err := t.CreateNodeAtTime(createTime, nodeSpec.Labels...)
		if err != nil {
			return nil, err
		}

		for key, value := range nodeSpec.Properties {
			if err := t.SetNodeProperty(node.ID, key, value); err != nil {
				return nil, err
			}
		}

		if nodeSpec.Variable != "" {
			createdVars[nodeSpec.Variable] = node
		}
		createdCount++
	}

	// Create relationships
	for _, relSpec := range cc.Relationships {
		fromNode, ok := createdVars[relSpec.FromVar].(*Node)
		if !ok {
			continue
		}
		toNode, ok := createdVars[relSpec.ToVar].(*Node)
		if !ok {
			continue
		}

		rel, err := t.CreateRelationshipAtTime(createTime, relSpec.Type, fromNode.ID, toNode.ID)
		if err != nil {
			return nil, err
		}

		for key, value := range relSpec.Properties {
			if err := t.SetRelationshipProperty(rel.ID, key, value); err != nil {
				return nil, err
			}
		}

		if relSpec.Variable != "" {
			createdVars[relSpec.Variable] = rel
		}
		createdCount++
	}

	return &QueryResult{
		Columns: []string{"created"},
		Rows:    []map[string]interface{}{{"created": createdCount}},
	}, nil
}

// executeMatchCreateQuery handles MATCH...CREATE within a transaction
func (t *DiskGraphTransaction) executeMatchCreateQuery(query *Query) (*QueryResult, error) {
	// Determine creation timestamp (AT TIME or now)
	createTime := time.Now()
	if query.TimeClause != nil && query.TimeClause.Timestamp > 0 {
		createTime = time.Unix(query.TimeClause.Timestamp, 0)
	}

	// Find matches using the underlying graph's read methods
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	createdCount := 0
	for _, match := range matches {
		for _, nodeSpec := range query.CreateClause.Nodes {
			node, err := t.CreateNodeAtTime(createTime, nodeSpec.Labels...)
			if err != nil {
				return nil, err
			}

			for key, value := range nodeSpec.Properties {
				if err := t.SetNodeProperty(node.ID, key, value); err != nil {
					return nil, err
				}
			}

			match[nodeSpec.Variable] = node
			createdCount++
		}

		for _, relSpec := range query.CreateClause.Relationships {
			fromID := t.resolveNodeID(match, relSpec.FromVar)
			toID := t.resolveNodeID(match, relSpec.ToVar)

			if fromID == "" || toID == "" {
				continue
			}

			rel, err := t.CreateRelationshipAtTime(createTime, relSpec.Type, fromID, toID)
			if err != nil {
				return nil, err
			}

			for key, value := range relSpec.Properties {
				if err := t.SetRelationshipProperty(rel.ID, key, value); err != nil {
					return nil, err
				}
			}
			createdCount++
		}
	}

	return &QueryResult{
		Columns: []string{"created"},
		Rows:    []map[string]interface{}{{"created": createdCount}},
	}, nil
}

// executeSetQuery handles MATCH...SET within a transaction
func (t *DiskGraphTransaction) executeSetQuery(query *Query) (*QueryResult, error) {
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	updatedCount := 0
	for _, match := range matches {
		for _, update := range query.SetClause.Updates {
			nodeID := t.resolveNodeID(match, update.Variable)
			if nodeID == "" {
				continue
			}

			if err := t.SetNodeProperty(nodeID, update.Property, update.Value); err != nil {
				return nil, err
			}
			updatedCount++
		}
	}

	return &QueryResult{
		Columns: []string{"updated"},
		Rows:    []map[string]interface{}{{"updated": updatedCount}},
	}, nil
}

// executeDeleteQuery handles MATCH...DELETE within a transaction
func (t *DiskGraphTransaction) executeDeleteQuery(query *Query) (*QueryResult, error) {
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	deleteTime := time.Now()
	if query.TimeClause != nil && query.TimeClause.Timestamp > 0 {
		deleteTime = time.Unix(query.TimeClause.Timestamp, 0)
	}

	deletedCount := 0
	deletedIDs := make(map[string]bool)

	for _, match := range matches {
		for _, varName := range query.DeleteClause.Variables {
			switch v := match[varName].(type) {
			case *Node:
				if !deletedIDs[v.ID] {
					if err := t.DeleteNodeAtTime(v.ID, deleteTime); err != nil {
						return nil, err
					}
					deletedIDs[v.ID] = true
					deletedCount++
				}
			case *Relationship:
				if !deletedIDs[v.ID] {
					if err := t.DeleteRelationshipAtTime(v.ID, deleteTime); err != nil {
						return nil, err
					}
					deletedIDs[v.ID] = true
					deletedCount++
				}
			}
		}
	}

	return &QueryResult{
		Columns: []string{"deleted"},
		Rows:    []map[string]interface{}{{"deleted": deletedCount}},
	}, nil
}

// executeReadQuery handles read-only MATCH queries using a snapshot
func (t *DiskGraphTransaction) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	// For read queries within a transaction, use the current transaction's view
	matches := t.findMatches(query.MatchPattern, query.WhereClause)

	if len(matches) == 0 {
		return &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}, nil
	}

	// Build result based on RETURN clause
	return t.buildQueryResult(matches, query.ReturnClause)
}

// findMatches finds pattern matches within the transaction
func (t *DiskGraphTransaction) findMatches(pattern *MatchPattern, where *WhereClause) []map[string]interface{} {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	// Get all nodes from the transaction
	allNodes, err := t.tx.GetAllNodes()
	if err != nil {
		return nil
	}

	// Filter by first node pattern
	firstPattern := pattern.Nodes[0]
	var candidates []*Node

	for _, node := range allNodes {
		if node.ValidTo != nil {
			continue // Skip deleted nodes
		}
		if len(firstPattern.Labels) > 0 {
			hasLabel := false
			for _, label := range node.Labels {
				if label == firstPattern.Labels[0] {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}
		// Check properties
		if t.nodeMatchesProperties(node, firstPattern.Properties) {
			candidates = append(candidates, node)
		}
	}

	// Build matches
	var matches []map[string]interface{}
	for _, node := range candidates {
		match := map[string]interface{}{
			firstPattern.Variable: node,
		}
		matches = append(matches, match)
	}

	// Handle relationships if present
	if len(pattern.Relationships) > 0 {
		matches = t.expandRelationshipMatches(matches, pattern, allNodes)
	}

	// Apply WHERE clause
	if where != nil {
		matches = t.filterByWhere(matches, where)
	}

	return matches
}

// Helper methods for transaction query execution
func (t *DiskGraphTransaction) nodeMatchesProperties(node *Node, props map[string]interface{}) bool {
	for key, expected := range props {
		actual, ok := node.Properties[key]
		if !ok {
			return false
		}
		if !valuesEqual(actual, expected) {
			return false
		}
	}
	return true
}

func (t *DiskGraphTransaction) expandRelationshipMatches(matches []map[string]interface{}, pattern *MatchPattern, allNodes []*Node) []map[string]interface{} {
	allRels, err := t.tx.GetAllRelationships()
	if err != nil {
		return matches
	}

	var expanded []map[string]interface{}

	for _, match := range matches {
		for _, relPattern := range pattern.Relationships {
			// Get from/to node patterns using indices
			if relPattern.FromIndex >= len(pattern.Nodes) || relPattern.ToIndex >= len(pattern.Nodes) {
				continue
			}
			fromNodePattern := pattern.Nodes[relPattern.FromIndex]
			toNodePattern := pattern.Nodes[relPattern.ToIndex]

			fromNode, _ := match[fromNodePattern.Variable].(*Node)
			if fromNode == nil {
				continue
			}

			for _, rel := range allRels {
				if rel.ValidTo != nil {
					continue
				}
				// Check relationship type
				if len(relPattern.Types) > 0 {
					typeMatch := false
					for _, t := range relPattern.Types {
						if rel.Type == t {
							typeMatch = true
							break
						}
					}
					if !typeMatch {
						continue
					}
				}

				var targetNode *Node
				if rel.FromNodeID == fromNode.ID {
					// Find the target node
					for _, n := range allNodes {
						if n.ID == rel.ToNodeID && n.ValidTo == nil {
							targetNode = n
							break
						}
					}
				}

				if targetNode == nil {
					continue
				}

				// Check if target matches the pattern
				if len(toNodePattern.Labels) > 0 {
					hasLabel := false
					for _, l := range targetNode.Labels {
						if l == toNodePattern.Labels[0] {
							hasLabel = true
							break
						}
					}
					if !hasLabel {
						continue
					}
				}

				newMatch := make(map[string]interface{})
				for k, v := range match {
					newMatch[k] = v
				}
				if relPattern.Variable != "" {
					newMatch[relPattern.Variable] = rel
				}
				newMatch[toNodePattern.Variable] = targetNode
				expanded = append(expanded, newMatch)
			}
		}
	}

	if len(expanded) > 0 {
		return expanded
	}
	return matches
}

func (t *DiskGraphTransaction) filterByWhere(matches []map[string]interface{}, where *WhereClause) []map[string]interface{} {
	var filtered []map[string]interface{}
	for _, match := range matches {
		if t.evaluateWhereConditions(match, where.Conditions) {
			filtered = append(filtered, match)
		}
	}
	return filtered
}

// evaluateWhereConditions evaluates all conditions in a WHERE clause
func (t *DiskGraphTransaction) evaluateWhereConditions(match map[string]interface{}, conditions []Condition) bool {
	for _, cond := range conditions {
		entity, ok := match[cond.Variable]
		if !ok {
			return false
		}

		var propVal interface{}
		switch e := entity.(type) {
		case *Node:
			propVal = e.Properties[cond.Property]
		case *Relationship:
			propVal = e.Properties[cond.Property]
		default:
			return false
		}

		if !evaluateCondition(propVal, cond.Operator, cond.Value) {
			return false
		}
	}
	return true
}

func (t *DiskGraphTransaction) resolveNodeID(match map[string]interface{}, varName string) string {
	if v, ok := match[varName]; ok {
		if node, ok := v.(*Node); ok {
			return node.ID
		}
	}
	return ""
}

func (t *DiskGraphTransaction) buildQueryResult(matches []map[string]interface{}, returnClause *ReturnClause) (*QueryResult, error) {
	if returnClause == nil || len(returnClause.Items) == 0 {
		return &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}, nil
	}

	columns := make([]string, len(returnClause.Items))
	for i, item := range returnClause.Items {
		if item.Alias != "" {
			columns[i] = item.Alias
		} else if item.Property != "" {
			columns[i] = item.Variable + "." + item.Property
		} else {
			columns[i] = item.Variable
		}
	}

	rows := make([]map[string]interface{}, 0, len(matches))
	for _, match := range matches {
		row := make(map[string]interface{})
		for i, item := range returnClause.Items {
			col := columns[i]
			if v, ok := match[item.Variable]; ok {
				if item.Property != "" {
					if node, ok := v.(*Node); ok {
						row[col] = node.Properties[item.Property]
					} else if rel, ok := v.(*Relationship); ok {
						row[col] = rel.Properties[item.Property]
					}
				} else {
					row[col] = v
				}
			}
		}
		rows = append(rows, row)
	}

	// Apply ORDER BY and LIMIT if present (from ReturnClause)
	if len(returnClause.OrderBy) > 0 {
		sortRowsByOrderItems(rows, returnClause.OrderBy)
	}
	if returnClause.Limit > 0 && len(rows) > returnClause.Limit {
		rows = rows[:returnClause.Limit]
	}

	return &QueryResult{Columns: columns, Rows: rows}, nil
}

// sortRowsByOrderItems sorts rows by order items
func sortRowsByOrderItems(rows []map[string]interface{}, orderBy []OrderItem) {
	if len(orderBy) == 0 || len(rows) == 0 {
		return
	}

	// Simple sort by first order item
	item := orderBy[0]
	key := item.Variable
	if item.Property != "" {
		key = item.Variable + "." + item.Property
	}

	// Use sort.Slice for stable sorting
	for i := 0; i < len(rows)-1; i++ {
		for j := i + 1; j < len(rows); j++ {
			shouldSwap := false
			vi := rows[i][key]
			vj := rows[j][key]

			// Compare based on type
			switch a := vi.(type) {
			case int:
				if b, ok := vj.(int); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			case int64:
				if b, ok := vj.(int64); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			case float64:
				if b, ok := vj.(float64); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			case string:
				if b, ok := vj.(string); ok {
					if item.Descending {
						shouldSwap = a < b
					} else {
						shouldSwap = a > b
					}
				}
			}

			if shouldSwap {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}

// Commit commits the transaction and updates in-memory indexes
func (t *DiskGraphTransaction) Commit() error {
	if t.committed {
		return fmt.Errorf("transaction already completed")
	}
	t.committed = true

	// Commit the underlying bbolt transaction
	if err := t.tx.Commit(); err != nil {
		t.g.mu.Unlock()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update in-memory indexes for created nodes.
	// Read the final version from disk so the cache has all properties that
	// were set via SetNodeProperty during the transaction.
	for _, node := range t.createdNodes {
		for _, label := range node.Labels {
			t.g.labelIndex[label] = append(t.g.labelIndex[label], node.ID)
		}
		if finalNode, err := t.g.boltStore.GetNode(node.ID); err == nil && finalNode != nil {
			t.g.nodeCache.Add(node.ID, finalNode)
		} else {
			t.g.nodeCache.Add(node.ID, node)
		}
	}

	// Update in-memory indexes for created relationships.
	for _, rel := range t.createdRels {
		t.g.nodeRelIndex[rel.FromNodeID] = append(t.g.nodeRelIndex[rel.FromNodeID], rel.ID)
		t.g.nodeRelIndex[rel.ToNodeID] = append(t.g.nodeRelIndex[rel.ToNodeID], rel.ID)
		if finalRel, err := t.g.boltStore.GetRelationship(rel.ID); err == nil && finalRel != nil {
			t.g.relCache.Add(rel.ID, finalRel)
		} else {
			t.g.relCache.Add(rel.ID, rel)
		}
	}

	// Update indexes for deleted nodes
	for _, nodeID := range t.deletedNodes {
		// Get the node to find its labels
		node, _ := t.g.boltStore.GetNode(nodeID)
		if node != nil {
			for _, label := range node.Labels {
				ids := t.g.labelIndex[label]
				for i, id := range ids {
					if id == nodeID {
						t.g.labelIndex[label] = append(ids[:i], ids[i+1:]...)
						break
					}
				}
			}
		}
		delete(t.g.nodeRelIndex, nodeID)
		t.g.nodeCache.Remove(nodeID)
	}

	// Update indexes for deleted relationships
	for _, relID := range t.deletedRels {
		rel, _ := t.g.boltStore.GetRelationship(relID)
		if rel != nil {
			t.g.removeFromRelIndex(rel.FromNodeID, relID)
			t.g.removeFromRelIndex(rel.ToNodeID, relID)
		}
		t.g.relCache.Remove(relID)
	}

	// Release the write lock
	t.g.mu.Unlock()

	return nil
}

// Rollback aborts the transaction, discarding all changes
func (t *DiskGraphTransaction) Rollback() error {
	if t.committed {
		return nil // Already completed
	}
	t.committed = true

	// Rollback the underlying bbolt transaction
	err := t.tx.Rollback()

	// Release the write lock
	t.g.mu.Unlock()

	return err
}
