package graph

import (
	"container/list"
	"fmt"
	"strings"
	"time"

	"github.com/MironCo/gravecdb/embedding"
)

// ExecuteQueryWithEmbedder executes a Cypher-like query
func (g *DiskGraph) ExecuteQueryWithEmbedder(query *Query, embedder Embedder) (*QueryResult, error) {
	// For mutating queries (CREATE, SET, DELETE), use DiskGraph's own methods
	// For read queries (MATCH without mutations), use in-memory graph

	switch query.QueryType {
	case "CREATE":
		return g.executeCreateQuery(query)
	case "MERGE":
		return g.executeMergeQuery(query)
	case "UNWIND":
		return g.executeUnwindQuery(query)
	case "MATCH":
		// Check if this is a mutating MATCH query
		if query.CreateClause != nil {
			return g.executeMatchCreateQuery(query)
		}
		if query.SetClause != nil {
			return g.executeSetQuery(query)
		}
		if query.DeleteClause != nil {
			return g.executeDeleteQuery(query)
		}
		if query.RemoveClause != nil {
			return g.executeRemoveQuery(query)
		}
		if query.UnwindClause != nil {
			return g.executeUnwindQuery(query)
		}
		// Handle EMBED queries specially - need to persist embeddings
		if query.EmbedClause != nil {
			return g.executeEmbedQuery(query, embedder)
		}
		// Read-only MATCH query - use in-memory approach
		return g.executeReadQuery(query, embedder)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", query.QueryType)
	}
}

// executeCreateQuery handles CREATE queries directly on DiskGraph
func (g *DiskGraph) executeCreateQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	cc := query.CreateClause
	createdVars := make(map[string]interface{})
	createdCount := 0

	// Create nodes
	for _, nodeSpec := range cc.Nodes {
		node := g.createNodeUnlocked(nodeSpec.Labels...)

		// Set properties
		for key, value := range nodeSpec.Properties {
			if err := g.setNodePropertyUnlocked(node.ID, key, value); err != nil {
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

		rel, err := g.createRelationshipUnlocked(relSpec.Type, fromNode.ID, toNode.ID)
		if err != nil {
			return nil, err
		}

		for key, value := range relSpec.Properties {
			if err := g.setRelPropertyUnlocked(rel.ID, key, value); err != nil {
				return nil, err
			}
		}

		if relSpec.Variable != "" {
			createdVars[relSpec.Variable] = rel
		}
		createdCount++
	}

	// Build result
	result := &QueryResult{
		Columns: []string{"created"},
		Rows: []map[string]interface{}{
			{"created": createdCount},
		},
	}

	return result, nil
}

// executeMatchCreateQuery handles MATCH...CREATE queries
func (g *DiskGraph) executeMatchCreateQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Use index-based matching instead of loading entire database
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	cc := query.CreateClause
	createdCount := 0

	for _, match := range matches {
		createdVars := make(map[string]interface{})
		for k, v := range match {
			createdVars[k] = v
		}

		// Create nodes (skip if already matched from MATCH clause)
		for _, nodeSpec := range cc.Nodes {
			// If this variable already exists from MATCH, don't create a new node
			if nodeSpec.Variable != "" {
				if _, exists := createdVars[nodeSpec.Variable]; exists {
					continue
				}
			}
			node := g.createNodeUnlocked(nodeSpec.Labels...)
			for key, value := range nodeSpec.Properties {
				g.setNodePropertyUnlocked(node.ID, key, value)
			}
			if nodeSpec.Variable != "" {
				createdVars[nodeSpec.Variable] = node
			}
			createdCount++
		}

		// Create relationships
		for _, relSpec := range cc.Relationships {
			var fromNode, toNode *Node
			if e, ok := createdVars[relSpec.FromVar]; ok {
				fromNode, _ = e.(*Node)
			}
			if e, ok := createdVars[relSpec.ToVar]; ok {
				toNode, _ = e.(*Node)
			}
			if fromNode == nil || toNode == nil {
				continue
			}

			rel, err := g.createRelationshipUnlocked(relSpec.Type, fromNode.ID, toNode.ID)
			if err != nil {
				continue
			}
			for key, value := range relSpec.Properties {
				g.setRelPropertyUnlocked(rel.ID, key, value)
			}
			createdCount++
		}
	}

	return &QueryResult{
		Columns: []string{"created"},
		Rows:    []map[string]interface{}{{"created": createdCount}},
	}, nil
}

// executeSetQuery handles MATCH...SET queries
func (g *DiskGraph) executeSetQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Use index-based matching
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	updatedCount := 0
	for _, match := range matches {
		for _, update := range query.SetClause.Updates {
			entity, ok := match[update.Variable]
			if !ok {
				continue
			}
			if node, ok := entity.(*Node); ok {
				g.setNodePropertyUnlocked(node.ID, update.Property, update.Value)
				updatedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				g.setRelPropertyUnlocked(rel.ID, update.Property, update.Value)
				updatedCount++
			}
		}
	}

	return &QueryResult{
		Columns: []string{"updated"},
		Rows:    []map[string]interface{}{{"updated": updatedCount}},
	}, nil
}

// executeDeleteQuery handles MATCH...DELETE queries
func (g *DiskGraph) executeDeleteQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Use index-based matching
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	deletedCount := 0
	for _, match := range matches {
		for _, varName := range query.DeleteClause.Variables {
			entity, ok := match[varName]
			if !ok {
				continue
			}
			if node, ok := entity.(*Node); ok {
				if query.DeleteClause.Detach {
					// Delete relationships first
					rels := g.getRelationshipsForNodeUnlocked(node.ID)
					for _, rel := range rels {
						g.deleteRelationshipUnlocked(rel.ID)
					}
				}
				g.deleteNodeUnlocked(node.ID)
				deletedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				g.deleteRelationshipUnlocked(rel.ID)
				deletedCount++
			}
		}
	}

	return &QueryResult{
		Columns: []string{"deleted"},
		Rows:    []map[string]interface{}{{"deleted": deletedCount}},
	}, nil
}

// executeReadQuery handles read-only MATCH queries using LRU cache + indexes.
// No full graph load for the common (non-temporal, non-path, non-similar) case.
func (g *DiskGraph) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Path queries (shortestPath / allShortestPaths)
	if query.IsPathQuery && query.MatchPattern.PathFunction != nil {
		return g.executePathQueryUnlocked(query)
	}

	// Semantic similarity queries
	if query.SimilarToClause != nil {
		return g.executeSimilarToQueryUnlocked(query, embedder)
	}

	// Temporal queries require a disk scan for historical data
	if query.TimeClause != nil {
		return g.executeTemporalReadQueryUnlocked(query)
	}

	// ── Common case: index + LRU cache, no full load ──────────────────────
	rawMatches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)
	matches := make([]Match, len(rawMatches))
	for i, m := range rawMatches {
		matches[i] = Match(m)
	}
	return buildResult(matches, query.ReturnClause), nil
}

// findMatchesUnlocked finds pattern matches using indexes (caller must hold lock)
func (g *DiskGraph) findMatchesUnlocked(pattern *MatchPattern, where *WhereClause) []map[string]interface{} {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	// Start with the first node pattern - use label index
	firstPattern := pattern.Nodes[0]
	var candidateNodes []*Node

	if len(firstPattern.Labels) > 0 {
		// Use label index for fast lookup
		nodeIDs := g.labelIndex[firstPattern.Labels[0]]
		candidateNodes = make([]*Node, 0, len(nodeIDs))
		for _, id := range nodeIDs {
			node := g.getNodeUnlocked(id)
			if node != nil && node.ValidTo == nil {
				candidateNodes = append(candidateNodes, node)
			}
		}
	} else {
		// No label specified - need all nodes (slower path)
		allNodes, _ := g.boltStore.GetAllNodes()
		for _, n := range allNodes {
			if n.ValidTo == nil {
				candidateNodes = append(candidateNodes, n)
			}
		}
	}

	// Filter by properties if specified
	if len(firstPattern.Properties) > 0 {
		filtered := make([]*Node, 0)
		for _, node := range candidateNodes {
			if matchesProperties(node.Properties, firstPattern.Properties) {
				filtered = append(filtered, node)
			}
		}
		candidateNodes = filtered
	}

	// Build initial matches
	var matches []map[string]interface{}
	for _, node := range candidateNodes {
		match := map[string]interface{}{}
		if firstPattern.Variable != "" {
			match[firstPattern.Variable] = node
		}
		matches = append(matches, match)
	}

	// For simple single-node patterns, apply WHERE and return
	if len(pattern.Nodes) == 1 && len(pattern.Relationships) == 0 {
		if where != nil {
			matches = g.filterMatchesUnlocked(matches, where)
		}
		return matches
	}

	// Handle multi-node patterns without relationships (e.g., MATCH (a:Person), (b:Company))
	// This computes a cartesian product of all matching nodes
	if len(pattern.Relationships) == 0 && len(pattern.Nodes) > 1 {
		// Process remaining node patterns (first one already processed)
		for i := 1; i < len(pattern.Nodes); i++ {
			nodePattern := pattern.Nodes[i]

			// Find candidate nodes for this pattern
			var nodeCandidates []*Node
			if len(nodePattern.Labels) > 0 {
				label := nodePattern.Labels[0]
				nodeIDs := g.labelIndex[label]
				for _, id := range nodeIDs {
					node := g.getNodeUnlocked(id)
					if node != nil && node.ValidTo == nil {
						nodeCandidates = append(nodeCandidates, node)
					}
				}
			} else {
				allNodes, _ := g.boltStore.GetAllNodes()
				for _, n := range allNodes {
					if n.ValidTo == nil {
						nodeCandidates = append(nodeCandidates, n)
					}
				}
			}

			// Filter by properties
			if len(nodePattern.Properties) > 0 {
				filtered := make([]*Node, 0)
				for _, node := range nodeCandidates {
					if matchesProperties(node.Properties, nodePattern.Properties) {
						filtered = append(filtered, node)
					}
				}
				nodeCandidates = filtered
			}

			// Compute cartesian product with existing matches
			var newMatches []map[string]interface{}
			for _, match := range matches {
				for _, node := range nodeCandidates {
					newMatch := make(map[string]interface{})
					for k, v := range match {
						newMatch[k] = v
					}
					if nodePattern.Variable != "" {
						newMatch[nodePattern.Variable] = node
					}
					newMatches = append(newMatches, newMatch)
				}
			}
			matches = newMatches
		}

		if where != nil {
			matches = g.filterMatchesUnlocked(matches, where)
		}
		return matches
	}

	// Handle relationship patterns
	for _, relPattern := range pattern.Relationships {
		var newMatches []map[string]interface{}

		for _, match := range matches {
			// Get the "from" node
			fromIdx := relPattern.FromIndex
			if fromIdx >= len(pattern.Nodes) {
				continue
			}
			fromVar := pattern.Nodes[fromIdx].Variable
			fromEntity, ok := match[fromVar]
			if !ok {
				continue
			}
			fromNode, ok := fromEntity.(*Node)
			if !ok {
				continue
			}

			// Get target pattern info
			toIdx := relPattern.ToIndex
			var toPattern *NodePattern
			if toIdx < len(pattern.Nodes) {
				toPattern = &pattern.Nodes[toIdx]
			}

			// Handle variable-length paths
			if relPattern.VarLength {
				// Use BFS to find all paths within hop range
				pathMatches := g.findVariableLengthPaths(
					fromNode,
					relPattern,
					toPattern,
					match,
				)
				newMatches = append(newMatches, pathMatches...)
				continue
			}

			// Regular single-hop relationship matching
			rels := g.getRelationshipsForNodeUnlocked(fromNode.ID)

			for _, rel := range rels {
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

				// Determine target node based on direction
				var targetNodeID string
				if relPattern.Direction == "->" {
					if rel.FromNodeID != fromNode.ID {
						continue
					}
					targetNodeID = rel.ToNodeID
				} else if relPattern.Direction == "<-" {
					if rel.ToNodeID != fromNode.ID {
						continue
					}
					targetNodeID = rel.FromNodeID
				} else {
					// Undirected - either end
					if rel.FromNodeID == fromNode.ID {
						targetNodeID = rel.ToNodeID
					} else {
						targetNodeID = rel.FromNodeID
					}
				}

				// Get target node
				targetNode := g.getNodeUnlocked(targetNodeID)
				if targetNode == nil || targetNode.ValidTo != nil {
					continue
				}

				// Check target node labels
				if toPattern != nil {
					if len(toPattern.Labels) > 0 {
						hasLabel := false
						for _, reqLabel := range toPattern.Labels {
							for _, nodeLabel := range targetNode.Labels {
								if reqLabel == nodeLabel {
									hasLabel = true
									break
								}
							}
							if hasLabel {
								break
							}
						}
						if !hasLabel {
							continue
						}
					}

					// Check target node properties
					if len(toPattern.Properties) > 0 {
						if !matchesProperties(targetNode.Properties, toPattern.Properties) {
							continue
						}
					}

					// Build new match
					newMatch := make(map[string]interface{})
					for k, v := range match {
						newMatch[k] = v
					}
					if toPattern.Variable != "" {
						newMatch[toPattern.Variable] = targetNode
					}
					if relPattern.Variable != "" {
						newMatch[relPattern.Variable] = rel
					}
					newMatches = append(newMatches, newMatch)
				}
			}
		}
		matches = newMatches
	}

	// Apply WHERE clause
	if where != nil {
		matches = g.filterMatchesUnlocked(matches, where)
	}

	return matches
}

// findVariableLengthPaths finds all paths matching variable-length pattern [*min..max]
// Uses BFS to explore paths within the specified hop range
func (g *DiskGraph) findVariableLengthPaths(
	startNode *Node,
	relPattern RelPattern,
	toPattern *NodePattern,
	baseMatch map[string]interface{},
) []map[string]interface{} {
	var results []map[string]interface{}

	minHops := relPattern.MinHops
	maxHops := relPattern.MaxHops
	if maxHops == -1 {
		maxHops = 10 // Reasonable default limit to prevent infinite loops
	}

	// pathState tracks a node and the path taken to reach it
	type pathState struct {
		node        *Node
		rels        []*Relationship
		depth       int
		visitedInPath map[string]bool // Track nodes visited in THIS path to avoid cycles
	}

	// BFS queue - each path tracks its own visited nodes
	initialVisited := make(map[string]bool)
	initialVisited[startNode.ID] = true
	queue := []pathState{{node: startNode, rels: nil, depth: 0, visitedInPath: initialVisited}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// If we're within valid depth range and target matches, add to results
		if current.depth >= minHops && current.depth <= maxHops {
			// Check if current node matches target pattern (if specified)
			if toPattern != nil && current.node.ID != startNode.ID {
				matches := true

				// Check labels
				if len(toPattern.Labels) > 0 {
					hasLabel := false
					for _, reqLabel := range toPattern.Labels {
						for _, nodeLabel := range current.node.Labels {
							if reqLabel == nodeLabel {
								hasLabel = true
								break
							}
						}
						if hasLabel {
							break
						}
					}
					if !hasLabel {
						matches = false
					}
				}

				// Check properties
				if matches && len(toPattern.Properties) > 0 {
					if !matchesProperties(current.node.Properties, toPattern.Properties) {
						matches = false
					}
				}

				if matches {
					newMatch := make(map[string]interface{})
					for k, v := range baseMatch {
						newMatch[k] = v
					}
					if toPattern.Variable != "" {
						newMatch[toPattern.Variable] = current.node
					}
					// For variable-length paths, store the list of relationships
					if relPattern.Variable != "" {
						newMatch[relPattern.Variable] = current.rels
					}
					results = append(results, newMatch)
				}
			}
		}

		// Continue BFS if we haven't reached max depth
		if current.depth >= maxHops {
			continue
		}

		// Get relationships for current node
		rels := g.getRelationshipsForNodeUnlocked(current.node.ID)

		for _, rel := range rels {
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

			// Determine next node based on direction
			var nextNodeID string
			if relPattern.Direction == "->" {
				if rel.FromNodeID != current.node.ID {
					continue
				}
				nextNodeID = rel.ToNodeID
			} else if relPattern.Direction == "<-" {
				if rel.ToNodeID != current.node.ID {
					continue
				}
				nextNodeID = rel.FromNodeID
			} else {
				// Undirected
				if rel.FromNodeID == current.node.ID {
					nextNodeID = rel.ToNodeID
				} else {
					nextNodeID = rel.FromNodeID
				}
			}

			// Avoid cycles within THIS path only
			if current.visitedInPath[nextNodeID] {
				continue
			}

			nextNode := g.getNodeUnlocked(nextNodeID)
			if nextNode == nil || nextNode.ValidTo != nil {
				continue
			}

			// Build new path with its own visited set
			newRels := make([]*Relationship, len(current.rels)+1)
			copy(newRels, current.rels)
			newRels[len(current.rels)] = rel

			// Copy visited set for new path
			newVisited := make(map[string]bool)
			for k, v := range current.visitedInPath {
				newVisited[k] = v
			}
			newVisited[nextNodeID] = true

			queue = append(queue, pathState{
				node:          nextNode,
				rels:          newRels,
				depth:         current.depth + 1,
				visitedInPath: newVisited,
			})
		}
	}

	return results
}

// filterMatchesUnlocked applies WHERE conditions (caller must hold lock)
func (g *DiskGraph) filterMatchesUnlocked(matches []map[string]interface{}, where *WhereClause) []map[string]interface{} {
	if where == nil || len(where.Conditions) == 0 {
		return matches
	}

	var result []map[string]interface{}
	for _, match := range matches {
		allMatch := true
		for _, cond := range where.Conditions {
			entity, ok := match[cond.Variable]
			if !ok {
				allMatch = false
				break
			}

			var props map[string]interface{}
			if node, ok := entity.(*Node); ok {
				props = node.Properties
			} else if rel, ok := entity.(*Relationship); ok {
				props = rel.Properties
			} else {
				allMatch = false
				break
			}

			propVal, exists := props[cond.Property]
			if !exists {
				allMatch = false
				break
			}

			if !evaluateCondition(propVal, cond.Operator, cond.Value) {
				allMatch = false
				break
			}
		}
		if allMatch {
			result = append(result, match)
		}
	}
	return result
}

// matchesProperties checks if node properties match required properties
func matchesProperties(nodeProps, required map[string]interface{}) bool {
	for key, reqVal := range required {
		if nodeVal, ok := nodeProps[key]; !ok || nodeVal != reqVal {
			return false
		}
	}
	return true
}

// evaluateCondition evaluates a single WHERE condition
func evaluateCondition(propVal interface{}, operator string, condVal interface{}) bool {
	switch operator {
	case "=", "==":
		return fmt.Sprintf("%v", propVal) == fmt.Sprintf("%v", condVal)
	case "!=", "<>":
		return fmt.Sprintf("%v", propVal) != fmt.Sprintf("%v", condVal)
	case ">":
		return compareValues(propVal, condVal) > 0
	case ">=":
		return compareValues(propVal, condVal) >= 0
	case "<":
		return compareValues(propVal, condVal) < 0
	case "<=":
		return compareValues(propVal, condVal) <= 0
	default:
		return false
	}
}

// compareValues compares two values numerically if possible
func compareValues(a, b interface{}) int {
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}
	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// toFloat64 converts a value to float64 if possible
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case float32:
		return float64(val), true
	default:
		return 0, false
	}
}

// executeMergeQuery handles MERGE queries (match or create pattern)
func (g *DiskGraph) executeMergeQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	mc := query.MergeClause
	if mc == nil || mc.Pattern == nil {
		return nil, fmt.Errorf("MERGE requires a pattern")
	}

	// Try to find existing matches for the pattern
	matches := g.findMatchesUnlocked(mc.Pattern, query.WhereClause)

	mergedCount := 0
	createdCount := 0
	var resultVars map[string]interface{}

	if len(matches) > 0 {
		// Pattern exists - apply ON MATCH SET if specified
		for _, match := range matches {
			for _, update := range mc.OnMatchSets {
				entity, ok := match[update.Variable]
				if !ok {
					continue
				}
				if node, ok := entity.(*Node); ok {
					g.setNodePropertyUnlocked(node.ID, update.Property, update.Value)
				} else if rel, ok := entity.(*Relationship); ok {
					g.setRelPropertyUnlocked(rel.ID, update.Property, update.Value)
				}
			}
			mergedCount++
			resultVars = match // Keep last match for RETURN
		}
	} else {
		// Pattern does not exist - create it and apply ON CREATE SET
		resultVars = make(map[string]interface{})

		// Create nodes from pattern
		for _, nodePattern := range mc.Pattern.Nodes {
			node := g.createNodeUnlocked(nodePattern.Labels...)

			// Set inline properties from pattern
			for key, value := range nodePattern.Properties {
				g.setNodePropertyUnlocked(node.ID, key, value)
			}

			if nodePattern.Variable != "" {
				resultVars[nodePattern.Variable] = node
			}
			createdCount++
		}

		// Create relationships from pattern
		for _, relPattern := range mc.Pattern.Relationships {
			if relPattern.FromIndex >= len(mc.Pattern.Nodes) || relPattern.ToIndex >= len(mc.Pattern.Nodes) {
				continue
			}

			fromVar := mc.Pattern.Nodes[relPattern.FromIndex].Variable
			toVar := mc.Pattern.Nodes[relPattern.ToIndex].Variable

			var fromNode, toNode *Node
			if e, ok := resultVars[fromVar]; ok {
				fromNode, _ = e.(*Node)
			}
			if e, ok := resultVars[toVar]; ok {
				toNode, _ = e.(*Node)
			}

			if fromNode == nil || toNode == nil {
				continue
			}

			relType := ""
			if len(relPattern.Types) > 0 {
				relType = relPattern.Types[0]
			}

			rel, err := g.createRelationshipUnlocked(relType, fromNode.ID, toNode.ID)
			if err != nil {
				continue
			}

			if relPattern.Variable != "" {
				resultVars[relPattern.Variable] = rel
			}
			createdCount++
		}

		// Apply ON CREATE SET
		for _, update := range mc.OnCreateSets {
			entity, ok := resultVars[update.Variable]
			if !ok {
				continue
			}
			if node, ok := entity.(*Node); ok {
				g.setNodePropertyUnlocked(node.ID, update.Property, update.Value)
			} else if rel, ok := entity.(*Relationship); ok {
				g.setRelPropertyUnlocked(rel.ID, update.Property, update.Value)
			}
		}
	}

	// Build result
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	if query.ReturnClause != nil && resultVars != nil {
		row := map[string]interface{}{}
		for _, item := range query.ReturnClause.Items {
			colName := item.Variable
			if item.Property != "" {
				colName = item.Variable + "." + item.Property
			}
			result.Columns = append(result.Columns, colName)

			if val, ok := resultVars[item.Variable]; ok {
				if item.Property != "" {
					if node, ok := val.(*Node); ok {
						row[colName] = node.Properties[item.Property]
					} else if rel, ok := val.(*Relationship); ok {
						row[colName] = rel.Properties[item.Property]
					}
				} else {
					row[colName] = val
				}
			}
		}
		result.Rows = append(result.Rows, row)
	} else {
		result.Columns = []string{"merged", "created"}
		result.Rows = append(result.Rows, map[string]interface{}{
			"merged":  mergedCount,
			"created": createdCount,
		})
	}

	return result, nil
}

// executeRemoveQuery handles MATCH...REMOVE queries (remove properties or labels from nodes/relationships)
func (g *DiskGraph) executeRemoveQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Find matches
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	removedCount := 0
	for _, match := range matches {
		for _, item := range query.RemoveClause.Items {
			entity, ok := match[item.Variable]
			if !ok {
				continue
			}

			if item.Property != "" {
				// Remove property
				if node, ok := entity.(*Node); ok {
					if err := g.deleteNodePropertyUnlocked(node.ID, item.Property); err == nil {
						removedCount++
					}
				} else if rel, ok := entity.(*Relationship); ok {
					if err := g.deleteRelPropertyUnlocked(rel.ID, item.Property); err == nil {
						removedCount++
					}
				}
			} else if item.Label != "" {
				// Remove label (only applicable to nodes)
				if node, ok := entity.(*Node); ok {
					if err := g.removeNodeLabelUnlocked(node.ID, item.Label); err == nil {
						removedCount++
					}
				}
			}
		}
	}

	return &QueryResult{
		Columns: []string{"removed"},
		Rows:    []map[string]interface{}{{"removed": removedCount}},
	}, nil
}

// executeUnwindQuery handles UNWIND...RETURN queries (expand lists into rows)
func (g *DiskGraph) executeUnwindQuery(query *Query) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	uc := query.UnwindClause
	if uc == nil {
		return nil, fmt.Errorf("UNWIND clause required")
	}

	// Get the list to unwind
	list, ok := uc.Expression.([]interface{})
	if !ok {
		return nil, fmt.Errorf("UNWIND requires a list expression")
	}

	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	// Build column names from RETURN clause
	if query.ReturnClause != nil {
		for _, item := range query.ReturnClause.Items {
			if item.Property != "" {
				result.Columns = append(result.Columns, item.Variable+"."+item.Property)
			} else {
				result.Columns = append(result.Columns, item.Variable)
			}
		}
	} else {
		result.Columns = []string{uc.Variable}
	}

	// Unwind the list into rows
	for _, elem := range list {
		row := map[string]interface{}{}

		if query.ReturnClause != nil {
			for _, item := range query.ReturnClause.Items {
				colName := item.Variable
				if item.Property != "" {
					colName = item.Variable + "." + item.Property
				}

				if item.Variable == uc.Variable {
					if item.Property != "" {
						// Try to access property on the element
						if m, ok := elem.(map[string]interface{}); ok {
							row[colName] = m[item.Property]
						}
					} else {
						row[colName] = elem
					}
				}
			}
		} else {
			row[uc.Variable] = elem
		}

		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// executeEmbedQuery handles MATCH...EMBED queries and persists embeddings to disk
func (g *DiskGraph) executeEmbedQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	if embedder == nil {
		return nil, fmt.Errorf("embedder required for EMBED clause")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Find matches using index
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	embeddedCount := 0
	ec := query.EmbedClause

	for _, match := range matches {
		entity, ok := match[ec.Variable]
		if !ok {
			continue
		}

		node, ok := entity.(*Node)
		if !ok {
			continue // Can only embed nodes
		}

		// Generate text based on mode
		var text string
		switch ec.Mode {
		case "AUTO":
			text = g.generateAutoEmbedText(node)
		case "TEXT":
			text = ec.Text
		case "PROPERTY":
			if propVal, exists := node.Properties[ec.Property]; exists {
				text = fmt.Sprint(propVal)
			} else {
				continue // Skip nodes without the property
			}
		}

		if text == "" {
			continue
		}

		// Generate embedding
		vector, err := embedder.Embed(text)
		if err != nil {
			return nil, fmt.Errorf("failed to embed node %s: %w", node.ID, err)
		}

		// Create property snapshot
		propertySnapshot := make(map[string]interface{})
		for k, v := range node.Properties {
			propertySnapshot[k] = v
		}

		// Get existing embeddings and append new one
		existingEmbs, _ := g.boltStore.GetEmbedding(node.ID)

		newEmb := &Embedding{
			Vector:           vector,
			Model:            "embedder",
			PropertySnapshot: propertySnapshot,
		}

		// Store embedding to disk
		if err := g.boltStore.SaveEmbedding(node.ID, append(existingEmbs, newEmb)); err != nil {
			return nil, fmt.Errorf("failed to save embedding: %w", err)
		}
		embeddedCount++
	}

	return &QueryResult{
		Columns: []string{"embedded"},
		Rows:    []map[string]interface{}{{"embedded": embeddedCount}},
	}, nil
}

// generateAutoEmbedText generates embedding text from node labels and properties
func (g *DiskGraph) generateAutoEmbedText(node *Node) string {
	var parts []string

	// Add labels
	if len(node.Labels) > 0 {
		parts = append(parts, strings.Join(node.Labels, " "))
	}

	// Add properties
	for key, value := range node.Properties {
		parts = append(parts, fmt.Sprintf("%s: %v", key, value))
	}

	return strings.Join(parts, ". ")
}

// ============================================================================
// Disk-native read helpers (no full graph load)
// ============================================================================

// getQueryTimeUnlocked converts a TimeClause to a *time.Time (caller holds lock)
func (g *DiskGraph) getQueryTimeUnlocked(tc *TimeClause) *time.Time {
	if tc == nil {
		return nil
	}
	if tc.Mode == "EARLIEST" {
		var earliest *time.Time
		if allNodes, err := g.boltStore.GetAllNodes(); err == nil {
			for _, n := range allNodes {
				if earliest == nil || n.ValidFrom.Before(*earliest) {
					t := n.ValidFrom
					earliest = &t
				}
			}
		}
		if allRels, err := g.boltStore.GetAllRelationships(); err == nil {
			for _, r := range allRels {
				if earliest == nil || r.ValidFrom.Before(*earliest) {
					t := r.ValidFrom
					earliest = &t
				}
			}
		}
		return earliest
	}
	t := time.Unix(tc.Timestamp, 0)
	return &t
}

// executeTemporalReadQueryUnlocked handles AT TIME / EARLIEST queries.
// Scans BoltDB for historical data — unavoidable for point-in-time queries.
func (g *DiskGraph) executeTemporalReadQueryUnlocked(query *Query) (*QueryResult, error) {
	queryTime := g.getQueryTimeUnlocked(query.TimeClause)
	if queryTime == nil {
		return &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}, nil
	}

	// Build temporal node snapshot: latest version valid at queryTime per ID
	allNodes, _ := g.boltStore.GetAllNodes()
	nodeMap := make(map[string]*Node)
	for _, node := range allNodes {
		if !node.IsValidAt(*queryTime) {
			continue
		}
		if ex, ok := nodeMap[node.ID]; !ok || node.ValidFrom.After(ex.ValidFrom) {
			nodeMap[node.ID] = node
		}
	}
	labelIdx := make(map[string][]*Node)
	for _, node := range nodeMap {
		for _, label := range node.Labels {
			labelIdx[label] = append(labelIdx[label], node)
		}
	}

	// Build temporal relationship snapshot
	allRels, _ := g.boltStore.GetAllRelationships()
	relMap := make(map[string]*Relationship)
	for _, rel := range allRels {
		if !rel.IsValidAt(*queryTime) {
			continue
		}
		if ex, ok := relMap[rel.ID]; !ok || rel.ValidFrom.After(ex.ValidFrom) {
			relMap[rel.ID] = rel
		}
	}
	relsByNode := make(map[string][]*Relationship)
	for _, rel := range relMap {
		relsByNode[rel.FromNodeID] = append(relsByNode[rel.FromNodeID], rel)
		relsByNode[rel.ToNodeID] = append(relsByNode[rel.ToNodeID], rel)
	}

	matches := findTemporalMatches(query.MatchPattern, nodeMap, labelIdx, relsByNode)
	if query.WhereClause != nil {
		matches = filterTemporalMatches(matches, query.WhereClause)
	}
	return buildResult(matches, query.ReturnClause), nil
}

// findTemporalMatches does pattern matching against a pre-built temporal snapshot.
func findTemporalMatches(
	pattern *MatchPattern,
	nodeMap map[string]*Node,
	labelIdx map[string][]*Node,
	relsByNode map[string][]*Relationship,
) []Match {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	firstPat := pattern.Nodes[0]
	var candidates []*Node
	if len(firstPat.Labels) > 0 {
		for _, label := range firstPat.Labels {
			candidates = append(candidates, labelIdx[label]...)
		}
	} else {
		for _, n := range nodeMap {
			candidates = append(candidates, n)
		}
	}
	if len(firstPat.Properties) > 0 {
		var filtered []*Node
		for _, n := range candidates {
			if matchesProperties(n.Properties, firstPat.Properties) {
				filtered = append(filtered, n)
			}
		}
		candidates = filtered
	}

	var matches []Match
	for _, node := range candidates {
		m := Match{}
		if firstPat.Variable != "" {
			m[firstPat.Variable] = node
		}
		matches = append(matches, m)
	}
	if len(pattern.Relationships) == 0 {
		return matches
	}

	for _, relPat := range pattern.Relationships {
		var newMatches []Match
		for _, match := range matches {
			if relPat.FromIndex >= len(pattern.Nodes) {
				continue
			}
			fromVar := pattern.Nodes[relPat.FromIndex].Variable
			fromEntity, ok := match[fromVar]
			if !ok {
				continue
			}
			fromNode, ok := fromEntity.(*Node)
			if !ok {
				continue
			}
			var toPat *NodePattern
			if relPat.ToIndex < len(pattern.Nodes) {
				toPat = &pattern.Nodes[relPat.ToIndex]
			}

			for _, rel := range relsByNode[fromNode.ID] {
				if len(relPat.Types) > 0 {
					typeMatch := false
					for _, t := range relPat.Types {
						if rel.Type == t {
							typeMatch = true
							break
						}
					}
					if !typeMatch {
						continue
					}
				}
				var targetID string
				if relPat.Direction == "->" {
					if rel.FromNodeID != fromNode.ID {
						continue
					}
					targetID = rel.ToNodeID
				} else if relPat.Direction == "<-" {
					if rel.ToNodeID != fromNode.ID {
						continue
					}
					targetID = rel.FromNodeID
				} else {
					if rel.FromNodeID == fromNode.ID {
						targetID = rel.ToNodeID
					} else {
						targetID = rel.FromNodeID
					}
				}

				targetNode, ok := nodeMap[targetID]
				if !ok {
					continue
				}
				if toPat != nil {
					if !temporalNodeMatchesPat(targetNode, *toPat) {
						continue
					}
				}
				newMatch := Match{}
				for k, v := range match {
					newMatch[k] = v
				}
				if toPat != nil && toPat.Variable != "" {
					newMatch[toPat.Variable] = targetNode
				}
				if relPat.Variable != "" {
					newMatch[relPat.Variable] = rel
				}
				newMatches = append(newMatches, newMatch)
			}
		}
		matches = newMatches
	}
	return matches
}

// temporalNodeMatchesPat checks labels + properties for temporal pattern matching.
func temporalNodeMatchesPat(node *Node, pat NodePattern) bool {
	for _, reqLabel := range pat.Labels {
		found := false
		for _, l := range node.Labels {
			if l == reqLabel {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return len(pat.Properties) == 0 || matchesProperties(node.Properties, pat.Properties)
}

// filterTemporalMatches applies WHERE conditions to a []Match slice.
func filterTemporalMatches(matches []Match, where *WhereClause) []Match {
	if where == nil || len(where.Conditions) == 0 {
		return matches
	}
	var result []Match
	for _, match := range matches {
		ok := true
		for _, cond := range where.Conditions {
			entity, exists := match[cond.Variable]
			if !exists {
				ok = false
				break
			}
			var props map[string]interface{}
			switch e := entity.(type) {
			case *Node:
				props = e.Properties
			case *Relationship:
				props = e.Properties
			default:
				ok = false
				break
			}
			propVal, has := props[cond.Property]
			if !has || !evaluateCondition(propVal, cond.Operator, cond.Value) {
				ok = false
				break
			}
		}
		if ok {
			result = append(result, match)
		}
	}
	return result
}

// getCandidateNodesUnlocked returns current nodes matching a node pattern (caller holds lock).
func (g *DiskGraph) getCandidateNodesUnlocked(pattern NodePattern) []*Node {
	var candidates []*Node
	if len(pattern.Labels) > 0 {
		for _, id := range g.labelIndex[pattern.Labels[0]] {
			n := g.getNodeUnlocked(id)
			if n != nil && n.ValidTo == nil {
				candidates = append(candidates, n)
			}
		}
	} else {
		allNodes, _ := g.boltStore.GetAllNodes()
		for _, n := range allNodes {
			if n.ValidTo == nil {
				candidates = append(candidates, n)
			}
		}
	}
	if len(pattern.Properties) > 0 {
		var filtered []*Node
		for _, n := range candidates {
			if matchesProperties(n.Properties, pattern.Properties) {
				filtered = append(filtered, n)
			}
		}
		return filtered
	}
	return candidates
}

// diskPathStep is used during BFS path reconstruction.
type diskPathStep struct {
	relationship *Relationship
	previousID   string
}

// executePathQueryUnlocked handles shortestPath / allShortestPaths using LRU cache.
func (g *DiskGraph) executePathQueryUnlocked(query *Query) (*QueryResult, error) {
	pf := query.MatchPattern.PathFunction
	startCandidates := g.getCandidateNodesUnlocked(pf.StartPattern)
	endCandidates := g.getCandidateNodesUnlocked(pf.EndPattern)

	// Apply per-variable WHERE filters to start / end candidates
	filteredStart := filterNodesByWhere(startCandidates, pf.StartPattern.Variable, query.WhereClause)
	filteredEnd := filterNodesByWhere(endCandidates, pf.EndPattern.Variable, query.WhereClause)

	var allPaths []*Path
	for _, startNode := range filteredStart {
		for _, endNode := range filteredEnd {
			if startNode.ID == endNode.ID {
				continue
			}
			if pf.Function == "shortestpath" {
				if path := g.diskShortestPath(startNode.ID, endNode.ID); path != nil {
					allPaths = append(allPaths, path)
				}
			} else if pf.Function == "allshortestpaths" {
				maxDepth := pf.MaxDepth
				if maxDepth == 0 {
					maxDepth = 10
				}
				allPaths = append(allPaths, g.diskAllPaths(startNode.ID, endNode.ID, maxDepth)...)
			}
		}
	}

	result := &QueryResult{Columns: []string{pf.Variable}, Rows: []map[string]interface{}{}}
	for _, path := range allPaths {
		result.Rows = append(result.Rows, map[string]interface{}{pf.Variable: path})
	}
	return result, nil
}

// filterNodesByWhere keeps only nodes that satisfy WHERE conditions for a given variable.
func filterNodesByWhere(nodes []*Node, variable string, where *WhereClause) []*Node {
	if where == nil || len(where.Conditions) == 0 {
		return nodes
	}
	var result []*Node
	for _, n := range nodes {
		ok := true
		for _, cond := range where.Conditions {
			if cond.Variable != variable {
				continue
			}
			propVal, has := n.Properties[cond.Property]
			if !has || !evaluateCondition(propVal, cond.Operator, cond.Value) {
				ok = false
				break
			}
		}
		if ok {
			result = append(result, n)
		}
	}
	return result
}

// diskShortestPath finds the shortest path between two nodes using BFS + LRU cache.
func (g *DiskGraph) diskShortestPath(fromID, toID string) *Path {
	queue := list.New()
	visited := map[string]bool{fromID: true}
	parent := map[string]*diskPathStep{}

	queue.PushBack(fromID)
	for queue.Len() > 0 {
		elem := queue.Front()
		currentID := elem.Value.(string)
		queue.Remove(elem)

		if currentID == toID {
			return g.reconstructDiskPath(fromID, toID, parent)
		}

		for _, rel := range g.getRelationshipsForNodeUnlocked(currentID) {
			var neighborID string
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if visited[neighborID] {
				continue
			}
			neighbor := g.getNodeUnlocked(neighborID)
			if neighbor == nil || neighbor.ValidTo != nil {
				continue
			}
			visited[neighborID] = true
			parent[neighborID] = &diskPathStep{relationship: rel, previousID: currentID}
			queue.PushBack(neighborID)
		}
	}
	return nil
}

// reconstructDiskPath rebuilds a Path from BFS parent map.
func (g *DiskGraph) reconstructDiskPath(fromID, toID string, parent map[string]*diskPathStep) *Path {
	var steps []string
	var rels []*Relationship
	current := toID
	for current != fromID {
		steps = append(steps, current)
		step := parent[current]
		if step == nil {
			return nil
		}
		rels = append(rels, step.relationship)
		current = step.previousID
	}
	steps = append(steps, fromID)

	path := &Path{Nodes: []*Node{}, Relationships: []*Relationship{}}
	for i := len(steps) - 1; i >= 0; i-- {
		if n := g.getNodeUnlocked(steps[i]); n != nil {
			path.Nodes = append(path.Nodes, n)
		}
	}
	for i := len(rels) - 1; i >= 0; i-- {
		path.Relationships = append(path.Relationships, rels[i])
	}
	path.Length = len(path.Relationships)
	return path
}

// diskAllPaths finds all simple paths via DFS + LRU cache.
func (g *DiskGraph) diskAllPaths(fromID, toID string, maxDepth int) []*Path {
	fromNode := g.getNodeUnlocked(fromID)
	if fromNode == nil || fromNode.ValidTo != nil {
		return nil
	}
	var allPaths []*Path
	visited := map[string]bool{fromID: true}
	currentPath := &Path{Nodes: []*Node{}, Relationships: []*Relationship{}}
	g.diskDFSAllPaths(fromID, toID, visited, currentPath, &allPaths, maxDepth, 0)
	return allPaths
}

func (g *DiskGraph) diskDFSAllPaths(
	currentID, targetID string,
	visited map[string]bool,
	currentPath *Path,
	allPaths *[]*Path,
	maxDepth, depth int,
) {
	if maxDepth > 0 && depth >= maxDepth {
		return
	}
	n := g.getNodeUnlocked(currentID)
	if n == nil || n.ValidTo != nil {
		return
	}
	currentPath.Nodes = append(currentPath.Nodes, n)
	visited[currentID] = true

	if currentID == targetID {
		cp := &Path{
			Nodes:         make([]*Node, len(currentPath.Nodes)),
			Relationships: make([]*Relationship, len(currentPath.Relationships)),
			Length:        len(currentPath.Relationships),
		}
		copy(cp.Nodes, currentPath.Nodes)
		copy(cp.Relationships, currentPath.Relationships)
		*allPaths = append(*allPaths, cp)
	} else {
		for _, rel := range g.getRelationshipsForNodeUnlocked(currentID) {
			var neighborID string
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if visited[neighborID] {
				continue
			}
			neighbor := g.getNodeUnlocked(neighborID)
			if neighbor == nil || neighbor.ValidTo != nil {
				continue
			}
			currentPath.Relationships = append(currentPath.Relationships, rel)
			g.diskDFSAllPaths(neighborID, targetID, visited, currentPath, allPaths, maxDepth, depth+1)
			currentPath.Relationships = currentPath.Relationships[:len(currentPath.Relationships)-1]
		}
	}
	currentPath.Nodes = currentPath.Nodes[:len(currentPath.Nodes)-1]
	visited[currentID] = false
}

// executeSimilarToQueryUnlocked handles SIMILAR TO without loading the full graph.
// It loads only embeddings from disk and uses labelIndex + LRU for candidate nodes.
func (g *DiskGraph) executeSimilarToQueryUnlocked(query *Query, embedder Embedder) (*QueryResult, error) {
	if embedder == nil {
		return nil, fmt.Errorf("embedder required for SIMILAR TO clause")
	}
	stc := query.SimilarToClause

	queryVector, err := embedder.Embed(stc.QueryText)
	if err != nil {
		return nil, fmt.Errorf("failed to embed query text: %w", err)
	}

	// Candidate node IDs filtered by label and WHERE
	candidateIDs := make(map[string]bool)
	if len(query.MatchPattern.Nodes) > 0 {
		pat := query.MatchPattern.Nodes[0]
		for _, n := range g.getCandidateNodesUnlocked(pat) {
			ok := true
			if query.WhereClause != nil {
				for _, cond := range query.WhereClause.Conditions {
					if cond.Variable != pat.Variable {
						continue
					}
					pv, has := n.Properties[cond.Property]
					if !has || !evaluateCondition(pv, cond.Operator, cond.Value) {
						ok = false
						break
					}
				}
			}
			if ok {
				candidateIDs[n.ID] = true
			}
		}
	}

	// Load embeddings from disk into a transient store
	embStore := embedding.NewStore()
	if allEmbs, err := g.boltStore.GetAllEmbeddings(); err == nil {
		for nodeID, embs := range allEmbs {
			for _, emb := range embs {
				embStore.Add(nodeID, emb.Vector, emb.Model, emb.PropertySnapshot)
			}
		}
	}

	limit := stc.Limit
	if limit == 0 {
		limit = 100
	}

	result := &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}
	if query.ReturnClause != nil {
		for _, item := range query.ReturnClause.Items {
			if item.Property != "" {
				result.Columns = append(result.Columns, item.Variable+"."+item.Property)
			} else {
				result.Columns = append(result.Columns, item.Variable)
			}
		}
		result.Columns = append(result.Columns, "similarity")
		if stc.ThroughTime {
			result.Columns = append(result.Columns, "valid_from", "valid_to")
			if stc.DriftMode {
				result.Columns = append(result.Columns, "drift_from_previous", "drift_from_first")
			}
		}
	} else {
		if stc.ThroughTime {
			if stc.DriftMode {
				result.Columns = []string{"node", "similarity", "valid_from", "valid_to", "drift_from_previous", "drift_from_first"}
			} else {
				result.Columns = []string{"node", "similarity", "valid_from", "valid_to"}
			}
		} else {
			result.Columns = []string{"node", "similarity"}
		}
	}

	if stc.ThroughTime {
		versionedResults := embStore.SearchAllVersions(queryVector, limit, candidateIDs, stc.Threshold, stc.DriftMode)
		for _, vsr := range versionedResults {
			node := g.getNodeUnlocked(vsr.NodeID)
			if node == nil {
				continue
			}
			properties := vsr.Embedding.PropertySnapshot
			if properties == nil {
				properties = node.Properties
			}
			row := map[string]interface{}{}
			if query.ReturnClause != nil {
				for _, item := range query.ReturnClause.Items {
					colName := item.Variable
					if item.Property != "" {
						colName = item.Variable + "." + item.Property
						row[colName] = properties[item.Property]
					} else {
						row[colName] = &Node{ID: node.ID, Labels: node.Labels, Properties: properties, ValidFrom: vsr.ValidFrom, ValidTo: vsr.ValidTo}
					}
				}
			} else {
				row["node"] = &Node{ID: node.ID, Labels: node.Labels, Properties: properties, ValidFrom: vsr.ValidFrom, ValidTo: vsr.ValidTo}
			}
			row["similarity"] = vsr.Similarity
			row["valid_from"] = vsr.ValidFrom
			if vsr.ValidTo != nil {
				row["valid_to"] = *vsr.ValidTo
			} else {
				row["valid_to"] = nil
			}
			if stc.DriftMode {
				row["drift_from_previous"] = vsr.DriftFromPrevious
				row["drift_from_first"] = vsr.DriftFromFirst
			}
			result.Rows = append(result.Rows, row)
		}
	} else {
		searchResults := embStore.Search(queryVector, limit, time.Now(), candidateIDs)
		for _, sr := range searchResults {
			if stc.Threshold > 0 && sr.Similarity < stc.Threshold {
				continue
			}
			node := g.getNodeUnlocked(sr.NodeID)
			if node == nil {
				continue
			}
			row := map[string]interface{}{}
			if query.ReturnClause != nil {
				for _, item := range query.ReturnClause.Items {
					colName := item.Variable
					if item.Property != "" {
						colName = item.Variable + "." + item.Property
						row[colName] = node.Properties[item.Property]
					} else {
						row[colName] = node
					}
				}
			} else {
				row["node"] = node
			}
			row["similarity"] = sr.Similarity
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}
