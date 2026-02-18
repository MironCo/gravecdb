package graph

import (
	"fmt"
	"strings"
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

// executeReadQuery handles read-only MATCH queries using in-memory graph
func (g *DiskGraph) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	memGraph := g.loadIntoMemoryUnlocked()

	// Load embeddings
	embeddings, _ := g.boltStore.GetAllEmbeddings()
	for nodeID, embs := range embeddings {
		for _, emb := range embs {
			memGraph.embeddings.Add(nodeID, emb.Vector, emb.Model, emb.PropertySnapshot)
		}
	}

	return memGraph.ExecuteQueryWithEmbedder(query, embedder)
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
