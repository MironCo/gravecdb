package graph

import (
	"fmt"
	"strings"
	"time"
)

// ExecuteQuery executes a parsed query against the graph
func (g *Graph) ExecuteQuery(query *Query) (*QueryResult, error) {
	switch query.QueryType {
	case "CREATE":
		return g.executeCreateQuery(query)
	case "MATCH":
		// Handle MATCH with SET or DELETE
		if query.SetClause != nil {
			return g.executeSetQuery(query)
		}
		if query.DeleteClause != nil {
			return g.executeDeleteQuery(query)
		}
		// Regular MATCH query
		return g.executeMatchQuery(query)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", query.QueryType)
	}
}

// executeMatchQuery executes a regular MATCH query
func (g *Graph) executeMatchQuery(query *Query) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check if this is a path-finding query
	if query.IsPathQuery && query.MatchPattern.PathFunction != nil {
		return g.executePathQuery(query)
	}

	// Find all matches for the pattern
	matches := g.findMatches(query.MatchPattern, query.TimeClause)

	// Apply WHERE filters
	if query.WhereClause != nil {
		matches = g.filterMatches(matches, query.WhereClause)
	}

	// Build result based on RETURN clause
	result := g.buildResult(matches, query.ReturnClause)

	return result, nil
}

// Match represents a single match of the pattern
type Match map[string]interface{} // variable -> node or relationship

// findMatches finds all matches of the pattern in the graph
func (g *Graph) findMatches(pattern *MatchPattern, timeClause *TimeClause) []Match {
	if len(pattern.Nodes) == 0 {
		return nil
	}

	matches := []Match{}

	// Determine the query time based on TimeClause
	queryTime := g.getQueryTime(timeClause)

	// Start with the first node pattern
	firstNodePattern := pattern.Nodes[0]
	candidateNodes := g.getCandidateNodes(firstNodePattern)

	// For each candidate starting node, try to match the pattern
	for _, startNode := range candidateNodes {
		// Check validity based on temporal mode
		if queryTime != nil {
			if !startNode.IsValidAt(*queryTime) {
				continue
			}
		} else {
			if !startNode.IsCurrentlyValid() {
				continue
			}
		}

		// Initialize a new match
		match := Match{}
		if firstNodePattern.Variable != "" {
			match[firstNodePattern.Variable] = startNode
		}

		// Try to extend this match through the pattern
		g.extendMatch(match, pattern, 0, queryTime, &matches)
	}

	return matches
}

// getQueryTime converts a TimeClause to an actual time.Time for querying
// Returns nil if no temporal filtering should be applied (current time query)
func (g *Graph) getQueryTime(timeClause *TimeClause) *time.Time {
	if timeClause == nil {
		return nil // No temporal filtering
	}

	if timeClause.Mode == "EARLIEST" {
		// For EARLIEST mode, we need to find the earliest ValidFrom timestamp
		// across all nodes and relationships in the graph
		var earliest *time.Time

		for _, node := range g.nodes {
			if earliest == nil || node.ValidFrom.Before(*earliest) {
				t := node.ValidFrom
				earliest = &t
			}
		}

		for _, rel := range g.relationships {
			if earliest == nil || rel.ValidFrom.Before(*earliest) {
				t := rel.ValidFrom
				earliest = &t
			}
		}

		return earliest
	}

	// TIMESTAMP mode: convert Unix timestamp to time.Time
	t := time.Unix(timeClause.Timestamp, 0)
	return &t
}

// extendMatch recursively extends a partial match
func (g *Graph) extendMatch(currentMatch Match, pattern *MatchPattern, relIndex int, queryTime *time.Time, allMatches *[]Match) {
	// Base case: if we've matched all relationships, we have a complete match
	if relIndex >= len(pattern.Relationships) {
		// Create a copy of the match
		matchCopy := Match{}
		for k, v := range currentMatch {
			matchCopy[k] = v
		}
		*allMatches = append(*allMatches, matchCopy)
		return
	}

	// Get the current relationship pattern to match
	relPattern := pattern.Relationships[relIndex]
	fromNodePattern := pattern.Nodes[relPattern.FromIndex]
	toNodePattern := pattern.Nodes[relPattern.ToIndex]

	// Get the "from" node from current match
	fromNode, ok := currentMatch[fromNodePattern.Variable].(*Node)
	if !ok {
		return
	}

	// Get all relationships from this node
	rels := g.getRelationshipsForNodeUnlocked(fromNode.ID)

	for _, rel := range rels {
		// Check validity based on temporal mode
		if queryTime != nil {
			if !rel.IsValidAt(*queryTime) {
				continue
			}
		} else {
			if !rel.IsCurrentlyValid() {
				continue
			}
		}

		// Check if relationship type matches
		if len(relPattern.Types) > 0 {
			typeMatch := false
			for _, relType := range relPattern.Types {
				if rel.Type == relType {
					typeMatch = true
					break
				}
			}
			if !typeMatch {
				continue
			}
		}

		// Determine the "to" node based on direction
		var toNodeID string
		if relPattern.Direction == "->" {
			// Must be: fromNode -> toNode
			if rel.FromNodeID != fromNode.ID {
				continue
			}
			toNodeID = rel.ToNodeID
		} else if relPattern.Direction == "<-" {
			// Must be: fromNode <- toNode (so toNode -> fromNode in data)
			if rel.ToNodeID != fromNode.ID {
				continue
			}
			toNodeID = rel.FromNodeID
		} else {
			// Bidirectional: either direction works
			if rel.FromNodeID == fromNode.ID {
				toNodeID = rel.ToNodeID
			} else if rel.ToNodeID == fromNode.ID {
				toNodeID = rel.FromNodeID
			} else {
				continue
			}
		}

		toNode := g.getNodeByID(toNodeID)
		if toNode == nil {
			continue
		}

		// Check validity based on temporal mode
		if queryTime != nil {
			if !toNode.IsValidAt(*queryTime) {
				continue
			}
		} else {
			if !toNode.IsCurrentlyValid() {
				continue
			}
		}

		// Check if toNode matches the pattern
		if !g.nodeMatchesPattern(toNode, toNodePattern) {
			continue
		}

		// Create extended match
		extendedMatch := Match{}
		for k, v := range currentMatch {
			extendedMatch[k] = v
		}
		if toNodePattern.Variable != "" {
			extendedMatch[toNodePattern.Variable] = toNode
		}
		if relPattern.Variable != "" {
			extendedMatch[relPattern.Variable] = rel
		}

		// Recurse to match next relationship
		g.extendMatch(extendedMatch, pattern, relIndex+1, queryTime, allMatches)
	}
}

// getCandidateNodes returns all nodes that match a node pattern
func (g *Graph) getCandidateNodes(pattern NodePattern) []*Node {
	candidates := []*Node{}

	if len(pattern.Labels) > 0 {
		// Get nodes by label
		for _, label := range pattern.Labels {
			nodes := g.getNodesByLabelUnlocked(label)
			candidates = append(candidates, nodes...)
		}
	} else {
		// Get all nodes
		for _, node := range g.nodes {
			candidates = append(candidates, node)
		}
	}

	return candidates
}

// nodeMatchesPattern checks if a node matches a pattern
func (g *Graph) nodeMatchesPattern(node *Node, pattern NodePattern) bool {
	if len(pattern.Labels) == 0 {
		return true
	}

	// Check if node has all required labels
	for _, requiredLabel := range pattern.Labels {
		hasLabel := false
		for _, nodeLabel := range node.Labels {
			if nodeLabel == requiredLabel {
				hasLabel = true
				break
			}
		}
		if !hasLabel {
			return false
		}
	}

	return true
}

// filterMatches applies WHERE clause filters to matches
func (g *Graph) filterMatches(matches []Match, whereClause *WhereClause) []Match {
	filtered := []Match{}

	for _, match := range matches {
		if g.matchSatisfiesWhere(match, whereClause) {
			filtered = append(filtered, match)
		}
	}

	return filtered
}

// matchSatisfiesWhere checks if a match satisfies all WHERE conditions
func (g *Graph) matchSatisfiesWhere(match Match, whereClause *WhereClause) bool {
	for _, condition := range whereClause.Conditions {
		if !g.evaluateCondition(match, condition) {
			return false
		}
	}
	return true
}

// evaluateCondition evaluates a single WHERE condition
func (g *Graph) evaluateCondition(match Match, condition Condition) bool {
	// Get the entity (node or relationship)
	entity, ok := match[condition.Variable]
	if !ok {
		return false
	}

	// Get the property value
	var propValue interface{}
	if node, ok := entity.(*Node); ok {
		propValue = node.Properties[condition.Property]
	} else if rel, ok := entity.(*Relationship); ok {
		propValue = rel.Properties[condition.Property]
	} else {
		return false
	}

	if propValue == nil {
		return false
	}

	// Evaluate based on operator
	switch condition.Operator {
	case "=":
		return fmt.Sprint(propValue) == fmt.Sprint(condition.Value)
	case "!=":
		return fmt.Sprint(propValue) != fmt.Sprint(condition.Value)
	case "CONTAINS":
		propStr := strings.ToLower(fmt.Sprint(propValue))
		valueStr := strings.ToLower(fmt.Sprint(condition.Value))
		return strings.Contains(propStr, valueStr)
	case ">", "<", ">=", "<=":
		// Numeric comparison
		return g.compareNumeric(propValue, condition.Value, condition.Operator)
	default:
		return false
	}
}

// compareNumeric compares two values numerically
func (g *Graph) compareNumeric(a, b interface{}, operator string) bool {
	aFloat, aOk := toFloat(a)
	bFloat, bOk := toFloat(b)
	if !aOk || !bOk {
		return false
	}

	switch operator {
	case ">":
		return aFloat > bFloat
	case "<":
		return aFloat < bFloat
	case ">=":
		return aFloat >= bFloat
	case "<=":
		return aFloat <= bFloat
	default:
		return false
	}
}

// toFloat converts a value to float64
func toFloat(v interface{}) (float64, bool) {
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

// buildResult constructs the query result based on RETURN clause
func (g *Graph) buildResult(matches []Match, returnClause *ReturnClause) *QueryResult {
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	// Build column names
	for _, item := range returnClause.Items {
		if item.Property != "" {
			result.Columns = append(result.Columns, item.Variable+"."+item.Property)
		} else {
			result.Columns = append(result.Columns, item.Variable)
		}
	}

	// Build rows
	for _, match := range matches {
		row := map[string]interface{}{}

		for _, item := range returnClause.Items {
			entity, ok := match[item.Variable]
			if !ok {
				continue
			}

			columnName := item.Variable
			if item.Property != "" {
				columnName = item.Variable + "." + item.Property
			}

			if item.Property != "" {
				// Return specific property
				if node, ok := entity.(*Node); ok {
					row[columnName] = node.Properties[item.Property]
				} else if rel, ok := entity.(*Relationship); ok {
					row[columnName] = rel.Properties[item.Property]
				}
			} else {
				// Return whole entity
				row[columnName] = entity
			}
		}

		result.Rows = append(result.Rows, row)
	}

	return result
}

// executePathQuery executes a path-finding query
func (g *Graph) executePathQuery(query *Query) (*QueryResult, error) {
	pf := query.MatchPattern.PathFunction

	// Find all candidate start nodes
	startCandidates := g.getCandidateNodes(pf.StartPattern)
	endCandidates := g.getCandidateNodes(pf.EndPattern)

	// Apply WHERE filters to start/end nodes if specified
	var filteredStartNodes []*Node
	var filteredEndNodes []*Node

	if query.WhereClause != nil {
		// Split conditions by variable to filter start and end nodes separately
		for _, startNode := range startCandidates {
			match := Match{pf.StartPattern.Variable: startNode}
			satisfies := true
			for _, cond := range query.WhereClause.Conditions {
				if cond.Variable == pf.StartPattern.Variable {
					if !g.evaluateCondition(match, cond) {
						satisfies = false
						break
					}
				}
			}
			if satisfies {
				filteredStartNodes = append(filteredStartNodes, startNode)
			}
		}
		for _, endNode := range endCandidates {
			match := Match{pf.EndPattern.Variable: endNode}
			satisfies := true
			for _, cond := range query.WhereClause.Conditions {
				if cond.Variable == pf.EndPattern.Variable {
					if !g.evaluateCondition(match, cond) {
						satisfies = false
						break
					}
				}
			}
			if satisfies {
				filteredEndNodes = append(filteredEndNodes, endNode)
			}
		}
	} else {
		filteredStartNodes = startCandidates
		filteredEndNodes = endCandidates
	}

	// Find paths between filtered nodes
	var allPaths []*Path

	for _, startNode := range filteredStartNodes {
		for _, endNode := range filteredEndNodes {
			if startNode.ID == endNode.ID {
				continue // Skip same node
			}

			if pf.Function == "shortestpath" {
				// Use temporal path-finding if TimeClause is present
				queryTime := g.getQueryTime(query.TimeClause)
				path := g.ShortestPathAt(startNode.ID, endNode.ID, queryTime)
				if path != nil {
					allPaths = append(allPaths, path)
				}
			} else if pf.Function == "allshortestpaths" {
				maxDepth := pf.MaxDepth
				if maxDepth == 0 {
					maxDepth = 10 // Default max depth to prevent infinite searches
				}
				paths := g.AllPaths(startNode.ID, endNode.ID, maxDepth)
				allPaths = append(allPaths, paths...)
			}
		}
	}

	// Build result
	result := &QueryResult{
		Columns: []string{pf.Variable},
		Rows:    []map[string]interface{}{},
	}

	for _, path := range allPaths {
		row := map[string]interface{}{
			pf.Variable: path,
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// executeCreateQuery executes a CREATE query
func (g *Graph) executeCreateQuery(query *Query) (*QueryResult, error) {
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
		// Get from and to nodes
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

		// Set properties
		for key, value := range relSpec.Properties {
			if err := g.setRelationshipPropertyUnlocked(rel.ID, key, value); err != nil {
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
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	if query.ReturnClause != nil {
		// Return specified variables
		row := map[string]interface{}{}
		for _, item := range query.ReturnClause.Items {
			if val, ok := createdVars[item.Variable]; ok {
				if item.Property != "" {
					// Return property
					if node, ok := val.(*Node); ok {
						result.Columns = append(result.Columns, item.Variable+"."+item.Property)
						row[item.Variable+"."+item.Property] = node.Properties[item.Property]
					} else if rel, ok := val.(*Relationship); ok {
						result.Columns = append(result.Columns, item.Variable+"."+item.Property)
						row[item.Variable+"."+item.Property] = rel.Properties[item.Property]
					}
				} else {
					// Return whole entity
					result.Columns = append(result.Columns, item.Variable)
					row[item.Variable] = val
				}
			}
		}
		result.Rows = append(result.Rows, row)
	} else {
		// Return count of created entities
		result.Columns = []string{"created"}
		result.Rows = append(result.Rows, map[string]interface{}{
			"created": createdCount,
		})
	}

	return result, nil
}

// executeSetQuery executes a MATCH...SET query
func (g *Graph) executeSetQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Find matches
	matches := g.findMatches(query.MatchPattern, query.TimeClause)

	// Apply WHERE filters
	if query.WhereClause != nil {
		matches = g.filterMatches(matches, query.WhereClause)
	}

	// Apply SET updates
	updatedCount := 0
	for _, match := range matches {
		for _, update := range query.SetClause.Updates {
			entity, ok := match[update.Variable]
			if !ok {
				continue
			}

			if node, ok := entity.(*Node); ok {
				if err := g.setNodePropertyUnlocked(node.ID, update.Property, update.Value); err != nil {
					return nil, err
				}
				updatedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				if err := g.setRelationshipPropertyUnlocked(rel.ID, update.Property, update.Value); err != nil {
					return nil, err
				}
				updatedCount++
			}
		}
	}

	// Build result
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	if query.ReturnClause != nil {
		result = g.buildResult(matches, query.ReturnClause)
	} else {
		result.Columns = []string{"updated"}
		result.Rows = append(result.Rows, map[string]interface{}{
			"updated": updatedCount,
		})
	}

	return result, nil
}

// executeDeleteQuery executes a MATCH...DELETE query
func (g *Graph) executeDeleteQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Find matches
	matches := g.findMatches(query.MatchPattern, query.TimeClause)

	// Apply WHERE filters
	if query.WhereClause != nil {
		matches = g.filterMatches(matches, query.WhereClause)
	}

	// Delete entities
	deletedCount := 0
	for _, match := range matches {
		for _, varName := range query.DeleteClause.Variables {
			entity, ok := match[varName]
			if !ok {
				continue
			}

			if node, ok := entity.(*Node); ok {
				if query.DeleteClause.Detach {
					// Delete all relationships first
					rels := g.getRelationshipsForNodeUnlocked(node.ID)
					for _, rel := range rels {
						if err := g.deleteRelationshipUnlocked(rel.ID); err != nil {
							// Already deleted, ignore error
							continue
						}
					}
				}
				if err := g.deleteNodeUnlocked(node.ID); err != nil {
					return nil, err
				}
				deletedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				if err := g.deleteRelationshipUnlocked(rel.ID); err != nil {
					return nil, err
				}
				deletedCount++
			}
		}
	}

	// Build result
	result := &QueryResult{
		Columns: []string{"deleted"},
		Rows: []map[string]interface{}{
			{"deleted": deletedCount},
		},
	}

	return result, nil
}
