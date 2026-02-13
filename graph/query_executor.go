package graph

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Embedder interface for generating embeddings
type Embedder interface {
	Embed(text string) ([]float32, error)
}

// ExecuteQuery executes a parsed query against the graph
func (g *Graph) ExecuteQuery(query *Query) (*QueryResult, error) {
	return g.ExecuteQueryWithEmbedder(query, nil)
}

// ExecuteQueryWithEmbedder executes a query with an optional embedder for EMBED/SIMILAR TO clauses
func (g *Graph) ExecuteQueryWithEmbedder(query *Query, embedder Embedder) (*QueryResult, error) {
	switch query.QueryType {
	case "CREATE":
		return g.executeCreateQuery(query)
	case "MATCH":
		// Handle MATCH with CREATE (e.g., MATCH...CREATE pattern)
		if query.CreateClause != nil {
			return g.executeMatchCreateQuery(query)
		}
		// Handle MATCH with SET or DELETE
		if query.SetClause != nil {
			return g.executeSetQuery(query)
		}
		if query.DeleteClause != nil {
			return g.executeDeleteQuery(query)
		}
		// Handle SIMILAR TO semantic search
		if query.SimilarToClause != nil {
			return g.executeSimilarToQuery(query, embedder)
		}
		// Handle EMBED clause
		if query.EmbedClause != nil {
			return g.executeEmbedQuery(query, embedder)
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
	// Check if node has all required labels
	if len(pattern.Labels) > 0 {
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
	}

	// Check inline property constraints
	for key, expectedValue := range pattern.Properties {
		actualValue, exists := node.Properties[key]
		if !exists {
			return false
		}
		// Simple equality check
		if actualValue != expectedValue {
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
// Handles aggregation, DISTINCT, ORDER BY, SKIP, and LIMIT
func (g *Graph) buildResult(matches []Match, returnClause *ReturnClause) *QueryResult {
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	// If no return clause, return empty result
	if returnClause == nil {
		return result
	}

	// Check if we have any aggregation functions
	hasAggregation := false
	for _, item := range returnClause.Items {
		if item.Aggregation != "" {
			hasAggregation = true
			break
		}
	}

	// Build column names
	for _, item := range returnClause.Items {
		colName := getColumnName(item)
		result.Columns = append(result.Columns, colName)
	}

	if hasAggregation {
		// Handle aggregation queries
		result.Rows = g.buildAggregatedRows(matches, returnClause)
	} else {
		// Build regular rows
		for _, match := range matches {
			row := g.buildRowFromMatch(match, returnClause.Items)
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply DISTINCT
	if returnClause.Distinct {
		result.Rows = applyDistinct(result.Rows)
	}

	// Apply ORDER BY
	if len(returnClause.OrderBy) > 0 {
		applyOrderBy(result.Rows, returnClause.OrderBy)
	}

	// Apply SKIP
	if returnClause.Skip > 0 && returnClause.Skip < len(result.Rows) {
		result.Rows = result.Rows[returnClause.Skip:]
	} else if returnClause.Skip >= len(result.Rows) {
		result.Rows = []map[string]interface{}{}
	}

	// Apply LIMIT
	if returnClause.Limit > 0 && returnClause.Limit < len(result.Rows) {
		result.Rows = result.Rows[:returnClause.Limit]
	}

	return result
}

// getColumnName returns the column name for a return item
func getColumnName(item ReturnItem) string {
	if item.Alias != "" {
		return item.Alias
	}
	if item.Aggregation != "" {
		if item.Property != "" {
			return fmt.Sprintf("%s(%s.%s)", item.Aggregation, item.Variable, item.Property)
		}
		return fmt.Sprintf("%s(%s)", item.Aggregation, item.Variable)
	}
	if item.Property != "" {
		return item.Variable + "." + item.Property
	}
	return item.Variable
}

// buildRowFromMatch builds a single row from a match
func (g *Graph) buildRowFromMatch(match Match, items []ReturnItem) map[string]interface{} {
	row := map[string]interface{}{}

	for _, item := range items {
		colName := getColumnName(item)
		entity, ok := match[item.Variable]
		if !ok {
			row[colName] = nil
			continue
		}

		if item.Property != "" {
			// Return specific property
			if node, ok := entity.(*Node); ok {
				row[colName] = node.Properties[item.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				row[colName] = rel.Properties[item.Property]
			}
		} else {
			// Return whole entity
			row[colName] = entity
		}
	}

	return row
}

// buildAggregatedRows handles aggregation functions like COUNT, SUM, AVG, etc.
func (g *Graph) buildAggregatedRows(matches []Match, returnClause *ReturnClause) []map[string]interface{} {
	// Find grouping columns (non-aggregated items)
	var groupByItems []ReturnItem
	var aggItems []ReturnItem

	for _, item := range returnClause.Items {
		if item.Aggregation != "" {
			aggItems = append(aggItems, item)
		} else {
			groupByItems = append(groupByItems, item)
		}
	}

	// If no grouping columns, treat all matches as one group
	if len(groupByItems) == 0 {
		row := map[string]interface{}{}
		for _, item := range aggItems {
			colName := getColumnName(item)
			row[colName] = g.computeAggregation(matches, item)
		}
		return []map[string]interface{}{row}
	}

	// Group matches by grouping columns
	groups := make(map[string][]Match)
	groupKeys := []string{} // Track order of keys

	for _, match := range matches {
		key := g.buildGroupKey(match, groupByItems)
		if _, exists := groups[key]; !exists {
			groupKeys = append(groupKeys, key)
		}
		groups[key] = append(groups[key], match)
	}

	// Build result rows for each group
	var rows []map[string]interface{}
	for _, key := range groupKeys {
		groupMatches := groups[key]
		if len(groupMatches) == 0 {
			continue
		}

		row := map[string]interface{}{}

		// Add grouping column values (from first match in group)
		firstMatch := groupMatches[0]
		for _, item := range groupByItems {
			colName := getColumnName(item)
			entity, ok := firstMatch[item.Variable]
			if !ok {
				row[colName] = nil
				continue
			}
			if item.Property != "" {
				if node, ok := entity.(*Node); ok {
					row[colName] = node.Properties[item.Property]
				} else if rel, ok := entity.(*Relationship); ok {
					row[colName] = rel.Properties[item.Property]
				}
			} else {
				row[colName] = entity
			}
		}

		// Compute aggregations for this group
		for _, item := range aggItems {
			colName := getColumnName(item)
			row[colName] = g.computeAggregation(groupMatches, item)
		}

		rows = append(rows, row)
	}

	return rows
}

// buildGroupKey creates a string key for grouping matches
func (g *Graph) buildGroupKey(match Match, groupByItems []ReturnItem) string {
	var parts []string
	for _, item := range groupByItems {
		entity, ok := match[item.Variable]
		if !ok {
			parts = append(parts, "<nil>")
			continue
		}
		if item.Property != "" {
			var val interface{}
			if node, ok := entity.(*Node); ok {
				val = node.Properties[item.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				val = rel.Properties[item.Property]
			}
			parts = append(parts, fmt.Sprintf("%v", val))
		} else {
			// Use entity ID for grouping
			if node, ok := entity.(*Node); ok {
				parts = append(parts, node.ID)
			} else if rel, ok := entity.(*Relationship); ok {
				parts = append(parts, rel.ID)
			}
		}
	}
	return strings.Join(parts, "|")
}

// computeAggregation computes an aggregation function over matches
func (g *Graph) computeAggregation(matches []Match, item ReturnItem) interface{} {
	switch strings.ToUpper(item.Aggregation) {
	case "COUNT":
		if item.Variable == "*" {
			return len(matches)
		}
		// Count non-null values
		count := 0
		for _, match := range matches {
			if _, ok := match[item.Variable]; ok {
				count++
			}
		}
		return count

	case "SUM":
		var sum float64
		for _, match := range matches {
			val := g.getNumericValue(match, item)
			if val != nil {
				sum += *val
			}
		}
		return sum

	case "AVG":
		var sum float64
		count := 0
		for _, match := range matches {
			val := g.getNumericValue(match, item)
			if val != nil {
				sum += *val
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "MIN":
		var min *float64
		for _, match := range matches {
			val := g.getNumericValue(match, item)
			if val != nil {
				if min == nil || *val < *min {
					min = val
				}
			}
		}
		if min == nil {
			return nil
		}
		return *min

	case "MAX":
		var max *float64
		for _, match := range matches {
			val := g.getNumericValue(match, item)
			if val != nil {
				if max == nil || *val > *max {
					max = val
				}
			}
		}
		if max == nil {
			return nil
		}
		return *max

	case "COLLECT":
		var collected []interface{}
		for _, match := range matches {
			entity, ok := match[item.Variable]
			if !ok {
				continue
			}
			if item.Property != "" {
				if node, ok := entity.(*Node); ok {
					collected = append(collected, node.Properties[item.Property])
				} else if rel, ok := entity.(*Relationship); ok {
					collected = append(collected, rel.Properties[item.Property])
				}
			} else {
				collected = append(collected, entity)
			}
		}
		return collected
	}

	return nil
}

// getNumericValue extracts a numeric value from a match for aggregation
func (g *Graph) getNumericValue(match Match, item ReturnItem) *float64 {
	entity, ok := match[item.Variable]
	if !ok {
		return nil
	}

	var val interface{}
	if item.Property != "" {
		if node, ok := entity.(*Node); ok {
			val = node.Properties[item.Property]
		} else if rel, ok := entity.(*Relationship); ok {
			val = rel.Properties[item.Property]
		}
	}

	if val == nil {
		return nil
	}

	var f float64
	switch v := val.(type) {
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case float64:
		f = v
	case float32:
		f = float64(v)
	default:
		return nil
	}
	return &f
}

// applyDistinct removes duplicate rows
func applyDistinct(rows []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]bool)
	var result []map[string]interface{}

	for _, row := range rows {
		key := rowToKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// rowToKey creates a string key from a row for deduplication
func rowToKey(row map[string]interface{}) string {
	// Sort keys for consistent ordering
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		v := row[k]
		if node, ok := v.(*Node); ok {
			parts = append(parts, fmt.Sprintf("%s=node:%s", k, node.ID))
		} else if rel, ok := v.(*Relationship); ok {
			parts = append(parts, fmt.Sprintf("%s=rel:%s", k, rel.ID))
		} else {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
	}
	return strings.Join(parts, "|")
}

// applyOrderBy sorts rows by the specified order items
func applyOrderBy(rows []map[string]interface{}, orderBy []OrderItem) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, order := range orderBy {
			colName := order.Variable
			if order.Property != "" {
				colName = order.Variable + "." + order.Property
			}

			vi := getOrderValue(rows[i], colName, order)
			vj := getOrderValue(rows[j], colName, order)

			cmp := compareValues(vi, vj)
			if cmp != 0 {
				if order.Descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// getOrderValue gets a value for ordering, handling entity properties
func getOrderValue(row map[string]interface{}, colName string, order OrderItem) interface{} {
	// First try direct column lookup
	if v, ok := row[colName]; ok {
		return v
	}

	// Try getting property from entity
	if order.Property != "" {
		if entity, ok := row[order.Variable]; ok {
			if node, ok := entity.(*Node); ok {
				return node.Properties[order.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				return rel.Properties[order.Property]
			}
		}
	}

	return nil
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

// executeMatchCreateQuery executes a MATCH...CREATE query
func (g *Graph) executeMatchCreateQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Find matches for the MATCH pattern
	matches := g.findMatches(query.MatchPattern, query.TimeClause)

	// Apply WHERE filters
	if query.WhereClause != nil {
		matches = g.filterMatches(matches, query.WhereClause)
	}

	// For each match, create the entities specified in CREATE clause
	cc := query.CreateClause
	createdCount := 0
	allCreatedVars := []map[string]interface{}{}

	for _, match := range matches {
		createdVars := make(map[string]interface{})

		// Copy matched variables into createdVars so they can be referenced
		for varName, entity := range match {
			createdVars[varName] = entity
		}

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
			// Get from and to nodes (could be matched or newly created)
			var fromNode, toNode *Node

			if fromEntity, ok := createdVars[relSpec.FromVar]; ok {
				fromNode, _ = fromEntity.(*Node)
			}
			if toEntity, ok := createdVars[relSpec.ToVar]; ok {
				toNode, _ = toEntity.(*Node)
			}

			if fromNode == nil || toNode == nil {
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

		allCreatedVars = append(allCreatedVars, createdVars)
	}

	// Build result
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	if query.ReturnClause != nil {
		// Return specified variables from all created entities
		for _, createdVars := range allCreatedVars {
			row := map[string]interface{}{}
			for _, item := range query.ReturnClause.Items {
				if len(result.Columns) < len(query.ReturnClause.Items) {
					if item.Property != "" {
						result.Columns = append(result.Columns, item.Variable+"."+item.Property)
					} else {
						result.Columns = append(result.Columns, item.Variable)
					}
				}

				if val, ok := createdVars[item.Variable]; ok {
					if item.Property != "" {
						// Return property
						if node, ok := val.(*Node); ok {
							row[item.Variable+"."+item.Property] = node.Properties[item.Property]
						} else if rel, ok := val.(*Relationship); ok {
							row[item.Variable+"."+item.Property] = rel.Properties[item.Property]
						}
					} else {
						// Return whole entity
						row[item.Variable] = val
					}
				}
			}
			if len(row) > 0 {
				result.Rows = append(result.Rows, row)
			}
		}
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

// executeEmbedQuery executes a MATCH...EMBED query to generate embeddings for matched nodes
func (g *Graph) executeEmbedQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	if embedder == nil {
		return nil, fmt.Errorf("embedder required for EMBED clause")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Find matches
	matches := g.findMatches(query.MatchPattern, query.TimeClause)

	// Apply WHERE filters
	if query.WhereClause != nil {
		matches = g.filterMatches(matches, query.WhereClause)
	}

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

		// Store embedding
		g.embeddings.Add(node.ID, vector, "embedder")
		embeddedCount++
	}

	// Build result
	result := &QueryResult{
		Columns: []string{"embedded"},
		Rows: []map[string]interface{}{
			{"embedded": embeddedCount},
		},
	}

	return result, nil
}

// generateAutoEmbedText generates embedding text from node labels and properties
func (g *Graph) generateAutoEmbedText(node *Node) string {
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

// executeSimilarToQuery executes a MATCH...SIMILAR TO semantic search query
func (g *Graph) executeSimilarToQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	if embedder == nil {
		return nil, fmt.Errorf("embedder required for SIMILAR TO clause")
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	stc := query.SimilarToClause

	// Generate embedding for the query text
	queryVector, err := embedder.Embed(stc.QueryText)
	if err != nil {
		return nil, fmt.Errorf("failed to embed query text: %w", err)
	}

	// Get query time
	queryTime := g.getQueryTime(query.TimeClause)

	// Find candidate nodes from MATCH pattern
	candidateIDs := make(map[string]bool)
	if len(query.MatchPattern.Nodes) > 0 {
		candidates := g.getCandidateNodes(query.MatchPattern.Nodes[0])
		for _, node := range candidates {
			if queryTime != nil {
				if node.IsValidAt(*queryTime) {
					candidateIDs[node.ID] = true
				}
			} else {
				if node.IsCurrentlyValid() {
					candidateIDs[node.ID] = true
				}
			}
		}

		// Apply WHERE filters if present
		if query.WhereClause != nil {
			filteredIDs := make(map[string]bool)
			for _, node := range candidates {
				if !candidateIDs[node.ID] {
					continue
				}
				match := Match{query.MatchPattern.Nodes[0].Variable: node}
				if g.matchSatisfiesWhere(match, query.WhereClause) {
					filteredIDs[node.ID] = true
				}
			}
			candidateIDs = filteredIDs
		}
	}

	// Search embeddings
	var asOf time.Time
	if queryTime != nil {
		asOf = *queryTime
	} else {
		asOf = time.Now()
	}

	limit := stc.Limit
	if limit == 0 {
		limit = 100 // Default limit
	}

	searchResults := g.embeddings.Search(queryVector, limit, asOf, candidateIDs)

	// Filter by threshold and build results
	result := &QueryResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}

	// Determine columns from RETURN clause
	if query.ReturnClause != nil {
		for _, item := range query.ReturnClause.Items {
			if item.Property != "" {
				result.Columns = append(result.Columns, item.Variable+"."+item.Property)
			} else {
				result.Columns = append(result.Columns, item.Variable)
			}
		}
		// Add similarity score column
		result.Columns = append(result.Columns, "similarity")
	} else {
		result.Columns = []string{"node", "similarity"}
	}

	for _, sr := range searchResults {
		// Apply threshold filter
		if stc.Threshold > 0 && sr.Similarity < stc.Threshold {
			continue
		}

		node := g.getNodeByID(sr.NodeID)
		if node == nil {
			continue
		}

		row := map[string]interface{}{}

		if query.ReturnClause != nil {
			for _, item := range query.ReturnClause.Items {
				columnName := item.Variable
				if item.Property != "" {
					columnName = item.Variable + "." + item.Property
					row[columnName] = node.Properties[item.Property]
				} else {
					row[columnName] = node
				}
			}
		} else {
			row["node"] = node
		}
		row["similarity"] = sr.Similarity

		result.Rows = append(result.Rows, row)
	}

	return result, nil
}
