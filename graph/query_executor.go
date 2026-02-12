package graph

import (
	"fmt"
	"strings"
)

// ExecuteQuery executes a parsed query against the graph
func (g *Graph) ExecuteQuery(query *Query) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Find all matches for the pattern
	matches := g.findMatches(query.MatchPattern)

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
func (g *Graph) findMatches(pattern *MatchPattern) []Match {
	if len(pattern.Nodes) == 0 {
		return nil
	}

	matches := []Match{}

	// Start with the first node pattern
	firstNodePattern := pattern.Nodes[0]
	candidateNodes := g.getCandidateNodes(firstNodePattern)

	// For each candidate starting node, try to match the pattern
	for _, startNode := range candidateNodes {
		if !startNode.IsCurrentlyValid() {
			continue
		}

		// Initialize a new match
		match := Match{}
		if firstNodePattern.Variable != "" {
			match[firstNodePattern.Variable] = startNode
		}

		// Try to extend this match through the pattern
		g.extendMatch(match, pattern, 0, &matches)
	}

	return matches
}

// extendMatch recursively extends a partial match
func (g *Graph) extendMatch(currentMatch Match, pattern *MatchPattern, relIndex int, allMatches *[]Match) {
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
	rels := g.GetRelationshipsForNode(fromNode.ID)

	for _, rel := range rels {
		if !rel.IsCurrentlyValid() {
			continue
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
		if toNode == nil || !toNode.IsCurrentlyValid() {
			continue
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
		g.extendMatch(extendedMatch, pattern, relIndex+1, allMatches)
	}
}

// getCandidateNodes returns all nodes that match a node pattern
func (g *Graph) getCandidateNodes(pattern NodePattern) []*Node {
	candidates := []*Node{}

	if len(pattern.Labels) > 0 {
		// Get nodes by label
		for _, label := range pattern.Labels {
			nodes := g.GetNodesByLabel(label)
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
