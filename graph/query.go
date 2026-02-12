package graph

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Query represents a parsed Cypher-like query
type Query struct {
	MatchPattern *MatchPattern
	WhereClause  *WhereClause
	ReturnClause *ReturnClause
	IsPathQuery  bool // true if this is a shortestPath() query
}

// MatchPattern represents a graph pattern to match
// Example: (a:Person)-[:KNOWS]->(b:Person)
type MatchPattern struct {
	Nodes         []NodePattern
	Relationships []RelPattern
	PathFunction  *PathFunction // for shortestPath() queries
}

// PathFunction represents a path-finding function call
type PathFunction struct {
	Function     string       // "shortestPath" or "allShortestPaths"
	Variable     string       // variable name for the path (e.g., "path" in "path = shortestPath(...)")
	StartPattern NodePattern  // starting node pattern
	EndPattern   NodePattern  // ending node pattern
	RelTypes     []string     // relationship types filter (empty = any type)
	MaxDepth     int          // max depth for variable-length paths (0 = unlimited)
}

// NodePattern represents a node in the pattern
type NodePattern struct {
	Variable string   // e.g., "a"
	Labels   []string // e.g., ["Person"]
}

// RelPattern represents a relationship in the pattern
type RelPattern struct {
	Variable  string   // e.g., "r"
	Types     []string // e.g., ["KNOWS", "FRIENDS_WITH"]
	FromIndex int      // Index in Nodes array
	ToIndex   int      // Index in Nodes array
	Direction string   // "->", "<-", or "-" (bidirectional)
}

// WhereClause represents filtering conditions
type WhereClause struct {
	Conditions []Condition
}

// Condition represents a single filter condition
type Condition struct {
	Variable string      // e.g., "a"
	Property string      // e.g., "name"
	Operator string      // e.g., "=", "!=", ">", "<", "CONTAINS"
	Value    interface{} // the value to compare against
}

// ReturnClause specifies what to return
type ReturnClause struct {
	Items []ReturnItem
}

// ReturnItem represents a single return item
type ReturnItem struct {
	Variable string // e.g., "a"
	Property string // e.g., "name" (empty string means return whole node)
}

// QueryResult represents the result of executing a query
type QueryResult struct {
	Columns []string
	Rows    []map[string]interface{}
}

// ParseQuery parses a Cypher-like query string
// Supports queries like:
//   MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN a, b
//   MATCH (p:Person) WHERE p.age > 25 RETURN p.name
//   MATCH (a)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name
//   MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "David" RETURN path
func ParseQuery(queryStr string) (*Query, error) {
	queryStr = strings.TrimSpace(queryStr)

	// Split into clauses
	matchRegex := regexp.MustCompile(`(?i)MATCH\s+(.+?)(?:\s+WHERE\s+|\s+RETURN\s+|$)`)
	whereRegex := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+RETURN\s+|$)`)
	returnRegex := regexp.MustCompile(`(?i)RETURN\s+(.+)$`)

	matchMatch := matchRegex.FindStringSubmatch(queryStr)
	if matchMatch == nil {
		return nil, fmt.Errorf("query must start with MATCH clause")
	}

	query := &Query{}

	// Check if this is a path query (contains shortestPath or allShortestPaths)
	matchStr := matchMatch[1]
	if strings.Contains(strings.ToLower(matchStr), "shortestpath") ||
	   strings.Contains(strings.ToLower(matchStr), "allshortestpaths") {
		query.IsPathQuery = true
	}

	// Parse MATCH clause
	matchPattern, err := parseMatchPattern(matchMatch[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing MATCH: %w", err)
	}
	query.MatchPattern = matchPattern

	// Parse WHERE clause (optional)
	whereMatch := whereRegex.FindStringSubmatch(queryStr)
	if whereMatch != nil {
		whereClause, err := parseWhereClause(whereMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing WHERE: %w", err)
		}
		query.WhereClause = whereClause
	}

	// Parse RETURN clause
	returnMatch := returnRegex.FindStringSubmatch(queryStr)
	if returnMatch == nil {
		return nil, fmt.Errorf("query must have RETURN clause")
	}
	returnClause, err := parseReturnClause(returnMatch[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing RETURN: %w", err)
	}
	query.ReturnClause = returnClause

	return query, nil
}

// parseMatchPattern parses the MATCH pattern
// Example: (a:Person)-[:KNOWS]->(b:Person)
// Example: path = shortestPath((a:Person)-[*]-(b:Person))
func parseMatchPattern(pattern string) (*MatchPattern, error) {
	pattern = strings.TrimSpace(pattern)

	mp := &MatchPattern{
		Nodes:         []NodePattern{},
		Relationships: []RelPattern{},
	}

	// Check if this is a path function call
	pathFuncRegex := regexp.MustCompile(`(?i)([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(shortestPath|allShortestPaths)\s*\(\s*(.+)\s*\)`)
	pathFuncMatch := pathFuncRegex.FindStringSubmatch(pattern)

	if pathFuncMatch != nil {
		// Parse path function
		pathFunc, err := parsePathFunction(pathFuncMatch[1], pathFuncMatch[2], pathFuncMatch[3])
		if err != nil {
			return nil, err
		}
		mp.PathFunction = pathFunc
		return mp, nil
	}

	// Simple pattern parser - supports basic node-relationship-node patterns
	// Pattern: (node)-[relationship]->(node) or (node)<-[relationship]-(node) or (node)-[relationship]-(node)

	// Find all node patterns: (variable:Label)
	nodeRegex := regexp.MustCompile(`\(([a-zA-Z_][a-zA-Z0-9_]*)?(?::([a-zA-Z_][a-zA-Z0-9_]*))?\)`)
	nodeMatches := nodeRegex.FindAllStringSubmatch(pattern, -1)

	for _, match := range nodeMatches {
		np := NodePattern{
			Variable: match[1],
			Labels:   []string{},
		}
		if match[2] != "" {
			np.Labels = append(np.Labels, match[2])
		}
		mp.Nodes = append(mp.Nodes, np)
	}

	// Find relationships: -[:TYPE]-> or <-[:TYPE]- or -[:TYPE]-
	relRegex := regexp.MustCompile(`(<)?-\[(?::([a-zA-Z_][a-zA-Z0-9_|]*))?(?:\*(\d+)\.\.(\d+))?\]->(?)`)
	relMatches := relRegex.FindAllStringSubmatch(pattern, -1)

	if len(relMatches) == 0 {
		// Try simpler relationship pattern without variable
		relRegex = regexp.MustCompile(`(<)?-\[:([a-zA-Z_][a-zA-Z0-9_|]*)\]->(?)`)
		relMatches = relRegex.FindAllStringSubmatch(pattern, -1)
	}

	for i, match := range relMatches {
		direction := "->"
		if match[1] == "<" {
			direction = "<-"
		}

		relType := match[2]
		types := []string{}
		if relType != "" {
			// Handle multiple types with | separator
			types = strings.Split(relType, "|")
		}

		rp := RelPattern{
			Types:     types,
			FromIndex: i,
			ToIndex:   i + 1,
			Direction: direction,
		}

		if len(mp.Nodes) > rp.ToIndex {
			mp.Relationships = append(mp.Relationships, rp)
		}
	}

	if len(mp.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes found in pattern")
	}

	return mp, nil
}

// parsePathFunction parses a path function like shortestPath((a:Person)-[*]-(b:Person))
func parsePathFunction(variable, function, innerPattern string) (*PathFunction, error) {
	innerPattern = strings.TrimSpace(innerPattern)

	pf := &PathFunction{
		Variable: variable,
		Function: strings.ToLower(function),
	}

	// Parse the inner pattern: (start)-[*|*1..5|:TYPE*]-(end)
	// Extract start and end node patterns
	nodeRegex := regexp.MustCompile(`\(([a-zA-Z_][a-zA-Z0-9_]*)?(?::([a-zA-Z_][a-zA-Z0-9_]*))?\)`)
	nodeMatches := nodeRegex.FindAllStringSubmatch(innerPattern, -1)

	if len(nodeMatches) < 2 {
		return nil, fmt.Errorf("path function requires start and end nodes")
	}

	// Parse start node
	pf.StartPattern = NodePattern{
		Variable: nodeMatches[0][1],
		Labels:   []string{},
	}
	if nodeMatches[0][2] != "" {
		pf.StartPattern.Labels = append(pf.StartPattern.Labels, nodeMatches[0][2])
	}

	// Parse end node
	pf.EndPattern = NodePattern{
		Variable: nodeMatches[1][1],
		Labels:   []string{},
	}
	if nodeMatches[1][2] != "" {
		pf.EndPattern.Labels = append(pf.EndPattern.Labels, nodeMatches[1][2])
	}

	// Parse variable-length relationship: -[*]-, -[*1..5]-, -[:TYPE*]-, etc.
	varLenRelRegex := regexp.MustCompile(`-\[(?::([a-zA-Z_][a-zA-Z0-9_|]*))?\*(?:(\d+)\.\.(\d+))?\]-`)
	varLenMatch := varLenRelRegex.FindStringSubmatch(innerPattern)

	if varLenMatch != nil {
		// Extract relationship types
		if varLenMatch[1] != "" {
			pf.RelTypes = strings.Split(varLenMatch[1], "|")
		}

		// Extract max depth (if specified)
		if varLenMatch[3] != "" {
			maxDepth, err := strconv.Atoi(varLenMatch[3])
			if err == nil {
				pf.MaxDepth = maxDepth
			}
		}
	}

	return pf, nil
}

// parseWhereClause parses the WHERE conditions
// Example: a.name = "Alice" AND a.age > 25
func parseWhereClause(whereStr string) (*WhereClause, error) {
	whereStr = strings.TrimSpace(whereStr)

	// For now, support simple conditions: variable.property operator value
	// Split by AND (ignore OR for now)
	parts := strings.Split(whereStr, " AND ")

	wc := &WhereClause{
		Conditions: []Condition{},
	}

	conditionRegex := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|>|<|>=|<=|CONTAINS)\s*(.+)`)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		match := conditionRegex.FindStringSubmatch(part)
		if match == nil {
			return nil, fmt.Errorf("invalid WHERE condition: %s", part)
		}

		variable := match[1]
		property := match[2]
		operator := match[3]
		valueStr := strings.TrimSpace(match[4])

		// Parse value
		var value interface{}
		if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
			// String value
			value = strings.Trim(valueStr, "\"")
		} else if num, err := strconv.Atoi(valueStr); err == nil {
			// Integer value
			value = num
		} else if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
			// Float value
			value = num
		} else if valueStr == "true" || valueStr == "false" {
			// Boolean value
			value = valueStr == "true"
		} else {
			value = valueStr
		}

		wc.Conditions = append(wc.Conditions, Condition{
			Variable: variable,
			Property: property,
			Operator: operator,
			Value:    value,
		})
	}

	return wc, nil
}

// parseReturnClause parses the RETURN items
// Example: a, b.name, c
func parseReturnClause(returnStr string) (*ReturnClause, error) {
	returnStr = strings.TrimSpace(returnStr)

	parts := strings.Split(returnStr, ",")
	rc := &ReturnClause{
		Items: []ReturnItem{},
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check if it's variable.property or just variable
		if strings.Contains(part, ".") {
			dotParts := strings.Split(part, ".")
			if len(dotParts) != 2 {
				return nil, fmt.Errorf("invalid return item: %s", part)
			}
			rc.Items = append(rc.Items, ReturnItem{
				Variable: dotParts[0],
				Property: dotParts[1],
			})
		} else {
			rc.Items = append(rc.Items, ReturnItem{
				Variable: part,
				Property: "",
			})
		}
	}

	return rc, nil
}
