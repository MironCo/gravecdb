package graph

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Query represents a parsed Cypher-like query
type Query struct {
	QueryType    string        // "MATCH", "CREATE", "DELETE"
	MatchPattern *MatchPattern
	CreateClause *CreateClause
	SetClause    *SetClause
	DeleteClause *DeleteClause
	WhereClause  *WhereClause
	ReturnClause *ReturnClause
	TimeClause   *TimeClause   // Optional temporal constraint
	IsPathQuery  bool          // true if this is a shortestPath() query
}

// TimeClause represents temporal query constraints
type TimeClause struct {
	Mode      string // "EARLIEST" or "TIMESTAMP"
	Timestamp int64  // Unix timestamp (only used when Mode == "TIMESTAMP")
}

// CreateClause represents a CREATE operation
type CreateClause struct {
	Nodes         []CreateNode
	Relationships []CreateRelationship
}

// CreateNode represents a node to create
type CreateNode struct {
	Variable   string
	Labels     []string
	Properties map[string]interface{}
}

// CreateRelationship represents a relationship to create
type CreateRelationship struct {
	Variable   string
	Type       string
	FromVar    string
	ToVar      string
	Properties map[string]interface{}
}

// SetClause represents property updates
type SetClause struct {
	Updates []PropertyUpdate
}

// PropertyUpdate represents a single property update
type PropertyUpdate struct {
	Variable string
	Property string
	Value    interface{}
}

// DeleteClause represents deletion operations
type DeleteClause struct {
	Variables []string // Variables to delete
	Detach    bool     // DETACH DELETE (also delete relationships)
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
//   CREATE (p:Person {name: "Alice", age: 28})
//   MATCH (p:Person) WHERE p.name = "Alice" SET p.age = 29
//   MATCH (p:Person) WHERE p.name = "Alice" DELETE p
func ParseQuery(queryStr string) (*Query, error) {
	queryStr = strings.TrimSpace(queryStr)
	queryLower := strings.ToLower(queryStr)

	query := &Query{}

	// Determine query type
	if strings.HasPrefix(queryLower, "create") {
		query.QueryType = "CREATE"
		return parseCreateQuery(queryStr)
	} else if strings.HasPrefix(queryLower, "match") {
		query.QueryType = "MATCH"
		return parseMatchQuery(queryStr)
	} else {
		return nil, fmt.Errorf("query must start with MATCH or CREATE")
	}
}

// parseMatchQuery parses a MATCH query (with optional SET/DELETE)
func parseMatchQuery(queryStr string) (*Query, error) {
	query := &Query{QueryType: "MATCH"}

	// Split into clauses
	matchRegex := regexp.MustCompile(`(?i)MATCH\s+(.+?)(?:\s+WHERE\s+|\s+AT\s+TIME\s+|\s+SET\s+|\s+DELETE\s+|\s+RETURN\s+|$)`)
	whereRegex := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+AT\s+TIME\s+|\s+SET\s+|\s+DELETE\s+|\s+RETURN\s+|$)`)
	timeRegex := regexp.MustCompile(`(?i)AT\s+TIME\s+(EARLIEST|(\d+))(?:\s+SET\s+|\s+DELETE\s+|\s+RETURN\s+|$)`)
	setRegex := regexp.MustCompile(`(?i)SET\s+(.+?)(?:\s+RETURN\s+|$)`)
	deleteRegex := regexp.MustCompile(`(?i)(DETACH\s+)?DELETE\s+(.+?)(?:\s+RETURN\s+|$)`)
	returnRegex := regexp.MustCompile(`(?i)RETURN\s+(.+)$`)

	matchMatch := matchRegex.FindStringSubmatch(queryStr)
	if matchMatch == nil {
		return nil, fmt.Errorf("query must start with MATCH clause")
	}

	// Check if this is a path query
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

	// Parse AT TIME clause (optional)
	timeMatch := timeRegex.FindStringSubmatch(queryStr)
	if timeMatch != nil {
		timeClause, err := parseTimeClause(timeMatch[1], timeMatch[2])
		if err != nil {
			return nil, fmt.Errorf("error parsing AT TIME: %w", err)
		}
		query.TimeClause = timeClause
	}

	// Parse SET clause (optional)
	setMatch := setRegex.FindStringSubmatch(queryStr)
	if setMatch != nil {
		setClause, err := parseSetClause(setMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing SET: %w", err)
		}
		query.SetClause = setClause
	}

	// Parse DELETE clause (optional)
	deleteMatch := deleteRegex.FindStringSubmatch(queryStr)
	if deleteMatch != nil {
		deleteClause, err := parseDeleteClause(deleteMatch[1] != "", deleteMatch[2])
		if err != nil {
			return nil, fmt.Errorf("error parsing DELETE: %w", err)
		}
		query.DeleteClause = deleteClause
	}

	// Parse RETURN clause (optional for SET/DELETE)
	returnMatch := returnRegex.FindStringSubmatch(queryStr)
	if returnMatch != nil {
		returnClause, err := parseReturnClause(returnMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing RETURN: %w", err)
		}
		query.ReturnClause = returnClause
	}

	return query, nil
}

// parseCreateQuery parses a CREATE query
func parseCreateQuery(queryStr string) (*Query, error) {
	query := &Query{QueryType: "CREATE"}

	// Extract CREATE clause
	createRegex := regexp.MustCompile(`(?i)CREATE\s+(.+?)(?:\s+RETURN\s+|$)`)
	returnRegex := regexp.MustCompile(`(?i)RETURN\s+(.+)$`)

	createMatch := createRegex.FindStringSubmatch(queryStr)
	if createMatch == nil {
		return nil, fmt.Errorf("invalid CREATE syntax")
	}

	createClause, err := parseCreateClause(createMatch[1])
	if err != nil {
		return nil, fmt.Errorf("error parsing CREATE: %w", err)
	}
	query.CreateClause = createClause

	// Parse RETURN clause (optional)
	returnMatch := returnRegex.FindStringSubmatch(queryStr)
	if returnMatch != nil {
		returnClause, err := parseReturnClause(returnMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing RETURN: %w", err)
		}
		query.ReturnClause = returnClause
	}

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

// parseCreateClause parses a CREATE clause
// Example: (p:Person {name: "Alice", age: 28})
// Example: (a:Person)-[:KNOWS]->(b:Person)
func parseCreateClause(createStr string) (*CreateClause, error) {
	createStr = strings.TrimSpace(createStr)
	cc := &CreateClause{
		Nodes:         []CreateNode{},
		Relationships: []CreateRelationship{},
	}

	// Parse nodes with properties: (var:Label {prop: value, ...})
	nodeRegex := regexp.MustCompile(`\(([a-zA-Z_][a-zA-Z0-9_]*)?(?::([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?:\{([^}]*)\})?\)`)
	nodeMatches := nodeRegex.FindAllStringSubmatch(createStr, -1)

	for _, match := range nodeMatches {
		node := CreateNode{
			Variable:   match[1],
			Labels:     []string{},
			Properties: make(map[string]interface{}),
		}
		if match[2] != "" {
			node.Labels = append(node.Labels, match[2])
		}
		if match[3] != "" {
			props, err := parseProperties(match[3])
			if err != nil {
				return nil, fmt.Errorf("error parsing properties: %w", err)
			}
			node.Properties = props
		}
		cc.Nodes = append(cc.Nodes, node)
	}

	// Parse relationships: -[:TYPE]->
	relRegex := regexp.MustCompile(`-\[:([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\{([^}]*)\})?\]->`)
	relMatches := relRegex.FindAllStringSubmatch(createStr, -1)

	for i, match := range relMatches {
		rel := CreateRelationship{
			Type:       match[1],
			Properties: make(map[string]interface{}),
		}
		if i < len(cc.Nodes)-1 {
			rel.FromVar = cc.Nodes[i].Variable
			rel.ToVar = cc.Nodes[i+1].Variable
		}
		if match[2] != "" {
			props, err := parseProperties(match[2])
			if err != nil {
				return nil, fmt.Errorf("error parsing relationship properties: %w", err)
			}
			rel.Properties = props
		}
		cc.Relationships = append(cc.Relationships, rel)
	}

	return cc, nil
}

// parseProperties parses property map from string
// Example: name: "Alice", age: 28
func parseProperties(propsStr string) (map[string]interface{}, error) {
	props := make(map[string]interface{})
	propsStr = strings.TrimSpace(propsStr)
	if propsStr == "" {
		return props, nil
	}

	// Split by comma (simple version, doesn't handle nested structures)
	parts := strings.Split(propsStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Split by colon
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		valueStr := strings.TrimSpace(kv[1])

		// Parse value
		var value interface{}
		if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
			value = strings.Trim(valueStr, "\"")
		} else if num, err := strconv.Atoi(valueStr); err == nil {
			value = num
		} else if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
			value = num
		} else if valueStr == "true" || valueStr == "false" {
			value = valueStr == "true"
		} else {
			value = valueStr
		}

		props[key] = value
	}

	return props, nil
}

// parseSetClause parses a SET clause
// Example: p.age = 29, p.name = "Bob"
func parseSetClause(setStr string) (*SetClause, error) {
	setStr = strings.TrimSpace(setStr)
	sc := &SetClause{
		Updates: []PropertyUpdate{},
	}

	// Split by comma
	parts := strings.Split(setStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Parse: variable.property = value
		updateRegex := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)`)
		match := updateRegex.FindStringSubmatch(part)
		if match == nil {
			return nil, fmt.Errorf("invalid SET syntax: %s", part)
		}

		variable := match[1]
		property := match[2]
		valueStr := strings.TrimSpace(match[3])

		// Parse value
		var value interface{}
		if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
			value = strings.Trim(valueStr, "\"")
		} else if num, err := strconv.Atoi(valueStr); err == nil {
			value = num
		} else if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
			value = num
		} else if valueStr == "true" || valueStr == "false" {
			value = valueStr == "true"
		} else {
			value = valueStr
		}

		sc.Updates = append(sc.Updates, PropertyUpdate{
			Variable: variable,
			Property: property,
			Value:    value,
		})
	}

	return sc, nil
}

// parseDeleteClause parses a DELETE clause
// Example: p, r
func parseDeleteClause(detach bool, deleteStr string) (*DeleteClause, error) {
	deleteStr = strings.TrimSpace(deleteStr)
	dc := &DeleteClause{
		Variables: []string{},
		Detach:    detach,
	}

	// Split by comma
	parts := strings.Split(deleteStr, ",")
	for _, part := range parts {
		variable := strings.TrimSpace(part)
		if variable != "" {
			dc.Variables = append(dc.Variables, variable)
		}
	}

	return dc, nil
}

// parseTimeClause parses an AT TIME clause
// Example: AT TIME EARLIEST or AT TIME 1609459200
func parseTimeClause(modeOrTimestamp string, timestamp string) (*TimeClause, error) {
	tc := &TimeClause{}

	// Check if mode is EARLIEST
	if strings.ToUpper(modeOrTimestamp) == "EARLIEST" {
		tc.Mode = "EARLIEST"
		tc.Timestamp = 0
		return tc, nil
	}

	// Otherwise, it's a timestamp (captured in first group when not EARLIEST)
	tc.Mode = "TIMESTAMP"
	ts, err := strconv.ParseInt(modeOrTimestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %s", modeOrTimestamp)
	}
	tc.Timestamp = ts
	return tc, nil
}
