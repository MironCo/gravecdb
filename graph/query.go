package graph

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/MironCo/gravecdb/graph/cypher"
)

// Query represents a parsed Cypher-like query
type Query struct {
	QueryType       string           // "MATCH", "CREATE", "DELETE"
	MatchPattern    *MatchPattern
	CreateClause    *CreateClause
	SetClause       *SetClause
	DeleteClause    *DeleteClause
	WhereClause     *WhereClause
	ReturnClause    *ReturnClause
	TimeClause      *TimeClause      // Optional temporal constraint
	EmbedClause     *EmbedClause     // Optional embedding generation
	SimilarToClause *SimilarToClause // Optional semantic search
	IsPathQuery     bool             // true if this is a shortestPath() query
}

// EmbedClause represents an EMBED operation for generating embeddings
type EmbedClause struct {
	Variable string    // The node variable to embed (e.g., "p" in "EMBED p")
	Mode     string    // "AUTO", "TEXT", "PROPERTY"
	Text     string    // Literal text (when Mode == "TEXT")
	Property string    // Property name (when Mode == "PROPERTY")
}

// SimilarToClause represents a SIMILAR TO semantic search
type SimilarToClause struct {
	Variable   string  // The node variable to search (e.g., "p" in "(p:Person)")
	QueryText  string  // The search query text
	Limit      int     // Max results (0 = no limit)
	Threshold  float32 // Min similarity threshold (0 = no threshold)
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
	Variable   string                 // e.g., "a"
	Labels     []string               // e.g., ["Person"]
	Properties map[string]interface{} // inline property constraints e.g., {name: "Alice"}
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
	Items    []ReturnItem
	Distinct bool        // RETURN DISTINCT
	OrderBy  []OrderItem // ORDER BY clause
	Skip     int         // SKIP n
	Limit    int         // LIMIT n (0 = no limit)
}

// ReturnItem represents a single return item
type ReturnItem struct {
	Variable    string // e.g., "a"
	Property    string // e.g., "name" (empty string means return whole node)
	Aggregation string // e.g., "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT" (empty = no aggregation)
	Alias       string // AS alias
}

// OrderItem represents an ORDER BY item
type OrderItem struct {
	Variable   string
	Property   string
	Descending bool // true for DESC, false for ASC
}

// QueryResult represents the result of executing a query
type QueryResult struct {
	Columns []string
	Rows    []map[string]interface{}
}

// UseNewParser controls whether to use the new Cypher parser
// Set to true to use the new recursive descent parser
var UseNewParser = true

// ParseQuery parses a Cypher-like query string
// Supports queries like:
//   MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN a, b
//   CREATE (p:Person {name: "Alice", age: 28})
//   MATCH (p:Person) WHERE p.name = "Alice" SET p.age = 29
//   MATCH (p:Person) WHERE p.name = "Alice" DELETE p
func ParseQuery(queryStr string) (*Query, error) {
	if UseNewParser {
		return parseQueryWithNewParser(queryStr)
	}
	return parseQueryLegacy(queryStr)
}

// parseQueryWithNewParser uses the new Cypher parser
func parseQueryWithNewParser(queryStr string) (*Query, error) {
	gq, err := cypher.ParseToGraph(queryStr)
	if err != nil {
		return nil, err
	}

	// Convert cypher.GraphQuery to Query
	return convertGraphQueryToQuery(gq), nil
}

// convertGraphQueryToQuery converts the new parser output to the old Query format
func convertGraphQueryToQuery(gq *cypher.GraphQuery) *Query {
	q := &Query{
		QueryType:   gq.QueryType,
		IsPathQuery: gq.IsPathQuery,
	}

	// Convert MatchPattern
	if gq.MatchPattern != nil {
		q.MatchPattern = &MatchPattern{
			Nodes:         make([]NodePattern, len(gq.MatchPattern.Nodes)),
			Relationships: make([]RelPattern, len(gq.MatchPattern.Relationships)),
		}
		for i, n := range gq.MatchPattern.Nodes {
			q.MatchPattern.Nodes[i] = NodePattern{
				Variable:   n.Variable,
				Labels:     n.Labels,
				Properties: n.Properties,
			}
		}
		for i, r := range gq.MatchPattern.Relationships {
			q.MatchPattern.Relationships[i] = RelPattern{
				Variable:  r.Variable,
				Types:     r.Types,
				FromIndex: r.FromIndex,
				ToIndex:   r.ToIndex,
				Direction: r.Direction,
			}
		}
		if gq.MatchPattern.PathFunction != nil {
			pf := gq.MatchPattern.PathFunction
			q.MatchPattern.PathFunction = &PathFunction{
				Function: pf.Function,
				Variable: pf.Variable,
				StartPattern: NodePattern{
					Variable:   pf.StartPattern.Variable,
					Labels:     pf.StartPattern.Labels,
					Properties: pf.StartPattern.Properties,
				},
				EndPattern: NodePattern{
					Variable:   pf.EndPattern.Variable,
					Labels:     pf.EndPattern.Labels,
					Properties: pf.EndPattern.Properties,
				},
				RelTypes: pf.RelTypes,
				MaxDepth: pf.MaxDepth,
			}
		}
	}

	// Convert CreateClause
	if gq.CreateClause != nil {
		q.CreateClause = &CreateClause{
			Nodes:         make([]CreateNode, len(gq.CreateClause.Nodes)),
			Relationships: make([]CreateRelationship, len(gq.CreateClause.Relationships)),
		}
		for i, n := range gq.CreateClause.Nodes {
			q.CreateClause.Nodes[i] = CreateNode{
				Variable:   n.Variable,
				Labels:     n.Labels,
				Properties: n.Properties,
			}
		}
		for i, r := range gq.CreateClause.Relationships {
			q.CreateClause.Relationships[i] = CreateRelationship{
				Variable:   r.Variable,
				Type:       r.Type,
				FromVar:    r.FromVar,
				ToVar:      r.ToVar,
				Properties: r.Properties,
			}
		}
	}

	// Convert SetClause
	if gq.SetClause != nil {
		q.SetClause = &SetClause{
			Updates: make([]PropertyUpdate, len(gq.SetClause.Updates)),
		}
		for i, u := range gq.SetClause.Updates {
			q.SetClause.Updates[i] = PropertyUpdate{
				Variable: u.Variable,
				Property: u.Property,
				Value:    u.Value,
			}
		}
	}

	// Convert DeleteClause
	if gq.DeleteClause != nil {
		q.DeleteClause = &DeleteClause{
			Variables: gq.DeleteClause.Variables,
			Detach:    gq.DeleteClause.Detach,
		}
	}

	// Convert WhereClause
	if gq.WhereClause != nil {
		q.WhereClause = &WhereClause{
			Conditions: make([]Condition, len(gq.WhereClause.Conditions)),
		}
		for i, c := range gq.WhereClause.Conditions {
			q.WhereClause.Conditions[i] = Condition{
				Variable: c.Variable,
				Property: c.Property,
				Operator: c.Operator,
				Value:    c.Value,
			}
		}
	}

	// Convert ReturnClause
	if gq.ReturnClause != nil {
		q.ReturnClause = &ReturnClause{
			Items:    make([]ReturnItem, len(gq.ReturnClause.Items)),
			Distinct: gq.ReturnClause.Distinct,
			Skip:     gq.ReturnClause.Skip,
			Limit:    gq.ReturnClause.Limit,
		}
		for i, item := range gq.ReturnClause.Items {
			q.ReturnClause.Items[i] = ReturnItem{
				Variable:    item.Variable,
				Property:    item.Property,
				Aggregation: item.Aggregation,
				Alias:       item.Alias,
			}
		}
		// Convert OrderBy
		for _, orderItem := range gq.ReturnClause.OrderBy {
			q.ReturnClause.OrderBy = append(q.ReturnClause.OrderBy, OrderItem{
				Variable:   orderItem.Variable,
				Property:   orderItem.Property,
				Descending: orderItem.Descending,
			})
		}
	}

	// Convert TimeClause
	if gq.TimeClause != nil {
		q.TimeClause = &TimeClause{
			Mode:      gq.TimeClause.Mode,
			Timestamp: gq.TimeClause.Timestamp,
		}
	}

	// Convert EmbedClause
	if gq.EmbedClause != nil {
		q.EmbedClause = &EmbedClause{
			Variable: gq.EmbedClause.Variable,
			Mode:     gq.EmbedClause.Mode,
			Text:     gq.EmbedClause.Text,
			Property: gq.EmbedClause.Property,
		}
	}

	// Convert SimilarToClause
	if gq.SimilarToClause != nil {
		q.SimilarToClause = &SimilarToClause{
			Variable:  gq.SimilarToClause.Variable,
			QueryText: gq.SimilarToClause.QueryText,
			Limit:     gq.SimilarToClause.Limit,
			Threshold: gq.SimilarToClause.Threshold,
		}
	}

	return q
}

// parseQueryLegacy is the original regex-based parser
func parseQueryLegacy(queryStr string) (*Query, error) {
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
	matchRegex := regexp.MustCompile(`(?i)MATCH\s+(.+?)(?:\s+WHERE\s+|\s+AT\s+TIME\s+|\s+SIMILAR\s+TO\s+|\s+CREATE\s+|\s+SET\s+|\s+DELETE\s+|\s+EMBED\s+|\s+RETURN\s+|$)`)
	whereRegex := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+AT\s+TIME\s+|\s+SIMILAR\s+TO\s+|\s+CREATE\s+|\s+SET\s+|\s+DELETE\s+|\s+EMBED\s+|\s+RETURN\s+|$)`)
	timeRegex := regexp.MustCompile(`(?i)AT\s+TIME\s+(EARLIEST|(\d+))(?:\s+SIMILAR\s+TO\s+|\s+CREATE\s+|\s+SET\s+|\s+DELETE\s+|\s+EMBED\s+|\s+RETURN\s+|$)`)
	similarToRegex := regexp.MustCompile(`(?i)SIMILAR\s+TO\s+(.+?)(?:\s+CREATE\s+|\s+SET\s+|\s+DELETE\s+|\s+EMBED\s+|\s+RETURN\s+|$)`)
	createRegex := regexp.MustCompile(`(?i)CREATE\s+(.+?)(?:\s+SET\s+|\s+DELETE\s+|\s+EMBED\s+|\s+RETURN\s+|$)`)
	embedRegex := regexp.MustCompile(`(?i)EMBED\s+(.+?)(?:\s+RETURN\s+|$)`)
	setRegex := regexp.MustCompile(`(?i)SET\s+(.+?)(?:\s+EMBED\s+|\s+RETURN\s+|$)`)
	deleteRegex := regexp.MustCompile(`(?i)(DETACH\s+)?DELETE\s+(.+?)(?:\s+EMBED\s+|\s+RETURN\s+|$)`)
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

	// Parse CREATE clause (optional - for MATCH...CREATE pattern)
	createMatch := createRegex.FindStringSubmatch(queryStr)
	if createMatch != nil {
		createClause, err := parseCreateClause(createMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing CREATE: %w", err)
		}
		query.CreateClause = createClause
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

	// Parse SIMILAR TO clause (optional)
	similarToMatch := similarToRegex.FindStringSubmatch(queryStr)
	if similarToMatch != nil {
		similarToClause, err := parseSimilarToClause(similarToMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing SIMILAR TO: %w", err)
		}
		query.SimilarToClause = similarToClause
	}

	// Parse EMBED clause (optional)
	embedMatch := embedRegex.FindStringSubmatch(queryStr)
	if embedMatch != nil {
		embedClause, err := parseEmbedClause(embedMatch[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing EMBED: %w", err)
		}
		query.EmbedClause = embedClause
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

	// Find all node patterns: (variable:Label {properties...})
	nodeRegex := regexp.MustCompile(`\(([a-zA-Z_][a-zA-Z0-9_]*)?(?::([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?:\{([^}]*)\})?\)`)
	nodeMatches := nodeRegex.FindAllStringSubmatch(pattern, -1)

	for _, match := range nodeMatches {
		np := NodePattern{
			Variable:   match[1],
			Labels:     []string{},
			Properties: make(map[string]interface{}),
		}
		if match[2] != "" {
			np.Labels = append(np.Labels, match[2])
		}
		// Parse inline properties
		if match[3] != "" {
			props, err := parseProperties(match[3])
			if err != nil {
				return nil, fmt.Errorf("error parsing inline properties: %w", err)
			}
			np.Properties = props
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
	// Extract start and end node patterns (with optional properties)
	nodeRegex := regexp.MustCompile(`\(([a-zA-Z_][a-zA-Z0-9_]*)?(?::([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?:\{([^}]*)\})?\)`)
	nodeMatches := nodeRegex.FindAllStringSubmatch(innerPattern, -1)

	if len(nodeMatches) < 2 {
		return nil, fmt.Errorf("path function requires start and end nodes")
	}

	// Parse start node
	pf.StartPattern = NodePattern{
		Variable:   nodeMatches[0][1],
		Labels:     []string{},
		Properties: make(map[string]interface{}),
	}
	if nodeMatches[0][2] != "" {
		pf.StartPattern.Labels = append(pf.StartPattern.Labels, nodeMatches[0][2])
	}
	if nodeMatches[0][3] != "" {
		props, err := parseProperties(nodeMatches[0][3])
		if err != nil {
			return nil, fmt.Errorf("error parsing start node properties: %w", err)
		}
		pf.StartPattern.Properties = props
	}

	// Parse end node
	pf.EndPattern = NodePattern{
		Variable:   nodeMatches[1][1],
		Labels:     []string{},
		Properties: make(map[string]interface{}),
	}
	if nodeMatches[1][2] != "" {
		pf.EndPattern.Labels = append(pf.EndPattern.Labels, nodeMatches[1][2])
	}
	if nodeMatches[1][3] != "" {
		props, err := parseProperties(nodeMatches[1][3])
		if err != nil {
			return nil, fmt.Errorf("error parsing end node properties: %w", err)
		}
		pf.EndPattern.Properties = props
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

// parseEmbedClause parses an EMBED clause
// Examples:
//   EMBED p AUTO                    - generates text from labels + properties
//   EMBED p "some literal text"     - embeds literal text
//   EMBED p.description             - embeds value of the description property
func parseEmbedClause(embedStr string) (*EmbedClause, error) {
	embedStr = strings.TrimSpace(embedStr)
	ec := &EmbedClause{}

	// Check for AUTO mode: "p AUTO" or just "p" (AUTO is default)
	autoRegex := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s+AUTO$`)
	if match := autoRegex.FindStringSubmatch(embedStr); match != nil {
		ec.Variable = match[1]
		ec.Mode = "AUTO"
		return ec, nil
	}

	// Check for literal text mode: "p \"some text\""
	textRegex := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s+"([^"]*)"$`)
	if match := textRegex.FindStringSubmatch(embedStr); match != nil {
		ec.Variable = match[1]
		ec.Mode = "TEXT"
		ec.Text = match[2]
		return ec, nil
	}

	// Check for property mode: "p.description"
	propRegex := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$`)
	if match := propRegex.FindStringSubmatch(embedStr); match != nil {
		ec.Variable = match[1]
		ec.Mode = "PROPERTY"
		ec.Property = match[2]
		return ec, nil
	}

	// Default: just variable name means AUTO
	varRegex := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)$`)
	if match := varRegex.FindStringSubmatch(embedStr); match != nil {
		ec.Variable = match[1]
		ec.Mode = "AUTO"
		return ec, nil
	}

	return nil, fmt.Errorf("invalid EMBED syntax: %s", embedStr)
}

// parseSimilarToClause parses a SIMILAR TO clause
// Examples:
//   SIMILAR TO "backend engineers"
//   SIMILAR TO "backend engineers" LIMIT 10
//   SIMILAR TO "backend engineers" THRESHOLD 0.7
//   SIMILAR TO "backend engineers" LIMIT 10 THRESHOLD 0.7
func parseSimilarToClause(similarStr string) (*SimilarToClause, error) {
	similarStr = strings.TrimSpace(similarStr)
	stc := &SimilarToClause{}

	// Extract the query text (in quotes)
	textRegex := regexp.MustCompile(`^"([^"]*)"`)
	textMatch := textRegex.FindStringSubmatch(similarStr)
	if textMatch == nil {
		return nil, fmt.Errorf("SIMILAR TO requires quoted query text")
	}
	stc.QueryText = textMatch[1]

	// Extract optional LIMIT
	limitRegex := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	if limitMatch := limitRegex.FindStringSubmatch(similarStr); limitMatch != nil {
		limit, err := strconv.Atoi(limitMatch[1])
		if err == nil {
			stc.Limit = limit
		}
	}

	// Extract optional THRESHOLD
	threshRegex := regexp.MustCompile(`(?i)THRESHOLD\s+([\d.]+)`)
	if threshMatch := threshRegex.FindStringSubmatch(similarStr); threshMatch != nil {
		thresh, err := strconv.ParseFloat(threshMatch[1], 32)
		if err == nil {
			stc.Threshold = float32(thresh)
		}
	}

	return stc, nil
}
