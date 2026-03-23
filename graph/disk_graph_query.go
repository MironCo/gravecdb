package graph

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MironCo/gravecdb/cypher"
	"github.com/MironCo/gravecdb/embedding"
)

// injectParams adds __params__ to each match so evalExpr can resolve $param references.
func injectParams(matches []Match, params map[string]interface{}) []Match {
	if len(params) == 0 {
		return matches
	}
	for i := range matches {
		matches[i]["__params__"] = params
	}
	return matches
}

// ExecuteQueryWithEmbedder executes a Cypher-like query
func (g *DiskGraph) ExecuteQueryWithEmbedder(query *Query, embedder Embedder) (*QueryResult, error) {
	// For mutating queries (CREATE, SET, DELETE), use DiskGraph's own methods
	// For read queries (MATCH without mutations), use in-memory graph

	switch query.QueryType {
	case "UNION":
		return g.executeUnionQuery(query, embedder)
	case "PIPELINE":
		return g.executePipelineQuery(query, embedder)
	case "LOAD_CSV":
		return g.executeLoadCSVQuery(query)
	case "UNWIND":
		return g.executeUnwindQuery(query)
	case "MERGE":
		return g.executeMergeQuery(query)
	case "CREATE":
		return g.executeCreateQuery(query)
	case "CALL":
		return g.executeCallQuery(query)
	case "MATCH":
		// Check for FOREACH first (MATCH ... FOREACH)
		if query.ForeachClause != nil {
			return g.executeForeachQuery(query)
		}
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

// executeCallQuery handles CALL procedure() YIELD ... queries.
func (g *DiskGraph) executeCallQuery(query *Query) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	cc := query.CallClause
	if cc == nil {
		return nil, fmt.Errorf("CALL clause is nil")
	}

	var matches []Match
	var err error

	if query.TimeClause != nil {
		// AT TIME: build temporal snapshot, run algorithm on it
		snap, snapErr := g.buildSnapshotFromTimeClause(query.TimeClause)
		if snapErr != nil {
			return nil, snapErr
		}
		if snap == nil {
			return buildResult(nil, query.ReturnClause), nil
		}
		switch cc.Procedure {
		case "pagerank":
			matches, err = snap.computePageRank(cc.Config)
		case "louvain":
			matches, err = snap.computeLouvain(cc.Config)
		default:
			return nil, fmt.Errorf("unknown procedure: %s", cc.Procedure)
		}
	} else if cc.ThroughTime {
		// THROUGH TIME: run algorithm at each topology-change event
		matches, err = g.executeCallThroughTime(cc)
	} else {
		// Current graph state
		switch cc.Procedure {
		case "pagerank":
			matches, err = g.computePageRank(cc.Config)
		case "louvain":
			matches, err = g.computeLouvain(cc.Config)
		default:
			return nil, fmt.Errorf("unknown procedure: %s", cc.Procedure)
		}
	}
	if err != nil {
		return nil, err
	}

	return buildResult(matches, query.ReturnClause), nil
}

// executeCallThroughTime runs an algorithm at every topology-change event
// and returns temporal intervals showing how each node's value evolves.
// Caller must hold g.mu (at least RLock).
func (g *DiskGraph) executeCallThroughTime(cc *CallClause) ([]Match, error) {
	allNodes, _ := g.boltStore.GetAllNodes()
	allRels, _ := g.boltStore.GetAllRelationships()

	// 1. Collect all distinct timestamps where the graph topology changes.
	timeSet := make(map[int64]bool)
	for _, n := range allNodes {
		timeSet[n.ValidFrom.Unix()] = true
		if n.ValidTo != nil {
			timeSet[n.ValidTo.Unix()] = true
		}
	}
	for _, r := range allRels {
		timeSet[r.ValidFrom.Unix()] = true
		if r.ValidTo != nil {
			timeSet[r.ValidTo.Unix()] = true
		}
	}
	if len(timeSet) == 0 {
		return nil, nil
	}

	times := make([]int64, 0, len(timeSet))
	for t := range timeSet {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })

	// 2. Determine value key and comparison mode.
	isLouvain := cc.Procedure == "louvain"
	valueKey := "score"
	if isLouvain {
		valueKey = "community"
	}

	// 3. Track per-node intervals.
	type nodeInterval struct {
		node      *Node
		value     interface{}
		validFrom time.Time
		// For Louvain: canonical community key (sorted member IDs hash)
		commKey string
	}
	current := make(map[string]*nodeInterval) // nodeID -> open interval
	var results []Match

	// closeInterval emits a completed interval as a Match row.
	closeInterval := func(ni *nodeInterval, validTo time.Time) {
		results = append(results, Match{
			"node":       ni.node,
			valueKey:     ni.value,
			"valid_from": ni.validFrom.Format(time.RFC3339),
			"valid_to":   validTo.Format(time.RFC3339),
		})
	}

	for _, ts := range times {
		t := time.Unix(ts, 0)
		snap := buildTemporalSnapshot(allNodes, allRels, t)

		var matches []Match
		var err error
		switch cc.Procedure {
		case "pagerank":
			matches, err = snap.computePageRank(cc.Config)
		case "louvain":
			matches, err = snap.computeLouvain(cc.Config)
		default:
			return nil, fmt.Errorf("unknown procedure: %s", cc.Procedure)
		}
		if err != nil {
			return nil, err
		}

		// Build current node->value map and community membership for Louvain.
		currentValues := make(map[string]interface{})
		currentNodes := make(map[string]*Node)
		// For Louvain: build community -> sorted member list -> canonical key.
		commMembers := make(map[int][]string) // communityID -> sorted nodeIDs
		for _, m := range matches {
			node := m["node"].(*Node)
			currentValues[node.ID] = m[valueKey]
			currentNodes[node.ID] = node
			if isLouvain {
				cid := m["community"].(int)
				commMembers[cid] = append(commMembers[cid], node.ID)
			}
		}

		// Canonicalize Louvain communities by sorted member set.
		commCanonical := make(map[int]string) // communityID -> canonical key
		if isLouvain {
			for cid, members := range commMembers {
				sort.Strings(members)
				commCanonical[cid] = strings.Join(members, ",")
			}
		}

		// Detect changes: nodes that disappeared or changed value.
		for nodeID, prev := range current {
			newVal, stillExists := currentValues[nodeID]
			if !stillExists {
				closeInterval(prev, t)
				delete(current, nodeID)
				continue
			}
			changed := false
			if isLouvain {
				// Compare by canonical community membership, not raw ID.
				newCID := newVal.(int)
				newKey := commCanonical[newCID]
				changed = prev.commKey != newKey
			} else {
				// PageRank: compare with tolerance.
				oldScore, _ := prev.value.(float64)
				newScore, _ := newVal.(float64)
				changed = math.Abs(oldScore-newScore) > 1e-4
			}
			if changed {
				closeInterval(prev, t)
				delete(current, nodeID)
			}
		}

		// Start new intervals for nodes that appeared or changed.
		for nodeID, val := range currentValues {
			if _, exists := current[nodeID]; !exists {
				ni := &nodeInterval{
					node:      currentNodes[nodeID],
					value:     val,
					validFrom: t,
				}
				if isLouvain {
					cid := val.(int)
					ni.commKey = commCanonical[cid]
				}
				current[nodeID] = ni
			}
		}
	}

	// 4. Close still-open intervals (valid_to = nil means "still current").
	for _, ni := range current {
		results = append(results, Match{
			"node":       ni.node,
			valueKey:     ni.value,
			"valid_from": ni.validFrom.Format(time.RFC3339),
			"valid_to":   nil,
		})
	}

	return results, nil
}

// executeUnwindQuery expands a list literal into rows and applies RETURN.
func (g *DiskGraph) executeUnwindQuery(query *Query) (*QueryResult, error) {
	uc := query.UnwindClause

	// Build a base match with params so evalExpr can resolve $param references
	baseMatch := Match{}
	if len(query.Parameters) > 0 {
		baseMatch["__params__"] = query.Parameters
	}

	// Resolve the list: either a pre-evaluated literal or a dynamic expression (e.g. range())
	list := uc.List
	if len(list) == 0 && uc.ListExpr != nil {
		val := evalExpr(uc.ListExpr, baseMatch)
		if l, ok := val.([]interface{}); ok {
			list = l
		}
	}

	var matches []Match
	for _, item := range list {
		m := Match{uc.Variable: item}
		if len(query.Parameters) > 0 {
			m["__params__"] = query.Parameters
		}
		matches = append(matches, m)
	}
	if query.WhereClause != nil {
		matches = g.filterMatchesUnlocked(matches, query.WhereClause)
	}
	return buildResult(matches, query.ReturnClause), nil
}

// executeLoadCSVQuery handles LOAD CSV FROM 'file' AS row CREATE/SET/...
func (g *DiskGraph) executeLoadCSVQuery(query *Query) (*QueryResult, error) {
	lc := query.LoadCSVClause
	if lc == nil {
		return nil, fmt.Errorf("LOAD CSV clause is nil")
	}

	// Open and parse the CSV file
	f, err := os.Open(lc.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	if lc.FieldTerminator != "" && len(lc.FieldTerminator) == 1 {
		reader.Comma = rune(lc.FieldTerminator[0])
	}

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return &QueryResult{Columns: []string{"loaded"}, Rows: []map[string]interface{}{{"loaded": 0}}}, nil
	}

	// Build row data: WITH HEADERS uses first row as keys, otherwise indexed access
	var headers []string
	startRow := 0
	if lc.WithHeaders {
		headers = records[0]
		startRow = 1
	}

	// Execute subsequent clauses for each row
	totalCreated := 0
	for i := startRow; i < len(records); i++ {
		record := records[i]

		// Build the row variable value
		var rowVal interface{}
		if lc.WithHeaders {
			rowMap := make(map[string]interface{})
			for j, header := range headers {
				if j < len(record) {
					rowMap[header] = autoConvert(record[j])
				}
			}
			rowVal = rowMap
		} else {
			rowList := make([]interface{}, len(record))
			for j, val := range record {
				rowList[j] = autoConvert(val)
			}
			rowVal = rowList
		}

		if query.CreateClause != nil {
			// Deep-copy the create clause so each row gets fresh properties
			rowCC := copyCreateClause(query.CreateClause)
			for idx := range rowCC.Nodes {
				resolveRowReferences(rowCC.Nodes[idx].Properties, lc.Variable, rowVal)
			}
			for idx := range rowCC.Relationships {
				resolveRowReferences(rowCC.Relationships[idx].Properties, lc.Variable, rowVal)
			}

			rowQuery := &Query{
				QueryType:    "CREATE",
				CreateClause: rowCC,
				TimeClause:   query.TimeClause,
			}
			_, err := g.executeCreateQuery(rowQuery)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", i+1, err)
			}
			totalCreated++
		}
	}

	return &QueryResult{
		Columns: []string{"loaded"},
		Rows:    []map[string]interface{}{{"loaded": totalCreated}},
	}, nil
}

// copyCreateClause makes a deep copy of a GraphCreateClause so row resolution doesn't mutate the original
func copyCreateClause(cc *cypher.GraphCreateClause) *cypher.GraphCreateClause {
	clone := &cypher.GraphCreateClause{
		Nodes:         make([]cypher.GraphCreateNode, len(cc.Nodes)),
		Relationships: make([]cypher.GraphCreateRelationship, len(cc.Relationships)),
	}
	for i, n := range cc.Nodes {
		clone.Nodes[i] = cypher.GraphCreateNode{
			Variable: n.Variable,
			Labels:   n.Labels,
			Properties: copyProps(n.Properties),
		}
	}
	for i, r := range cc.Relationships {
		clone.Relationships[i] = cypher.GraphCreateRelationship{
			Variable:   r.Variable,
			Type:       r.Type,
			FromVar:    r.FromVar,
			ToVar:      r.ToVar,
			Properties: copyProps(r.Properties),
		}
	}
	return clone
}

func copyProps(props map[string]interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(props))
	for k, v := range props {
		m[k] = v
	}
	return m
}

// autoConvert attempts to convert a CSV string value to a numeric type if possible
func autoConvert(s string) interface{} {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	return s
}

// resolveRowReferences replaces row.field references in property maps with actual values
func resolveRowReferences(props map[string]interface{}, rowVar string, rowVal interface{}) {
	for key, val := range props {
		if strVal, ok := val.(string); ok {
			// Check for row.field reference pattern
			if strings.HasPrefix(strVal, rowVar+".") {
				field := strVal[len(rowVar)+1:]
				if rowMap, ok := rowVal.(map[string]interface{}); ok {
					if resolved, exists := rowMap[field]; exists {
						props[key] = resolved
					}
				}
			} else if strVal == rowVar {
				props[key] = rowVal
			}
		}
		// Handle cypher.PropertyAccess expressions stored during parsing
		if pa, ok := val.(*cypher.PropertyAccess); ok {
			if ident, ok := pa.Object.(*cypher.Identifier); ok && ident.Name == rowVar {
				if rowMap, ok := rowVal.(map[string]interface{}); ok {
					if resolved, exists := rowMap[pa.Property]; exists {
						props[key] = resolved
					}
				}
			}
		}
	}
}

// executeUnionQuery runs each sub-query and combines the results.
func (g *DiskGraph) executeUnionQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	if len(query.UnionQueries) == 0 {
		return &QueryResult{}, nil
	}

	// Execute first sub-query to establish columns
	first, err := g.ExecuteQueryWithEmbedder(query.UnionQueries[0], embedder)
	if err != nil {
		return nil, err
	}

	combined := &QueryResult{
		Columns: first.Columns,
		Rows:    make([]map[string]interface{}, 0, len(first.Rows)),
	}
	combined.Rows = append(combined.Rows, first.Rows...)

	// Execute remaining sub-queries
	for _, sub := range query.UnionQueries[1:] {
		result, err := g.ExecuteQueryWithEmbedder(sub, embedder)
		if err != nil {
			return nil, err
		}
		combined.Rows = append(combined.Rows, result.Rows...)
	}

	// UNION (without ALL) removes duplicate rows
	if !query.UnionAll {
		seen := make(map[string]bool)
		var unique []map[string]interface{}
		for _, row := range combined.Rows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		combined.Rows = unique
	}

	return combined, nil
}

// executeForeachQuery handles MATCH ... FOREACH (var IN list | SET ...)
func (g *DiskGraph) executeForeachQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	updatedCount := 0
	fc := query.ForeachClause
	for _, match := range matches {
		// Evaluate the list expression in context of this match
		listVal := evalExpr(fc.ListExpr, match)
		list, ok := listVal.([]interface{})
		if !ok {
			continue
		}

		for _, item := range list {
			// Create a local match with the iteration variable bound
			localMatch := copyMatch(match)
			localMatch[fc.Variable] = item

			// Apply each SET operation
			for _, update := range fc.Updates {
				if update.Property == nil {
					continue
				}
				varName := ""
				if ident, ok := update.Property.Object.(*cypher.Identifier); ok {
					varName = ident.Name
				}
				propName := update.Property.Property
				val := evalExpr(update.Expression, localMatch)

				entity, ok := localMatch[varName]
				if !ok {
					continue
				}
				if node, ok := entity.(*Node); ok {
					g.setNodePropertyUnlocked(node.ID, propName, val)
					updatedCount++
				} else if rel, ok := entity.(*Relationship); ok {
					g.setRelPropertyUnlocked(rel.ID, propName, val)
					updatedCount++
				}
			}
		}
	}

	return &QueryResult{
		Columns: []string{"updated"},
		Rows:    []map[string]interface{}{{"updated": updatedCount}},
	}, nil
}

// executeMergeQuery finds a node matching the pattern; creates it if absent.
func (g *DiskGraph) executeMergeQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	mc := query.MergeClause
	result := &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}

	for _, mn := range mc.Create.Nodes {
		// Try to find an existing node with matching labels + properties
		var found *Node
		created := false
		if len(mn.Labels) > 0 {
			for _, id := range g.labelIndex[mn.Labels[0]] {
				n := g.getNodeUnlocked(id)
				if n == nil || n.ValidTo != nil {
					continue
				}
				match := true
				for k, v := range mn.Properties {
					if !valuesEqual(n.Properties[k], v) {
						match = false
						break
					}
				}
				if match {
					found = n
					break
				}
			}
		}

		if found == nil {
			created = true
			var err error
			found, err = g.createNodeUnlocked(mn.Labels...)
			if err != nil {
				return nil, err
			}
			for k, v := range mn.Properties {
				if err := g.setNodePropertyUnlocked(found.ID, k, v); err != nil {
					return nil, err
				}
			}
			// Re-fetch: setNodePropertyUnlocked creates a new version, so found is now stale
			if refreshed := g.getNodeUnlocked(found.ID); refreshed != nil {
				found = refreshed
			}
		}

		// Apply ON CREATE SET or ON MATCH SET
		setItems := mc.OnMatch
		if created {
			setItems = mc.OnCreate
		}
		if len(setItems) > 0 {
			localMatch := Match{}
			if mn.Variable != "" {
				localMatch[mn.Variable] = found
			}
			for _, si := range setItems {
				g.applySetItemUnlocked(si, localMatch)
			}
			// Re-fetch after SET
			if refreshed := g.getNodeUnlocked(found.ID); refreshed != nil {
				found = refreshed
			}
		}

		if mn.Variable != "" && query.ReturnClause != nil {
			result.Rows = append(result.Rows, map[string]interface{}{mn.Variable: found})
		}
	}

	if query.ReturnClause != nil {
		result.Columns = make([]string, len(query.ReturnClause.Items))
		for i, item := range query.ReturnClause.Items {
			if item.Alias != "" {
				result.Columns[i] = item.Alias
			} else {
				result.Columns[i] = item.Variable
			}
		}
	}

	return result, nil
}

// executeCreateQuery handles CREATE queries directly on DiskGraph
func (g *DiskGraph) executeCreateQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

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
		node, err := g.createNodeAtTimeUnlocked(createTime, nodeSpec.Labels...)
		if err != nil {
			return nil, err
		}

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

		rel, err := g.createRelationshipAtTimeUnlocked(createTime, relSpec.Type, fromNode.ID, toNode.ID)
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

	// Determine creation timestamp (AT TIME or now)
	createTime := time.Now()
	if query.TimeClause != nil && query.TimeClause.Timestamp > 0 {
		createTime = time.Unix(query.TimeClause.Timestamp, 0)
	}

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
			node, err := g.createNodeAtTimeUnlocked(createTime, nodeSpec.Labels...)
			if err != nil {
				continue
			}
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

			rel, err := g.createRelationshipAtTimeUnlocked(createTime, relSpec.Type, fromNode.ID, toNode.ID)
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

// applySetItemUnlocked applies a single SET item (from AST) against matched entities.
// Caller must hold g.mu.
func (g *DiskGraph) applySetItemUnlocked(si *cypher.SetItem, match Match) {
	if si.Property == nil {
		return
	}
	varName := ""
	if ident, ok := si.Property.Object.(*cypher.Identifier); ok {
		varName = ident.Name
	}
	prop := si.Property.Property
	val := evalExpr(si.Expression, match)

	entity, ok := match[varName]
	if !ok {
		return
	}
	if node, ok := entity.(*Node); ok {
		g.setNodePropertyUnlocked(node.ID, prop, val)
	} else if rel, ok := entity.(*Relationship); ok {
		g.setRelPropertyUnlocked(rel.ID, prop, val)
	}
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

	deleteTime := time.Now()
	if query.TimeClause != nil && query.TimeClause.Timestamp > 0 {
		deleteTime = time.Unix(query.TimeClause.Timestamp, 0)
	}

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
						g.deleteRelationshipAtTimeUnlocked(rel.ID, deleteTime)
					}
				}
				g.deleteNodeAtTimeUnlocked(node.ID, deleteTime)
				deletedCount++
			} else if rel, ok := entity.(*Relationship); ok {
				g.deleteRelationshipAtTimeUnlocked(rel.ID, deleteTime)
				deletedCount++
			}
		}
	}

	return &QueryResult{
		Columns: []string{"deleted"},
		Rows:    []map[string]interface{}{{"deleted": deletedCount}},
	}, nil
}

// executeRemoveQuery handles MATCH...REMOVE queries
func (g *DiskGraph) executeRemoveQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)

	removedCount := 0
	for _, match := range matches {
		for _, item := range query.RemoveClause.Items {
			entity, ok := match[item.Variable]
			if !ok {
				continue
			}
			if node, ok := entity.(*Node); ok {
				if item.Property != "" {
					g.deleteNodePropertyUnlocked(node.ID, item.Property)
					removedCount++
				} else if item.Label != "" {
					g.removeNodeLabelUnlocked(node.ID, item.Label)
					removedCount++
				}
			}
		}
	}

	return &QueryResult{
		Columns: []string{"removed"},
		Rows:    []map[string]interface{}{{"removed": removedCount}},
	}, nil
}

// executeReadQuery handles read-only MATCH queries disk-natively.
func (g *DiskGraph) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Path queries (shortestPath / allShortestPaths)
	if query.IsPathQuery && query.MatchPattern != nil && query.MatchPattern.PathFunction != nil {
		return g.executePathQueryUnlocked(query)
	}

	// SIMILAR TO semantic search
	if query.SimilarToClause != nil {
		return g.executeSimilarToUnlocked(query, embedder)
	}

	// Temporal MATCH (AT TIME / EARLIEST)
	if query.TimeClause != nil {
		return g.executeTemporalMatchUnlocked(query)
	}

	// Regular MATCH
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause, query.Parameters)
	matches = injectParams(matches, query.Parameters)

	// Apply semantic WHERE conditions: WHERE p SIMILAR TO "..."
	if query.WhereClause != nil && len(query.WhereClause.SemanticConditions) > 0 {
		var err error
		matches, err = g.filterSemanticUnlocked(matches, query.WhereClause.SemanticConditions, embedder)
		if err != nil {
			return nil, err
		}
	}

	// OPTIONAL MATCH: if no rows were found, produce one empty row so that
	// downstream RETURN items resolve to null rather than nothing.
	if query.Optional && len(matches) == 0 {
		matches = []Match{{}}
	}

	return buildResult(matches, query.ReturnClause), nil
}

// filterSemanticUnlocked filters matches by embedding similarity (caller holds read lock).
func (g *DiskGraph) filterSemanticUnlocked(matches []Match, semConds []GraphSemanticCondition, embedder Embedder) ([]Match, error) {
	if embedder == nil {
		return nil, fmt.Errorf("embedder required for SIMILAR TO in WHERE clause")
	}

	// Load all current embeddings from disk once
	allEmbs, err := g.boltStore.GetAllEmbeddings()
	if err != nil {
		return nil, fmt.Errorf("failed to load embeddings: %w", err)
	}
	embStore := embedding.NewStore()
	for nodeID, embs := range allEmbs {
		for _, emb := range embs {
			emb.NodeID = nodeID
			embStore.LoadEmbedding(emb)
		}
	}

	// Pre-compute one query vector per semantic condition
	type semFilter struct {
		variable  string
		vector    []float32
		threshold float32
	}
	var filters []semFilter
	for _, cond := range semConds {
		vec, err := embedder.Embed(cond.QueryText)
		if err != nil {
			return nil, fmt.Errorf("failed to embed query %q: %w", cond.QueryText, err)
		}
		filters = append(filters, semFilter{cond.Variable, vec, cond.Threshold})
	}

	var result []Match
	for _, match := range matches {
		allPass := true
		for _, f := range filters {
			entity, ok := match[f.variable]
			if !ok {
				allPass = false
				break
			}
			node, ok := entity.(*Node)
			if !ok {
				allPass = false
				break
			}
			emb := embStore.GetCurrent(node.ID)
			if emb == nil {
				allPass = false
				break
			}
			sim := embedding.CosineSimilarity(f.vector, emb.Vector)
			if f.threshold > 0 && sim < f.threshold {
				allPass = false
				break
			}
		}
		if allPass {
			result = append(result, match)
		}
	}
	return result, nil
}

// ============================================================================
// Path queries
// ============================================================================

// executePathQueryUnlocked handles shortestPath / allShortestPaths / earliestPath (caller holds read lock).
func (g *DiskGraph) executePathQueryUnlocked(query *Query) (*QueryResult, error) {
	pf := query.MatchPattern.PathFunction

	// Earliest arrival path — temporal Dijkstra, independent of AT TIME
	if pf.Function == "earliestpath" {
		return g.executeEarliestPathUnlocked(query)
	}

	// Temporal path query — build snapshot and run BFS/DFS against it
	if query.TimeClause != nil {
		snap, err := g.buildSnapshotFromTimeClause(query.TimeClause)
		if err != nil || snap == nil {
			return &QueryResult{Columns: []string{pf.Variable}, Rows: []map[string]interface{}{}}, nil
		}
		startNodes := filterNodesByWhere(candidateNodesInSnapshot(snap, pf.StartPattern), pf.StartPattern.Variable, query.WhereClause)
		endNodes := filterNodesByWhere(candidateNodesInSnapshot(snap, pf.EndPattern), pf.EndPattern.Variable, query.WhereClause)
		result := &QueryResult{Columns: []string{pf.Variable}, Rows: []map[string]interface{}{}}
		for _, startNode := range startNodes {
			for _, endNode := range endNodes {
				if startNode.ID == endNode.ID {
					continue
				}
				switch pf.Function {
				case "shortestpath":
					if path := shortestPathInSnapshot(snap, startNode.ID, endNode.ID); path != nil {
						result.Rows = append(result.Rows, map[string]interface{}{pf.Variable: path})
					}
				case "allshortestpaths":
					maxDepth := pf.MaxDepth
					if maxDepth == 0 {
						maxDepth = 10
					}
					for _, path := range allPathsInSnapshot(snap, startNode.ID, endNode.ID, maxDepth) {
						result.Rows = append(result.Rows, map[string]interface{}{pf.Variable: path})
					}
				}
			}
		}
		return result, nil
	}

	startCandidates := g.candidateNodesUnlocked(pf.StartPattern)
	endCandidates := g.candidateNodesUnlocked(pf.EndPattern)

	// Filter by WHERE clause
	startNodes := filterNodesByWhere(startCandidates, pf.StartPattern.Variable, query.WhereClause)
	endNodes := filterNodesByWhere(endCandidates, pf.EndPattern.Variable, query.WhereClause)

	result := &QueryResult{Columns: []string{pf.Variable}, Rows: []map[string]interface{}{}}

	for _, startNode := range startNodes {
		for _, endNode := range endNodes {
			if startNode.ID == endNode.ID {
				continue
			}
			switch pf.Function {
			case "shortestpath":
				if path := g.shortestPathUnlocked(startNode.ID, endNode.ID); path != nil {
					result.Rows = append(result.Rows, map[string]interface{}{pf.Variable: path})
				}
			case "allshortestpaths":
				maxDepth := pf.MaxDepth
				if maxDepth == 0 {
					maxDepth = 10
				}
				for _, path := range g.allPathsUnlocked(startNode.ID, endNode.ID, maxDepth) {
					result.Rows = append(result.Rows, map[string]interface{}{pf.Variable: path})
				}
			}
		}
	}
	return result, nil
}

// ============================================================================
// Earliest arrival path — temporal Dijkstra
// ============================================================================


// executeEarliestPathUnlocked runs earliestPath() — finds the temporally earliest route
// from startNode to endNode by treating arrival_time as the Dijkstra cost.
// Traversal considers ALL relationships regardless of ValidTo, so historical edges count.
func (g *DiskGraph) executeEarliestPathUnlocked(query *Query) (*QueryResult, error) {
	pf := query.MatchPattern.PathFunction
	result := &QueryResult{
		Columns: []string{pf.Variable, "arrival_time"},
		Rows:    []map[string]interface{}{},
	}

	// Load full temporal data (including expired relationships / old node versions)
	allNodesList, err := g.boltStore.GetAllNodes()
	if err != nil {
		return nil, err
	}
	allRelsList, err := g.boltStore.GetAllRelationships()
	if err != nil {
		return nil, err
	}

	// Build node map: keep the earliest version of each node (lowest ValidFrom = birth time)
	allNodes := make(map[string]*Node, len(allNodesList))
	for _, n := range allNodesList {
		if existing, ok := allNodes[n.ID]; !ok || n.ValidFrom.Before(existing.ValidFrom) {
			allNodes[n.ID] = n
		}
	}

	// Build adjacency list: forward direction only (From → To), respecting -[*]->
	relsByNode := make(map[string][]*Relationship, len(allNodes))
	for _, rel := range allRelsList {
		relsByNode[rel.FromNodeID] = append(relsByNode[rel.FromNodeID], rel)
	}

	// Start/end candidates are currently-alive nodes matching the patterns
	startNodes := filterNodesByWhere(g.candidateNodesUnlocked(pf.StartPattern), pf.StartPattern.Variable, query.WhereClause)
	endNodes := filterNodesByWhere(g.candidateNodesUnlocked(pf.EndPattern), pf.EndPattern.Variable, query.WhereClause)

	for _, startNode := range startNodes {
		for _, endNode := range endNodes {
			if startNode.ID == endNode.ID {
				continue
			}
			path, arrivalTime := earliestPathDijkstra(allNodes, relsByNode, startNode.ID, endNode.ID)
			if path != nil {
				result.Rows = append(result.Rows, map[string]interface{}{
					pf.Variable:   path,
					"arrival_time": arrivalTime.Format(time.RFC3339),
				})
			}
		}
	}
	return result, nil
}



// candidateNodesUnlocked returns all currently-valid nodes matching a node pattern.
func (g *DiskGraph) candidateNodesUnlocked(pattern NodePattern) []*Node {
	var candidates []*Node
	if len(pattern.Labels) > 0 {
		nodeIDs := g.labelIndex[pattern.Labels[0]]
		for _, id := range nodeIDs {
			if n := g.getNodeUnlocked(id); n != nil && n.ValidTo == nil {
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

// filterNodesByWhere keeps only nodes that satisfy conditions for a specific variable.
func filterNodesByWhere(nodes []*Node, variable string, where *WhereClause) []*Node {
	if where == nil {
		return nodes
	}
	var result []*Node
	for _, n := range nodes {
		ok := true
		for _, cond := range where.Conditions {
			if cond.Variable != variable {
				continue
			}
			propVal := n.Properties[cond.Property]
			if cond.FunctionName != "" {
				propVal = applyScalarFunction(cond.FunctionName, propVal)
			}
			if !evaluateCondition(propVal, cond.Operator, cond.Value) {
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

// ============================================================================
// SIMILAR TO queries
// ============================================================================

// executeSimilarToUnlocked handles SIMILAR TO semantic search (caller holds read lock).
func (g *DiskGraph) executeSimilarToUnlocked(query *Query, embedder Embedder) (*QueryResult, error) {
	if embedder == nil {
		return nil, fmt.Errorf("embedder required for SIMILAR TO clause")
	}

	stc := query.SimilarToClause

	queryVector, err := embedder.Embed(stc.QueryText)
	if err != nil {
		return nil, fmt.Errorf("failed to embed query text: %w", err)
	}

	// Candidate nodes from the MATCH pattern
	candidateIDs := make(map[string]bool)
	var nodeVar string
	if query.MatchPattern != nil && len(query.MatchPattern.Nodes) > 0 {
		nodePattern := query.MatchPattern.Nodes[0]
		nodeVar = nodePattern.Variable
		candidates := g.candidateNodesUnlocked(nodePattern)
		for _, n := range candidates {
			candidateIDs[n.ID] = true
		}
		if query.WhereClause != nil {
			filtered := make(map[string]bool)
			for id := range candidateIDs {
				n := g.getNodeUnlocked(id)
				if n == nil {
					continue
				}
				match := Match{nodeVar: n}
				if len(g.filterMatchesUnlocked([]Match{match}, query.WhereClause)) > 0 {
					filtered[id] = true
				}
			}
			candidateIDs = filtered
		}
	}

	// Load all embeddings from disk into a temporary store, preserving original timestamps
	embStore := embedding.NewStore()
	allEmbs, _ := g.boltStore.GetAllEmbeddings()
	for nodeID, embs := range allEmbs {
		for _, emb := range embs {
			emb.NodeID = nodeID
			embStore.LoadEmbedding(emb)
		}
	}

	limit := stc.Limit
	if limit == 0 {
		limit = 100
	}

	// Build result columns
	result := &QueryResult{Rows: []map[string]interface{}{}}
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
		vsr := embStore.SearchAllVersions(queryVector, limit, candidateIDs, stc.Threshold, stc.DriftMode)
		for _, v := range vsr {
			node := g.getNodeUnlocked(v.NodeID)
			if node == nil {
				continue
			}
			props := v.Embedding.PropertySnapshot
			if props == nil {
				props = node.Properties
			}
			row := map[string]interface{}{}
			if query.ReturnClause != nil {
				for _, item := range query.ReturnClause.Items {
					col := item.Variable
					if item.Property != "" {
						col = item.Variable + "." + item.Property
						row[col] = props[item.Property]
					} else {
						row[col] = &Node{ID: node.ID, Labels: node.Labels, Properties: props, ValidFrom: v.ValidFrom, ValidTo: v.ValidTo}
					}
				}
			} else {
				row["node"] = &Node{ID: node.ID, Labels: node.Labels, Properties: props, ValidFrom: v.ValidFrom, ValidTo: v.ValidTo}
			}
			row["similarity"] = v.Similarity
			row["valid_from"] = v.ValidFrom
			if v.ValidTo != nil {
				row["valid_to"] = *v.ValidTo
			} else {
				row["valid_to"] = nil
			}
			if stc.DriftMode {
				row["drift_from_previous"] = v.DriftFromPrevious
				row["drift_from_first"] = v.DriftFromFirst
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
					col := item.Variable
					if item.Property != "" {
						col = item.Variable + "." + item.Property
						row[col] = node.Properties[item.Property]
					} else {
						row[col] = node
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

// ============================================================================
// Temporal MATCH queries
// ============================================================================

// executeTemporalMatchUnlocked handles MATCH ... AT TIME queries (caller holds read lock).
func (g *DiskGraph) executeTemporalMatchUnlocked(query *Query) (*QueryResult, error) {
	tc := query.TimeClause

	var queryTime *time.Time
	if tc.Mode == "EARLIEST" {
		allNodes, _ := g.boltStore.GetAllNodes()
		allRels, _ := g.boltStore.GetAllRelationships()
		var earliest *time.Time
		for _, n := range allNodes {
			if earliest == nil || n.ValidFrom.Before(*earliest) {
				t := n.ValidFrom
				earliest = &t
			}
		}
		for _, r := range allRels {
			if earliest == nil || r.ValidFrom.Before(*earliest) {
				t := r.ValidFrom
				earliest = &t
			}
		}
		queryTime = earliest
	} else {
		t := time.Unix(tc.Timestamp, 0)
		queryTime = &t
	}

	if queryTime == nil {
		return buildResult(nil, query.ReturnClause), nil
	}

	allNodes, _ := g.boltStore.GetAllNodes()
	allRels, _ := g.boltStore.GetAllRelationships()
	snap := buildTemporalSnapshot(allNodes, allRels, *queryTime)
	matches := findMatchesInSnapshot(snap, query.MatchPattern, query.WhereClause)
	return buildResult(matches, query.ReturnClause), nil
}

// findMatchesInSnapshot performs pattern matching against a temporal snapshot.
func findMatchesInSnapshot(snap *temporalSnapshot, pattern *MatchPattern, where *WhereClause) []Match {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	firstPattern := pattern.Nodes[0]
	var candidateNodes []*Node

	if len(firstPattern.Labels) > 0 {
		for _, id := range snap.nodesByLabel[firstPattern.Labels[0]] {
			if n, ok := snap.nodes[id]; ok {
				candidateNodes = append(candidateNodes, n)
			}
		}
	} else {
		for _, n := range snap.nodes {
			candidateNodes = append(candidateNodes, n)
		}
	}

	if len(firstPattern.Properties) > 0 {
		var filtered []*Node
		for _, n := range candidateNodes {
			if matchesProperties(n.Properties, firstPattern.Properties) {
				filtered = append(filtered, n)
			}
		}
		candidateNodes = filtered
	}

	var matches []Match
	for _, node := range candidateNodes {
		m := Match{}
		if firstPattern.Variable != "" {
			m[firstPattern.Variable] = node
		}
		matches = append(matches, m)
	}

	if len(pattern.Nodes) == 1 && len(pattern.Relationships) == 0 {
		if where != nil {
			matches = filterMatchesInSnapshot(matches, where)
		}
		return matches
	}

	// Handle relationship patterns
	for _, relPattern := range pattern.Relationships {
		var newMatches []Match
		for _, match := range matches {
			fromVar := pattern.Nodes[relPattern.FromIndex].Variable
			fromEntity, ok := match[fromVar]
			if !ok {
				continue
			}
			fromNode, ok := fromEntity.(*Node)
			if !ok {
				continue
			}

			for _, relID := range snap.nodeRelIndex[fromNode.ID] {
				rel, ok := snap.rels[relID]
				if !ok {
					continue
				}
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

				var targetID string
				switch relPattern.Direction {
				case "->":
					if rel.FromNodeID != fromNode.ID {
						continue
					}
					targetID = rel.ToNodeID
				case "<-":
					if rel.ToNodeID != fromNode.ID {
						continue
					}
					targetID = rel.FromNodeID
				default:
					if rel.FromNodeID == fromNode.ID {
						targetID = rel.ToNodeID
					} else {
						targetID = rel.FromNodeID
					}
				}

				targetNode, ok := snap.nodes[targetID]
				if !ok {
					continue
				}
				toPattern := pattern.Nodes[relPattern.ToIndex]
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
				if len(toPattern.Properties) > 0 && !matchesProperties(targetNode.Properties, toPattern.Properties) {
					continue
				}

				newMatch := make(Match)
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
		matches = newMatches
	}

	if where != nil {
		matches = filterMatchesInSnapshot(matches, where)
	}
	return matches
}

// filterMatchesInSnapshot applies WHERE conditions to snapshot matches.
func filterMatchesInSnapshot(matches []Match, where *WhereClause) []Match {
	if where.BoolExpr != nil {
		var result []Match
		for _, match := range matches {
			if evalBoolExpr(where.BoolExpr, match) {
				result = append(result, match)
			}
		}
		return result
	}

	var result []Match
	for _, match := range matches {
		allMatch := true
		for _, cond := range where.Conditions {
			entity, ok := match[cond.Variable]
			if !ok {
				allMatch = false
				break
			}
			var propVal interface{}
			if node, ok := entity.(*Node); ok {
				propVal = node.Properties[cond.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				propVal = rel.Properties[cond.Property]
			} else if cond.Property == "" {
				propVal = entity
			} else {
				allMatch = false
				break
			}
			if cond.FunctionName != "" {
				propVal = applyScalarFunction(cond.FunctionName, propVal)
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

// ============================================================================
// Pipeline (WITH clause) execution
// ============================================================================

// executePipelineQuery handles queries that chain MATCH stages via WITH.
func (g *DiskGraph) executePipelineQuery(query *Query, embedder Embedder) (*QueryResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	pipeline := query.Pipeline

	// Start with one empty binding so stage 0 can merge into it
	currentBindings := []Match{{}}

	for _, stage := range pipeline.Stages {
		var nextBindings []Match

		for _, binding := range currentBindings {
			var matches []Match
			if stage.MatchPattern != nil {
				matches = g.findMatchesWithExisting(stage.MatchPattern, stage.WhereClause, binding)
			} else {
				matches = []Match{copyMatch(binding)}
			}

			for _, match := range matches {
				if len(stage.WithVars) > 0 {
					// Project through WITH — keep only the listed variables
					projected := make(Match, len(stage.WithVars))
					for _, v := range stage.WithVars {
						if val, ok := match[v]; ok {
							projected[v] = val
						}
					}
					nextBindings = append(nextBindings, projected)
				} else {
					// Final stage (no explicit WITH) — keep everything
					nextBindings = append(nextBindings, match)
				}
			}
		}

		currentBindings = nextBindings
	}

	return buildResult(currentBindings, query.ReturnClause), nil
}

// findMatchesWithExisting is like findMatchesUnlocked but seeds the search with
// an existing binding. If the first node pattern's variable is already bound, that
// node is used as the anchor; otherwise the normal label/property index scan runs.
func (g *DiskGraph) findMatchesWithExisting(pattern *MatchPattern, where *WhereClause, existing Match) []Match {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return []Match{copyMatch(existing)}
	}

	firstPattern := pattern.Nodes[0]
	var candidateNodes []*Node

	// Anchor: use the already-bound node directly
	if firstPattern.Variable != "" {
		if entity, ok := existing[firstPattern.Variable]; ok {
			if node, ok := entity.(*Node); ok {
				candidateNodes = []*Node{node}
			}
		}
	}

	// Fall back to label/property index scan
	if candidateNodes == nil {
		if len(firstPattern.Labels) > 0 {
			nodeIDs := g.labelIndex[firstPattern.Labels[0]]
			for _, id := range nodeIDs {
				if n := g.getNodeUnlocked(id); n != nil && n.ValidTo == nil {
					candidateNodes = append(candidateNodes, n)
				}
			}
		} else {
			allNodes, _ := g.boltStore.GetAllNodes()
			for _, n := range allNodes {
				if n.ValidTo == nil {
					candidateNodes = append(candidateNodes, n)
				}
			}
		}
		if len(firstPattern.Properties) > 0 {
			var filtered []*Node
			for _, n := range candidateNodes {
				if matchesProperties(n.Properties, firstPattern.Properties) {
					filtered = append(filtered, n)
				}
			}
			candidateNodes = filtered
		}
	}

	// Seed initial matches from existing binding
	var matches []Match
	for _, node := range candidateNodes {
		m := copyMatch(existing)
		if firstPattern.Variable != "" {
			m[firstPattern.Variable] = node
		}
		matches = append(matches, m)
	}

	// Single-node pattern
	if len(pattern.Nodes) == 1 && len(pattern.Relationships) == 0 {
		if where != nil {
			matches = g.filterMatchesUnlocked(matches, where)
		}
		return matches
	}

	// Expand relationship patterns
	for _, relPattern := range pattern.Relationships {
		var newMatches []Match

		for _, match := range matches {
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

			rels := g.getRelationshipsForNodeUnlocked(fromNode.ID)
			for _, rel := range rels {
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
					if rel.FromNodeID == fromNode.ID {
						targetNodeID = rel.ToNodeID
					} else {
						targetNodeID = rel.FromNodeID
					}
				}

				targetNode := g.getNodeUnlocked(targetNodeID)
				if targetNode == nil || targetNode.ValidTo != nil {
					continue
				}

				toIdx := relPattern.ToIndex
				if toIdx >= len(pattern.Nodes) {
					continue
				}
				toPattern := pattern.Nodes[toIdx]

				// If the target variable is already bound, it must match
				if toPattern.Variable != "" {
					if boundEntity, ok := existing[toPattern.Variable]; ok {
						if boundNode, ok := boundEntity.(*Node); ok {
							if targetNode.ID != boundNode.ID {
								continue
							}
						}
					}
				}

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

				if len(toPattern.Properties) > 0 && !matchesProperties(targetNode.Properties, toPattern.Properties) {
					continue
				}

				newMatch := copyMatch(match)
				if toPattern.Variable != "" {
					newMatch[toPattern.Variable] = targetNode
				}
				if relPattern.Variable != "" {
					newMatch[relPattern.Variable] = rel
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

// findMatchesUnlocked finds pattern matches using indexes (caller must hold lock).
// Optional params are injected into each match for $param resolution in WHERE.
func (g *DiskGraph) findMatchesUnlocked(pattern *MatchPattern, where *WhereClause, params ...map[string]interface{}) []map[string]interface{} {
	if pattern == nil || len(pattern.Nodes) == 0 {
		return nil
	}

	// Extract optional params
	var queryParams map[string]interface{}
	if len(params) > 0 {
		queryParams = params[0]
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
		if len(queryParams) > 0 {
			match["__params__"] = queryParams
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

			if relPattern.VarLength {
				// Variable-length path: BFS within [minHops, maxHops]
				minHops := relPattern.MinHops
				if minHops == 0 {
					minHops = 1
				}
				maxHops := relPattern.MaxHops
				if maxHops == 0 {
					maxHops = 10
				}

				type bfsState struct {
					nodeID string
					hops   int
				}
				visited := map[string]bool{fromNode.ID: true}
				queue := []bfsState{{fromNode.ID, 0}}

				for len(queue) > 0 {
					cur := queue[0]
					queue = queue[1:]

					if cur.hops >= minHops {
						targetNode := g.getNodeUnlocked(cur.nodeID)
						if targetNode != nil && targetNode.ValidTo == nil && cur.nodeID != fromNode.ID {
							toIdx := relPattern.ToIndex
							if toIdx < len(pattern.Nodes) {
								toPattern := pattern.Nodes[toIdx]
								if g.nodeMatchesPattern(targetNode, toPattern) {
									newMatch := make(map[string]interface{})
									for k, v := range match {
										newMatch[k] = v
									}
									if toPattern.Variable != "" {
										newMatch[toPattern.Variable] = targetNode
									}
									newMatches = append(newMatches, newMatch)
								}
							}
						}
					}

					if cur.hops < maxHops {
						rels := g.getRelationshipsForNodeUnlocked(cur.nodeID)
						for _, rel := range rels {
							if !g.relMatchesTypeAndDirection(rel, cur.nodeID, relPattern.Types, relPattern.Direction) {
								continue
							}
							nextID := rel.ToNodeID
							if rel.ToNodeID == cur.nodeID {
								nextID = rel.FromNodeID
							}
							if !visited[nextID] {
								visited[nextID] = true
								queue = append(queue, bfsState{nextID, cur.hops + 1})
							}
						}
					}
				}
				continue
			}

			// Single-hop relationship matching
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
				toIdx := relPattern.ToIndex
				if toIdx < len(pattern.Nodes) {
					toPattern := pattern.Nodes[toIdx]
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

// nodeMatchesPattern checks if a node satisfies a NodePattern's label/property constraints
func (g *DiskGraph) nodeMatchesPattern(node *Node, pattern NodePattern) bool {
	for _, reqLabel := range pattern.Labels {
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
	if len(pattern.Properties) > 0 && !matchesProperties(node.Properties, pattern.Properties) {
		return false
	}
	return true
}

// relMatchesTypeAndDirection checks if a relationship traversal from curNodeID satisfies type+direction constraints
func (g *DiskGraph) relMatchesTypeAndDirection(rel *Relationship, curNodeID string, types []string, direction string) bool {
	if len(types) > 0 {
		found := false
		for _, t := range types {
			if rel.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	switch direction {
	case "->":
		return rel.FromNodeID == curNodeID
	case "<-":
		return rel.ToNodeID == curNodeID
	default:
		return rel.FromNodeID == curNodeID || rel.ToNodeID == curNodeID
	}
}

// filterMatchesUnlocked applies WHERE conditions (caller must hold lock)
func (g *DiskGraph) filterMatchesUnlocked(matches []map[string]interface{}, where *WhereClause) []map[string]interface{} {
	if where == nil {
		return matches
	}

	// Full expression tree is available: handles OR, NOT, functions, etc.
	if where.BoolExpr != nil {
		var result []map[string]interface{}
		for _, match := range matches {
			if evalBoolExpr(where.BoolExpr, match) {
				result = append(result, match)
			}
		}
		return result
	}

	if len(where.Conditions) == 0 {
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

			var propVal interface{}
			if node, ok := entity.(*Node); ok {
				propVal = node.Properties[cond.Property]
			} else if rel, ok := entity.(*Relationship); ok {
				propVal = rel.Properties[cond.Property]
			} else if cond.Property == "" {
				// Scalar variable (e.g. from UNWIND)
				propVal = entity
			} else {
				allMatch = false
				break
			}

			if cond.FunctionName != "" {
				propVal = applyScalarFunction(cond.FunctionName, propVal)
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
	case "STARTS WITH":
		ps := fmt.Sprintf("%v", propVal)
		cs := fmt.Sprintf("%v", condVal)
		return strings.HasPrefix(ps, cs)
	case "ENDS WITH":
		ps := fmt.Sprintf("%v", propVal)
		cs := fmt.Sprintf("%v", condVal)
		return strings.HasSuffix(ps, cs)
	case "CONTAINS":
		ps := fmt.Sprintf("%v", propVal)
		cs := fmt.Sprintf("%v", condVal)
		return strings.Contains(ps, cs)
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

		// Load all historical versions of this node so we can embed each one
		allVersions, _ := g.boltStore.GetNodeVersions(node.ID)
		if len(allVersions) == 0 {
			allVersions = []*Node{node} // fallback to current
		}

		// Deduplicate consecutive versions that produce the same embedded text.
		// When the embedded value hasn't changed (e.g. only an unrelated property
		// was updated), we extend the previous embedding's time range instead of
		// creating a duplicate entry.
		type pendingEmb struct {
			text      string
			snapshot  map[string]interface{}
			validFrom time.Time
			validTo   *time.Time
		}
		var deduped []pendingEmb
		for _, version := range allVersions {
			var text string
			switch ec.Mode {
			case "AUTO":
				text = g.generateAutoEmbedText(version)
			case "TEXT":
				text = ec.Text
			case "PROPERTY":
				if propVal, exists := version.Properties[ec.Property]; exists {
					text = fmt.Sprint(propVal)
				}
			}
			if text == "" {
				continue
			}
			if len(deduped) > 0 && deduped[len(deduped)-1].text == text {
				// Same value — extend the time range and update snapshot to latest props
				last := &deduped[len(deduped)-1]
				last.validTo = version.ValidTo
				for k, v := range version.Properties {
					last.snapshot[k] = v
				}
				continue
			}
			snapshot := make(map[string]interface{})
			for k, v := range version.Properties {
				snapshot[k] = v
			}
			deduped = append(deduped, pendingEmb{
				text:      text,
				snapshot:  snapshot,
				validFrom: version.ValidFrom,
				validTo:   version.ValidTo,
			})
		}

		var newEmbs []*Embedding
		for _, pe := range deduped {
			vector, err := embedder.Embed(pe.text)
			if err != nil {
				return nil, fmt.Errorf("failed to embed node %s: %w", node.ID, err)
			}
			newEmbs = append(newEmbs, &Embedding{
				Vector:           vector,
				Model:            "embedder",
				PropertySnapshot: pe.snapshot,
				ValidFrom:        pe.validFrom,
				ValidTo:          pe.validTo,
			})
		}

		if len(newEmbs) == 0 {
			continue
		}

		if err := g.boltStore.SaveEmbedding(node.ID, newEmbs); err != nil {
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
