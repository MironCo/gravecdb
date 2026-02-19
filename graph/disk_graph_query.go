package graph

import (
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
	matches := g.findMatchesUnlocked(query.MatchPattern, query.WhereClause)
	return buildResult(matches, query.ReturnClause), nil
}

// ============================================================================
// Path queries
// ============================================================================

// executePathQueryUnlocked handles shortestPath / allShortestPaths (caller holds read lock).
func (g *DiskGraph) executePathQueryUnlocked(query *Query) (*QueryResult, error) {
	pf := query.MatchPattern.PathFunction

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
			if !evaluateCondition(n.Properties[cond.Property], cond.Operator, cond.Value) {
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

// temporalSnapshot is an in-memory snapshot of graph data valid at a specific time.
type temporalSnapshot struct {
	nodes        map[string]*Node
	nodesByLabel map[string][]string
	nodeRelIndex map[string][]string
	rels         map[string]*Relationship
}

// buildTemporalSnapshot filters nodes/rels to those valid at time t.
func buildTemporalSnapshot(allNodes []*Node, allRels []*Relationship, t time.Time) *temporalSnapshot {
	snap := &temporalSnapshot{
		nodes:        make(map[string]*Node),
		nodesByLabel: make(map[string][]string),
		nodeRelIndex: make(map[string][]string),
		rels:         make(map[string]*Relationship),
	}
	for _, n := range allNodes {
		if n.IsValidAt(t) {
			snap.nodes[n.ID] = n
			for _, label := range n.Labels {
				snap.nodesByLabel[label] = append(snap.nodesByLabel[label], n.ID)
			}
		}
	}
	for _, r := range allRels {
		if r.IsValidAt(t) {
			snap.rels[r.ID] = r
			snap.nodeRelIndex[r.FromNodeID] = append(snap.nodeRelIndex[r.FromNodeID], r.ID)
			snap.nodeRelIndex[r.ToNodeID] = append(snap.nodeRelIndex[r.ToNodeID], r.ID)
		}
	}
	return snap
}

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
	var result []Match
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
			if !evaluateCondition(props[cond.Property], cond.Operator, cond.Value) {
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

			// Get relationships for this node using index
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
