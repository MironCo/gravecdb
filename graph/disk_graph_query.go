package graph

import (
	"container/heap"
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
	case "PIPELINE":
		return g.executePipelineQuery(query, embedder)
	case "UNWIND":
		return g.executeUnwindQuery(query)
	case "MERGE":
		return g.executeMergeQuery(query)
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

// executeUnwindQuery expands a list literal into rows and applies RETURN.
func (g *DiskGraph) executeUnwindQuery(query *Query) (*QueryResult, error) {
	uc := query.UnwindClause
	var matches []Match
	for _, item := range uc.List {
		matches = append(matches, Match{uc.Variable: item})
	}
	if query.WhereClause != nil {
		matches = g.filterMatchesUnlocked(matches, query.WhereClause)
	}
	return buildResult(matches, query.ReturnClause), nil
}

// executeMergeQuery finds a node matching the pattern; creates it if absent.
func (g *DiskGraph) executeMergeQuery(query *Query) (*QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	mc := query.MergeClause
	result := &QueryResult{Columns: []string{}, Rows: []map[string]interface{}{}}

	for _, mn := range mc.Nodes {
		// Try to find an existing node with matching labels + properties
		var found *Node
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
			found = g.createNodeUnlocked(mn.Labels...)
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

// earliestHeapItem is one entry in the Dijkstra priority queue.
type earliestHeapItem struct {
	arrivalTime time.Time
	nodeID      string
}

type earliestHeap []earliestHeapItem

func (h earliestHeap) Len() int            { return len(h) }
func (h earliestHeap) Less(i, j int) bool  { return h[i].arrivalTime.Before(h[j].arrivalTime) }
func (h earliestHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *earliestHeap) Push(x interface{}) { *h = append(*h, x.(earliestHeapItem)) }
func (h *earliestHeap) Pop() interface{} {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

type earliestParent struct {
	rel        *Relationship
	previousID string
}

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

// earliestPathDijkstra finds the path from fromID to toID that minimises arrival time.
// Cost of traversing a relationship = max(current_arrival_time, rel.ValidFrom).
func earliestPathDijkstra(
	allNodes map[string]*Node,
	relsByNode map[string][]*Relationship,
	fromID, toID string,
) (*Path, time.Time) {
	startNode, ok := allNodes[fromID]
	if !ok {
		return nil, time.Time{}
	}

	dist := make(map[string]time.Time)
	parent := make(map[string]*earliestParent)

	h := &earliestHeap{}
	heap.Init(h)

	dist[fromID] = startNode.ValidFrom
	heap.Push(h, earliestHeapItem{arrivalTime: startNode.ValidFrom, nodeID: fromID})

	for h.Len() > 0 {
		cur := heap.Pop(h).(earliestHeapItem)
		nodeID := cur.nodeID
		t := cur.arrivalTime

		// Stale entry — already found a better path to this node
		if best, ok := dist[nodeID]; ok && t.After(best) {
			continue
		}

		if nodeID == toID {
			break
		}

		for _, rel := range relsByNode[nodeID] {
			// Relationship expired before we arrived — can't use it
			if rel.ValidTo != nil && rel.ValidTo.Before(t) {
				continue
			}

			// Arrival at neighbor = max(now, when the relationship became valid)
			arrivalAtNeighbor := t
			if rel.ValidFrom.After(t) {
				arrivalAtNeighbor = rel.ValidFrom
			}

			neighborID := rel.ToNodeID

			// Neighbor must have existed by the time we arrive
			neighbor, ok := allNodes[neighborID]
			if !ok || neighbor.ValidFrom.After(arrivalAtNeighbor) {
				continue
			}

			if best, seen := dist[neighborID]; !seen || arrivalAtNeighbor.Before(best) {
				dist[neighborID] = arrivalAtNeighbor
				parent[neighborID] = &earliestParent{rel: rel, previousID: nodeID}
				heap.Push(h, earliestHeapItem{arrivalTime: arrivalAtNeighbor, nodeID: neighborID})
			}
		}
	}

	arrivalTime, reached := dist[toID]
	if !reached {
		return nil, time.Time{}
	}

	// Reconstruct path back-to-front
	path := &Path{}
	curID := toID
	for curID != fromID {
		step := parent[curID]
		if step == nil {
			return nil, time.Time{}
		}
		path.Nodes = append([]*Node{allNodes[curID]}, path.Nodes...)
		path.Relationships = append([]*Relationship{step.rel}, path.Relationships...)
		curID = step.previousID
	}
	path.Nodes = append([]*Node{allNodes[fromID]}, path.Nodes...)
	path.Length = len(path.Relationships)

	return path, arrivalTime
}

// buildSnapshotFromTimeClause resolves a TimeClause and returns a temporalSnapshot.
func (g *DiskGraph) buildSnapshotFromTimeClause(tc *TimeClause) (*temporalSnapshot, error) {
	var queryTime time.Time
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
		if earliest == nil {
			return nil, nil
		}
		queryTime = *earliest
	} else {
		queryTime = time.Unix(tc.Timestamp, 0)
	}
	allNodes, _ := g.boltStore.GetAllNodes()
	allRels, _ := g.boltStore.GetAllRelationships()
	return buildTemporalSnapshot(allNodes, allRels, queryTime), nil
}

// candidateNodesInSnapshot returns nodes from a snapshot matching a node pattern.
func candidateNodesInSnapshot(snap *temporalSnapshot, pattern NodePattern) []*Node {
	var candidates []*Node
	if len(pattern.Labels) > 0 {
		for _, id := range snap.nodesByLabel[pattern.Labels[0]] {
			if n, ok := snap.nodes[id]; ok {
				candidates = append(candidates, n)
			}
		}
	} else {
		for _, n := range snap.nodes {
			candidates = append(candidates, n)
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

// shortestPathInSnapshot runs BFS against a temporal snapshot.
func shortestPathInSnapshot(snap *temporalSnapshot, fromID, toID string) *Path {
	if _, ok := snap.nodes[fromID]; !ok {
		return nil
	}
	if _, ok := snap.nodes[toID]; !ok {
		return nil
	}

	queue := []string{fromID}
	visited := map[string]bool{fromID: true}
	parent := map[string]*diskPathStep{}

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		if currentID == toID {
			return reconstructPathInSnapshot(snap, fromID, toID, parent)
		}

		for _, relID := range snap.nodeRelIndex[currentID] {
			rel, ok := snap.rels[relID]
			if !ok {
				continue
			}
			var neighborID string
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if !visited[neighborID] {
				if _, ok := snap.nodes[neighborID]; !ok {
					continue
				}
				visited[neighborID] = true
				parent[neighborID] = &diskPathStep{rel: rel, previousID: currentID}
				queue = append(queue, neighborID)
			}
		}
	}
	return nil
}

func reconstructPathInSnapshot(snap *temporalSnapshot, fromID, toID string, parent map[string]*diskPathStep) *Path {
	path := &Path{Nodes: []*Node{}, Relationships: []*Relationship{}}
	current := toID
	var steps []string
	var rels []*Relationship
	for current != fromID {
		steps = append(steps, current)
		step := parent[current]
		if step == nil {
			return nil
		}
		rels = append(rels, step.rel)
		current = step.previousID
	}
	steps = append(steps, fromID)
	for i := len(steps) - 1; i >= 0; i-- {
		if node, ok := snap.nodes[steps[i]]; ok {
			path.Nodes = append(path.Nodes, node)
		}
	}
	for i := len(rels) - 1; i >= 0; i-- {
		path.Relationships = append(path.Relationships, rels[i])
	}
	path.Length = len(path.Relationships)
	return path
}

// allPathsInSnapshot runs DFS against a temporal snapshot.
func allPathsInSnapshot(snap *temporalSnapshot, fromID, toID string, maxDepth int) []*Path {
	if _, ok := snap.nodes[fromID]; !ok {
		return nil
	}
	if _, ok := snap.nodes[toID]; !ok {
		return nil
	}
	var paths []*Path
	visited := make(map[string]bool)
	currentPath := &Path{Nodes: []*Node{}, Relationships: []*Relationship{}}
	dfsAllPathsInSnapshot(snap, fromID, toID, visited, currentPath, &paths, maxDepth, 0)
	return paths
}

func dfsAllPathsInSnapshot(snap *temporalSnapshot, currentID, targetID string, visited map[string]bool, currentPath *Path, allPaths *[]*Path, maxDepth, depth int) {
	if maxDepth > 0 && depth >= maxDepth {
		return
	}
	visited[currentID] = true
	currentNode, ok := snap.nodes[currentID]
	if !ok {
		visited[currentID] = false
		return
	}
	currentPath.Nodes = append(currentPath.Nodes, currentNode)

	if currentID == targetID {
		pathCopy := &Path{
			Nodes:         make([]*Node, len(currentPath.Nodes)),
			Relationships: make([]*Relationship, len(currentPath.Relationships)),
			Length:        len(currentPath.Relationships),
		}
		copy(pathCopy.Nodes, currentPath.Nodes)
		copy(pathCopy.Relationships, currentPath.Relationships)
		*allPaths = append(*allPaths, pathCopy)
	} else {
		for _, relID := range snap.nodeRelIndex[currentID] {
			rel, ok := snap.rels[relID]
			if !ok {
				continue
			}
			var neighborID string
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if !visited[neighborID] {
				if _, ok := snap.nodes[neighborID]; !ok {
					continue
				}
				currentPath.Relationships = append(currentPath.Relationships, rel)
				dfsAllPathsInSnapshot(snap, neighborID, targetID, visited, currentPath, allPaths, maxDepth, depth+1)
				currentPath.Relationships = currentPath.Relationships[:len(currentPath.Relationships)-1]
			}
		}
	}
	currentPath.Nodes = currentPath.Nodes[:len(currentPath.Nodes)-1]
	visited[currentID] = false
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
