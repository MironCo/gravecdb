package graph

import (
	"container/heap"
	"math"
	"time"
)

// ============================================================================
// PageRank — power iteration
// ============================================================================

// computePageRank runs PageRank on the current graph.
// Config keys: "label" (string) — restrict to nodes with this label.
// Caller must hold g.mu (at least RLock).
func (g *DiskGraph) computePageRank(config map[string]interface{}) ([]Match, error) {
	const (
		damping    = 0.85
		maxIter    = 20
		convergeTh = 1e-6
	)

	// Gather nodes
	nodes := g.gatherNodes(config)
	if len(nodes) == 0 {
		return nil, nil
	}

	n := len(nodes)
	nodeSet := make(map[string]bool, n)
	for _, nd := range nodes {
		nodeSet[nd.ID] = true
	}

	// Build adjacency: outgoing edges within the node set
	outLinks := make(map[string][]string, n) // nodeID -> []targetIDs
	for _, nd := range nodes {
		relIDs := g.nodeRelIndex[nd.ID]
		for _, relID := range relIDs {
			rel := g.getRelUnlocked(relID)
			if rel == nil || rel.ValidTo != nil {
				continue
			}
			if rel.FromNodeID == nd.ID && nodeSet[rel.ToNodeID] {
				outLinks[nd.ID] = append(outLinks[nd.ID], rel.ToNodeID)
			}
		}
	}

	// Initialize scores
	score := make(map[string]float64, n)
	initVal := 1.0 / float64(n)
	for _, nd := range nodes {
		score[nd.ID] = initVal
	}

	// Power iteration
	base := (1.0 - damping) / float64(n)
	for iter := 0; iter < maxIter; iter++ {
		newScore := make(map[string]float64, n)
		for _, nd := range nodes {
			newScore[nd.ID] = base
		}

		for _, nd := range nodes {
			out := outLinks[nd.ID]
			if len(out) == 0 {
				// Dangling node: distribute evenly
				share := damping * score[nd.ID] / float64(n)
				for _, nd2 := range nodes {
					newScore[nd2.ID] += share
				}
			} else {
				share := damping * score[nd.ID] / float64(len(out))
				for _, target := range out {
					newScore[target] += share
				}
			}
		}

		// Check convergence
		diff := 0.0
		for _, nd := range nodes {
			diff += math.Abs(newScore[nd.ID] - score[nd.ID])
		}
		score = newScore
		if diff < convergeTh {
			break
		}
	}

	// Build results
	matches := make([]Match, 0, n)
	for _, nd := range nodes {
		matches = append(matches, Match{
			"node":  nd,
			"score": math.Round(score[nd.ID]*1e6) / 1e6, // round to 6 decimals
		})
	}
	return matches, nil
}

// ============================================================================
// Louvain — community detection via modularity optimization
// ============================================================================

// computeLouvain runs the Louvain algorithm on the current graph.
// Config keys: "label" (string) — restrict to nodes with this label.
// Caller must hold g.mu (at least RLock).
func (g *DiskGraph) computeLouvain(config map[string]interface{}) ([]Match, error) {
	const maxPasses = 10

	// Gather nodes
	nodes := g.gatherNodes(config)
	if len(nodes) == 0 {
		return nil, nil
	}

	n := len(nodes)
	nodeSet := make(map[string]bool, n)
	idToIdx := make(map[string]int, n)
	for i, nd := range nodes {
		nodeSet[nd.ID] = true
		idToIdx[nd.ID] = i
	}

	// Build weighted adjacency (undirected). Weight = number of edges between pair.
	// adj[i] -> map[j] -> weight
	adj := make([]map[int]float64, n)
	for i := range adj {
		adj[i] = make(map[int]float64)
	}

	totalWeight := 0.0
	for _, nd := range nodes {
		relIDs := g.nodeRelIndex[nd.ID]
		for _, relID := range relIDs {
			rel := g.getRelUnlocked(relID)
			if rel == nil || rel.ValidTo != nil {
				continue
			}
			var neighborID string
			if rel.FromNodeID == nd.ID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}
			if !nodeSet[neighborID] {
				continue
			}
			i := idToIdx[nd.ID]
			j := idToIdx[neighborID]
			if i < j { // count each edge once for total weight
				totalWeight += 1.0
			}
			adj[i][j] += 1.0
		}
	}

	if totalWeight == 0 {
		// No edges: each node is its own community
		matches := make([]Match, 0, n)
		for i, nd := range nodes {
			matches = append(matches, Match{"node": nd, "community": i})
		}
		return matches, nil
	}

	m2 := 2.0 * totalWeight // 2m

	// Initialize: each node in its own community
	community := make([]int, n)
	for i := range community {
		community[i] = i
	}

	// Degree of each node (sum of edge weights)
	degree := make([]float64, n)
	for i := range nodes {
		for _, w := range adj[i] {
			degree[i] += w
		}
	}

	// Phase 1: local moves
	for pass := 0; pass < maxPasses; pass++ {
		moved := false
		for i := 0; i < n; i++ {
			bestComm := community[i]
			bestGain := 0.0

			// Sum of weights inside current community and to other communities
			commWeights := make(map[int]float64) // community -> sum of edge weights from i
			for j, w := range adj[i] {
				commWeights[community[j]] += w
			}

			// Total degree of each community (sum of degrees of members)
			commDegree := make(map[int]float64)
			for k := 0; k < n; k++ {
				commDegree[community[k]] += degree[k]
			}

			// Remove i from its current community for the calculation
			curComm := community[i]
			ki := degree[i]

			for c, wic := range commWeights {
				if c == curComm {
					continue
				}
				// Modularity gain of moving i from curComm to c
				// ΔQ = [wic/m - ki*Σc/(2m²)] - [wi_curComm/m - ki*(Σ_curComm - ki)/(2m²)]
				// Simplified: gain = (wic - commWeights[curComm])/totalWeight +
				//                    ki*(commDegree[curComm] - ki - commDegree[c])/(m2*totalWeight)
				gain := (wic-commWeights[curComm])/totalWeight +
					ki*(commDegree[curComm]-ki-commDegree[c])/(m2*totalWeight)

				if gain > bestGain {
					bestGain = gain
					bestComm = c
				}
			}

			if bestComm != curComm {
				community[i] = bestComm
				moved = true
			}
		}

		if !moved {
			break
		}
	}

	// Normalize community IDs to 0, 1, 2, ...
	commMap := make(map[int]int)
	nextID := 0
	for i := range nodes {
		if _, ok := commMap[community[i]]; !ok {
			commMap[community[i]] = nextID
			nextID++
		}
	}

	matches := make([]Match, 0, n)
	for i, nd := range nodes {
		matches = append(matches, Match{
			"node":      nd,
			"community": commMap[community[i]],
		})
	}
	return matches, nil
}

// ============================================================================
// Earliest Path — temporal Dijkstra
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

// ============================================================================
// Temporal snapshots and snapshot-based path algorithms
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

// ============================================================================
// Helpers
// ============================================================================

// gatherNodes returns current (non-expired) nodes, optionally filtered by label.
// Caller must hold g.mu.
func (g *DiskGraph) gatherNodes(config map[string]interface{}) []*Node {
	label, _ := config["label"].(string)

	if label != "" {
		nodeIDs := g.labelIndex[label]
		nodes := make([]*Node, 0, len(nodeIDs))
		for _, id := range nodeIDs {
			if nd := g.getNodeUnlocked(id); nd != nil && nd.ValidTo == nil {
				nodes = append(nodes, nd)
			}
		}
		return nodes
	}

	// No label filter — all current nodes
	allNodes, err := g.boltStore.GetAllNodes()
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var nodes []*Node
	for _, nd := range allNodes {
		if nd.ValidTo == nil && !seen[nd.ID] {
			seen[nd.ID] = true
			nodes = append(nodes, nd)
		}
	}
	return nodes
}

// getRelUnlocked fetches a relationship by ID, checking cache first.
// Caller must hold g.mu.
func (g *DiskGraph) getRelUnlocked(relID string) *Relationship {
	if rel, ok := g.relCache.Get(relID); ok {
		return rel
	}
	rel, err := g.boltStore.GetRelationship(relID)
	if err != nil || rel == nil {
		return nil
	}
	g.relCache.Add(relID, rel)
	return rel
}
