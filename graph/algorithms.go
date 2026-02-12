package graph

import (
	"container/list"
	"time"
)

// Path represents a path through the graph
type Path struct {
	Nodes         []*Node
	Relationships []*Relationship
	Length        int
}

// ShortestPath finds the shortest path between two nodes using BFS
// Returns nil if no path exists
func (g *Graph) ShortestPath(fromID, toID string) *Path {
	return g.ShortestPathAt(fromID, toID, nil)
}

// ShortestPathAt finds the shortest path between two nodes at a specific point in time
// If asOf is nil, uses current time (same as ShortestPath)
func (g *Graph) ShortestPathAt(fromID, toID string, asOf *time.Time) *Path {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check if both nodes exist and are valid
	fromNode := g.getNodeByID(fromID)
	toNode := g.getNodeByID(toID)
	if fromNode == nil || toNode == nil {
		return nil
	}

	// Check validity at specific time or current
	if asOf == nil {
		if !fromNode.IsCurrentlyValid() || !toNode.IsCurrentlyValid() {
			return nil
		}
	} else {
		if !fromNode.IsValidAt(*asOf) || !toNode.IsValidAt(*asOf) {
			return nil
		}
	}

	// BFS to find shortest path
	queue := list.New()
	visited := make(map[string]bool)
	parent := make(map[string]*pathStep)

	queue.PushBack(fromID)
	visited[fromID] = true

	for queue.Len() > 0 {
		element := queue.Front()
		currentID := element.Value.(string)
		queue.Remove(element)

		if currentID == toID {
			// Found the target - reconstruct path
			return g.reconstructPath(fromID, toID, parent)
		}

		// Explore neighbors
		rels := g.GetRelationshipsForNode(currentID)
		for _, rel := range rels {
			// Check if relationship was valid at the given time
			if asOf == nil {
				if !rel.IsCurrentlyValid() {
					continue
				}
			} else {
				if !rel.IsValidAt(*asOf) {
					continue
				}
			}

			// Determine the neighbor node
			neighborID := ""
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}

			if !visited[neighborID] {
				neighbor := g.getNodeByID(neighborID)
				if neighbor == nil {
					continue
				}

				// Check if neighbor was valid at the given time
				if asOf == nil {
					if !neighbor.IsCurrentlyValid() {
						continue
					}
				} else {
					if !neighbor.IsValidAt(*asOf) {
						continue
					}
				}

				visited[neighborID] = true
				parent[neighborID] = &pathStep{
					nodeID:       neighborID,
					relationship: rel,
					previousID:   currentID,
				}
				queue.PushBack(neighborID)
			}
		}
	}

	// No path found
	return nil
}

// AllPaths finds all simple paths between two nodes (no repeated nodes)
// maxDepth limits the search depth to avoid infinite loops (0 = unlimited)
func (g *Graph) AllPaths(fromID, toID string, maxDepth int) []*Path {
	return g.AllPathsAt(fromID, toID, maxDepth, nil)
}

// AllPathsAt finds all simple paths between two nodes at a specific point in time
// maxDepth limits the search depth to avoid infinite loops (0 = unlimited)
// If asOf is nil, uses current time (same as AllPaths)
func (g *Graph) AllPathsAt(fromID, toID string, maxDepth int, asOf *time.Time) []*Path {
	g.mu.RLock()
	defer g.mu.RUnlock()

	fromNode := g.getNodeByID(fromID)
	toNode := g.getNodeByID(toID)
	if fromNode == nil || toNode == nil {
		return nil
	}

	// Check validity at specific time or current
	if asOf == nil {
		if !fromNode.IsCurrentlyValid() || !toNode.IsCurrentlyValid() {
			return nil
		}
	} else {
		if !fromNode.IsValidAt(*asOf) || !toNode.IsValidAt(*asOf) {
			return nil
		}
	}

	paths := []*Path{}
	visited := make(map[string]bool)
	currentPath := &Path{
		Nodes:         []*Node{},
		Relationships: []*Relationship{},
	}

	g.dfsAllPaths(fromID, toID, visited, currentPath, &paths, maxDepth, 0, asOf)
	return paths
}

// PathExists checks if any path exists between two nodes
func (g *Graph) PathExists(fromID, toID string) bool {
	return g.ShortestPath(fromID, toID) != nil
}

// Helper types and methods

type pathStep struct {
	nodeID       string
	relationship *Relationship
	previousID   string
}

func (g *Graph) reconstructPath(fromID, toID string, parent map[string]*pathStep) *Path {
	path := &Path{
		Nodes:         []*Node{},
		Relationships: []*Relationship{},
	}

	// Reconstruct path backwards
	current := toID
	steps := []string{}
	rels := []*Relationship{}

	for current != fromID {
		steps = append(steps, current)
		step := parent[current]
		if step != nil {
			rels = append(rels, step.relationship)
			current = step.previousID
		} else {
			return nil // Should never happen if BFS worked correctly
		}
	}
	steps = append(steps, fromID)

	// Reverse to get forward path
	for i := len(steps) - 1; i >= 0; i-- {
		node := g.getNodeByID(steps[i])
		if node != nil {
			path.Nodes = append(path.Nodes, node)
		}
	}

	// Reverse relationships
	for i := len(rels) - 1; i >= 0; i-- {
		path.Relationships = append(path.Relationships, rels[i])
	}

	path.Length = len(path.Relationships)
	return path
}

func (g *Graph) dfsAllPaths(currentID, targetID string, visited map[string]bool, currentPath *Path, allPaths *[]*Path, maxDepth, currentDepth int, asOf *time.Time) {
	// Check depth limit
	if maxDepth > 0 && currentDepth >= maxDepth {
		return
	}

	// Mark current node as visited
	visited[currentID] = true
	currentNode := g.getNodeByID(currentID)
	if currentNode == nil {
		visited[currentID] = false
		return
	}

	// Check if node was valid at the given time
	if asOf == nil {
		if !currentNode.IsCurrentlyValid() {
			visited[currentID] = false
			return
		}
	} else {
		if !currentNode.IsValidAt(*asOf) {
			visited[currentID] = false
			return
		}
	}

	currentPath.Nodes = append(currentPath.Nodes, currentNode)

	// Found target - save this path
	if currentID == targetID {
		// Create a copy of the current path
		pathCopy := &Path{
			Nodes:         make([]*Node, len(currentPath.Nodes)),
			Relationships: make([]*Relationship, len(currentPath.Relationships)),
			Length:        len(currentPath.Relationships),
		}
		copy(pathCopy.Nodes, currentPath.Nodes)
		copy(pathCopy.Relationships, currentPath.Relationships)
		*allPaths = append(*allPaths, pathCopy)
	} else {
		// Explore neighbors
		rels := g.GetRelationshipsForNode(currentID)
		for _, rel := range rels {
			// Check if relationship was valid at the given time
			if asOf == nil {
				if !rel.IsCurrentlyValid() {
					continue
				}
			} else {
				if !rel.IsValidAt(*asOf) {
					continue
				}
			}

			// Determine the neighbor node
			neighborID := ""
			if rel.FromNodeID == currentID {
				neighborID = rel.ToNodeID
			} else {
				neighborID = rel.FromNodeID
			}

			if !visited[neighborID] {
				neighbor := g.getNodeByID(neighborID)
				if neighbor == nil {
					continue
				}

				// Check if neighbor was valid at the given time
				if asOf == nil {
					if !neighbor.IsCurrentlyValid() {
						continue
					}
				} else {
					if !neighbor.IsValidAt(*asOf) {
						continue
					}
				}

				// Add relationship to current path and recurse
				currentPath.Relationships = append(currentPath.Relationships, rel)
				g.dfsAllPaths(neighborID, targetID, visited, currentPath, allPaths, maxDepth, currentDepth+1, asOf)
				// Backtrack
				currentPath.Relationships = currentPath.Relationships[:len(currentPath.Relationships)-1]
			}
		}
	}

	// Backtrack
	currentPath.Nodes = currentPath.Nodes[:len(currentPath.Nodes)-1]
	visited[currentID] = false
}

// getNodeByID is a helper that finds a node by ID (assumes lock is held)
func (g *Graph) getNodeByID(id string) *Node {
	// Direct lookup in nodes map
	return g.nodes[id]
}

// GetNodeByID is the public version that acquires the lock
func (g *Graph) GetNodeByID(id string) *Node {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getNodeByID(id)
}
