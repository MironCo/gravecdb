package main

import (
	"fmt"
	"time"

	"github.com/miron/go-graph-database/graph"
)

func main() {
	fmt.Println("=== Graph Path-Finding Demo ===\n")

	// Create a graph
	db := graph.NewGraph()

	// Create a network of nodes
	fmt.Println("Creating graph structure...")
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")

	david := db.CreateNode("Person")
	db.SetNodeProperty(david.ID, "name", "David")

	eve := db.CreateNode("Person")
	db.SetNodeProperty(eve.ID, "name", "Eve")

	frank := db.CreateNode("Person")
	db.SetNodeProperty(frank.ID, "name", "Frank")

	// Create relationships to form a network
	// Alice -> Bob -> David
	// Alice -> Carol -> Eve -> Frank
	// Bob -> Carol
	// David -> Frank
	db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	db.CreateRelationship("KNOWS", alice.ID, carol.ID)
	db.CreateRelationship("KNOWS", bob.ID, david.ID)
	db.CreateRelationship("KNOWS", bob.ID, carol.ID)
	db.CreateRelationship("KNOWS", carol.ID, eve.ID)
	db.CreateRelationship("KNOWS", eve.ID, frank.ID)
	db.CreateRelationship("KNOWS", david.ID, frank.ID)

	fmt.Println("Graph structure:")
	fmt.Println("  Alice -> Bob -> David")
	fmt.Println("  Alice -> Carol -> Eve -> Frank")
	fmt.Println("  Bob -> Carol")
	fmt.Println("  David -> Frank")
	fmt.Println()

	// Find shortest path from Alice to Frank
	fmt.Println("=== Shortest Path ===")
	fmt.Println("Finding shortest path from Alice to Frank...")
	shortestPath := db.ShortestPath(alice.ID, frank.ID)
	if shortestPath != nil {
		fmt.Printf("Found path with length %d:\n", shortestPath.Length)
		printPath(shortestPath)
	} else {
		fmt.Println("No path found")
	}
	fmt.Println()

	// Find all paths from Alice to Frank
	fmt.Println("=== All Paths ===")
	fmt.Println("Finding all paths from Alice to Frank (max depth 10)...")
	allPaths := db.AllPaths(alice.ID, frank.ID, 10)
	fmt.Printf("Found %d paths:\n\n", len(allPaths))
	for i, path := range allPaths {
		fmt.Printf("Path %d (length %d):\n", i+1, path.Length)
		printPath(path)
		fmt.Println()
	}

	// Check if path exists
	fmt.Println("=== Path Existence ===")
	exists := db.PathExists(alice.ID, frank.ID)
	fmt.Printf("Path exists from Alice to Frank: %v\n", exists)

	exists = db.PathExists(alice.ID, alice.ID)
	fmt.Printf("Path exists from Alice to Alice: %v\n", exists)
	fmt.Println()

	// Demonstrate with temporal queries
	fmt.Println("=== Temporal Path Finding ===")
	fmt.Println("Deleting Bob's connection to David...")
	time.Sleep(100 * time.Millisecond)

	// Find the relationship to delete
	bobRels := db.GetRelationshipsForNode(bob.ID)
	for _, rel := range bobRels {
		if rel.ToNodeID == david.ID || rel.FromNodeID == david.ID {
			db.DeleteRelationship(rel.ID)
			break
		}
	}

	fmt.Println("\nFinding shortest path again (after deletion)...")
	newShortestPath := db.ShortestPath(alice.ID, frank.ID)
	if newShortestPath != nil {
		fmt.Printf("Found path with length %d:\n", newShortestPath.Length)
		printPath(newShortestPath)
		fmt.Println("(Notice the path changed because Bob->David was deleted)")
	}
}

func printPath(path *graph.Path) {
	for i, node := range path.Nodes {
		name := node.Properties["name"]
		fmt.Printf("  %s", name)

		if i < len(path.Relationships) {
			rel := path.Relationships[i]
			fmt.Printf(" -[%s]-> ", rel.Type)
		}
	}
	fmt.Println()
}
