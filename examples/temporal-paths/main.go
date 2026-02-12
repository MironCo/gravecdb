package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/miron/go-graph-database/graph"
)

func main() {
	fmt.Println("=== Temporal Path-Finding Demo ===")
	fmt.Println("Finding paths through TIME! 🕐\n")

	db := graph.NewGraph()

	// Create nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")

	david := db.CreateNode("Person")
	db.SetNodeProperty(david.ID, "name", "David")

	// Record time points
	t0 := time.Now()
	time.Sleep(100 * time.Millisecond)

	// Initial connections: Alice -> Bob -> David
	fmt.Println("=== Time T1: Initial Network ===")
	fmt.Println("Creating: Alice -> Bob -> David")
	db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	db.CreateRelationship("KNOWS", bob.ID, david.ID)

	t1 := time.Now()
	time.Sleep(100 * time.Millisecond)

	// Add Carol to the network
	fmt.Println("\n=== Time T2: Carol Joins ===")
	fmt.Println("Creating: Alice -> Carol -> David")
	db.CreateRelationship("KNOWS", alice.ID, carol.ID)
	db.CreateRelationship("KNOWS", carol.ID, david.ID)

	t2 := time.Now()
	time.Sleep(100 * time.Millisecond)

	// Bob leaves (delete his connections)
	fmt.Println("\n=== Time T3: Bob Disconnects ===")
	fmt.Println("Deleting Bob's relationships")
	bobRels := db.GetRelationshipsForNode(bob.ID)
	for _, rel := range bobRels {
		db.DeleteRelationship(rel.ID)
	}

	t3 := time.Now()

	// Now let's find paths at different time points!
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("TEMPORAL PATH ANALYSIS")
	fmt.Println(strings.Repeat("=", 60))

	// Path at T0 (before any connections)
	fmt.Println("\n📅 At T0 (before any connections):")
	path := db.ShortestPathAt(alice.ID, david.ID, &t0)
	if path == nil {
		fmt.Println("  ❌ No path exists")
	} else {
		fmt.Printf("  ✓ Path found: ")
		printPath(path)
	}

	// Path at T1 (only Alice->Bob->David exists)
	fmt.Println("\n📅 At T1 (Alice -> Bob -> David):")
	path = db.ShortestPathAt(alice.ID, david.ID, &t1)
	if path != nil {
		fmt.Printf("  ✓ Shortest path (length %d): ", path.Length)
		printPath(path)
	}

	allPaths := db.AllPathsAt(alice.ID, david.ID, 10, &t1)
	fmt.Printf("  ✓ Total paths: %d\n", len(allPaths))

	// Path at T2 (both routes exist)
	fmt.Println("\n📅 At T2 (Alice -> Bob -> David AND Alice -> Carol -> David):")
	path = db.ShortestPathAt(alice.ID, david.ID, &t2)
	if path != nil {
		fmt.Printf("  ✓ Shortest path (length %d): ", path.Length)
		printPath(path)
	}

	allPaths = db.AllPathsAt(alice.ID, david.ID, 10, &t2)
	fmt.Printf("  ✓ Total paths: %d\n", len(allPaths))
	for i, p := range allPaths {
		fmt.Printf("    Path %d (length %d): ", i+1, p.Length)
		printPath(p)
	}

	// Path at T3 (Bob disconnected, only Carol route exists)
	fmt.Println("\n📅 At T3 (Bob disconnected, only Alice -> Carol -> David):")
	path = db.ShortestPathAt(alice.ID, david.ID, &t3)
	if path != nil {
		fmt.Printf("  ✓ Shortest path (length %d): ", path.Length)
		printPath(path)
	}

	allPaths = db.AllPathsAt(alice.ID, david.ID, 10, &t3)
	fmt.Printf("  ✓ Total paths: %d\n", len(allPaths))

	// Current time (same as T3 since no changes after)
	fmt.Println("\n📅 Current time (same as T3):")
	path = db.ShortestPath(alice.ID, david.ID)
	if path != nil {
		fmt.Printf("  ✓ Shortest path (length %d): ", path.Length)
		printPath(path)
	}

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🎉 Time-travel complete! The graph history is preserved.")
	fmt.Println(strings.Repeat("=", 60))
}

func printPath(path *graph.Path) {
	for i, node := range path.Nodes {
		name := node.Properties["name"]
		fmt.Printf("%s", name)

		if i < len(path.Relationships) {
			rel := path.Relationships[i]
			fmt.Printf(" -[%s]-> ", rel.Type)
		}
	}
	fmt.Println()
}
