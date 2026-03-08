package graph

import "testing"

// TestShortestPath tests finding the shortest path between nodes
func TestShortestPath(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create a network of nodes
	alice, _ := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob, _ := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	carol, _ := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")

	david, _ := db.CreateNode("Person")
	db.SetNodeProperty(david.ID, "name", "David")

	eve, _ := db.CreateNode("Person")
	db.SetNodeProperty(eve.ID, "name", "Eve")

	frank, _ := db.CreateNode("Person")
	db.SetNodeProperty(frank.ID, "name", "Frank")

	// Create relationships
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

	// Find shortest path from Alice to Frank
	shortestPath := db.ShortestPath(alice.ID, frank.ID)
	if shortestPath == nil {
		t.Fatal("Expected to find a path from Alice to Frank")
	}

	// Verify path length
	// Shortest should be: Alice -> Carol -> Eve -> Frank (length 3)
	if shortestPath.Length != 3 {
		t.Errorf("Expected shortest path length to be 3, got %d", shortestPath.Length)
	}

	// Verify path contains correct number of nodes
	if len(shortestPath.Nodes) != 4 {
		t.Errorf("Expected path to contain 4 nodes, got %d", len(shortestPath.Nodes))
	}

	// Verify start and end nodes
	if shortestPath.Nodes[0].ID != alice.ID {
		t.Error("Path should start with Alice")
	}
	if shortestPath.Nodes[len(shortestPath.Nodes)-1].ID != frank.ID {
		t.Error("Path should end with Frank")
	}
}

// TestAllPaths tests finding all paths between nodes
func TestAllPaths(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create a simple network
	alice, _ := db.CreateNode("Person")
	bob, _ := db.CreateNode("Person")
	carol, _ := db.CreateNode("Person")
	david, _ := db.CreateNode("Person")

	// Create relationships forming multiple paths
	db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	db.CreateRelationship("KNOWS", alice.ID, carol.ID)
	db.CreateRelationship("KNOWS", bob.ID, david.ID)
	db.CreateRelationship("KNOWS", carol.ID, david.ID)

	// Find all paths from Alice to David
	allPaths := db.AllPaths(alice.ID, david.ID, 10)

	// Should find 2 paths: Alice->Bob->David and Alice->Carol->David
	if len(allPaths) != 2 {
		t.Errorf("Expected 2 paths from Alice to David, got %d", len(allPaths))
	}

	// Verify both paths have length 2
	for i, path := range allPaths {
		if path.Length != 2 {
			t.Errorf("Path %d: expected length 2, got %d", i, path.Length)
		}
		if len(path.Nodes) != 3 {
			t.Errorf("Path %d: expected 3 nodes, got %d", i, len(path.Nodes))
		}
	}
}

// TestPathExistence tests checking if a path exists
func TestPathExistence(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice, _ := db.CreateNode("Person")
	bob, _ := db.CreateNode("Person")
	carol, _ := db.CreateNode("Person")
	isolated, _ := db.CreateNode("Person")

	// Create a path Alice -> Bob -> Carol
	db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	db.CreateRelationship("KNOWS", bob.ID, carol.ID)

	// Test path exists
	if !db.PathExists(alice.ID, carol.ID) {
		t.Error("Path should exist from Alice to Carol")
	}

	// Test path doesn't exist to isolated node
	if db.PathExists(alice.ID, isolated.ID) {
		t.Error("Path should not exist from Alice to isolated node")
	}

	// Test bidirectional (graph treats relationships as bidirectional for pathfinding)
	if !db.PathExists(carol.ID, alice.ID) {
		t.Error("Path should exist from Carol to Alice (graph treats relationships as bidirectional)")
	}
}

// TestNoPath tests behavior when no path exists
func TestNoPath(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice, _ := db.CreateNode("Person")
	bob, _ := db.CreateNode("Person")

	// No relationships, so no path
	path := db.ShortestPath(alice.ID, bob.ID)
	if path != nil {
		t.Error("Expected no path between disconnected nodes")
	}

	allPaths := db.AllPaths(alice.ID, bob.ID, 10)
	if len(allPaths) != 0 {
		t.Errorf("Expected 0 paths between disconnected nodes, got %d", len(allPaths))
	}
}

// TestSelfPath tests path from node to itself
func TestSelfPath(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice, _ := db.CreateNode("Person")

	// Path from node to itself should have length 0
	path := db.ShortestPath(alice.ID, alice.ID)
	if path == nil {
		t.Fatal("Expected to find path from node to itself")
	}

	if path.Length != 0 {
		t.Errorf("Path from node to itself should have length 0, got %d", path.Length)
	}

	if len(path.Nodes) != 1 {
		t.Errorf("Path from node to itself should contain 1 node, got %d", len(path.Nodes))
	}
}
