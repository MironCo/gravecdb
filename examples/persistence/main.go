package main

import (
	"fmt"
	"os"

	"github.com/miron/go-graph-database/graph"
)

func main() {
	// Specify where the database files should be stored
	dataDir := "./graphdb_data"

	// Clean up any existing database for demo purposes
	// In a real application, you'd want to keep this data!
	os.RemoveAll(dataDir)

	fmt.Println("=== Creating a new persistent graph database ===")

	// Create a graph database with persistence enabled
	// This will create a WAL (write-ahead log) and snapshot files in dataDir
	db, err := graph.NewGraphWithPersistence(dataDir)
	if err != nil {
		panic(err)
	}

	// Create some nodes
	// Each operation is automatically logged to the WAL before being applied
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "age", 30)

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "age", 25)

	company := db.CreateNode("Company")
	db.SetNodeProperty(company.ID, "name", "TechCorp")
	db.SetNodeProperty(company.ID, "founded", 2020)

	// Create relationships
	// These are also logged to the WAL
	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	db.SetRelationshipProperty(friendship.ID, "since", 2015)

	employment1, _ := db.CreateRelationship("WORKS_AT", alice.ID, company.ID)
	db.SetRelationshipProperty(employment1.ID, "role", "Engineer")

	employment2, _ := db.CreateRelationship("WORKS_AT", bob.ID, company.ID)
	db.SetRelationshipProperty(employment2.ID, "role", "Designer")

	fmt.Println("Created nodes and relationships:")
	people := db.GetNodesByLabel("Person")
	for _, person := range people {
		name, _ := person.GetProperty("name")
		fmt.Printf("  - Person: %s\n", name)
	}

	// Create a snapshot to demonstrate the snapshot mechanism
	// This saves the entire graph state to disk and clears the WAL
	// In production, you'd do this periodically (e.g., every hour or when WAL reaches certain size)
	fmt.Println("\n=== Creating snapshot ===")
	if err := db.Snapshot(); err != nil {
		panic(err)
	}
	fmt.Println("Snapshot created successfully!")

	// Close the database properly to flush any pending writes
	// This is important - always call Close() before shutting down
	if err := db.Close(); err != nil {
		panic(err)
	}
	fmt.Println("Database closed.")

	// Now let's simulate a restart by creating a new database instance
	// pointing to the same data directory
	fmt.Println("\n=== Simulating database restart ===")
	fmt.Println("Recovering database from disk...")

	// This will:
	// 1. Load the snapshot we created earlier
	// 2. Replay any operations from the WAL that occurred after the snapshot
	db2, err := graph.NewGraphWithPersistence(dataDir)
	if err != nil {
		panic(err)
	}

	// Verify that all our data was recovered
	fmt.Println("\nRecovered data:")
	people = db2.GetNodesByLabel("Person")
	for _, person := range people {
		name, _ := person.GetProperty("name")
		age, _ := person.GetProperty("age")
		fmt.Printf("  - %s (age: %v)\n", name, age)

		// Get their relationships
		rels := db2.GetRelationshipsForNode(person.ID)
		for _, rel := range rels {
			var otherNodeID string
			if rel.FromNodeID == person.ID {
				otherNodeID = rel.ToNodeID
			} else {
				otherNodeID = rel.FromNodeID
			}
			otherNode, _ := db2.GetNode(otherNodeID)
			otherName, _ := otherNode.GetProperty("name")
			fmt.Printf("    -> %s: %s\n", rel.Type, otherName)
		}
	}

	// Add some new data after recovery to demonstrate WAL is still working
	fmt.Println("\n=== Adding new data after recovery ===")
	charlie := db2.CreateNode("Person")
	db2.SetNodeProperty(charlie.ID, "name", "Charlie")
	db2.SetNodeProperty(charlie.ID, "age", 35)

	newFriendship, _ := db2.CreateRelationship("FRIENDS_WITH", alice.ID, charlie.ID)
	db2.SetRelationshipProperty(newFriendship.ID, "since", 2020)

	fmt.Println("Added Charlie and created friendship with Alice")

	// Close the database
	if err := db2.Close(); err != nil {
		panic(err)
	}

	fmt.Println("\n=== Demo complete! ===")
	fmt.Printf("Check the '%s' directory to see the database files:\n", dataDir)
	fmt.Println("  - wal.log: Write-ahead log containing all operations")
	fmt.Println("  - snapshot.json: Latest snapshot of the graph state")
}
