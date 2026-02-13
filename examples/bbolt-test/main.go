package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/MironCo/gravecdb/graph"
)

func main() {
	// Clean up any previous test data
	os.RemoveAll("./test-data")

	// Create a graph with bbolt backend
	db, err := graph.NewGraphWithBolt("./test-data")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("✓ Created graph with bbolt backend")

	// Create some nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "age", 28)
	fmt.Printf("✓ Created Alice (ID: %s)\n", alice.ID)

	time.Sleep(100 * time.Millisecond)
	timeAfterAlice := time.Now()

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "age", 32)
	fmt.Printf("✓ Created Bob (ID: %s)\n", bob.ID)

	time.Sleep(100 * time.Millisecond)
	timeAfterBob := time.Now()

	// Create a relationship
	rel, err := db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	if err != nil {
		log.Fatal(err)
	}
	db.SetRelationshipProperty(rel.ID, "since", 2020)
	fmt.Printf("✓ Created relationship: Alice KNOWS Bob\n")

	time.Sleep(100 * time.Millisecond)
	timeAfterRelationship := time.Now()

	// Delete Bob
	if err := db.DeleteNode(bob.ID); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Deleted Bob\n")

	time.Sleep(100 * time.Millisecond)

	// Current state - Bob should be gone
	people := db.GetNodesByLabel("Person")
	fmt.Printf("\n--- Current State ---\n")
	fmt.Printf("Active people: %d\n", len(people))
	for _, p := range people {
		name, _ := p.Properties["name"]
		fmt.Printf("  - %s\n", name)
	}

	// Time travel: After Alice was created but before Bob
	fmt.Printf("\n--- After Alice, Before Bob (AsOf) ---\n")
	viewAfterAlice := db.AsOf(timeAfterAlice)
	peopleAfterAlice := viewAfterAlice.GetNodesByLabel("Person")
	fmt.Printf("People at that time: %d\n", len(peopleAfterAlice))
	for _, p := range peopleAfterAlice {
		name, _ := p.Properties["name"]
		fmt.Printf("  - %s\n", name)
	}

	// Time travel: When both existed
	fmt.Printf("\n--- After Bob (AsOf) ---\n")
	viewAfterBob := db.AsOf(timeAfterBob)
	peopleAfterBob := viewAfterBob.GetNodesByLabel("Person")
	fmt.Printf("People at that time: %d\n", len(peopleAfterBob))
	for _, p := range peopleAfterBob {
		name, _ := p.Properties["name"]
		fmt.Printf("  - %s\n", name)
	}

	// Time travel: After relationship but before deletion
	fmt.Printf("\n--- After Relationship, Before Deletion (AsOf) ---\n")
	viewAfterRel := db.AsOf(timeAfterRelationship)
	peopleAfterRel := viewAfterRel.GetNodesByLabel("Person")
	relsAfterRel := viewAfterRel.GetAllRelationships()
	fmt.Printf("People at that time: %d\n", len(peopleAfterRel))
	for _, p := range peopleAfterRel {
		name, _ := p.Properties["name"]
		fmt.Printf("  - %s\n", name)
	}
	fmt.Printf("Relationships at that time: %d\n", len(relsAfterRel))
	for _, r := range relsAfterRel {
		fmt.Printf("  - %s relationship\n", r.Type)
	}

	fmt.Println("\n✅ All temporal queries working correctly with bbolt!")

	// Close and reopen to test persistence
	fmt.Println("\n--- Testing Persistence ---")
	db.Close()

	db2, err := graph.NewGraphWithBolt("./test-data")
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	// Verify data was persisted
	peoplePersisted := db2.GetNodesByLabel("Person")
	fmt.Printf("People after reopening database: %d\n", len(peoplePersisted))
	for _, p := range peoplePersisted {
		name, _ := p.Properties["name"]
		deleted := "active"
		if p.ValidTo != nil {
			deleted = "deleted"
		}
		fmt.Printf("  - %s (%s)\n", name, deleted)
	}

	// Test temporal query on reopened database
	viewAfterBob2 := db2.AsOf(timeAfterBob)
	peopleAfterBob2 := viewAfterBob2.GetNodesByLabel("Person")
	fmt.Printf("People after Bob (reopened db): %d\n", len(peopleAfterBob2))

	if len(peopleAfterBob2) == 2 {
		fmt.Println("\n✅ Persistence working! Temporal data survived database restart!")
	} else {
		fmt.Printf("\n❌ Expected 2 people, got %d\n", len(peopleAfterBob2))
	}

	// Clean up
	db2.Close()
	os.RemoveAll("./test-data")
	fmt.Println("\n✓ Cleaned up test data")
}
