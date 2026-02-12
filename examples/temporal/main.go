package main

import (
	"fmt"
	"time"

	"github.com/MironCo/gravecdb/graph"
)

func main() {
	fmt.Println("=== Temporal Graph Database Demo ===\n")

	// Create a new graph database
	db := graph.NewGraph()

	// Record the start time for our demo
	startTime := time.Now()
	fmt.Printf("Demo start time: %s\n\n", startTime.Format("15:04:05"))

	// Create some nodes and relationships
	fmt.Println("--- Creating initial data ---")
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "Engineer")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "Designer")

	techCorp := db.CreateNode("Company")
	db.SetNodeProperty(techCorp.ID, "name", "TechCorp")

	// Alice and Bob both work at TechCorp
	aliceJob1, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(aliceJob1.ID, "title", "Junior Engineer")

	bobJob, _ := db.CreateRelationship("WORKS_AT", bob.ID, techCorp.ID)
	db.SetRelationshipProperty(bobJob.ID, "title", "Senior Designer")

	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	db.SetRelationshipProperty(friendship.ID, "since", 2020)

	fmt.Println("Created Alice, Bob, TechCorp")
	fmt.Println("Alice and Bob both work at TechCorp")
	fmt.Println("Alice and Bob are friends\n")

	// Record time after initial setup
	time1 := time.Now()
	time.Sleep(100 * time.Millisecond) // Small delay to ensure distinct timestamps

	// === Change 1: Alice gets promoted ===
	fmt.Println("--- Alice gets promoted ---")
	db.DeleteRelationship(aliceJob1.ID) // End the junior role
	aliceJob2, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(aliceJob2.ID, "title", "Senior Engineer")
	fmt.Println("Alice is now a Senior Engineer\n")

	// Record time after promotion
	time2 := time.Now()
	time.Sleep(100 * time.Millisecond)

	// === Change 2: Bob leaves TechCorp for a startup ===
	fmt.Println("--- Bob changes jobs ---")
	db.DeleteRelationship(bobJob.ID) // Bob leaves TechCorp

	startup := db.CreateNode("Company")
	db.SetNodeProperty(startup.ID, "name", "CoolStartup")

	bobJob2, _ := db.CreateRelationship("WORKS_AT", bob.ID, startup.ID)
	db.SetRelationshipProperty(bobJob2.ID, "title", "Design Lead")
	fmt.Println("Bob now works at CoolStartup\n")

	// Record time after Bob's job change
	time3 := time.Now()
	time.Sleep(100 * time.Millisecond)

	// === Change 3: Alice and Bob stop being friends ===
	fmt.Println("--- Alice and Bob's friendship ends ---")
	db.DeleteRelationship(friendship.ID)
	fmt.Println("Friendship deleted\n")

	// === Now let's do temporal queries! ===
	fmt.Println("\n=== Temporal Queries ===\n")

	// Query 1: Current state
	fmt.Println("1. Current state (now):")
	people := db.GetNodesByLabel("Person")
	for _, person := range people {
		name, _ := person.GetProperty("name")
		rels := db.GetRelationshipsForNode(person.ID)
		fmt.Printf("  %s:\n", name)
		for _, rel := range rels {
			if rel.Type == "WORKS_AT" && rel.FromNodeID == person.ID {
				company, _ := db.GetNode(rel.ToNodeID)
				companyName, _ := company.GetProperty("name")
				title, _ := rel.GetProperty("title")
				fmt.Printf("    - Works at %s as %s\n", companyName, title)
			}
		}
	}
	fmt.Println()

	// Query 2: State at time1 (right after initial setup)
	fmt.Println("2. Graph state at beginning (after initial setup):")
	view1 := db.AsOf(time1)
	peopleAt1 := view1.GetNodesByLabel("Person")
	for _, person := range peopleAt1 {
		name, _ := person.GetProperty("name")
		rels := view1.GetRelationshipsForNode(person.ID)
		fmt.Printf("  %s:\n", name)
		for _, rel := range rels {
			if rel.Type == "WORKS_AT" && rel.FromNodeID == person.ID {
				company, _ := view1.GetNode(rel.ToNodeID)
				companyName, _ := company.GetProperty("name")
				title, _ := rel.GetProperty("title")
				fmt.Printf("    - Works at %s as %s\n", companyName, title)
			}
			if rel.Type == "FRIENDS_WITH" {
				var otherID string
				if rel.FromNodeID == person.ID {
					otherID = rel.ToNodeID
				} else {
					otherID = rel.FromNodeID
				}
				other, _ := view1.GetNode(otherID)
				otherName, _ := other.GetProperty("name")
				fmt.Printf("    - Friends with %s\n", otherName)
			}
		}
	}
	fmt.Println()

	// Query 3: State at time2 (after Alice's promotion)
	fmt.Println("3. Graph state after Alice's promotion:")
	view2 := db.AsOf(time2)
	aliceAt2, _ := view2.GetNode(alice.ID)
	if aliceAt2 != nil {
		name, _ := aliceAt2.GetProperty("name")
		rels := view2.GetRelationshipsForNode(alice.ID)
		fmt.Printf("  %s:\n", name)
		for _, rel := range rels {
			if rel.Type == "WORKS_AT" && rel.FromNodeID == alice.ID {
				company, _ := view2.GetNode(rel.ToNodeID)
				companyName, _ := company.GetProperty("name")
				title, _ := rel.GetProperty("title")
				fmt.Printf("    - Works at %s as %s\n", companyName, title)
			}
		}
	}
	fmt.Println()

	// Query 4: State at time3 (after Bob changed jobs)
	fmt.Println("4. Where did everyone work after Bob changed jobs?")
	view3 := db.AsOf(time3)
	peopleAt3 := view3.GetNodesByLabel("Person")
	for _, person := range peopleAt3 {
		name, _ := person.GetProperty("name")
		rels := view3.GetRelationshipsForNode(person.ID)
		fmt.Printf("  %s:\n", name)
		for _, rel := range rels {
			if rel.Type == "WORKS_AT" && rel.FromNodeID == person.ID {
				company, _ := view3.GetNode(rel.ToNodeID)
				companyName, _ := company.GetProperty("name")
				title, _ := rel.GetProperty("title")
				fmt.Printf("    - Works at %s as %s\n", companyName, title)
			}
		}
	}
	fmt.Println()

	// Query 5: Were Alice and Bob friends at time3?
	fmt.Println("5. Were Alice and Bob still friends after Bob changed jobs?")
	view3Again := db.AsOf(time3)
	aliceRelsAt3 := view3Again.GetRelationshipsForNode(alice.ID)
	friendshipExists := false
	for _, rel := range aliceRelsAt3 {
		if rel.Type == "FRIENDS_WITH" {
			friendshipExists = true
			break
		}
	}
	if friendshipExists {
		fmt.Println("  Yes, they were still friends")
	} else {
		fmt.Println("  No, they were no longer friends")
	}
	fmt.Println()

	// Query 6: Are they friends now?
	fmt.Println("6. Are Alice and Bob friends now (current state)?")
	currentRels := db.GetRelationshipsForNode(alice.ID)
	currentFriendship := false
	for _, rel := range currentRels {
		if rel.Type == "FRIENDS_WITH" {
			currentFriendship = true
			break
		}
	}
	if currentFriendship {
		fmt.Println("  Yes, they are friends")
	} else {
		fmt.Println("  No, the friendship ended")
	}
	fmt.Println()

	fmt.Println("=== Demo Complete ===")
	fmt.Println("\nKey takeaway: We can query the graph at any point in time!")
	fmt.Println("All historical data is preserved through ValidFrom/ValidTo timestamps.")
}