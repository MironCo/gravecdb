package main

import (
	"fmt"

	"github.com/miron/go-graph-database/graph"
)

func main() {
	// Create a new graph database
	db := graph.NewGraph()

	// Create some nodes
	alice := db.CreateNode("Person")
	alice.SetProperty("name", "Alice")
	alice.SetProperty("age", 30)

	bob := db.CreateNode("Person")
	bob.SetProperty("name", "Bob")
	bob.SetProperty("age", 25)

	company := db.CreateNode("Company")
	company.SetProperty("name", "TechCorp")
	company.SetProperty("founded", 2020)

	// Create relationships
	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	friendship.SetProperty("since", 2015)

	employment1, _ := db.CreateRelationship("WORKS_AT", alice.ID, company.ID)
	employment1.SetProperty("role", "Engineer")

	employment2, _ := db.CreateRelationship("WORKS_AT", bob.ID, company.ID)
	employment2.SetProperty("role", "Designer")

	// Query nodes by label
	fmt.Println("All People:")
	people := db.GetNodesByLabel("Person")
	for _, person := range people {
		name, _ := person.GetProperty("name")
		age, _ := person.GetProperty("age")
		fmt.Printf("  - %s (age: %v)\n", name, age)
	}

	// Get relationships for a node
	fmt.Println("\nAlice's relationships:")
	aliceRels := db.GetRelationshipsForNode(alice.ID)
	for _, rel := range aliceRels {
		var otherNodeID string
		if rel.FromNodeID == alice.ID {
			otherNodeID = rel.ToNodeID
		} else {
			otherNodeID = rel.FromNodeID
		}
		otherNode, _ := db.GetNode(otherNodeID)
		otherName, _ := otherNode.GetProperty("name")
		fmt.Printf("  - %s -> %s\n", rel.Type, otherName)
	}

	// Get company information
	fmt.Println("\nCompany details:")
	companies := db.GetNodesByLabel("Company")
	for _, comp := range companies {
		name, _ := comp.GetProperty("name")
		founded, _ := comp.GetProperty("founded")
		fmt.Printf("  %s (founded: %v)\n", name, founded)

		// Find employees
		compRels := db.GetRelationshipsForNode(comp.ID)
		fmt.Println("  Employees:")
		for _, rel := range compRels {
			if rel.Type == "WORKS_AT" && rel.ToNodeID == comp.ID {
				employee, _ := db.GetNode(rel.FromNodeID)
				empName, _ := employee.GetProperty("name")
				role, _ := rel.GetProperty("role")
				fmt.Printf("    - %s (%s)\n", empName, role)
			}
		}
	}
}
