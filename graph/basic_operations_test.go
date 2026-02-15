package graph

import "testing"

// TestBasicNodeOperations tests creating nodes and setting properties
func TestBasicNodeOperations(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "age", 30)

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "age", 25)

	company := db.CreateNode("Company")
	db.SetNodeProperty(company.ID, "name", "TechCorp")
	db.SetNodeProperty(company.ID, "founded", 2020)

	// Verify nodes were created
	if alice == nil || bob == nil || company == nil {
		t.Fatal("Failed to create nodes")
	}

	// Refresh alice to get updated properties
	alice, _ = db.GetNode(alice.ID)

	// Verify properties
	if name, _ := alice.GetProperty("name"); name != "Alice" {
		t.Errorf("Expected Alice's name to be 'Alice', got %v", name)
	}
	if age, _ := alice.GetProperty("age"); age != 30 {
		t.Errorf("Expected Alice's age to be 30, got %v", age)
	}

	// Test GetNodesByLabel
	people := db.GetNodesByLabel("Person")
	if len(people) != 2 {
		t.Errorf("Expected 2 people, got %d", len(people))
	}

	companies := db.GetNodesByLabel("Company")
	if len(companies) != 1 {
		t.Errorf("Expected 1 company, got %d", len(companies))
	}
}

// TestBasicRelationshipOperations tests creating relationships and relationship properties
func TestBasicRelationshipOperations(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	company := db.CreateNode("Company")
	db.SetNodeProperty(company.ID, "name", "TechCorp")

	// Create relationships
	friendship, err := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("Failed to create friendship relationship: %v", err)
	}
	db.SetRelationshipProperty(friendship.ID, "since", 2015)

	employment1, err := db.CreateRelationship("WORKS_AT", alice.ID, company.ID)
	if err != nil {
		t.Fatalf("Failed to create employment relationship: %v", err)
	}
	db.SetRelationshipProperty(employment1.ID, "role", "Engineer")

	employment2, err := db.CreateRelationship("WORKS_AT", bob.ID, company.ID)
	if err != nil {
		t.Fatalf("Failed to create employment relationship: %v", err)
	}
	db.SetRelationshipProperty(employment2.ID, "role", "Designer")

	// Refresh relationship to get updated properties
	friendship, _ = db.GetRelationship(friendship.ID)
	employment1, _ = db.GetRelationship(employment1.ID)

	// Verify relationship properties
	if since, _ := friendship.GetProperty("since"); since != 2015 {
		t.Errorf("Expected friendship since 2015, got %v", since)
	}

	if role, _ := employment1.GetProperty("role"); role != "Engineer" {
		t.Errorf("Expected Alice's role to be 'Engineer', got %v", role)
	}

	// Test GetRelationshipsForNode
	aliceRels := db.GetRelationshipsForNode(alice.ID)
	if len(aliceRels) != 2 {
		t.Errorf("Expected Alice to have 2 relationships, got %d", len(aliceRels))
	}

	companyRels := db.GetRelationshipsForNode(company.ID)
	if len(companyRels) != 2 {
		t.Errorf("Expected company to have 2 relationships, got %d", len(companyRels))
	}
}

// TestRelationshipQueries tests querying relationships and traversing the graph
func TestRelationshipQueries(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	company := db.CreateNode("Company")
	db.SetNodeProperty(company.ID, "name", "TechCorp")

	// Create relationships
	db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	db.CreateRelationship("WORKS_AT", alice.ID, company.ID)
	db.CreateRelationship("WORKS_AT", bob.ID, company.ID)

	// Find all employees of the company
	companyRels := db.GetRelationshipsForNode(company.ID)
	employeeCount := 0
	for _, rel := range companyRels {
		if rel.Type == "WORKS_AT" && rel.ToNodeID == company.ID {
			employeeCount++
		}
	}

	if employeeCount != 2 {
		t.Errorf("Expected 2 employees, got %d", employeeCount)
	}

	// Verify Alice's connections
	aliceRels := db.GetRelationshipsForNode(alice.ID)
	hasFriendship := false
	hasEmployment := false

	for _, rel := range aliceRels {
		if rel.Type == "FRIENDS_WITH" {
			hasFriendship = true
		}
		if rel.Type == "WORKS_AT" {
			hasEmployment = true
		}
	}

	if !hasFriendship {
		t.Error("Alice should have a FRIENDS_WITH relationship")
	}
	if !hasEmployment {
		t.Error("Alice should have a WORKS_AT relationship")
	}
}

// TestNodeRetrieval tests getting nodes by ID
func TestNodeRetrieval(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	// Get node by ID
	retrieved, err := db.GetNode(alice.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve node: %v", err)
	}

	if retrieved.ID != alice.ID {
		t.Errorf("Retrieved node ID doesn't match: expected %s, got %s", alice.ID, retrieved.ID)
	}

	name, _ := retrieved.GetProperty("name")
	if name != "Alice" {
		t.Errorf("Retrieved node name doesn't match: expected 'Alice', got %v", name)
	}

	// Try to get non-existent node
	_, err = db.GetNode("non-existent-id")
	if err == nil {
		t.Error("Expected error when getting non-existent node")
	}
}
