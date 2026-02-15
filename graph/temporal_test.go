package graph

import (
	"testing"
	"time"
)

// TestTemporalNodeProperties tests that node property changes are tracked over time
func TestTemporalNodeProperties(t *testing.T) {
	db := NewGraph()

	// Create a node with initial properties
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "Engineer")

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Change role
	db.SetNodeProperty(alice.ID, "role", "Senior Engineer")

	time2 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Change role again
	db.SetNodeProperty(alice.ID, "role", "Engineering Manager")

	// Verify current state - need to get the latest version from the graph
	currentAlice, err := db.GetNode(alice.ID)
	if err != nil {
		t.Fatalf("Failed to get current alice: %v", err)
	}
	currentRole, _ := currentAlice.GetProperty("role")
	if currentRole != "Engineering Manager" {
		t.Errorf("Expected current role 'Engineering Manager', got %v", currentRole)
	}

	// Verify historical state at time1
	view1 := db.AsOf(time1)
	aliceAt1, _ := view1.GetNode(alice.ID)
	if aliceAt1 == nil {
		t.Fatal("Expected to find Alice at time1")
	}
	roleAt1, _ := aliceAt1.GetProperty("role")
	if roleAt1 != "Engineer" {
		t.Errorf("Expected role 'Engineer' at time1, got %v", roleAt1)
	}

	// Verify historical state at time2
	view2 := db.AsOf(time2)
	aliceAt2, _ := view2.GetNode(alice.ID)
	if aliceAt2 == nil {
		t.Fatal("Expected to find Alice at time2")
	}
	roleAt2, _ := aliceAt2.GetProperty("role")
	if roleAt2 != "Senior Engineer" {
		t.Errorf("Expected role 'Senior Engineer' at time2, got %v", roleAt2)
	}
}

// TestTemporalRelationships tests that relationships are tracked over time
func TestTemporalRelationships(t *testing.T) {
	db := NewGraph()

	// Create nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	techCorp := db.CreateNode("Company")
	db.SetNodeProperty(techCorp.ID, "name", "TechCorp")

	startup := db.CreateNode("Company")
	db.SetNodeProperty(startup.ID, "name", "Startup")

	// Alice works at TechCorp
	job1, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(job1.ID, "title", "Engineer")

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Alice leaves TechCorp and joins Startup
	db.DeleteRelationship(job1.ID)
	job2, _ := db.CreateRelationship("WORKS_AT", alice.ID, startup.ID)
	db.SetRelationshipProperty(job2.ID, "title", "Senior Engineer")

	time2 := time.Now()

	// Verify current state - should only have Startup job
	currentRels := db.GetRelationshipsForNode(alice.ID)
	if len(currentRels) != 1 {
		t.Errorf("Expected 1 current relationship, got %d", len(currentRels))
	}
	if currentRels[0].ToNodeID != startup.ID {
		t.Error("Expected current job to be at Startup")
	}

	// Verify historical state at time1 - should have TechCorp job
	view1 := db.AsOf(time1)
	relsAt1 := view1.GetRelationshipsForNode(alice.ID)
	if len(relsAt1) != 1 {
		t.Errorf("Expected 1 relationship at time1, got %d", len(relsAt1))
	}
	if relsAt1[0].ToNodeID != techCorp.ID {
		t.Error("Expected job at time1 to be at TechCorp")
	}

	// Verify historical state at time2 - should have Startup job
	view2 := db.AsOf(time2)
	relsAt2 := view2.GetRelationshipsForNode(alice.ID)
	if len(relsAt2) != 1 {
		t.Errorf("Expected 1 relationship at time2, got %d", len(relsAt2))
	}
	if relsAt2[0].ToNodeID != startup.ID {
		t.Error("Expected job at time2 to be at Startup")
	}
}

// TestTemporalNodeDeletion tests that deleted nodes are still accessible historically
func TestTemporalNodeDeletion(t *testing.T) {
	db := NewGraph()

	// Create a node
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Delete the node
	db.DeleteNode(alice.ID)

	// Current state should not find the node
	currentNode, err := db.GetNode(alice.ID)
	if err == nil && currentNode != nil {
		t.Error("Expected node to not be found in current state after deletion")
	}

	// Historical query should still find it
	view1 := db.AsOf(time1)
	historicalNode, err := view1.GetNode(alice.ID)
	if err != nil || historicalNode == nil {
		t.Fatal("Expected to find node at historical time1")
	}
	name, _ := historicalNode.GetProperty("name")
	if name != "Alice" {
		t.Errorf("Expected name 'Alice' at time1, got %v", name)
	}
}

// TestTemporalRelationshipProperties tests that relationship property changes are tracked
func TestTemporalRelationshipProperties(t *testing.T) {
	db := NewGraph()

	// Create nodes and relationship
	alice := db.CreateNode("Person")
	bob := db.CreateNode("Person")
	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	db.SetRelationshipProperty(friendship.ID, "since", 2020)

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Update property
	db.SetRelationshipProperty(friendship.ID, "since", 2021)

	// Verify current state - need to get the latest version from the graph
	currentFriendship, err := db.GetRelationship(friendship.ID)
	if err != nil {
		t.Fatalf("Failed to get current friendship: %v", err)
	}
	currentSince, _ := currentFriendship.GetProperty("since")
	if currentSince != 2021 {
		t.Errorf("Expected current 'since' to be 2021, got %v", currentSince)
	}

	// Verify historical state
	view1 := db.AsOf(time1)
	relsAt1 := view1.GetRelationshipsForNode(alice.ID)
	if len(relsAt1) != 1 {
		t.Fatalf("Expected 1 relationship at time1, got %d", len(relsAt1))
	}
	sinceAt1, _ := relsAt1[0].GetProperty("since")
	if sinceAt1 != 2020 {
		t.Errorf("Expected 'since' to be 2020 at time1, got %v", sinceAt1)
	}
}

// TestTemporalComplexScenario tests a complex scenario with multiple changes
func TestTemporalComplexScenario(t *testing.T) {
	db := NewGraph()

	// Create initial state
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	techCorp := db.CreateNode("Company")
	db.SetNodeProperty(techCorp.ID, "name", "TechCorp")

	aliceJob1, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(aliceJob1.ID, "title", "Junior Engineer")

	bobJob, _ := db.CreateRelationship("WORKS_AT", bob.ID, techCorp.ID)
	db.SetRelationshipProperty(bobJob.ID, "title", "Senior Designer")

	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Alice gets promoted
	db.DeleteRelationship(aliceJob1.ID)
	aliceJob2, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(aliceJob2.ID, "title", "Senior Engineer")

	time2 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Bob leaves TechCorp
	db.DeleteRelationship(bobJob.ID)
	startup := db.CreateNode("Company")
	db.SetNodeProperty(startup.ID, "name", "Startup")
	bobJob2, _ := db.CreateRelationship("WORKS_AT", bob.ID, startup.ID)
	db.SetRelationshipProperty(bobJob2.ID, "title", "Design Lead")

	time3 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Friendship ends
	db.DeleteRelationship(friendship.ID)

	// Test 1: At time1, both should work at TechCorp and be friends
	t.Run("StateAtTime1", func(t *testing.T) {
		view1 := db.AsOf(time1)

		aliceRels := view1.GetRelationshipsForNode(alice.ID)
		techCorpCount := 0
		friendshipExists := false
		for _, rel := range aliceRels {
			if rel.Type == "WORKS_AT" && rel.ToNodeID == techCorp.ID {
				techCorpCount++
				title, _ := rel.GetProperty("title")
				if title != "Junior Engineer" {
					t.Errorf("Expected Alice's title 'Junior Engineer', got %v", title)
				}
			}
			if rel.Type == "FRIENDS_WITH" {
				friendshipExists = true
			}
		}
		if techCorpCount != 1 {
			t.Errorf("Expected Alice to have 1 TechCorp job at time1, got %d", techCorpCount)
		}
		if !friendshipExists {
			t.Error("Expected friendship to exist at time1")
		}
	})

	// Test 2: At time2, Alice should be promoted
	t.Run("StateAtTime2", func(t *testing.T) {
		view2 := db.AsOf(time2)

		aliceRels := view2.GetRelationshipsForNode(alice.ID)
		found := false
		for _, rel := range aliceRels {
			if rel.Type == "WORKS_AT" {
				title, _ := rel.GetProperty("title")
				if title == "Senior Engineer" {
					found = true
				}
			}
		}
		if !found {
			t.Error("Expected Alice to be Senior Engineer at time2")
		}
	})

	// Test 3: At time3, Bob should work at Startup, friendship should still exist
	t.Run("StateAtTime3", func(t *testing.T) {
		view3 := db.AsOf(time3)

		bobRels := view3.GetRelationshipsForNode(bob.ID)
		worksAtStartup := false
		for _, rel := range bobRels {
			if rel.Type == "WORKS_AT" && rel.ToNodeID == startup.ID {
				worksAtStartup = true
			}
		}
		if !worksAtStartup {
			t.Error("Expected Bob to work at Startup at time3")
		}

		aliceRels := view3.GetRelationshipsForNode(alice.ID)
		friendshipExists := false
		for _, rel := range aliceRels {
			if rel.Type == "FRIENDS_WITH" {
				friendshipExists = true
			}
		}
		if !friendshipExists {
			t.Error("Expected friendship to still exist at time3")
		}
	})

	// Test 4: Current state - friendship should not exist
	t.Run("CurrentState", func(t *testing.T) {
		aliceRels := db.GetRelationshipsForNode(alice.ID)
		friendshipExists := false
		for _, rel := range aliceRels {
			if rel.Type == "FRIENDS_WITH" {
				friendshipExists = true
			}
		}
		if friendshipExists {
			t.Error("Expected friendship to not exist in current state")
		}
	})
}

// TestAsOfWithNonexistentNode tests querying for a node that didn't exist yet
func TestAsOfWithNonexistentNode(t *testing.T) {
	db := NewGraph()

	// Record time before creating node
	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Create node
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	// Query before node existed
	view1 := db.AsOf(time1)
	node, err := view1.GetNode(alice.ID)
	if err == nil && node != nil {
		t.Error("Expected to not find node at time before it was created")
	}

	// Query after node existed (current)
	currentNode, err := db.GetNode(alice.ID)
	if err != nil || currentNode == nil {
		t.Error("Expected to find node in current state")
	}
}
