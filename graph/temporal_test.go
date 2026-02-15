package graph

import (
	"testing"
	"time"
)

// TestTemporalNodeProperties tests that node property changes are tracked over time
func TestTemporalNodeProperties(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

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
	db, cleanup := newTestGraph(t)
	defer cleanup()

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
	db, cleanup := newTestGraph(t)
	defer cleanup()

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
	db, cleanup := newTestGraph(t)
	defer cleanup()

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
	db, cleanup := newTestGraph(t)
	defer cleanup()

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
	db, cleanup := newTestGraph(t)
	defer cleanup()

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

// TestTemporalQueryAtExactCreationTime tests querying at the exact moment of creation
func TestTemporalQueryAtExactCreationTime(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	creationTime := time.Now()

	// Query at time after creation - should find the node
	view := db.AsOf(creationTime)
	node, err := view.GetNode(alice.ID)
	if err != nil || node == nil {
		t.Error("Expected to find node at creation time")
	}
}

// TestTemporalRapidUpdates tests multiple rapid property updates
func TestTemporalRapidUpdates(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	// Capture times and perform rapid updates
	times := make([]time.Time, 5)
	roles := []string{"Junior", "Mid-level", "Senior", "Lead", "Principal"}

	for i, role := range roles {
		db.SetNodeProperty(alice.ID, "role", role)
		time.Sleep(5 * time.Millisecond)
		times[i] = time.Now()
	}

	// Verify each historical state
	for i, expectedRole := range roles {
		view := db.AsOf(times[i])
		node, _ := view.GetNode(alice.ID)
		if node == nil {
			t.Errorf("Expected to find node at time %d", i)
			continue
		}
		actualRole, _ := node.GetProperty("role")
		if actualRole != expectedRole {
			t.Errorf("At time %d, expected role %s, got %v", i, expectedRole, actualRole)
		}
	}
}

// TestTemporalPropertyRemovalAndReaddition tests removing and re-adding properties
func TestTemporalPropertyRemovalAndReaddition(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "email", "alice@example.com")

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Remove email property
	db.DeleteNodeProperty(alice.ID, "email")

	time2 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Re-add email with different value
	db.SetNodeProperty(alice.ID, "email", "alice@newdomain.com")

	time3 := time.Now()

	// Verify at time1 - should have original email
	view1 := db.AsOf(time1)
	node1, _ := view1.GetNode(alice.ID)
	if node1 == nil {
		t.Fatal("Expected to find node at time1")
	}
	email1, exists := node1.GetProperty("email")
	if !exists || email1 != "alice@example.com" {
		t.Errorf("Expected email 'alice@example.com' at time1, got %v (exists: %v)", email1, exists)
	}

	// Verify at time2 - should not have email
	view2 := db.AsOf(time2)
	node2, _ := view2.GetNode(alice.ID)
	if node2 == nil {
		t.Fatal("Expected to find node at time2")
	}
	email2, exists := node2.GetProperty("email")
	if exists {
		t.Errorf("Expected email to not exist at time2, got %v", email2)
	}

	// Verify at time3 - should have new email
	view3 := db.AsOf(time3)
	node3, _ := view3.GetNode(alice.ID)
	if node3 == nil {
		t.Fatal("Expected to find node at time3")
	}
	email3, exists := node3.GetProperty("email")
	if !exists || email3 != "alice@newdomain.com" {
		t.Errorf("Expected email 'alice@newdomain.com' at time3, got %v (exists: %v)", email3, exists)
	}
}

// TestTemporalMultipleNodesWithSameLabel tests temporal queries with multiple nodes
func TestTemporalMultipleNodesWithSameLabel(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create initial state with 2 people
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Add a third person
	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")

	time2 := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Delete Bob
	db.DeleteNode(bob.ID)

	// Verify at time1 - should have 2 people
	view1 := db.AsOf(time1)
	people1 := view1.GetNodesByLabel("Person")
	if len(people1) != 2 {
		t.Errorf("Expected 2 people at time1, got %d", len(people1))
	}

	// Verify at time2 - should have 3 people
	view2 := db.AsOf(time2)
	people2 := view2.GetNodesByLabel("Person")
	if len(people2) != 3 {
		t.Errorf("Expected 3 people at time2, got %d", len(people2))
	}

	// Verify current - should have 2 people (Bob deleted)
	currentPeople := db.GetNodesByLabel("Person")
	if len(currentPeople) != 2 {
		t.Errorf("Expected 2 people in current state, got %d", len(currentPeople))
	}
}

// TestTemporalRelationshipPropertyCascade tests relationship properties changing multiple times
func TestTemporalRelationshipPropertyCascade(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice := db.CreateNode("Person")
	bob := db.CreateNode("Person")
	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)

	// Track property changes
	db.SetRelationshipProperty(friendship.ID, "closeness", 5)
	time1 := time.Now()
	time.Sleep(10 * time.Millisecond)

	db.SetRelationshipProperty(friendship.ID, "closeness", 7)
	time2 := time.Now()
	time.Sleep(10 * time.Millisecond)

	db.SetRelationshipProperty(friendship.ID, "closeness", 10)
	time3 := time.Now()

	// Verify each state
	view1 := db.AsOf(time1)
	rel1, _ := view1.GetRelationship(friendship.ID)
	if rel1 == nil {
		t.Fatal("Expected to find relationship at time1")
	}
	closeness1, _ := rel1.GetProperty("closeness")
	if closeness1 != 5 {
		t.Errorf("Expected closeness 5 at time1, got %v", closeness1)
	}

	view2 := db.AsOf(time2)
	rel2, _ := view2.GetRelationship(friendship.ID)
	if rel2 == nil {
		t.Fatal("Expected to find relationship at time2")
	}
	closeness2, _ := rel2.GetProperty("closeness")
	if closeness2 != 7 {
		t.Errorf("Expected closeness 7 at time2, got %v", closeness2)
	}

	view3 := db.AsOf(time3)
	rel3, _ := view3.GetRelationship(friendship.ID)
	if rel3 == nil {
		t.Fatal("Expected to find relationship at time3")
	}
	closeness3, _ := rel3.GetProperty("closeness")
	if closeness3 != 10 {
		t.Errorf("Expected closeness 10 at time3, got %v", closeness3)
	}
}

// TestTemporalEmptyGraph tests temporal queries on an empty graph
func TestTemporalEmptyGraph(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	pastTime := time.Now()

	// Query empty graph
	view := db.AsOf(pastTime)
	nodes := view.GetAllNodes()
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes in empty graph, got %d", len(nodes))
	}

	rels := view.GetAllRelationships()
	if len(rels) != 0 {
		t.Errorf("Expected 0 relationships in empty graph, got %d", len(rels))
	}
}

// TestTemporalFutureQuery tests querying the graph at a future time
func TestTemporalFutureQuery(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	// Query at a future time (should see current state)
	futureTime := time.Now().Add(24 * time.Hour)
	view := db.AsOf(futureTime)
	node, _ := view.GetNode(alice.ID)
	if node == nil {
		t.Error("Expected to find node when querying future time")
	}
}
