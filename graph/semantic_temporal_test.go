package graph

import (
	"testing"
	"time"

	"github.com/MironCo/gravecdb/embedding"
)

// TestSemanticSearchThroughTime tests the THROUGH TIME syntax for semantic search
func TestSemanticSearchThroughTime(t *testing.T) {
	// Create database and mock embedder
	db := NewGraph()
	embedder := embedding.NewMockEmbedder()

	// Create a person whose role evolves over time
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "software engineer")

	// Embed Alice's initial role
	query1, err := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}
	result1, err := db.ExecuteQueryWithEmbedder(query1, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	if len(result1.Rows) != 1 {
		t.Fatalf("Expected 1 embedded node, got %d", len(result1.Rows))
	}

	// Wait to ensure timestamp difference
	time.Sleep(10 * time.Millisecond)

	// Alice gets promoted
	db.SetNodeProperty(alice.ID, "role", "engineering manager")

	// Embed Alice's new role
	query2, err := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}
	result2, err := db.ExecuteQueryWithEmbedder(query2, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	if len(result2.Rows) != 1 {
		t.Fatalf("Expected 1 re-embedded node, got %d", len(result2.Rows))
	}

	// Wait again
	time.Sleep(10 * time.Millisecond)

	// Alice gets promoted again
	db.SetNodeProperty(alice.ID, "role", "director of engineering")

	// Embed Alice's third role
	query3, err := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}
	result3, err := db.ExecuteQueryWithEmbedder(query3, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	if len(result3.Rows) != 1 {
		t.Fatalf("Expected 1 re-embedded node, got %d", len(result3.Rows))
	}

	// Test 1: Regular semantic search (current time only)
	t.Run("RegularSemanticSearch", func(t *testing.T) {
		searchQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "engineer" RETURN p.name, p.role, similarity`)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}
		searchResult, err := db.ExecuteQueryWithEmbedder(searchQuery, embedder)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Should only return current version
		if len(searchResult.Rows) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(searchResult.Rows))
		}

		// Should be the current role
		if searchResult.Rows[0]["p.role"] != "director of engineering" {
			t.Errorf("Expected current role 'director of engineering', got %v", searchResult.Rows[0]["p.role"])
		}
	})

	// Test 2: THROUGH TIME search - returns all historical versions
	t.Run("ThroughTimeSearch", func(t *testing.T) {
		throughTimeQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "engineer" THROUGH TIME RETURN p.name, p.role, similarity, valid_from, valid_to`)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}
		throughTimeResult, err := db.ExecuteQueryWithEmbedder(throughTimeQuery, embedder)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Should return all 3 versions
		if len(throughTimeResult.Rows) != 3 {
			t.Fatalf("Expected 3 versions, got %d", len(throughTimeResult.Rows))
		}

		// Check that we have all three roles in the results
		roles := make(map[string]bool)
		for _, row := range throughTimeResult.Rows {
			role, ok := row["p.role"].(string)
			if !ok {
				t.Errorf("Role is not a string: %v", row["p.role"])
				continue
			}
			roles[role] = true

			// Verify valid_from exists and is a time.Time
			if _, ok := row["valid_from"].(time.Time); !ok {
				t.Errorf("valid_from is not a time.Time: %v", row["valid_from"])
			}

			// valid_to can be nil or time.Time
			if row["valid_to"] != nil {
				if _, ok := row["valid_to"].(time.Time); !ok {
					t.Errorf("valid_to is not a time.Time or nil: %v", row["valid_to"])
				}
			}
		}

		// Verify all three roles are present
		expectedRoles := []string{"software engineer", "engineering manager", "director of engineering"}
		for _, expected := range expectedRoles {
			if !roles[expected] {
				t.Errorf("Missing expected role: %s", expected)
			}
		}
	})

	// Test 3: VERSIONS THROUGH TIME syntax (should work the same as THROUGH TIME)
	t.Run("VersionsThroughTimeSearch", func(t *testing.T) {
		versionsQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "engineer" VERSIONS THROUGH TIME RETURN p.name, p.role, similarity`)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}
		versionsResult, err := db.ExecuteQueryWithEmbedder(versionsQuery, embedder)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Should return all 3 versions
		if len(versionsResult.Rows) != 3 {
			t.Fatalf("Expected 3 versions, got %d", len(versionsResult.Rows))
		}

		// Verify all three roles are present
		roles := make(map[string]bool)
		for _, row := range versionsResult.Rows {
			role, ok := row["p.role"].(string)
			if !ok {
				t.Errorf("Role is not a string: %v", row["p.role"])
				continue
			}
			roles[role] = true
		}

		expectedRoles := []string{"software engineer", "engineering manager", "director of engineering"}
		for _, expected := range expectedRoles {
			if !roles[expected] {
				t.Errorf("Missing expected role: %s", expected)
			}
		}
	})

	// Test 4: DRIFT THROUGH TIME - includes drift metrics
	t.Run("DriftThroughTimeSearch", func(t *testing.T) {
		driftQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "engineer" DRIFT THROUGH TIME RETURN p.name, p.role, similarity, drift_from_previous, drift_from_first`)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}
		driftResult, err := db.ExecuteQueryWithEmbedder(driftQuery, embedder)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Should return all 3 versions
		if len(driftResult.Rows) != 3 {
			t.Fatalf("Expected 3 versions, got %d", len(driftResult.Rows))
		}

		// Verify drift columns exist
		for i, row := range driftResult.Rows {
			if _, ok := row["drift_from_previous"]; !ok {
				t.Errorf("Row %d missing drift_from_previous", i)
			}
			if _, ok := row["drift_from_first"]; !ok {
				t.Errorf("Row %d missing drift_from_first", i)
			}

			// Verify drift values are float32
			driftPrev, ok := row["drift_from_previous"].(float32)
			if !ok {
				t.Errorf("drift_from_previous is not float32: %v", row["drift_from_previous"])
			}
			driftFirst, ok := row["drift_from_first"].(float32)
			if !ok {
				t.Errorf("drift_from_first is not float32: %v", row["drift_from_first"])
			}

			// Verify drift values are in valid range
			// Note: MockEmbedder returns same vector for all inputs, so drift will be ~0
			// In a real scenario with different embeddings, drift would be > 0
			role := row["p.role"].(string)
			if role == "software engineer" {
				// First version should have zero drift
				if driftPrev != 0 {
					t.Errorf("First version should have drift_from_previous = 0, got %f", driftPrev)
				}
				if driftFirst != 0 {
					t.Errorf("First version should have drift_from_first = 0, got %f", driftFirst)
				}
			} else {
				// Drift should be in valid range [0, 1]
				// Allow small negative values due to floating point precision
				if driftPrev < -0.0001 || driftPrev > 1 {
					t.Errorf("drift_from_previous should be in [0,1], got %f", driftPrev)
				}
				if driftFirst < -0.0001 || driftFirst > 1 {
					t.Errorf("drift_from_first should be in [0,1], got %f", driftFirst)
				}
			}
		}
	})
}

// TestSemanticSearchThroughTimeWithLimit tests LIMIT with THROUGH TIME
func TestSemanticSearchThroughTimeWithLimit(t *testing.T) {
	db := NewGraph()
	embedder := embedding.NewMockEmbedder()

	// Create a person with multiple role changes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "junior engineer")

	// Embed initial role
	query1, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query1, embedder)

	time.Sleep(10 * time.Millisecond)

	// Second role
	db.SetNodeProperty(alice.ID, "role", "senior engineer")
	query2, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query2, embedder)

	time.Sleep(10 * time.Millisecond)

	// Third role
	db.SetNodeProperty(alice.ID, "role", "principal engineer")
	query3, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query3, embedder)

	// Test LIMIT with THROUGH TIME
	limitQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "engineer" THROUGH TIME LIMIT 2 RETURN p.name, p.role`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}
	limitResult, err := db.ExecuteQueryWithEmbedder(limitQuery, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should only return 2 versions due to LIMIT
	if len(limitResult.Rows) != 2 {
		t.Fatalf("Expected 2 results due to LIMIT, got %d", len(limitResult.Rows))
	}
}

// TestPropertySnapshotInThroughTime verifies that historical property values are preserved
func TestPropertySnapshotInThroughTime(t *testing.T) {
	db := NewGraph()
	embedder := embedding.NewMockEmbedder()

	// Create a person with properties that change
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "engineer")
	db.SetNodeProperty(alice.ID, "level", "L3")

	// Embed initial state
	query1, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query1, embedder)

	time.Sleep(10 * time.Millisecond)

	// Change both role and level
	db.SetNodeProperty(alice.ID, "role", "manager")
	db.SetNodeProperty(alice.ID, "level", "L5")

	// Embed new state
	query2, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query2, embedder)

	// Query through time
	throughTimeQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "engineer" THROUGH TIME RETURN p.name, p.role, p.level`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}
	result, err := db.ExecuteQueryWithEmbedder(throughTimeQuery, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 versions, got %d", len(result.Rows))
	}

	// Verify historical property snapshot
	foundEngineer := false
	foundManager := false
	for _, row := range result.Rows {
		role := row["p.role"].(string)
		level := row["p.level"].(string)

		if role == "engineer" {
			foundEngineer = true
			if level != "L3" {
				t.Errorf("Historical 'engineer' version should have level L3, got %s", level)
			}
		} else if role == "manager" {
			foundManager = true
			if level != "L5" {
				t.Errorf("Historical 'manager' version should have level L5, got %s", level)
			}
		}
	}

	if !foundEngineer {
		t.Error("Did not find historical 'engineer' version")
	}
	if !foundManager {
		t.Error("Did not find historical 'manager' version")
	}
}
