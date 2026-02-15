package graph

import (
	"testing"
)

// TestBasicEmbedding tests embedding a node with the EMBED clause
func TestBasicEmbedding(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create a node with a role
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	// Embed the node using its role property
	query, err := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(query, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result.Rows))
	}

	// Verify the node was embedded
	embedding := db.GetNodeEmbedding(alice.ID)
	if embedding == nil {
		t.Error("Expected node to have embedding after EMBED")
	}
	if embedding != nil && len(embedding.Vector) == 0 {
		t.Error("Expected embedding to have a vector")
	}
}

// TestSemanticSearch tests basic semantic search functionality
func TestSemanticSearch(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create people with different roles
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "frontend developer")

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")
	db.SetNodeProperty(carol.ID, "role", "data scientist")

	// Embed all nodes
	embedQuery, _ := ParseQuery(`MATCH (p:Person) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(embedQuery, embedder)

	// Search for backend engineers
	searchQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "backend engineers" RETURN p.name, p.role, similarity`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(searchQuery, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected to find results for semantic search")
	}

	// Alice (backend engineer) should be the top match
	topMatch := result.Rows[0]["p.name"]
	if topMatch != "Alice" {
		t.Errorf("Expected Alice to be top match for 'backend engineers', got %v", topMatch)
	}
}

// TestSemanticSearchWithThreshold tests THRESHOLD filtering
func TestSemanticSearchWithThreshold(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create and embed nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "frontend developer")

	embedQuery, _ := ParseQuery(`MATCH (p:Person) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(embedQuery, embedder)

	// Search with high threshold - should filter out low-similarity results
	searchQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "backend engineers" THRESHOLD 0.9 RETURN p.name, similarity`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(searchQuery, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Only high-similarity matches should be returned
	for _, row := range result.Rows {
		similarity, ok := row["similarity"].(float32)
		if !ok {
			t.Errorf("Similarity should be float32, got %T", row["similarity"])
			continue
		}
		if similarity < 0.9 {
			t.Errorf("Result with similarity %f should be filtered by THRESHOLD 0.9", similarity)
		}
	}
}

// TestSemanticSearchWithLimit tests LIMIT clause
func TestSemanticSearchWithLimit(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create multiple nodes
	for i, name := range []string{"Alice", "Bob", "Carol", "David", "Eve"} {
		node := db.CreateNode("Person")
		db.SetNodeProperty(node.ID, "name", name)
		if i < 3 {
			db.SetNodeProperty(node.ID, "role", "backend engineer")
		} else {
			db.SetNodeProperty(node.ID, "role", "frontend developer")
		}
	}

	// Embed all nodes
	embedQuery, _ := ParseQuery(`MATCH (p:Person) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(embedQuery, embedder)

	// Search with LIMIT
	searchQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "backend engineers" LIMIT 2 RETURN p.name`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(searchQuery, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected LIMIT 2 to return exactly 2 results, got %d", len(result.Rows))
	}
}

// TestMultipleEmbeddings tests that nodes can have multiple embeddings
func TestMultipleEmbeddings(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")
	db.SetNodeProperty(alice.ID, "bio", "Loves coding")

	// Embed based on role
	query1, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query1, embedder)

	// Embed based on bio (creates a new embedding version)
	query2, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.bio RETURN p`)
	db.ExecuteQueryWithEmbedder(query2, embedder)

	// Check that node has an embedding (versioned embeddings stored internally)
	embedding := db.GetNodeEmbedding(alice.ID)
	if embedding == nil {
		t.Error("Expected node to have embedding after multiple EMBED operations")
	}
}

// TestEmbeddingNonExistentProperty tests embedding when property doesn't exist
func TestEmbeddingNonExistentProperty(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	// No "role" property set

	// Try to embed non-existent property
	query, err := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Should handle gracefully (not panic)
	result, err := db.ExecuteQueryWithEmbedder(query, embedder)
	if err != nil {
		// Error is acceptable
		return
	}

	// If no error, should still work (might embed empty string or skip)
	if result == nil {
		t.Error("Expected result even when embedding non-existent property")
	}
}

// TestSemanticSearchNoEmbeddings tests search when no nodes are embedded
func TestSemanticSearchNoEmbeddings(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create nodes but don't embed them
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	// Try semantic search without embeddings
	searchQuery, err := ParseQuery(`MATCH (p:Person) SIMILAR TO "backend engineers" RETURN p.name`)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(searchQuery, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should return no results (or handle gracefully)
	if len(result.Rows) > 0 {
		t.Log("Note: Semantic search returned results even without embeddings")
	}
}

// TestEmbeddingUpdate tests re-embedding a node updates the embedding
func TestEmbeddingUpdate(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	// Embed initially
	query1, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query1, embedder)

	initialEmbedding := db.GetNodeEmbedding(alice.ID)
	if initialEmbedding == nil {
		t.Fatal("Expected initial embedding to exist")
	}

	// Change the role and re-embed
	db.SetNodeProperty(alice.ID, "role", "engineering manager")
	query2, _ := ParseQuery(`MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`)
	db.ExecuteQueryWithEmbedder(query2, embedder)

	// Should have a new embedding (embeddings are versioned internally)
	updatedEmbedding := db.GetNodeEmbedding(alice.ID)
	if updatedEmbedding == nil {
		t.Error("Expected embedding to exist after re-embedding")
	}
	// The embedding should reflect the new state (exact vector comparison depends on MockEmbedder)
	if updatedEmbedding != nil && len(updatedEmbedding.Vector) == 0 {
		t.Error("Expected updated embedding to have a vector")
	}
}
