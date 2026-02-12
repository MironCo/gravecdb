package graph

import (
	"testing"
)

func TestParseSimpleNodeQuery(t *testing.T) {
	query := `MATCH (p:Person) RETURN p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if parsed.MatchPattern == nil {
		t.Fatal("MatchPattern is nil")
	}

	if len(parsed.MatchPattern.Nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(parsed.MatchPattern.Nodes))
	}

	node := parsed.MatchPattern.Nodes[0]
	if node.Variable != "p" {
		t.Errorf("Expected variable 'p', got '%s'", node.Variable)
	}

	if len(node.Labels) != 1 || node.Labels[0] != "Person" {
		t.Errorf("Expected label 'Person', got %v", node.Labels)
	}
}

func TestParseRelationshipQuery(t *testing.T) {
	query := `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if len(parsed.MatchPattern.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(parsed.MatchPattern.Nodes))
	}

	if len(parsed.MatchPattern.Relationships) != 1 {
		t.Errorf("Expected 1 relationship, got %d", len(parsed.MatchPattern.Relationships))
	}

	rel := parsed.MatchPattern.Relationships[0]
	if len(rel.Types) != 1 || rel.Types[0] != "KNOWS" {
		t.Errorf("Expected relationship type 'KNOWS', got %v", rel.Types)
	}

	if rel.Direction != "->" {
		t.Errorf("Expected direction '->', got '%s'", rel.Direction)
	}
}

func TestParseWhereClause(t *testing.T) {
	query := `MATCH (p:Person) WHERE p.age > 25 AND p.name = "Alice" RETURN p.name`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if parsed.WhereClause == nil {
		t.Fatal("WhereClause is nil")
	}

	if len(parsed.WhereClause.Conditions) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(parsed.WhereClause.Conditions))
	}

	cond1 := parsed.WhereClause.Conditions[0]
	if cond1.Variable != "p" || cond1.Property != "age" || cond1.Operator != ">" {
		t.Errorf("First condition parsed incorrectly: %+v", cond1)
	}

	cond2 := parsed.WhereClause.Conditions[1]
	if cond2.Variable != "p" || cond2.Property != "name" || cond2.Operator != "=" {
		t.Errorf("Second condition parsed incorrectly: %+v", cond2)
	}

	if cond2.Value != "Alice" {
		t.Errorf("Expected value 'Alice', got %v", cond2.Value)
	}
}

func TestParseShortestPathQuery(t *testing.T) {
	query := `MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "David" RETURN path`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if !parsed.IsPathQuery {
		t.Error("Expected IsPathQuery to be true")
	}

	if parsed.MatchPattern.PathFunction == nil {
		t.Fatal("PathFunction is nil")
	}

	pf := parsed.MatchPattern.PathFunction
	if pf.Function != "shortestpath" {
		t.Errorf("Expected function 'shortestpath', got '%s'", pf.Function)
	}

	if pf.Variable != "path" {
		t.Errorf("Expected variable 'path', got '%s'", pf.Variable)
	}

	if pf.StartPattern.Variable != "a" {
		t.Errorf("Expected start variable 'a', got '%s'", pf.StartPattern.Variable)
	}

	if len(pf.StartPattern.Labels) != 1 || pf.StartPattern.Labels[0] != "Person" {
		t.Errorf("Expected start label 'Person', got %v", pf.StartPattern.Labels)
	}

	if pf.EndPattern.Variable != "b" {
		t.Errorf("Expected end variable 'b', got '%s'", pf.EndPattern.Variable)
	}
}

func TestParseShortestPathWithRelTypes(t *testing.T) {
	query := `MATCH path = shortestPath((a)-[:KNOWS|FRIENDS_WITH*]-(b)) RETURN path`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	pf := parsed.MatchPattern.PathFunction
	if len(pf.RelTypes) != 2 {
		t.Errorf("Expected 2 relationship types, got %d", len(pf.RelTypes))
	}

	if pf.RelTypes[0] != "KNOWS" || pf.RelTypes[1] != "FRIENDS_WITH" {
		t.Errorf("Expected rel types [KNOWS, FRIENDS_WITH], got %v", pf.RelTypes)
	}
}

func TestQueryExecution(t *testing.T) {
	db := NewGraph()

	// Create test data
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "age", 28)

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "age", 32)

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")
	db.SetNodeProperty(carol.ID, "age", 25)

	// Test simple node query
	query := `MATCH (p:Person) RETURN p.name`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

func TestQueryWithWhereFilter(t *testing.T) {
	db := NewGraph()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "age", 28)

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "age", 32)

	// Test WHERE filter
	query := `MATCH (p:Person) WHERE p.age > 30 RETURN p.name`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0]["p.name"] != "Bob" {
		t.Errorf("Expected Bob, got %v", result.Rows[0]["p.name"])
	}
}

func TestRelationshipQuery(t *testing.T) {
	db := NewGraph()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	db.CreateRelationship("KNOWS", alice.ID, bob.ID)

	// Test relationship query
	query := `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0]["a.name"] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0]["a.name"])
	}

	if result.Rows[0]["b.name"] != "Bob" {
		t.Errorf("Expected Bob, got %v", result.Rows[0]["b.name"])
	}
}

func TestShortestPathQuery(t *testing.T) {
	db := NewGraph()

	// Create nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")

	david := db.CreateNode("Person")
	db.SetNodeProperty(david.ID, "name", "David")

	// Create path: Alice -> Bob -> Carol -> David
	db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	db.CreateRelationship("KNOWS", bob.ID, carol.ID)
	db.CreateRelationship("KNOWS", carol.ID, david.ID)

	// Test shortest path query
	query := `MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "David" RETURN path`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatalf("Expected at least 1 path, got 0 (query returned no results)")
	}

	path, ok := result.Rows[0]["path"].(*Path)
	if !ok {
		t.Fatalf("Result is not a Path type: %T", result.Rows[0]["path"])
	}

	if path.Length != 3 {
		t.Errorf("Expected path length 3, got %d", path.Length)
	}

	if len(path.Nodes) != 4 {
		t.Errorf("Expected 4 nodes in path, got %d", len(path.Nodes))
	}
}

func TestMultipleRelationshipTypes(t *testing.T) {
	db := NewGraph()

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")

	db.CreateRelationship("KNOWS", alice.ID, bob.ID)
	db.CreateRelationship("WORKS_WITH", bob.ID, carol.ID)

	// Test query with multiple relationship types
	query := `MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_WITH]->(c:Person) RETURN a.name, b.name, c.name`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestCreateNode(t *testing.T) {
	db := NewGraph()

	query := `CREATE (p:Person {name: "Alice", age: 28})`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Verify node was created
	verifyQuery := `MATCH (p:Person) WHERE p.name = "Alice" RETURN p.name, p.age`
	verifyParsed, err := ParseQuery(verifyQuery)
	if err != nil {
		t.Fatalf("Failed to parse verify query: %v", err)
	}

	verifyResult, err := db.ExecuteQuery(verifyParsed)
	if err != nil {
		t.Fatalf("Failed to execute verify query: %v", err)
	}

	if len(verifyResult.Rows) != 1 {
		t.Errorf("Expected 1 person, got %d", len(verifyResult.Rows))
	}

	if verifyResult.Rows[0]["p.name"] != "Alice" {
		t.Errorf("Expected Alice, got %v", verifyResult.Rows[0]["p.name"])
	}

	if verifyResult.Rows[0]["p.age"] != 28 {
		t.Errorf("Expected age 28, got %v", verifyResult.Rows[0]["p.age"])
	}
}

func TestSetProperty(t *testing.T) {
	db := NewGraph()

	// Create a node
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "age", 28)

	// Update property using SET
	query := `MATCH (p:Person) WHERE p.name = "Alice" SET p.age = 29`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.Rows[0]["updated"] != 1 {
		t.Errorf("Expected 1 update, got %v", result.Rows[0]["updated"])
	}

	// Verify update
	verifyQuery := `MATCH (p:Person) WHERE p.name = "Alice" RETURN p.age`
	verifyParsed, err := ParseQuery(verifyQuery)
	if err != nil {
		t.Fatalf("Failed to parse verify query: %v", err)
	}

	verifyResult, err := db.ExecuteQuery(verifyParsed)
	if err != nil {
		t.Fatalf("Failed to execute verify query: %v", err)
	}

	if verifyResult.Rows[0]["p.age"] != 29 {
		t.Errorf("Expected age 29, got %v", verifyResult.Rows[0]["p.age"])
	}
}

func TestDeleteNode(t *testing.T) {
	db := NewGraph()

	// Create a node
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")

	// Delete using DELETE
	query := `MATCH (p:Person) WHERE p.name = "Alice" DELETE p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.Rows[0]["deleted"] != 1 {
		t.Errorf("Expected 1 deletion, got %v", result.Rows[0]["deleted"])
	}

	// Verify deletion
	verifyQuery := `MATCH (p:Person) WHERE p.name = "Alice" RETURN p`
	verifyParsed, err := ParseQuery(verifyQuery)
	if err != nil {
		t.Fatalf("Failed to parse verify query: %v", err)
	}

	verifyResult, err := db.ExecuteQuery(verifyParsed)
	if err != nil {
		t.Fatalf("Failed to execute verify query: %v", err)
	}

	if len(verifyResult.Rows) != 0 {
		t.Errorf("Expected 0 nodes after deletion, got %d", len(verifyResult.Rows))
	}
}

func TestCreateRelationship(t *testing.T) {
	db := NewGraph()

	query := `CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQuery(parsed)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.Rows[0]["created"] != 3 { // 2 nodes + 1 relationship
		t.Errorf("Expected 3 created entities, got %v", result.Rows[0]["created"])
	}

	// Verify relationship was created
	verifyQuery := `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name`
	verifyParsed, err := ParseQuery(verifyQuery)
	if err != nil {
		t.Fatalf("Failed to parse verify query: %v", err)
	}

	verifyResult, err := db.ExecuteQuery(verifyParsed)
	if err != nil {
		t.Fatalf("Failed to execute verify query: %v", err)
	}

	if len(verifyResult.Rows) != 1 {
		t.Errorf("Expected 1 relationship, got %d", len(verifyResult.Rows))
	}
}

// MockEmbedder is a test embedder that generates predictable embeddings
type MockEmbedder struct {
	embeddings map[string][]float32
}

func NewMockEmbedder() *MockEmbedder {
	return &MockEmbedder{
		embeddings: map[string][]float32{
			"backend engineers":     {0.8, 0.2, 0.1},
			"frontend developers":   {0.2, 0.8, 0.1},
			"data scientists":       {0.1, 0.2, 0.8},
			"Person. name: Alice. role: backend engineer":   {0.75, 0.25, 0.1},
			"Person. name: Bob. role: frontend developer":   {0.25, 0.75, 0.1},
			"Person. name: Carol. role: data scientist":     {0.1, 0.25, 0.75},
			"backend engineer":      {0.75, 0.25, 0.1},
			"frontend developer":    {0.25, 0.75, 0.1},
			"data scientist":        {0.1, 0.25, 0.75},
		},
	}
}

func (m *MockEmbedder) Embed(text string) ([]float32, error) {
	if vec, ok := m.embeddings[text]; ok {
		return vec, nil
	}
	// Return a default embedding for unknown text
	return []float32{0.33, 0.33, 0.33}, nil
}

func TestParseEmbedClauseAuto(t *testing.T) {
	query := `MATCH (p:Person) EMBED p AUTO RETURN p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if parsed.EmbedClause == nil {
		t.Fatal("EmbedClause is nil")
	}

	if parsed.EmbedClause.Variable != "p" {
		t.Errorf("Expected variable 'p', got '%s'", parsed.EmbedClause.Variable)
	}

	if parsed.EmbedClause.Mode != "AUTO" {
		t.Errorf("Expected mode 'AUTO', got '%s'", parsed.EmbedClause.Mode)
	}
}

func TestParseEmbedClauseText(t *testing.T) {
	query := `MATCH (p:Person) EMBED p "custom text for embedding" RETURN p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if parsed.EmbedClause == nil {
		t.Fatal("EmbedClause is nil")
	}

	if parsed.EmbedClause.Mode != "TEXT" {
		t.Errorf("Expected mode 'TEXT', got '%s'", parsed.EmbedClause.Mode)
	}

	if parsed.EmbedClause.Text != "custom text for embedding" {
		t.Errorf("Expected text 'custom text for embedding', got '%s'", parsed.EmbedClause.Text)
	}
}

func TestParseEmbedClauseProperty(t *testing.T) {
	query := `MATCH (p:Person) EMBED p.description RETURN p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if parsed.EmbedClause == nil {
		t.Fatal("EmbedClause is nil")
	}

	if parsed.EmbedClause.Mode != "PROPERTY" {
		t.Errorf("Expected mode 'PROPERTY', got '%s'", parsed.EmbedClause.Mode)
	}

	if parsed.EmbedClause.Property != "description" {
		t.Errorf("Expected property 'description', got '%s'", parsed.EmbedClause.Property)
	}
}

func TestParseSimilarToClause(t *testing.T) {
	query := `MATCH (p:Person) SIMILAR TO "backend engineers" LIMIT 10 THRESHOLD 0.7 RETURN p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	if parsed.SimilarToClause == nil {
		t.Fatal("SimilarToClause is nil")
	}

	if parsed.SimilarToClause.QueryText != "backend engineers" {
		t.Errorf("Expected query text 'backend engineers', got '%s'", parsed.SimilarToClause.QueryText)
	}

	if parsed.SimilarToClause.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", parsed.SimilarToClause.Limit)
	}

	if parsed.SimilarToClause.Threshold != 0.7 {
		t.Errorf("Expected threshold 0.7, got %f", parsed.SimilarToClause.Threshold)
	}
}

func TestEmbedQueryExecution(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create test nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "frontend developer")

	// Embed using PROPERTY mode
	query := `MATCH (p:Person) EMBED p.role RETURN p`
	parsed, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(parsed, embedder)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.Rows[0]["embedded"] != 2 {
		t.Errorf("Expected 2 nodes embedded, got %v", result.Rows[0]["embedded"])
	}

	// Verify embeddings were stored
	aliceEmb := db.GetNodeEmbedding(alice.ID)
	if aliceEmb == nil {
		t.Fatal("Alice's embedding is nil")
	}

	bobEmb := db.GetNodeEmbedding(bob.ID)
	if bobEmb == nil {
		t.Fatal("Bob's embedding is nil")
	}
}

func TestSimilarToQueryExecution(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create test nodes with embeddings
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "frontend developer")

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")
	db.SetNodeProperty(carol.ID, "role", "data scientist")

	// First embed all nodes
	embedQuery := `MATCH (p:Person) EMBED p.role RETURN p`
	parsed, err := ParseQuery(embedQuery)
	if err != nil {
		t.Fatalf("Failed to parse embed query: %v", err)
	}

	_, err = db.ExecuteQueryWithEmbedder(parsed, embedder)
	if err != nil {
		t.Fatalf("Failed to execute embed query: %v", err)
	}

	// Now search for similar nodes
	searchQuery := `MATCH (p:Person) SIMILAR TO "backend engineers" RETURN p.name`
	searchParsed, err := ParseQuery(searchQuery)
	if err != nil {
		t.Fatalf("Failed to parse search query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(searchParsed, embedder)
	if err != nil {
		t.Fatalf("Failed to execute search query: %v", err)
	}

	// Should return all 3 nodes sorted by similarity
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 results, got %d", len(result.Rows))
	}

	// First result should be Alice (backend engineer - highest similarity)
	if len(result.Rows) > 0 {
		if result.Rows[0]["p.name"] != "Alice" {
			t.Errorf("Expected first result to be Alice (most similar to backend engineers), got %v", result.Rows[0]["p.name"])
		}
	}
}

func TestSimilarToWithThreshold(t *testing.T) {
	db := NewGraph()
	embedder := NewMockEmbedder()

	// Create test nodes
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "backend engineer")

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "frontend developer")

	// Embed nodes
	embedQuery := `MATCH (p:Person) EMBED p.role RETURN p`
	parsed, _ := ParseQuery(embedQuery)
	db.ExecuteQueryWithEmbedder(parsed, embedder)

	// Search with high threshold - should only return high similarity matches
	searchQuery := `MATCH (p:Person) SIMILAR TO "backend engineers" THRESHOLD 0.9 RETURN p.name`
	searchParsed, err := ParseQuery(searchQuery)
	if err != nil {
		t.Fatalf("Failed to parse search query: %v", err)
	}

	result, err := db.ExecuteQueryWithEmbedder(searchParsed, embedder)
	if err != nil {
		t.Fatalf("Failed to execute search query: %v", err)
	}

	// With high threshold, might get fewer or no results depending on similarity
	// This tests that threshold filtering works
	for _, row := range result.Rows {
		similarity, ok := row["similarity"].(float32)
		if ok && similarity < 0.9 {
			t.Errorf("Result with similarity %f should have been filtered by threshold 0.9", similarity)
		}
	}
}
