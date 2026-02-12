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
