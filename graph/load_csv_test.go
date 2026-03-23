package graph

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCSVWithHeaders(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Create a temp CSV file with headers
	csvContent := `name,role,age
Alice,engineer,30
Bob,designer,25
Carol,manager,35`

	csvPath := filepath.Join(t.TempDir(), "people.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Load CSV and create nodes
	query, err := ParseQuery(`LOAD CSV WITH HEADERS FROM '` + csvPath + `' AS row CREATE (p:Person {name: row.name, role: row.role, age: row.age})`)
	if err != nil {
		t.Fatalf("Failed to parse LOAD CSV: %v", err)
	}
	result, err := db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute LOAD CSV: %v", err)
	}

	// Should report 3 rows loaded
	if len(result.Rows) != 1 || result.Rows[0]["loaded"] != 3 {
		t.Fatalf("Expected loaded=3, got %v", result.Rows)
	}

	// Verify all 3 nodes were created
	nodes := db.GetNodesByLabel("Person")
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 Person nodes, got %d", len(nodes))
	}

	// Verify properties
	nameSet := make(map[string]bool)
	for _, n := range nodes {
		name, _ := n.Properties["name"].(string)
		nameSet[name] = true
		if name == "Alice" {
			if n.Properties["role"] != "engineer" {
				t.Errorf("Alice role: expected 'engineer', got %v", n.Properties["role"])
			}
		}
	}
	for _, name := range []string{"Alice", "Bob", "Carol"} {
		if !nameSet[name] {
			t.Errorf("Missing person: %s", name)
		}
	}
}

func TestLoadCSVWithoutHeaders(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// CSV without headers — rows are indexed arrays
	csvContent := `TechCorp,2020
CoolStartup,2023
BigCo,2010`

	csvPath := filepath.Join(t.TempDir(), "companies.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	query, err := ParseQuery(`LOAD CSV FROM '` + csvPath + `' AS row CREATE (c:Company {name: row})`)
	if err != nil {
		t.Fatalf("Failed to parse LOAD CSV: %v", err)
	}
	result, err := db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute LOAD CSV: %v", err)
	}

	if len(result.Rows) != 1 || result.Rows[0]["loaded"] != 3 {
		t.Fatalf("Expected loaded=3, got %v", result.Rows)
	}

	nodes := db.GetNodesByLabel("Company")
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 Company nodes, got %d", len(nodes))
	}
}

func TestLoadCSVWithCustomDelimiter(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	// Tab-separated file
	csvContent := "name\trole\nAlice\tengineer\nBob\tdesigner"

	csvPath := filepath.Join(t.TempDir(), "people.tsv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write TSV: %v", err)
	}

	query, err := ParseQuery(`LOAD CSV WITH HEADERS FROM '` + csvPath + `' AS row FIELDTERMINATOR '\t' CREATE (p:Person {name: row.name, role: row.role})`)
	if err != nil {
		t.Fatalf("Failed to parse LOAD CSV with FIELDTERMINATOR: %v", err)
	}
	result, err := db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute LOAD CSV: %v", err)
	}

	if len(result.Rows) != 1 || result.Rows[0]["loaded"] != 2 {
		t.Fatalf("Expected loaded=2, got %v", result.Rows)
	}

	nodes := db.GetNodesByLabel("Person")
	if len(nodes) != 2 {
		t.Fatalf("Expected 2 Person nodes, got %d", len(nodes))
	}
}

func TestLoadCSVWithAtTime(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	csvContent := `name,role
Alice,engineer
Bob,designer`

	csvPath := filepath.Join(t.TempDir(), "people.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Load with AT TIME — all nodes should get the historical timestamp
	query, err := ParseQuery(`LOAD CSV WITH HEADERS FROM '` + csvPath + `' AS row CREATE (p:Person {name: row.name}) AT TIME 1672531200`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	_, err = db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute: %v", err)
	}

	// Nodes should be visible at Jan 2023
	query, err = ParseQuery(`MATCH (p:Person) AT TIME 1672531201 RETURN p.name`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	result, err := db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed query: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 nodes visible at Jan 2023, got %d", len(result.Rows))
	}

	// Nodes should NOT be visible before creation
	query, err = ParseQuery(`MATCH (p:Person) AT TIME 1672531199 RETURN p.name`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	result, err = db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed query: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 nodes visible before creation, got %d", len(result.Rows))
	}
}

func TestLoadCSVAutoConvert(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	csvContent := `name,age,score,active
Alice,30,95.5,true
Bob,25,88.0,false`

	csvPath := filepath.Join(t.TempDir(), "typed.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	query, err := ParseQuery(`LOAD CSV WITH HEADERS FROM '` + csvPath + `' AS row CREATE (p:Person {name: row.name, age: row.age, score: row.score, active: row.active})`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	_, err = db.ExecuteQueryWithEmbedder(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute: %v", err)
	}

	nodes := db.GetNodesByLabel("Person")
	if len(nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(nodes))
	}

	for _, n := range nodes {
		name := n.Properties["name"]
		if name == "Alice" {
			// age and score come through as float64 after storage roundtrip
			age, _ := toFloat64(n.Properties["age"])
			if age != 30 {
				t.Errorf("Alice age: expected 30, got %v (%T)", n.Properties["age"], n.Properties["age"])
			}
			score, _ := toFloat64(n.Properties["score"])
			if score != 95.5 {
				t.Errorf("Alice score: expected 95.5, got %v (%T)", n.Properties["score"], n.Properties["score"])
			}
			if active, ok := n.Properties["active"].(bool); !ok || active != true {
				t.Errorf("Alice active: expected bool(true), got %T(%v)", n.Properties["active"], n.Properties["active"])
			}
		}
	}
}

func TestLoadCSVFileNotFound(t *testing.T) {
	db, cleanup := newTestGraph(t)
	defer cleanup()

	query, err := ParseQuery(`LOAD CSV FROM '/nonexistent/file.csv' AS row CREATE (n:Node {name: row})`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	_, err = db.ExecuteQueryWithEmbedder(query, nil)
	if err == nil {
		t.Fatal("Expected error for nonexistent file")
	}
}
