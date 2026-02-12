package main

import (
	"fmt"

	"github.com/miron/go-graph-database/graph"
)

func main() {
	fmt.Println("=== Cypher-like Query Language Demo ===\n")

	// Create a graph with demo data
	db := graph.NewGraph()
	setupDemoData(db)

	// Example queries
	queries := []string{
		// Basic node query
		`MATCH (p:Person) RETURN p`,

		// Query with property filter
		`MATCH (p:Person) WHERE p.name = "Alice" RETURN p`,

		// Relationship pattern
		`MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name`,

		// Multiple filters
		`MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age`,

		// Friendship query
		`MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) RETURN a.name, b.name`,

		// Find engineers at TechCorp
		`MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE p.role = "Engineer" AND c.name = "TechCorp" RETURN p.name, p.role`,

		// Mentorship relationships
		`MATCH (mentor:Person)-[:MENTORS]->(mentee:Person) RETURN mentor.name, mentee.name`,
	}

	for i, queryStr := range queries {
		fmt.Printf("Query %d:\n%s\n\n", i+1, queryStr)

		// Parse the query
		query, err := graph.ParseQuery(queryStr)
		if err != nil {
			fmt.Printf("Error parsing query: %v\n\n", err)
			continue
		}

		// Execute the query
		result, err := db.ExecuteQuery(query)
		if err != nil {
			fmt.Printf("Error executing query: %v\n\n", err)
			continue
		}

		// Print results
		printResult(result)
		fmt.Println()
	}
}

func setupDemoData(db *graph.Graph) {
	// Create people
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "Engineer")
	db.SetNodeProperty(alice.ID, "age", 28)

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "Designer")
	db.SetNodeProperty(bob.ID, "age", 32)

	carol := db.CreateNode("Person")
	db.SetNodeProperty(carol.ID, "name", "Carol")
	db.SetNodeProperty(carol.ID, "role", "Manager")
	db.SetNodeProperty(carol.ID, "age", 35)

	david := db.CreateNode("Person")
	db.SetNodeProperty(david.ID, "name", "David")
	db.SetNodeProperty(david.ID, "role", "DevOps")
	db.SetNodeProperty(david.ID, "age", 24)

	// Create companies
	techCorp := db.CreateNode("Company")
	db.SetNodeProperty(techCorp.ID, "name", "TechCorp")

	startup := db.CreateNode("Company")
	db.SetNodeProperty(startup.ID, "name", "CoolStartup")

	// Create employment relationships
	rel1, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(rel1.ID, "title", "Senior Engineer")

	rel2, _ := db.CreateRelationship("WORKS_AT", bob.ID, techCorp.ID)
	db.SetRelationshipProperty(rel2.ID, "title", "Lead Designer")

	rel3, _ := db.CreateRelationship("WORKS_AT", carol.ID, techCorp.ID)
	db.SetRelationshipProperty(rel3.ID, "title", "Engineering Manager")

	rel4, _ := db.CreateRelationship("WORKS_AT", david.ID, startup.ID)
	db.SetRelationshipProperty(rel4.ID, "title", "DevOps Engineer")

	// Create friendships
	db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	db.CreateRelationship("FRIENDS_WITH", bob.ID, david.ID)

	// Create mentorship
	db.CreateRelationship("MENTORS", carol.ID, alice.ID)
}

func printResult(result *graph.QueryResult) {
	if len(result.Rows) == 0 {
		fmt.Println("No results found")
		return
	}

	// Print header
	fmt.Print("┌")
	for i, col := range result.Columns {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(padString(col, 25, "─"))
	}
	fmt.Println("┐")

	// Print column names
	fmt.Print("│")
	for _, col := range result.Columns {
		fmt.Printf(" %-24s│", col)
	}
	fmt.Println()

	// Print separator
	fmt.Print("├")
	for i := range result.Columns {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(padString("", 25, "─"))
	}
	fmt.Println("┤")

	// Print rows
	for _, row := range result.Rows {
		fmt.Print("│")
		for _, col := range result.Columns {
			value := row[col]
			valueStr := formatValue(value)
			fmt.Printf(" %-24s│", truncate(valueStr, 24))
		}
		fmt.Println()
	}

	// Print footer
	fmt.Print("└")
	for i := range result.Columns {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(padString("", 25, "─"))
	}
	fmt.Println("┘")

	fmt.Printf("(%d rows)\n", len(result.Rows))
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case *graph.Node:
		name := val.Properties["name"]
		if name != nil {
			return fmt.Sprintf("Node{%v}", name)
		}
		return fmt.Sprintf("Node{%s}", val.ID[:8])
	case *graph.Relationship:
		return fmt.Sprintf("Rel{%s}", val.Type)
	default:
		return fmt.Sprint(val)
	}
}

func padString(s string, length int, pad string) string {
	for len(s) < length {
		s += pad
	}
	return s
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
