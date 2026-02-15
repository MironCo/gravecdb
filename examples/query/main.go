package main

import (
	"fmt"
	"time"

	"github.com/MironCo/gravecdb/graph"
)

func main() {
	fmt.Println("=== Cypher-like Query Language Demo ===\n")

	// Create a graph with persistence
	db, err := graph.NewGraphWithBolt("./data")
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}
	fmt.Println("Database loaded from ./data (will persist changes)\n")

	// Check if data already exists
	checkQuery, _ := graph.ParseQuery(`MATCH (p:Person) RETURN p`)
	checkResult, _ := db.ExecuteQuery(checkQuery)

	if len(checkResult.Rows) == 0 {
		// Setup demo data using query language
		fmt.Println("Setting up demo data via CREATE queries...\n")
		setupDemoDataViaQueries(db)
	} else {
		fmt.Printf("Found existing data (%d people in database)\n\n", len(checkResult.Rows))
	}

	// Example queries
	queries := []string{
		// === READ queries ===
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

		// === CREATE queries ===
		// Create a new person
		`CREATE (p:Person {name: "Eve", role: "Architect", age: 30})`,

		// Create a person with relationships
		`CREATE (p:Person {name: "Frank", role: "Intern", age: 22})-[:WORKS_AT]->(c:Company {name: "NewCo"})`,

		// Verify Eve was created
		`MATCH (p:Person) WHERE p.name = "Eve" RETURN p.name, p.role, p.age`,

		// === UPDATE queries ===
		// Update Alice's age
		`MATCH (p:Person) WHERE p.name = "Alice" SET p.age = 29`,

		// Verify Alice's age was updated
		`MATCH (p:Person) WHERE p.name = "Alice" RETURN p.name, p.age`,

		// === DELETE queries ===
		// Delete Eve
		`MATCH (p:Person) WHERE p.name = "Eve" DELETE p`,

		// Verify Eve was deleted
		`MATCH (p:Person) WHERE p.name = "Eve" RETURN p.name`,

		// === PATH-FINDING queries ===
		// Find shortest path from Alice to David
		`MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "David" RETURN path`,

		// === TEMPORAL queries ===
		// Find earliest connection between Alice and Bob
		`MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "Bob" AT TIME EARLIEST RETURN path`,

		// Query nodes at earliest time
		`MATCH (p:Person) AT TIME EARLIEST RETURN p.name, p.role`,

		// === EMBEDDING queries ===
		// Note: These require an embedder to be passed to ExecuteQueryWithEmbedder()
		// Uncomment and use with OpenAI embedder for real semantic search

		// Embed nodes using AUTO mode (combines labels + properties)
		// `MATCH (p:Person) EMBED p AUTO RETURN p`,

		// Embed nodes using a specific property
		// `MATCH (p:Person) EMBED p.role RETURN p`,

		// Embed nodes using literal text
		// `MATCH (p:Person) EMBED p "software engineer with experience" RETURN p`,

		// Semantic search - find similar nodes
		// `MATCH (p:Person) SIMILAR TO "backend developers" RETURN p.name`,

		// Semantic search with limit and threshold
		// `MATCH (p:Person) SIMILAR TO "engineering managers" LIMIT 5 THRESHOLD 0.7 RETURN p.name, p.role`,
	}

	for i, queryStr := range queries {
		fmt.Printf("Query %d:\n%s\n\n", i+1, queryStr)

		// Parse the query
		start := time.Now()
		query, err := graph.ParseQuery(queryStr)
		parseTime := time.Since(start)

		if err != nil {
			fmt.Printf("Error parsing query: %v\n\n", err)
			continue
		}

		// Execute the query
		start = time.Now()
		result, err := db.ExecuteQuery(query)
		execTime := time.Since(start)

		if err != nil {
			fmt.Printf("Error executing query: %v\n\n", err)
			continue
		}

		// Print results
		printResult(result)
		fmt.Printf("⏱️  Parse: %v | Execute: %v | Total: %v\n", parseTime, execTime, parseTime+execTime)
		fmt.Println("---\n")
	}

	// Performance benchmark
	fmt.Println("\n=== Performance Benchmark ===\n")
	runPerformanceBenchmark(db)
}

func setupDemoDataViaQueries(db *graph.Graph) {
	// Create demo data using CREATE queries
	setupQueries := []string{
		// Create people
		`CREATE (p:Person {name: "Alice", role: "Engineer", age: 28})`,
		`CREATE (p:Person {name: "Bob", role: "Designer", age: 32})`,
		`CREATE (p:Person {name: "Carol", role: "Manager", age: 35})`,
		`CREATE (p:Person {name: "David", role: "DevOps", age: 24})`,

		// Create companies
		`CREATE (c:Company {name: "TechCorp"})`,
		`CREATE (c:Company {name: "CoolStartup"})`,

		// Create employment relationships (creates nodes if they don't exist in this simple version)
		`CREATE (a:Person {name: "Alice"})-[:WORKS_AT {title: "Senior Engineer"}]->(c:Company {name: "TechCorp"})`,
		`CREATE (b:Person {name: "Bob"})-[:WORKS_AT {title: "Lead Designer"}]->(c:Company {name: "TechCorp"})`,
		`CREATE (c:Person {name: "Carol"})-[:WORKS_AT {title: "Engineering Manager"}]->(co:Company {name: "TechCorp"})`,
		`CREATE (d:Person {name: "David"})-[:WORKS_AT {title: "DevOps Engineer"}]->(s:Company {name: "CoolStartup"})`,

		// Create friendships
		`CREATE (a:Person {name: "Alice"})-[:FRIENDS_WITH]->(b:Person {name: "Bob"})`,
		`CREATE (b:Person {name: "Bob"})-[:FRIENDS_WITH]->(d:Person {name: "David"})`,

		// Create mentorship
		`CREATE (c:Person {name: "Carol"})-[:MENTORS]->(a:Person {name: "Alice"})`,
	}

	start := time.Now()
	for _, queryStr := range setupQueries {
		query, err := graph.ParseQuery(queryStr)
		if err != nil {
			fmt.Printf("Error parsing setup query: %v\n", err)
			continue
		}
		_, err = db.ExecuteQuery(query)
		if err != nil {
			fmt.Printf("Error executing setup query: %v\n", err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("Created demo data in %v\n\n", elapsed)
}

func printResult(result *graph.QueryResult) {
	if len(result.Rows) == 0 {
		fmt.Println("No results found")
		return
	}

	// Check if this is a path result
	if len(result.Columns) == 1 && len(result.Rows) > 0 {
		firstVal := result.Rows[0][result.Columns[0]]
		if _, ok := firstVal.(*graph.Path); ok {
			printPathResult(result)
			return
		}
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

func printPathResult(result *graph.QueryResult) {
	for i, row := range result.Rows {
		for _, col := range result.Columns {
			value := row[col]
			if p, ok := value.(*graph.Path); ok {
				fmt.Printf("Path %d (length: %d hops):\n", i+1, p.Length)
				fmt.Print("  ")
				for j, node := range p.Nodes {
					name := node.Properties["name"]
					fmt.Printf("%s", name)
					if j < len(p.Relationships) {
						rel := p.Relationships[j]
						fmt.Printf(" -[%s]-> ", rel.Type)
					}
				}
				fmt.Println()
			}
		}
	}
	fmt.Printf("(%d paths)\n", len(result.Rows))
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

func runPerformanceBenchmark(db *graph.Graph) {
	benchmarks := []struct {
		name  string
		query string
		runs  int
	}{
		{"Simple MATCH", `MATCH (p:Person) RETURN p.name`, 1000},
		{"Filtered MATCH", `MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age`, 1000},
		{"Relationship MATCH", `MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name`, 1000},
		{"CREATE Node", `CREATE (p:Person {name: "Temp", age: 25})`, 100},
		{"UPDATE Property", `MATCH (p:Person) WHERE p.name = "Alice" SET p.age = 28`, 100},
		{"Path Finding", `MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "David" RETURN path`, 100},
	}

	for _, bm := range benchmarks {
		// Warmup
		parsed, _ := graph.ParseQuery(bm.query)
		db.ExecuteQuery(parsed)

		// Benchmark
		start := time.Now()
		for i := 0; i < bm.runs; i++ {
			parsed, err := graph.ParseQuery(bm.query)
			if err != nil {
				continue
			}
			db.ExecuteQuery(parsed)
		}
		elapsed := time.Since(start)

		avgTime := elapsed / time.Duration(bm.runs)
		qps := float64(bm.runs) / elapsed.Seconds()

		fmt.Printf("%-20s | %d runs | Avg: %8v | QPS: %8.0f\n", bm.name, bm.runs, avgTime, qps)
	}

	// Cleanup temp nodes
	cleanupQuery, _ := graph.ParseQuery(`MATCH (p:Person) WHERE p.name = "Temp" DELETE p`)
	db.ExecuteQuery(cleanupQuery)
}
