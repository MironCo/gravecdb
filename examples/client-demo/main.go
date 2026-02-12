package main

import (
	"fmt"
	"log"

	"github.com/MironCo/gravecdb/client"
	"github.com/MironCo/gravecdb/graph"
)

func main() {
	fmt.Println("=== Graph Database Client Demo ===\n")

	// Connect to the graph database server
	fmt.Println("Connecting to http://localhost:8080...")
	conn, err := client.Connect("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Println("Connected!\n")

	// Example 1: Query all people
	fmt.Println("=== Query 1: Find all people ===")
	result, err := conn.Query(`MATCH (p:Person) RETURN p.name, p.role`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printResult(result)
	}

	// Example 2: Find engineers
	fmt.Println("\n=== Query 2: Find all engineers ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.role = "Engineer" RETURN p.name, p.role`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printResult(result)
	}

	// Example 3: Find work relationships
	fmt.Println("\n=== Query 3: Find who works where ===")
	result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printResult(result)
	}

	// Example 4: Create a new person
	fmt.Println("\n=== Creating a new person ===")
	newPersonID, err := conn.CreateNode(
		[]string{"Person"},
		map[string]interface{}{
			"name": "Grace",
			"role": "Architect",
			"age":  29,
		},
	)
	if err != nil {
		log.Printf("Create error: %v\n", err)
	} else {
		fmt.Printf("Created new person with ID: %s\n", newPersonID)
	}

	// Example 5: Query the new person
	fmt.Println("\n=== Query 4: Find Grace ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.name = "Grace" RETURN p.name, p.role, p.age`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printResult(result)
	}

	// Example 6: Complex query with filters
	fmt.Println("\n=== Query 5: Find senior people (age > 30) ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printResult(result)
	}

	// Example 7: Path-finding query - find shortest path
	fmt.Println("\n=== Query 6: Find shortest path between Alice and David ===")
	result, err = conn.Query(`MATCH path = shortestPath((a:Person)-[*]-(b:Person)) WHERE a.name = "Alice" AND b.name = "David" RETURN path`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printPathResult(result)
	}

	// Example 8: CREATE nodes and relationships via query
	fmt.Println("\n=== CREATE: Adding new person and relationship ===")
	result, err = conn.Query(`CREATE (h:Person {name: "Henry", role: "Designer", age: 27})`)
	if err != nil {
		log.Printf("CREATE error: %v\n", err)
	} else {
		fmt.Printf("  Created %v entities\n", result.Rows[0]["created"])
	}

	// Example 9: CREATE relationship between existing nodes
	fmt.Println("\n=== CREATE: Connect Henry to Grace ===")
	result, err = conn.Query(`CREATE (a:Person {name: "Henry"})-[:KNOWS]->(b:Person {name: "Grace"})`)
	if err != nil {
		log.Printf("CREATE error: %v\n", err)
	} else {
		fmt.Printf("  Created %v entities (reusing existing nodes pattern)\n", result.Rows[0]["created"])
	}

	// Example 10: UPDATE using SET
	fmt.Println("\n=== UPDATE: Change Grace's age ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.name = "Grace" SET p.age = 30`)
	if err != nil {
		log.Printf("UPDATE error: %v\n", err)
	} else {
		fmt.Printf("  Updated %v properties\n", result.Rows[0]["updated"])
	}

	// Example 11: Verify the UPDATE
	fmt.Println("\n=== Query 7: Verify Grace's new age ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.name = "Grace" RETURN p.name, p.age`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		printResult(result)
	}

	// Example 12: DELETE a node
	fmt.Println("\n=== DELETE: Remove Henry ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.name = "Henry" DELETE p`)
	if err != nil {
		log.Printf("DELETE error: %v\n", err)
	} else {
		fmt.Printf("  Deleted %v entities\n", result.Rows[0]["deleted"])
	}

	// Example 13: Verify the DELETE
	fmt.Println("\n=== Query 8: Verify Henry is gone ===")
	result, err = conn.Query(`MATCH (p:Person) WHERE p.name = "Henry" RETURN p.name`)
	if err != nil {
		log.Printf("Query error: %v\n", err)
	} else {
		if len(result.Rows) == 0 {
			fmt.Println("  Henry has been deleted successfully!")
		} else {
			printResult(result)
		}
	}

	fmt.Println("\n=== Demo Complete ===")
}

func printResult(result *graph.QueryResult) {
	if len(result.Rows) == 0 {
		fmt.Println("  (no results)")
		return
	}

	// Print header
	fmt.Print("  ")
	for i, col := range result.Columns {
		if i > 0 {
			fmt.Print(" | ")
		}
		fmt.Printf("%-20s", col)
	}
	fmt.Println()

	// Print separator
	fmt.Print("  ")
	for i := range result.Columns {
		if i > 0 {
			fmt.Print("-+-")
		}
		fmt.Print("--------------------")
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		fmt.Print("  ")
		for i, col := range result.Columns {
			if i > 0 {
				fmt.Print(" | ")
			}
			value := row[col]
			fmt.Printf("%-20v", value)
		}
		fmt.Println()
	}

	fmt.Printf("\n  (%d rows)\n", len(result.Rows))
}

func printPathResult(result *graph.QueryResult) {
	if len(result.Rows) == 0 {
		fmt.Println("  (no paths found)")
		return
	}

	for i, row := range result.Rows {
		fmt.Printf("  Path %d:\n", i+1)
		for _, col := range result.Columns {
			value := row[col]
			if path, ok := value.(*graph.Path); ok {
				fmt.Printf("    Length: %d hops\n", path.Length)
				fmt.Print("    Route: ")
				for j, node := range path.Nodes {
					name := node.Properties["name"]
					fmt.Printf("%s", name)
					if j < len(path.Relationships) {
						rel := path.Relationships[j]
						fmt.Printf(" -[%s]-> ", rel.Type)
					}
				}
				fmt.Println()
			}
		}
	}

	fmt.Printf("\n  (%d paths)\n", len(result.Rows))
}
