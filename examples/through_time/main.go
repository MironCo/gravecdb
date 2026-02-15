package main

import (
	"fmt"
	"log"
	"time"

	"github.com/MironCo/gravecdb/embedding"
	"github.com/MironCo/gravecdb/graph"
)

func main() {
	fmt.Println("=== GravecDB: Semantic Search THROUGH TIME Demo ===\n")

	// Create database and embedder
	db := graph.NewGraph()
	embedder := embedding.NewOllamaEmbedder()

	fmt.Println("Using Ollama embedder with nomic-embed-text model")
	fmt.Println()

	// Create a person whose role evolves over time
	fmt.Println("1. Creating Alice with initial role...")
	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "software engineer")

	// Embed Alice's initial role
	fmt.Println("2. Embedding Alice's initial role: 'software engineer'")
	query1Str := `MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`
	query1, err := graph.ParseQuery(query1Str)
	if err != nil {
		log.Fatal(err)
	}
	result1, err := db.ExecuteQueryWithEmbedder(query1, embedder)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Embedded %d nodes\n\n", len(result1.Rows))

	// Wait a moment to ensure timestamp difference
	time.Sleep(100 * time.Millisecond)

	// Alice gets promoted
	fmt.Println("3. Alice gets promoted to 'engineering manager'...")
	db.SetNodeProperty(alice.ID, "role", "engineering manager")

	// Embed Alice's new role
	fmt.Println("4. Embedding Alice's new role: 'engineering manager'")
	query2Str := `MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`
	query2, err := graph.ParseQuery(query2Str)
	if err != nil {
		log.Fatal(err)
	}
	result2, err := db.ExecuteQueryWithEmbedder(query2, embedder)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Re-embedded %d nodes\n\n", len(result2.Rows))

	// Wait again
	time.Sleep(100 * time.Millisecond)

	// Alice gets promoted again
	fmt.Println("5. Alice gets promoted to 'director of engineering'...")
	db.SetNodeProperty(alice.ID, "role", "director of engineering")

	// Embed Alice's third role
	fmt.Println("6. Embedding Alice's latest role: 'director of engineering'")
	query3Str := `MATCH (p:Person {name: "Alice"}) EMBED p.role RETURN p`
	query3, err := graph.ParseQuery(query3Str)
	if err != nil {
		log.Fatal(err)
	}
	result3, err := db.ExecuteQueryWithEmbedder(query3, embedder)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Re-embedded %d nodes\n\n", len(result3.Rows))

	fmt.Println("=== Now let's search! ===\n")

	// Regular semantic search (current time only)
	fmt.Println("7. Regular semantic search for 'engineer':")
	searchQueryStr := `MATCH (p:Person) SIMILAR TO "engineer" RETURN p.name, p.role, similarity`
	searchQuery, err := graph.ParseQuery(searchQueryStr)
	if err != nil {
		log.Fatal(err)
	}
	searchResult, err := db.ExecuteQueryWithEmbedder(searchQuery, embedder)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("   Found %d result(s):\n", len(searchResult.Rows))
	for _, row := range searchResult.Rows {
		fmt.Printf("   - %s: %s (similarity: %.2f)\n",
			row["p.name"], row["p.role"], row["similarity"])
	}
	fmt.Println()

	// THROUGH TIME search - this is the new feature!
	fmt.Println("8. Semantic search for 'engineer' THROUGH TIME:")
	throughTimeQueryStr := `MATCH (p:Person) SIMILAR TO "engineer" THROUGH TIME RETURN p.name, p.role, similarity, valid_from, valid_to`
	throughTimeQuery, err := graph.ParseQuery(throughTimeQueryStr)
	if err != nil {
		log.Fatal(err)
	}
	throughTimeResult, err := db.ExecuteQueryWithEmbedder(throughTimeQuery, embedder)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("   Found %d version(s):\n", len(throughTimeResult.Rows))
	for i, row := range throughTimeResult.Rows {
		validTo := "present"
		if row["valid_to"] != nil {
			if t, ok := row["valid_to"].(time.Time); ok {
				validTo = t.Format("15:04:05")
			}
		}
		validFrom := ""
		if t, ok := row["valid_from"].(time.Time); ok {
			validFrom = t.Format("15:04:05")
		}

		fmt.Printf("   Version %d:\n", i+1)
		fmt.Printf("     Name: %s\n", row["p.name"])
		fmt.Printf("     Role: %s\n", row["p.role"])
		fmt.Printf("     Similarity: %.2f\n", row["similarity"])
		fmt.Printf("     Valid: %s → %s\n", validFrom, validTo)
		fmt.Println()
	}

	// Search for manager roles through time
	fmt.Println("9. Semantic search for 'manager' THROUGH TIME:")
	managerQueryStr := `MATCH (p:Person) SIMILAR TO "manager" THROUGH TIME LIMIT 5 RETURN p.name, p.role, similarity`
	managerQuery, err := graph.ParseQuery(managerQueryStr)
	if err != nil {
		log.Fatal(err)
	}
	managerResult, err := db.ExecuteQueryWithEmbedder(managerQuery, embedder)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("   Found %d version(s):\n", len(managerResult.Rows))
	for i, row := range managerResult.Rows {
		fmt.Printf("   Version %d: %s - %s (similarity: %.2f)\n",
			i+1, row["p.name"], row["p.role"], row["similarity"])
	}
	fmt.Println()

	// Test VERSIONS THROUGH TIME syntax
	fmt.Println("10. Semantic search with VERSIONS THROUGH TIME syntax:")
	versionsQueryStr := `MATCH (p:Person) SIMILAR TO "engineer" VERSIONS THROUGH TIME RETURN p.name, p.role, similarity`
	versionsQuery, err := graph.ParseQuery(versionsQueryStr)
	if err != nil {
		log.Fatal(err)
	}
	versionsResult, err := db.ExecuteQueryWithEmbedder(versionsQuery, embedder)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("   Found %d version(s) using VERSIONS keyword:\n", len(versionsResult.Rows))
	for i, row := range versionsResult.Rows {
		fmt.Printf("   Version %d: %s - %s (similarity: %.2f)\n",
			i+1, row["p.name"], row["p.role"], row["similarity"])
	}
	fmt.Println()

	// DRIFT THROUGH TIME - shows semantic drift metrics!
	fmt.Println("11. Semantic search with DRIFT THROUGH TIME:")
	driftQueryStr := `MATCH (p:Person) SIMILAR TO "engineer" DRIFT THROUGH TIME RETURN p.name, p.role, similarity, drift_from_previous, drift_from_first`
	driftQuery, err := graph.ParseQuery(driftQueryStr)
	if err != nil {
		log.Fatal(err)
	}
	driftResult, err := db.ExecuteQueryWithEmbedder(driftQuery, embedder)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("   Found %d version(s) with drift analysis:\n", len(driftResult.Rows))
	for i, row := range driftResult.Rows {
		fmt.Printf("   Version %d:\n", i+1)
		fmt.Printf("     Name: %s\n", row["p.name"])
		fmt.Printf("     Role: %s\n", row["p.role"])
		fmt.Printf("     Similarity: %.2f\n", row["similarity"])
		fmt.Printf("     Drift from previous: %.3f\n", row["drift_from_previous"])
		fmt.Printf("     Drift from first: %.3f\n", row["drift_from_first"])
		fmt.Println()
	}

	fmt.Println("=== Demo Complete! ===")
	fmt.Println("\nKey takeaways:")
	fmt.Println("- THROUGH TIME or VERSIONS THROUGH TIME returns ALL historical versions")
	fmt.Println("- DRIFT THROUGH TIME adds semantic drift metrics")
	fmt.Println("- Track how a node's meaning evolved over time!")
}
