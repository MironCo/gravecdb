package main

import (
	"fmt"
	"os"

	"github.com/MironCo/gravecdb/embedding"
	"github.com/MironCo/gravecdb/graph"
)

func main() {
	fmt.Println("=== Vector Embedding & Semantic Search Demo ===")
	fmt.Println()

	// Create embedder from environment or use default Ollama
	embedder, err := getEmbedder()
	if err != nil {
		fmt.Printf("Error initializing embedder: %v\n", err)
		fmt.Println("\nMake sure Ollama is running:")
		fmt.Println("  ollama pull nomic-embed-text")
		fmt.Println("  ollama serve")
		os.Exit(1)
	}

	// Display embedder info
	printEmbedderInfo(embedder)

	// Create a new graph
	db := graph.NewGraph()

	// Create some people with roles
	fmt.Println("\nCreating nodes...")
	createPeople(db)

	// Step 1: Embed all person nodes using their role property
	fmt.Println("\n--- Embedding Nodes ---")
	embedQuery := `MATCH (p:Person) EMBED p.role RETURN p`
	runQuery(db, embedder, embedQuery, "Embed nodes by role property")

	// Step 2: Semantic search for backend developers
	fmt.Println("\n--- Semantic Search ---")
	searchQuery := `MATCH (p:Person) SIMILAR TO "backend developers" RETURN p.name, p.role`
	runQuery(db, embedder, searchQuery, "Find people similar to 'backend developers'")

	// Step 3: Search for frontend developers
	searchQuery2 := `MATCH (p:Person) SIMILAR TO "frontend developers" RETURN p.name, p.role`
	runQuery(db, embedder, searchQuery2, "Find people similar to 'frontend developers'")

	// Step 4: Search for data scientists
	searchQuery3 := `MATCH (p:Person) SIMILAR TO "data scientists" RETURN p.name, p.role`
	runQuery(db, embedder, searchQuery3, "Find people similar to 'data scientists'")

	// Step 5: Search with threshold
	fmt.Println("\n--- Search with Threshold ---")
	thresholdQuery := `MATCH (p:Person) SIMILAR TO "backend developers" THRESHOLD 0.8 RETURN p.name, p.role`
	runQuery(db, embedder, thresholdQuery, "Backend developers with similarity >= 0.8")

	// Step 6: Search with limit
	limitQuery := `MATCH (p:Person) SIMILAR TO "engineering managers" LIMIT 2 RETURN p.name, p.role`
	runQuery(db, embedder, limitQuery, "Top 2 matches for 'engineering managers'")

	fmt.Println("\n=== Demo Complete ===")
}

func getEmbedder() (embedding.Embedder, error) {
	// Try to get embedder from DSN or environment
	embedder, err := embedding.Default()
	if err == nil {
		return embedder, nil
	}

	// Fall back to default Ollama
	return embedding.NewOllamaEmbedder(), nil
}

func printEmbedderInfo(embedder embedding.Embedder) {
	switch e := embedder.(type) {
	case *embedding.OllamaEmbedder:
		fmt.Printf("Using Ollama embedder\n")
		fmt.Printf("  Model: %s\n", e.Model())
	case *embedding.OpenAIEmbedder:
		fmt.Printf("Using OpenAI embedder\n")
		fmt.Printf("  Model: %s\n", e.Model())
	default:
		fmt.Printf("Using embedder: %T\n", embedder)
	}
}

func createPeople(db *graph.Graph) {
	people := []struct {
		name string
		role string
		age  int
	}{
		{"Alice", "backend engineer", 28},
		{"Bob", "frontend developer", 32},
		{"Carol", "data scientist", 29},
		{"David", "devops engineer", 26},
		{"Eve", "engineering manager", 35},
	}

	for _, p := range people {
		node := db.CreateNode("Person")
		db.SetNodeProperty(node.ID, "name", p.name)
		db.SetNodeProperty(node.ID, "role", p.role)
		db.SetNodeProperty(node.ID, "age", p.age)
		fmt.Printf("  Created: %s (%s)\n", p.name, p.role)
	}
}

func runQuery(db *graph.Graph, embedder embedding.Embedder, queryStr string, description string) {
	fmt.Printf("\nQuery: %s\n", description)
	fmt.Printf("  %s\n", queryStr)

	parsed, err := graph.ParseQuery(queryStr)
	if err != nil {
		fmt.Printf("  Error parsing: %v\n", err)
		return
	}

	result, err := db.ExecuteQueryWithEmbedder(parsed, embedder)
	if err != nil {
		fmt.Printf("  Error executing: %v\n", err)
		return
	}

	fmt.Printf("\nResults (%d rows):\n", len(result.Rows))
	if len(result.Rows) == 0 {
		fmt.Println("  No results")
		return
	}

	// Print header
	fmt.Print("  ")
	for _, col := range result.Columns {
		fmt.Printf("%-20s", col)
	}
	fmt.Println()
	fmt.Print("  ")
	for range result.Columns {
		fmt.Print("--------------------")
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		fmt.Print("  ")
		for _, col := range result.Columns {
			val := row[col]
			if similarity, ok := val.(float32); ok {
				fmt.Printf("%-20.4f", similarity)
			} else {
				fmt.Printf("%-20v", val)
			}
		}
		fmt.Println()
	}
}
