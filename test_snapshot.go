package main

import (
	"fmt"
	"github.com/miron/go-graph-database/graph"
)

func main() {
	// Create database
	db, _ := graph.NewGraphWithPersistence("./test-data")
	
	// Add some data
	q1, _ := graph.ParseQuery(`CREATE (p:Person {name: "Alice", age: 30})`)
	db.ExecuteQuery(q1)
	
	q2, _ := graph.ParseQuery(`CREATE (p:Person {name: "Bob", age: 25})`)
	db.ExecuteQuery(q2)
	
	q3, _ := graph.ParseQuery(`CREATE (p:Person {name: "Carol", age: 35})`)
	db.ExecuteQuery(q3)
	
	// Manually create a snapshot
	fmt.Println("Creating snapshot...")
	if err := db.Snapshot(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	db.Close()
	fmt.Println("Snapshot created successfully!")
}
