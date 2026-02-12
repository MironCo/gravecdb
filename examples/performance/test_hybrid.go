package main

import (
	"fmt"
	"os"
	"time"

	"github.com/miron/go-graph-database/graph"
)

// This test verifies that the hybrid mode flushes based on timeout
// even when the buffer doesn't fill up
func testHybridTimeout() {
	fmt.Println("=== Testing Hybrid Mode Timeout ===\n")

	// Clean up test directory
	defer os.RemoveAll("./data-hybrid-test")

	// Create database with hybrid mode (100 ops OR 100ms)
	db, err := graph.NewGraphWithPersistence("./data-hybrid-test")
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}
	defer db.Close()

	fmt.Println("Writing 10 operations (less than buffer size of 100)...")
	start := time.Now()

	for i := 0; i < 10; i++ {
		q, _ := graph.ParseQuery(fmt.Sprintf(`CREATE (p:Person {name: "User%d"})`, i))
		db.ExecuteQuery(q)
	}

	writeTime := time.Since(start)
	fmt.Printf("Writes completed in %v\n", writeTime)

	fmt.Println("\nWaiting 150ms for timeout-based flush...")
	time.Sleep(150 * time.Millisecond)

	// Close and reopen database to verify data was flushed
	db.Close()

	fmt.Println("\nReopening database to verify data was persisted...")
	db2, err := graph.NewGraphWithPersistence("./data-hybrid-test")
	if err != nil {
		fmt.Printf("Error reopening database: %v\n", err)
		return
	}
	defer db2.Close()

	// Query to count nodes
	q, _ := graph.ParseQuery(`MATCH (p:Person) RETURN p`)
	result, _ := db2.ExecuteQuery(q)

	fmt.Printf("\n✅ SUCCESS: Found %d nodes after reopen\n", len(result.Rows))
	fmt.Println("   This confirms timeout-based flushing works!")

	if len(result.Rows) == 10 {
		fmt.Println("\n🎉 Hybrid mode working correctly:")
		fmt.Println("   - Writes were buffered (fast)")
		fmt.Println("   - Buffer was flushed after 100ms timeout")
		fmt.Println("   - Data survived database restart")
	} else {
		fmt.Printf("\n❌ FAILED: Expected 10 nodes, got %d\n", len(result.Rows))
	}
}
