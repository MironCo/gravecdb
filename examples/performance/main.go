package main

import (
	"fmt"
	"os"
	"time"

	"github.com/MironCo/gravecdb/graph"
)

func main() {
	fmt.Println("=== Graph Database Performance Modes ===\n")

	// Clean up test directories
	defer func() {
		os.RemoveAll("./data-maxdur")
		os.RemoveAll("./data-hybrid")
		os.RemoveAll("./data-maxperf")
	}()

	fmt.Println("This demo shows three persistence modes with different performance/durability tradeoffs:\n")

	// Mode 1: Maximum Durability
	fmt.Println("1. MAX DURABILITY MODE")
	fmt.Println("   - Syncs every write to disk immediately")
	fmt.Println("   - Safest: survives crashes with zero data loss")
	fmt.Println("   - Slowest: ~12ms per write")
	fmt.Println("   - Use for: Financial systems, critical data\n")

	db1, _ := graph.NewGraphWithMaxDurability("./data-maxdur")
	testWrites("Max Durability", db1, 50)
	db1.Close()

	// Mode 2: Hybrid (Default)
	fmt.Println("\n2. HYBRID MODE (DEFAULT)")
	fmt.Println("   - Syncs on buffer full (100 ops) OR timeout (100ms)")
	fmt.Println("   - Best balance: guarantees max 100ms data loss")
	fmt.Println("   - Fast: ~175µs per write (73x faster than max durability)")
	fmt.Println("   - Use for: Most applications\n")

	db2, _ := graph.NewGraphWithPersistence("./data-hybrid")
	testWrites("Hybrid", db2, 50)
	db2.Close()

	// Mode 3: Maximum Performance
	fmt.Println("\n3. MAX PERFORMANCE MODE")
	fmt.Println("   - Large buffer, syncs periodically (every second)")
	fmt.Println("   - Fastest: ~16µs per write (785x faster than max durability)")
	fmt.Println("   - May lose up to 1 second of writes on crash")
	fmt.Println("   - Use for: Analytics, caching, non-critical data\n")

	db3, _ := graph.NewGraphWithMaxPerformance("./data-maxperf")
	testWrites("Max Performance", db3, 50)
	db3.Close()

	fmt.Println("\n=== Performance Summary ===")
	fmt.Println("Mode                Write Time    Durability Risk")
	fmt.Println("--------------------+--------------+------------------")
	fmt.Println("Max Durability      | ~12ms        | None (safest)")
	fmt.Println("Hybrid (default)    | ~175µs       | < 100ms")
	fmt.Println("Max Performance     | ~16µs        | < 1 second")
	fmt.Println("")
	fmt.Println("💡 Tip: Use NewGraphWithPersistence() for most use cases")
}

func testWrites(mode string, db *graph.Graph, count int) {
	start := time.Now()

	for i := 0; i < count; i++ {
		q, _ := graph.ParseQuery(fmt.Sprintf(`CREATE (p:Person {name: "User%d", age: %d})`, i, 20+i))
		db.ExecuteQuery(q)
	}

	elapsed := time.Since(start)
	avgTime := elapsed / time.Duration(count)

	fmt.Printf("   Created %d nodes in %v (avg: %v per write)\n", count, elapsed, avgTime)
}
