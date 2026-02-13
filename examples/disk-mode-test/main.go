package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/MironCo/gravecdb/graph"
)

func main() {
	// Clean up test data
	os.RemoveAll("./test-memory")
	os.RemoveAll("./test-disk")

	fmt.Println("=== Disk Mode vs Memory Mode Performance Test ===\n")

	// Test 1: Memory Mode (load all into RAM, persist to bolt)
	fmt.Println("--- Memory Mode (load all into RAM) ---")
	startMem := time.Now()
	memGraph, err := graph.NewGraphWithBolt("./test-memory")
	if err != nil {
		log.Fatal(err)
	}
	defer memGraph.Close()

	// Create 1000 nodes
	fmt.Println("Creating 1000 nodes...")
	nodeIDs := make([]string, 1000)
	createStart := time.Now()
	for i := 0; i < 1000; i++ {
		node := memGraph.CreateNode("Person")
		memGraph.SetNodeProperty(node.ID, "name", fmt.Sprintf("Person_%d", i))
		memGraph.SetNodeProperty(node.ID, "age", 20+i%50)
		nodeIDs[i] = node.ID
	}
	createDuration := time.Since(createStart)
	fmt.Printf("✓ Created 1000 nodes in %v (%.2f nodes/sec)\n", createDuration, 1000/createDuration.Seconds())

	// Query by label
	queryStart := time.Now()
	people := memGraph.GetNodesByLabel("Person")
	queryDuration := time.Since(queryStart)
	fmt.Printf("✓ Queried %d nodes in %v\n", len(people), queryDuration)

	// Random access
	randomStart := time.Now()
	for i := 0; i < 100; i++ {
		memGraph.GetNode(nodeIDs[i*10])
	}
	randomDuration := time.Since(randomStart)
	fmt.Printf("✓ Random access (100 reads) in %v (%.0f µs/read)\n", randomDuration, float64(randomDuration.Microseconds())/100.0)

	totalMem := time.Since(startMem)
	fmt.Printf("\nTotal memory mode time: %v\n\n", totalMem)

	// Test 2: Disk Mode (hybrid: indexes in RAM, data on disk with LRU cache)
	fmt.Println("--- Disk Mode (hybrid: indexed + LRU cache) ---")
	startDisk := time.Now()
	diskGraph, err := graph.NewDiskGraph("./test-disk", 1000) // Cache 1000 nodes
	if err != nil {
		log.Fatal(err)
	}
	defer diskGraph.Close()

	// Create 1000 nodes
	fmt.Println("Creating 1000 nodes...")
	diskNodeIDs := make([]string, 1000)
	createStartDisk := time.Now()
	for i := 0; i < 1000; i++ {
		node := diskGraph.CreateNode("Person")
		diskGraph.SetNodeProperty(node.ID, "name", fmt.Sprintf("Person_%d", i))
		diskGraph.SetNodeProperty(node.ID, "age", 20+i%50)
		diskNodeIDs[i] = node.ID
	}
	createDurationDisk := time.Since(createStartDisk)
	fmt.Printf("✓ Created 1000 nodes in %v (%.2f nodes/sec)\n", createDurationDisk, 1000/createDurationDisk.Seconds())

	// Query by label
	queryStartDisk := time.Now()
	peopleDisk := diskGraph.GetNodesByLabel("Person")
	queryDurationDisk := time.Since(queryStartDisk)
	fmt.Printf("✓ Queried %d nodes in %v\n", len(peopleDisk), queryDurationDisk)

	// Random access
	randomStartDisk := time.Now()
	for i := 0; i < 100; i++ {
		diskGraph.GetNode(diskNodeIDs[i*10])
	}
	randomDurationDisk := time.Since(randomStartDisk)
	fmt.Printf("✓ Random access (100 reads) in %v (%.0f µs/read)\n", randomDurationDisk, float64(randomDurationDisk.Microseconds())/100.0)

	totalDisk := time.Since(startDisk)
	fmt.Printf("\nTotal disk mode time: %v\n\n", totalDisk)

	// Comparison
	fmt.Println("=== Comparison ===")
	fmt.Printf("Memory mode: %v\n", totalMem)
	fmt.Printf("Disk mode:   %v\n", totalDisk)
	fmt.Printf("Disk is %.2fx slower\n", totalDisk.Seconds()/totalMem.Seconds())

	fmt.Println("\n=== Query Performance ===")
	fmt.Printf("Label query (memory): %v\n", queryDuration)
	fmt.Printf("Label query (disk):   %v (%.2fx slower)\n", queryDurationDisk, queryDurationDisk.Seconds()/queryDuration.Seconds())

	fmt.Printf("\nRandom reads (memory): %.0f µs/read\n", float64(randomDuration.Microseconds())/100.0)
	fmt.Printf("Random reads (disk):   %.0f µs/read (%.2fx slower)\n", float64(randomDurationDisk.Microseconds())/100.0, float64(randomDurationDisk.Microseconds())/float64(randomDuration.Microseconds()))

	// Clean up
	diskGraph.Close()
	memGraph.Close()
	os.RemoveAll("./test-memory")
	os.RemoveAll("./test-disk")
	fmt.Println("\n✓ Cleaned up test data")
}
