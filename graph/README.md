# Graph Package Architecture

## Overview

The graph package has two main components:

- **DiskGraph** - The public API. Persists data to disk via BoltDB, uses LRU caches and in-memory indexes for performance.
- **memGraph** - Internal only. An in-memory graph used by DiskGraph for complex operations.

## DiskGraph (Public API)

DiskGraph is the only type external code should use. It handles:

- **Storage**: All data persisted to BoltDB
- **Caching**: LRU caches for recently accessed nodes/relationships
- **Indexing**: In-memory indexes for label lookups and relationship traversal
- **CRUD**: Direct create/update/delete operations that write to disk

## memGraph (Internal)

memGraph is unexported and exists solely as a computation engine. DiskGraph creates temporary memGraph instances for operations that benefit from having all data in memory.

## When DiskGraph Uses memGraph

### 1. Path Finding (`ShortestPath`, `AllPaths`)

```go
func (g *DiskGraph) ShortestPath(fromID, toID string) *Path {
    memGraph := g.loadIntoMemory()  // Load current state
    return memGraph.ShortestPath(fromID, toID)
}
```

Path algorithms need to traverse the graph structure repeatedly. Loading into memory once is faster than hitting disk for each hop.

### 2. Read Queries (MATCH without mutations)

```go
func (g *DiskGraph) executeReadQuery(query *Query, embedder Embedder) (*QueryResult, error) {
    memGraph := g.loadIntoMemoryUnlocked()
    // Also load embeddings for semantic search
    return memGraph.ExecuteQueryWithEmbedder(query, embedder)
}
```

Complex read queries with pattern matching, WHERE clauses, and aggregations run against an in-memory snapshot.

### 3. Time Travel (`AsOf`)

```go
func (g *DiskGraph) AsOf(t time.Time) *TemporalView {
    snapshot := newMemGraph()
    // Load and filter nodes/relationships valid at time t
    return snapshot.asOf(t)
}
```

Historical queries load a filtered snapshot into memGraph.

## When DiskGraph Does NOT Use memGraph

### Write Operations

CREATE, SET, DELETE queries go directly to disk:

```go
func (g *DiskGraph) executeCreateQuery(query *Query) (*QueryResult, error) {
    g.mu.Lock()
    defer g.mu.Unlock()

    // Direct writes to BoltDB + cache/index updates
    node := g.createNodeUnlocked(nodeSpec.Labels...)
    // ...
}
```

### Simple Lookups

Single node/relationship fetches use the LRU cache:

```go
func (g *DiskGraph) GetNode(id string) (*Node, error) {
    // Check cache first, then disk
    if node, ok := g.nodeCache.Get(id); ok {
        return node, nil
    }
    return g.boltStore.GetNode(id)
}
```

### Index-Based Pattern Matching

MATCH queries with mutations use `findMatchesUnlocked()` which leverages the label index directly instead of loading everything into memory.

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                      DiskGraph (Public)                      │
├─────────────────────────────────────────────────────────────┤
│  Writes ──────────────────────────────────────► BoltDB      │
│                                                    ▲        │
│  Simple Reads ─────► LRU Cache ───────────────────┘        │
│                          │                                  │
│  Complex Reads ──► loadIntoMemory() ──► memGraph           │
│  Path Finding                              │                │
│  Time Travel                               ▼                │
│                                      Query Execution        │
│                                      Path Algorithms        │
└─────────────────────────────────────────────────────────────┘
```

## File Structure

```
graph/
├── disk_graph.go          # DiskGraph struct, loadIntoMemory, path delegation
├── disk_graph_nodes.go    # Node CRUD (writes to disk)
├── disk_graph_rels.go     # Relationship CRUD (writes to disk)
├── disk_graph_query.go    # Query routing, write queries, index-based matching
├── disk_graph_tx.go       # Transaction support
│
├── mem_graph.go           # memGraph struct, CRUD, embeddings, TemporalView
├── mem_graph_query.go     # Query execution engine (runs on memGraph)
├── mem_graph_algorithms.go # Path finding algorithms (BFS, DFS)
│
├── query.go               # Query parsing
├── config.go              # Configuration
└── interface.go           # GraphTransaction interface
```

## Why This Design?

1. **Memory efficiency**: Only hot data in RAM (via LRU cache), rest on disk
2. **Fast writes**: Direct to disk, no full-graph loading
3. **Fast complex reads**: Load once, traverse in memory
4. **Clean separation**: memGraph is a pure computation engine with no I/O
