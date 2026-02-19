# memGraph Removal Migration

## What Was memGraph?

`memGraph` was an internal struct that held the **entire graph in RAM**. Every read query on `DiskGraph` (MATCH, path-finding, SIMILAR TO, temporal queries) would call `loadIntoMemory()`, which scanned all nodes and relationships from BoltDB and stuffed them into a `memGraph`. The `memGraph` then ran the query against its in-memory maps and returned results.

This meant a graph with 1M nodes used ~1M × (node size) of extra RAM on every single read query, even if the query only touched 3 nodes.

## What Replaced It

### Common MATCH queries
`findMatchesUnlocked` — uses the in-memory **label index** (`labelIndex map[string][]string`) to get candidate node IDs, then fetches each node from the **LRU cache** (falling back to BoltDB on a miss). No full scan, no extra allocation beyond the working set.

### Path queries (shortestPath / allShortestPaths)
`diskShortestPath` (BFS) and `diskAllPaths` / `diskDFSAllPaths` (DFS) — traverse the graph using the **relationship index** (`nodeRelIndex map[string][]string`) and LRU cache. Only the nodes on the traversal frontier are loaded.

### Temporal queries (AT TIME)
`executeTemporalReadQueryUnlocked` — does a targeted BoltDB scan for **all versions** of nodes/relationships, filters to those valid at the requested timestamp, then runs pattern matching against that snapshot. Still a full scan, but unavoidable for point-in-time queries.

### SIMILAR TO queries
`executeSimilarToQueryUnlocked` — loads embeddings from the embedding store, runs vector search, then fetches only the matching nodes from the LRU cache. No full graph load.

### AsOf (temporal snapshot API)
`DiskGraph.AsOf(t)` now builds a `TemporalView` directly — scans BoltDB once, filters by `IsValidAt(t)`, and populates the snapshot struct. No `memGraph` involved.

### ShortestPath / AllPaths public API
These now delegate to `diskShortestPath` / `diskAllPaths` instead of loading the whole graph.

## Files Deleted

| File | What it contained |
|------|-------------------|
| `graph/mem_graph.go` | `memGraph` struct and all its CRUD/embedding methods; `TemporalView` (backed by memGraph) |
| `graph/mem_graph_query.go` | `ExecuteQuery` / `ExecuteQueryWithEmbedder` on memGraph; all `execute*Query` methods; result builders; type aliases |
| `graph/mem_graph_algorithms.go` | `ShortestPath`, `AllPaths`, `PathExists` on memGraph; BFS/DFS helpers |

## Files Added

| File | What it contains |
|------|------------------|
| `graph/types.go` | All shared type definitions that used to be scattered across the mem_graph files: `Node`/`Relationship` aliases (from `core`), `Embedding`/`SearchResult` aliases (from `embedding`), all `cypher.Graph*` type aliases, `ParseQuery`, `QueryResult`, `Match`, `Path`, `Embedder`, `valuesEqual`, `toFloat64ForCompare`, and the new self-contained `TemporalView` |
| `graph/query_builders.go` | Package-level result-building functions: `buildResult`, `buildRowFromMatch`, `applyScalarFunction`, `buildAggregatedRows`, `buildGroupKey`, `computeAggregation`, `getNumericValue`, `applyDistinct`, `applyOrderBy`, `rowToKey`, `getOrderValue`, `getColumnName` |

## Files Significantly Changed

### `graph/disk_graph.go`
- Removed `loadIntoMemory()` and `loadIntoMemoryUnlocked()`
- `ShortestPath` now calls `g.diskShortestPath(fromID, toID)` directly
- `AllPaths` now calls `g.diskAllPaths(fromID, toID, maxDepth)` directly
- `AsOf` now builds `TemporalView` from disk without creating a `memGraph`

### `graph/disk_graph_query.go`
- `executeReadQuery` rewritten — no longer calls `loadIntoMemoryUnlocked()` + `memGraph.ExecuteQueryWithEmbedder()`
- Added disk-native helpers: `findMatchesUnlocked`, `getNodeUnlocked`, `getRelationshipsForNodeUnlocked`, `getCandidateNodesUnlocked`, `executePathQueryUnlocked`, `diskShortestPath`, `reconstructDiskPath`, `diskAllPaths`, `diskDFSAllPaths`, `executeTemporalReadQueryUnlocked`, `findTemporalMatches`, `executeSimilarToQueryUnlocked`

## TemporalView API — No Breaking Changes

The public `TemporalView` methods are identical. Internally it no longer holds a `*memGraph`; it holds its own snapshot maps populated at construction time by `DiskGraph.AsOf`.

```go
// Before (internal): TemporalView{ graph: *memGraph, asOf: time.Time }
// After  (internal): TemporalView{ asOf, nodes, nodesByLabel, relationships, relsByNode }

// Public API unchanged:
tv := g.AsOf(t)
tv.GetNode(id)
tv.GetNodesByLabel(label)
tv.GetAllNodes()
tv.GetRelationshipsForNode(nodeID)
tv.GetRelationship(id)
tv.GetAllRelationships()
```

## Node / Relationship Types — No Breaking Changes

`Node` and `Relationship` were already defined in the `core` package. `mem_graph.go` only held type aliases re-exporting them into the `graph` package. Those aliases moved to `types.go`. All call sites are unaffected.

## RAM Impact

| Operation | Before | After |
|-----------|--------|-------|
| Simple MATCH | Load entire graph into RAM | LRU cache only |
| Path finding | Load entire graph into RAM | Frontier nodes only |
| SIMILAR TO | Load entire graph into RAM | Embeddings + matched nodes only |
| AT TIME query | Load entire graph into RAM | Full scan (unavoidable for temporal) |
| AsOf snapshot | Load entire graph into RAM | Full scan (unavoidable for snapshot) |
