# GravecDB

A temporal graph database written in Go with Neo4j Bolt protocol compatibility, built-in vector embeddings, ACID transactions, and real-time visualization.

![GravecDB Preview](docs/gifs/preview.gif)

[![CI](https://github.com/MironCo/gravecdb/actions/workflows/ci.yml/badge.svg)](https://github.com/MironCo/gravecdb/actions/workflows/ci.yml)

---

## Features

- **Bolt Protocol** — Drop-in compatible with Neo4j drivers (Python, Go, JS, Java, etc.)
- **Cypher Query Language** — Full parser with pattern matching, WHERE, RETURN, ORDER BY, SKIP, LIMIT
- **Temporal Storage** — Every node and relationship has `ValidFrom`/`ValidTo` timestamps; soft deletes preserve full history
- **Time-Travel Queries** — Query the graph as it existed at any past timestamp
- **ACID Transactions** — Explicit `BEGIN`/`COMMIT`/`ROLLBACK` over the Bolt protocol
- **Vector Embeddings** — Versioned semantic search via Ollama (local) or OpenAI (cloud)
- **Disk Persistence** — BoltDB backend with B+ tree storage and LRU caching
- **Graph Algorithms** — PageRank, Louvain community detection via `CALL` procedures with `THROUGH TIME` support
- **Path Finding** — Shortest path, all paths, temporal Dijkstra (`earliestPath`)
- **Interactive Visualization** — Vue 3 frontend with time-travel slider and graph inspector
- **CI/CD** — GitHub Actions pipeline runs Go unit tests + Bolt integration tests on every push

---

## Quick Start

### Docker (Easiest)

Includes Ollama for local embeddings:

```bash
make compose-up        # Start database + Ollama
# Open http://localhost:8080
make compose-down      # Stop everything
```

### Local

```bash
make run               # Build and start the server
# Open http://localhost:8080
```

### With Ollama Embeddings

```bash
ollama pull nomic-embed-text

export GRAVECDB_DSN="gravecdb://0.0.0.0:8080/data?embedder=ollama://localhost:11434/nomic-embed-text"
make run
```

### Development (Hot Reload)

```bash
make server            # Terminal 1: Go backend on :8080
make web-dev           # Terminal 2: Vite dev server on :5173
```

---

## Connecting via Bolt

GravecDB speaks the Neo4j Bolt protocol on port **7687**. Use any Neo4j-compatible driver:

**Python**
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (p:Person) RETURN p.name LIMIT 5")
    for record in result:
        print(record["p.name"])
```

**Go**
```go
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

driver, _ := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.NoAuth())
session := driver.NewSession(ctx, neo4j.SessionConfig{})
result, _ := session.Run(ctx, "MATCH (p:Person) RETURN p.name", nil)
```

**Explicit Transactions**
```python
with driver.session() as session:
    with session.begin_transaction() as tx:
        tx.run("CREATE (p:Person {name: 'Alice'})")
        tx.run("CREATE (p:Person {name: 'Bob'})")
        tx.commit()   # or tx.rollback()
```

---

## Cypher Query Language

GravecDB implements a Cypher-compatible query language. Queries can be run via Bolt or the HTTP API.

### Node & Relationship Patterns

```cypher
-- Create a node
CREATE (p:Person {name: 'Alice', age: 28})

-- Create a relationship
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020}]->(b)

-- Pattern match
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name

-- Multi-pattern (cartesian product)
MATCH (a:Person), (b:Company)
RETURN a.name, b.name
```

### WHERE Clause

```cypher
-- Comparison operators
MATCH (p:Person) WHERE p.age > 25 AND p.age <= 40 RETURN p.name

-- String predicates
MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name
MATCH (p:Person) WHERE p.email ENDS WITH '@example.com' RETURN p.name
MATCH (p:Person) WHERE p.bio CONTAINS 'engineer' RETURN p.name

-- Boolean logic
MATCH (p:Person) WHERE p.role = 'admin' OR p.role = 'moderator' RETURN p
MATCH (p:Person) WHERE NOT p.active = false RETURN p

-- IN operator
MATCH (p:Person) WHERE p.role IN ['admin', 'moderator'] RETURN p.name

-- IS NULL / IS NOT NULL
MATCH (p:Person) WHERE p.email IS NULL RETURN p.name
MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name

-- Functions in WHERE
MATCH (p:Person) WHERE toUpper(p.name) = 'ALICE' RETURN p
```

### RETURN Clause

```cypher
-- Property access
MATCH (p:Person) RETURN p.name, p.age

-- Aliases
MATCH (p:Person) RETURN p.name AS name, p.age AS age

-- String concatenation
MATCH (p:Person) RETURN p.firstName + ' ' + p.lastName AS fullName

-- Aggregation
MATCH (p:Person) RETURN COUNT(p) AS total
MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, COUNT(p) AS headcount

-- DISTINCT, ORDER BY, SKIP, LIMIT
MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city SKIP 0 LIMIT 10

-- CASE WHEN
MATCH (p:Person)
RETURN p.name,
       CASE WHEN p.age < 18 THEN 'minor'
            WHEN p.age < 65 THEN 'adult'
            ELSE 'senior' END AS group
```

### String & Math Functions

| Function | Example | Description |
|---|---|---|
| `toUpper(s)` | `toUpper(p.name)` | Uppercase |
| `toLower(s)` | `toLower(p.name)` | Lowercase |
| `trim(s)` | `trim(p.name)` | Strip whitespace |
| `ltrim(s)` | `ltrim(p.name)` | Strip left whitespace |
| `rtrim(s)` | `rtrim(p.name)` | Strip right whitespace |
| `reverse(s)` | `reverse(p.name)` | Reverse string |
| `size(s)` | `size(p.name)` | String or list length |
| `toString(x)` | `toString(p.age)` | Convert to string |
| `toInteger(s)` | `toInteger(p.score)` | Convert to integer |
| `toFloat(s)` | `toFloat(p.score)` | Convert to float |
| `toBoolean(s)` | `toBoolean(p.active)` | Convert to boolean |
| `abs(n)` | `abs(p.balance)` | Absolute value |
| `ceil(n)` | `ceil(p.score)` | Round up |
| `floor(n)` | `floor(p.score)` | Round down |
| `round(n)` | `round(p.score)` | Round to nearest |
| `sqrt(n)` | `sqrt(p.area)` | Square root |
| `sign(n)` | `sign(p.delta)` | -1, 0, or 1 |
| `log(n)` | `log(p.value)` | Natural logarithm |
| `log10(n)` | `log10(p.value)` | Base-10 logarithm |
| `exp(n)` | `exp(p.value)` | e^n |

### Other Clauses

```cypher
-- UNWIND a list into rows
UNWIND [1, 2, 3] AS x RETURN x * 2

-- UNWIND with WHERE
UNWIND ['Alice', 'Bob', 'Charlie'] AS name WHERE name STARTS WITH 'A' RETURN name

-- MERGE (create if not exists)
MERGE (p:Person {email: 'alice@example.com'})

-- SET properties
MATCH (p:Person {name: 'Alice'}) SET p.age = 29

-- DELETE (soft delete — preserves history)
MATCH (p:Person {name: 'OldNode'}) DELETE p

-- OPTIONAL MATCH (null row on no match)
OPTIONAL MATCH (p:Person {name: 'Ghost'}) RETURN p.name

-- WITH (pipeline / chained MATCH)
MATCH (a:Person)-[:KNOWS]->(b:Person)
WITH b
MATCH (b)-[:WORKS_AT]->(c:Company)
RETURN b.name, c.name

-- REMOVE properties or labels
MATCH (p:Person {name: 'Alice'}) REMOVE p.age
MATCH (p:Person {name: 'Alice'}) REMOVE p:Temporary

-- FOREACH (iterate and mutate)
MATCH (p:Person)
FOREACH (tag IN ['active', 'verified'] | SET p.status = tag)

-- UNION (combine queries, deduplicated)
MATCH (p:Person) RETURN p.name AS name
UNION
MATCH (c:Company) RETURN c.name AS name

-- UNION ALL (combine queries, keep duplicates)
MATCH (p:Person) RETURN p.name AS name
UNION ALL
MATCH (c:Company) RETURN c.name AS name

-- Variable-length relationships
MATCH (a:Person)-[*2..5]->(b:Person) RETURN a.name, b.name
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b
```

### Temporal Queries

```cypher
-- Query the graph at a specific Unix timestamp
MATCH (p:Person) AT TIME 1609459200 RETURN p

-- How long has each person worked at their company (in days)?
MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
RETURN p.name, c.name, DURATION(r) AS tenure_days
ORDER BY tenure_days DESC
```

### Vector Semantic Search

```cypher
-- First, generate embeddings
MATCH (p:Person) EMBED p.bio RETURN p

-- Semantic search (cosine similarity)
MATCH (p:Person) SIMILAR TO 'backend engineer' RETURN p.name, similarity

-- With similarity threshold
MATCH (p:Person) SIMILAR TO 'data scientist' THRESHOLD 0.8 RETURN p.name

-- Through time (search historical versions)
MATCH (p:Person)
SIMILAR TO 'engineer' THROUGH TIME
RETURN p.name, similarity, valid_from, valid_to

-- Semantic drift analysis (track how embeddings evolve)
MATCH (p:Person)
SIMILAR TO 'engineer' THROUGH TIME
RETURN p.name, similarity, valid_from, valid_to, drift_from_previous, drift_from_first
```

### Path Finding

```cypher
-- Shortest path
MATCH path = shortestPath((a:Person {name:'Alice'})-[*]-(b:Person {name:'Bob'}))
RETURN path

-- All shortest paths
MATCH path = allShortestPaths((a:Person)-[*..5]-(b:Person))
RETURN path

-- Earliest arrival path (temporal Dijkstra)
-- "When could a message first have reached Bob from Alice?"
MATCH path = earliestPath((a:Person {name:'Alice'})-[*]->(b:Person {name:'Bob'}))
RETURN path, arrival_time
```

### Graph Algorithms

```cypher
-- PageRank (configurable damping factor, convergence threshold)
CALL pagerank() YIELD node, score
RETURN node.name, score ORDER BY score DESC

-- PageRank filtered by label
CALL pagerank({label: 'Person'}) YIELD node, score
RETURN node.name, score

-- Louvain community detection
CALL louvain() YIELD node, community
RETURN node.name, community ORDER BY community

-- PageRank through time (track score evolution across topology changes)
CALL pagerank() YIELD node, score THROUGH TIME
RETURN node.name, score, valid_from, valid_to

-- Louvain through time (track community shifts)
CALL louvain() YIELD node, community THROUGH TIME
RETURN node.name, community, valid_from, valid_to
```

---

## HTTP API

The server also exposes a REST API on port **8080**:

```
GET  /api/graph                    Current graph state
GET  /api/graph/asof?t=TIMESTAMP   Graph at a past Unix timestamp
GET  /api/timeline                 All events in chronological order
POST /api/query                    Execute a Cypher query (JSON body: {"query": "..."})
GET  /api/path/shortest?from=X&to=Y  Shortest path between nodes
GET  /api/path/all?from=X&to=Y       All paths between nodes
POST /api/nodes                    Create a node
POST /api/relationships            Create a relationship
PUT  /api/nodes/:id/properties     Update node property
DELETE /api/nodes/:id              Soft delete a node
DELETE /api/relationships/:id      Soft delete a relationship
```

---

## Embedded Go API

Use GravecDB directly as a Go library:

```go
import "github.com/MironCo/gravecdb/graph"

// Open (or create) a database
db, _ := graph.Open("data/mydb.db")
defer db.Close()

// Create nodes
alice := db.CreateNode("Person")
db.SetNodeProperty(alice.ID, "name", "Alice")
db.SetNodeProperty(alice.ID, "age", 28)

bob := db.CreateNode("Person")
db.SetNodeProperty(bob.ID, "name", "Bob")

// Create a relationship
rel, _ := db.CreateRelationship("KNOWS", alice.ID, bob.ID)
db.SetRelationshipProperty(rel.ID, "since", 2020)

// Cypher query
result, _ := db.ExecuteQuery(`
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    WHERE a.name = 'Alice'
    RETURN b.name
`)

// Time travel
view := db.AsOf(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
people := view.GetNodesByLabel("Person")
```

---

## Configuration

### DSN Format

```
gravecdb://[username:password@][host][:port]/[datadir]?[options]
```

```bash
# Default (local BoltDB, no auth)
gravecdb:///data

# With authentication
gravecdb://admin:secret@0.0.0.0:8080/data

# With Ollama embeddings
gravecdb://0.0.0.0:8080/data?embedder=ollama://localhost:11434/nomic-embed-text

# With OpenAI embeddings
gravecdb://0.0.0.0:8080/data?embedder=openai://

# In-memory only (no persistence)
gravecdb://:memory:
```

### Environment Variables

```bash
GRAVECDB_DSN          # Full DSN string
EMBEDDER_URL          # Embedder endpoint
OPENAI_API_KEY        # Required for OpenAI embeddings
```

---

## Testing

```bash
# Go unit tests
go test -v ./graph/...

# Bolt integration tests (requires server on :7687)
make server &          # In another terminal
make test
```

The test suite covers:
- Basic MATCH / CREATE / DELETE / SET / MERGE
- Relationship traversal and direction
- Aggregations: COUNT, SUM, AVG, MIN, MAX, COLLECT
- String functions: toUpper, toLower, trim, reverse, size, toString
- Math functions: abs, round, ceil, floor, sqrt, log, exp
- Type conversions: toInteger, toFloat, toBoolean
- DURATION on nodes and relationships
- Temporal queries (AT TIME)
- Path finding (shortestPath, earliestPath)
- UNWIND with WHERE filtering
- WITH clause (multi-stage pipelines)
- CASE WHEN in RETURN
- OR, NOT, IN, IS NULL / IS NOT NULL in WHERE
- STARTS WITH, ENDS WITH, CONTAINS in WHERE
- String concatenation in RETURN
- Variable-length relationships
- Multi-pattern MATCH (cartesian product)
- OPTIONAL MATCH
- UNION / UNION ALL
- REMOVE (properties and labels)
- FOREACH
- CALL procedures (pagerank, louvain)
- ACID transactions: COMMIT and ROLLBACK over Bolt
- Semantic search (SIMILAR TO) — skipped if no embedder
- Semantic drift analysis (THROUGH TIME)

---

## Project Structure

```
├── graph/                    # Core database engine
│   ├── disk_graph.go         # DiskGraph — public API, LRU caches, indexes
│   ├── disk_graph_nodes.go   # Node CRUD (writes to disk)
│   ├── disk_graph_relationships.go # Relationship CRUD
│   ├── disk_graph_query.go   # Query routing, pattern matching, filtering
│   ├── disk_graph_tx.go      # Bolt-protocol transaction support
│   ├── query_builders.go     # RETURN builder, expression evaluator, scalar functions
│   ├── types.go              # Node, Relationship, Query types
│   └── *_test.go             # Go unit tests
│
├── cypher/                   # Cypher parser
│   ├── lexer.go              # Tokenizer
│   ├── parser.go             # Pratt parser → AST
│   ├── ast.go                # AST node types
│   ├── integration.go        # AST → GraphQuery converter
│   └── token.go              # Token definitions
│
├── bolt/                     # Neo4j Bolt protocol server
│   ├── server.go             # TCP listener, session management
│   ├── messages/             # Bolt message types
│   └── packstream/           # PackStream binary encoding/decoding
│
├── server/                   # HTTP server (Gin)
│   └── main.go               # REST API + serves web-ui
│
├── web-ui/                   # Frontend (Vue 3 + Vite + Cytoscape.js)
│   └── src/App.vue           # Graph visualization + time-travel slider
│
├── client/                   # Go client library for remote connections
├── embedding/                # Embedding store and cosine similarity
├── storage/                  # BoltDB storage layer
│
├── test_bolt/                # Bolt integration tests (Python)
│   ├── test_connection.py    # 30 Bolt protocol tests
│   ├── test_transactions.py  # 4 ACID transaction tests
│   └── requirements.txt      # neo4j>=5.0.0
│
├── .github/workflows/ci.yml  # GitHub Actions CI
├── Makefile                  # All build/run/test commands
├── Dockerfile                # Container build
└── docker-compose.yml        # Database + Ollama stack
```

---

## How It Works

### Temporal Storage

Every node and relationship carries two timestamps:

```
ValidFrom  time.Time   — when it was created
ValidTo   *time.Time   — when it was deleted (nil = currently active)
```

Deletes are soft — `ValidTo` is set to now, data is never removed. This enables time-travel queries at any past point in time.

### Storage Engine

GravecDB uses **BoltDB** (bbolt) as its storage backend:
- **B+ tree** structure for O(log n) lookups
- **Single file** database (`gravecdb.db`)
- **ACID transactions** — atomic, consiste nt, isolated, durable
- **MVCC** — multiple readers don't block writers
- **LRU caches** for hot nodes and relationships (tunable size)
- **In-memory label index** for fast label-based lookups

### Query Execution

```
Bolt/HTTP request
      │
      ▼
Cypher Parser (Pratt parser → AST)
      │
      ▼
AST → GraphQuery converter
      │
      ▼
Query Router
  ├── CREATE / SET / DELETE / MERGE  →  Direct BoltDB writes
  ├── MATCH (read-only)              →  Index-based matching + WHERE filter
  ├── MATCH + mutations              →  Read matches then write
  ├── UNWIND                         →  Expand list → filter → RETURN
  ├── PIPELINE (WITH)                →  Multi-stage match chain
  └── Path queries                   →  BFS/DFS/Dijkstra on graph snapshot
```

### Bolt Protocol

GravecDB implements the Neo4j Bolt v4.4 binary protocol, meaning any standard Neo4j driver (Python `neo4j`, Go `neo4j-go-driver`, Node.js `neo4j-driver`, etc.) can connect to it directly without modification.

---

## Makefile Reference

```bash
make build              # Build server binary (current platform)
make build-linux        # Build for Linux amd64
make build-linux-arm    # Build for Linux arm64
make build-all          # Build all platforms
make run                # Build and start server
make server             # Run server without rebuilding (go run)
make test               # Go unit tests + Bolt integration tests
make web-dev            # Frontend dev server (hot reload)
make web-build          # Build frontend for production
make docker             # Build Docker image
make docker-run         # Run Docker container
make compose-up         # Start with Docker Compose (+ Ollama)
make compose-down       # Stop Docker Compose
make clean              # Remove build artifacts and data
```

---

## Visualization

The built-in web UI (Vue 3 + Cytoscape.js) provides:

- **Live graph view** — nodes and relationships rendered as a force-directed graph
- **Time slider** — scrub through history to see how the graph evolved
- **Play / Pause** — animate changes at 0.5×–5× speed
- **Node inspector** — click any node to view all properties in a sidebar
- **Deleted entity display** — removed nodes/relationships shown with red outline and reduced opacity

---

## Technology Stack

| Layer | Technology |
|---|---|
| Language | Go 1.23 |
| HTTP server | Gin |
| Wire protocol | Neo4j Bolt v4.4 |
| Query language | Custom Cypher parser (Pratt) |
| Storage | BoltDB (bbolt) |
| Frontend | Vue 3 + Vite + Cytoscape.js |
| Embeddings | Ollama (local) / OpenAI (cloud) |
| CI | GitHub Actions |
