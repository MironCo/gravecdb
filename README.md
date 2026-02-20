# GravecDB

A temporal graph database with built-in vector embeddings, featuring time-travel queries and real-time visualization.

![GravecDB Preview](docs/gifs/preview.gif)

## Quick Start

### Docker (Easiest)

Includes Ollama for embeddings:

```bash
# Start everything (database + Ollama)
make compose-up

# Access at http://localhost:8080
```

To stop:
```bash
make compose-down
```

### Local with Ollama

If you already have Ollama installed:

```bash
# Pull the embedding model
ollama pull nomic-embed-text

# Set up environment
export GRAVECDB_DSN="gravecdb://0.0.0.0:8080/data?embedder=ollama://localhost:11434/nomic-embed-text"

# Run the server
make run
```

Open your browser to `http://localhost:8080`

### Development Mode

Run the backend and frontend separately for hot-reload:

```bash
# Terminal 1: Run the Go backend
make server

# Terminal 2: Run the Vite dev server
make web-dev
```

Open your browser to `http://localhost:5173`

## Features

- **Temporal Queries** - Query the graph at any point in time with `AsOf(timestamp)`
- **Vector Embeddings** - Versioned semantic search with Ollama or OpenAI
- **Soft Deletes** - All data preserved for historical queries (ValidFrom/ValidTo timestamps)
- **Cypher-like Query Language** - Pattern matching with WHERE clauses and semantic search
- **Disk Persistence** - BoltDB backend with ACID transactions (WAL mode also available)
- **Interactive Visualization** - Real-time graph visualization with time-travel slider
- **Path-Finding Algorithms** - Shortest path, all paths, and path existence checks

### CLI Demos

```bash
# Basic graph operations
make demo-basic

# Temporal query demo
make demo-temporal

# Persistence demo
make demo-persistence

# Path-finding algorithms demo
make demo-pathfinding

# Cypher-like query language demo
make demo-query

# Temporal path-finding demo (3D path-finding through time!)
make demo-temporal-paths

# Vector embeddings & semantic search (requires Ollama)
make demo-embeddings

# Client library demo (requires server running)
make demo-client

# Performance comparison demo
make demo-performance
```

## API Endpoints

```
GET  /api/graph                    # Current graph state
GET  /api/graph/asof?t=TIME        # Graph at specific timestamp
GET  /api/timeline                 # All events in chronological order
POST /api/query                    # Execute Cypher-like query
GET  /api/path/shortest?from=X&to=Y # Find shortest path between nodes
GET  /api/path/all?from=X&to=Y     # Find all paths between nodes
POST /api/nodes                    # Create node
POST /api/relationships            # Create relationship
DELETE /api/nodes/:id              # Soft delete node
DELETE /api/relationships/:id      # Soft delete relationship
```

## Configuration

### DSN Format

```
gravecdb://[username:password@][host][:port]/[datadir]?[options]
```

Examples:
```bash
# Local persistence (uses BoltDB by default)
gravecdb:///data

# With authentication
gravecdb://admin:secret@0.0.0.0:8080/data

# With Ollama embeddings
gravecdb://0.0.0.0:8080/data?embedder=ollama://localhost:11434/nomic-embed-text

# With OpenAI embeddings
gravecdb://0.0.0.0:8080/data?embedder=openai://

# Use WAL backend (legacy mode)
gravecdb:///data?backend=wal

# In-memory only (no persistence)
gravecdb://:memory:
```

Environment variables:
```bash
GRAVECDB_DSN              # Connection string
EMBEDDER_URL              # Embedder configuration
OPENAI_API_KEY            # For OpenAI embeddings
```

## Example Usage

### Embedded Database

```go
// Create a graph database
db := graph.NewGraph()

// Create nodes
alice := db.CreateNode("Person")
db.SetNodeProperty(alice.ID, "name", "Alice")

bob := db.CreateNode("Person")
db.SetNodeProperty(bob.ID, "name", "Bob")

// Create relationship
friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
db.SetRelationshipProperty(friendship.ID, "since", 2020)

// Query current state
people := db.GetNodesByLabel("Person")

// Time travel - see graph as it was on Jan 1, 2023
historicalView := db.AsOf(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
historicalPeople := historicalView.GetNodesByLabel("Person")
```

### Vector Embeddings & Semantic Search

```go
// Create embedder
embedder := graph.NewOllamaEmbedder()

// Create and embed nodes
db.ExecuteQueryWithEmbedder(
  graph.ParseQuery(`MATCH (p:Person) EMBED p.role RETURN p`),
  embedder,
)

// Semantic search
result, _ := db.ExecuteQueryWithEmbedder(
  graph.ParseQuery(`MATCH (p:Person) SIMILAR TO "backend developers" RETURN p.name`),
  embedder,
)
```

### Client Library (Remote Connection)

```go
import "github.com/MironCo/gravecdb/client"

// Connect to a running server
conn, _ := client.Connect("http://localhost:8080")
defer conn.Close()

// Execute Cypher-like queries
result, _ := conn.Query(`
  MATCH (p:Person)-[:WORKS_AT]->(c:Company)
  WHERE p.role = "Engineer"
  RETURN p.name, c.name
`)

// Semantic search query
result, _ := conn.Query(`
  MATCH (p:Person)
  SIMILAR TO "backend developers"
  THRESHOLD 0.8
  RETURN p.name, p.role
`)

// Create nodes and relationships
personID, _ := conn.CreateNode(
  []string{"Person"},
  map[string]interface{}{"name": "Alice", "age": 28},
)

relID, _ := conn.CreateRelationship(
  "WORKS_AT",
  personID,
  companyID,
  map[string]interface{}{"title": "Engineer"},
)
```

### Query Language Examples

```cypher
-- Create nodes
CREATE (p:Person {name: "Alice", age: 28})

-- Pattern matching
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE p.age > 25
RETURN p.name, c.name

-- Temporal queries
MATCH (p:Person) AT TIME 1609459200 RETURN p

-- Generate embeddings
MATCH (p:Person) EMBED p.role RETURN p

-- Semantic search
MATCH (p:Person) SIMILAR TO "backend engineers" LIMIT 10 RETURN p.name

-- With similarity threshold
MATCH (p:Person) SIMILAR TO "data scientists" THRESHOLD 0.8 RETURN p.name

-- Path finding
MATCH path = shortestPath((a:Person)-[*]-(b:Person)) RETURN path

-- Earliest arrival path (beta) — temporal Dijkstra
-- "When could information have first reached Bob from Alice?"
-- Traverses historical relationships; cost = max(arrival_time, rel.ValidFrom)
MATCH path = earliestPath((a:Person {name: 'Alice'})-[*]->(b:Person {name: 'Bob'}))
RETURN path, arrival_time
```

## Visualization Controls

- **Time Slider** - Scrub through graph history
- **Play/Pause** - Animate changes over time
- **Speed Control** - 0.5x to 5x playback speed
- **Click Nodes** - View properties in sidebar
- **Deleted Entities** - Shown with red outline and reduced opacity

## Project Structure

```
├── graph/
│   ├── node.go           # Node implementation
│   ├── relationship.go   # Relationship implementation
│   ├── graph.go          # Core graph database
│   ├── temporal.go       # Temporal query support
│   ├── bbolt_store.go    # BoltDB persistence backend
│   └── persistence.go    # WAL persistence (legacy)
├── server/
│   └── main.go          # HTTP server with API
├── web-ui/              # Vue + Vite frontend
│   ├── src/
│   │   └── App.vue      # Main visualization component
│   └── vite.config.js   # Vite configuration
├── web/
│   └── dist/            # Production build output
├── examples/
│   ├── basic/           # Basic graph operations demo
│   ├── temporal/        # Time-travel demo
│   └── persistence/     # Persistence demo
├── data/                # Database files (gravecdb.db)
└── Makefile             # Build commands
```

## How It Works

### Temporal Storage

Every node and relationship has:
- `ValidFrom` - When it became active
- `ValidTo` - When it was deleted (nil = still active)

Deletes are "soft" - they set `ValidTo` instead of removing data.

### Time-Travel Queries

```go
// Query graph as it existed at specific time
view := graph.AsOf(timestamp)
nodes := view.GetNodesByLabel("Person")
```

The temporal view filters nodes/relationships to only show those valid at the query time.

### Persistence

**BoltDB Backend (Default):**
- **ACID Transactions** - Atomic, consistent, isolated, durable writes
- **B+ Tree Storage** - Fast O(log n) lookups and range scans
- **Single File** - Database stored in `gravecdb.db`
- **MVCC** - Multiple readers don't block writers

**WAL Backend (Legacy):**
- **Write-Ahead Log** - Every operation logged before being applied
- **Snapshots** - Periodic full state saves
- **Recovery** - Load snapshot + replay WAL entries

## Technology Stack

- **Backend**: Go + Gin
- **Frontend**: Vue 3 + Vite + Cytoscape.js
- **Embeddings**: Ollama (local) or OpenAI (cloud)
- **Persistence**: BoltDB (default) or WAL + Snapshots

## Development

```bash
# Install frontend dependencies
cd web-ui && npm install

# Run backend (port 8080)
make server

# Run frontend dev server with hot-reload (port 5173)
make web-dev

# Build production frontend
make web-build

# Clean all build artifacts
make clean
```
