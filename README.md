# Temporal Graph Database

A Neo4j-inspired graph database with temporal capabilities built in Go, featuring time-travel queries and real-time visualization.

## Features

- **Labeled Property Graph** - Nodes with labels and properties, relationships with types and properties
- **Temporal Queries** - Query the graph at any point in time with `AsOf(timestamp)`
- **Soft Deletes** - All data preserved for historical queries (ValidFrom/ValidTo timestamps)
- **Disk Persistence** - Write-Ahead Log (WAL) + Snapshots for durability
- **Interactive Visualization** - Real-time graph visualization with time-travel slider

## Quick Start

### Development Mode (Recommended)

Run the backend and frontend separately for hot-reload:

```bash
# Terminal 1: Run the Go backend
make server

# Terminal 2: Run the Vite dev server
make web-dev
```

Open your browser to `http://localhost:5173`

### Production Mode

Build and run the production binary:

```bash
# Build everything
make build
make web-build

# Run the server (serves both API and static frontend)
make run
```

Open your browser to `http://localhost:8080`

### CLI Demos

```bash
# Basic graph operations
make demo-basic

# Temporal query demo
make demo-temporal

# Persistence demo
make demo-persistence
```

## API Endpoints

```
GET  /api/graph              # Current graph state
GET  /api/graph/asof?t=TIME  # Graph at specific timestamp
GET  /api/timeline           # All events in chronological order
POST /api/nodes              # Create node
POST /api/relationships      # Create relationship
DELETE /api/nodes/:id        # Soft delete node
DELETE /api/relationships/:id # Soft delete relationship
```

## Example Usage

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
│   └── persistence.go    # WAL and snapshots
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
├── data/                # Database files (WAL + snapshots)
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

- **Write-Ahead Log** - Every operation logged before being applied
- **Snapshots** - Periodic full state saves
- **Recovery** - Load snapshot + replay WAL entries

## Technology Stack

- **Backend**: Go + Gin
- **Frontend**: Vue 3 + Vite + Cytoscape.js
- **Persistence**: JSON (WAL + Snapshots)

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
