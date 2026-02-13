package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/MironCo/gravecdb/graph"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

// Global graph instance
var db graph.GraphDB
var serverConfig *graph.ServerConfig
var embedder graph.Embedder

func main() {
	// Load .env file if present (silently ignore if not found)
	_ = godotenv.Load()

	// Parse config from DSN environment variable or use defaults
	// Examples:
	//   GRAVECDB_DSN=gravecdb:///data                     (persist to ./data, no auth)
	//   GRAVECDB_DSN=gravecdb://admin:secret@:8080/data   (with auth)
	//   GRAVECDB_DSN=gravecdb://admin:secret@:8080/data?embedder=ollama://localhost:11434
	dsn := os.Getenv("GRAVECDB_DSN")
	if dsn == "" {
		dsn = "gravecdb:///data" // Default: persist to ./data, no auth
	}

	var err error
	serverConfig, err = graph.ParseServerDSN(dsn)
	if err != nil {
		panic(fmt.Errorf("invalid GRAVECDB_DSN: %w", err))
	}

	// Initialize the graph database
	db, err = serverConfig.Open()
	if err != nil {
		panic(err)
	}

	// Initialize embedder if configured
	embedder, err = serverConfig.GetEmbedder()
	if err != nil {
		fmt.Printf("Warning: failed to initialize embedder: %v\n", err)
	}

	if serverConfig.RequiresAuth() {
		fmt.Printf("Authentication enabled (user: %s)\n", serverConfig.Username)
	}
	fmt.Printf("Data directory: %s\n", serverConfig.DataDir)
	fmt.Printf("Server address: %s\n", serverConfig.Address())

	// Load some demo data (only if database is empty)
	if len(db.GetNodesByLabel("Person")) == 0 {
		loadDemoData()
	}

	// Create Gin router
	r := gin.Default()

	// Enable CORS for frontend
	allowOrigins := serverConfig.AllowOrigins
	if len(allowOrigins) == 0 {
		allowOrigins = []string{"http://localhost:5173", "http://localhost:8080"}
	}
	r.Use(cors.New(cors.Config{
		AllowOrigins:     allowOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		AllowCredentials: true,
	}))

	// Add auth middleware if configured
	if serverConfig.RequiresAuth() {
		auth := graph.NewAuthMiddlewareFromConfig(serverConfig.Config)
		r.Use(func(c *gin.Context) {
			// Skip auth for static files
			if c.Request.URL.Path == "/" || len(c.Request.URL.Path) > 7 && c.Request.URL.Path[:7] == "/static" {
				c.Next()
				return
			}
			// Require auth for API routes
			if !auth.Authenticate(c.Request) {
				c.Header("WWW-Authenticate", `Basic realm="gravecdb"`)
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
			c.Next()
		})
	}

	// Serve static files (frontend)
	r.Static("/static", "./web/dist")
	r.StaticFile("/", "./web/index.html")

	// API routes
	api := r.Group("/api")
	{
		// Graph queries
		api.GET("/graph", getCurrentGraph)
		api.GET("/graph/asof", getGraphAsOf)
		api.GET("/timeline", getTimeline)

		// Cypher-like query endpoint
		api.POST("/query", executeQuery)

		// Path-finding endpoints
		api.GET("/path/shortest", findShortestPath)
		api.GET("/path/all", findAllPaths)

		// CRUD operations
		api.POST("/nodes", createNode)
		api.POST("/relationships", createRelationship)
		api.DELETE("/nodes/:id", deleteNode)
		api.DELETE("/relationships/:id", deleteRelationship)
	}

	// Start server
	addr := serverConfig.Address()
	if serverConfig.TLSCert != "" && serverConfig.TLSKey != "" {
		fmt.Printf("Starting HTTPS server on %s\n", addr)
		r.RunTLS(addr, serverConfig.TLSCert, serverConfig.TLSKey)
	} else {
		fmt.Printf("Starting HTTP server on %s\n", addr)
		r.Run(addr)
	}
}

// GraphResponse represents the graph data sent to the frontend
type GraphResponse struct {
	Nodes         []NodeResponse         `json:"nodes"`
	Relationships []RelationshipResponse `json:"relationships"`
}

// NodeResponse represents a node for the frontend
type NodeResponse struct {
	ID         string                 `json:"id"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
	ValidFrom  time.Time              `json:"validFrom"`
	ValidTo    *time.Time             `json:"validTo,omitempty"`
	Embedding  *EmbeddingResponse     `json:"embedding,omitempty"`
}

// EmbeddingResponse represents an embedding for the frontend
type EmbeddingResponse struct {
	Model      string     `json:"model"`
	Dimensions int        `json:"dimensions"`
	ValidFrom  time.Time  `json:"validFrom"`
	ValidTo    *time.Time `json:"validTo,omitempty"`
}

// RelationshipResponse represents a relationship for the frontend
type RelationshipResponse struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	From       string                 `json:"from"`
	To         string                 `json:"to"`
	Properties map[string]interface{} `json:"properties"`
	ValidFrom  time.Time              `json:"validFrom"`
	ValidTo    *time.Time             `json:"validTo,omitempty"`
}

// TimelineEvent represents an event in the timeline
type TimelineEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	Type        string    `json:"type"` // "CREATE_NODE", "CREATE_REL", "DELETE_NODE", "DELETE_REL"
	Description string    `json:"description"`
}

// getCurrentGraph returns the current state of the graph
func getCurrentGraph(c *gin.Context) {
	response := buildGraphResponse(nil)
	c.JSON(http.StatusOK, response)
}

// getGraphAsOf returns the graph state at a specific time
func getGraphAsOf(c *gin.Context) {
	timeStr := c.Query("t")
	if timeStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing time parameter"})
		return
	}

	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid time format"})
		return
	}

	view := db.AsOf(t)

	// Use the temporal view to get nodes and relationships
	c.JSON(http.StatusOK, gin.H{
		"nodes":         buildNodesFromView(view),
		"relationships": buildRelationshipsFromView(view),
	})
}

// getTimeline returns all events in chronological order
func getTimeline(c *gin.Context) {
	events := []TimelineEvent{}

	// Collect all node creation events
	for _, node := range getAllNodes() {
		name := "Node"
		if n, ok := node.Properties["name"]; ok {
			name = n.(string)
		}
		events = append(events, TimelineEvent{
			Timestamp:   node.ValidFrom,
			Type:        "CREATE_NODE",
			Description: "Created " + name,
		})

		if node.ValidTo != nil {
			events = append(events, TimelineEvent{
				Timestamp:   *node.ValidTo,
				Type:        "DELETE_NODE",
				Description: "Deleted " + name,
			})
		}
	}

	// Collect all relationship events
	for _, rel := range getAllRelationships() {
		events = append(events, TimelineEvent{
			Timestamp:   rel.ValidFrom,
			Type:        "CREATE_REL",
			Description: "Created " + rel.Type + " relationship",
		})

		if rel.ValidTo != nil {
			events = append(events, TimelineEvent{
				Timestamp:   *rel.ValidTo,
				Type:        "DELETE_REL",
				Description: "Deleted " + rel.Type + " relationship",
			})
		}
	}

	c.JSON(http.StatusOK, events)
}

// createNode creates a new node
func createNode(c *gin.Context) {
	var req struct {
		Labels     []string               `json:"labels"`
		Properties map[string]interface{} `json:"properties"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	node := db.CreateNode(req.Labels...)
	for key, value := range req.Properties {
		db.SetNodeProperty(node.ID, key, value)
	}

	c.JSON(http.StatusCreated, gin.H{"id": node.ID})
}

// createRelationship creates a new relationship
func createRelationship(c *gin.Context) {
	var req struct {
		Type       string                 `json:"type"`
		From       string                 `json:"from"`
		To         string                 `json:"to"`
		Properties map[string]interface{} `json:"properties"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rel, err := db.CreateRelationship(req.Type, req.From, req.To)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	for key, value := range req.Properties {
		db.SetRelationshipProperty(rel.ID, key, value)
	}

	c.JSON(http.StatusCreated, gin.H{"id": rel.ID})
}

// deleteNode soft-deletes a node
func deleteNode(c *gin.Context) {
	id := c.Param("id")
	if err := db.DeleteNode(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "node deleted"})
}

// deleteRelationship soft-deletes a relationship
func deleteRelationship(c *gin.Context) {
	id := c.Param("id")
	if err := db.DeleteRelationship(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "relationship deleted"})
}

// executeQuery executes a Cypher-like query
func executeQuery(c *gin.Context) {
	var req struct {
		Query string `json:"query"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Parse the query
	query, err := graph.ParseQuery(req.Query)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "parse error: " + err.Error()})
		return
	}

	// Execute the query (with embedder if configured)
	result, err := db.ExecuteQueryWithEmbedder(query, embedder)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "execution error: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// findShortestPath finds the shortest path between two nodes
func findShortestPath(c *gin.Context) {
	fromID := c.Query("from")
	toID := c.Query("to")

	if fromID == "" || toID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from and to parameters required"})
		return
	}

	path := db.ShortestPath(fromID, toID)
	if path == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no path found"})
		return
	}

	// Convert to response format
	nodeResponses := []NodeResponse{}
	for _, node := range path.Nodes {
		nodeResponses = append(nodeResponses, nodeToResponse(node))
	}

	relResponses := []RelationshipResponse{}
	for _, rel := range path.Relationships {
		relResponses = append(relResponses, relToResponse(rel))
	}

	c.JSON(http.StatusOK, gin.H{
		"nodes":         nodeResponses,
		"relationships": relResponses,
		"length":        path.Length,
	})
}

// findAllPaths finds all paths between two nodes
func findAllPaths(c *gin.Context) {
	fromID := c.Query("from")
	toID := c.Query("to")
	maxDepth := 10 // Default max depth

	if fromID == "" || toID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from and to parameters required"})
		return
	}

	paths := db.AllPaths(fromID, toID, maxDepth)
	if len(paths) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "no paths found"})
		return
	}

	// Convert to response format
	pathResponses := []map[string]interface{}{}
	for _, path := range paths {
		nodeResponses := []NodeResponse{}
		for _, node := range path.Nodes {
			nodeResponses = append(nodeResponses, nodeToResponse(node))
		}

		relResponses := []RelationshipResponse{}
		for _, rel := range path.Relationships {
			relResponses = append(relResponses, relToResponse(rel))
		}

		pathResponses = append(pathResponses, map[string]interface{}{
			"nodes":         nodeResponses,
			"relationships": relResponses,
			"length":        path.Length,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"paths": pathResponses,
		"count": len(pathResponses),
	})
}

// Helper functions

func buildGraphResponse(asOf *time.Time) GraphResponse {
	nodes := []NodeResponse{}
	rels := []RelationshipResponse{}

	for _, node := range getAllNodes() {
		if asOf == nil {
			if node.IsCurrentlyValid() {
				nodes = append(nodes, nodeToResponse(node))
			}
		} else {
			if node.IsValidAt(*asOf) {
				nodes = append(nodes, nodeToResponse(node))
			}
		}
	}

	for _, rel := range getAllRelationships() {
		if asOf == nil {
			if rel.IsCurrentlyValid() {
				rels = append(rels, relToResponse(rel))
			}
		} else {
			if rel.IsValidAt(*asOf) {
				rels = append(rels, relToResponse(rel))
			}
		}
	}

	return GraphResponse{Nodes: nodes, Relationships: rels}
}

func buildNodesFromView(view *graph.TemporalView) []NodeResponse {
	nodes := []NodeResponse{}
	for _, node := range view.GetAllNodes() {
		nodes = append(nodes, nodeToResponse(node))
	}
	return nodes
}

func buildRelationshipsFromView(view *graph.TemporalView) []RelationshipResponse {
	rels := []RelationshipResponse{}
	for _, rel := range view.GetAllRelationships() {
		rels = append(rels, relToResponse(rel))
	}
	return rels
}

func nodeToResponse(node *graph.Node) NodeResponse {
	resp := NodeResponse{
		ID:         node.ID,
		Labels:     node.Labels,
		Properties: node.Properties,
		ValidFrom:  node.ValidFrom,
		ValidTo:    node.ValidTo,
	}

	// Include embedding if present
	if emb := db.GetNodeEmbedding(node.ID); emb != nil {
		resp.Embedding = &EmbeddingResponse{
			Model:      emb.Model,
			Dimensions: len(emb.Vector),
			ValidFrom:  emb.ValidFrom,
			ValidTo:    emb.ValidTo,
		}
	}

	return resp
}

func relToResponse(rel *graph.Relationship) RelationshipResponse {
	return RelationshipResponse{
		ID:         rel.ID,
		Type:       rel.Type,
		From:       rel.FromNodeID,
		To:         rel.ToNodeID,
		Properties: rel.Properties,
		ValidFrom:  rel.ValidFrom,
		ValidTo:    rel.ValidTo,
	}
}

// Access graph internals (normally you'd add these as methods on Graph)
func getAllNodes() []*graph.Node {
	// This is a hack - in production you'd add a GetAllNodes method to Graph
	nodes := []*graph.Node{}
	for _, label := range []string{"Person", "Company"} {
		nodes = append(nodes, db.GetNodesByLabel(label)...)
	}
	return nodes
}

func getAllRelationships() []*graph.Relationship {
	// This is a hack - in production you'd add a GetAllRelationships method to Graph
	rels := []*graph.Relationship{}
	for _, node := range getAllNodes() {
		nodeRels := db.GetRelationshipsForNode(node.ID)
		for _, rel := range nodeRels {
			// Avoid duplicates
			found := false
			for _, existing := range rels {
				if existing.ID == rel.ID {
					found = true
					break
				}
			}
			if !found {
				rels = append(rels, rel)
			}
		}
	}
	return rels
}
