package main

import (
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/miron/go-graph-database/graph"
)

// Global graph instance
var db *graph.Graph

func main() {
	// Initialize the graph database with persistence
	var err error
	db, err = graph.NewGraphWithPersistence("data")
	if err != nil {
		panic(err)
	}

	// Load some demo data (only if database is empty)
	if len(db.GetNodesByLabel("Person")) == 0 {
		loadDemoData()
	}

	// Create Gin router
	r := gin.Default()

	// Enable CORS for frontend
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173", "http://localhost:8080"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type"},
		AllowCredentials: true,
	}))

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

		// CRUD operations
		api.POST("/nodes", createNode)
		api.POST("/relationships", createRelationship)
		api.DELETE("/nodes/:id", deleteNode)
		api.DELETE("/relationships/:id", deleteRelationship)
	}

	// Start server
	r.Run(":8080")
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
	return NodeResponse{
		ID:         node.ID,
		Labels:     node.Labels,
		Properties: node.Properties,
		ValidFrom:  node.ValidFrom,
		ValidTo:    node.ValidTo,
	}
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

// loadDemoData loads the same demo data from temporal_demo.go
func loadDemoData() {
	time.Sleep(100 * time.Millisecond)

	alice := db.CreateNode("Person")
	db.SetNodeProperty(alice.ID, "name", "Alice")
	db.SetNodeProperty(alice.ID, "role", "Engineer")

	time.Sleep(100 * time.Millisecond)

	bob := db.CreateNode("Person")
	db.SetNodeProperty(bob.ID, "name", "Bob")
	db.SetNodeProperty(bob.ID, "role", "Designer")

	time.Sleep(100 * time.Millisecond)

	techCorp := db.CreateNode("Company")
	db.SetNodeProperty(techCorp.ID, "name", "TechCorp")

	time.Sleep(100 * time.Millisecond)

	aliceJob1, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(aliceJob1.ID, "title", "Junior Engineer")

	time.Sleep(100 * time.Millisecond)

	bobJob, _ := db.CreateRelationship("WORKS_AT", bob.ID, techCorp.ID)
	db.SetRelationshipProperty(bobJob.ID, "title", "Senior Designer")

	time.Sleep(100 * time.Millisecond)

	friendship, _ := db.CreateRelationship("FRIENDS_WITH", alice.ID, bob.ID)
	db.SetRelationshipProperty(friendship.ID, "since", 2020)

	time.Sleep(500 * time.Millisecond)

	// Alice gets promoted
	db.DeleteRelationship(aliceJob1.ID)
	time.Sleep(100 * time.Millisecond)

	aliceJob2, _ := db.CreateRelationship("WORKS_AT", alice.ID, techCorp.ID)
	db.SetRelationshipProperty(aliceJob2.ID, "title", "Senior Engineer")

	time.Sleep(500 * time.Millisecond)

	// Bob changes jobs
	db.DeleteRelationship(bobJob.ID)
	time.Sleep(100 * time.Millisecond)

	startup := db.CreateNode("Company")
	db.SetNodeProperty(startup.ID, "name", "CoolStartup")

	time.Sleep(100 * time.Millisecond)

	bobJob2, _ := db.CreateRelationship("WORKS_AT", bob.ID, startup.ID)
	db.SetRelationshipProperty(bobJob2.ID, "title", "Design Lead")

	time.Sleep(500 * time.Millisecond)

	// Friendship ends
	db.DeleteRelationship(friendship.ID)
}
