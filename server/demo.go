package main

import (
	"fmt"
	"log"
	"time"

	"github.com/MironCo/gravecdb/client"
)

func loadDemoData() {
	// Run in background to avoid blocking server startup
	go func() {
		time.Sleep(100 * time.Millisecond) // Wait for server to start

		// Connect to ourselves
		conn, err := client.Connect(fmt.Sprintf("http://localhost:%d", serverConfig.Port))
		if err != nil {
			log.Printf("Failed to connect for demo data: %v", err)
			return
		}
		defer conn.Close()

		// Create people with roles
		alice, _ := conn.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Alice", "role": "backend engineer"})
		time.Sleep(50 * time.Millisecond)

		bob, _ := conn.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Bob", "role": "frontend developer"})
		time.Sleep(50 * time.Millisecond)

		carol, _ := conn.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Carol", "role": "engineering manager"})
		time.Sleep(50 * time.Millisecond)

		david, _ := conn.CreateNode([]string{"Person"}, map[string]interface{}{"name": "David", "role": "devops engineer"})
		time.Sleep(50 * time.Millisecond)

		eve, _ := conn.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Eve", "role": "product manager"})
		time.Sleep(50 * time.Millisecond)

		frank, _ := conn.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Frank", "role": "data scientist"})
		time.Sleep(50 * time.Millisecond)

		// Create companies
		techCorp, _ := conn.CreateNode([]string{"Company"}, map[string]interface{}{"name": "TechCorp"})
		time.Sleep(50 * time.Millisecond)

		startup, _ := conn.CreateNode([]string{"Company"}, map[string]interface{}{"name": "CoolStartup"})
		time.Sleep(50 * time.Millisecond)

		bigCo, _ := conn.CreateNode([]string{"Company"}, map[string]interface{}{"name": "BigCo"})
		time.Sleep(50 * time.Millisecond)

		// Initial employment at TechCorp
		aliceJob1, _ := conn.CreateRelationship("WORKS_AT", alice, techCorp, map[string]interface{}{"title": "Junior Engineer"})
		time.Sleep(50 * time.Millisecond)

		bobJob1, _ := conn.CreateRelationship("WORKS_AT", bob, techCorp, map[string]interface{}{"title": "Senior Designer"})
		time.Sleep(50 * time.Millisecond)

		carolJob1, _ := conn.CreateRelationship("WORKS_AT", carol, techCorp, map[string]interface{}{"title": "Engineering Manager"})
		time.Sleep(50 * time.Millisecond)

		davidJob1, _ := conn.CreateRelationship("WORKS_AT", david, bigCo, map[string]interface{}{"title": "DevOps Engineer"})
		time.Sleep(50 * time.Millisecond)

		eveJob1, _ := conn.CreateRelationship("WORKS_AT", eve, startup, map[string]interface{}{"title": "Product Manager"})
		time.Sleep(50 * time.Millisecond)

		frankJob1, _ := conn.CreateRelationship("WORKS_AT", frank, bigCo, map[string]interface{}{"title": "Senior Data Scientist"})
		time.Sleep(50 * time.Millisecond)

		// Friendships
		friendship1, _ := conn.CreateRelationship("FRIENDS_WITH", alice, bob, map[string]interface{}{"since": 2020})
		time.Sleep(50 * time.Millisecond)

		friendship2, _ := conn.CreateRelationship("FRIENDS_WITH", bob, david, map[string]interface{}{"since": 2019})
		time.Sleep(50 * time.Millisecond)

		friendship3, _ := conn.CreateRelationship("FRIENDS_WITH", carol, eve, map[string]interface{}{"since": 2021})
		time.Sleep(50 * time.Millisecond)

		// Mentorship
		mentorship1, _ := conn.CreateRelationship("MENTORS", carol, alice, map[string]interface{}{"started": 2021})
		time.Sleep(50 * time.Millisecond)

		mentorship2, _ := conn.CreateRelationship("MENTORS", frank, alice, map[string]interface{}{"started": 2022})
		time.Sleep(200 * time.Millisecond)

		// Alice gets promoted
		conn.DeleteRelationship(aliceJob1)
		time.Sleep(50 * time.Millisecond)

		aliceJob2, _ := conn.CreateRelationship("WORKS_AT", alice, techCorp, map[string]interface{}{"title": "Senior Engineer"})
		time.Sleep(200 * time.Millisecond)

		// Bob moves to startup
		conn.DeleteRelationship(bobJob1)
		time.Sleep(50 * time.Millisecond)

		bobJob2, _ := conn.CreateRelationship("WORKS_AT", bob, startup, map[string]interface{}{"title": "Design Lead"})
		time.Sleep(100 * time.Millisecond)

		// Friendship ends due to job change
		conn.DeleteRelationship(friendship1)
		time.Sleep(200 * time.Millisecond)

		// David joins startup too
		conn.DeleteRelationship(davidJob1)
		time.Sleep(50 * time.Millisecond)

		davidJob2, _ := conn.CreateRelationship("WORKS_AT", david, startup, map[string]interface{}{"title": "Lead DevOps"})
		time.Sleep(100 * time.Millisecond)

		// New collaboration relationships
		collab1, _ := conn.CreateRelationship("COLLABORATES", bob, david, map[string]interface{}{"project": "Platform"})
		time.Sleep(50 * time.Millisecond)

		collab2, _ := conn.CreateRelationship("COLLABORATES", eve, bob, map[string]interface{}{"project": "Product"})
		time.Sleep(50 * time.Millisecond)

		// Generate embeddings if embedder is available
		if embedder != nil {
			log.Println("Generating embeddings for Person nodes...")
			_, err := conn.Query(`MATCH (p:Person) EMBED p.role RETURN p`)
			if err != nil {
				log.Printf("Failed to generate embeddings: %v", err)
			} else {
				log.Println("Embeddings generated successfully")
			}
		}

		// Avoid unused variable warnings
		_ = aliceJob2
		_ = bobJob2
		_ = carolJob1
		_ = davidJob2
		_ = eveJob1
		_ = frankJob1
		_ = friendship2
		_ = friendship3
		_ = mentorship1
		_ = mentorship2
		_ = collab1
		_ = collab2

		log.Println("Demo data loaded successfully")
	}()
}
