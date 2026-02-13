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

		log.Println("Loading demo data using query language...")

		// Create people
		createResult, err := conn.Query(`CREATE (alice:Person {name: "Alice", role: "backend engineer"})`)
		if err != nil {
			log.Printf("Failed to create Alice: %v", err)
		} else {
			log.Printf("Created Alice, result columns: %v, rows: %d", createResult.Columns, len(createResult.Rows))
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (bob:Person {name: "Bob", role: "frontend developer"})`)
		if err != nil {
			log.Printf("Failed to create Bob: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (carol:Person {name: "Carol", role: "engineering manager"})`)
		if err != nil {
			log.Printf("Failed to create Carol: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (david:Person {name: "David", role: "devops engineer"})`)
		if err != nil {
			log.Printf("Failed to create David: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (eve:Person {name: "Eve", role: "product manager"})`)
		if err != nil {
			log.Printf("Failed to create Eve: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (frank:Person {name: "Frank", role: "data scientist"})`)
		if err != nil {
			log.Printf("Failed to create Frank: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (grace:Person {name: "Grace", role: "UX designer"})`)
		if err != nil {
			log.Printf("Failed to create Grace: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (henry:Person {name: "Henry", role: "QA engineer"})`)
		if err != nil {
			log.Printf("Failed to create Henry: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		// Create companies
		_, err = conn.Query(`CREATE (tc:Company {name: "TechCorp"})`)
		if err != nil {
			log.Printf("Failed to create TechCorp: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (startup:Company {name: "CoolStartup"})`)
		if err != nil {
			log.Printf("Failed to create CoolStartup: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (bigco:Company {name: "BigCo"})`)
		if err != nil {
			log.Printf("Failed to create BigCo: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		// Create projects
		_, err = conn.Query(`CREATE (proj1:Project {name: "Platform Rewrite", status: "in progress"})`)
		if err != nil {
			log.Printf("Failed to create Platform Rewrite: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`CREATE (proj2:Project {name: "Mobile App", status: "planning"})`)
		if err != nil {
			log.Printf("Failed to create Mobile App: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		// Initial employment
		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"}), (tc:Company {name: "TechCorp"}) CREATE (alice)-[:WORKS_AT {title: "Junior Engineer"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Alice's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (bob:Person {name: "Bob"}), (tc:Company {name: "TechCorp"}) CREATE (bob)-[:WORKS_AT {title: "Senior Designer"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Bob's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (carol:Person {name: "Carol"}), (tc:Company {name: "TechCorp"}) CREATE (carol)-[:WORKS_AT {title: "Engineering Manager"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Carol's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (david:Person {name: "David"}), (bc:Company {name: "BigCo"}) CREATE (david)-[:WORKS_AT {title: "DevOps Engineer"}]->(bc)`)
		if err != nil {
			log.Printf("Failed to create David's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (eve:Person {name: "Eve"}), (s:Company {name: "CoolStartup"}) CREATE (eve)-[:WORKS_AT {title: "Product Manager"}]->(s)`)
		if err != nil {
			log.Printf("Failed to create Eve's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (frank:Person {name: "Frank"}), (bc:Company {name: "BigCo"}) CREATE (frank)-[:WORKS_AT {title: "Senior Data Scientist"}]->(bc)`)
		if err != nil {
			log.Printf("Failed to create Frank's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (grace:Person {name: "Grace"}), (s:Company {name: "CoolStartup"}) CREATE (grace)-[:WORKS_AT {title: "Lead UX Designer"}]->(s)`)
		if err != nil {
			log.Printf("Failed to create Grace's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (henry:Person {name: "Henry"}), (tc:Company {name: "TechCorp"}) CREATE (henry)-[:WORKS_AT {title: "QA Lead"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Henry's job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		// Friendships
		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"}), (bob:Person {name: "Bob"}) CREATE (alice)-[:FRIENDS_WITH {since: 2020}]->(bob)`)
		if err != nil {
			log.Printf("Failed to create Alice-Bob friendship: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (bob:Person {name: "Bob"}), (david:Person {name: "David"}) CREATE (bob)-[:FRIENDS_WITH {since: 2019}]->(david)`)
		if err != nil {
			log.Printf("Failed to create Bob-David friendship: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (carol:Person {name: "Carol"}), (eve:Person {name: "Eve"}) CREATE (carol)-[:FRIENDS_WITH {since: 2021}]->(eve)`)
		if err != nil {
			log.Printf("Failed to create Carol-Eve friendship: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		// Mentorship
		_, err = conn.Query(`MATCH (carol:Person {name: "Carol"}), (alice:Person {name: "Alice"}) CREATE (carol)-[:MENTORS {started: 2021}]->(alice)`)
		if err != nil {
			log.Printf("Failed to create Carol mentors Alice: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (frank:Person {name: "Frank"}), (alice:Person {name: "Alice"}) CREATE (frank)-[:MENTORS {started: 2022}]->(alice)`)
		if err != nil {
			log.Printf("Failed to create Frank mentors Alice: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// Alice gets promoted - delete old relationship and create new one
		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"})-[r:WORKS_AT {title: "Junior Engineer"}]->(:Company {name: "TechCorp"}) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete Alice's old job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"}), (tc:Company {name: "TechCorp"}) CREATE (alice)-[:WORKS_AT {title: "Senior Engineer"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Alice's promotion: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// Bob moves to startup
		_, err = conn.Query(`MATCH (bob:Person {name: "Bob"})-[r:WORKS_AT]->(:Company {name: "TechCorp"}) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete Bob's old job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (bob:Person {name: "Bob"}), (s:Company {name: "CoolStartup"}) CREATE (bob)-[:WORKS_AT {title: "Design Lead"}]->(s)`)
		if err != nil {
			log.Printf("Failed to create Bob's new job: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// Friendship ends due to job change
		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"})-[r:FRIENDS_WITH]->(bob:Person {name: "Bob"}) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete Alice-Bob friendship: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// David joins startup too
		_, err = conn.Query(`MATCH (david:Person {name: "David"})-[r:WORKS_AT]->(:Company {name: "BigCo"}) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete David's old job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (david:Person {name: "David"}), (s:Company {name: "CoolStartup"}) CREATE (david)-[:WORKS_AT {title: "Lead DevOps"}]->(s)`)
		if err != nil {
			log.Printf("Failed to create David's new job: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// Collaboration relationships
		_, err = conn.Query(`MATCH (bob:Person {name: "Bob"}), (david:Person {name: "David"}) CREATE (bob)-[:COLLABORATES {project: "Platform"}]->(david)`)
		if err != nil {
			log.Printf("Failed to create Bob-David collaboration: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (eve:Person {name: "Eve"}), (bob:Person {name: "Bob"}) CREATE (eve)-[:COLLABORATES {project: "Product"}]->(bob)`)
		if err != nil {
			log.Printf("Failed to create Eve-Bob collaboration: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		// Assign people to projects
		_, err = conn.Query(`MATCH (bob:Person {name: "Bob"}), (p:Project {name: "Platform Rewrite"}) CREATE (bob)-[:WORKS_ON {role: "tech lead"}]->(p)`)
		if err != nil {
			log.Printf("Failed to assign Bob to project: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (grace:Person {name: "Grace"}), (p:Project {name: "Mobile App"}) CREATE (grace)-[:WORKS_ON {role: "design lead"}]->(p)`)
		if err != nil {
			log.Printf("Failed to assign Grace to project: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (david:Person {name: "David"}), (p:Project {name: "Platform Rewrite"}) CREATE (david)-[:WORKS_ON {role: "infrastructure"}]->(p)`)
		if err != nil {
			log.Printf("Failed to assign David to project: %v", err)
		}
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

		// Demonstrate temporal queries
		log.Println("Testing temporal queries...")
		time.Sleep(100 * time.Millisecond)

		// Query the graph at the earliest time (before any changes)
		result, err := conn.Query(`MATCH (p:Person) AT TIME EARLIEST RETURN p`)
		if err != nil {
			log.Printf("Failed to query earliest state: %v", err)
		} else {
			log.Printf("At earliest time: found %d people", len(result.Rows))
		}
		time.Sleep(50 * time.Millisecond)

		// Query Alice's employment history by looking at different time points
		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"})-[r:WORKS_AT]->(c:Company) RETURN alice, r, c`)
		if err != nil {
			log.Printf("Failed to query current Alice's job: %v", err)
		} else {
			log.Println("Current state: Alice's job queried successfully")
		}
		time.Sleep(50 * time.Millisecond)

		// Test semantic search if embedder is available
		if embedder != nil {
			log.Println("Testing semantic search...")
			_, err = conn.Query(`MATCH (p:Person) SIMILAR TO "software development" RETURN p`)
			if err != nil {
				log.Printf("Failed semantic search: %v", err)
			} else {
				log.Println("Semantic search completed successfully")
			}
			time.Sleep(50 * time.Millisecond)
		}

		// Complex query: Find all current employment relationships
		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name`)
		if err != nil {
			log.Printf("Failed to find employment: %v", err)
		} else {
			log.Printf("Current employment: %d relationships found", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v works at %v", row["p.name"], row["c.name"])
			}
		}
		time.Sleep(50 * time.Millisecond)

		// Test simpler pattern first
		result, err = conn.Query(`MATCH (p:Person) RETURN p.name`)
		if err != nil {
			log.Printf("Failed to find people: %v", err)
		} else {
			log.Printf("Total people in database: %d", len(result.Rows))
		}
		time.Sleep(50 * time.Millisecond)

		// Test path finding
		log.Println("Testing shortest path query...")
		result, err = conn.Query(`MATCH path = shortestPath((alice:Person {name: "Alice"})-[*]-(bob:Person {name: "Bob"})) RETURN path`)
		if err != nil {
			log.Printf("Failed to find shortest path: %v", err)
		} else {
			if len(result.Rows) > 0 {
				log.Println("Shortest path from Alice to Bob found")
			} else {
				log.Println("No path found between Alice and Bob (friendship was deleted)")
			}
		}
		time.Sleep(50 * time.Millisecond)

		// Test complex pattern: Find people working on projects
		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_ON]->(proj:Project) RETURN p.name, proj.name`)
		if err != nil {
			log.Printf("Failed to find collaborations: %v", err)
		} else {
			log.Printf("Project assignments: %d found", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v works on %v", row["p.name"], row["proj.name"])
			}
		}

		// ============================================
		// TEMPORAL TOMFOOLERY DEMONSTRATION
		// ============================================
		log.Println("")
		log.Println("========== TEMPORAL TOMFOOLERY ==========")
		log.Println("")

		// Record the current time for comparison
		timeAfterSetup := time.Now()

		// Make some more changes with timestamps
		log.Println("Making more changes to create temporal history...")
		time.Sleep(200 * time.Millisecond)

		// Alice gets another promotion!
		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"})-[r:WORKS_AT]->(c:Company) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete Alice's job for second promotion: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (alice:Person {name: "Alice"}), (tc:Company {name: "TechCorp"}) CREATE (alice)-[:WORKS_AT {title: "Staff Engineer"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Alice's second promotion: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// Eve quits startup and joins TechCorp
		_, err = conn.Query(`MATCH (eve:Person {name: "Eve"})-[r:WORKS_AT]->(:Company {name: "CoolStartup"}) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete Eve's startup job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (eve:Person {name: "Eve"}), (tc:Company {name: "TechCorp"}) CREATE (eve)-[:WORKS_AT {title: "Director of Product"}]->(tc)`)
		if err != nil {
			log.Printf("Failed to create Eve's TechCorp job: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// Grace moves from startup to BigCo
		_, err = conn.Query(`MATCH (grace:Person {name: "Grace"})-[r:WORKS_AT]->(:Company {name: "CoolStartup"}) DELETE r`)
		if err != nil {
			log.Printf("Failed to delete Grace's startup job: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		_, err = conn.Query(`MATCH (grace:Person {name: "Grace"}), (bc:Company {name: "BigCo"}) CREATE (grace)-[:WORKS_AT {title: "VP of Design"}]->(bc)`)
		if err != nil {
			log.Printf("Failed to create Grace's BigCo job: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// New friendship forms after job changes
		_, err = conn.Query(`MATCH (eve:Person {name: "Eve"}), (alice:Person {name: "Alice"}) CREATE (eve)-[:FRIENDS_WITH {since: 2024}]->(alice)`)
		if err != nil {
			log.Printf("Failed to create Eve-Alice friendship: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		timeAfterAllChanges := time.Now()

		// ---- TEMPORAL QUERY 1: Compare employment NOW vs BEFORE ----
		log.Println("")
		log.Println("--- TEMPORAL QUERY 1: Employment changes over time ---")

		// Get current employment
		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name`)
		if err != nil {
			log.Printf("Failed current employment query: %v", err)
		} else {
			log.Printf("CURRENT employment (%d):", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v @ %v", row["p.name"], row["c.name"])
			}
		}

		// Get employment at earliest time
		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company) AT TIME EARLIEST RETURN p.name, c.name`)
		if err != nil {
			log.Printf("Failed earliest employment query: %v", err)
		} else {
			log.Printf("EARLIEST employment (%d):", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v @ %v", row["p.name"], row["c.name"])
			}
		}

		// ---- TEMPORAL QUERY 2: Track a specific person's job history ----
		log.Println("")
		log.Println("--- TEMPORAL QUERY 2: Alice's career progression ---")
		log.Println("Alice started as Junior Engineer -> Senior Engineer -> Staff Engineer")

		// Query at different times to show progression
		result, err = conn.Query(`MATCH (alice:Person {name: "Alice"})-[:WORKS_AT]->(c:Company) AT TIME EARLIEST RETURN c.name`)
		if err != nil {
			log.Printf("Failed Alice earliest query: %v", err)
		} else {
			if len(result.Rows) > 0 {
				log.Printf("  EARLIEST: Alice worked at %v", result.Rows[0]["c.name"])
			}
		}

		result, err = conn.Query(`MATCH (alice:Person {name: "Alice"})-[:WORKS_AT]->(c:Company) RETURN c.name`)
		if err != nil {
			log.Printf("Failed Alice current query: %v", err)
		} else {
			if len(result.Rows) > 0 {
				log.Printf("  CURRENT: Alice works at %v (same company, but promoted 3x!)", result.Rows[0]["c.name"])
			}
		}

		// ---- TEMPORAL QUERY 3: Who worked at CoolStartup over time ----
		log.Println("")
		log.Println("--- TEMPORAL QUERY 3: CoolStartup employee history ---")

		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: "CoolStartup"}) AT TIME EARLIEST RETURN p.name`)
		if err != nil {
			log.Printf("Failed CoolStartup earliest query: %v", err)
		} else {
			log.Printf("CoolStartup employees (EARLIEST): %d", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v", row["p.name"])
			}
		}

		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: "CoolStartup"}) RETURN p.name`)
		if err != nil {
			log.Printf("Failed CoolStartup current query: %v", err)
		} else {
			log.Printf("CoolStartup employees (CURRENT): %d", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v", row["p.name"])
			}
		}

		// ---- TEMPORAL QUERY 4: Track friendship changes ----
		log.Println("")
		log.Println("--- TEMPORAL QUERY 4: Friendship dynamics ---")

		result, err = conn.Query(`MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) AT TIME EARLIEST RETURN a.name, b.name`)
		if err != nil {
			log.Printf("Failed earliest friendships query: %v", err)
		} else {
			log.Printf("Friendships at EARLIEST: %d", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v <-> %v", row["a.name"], row["b.name"])
			}
		}

		result, err = conn.Query(`MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) RETURN a.name, b.name`)
		if err != nil {
			log.Printf("Failed current friendships query: %v", err)
		} else {
			log.Printf("Friendships CURRENT: %d", len(result.Rows))
			for _, row := range result.Rows {
				log.Printf("  - %v <-> %v", row["a.name"], row["b.name"])
			}
		}
		log.Println("  Note: Alice-Bob friendship was deleted when Bob left TechCorp")
		log.Println("        Eve-Alice friendship formed after Eve joined TechCorp")

		// ---- TEMPORAL QUERY 5: Company headcount over time ----
		log.Println("")
		log.Println("--- TEMPORAL QUERY 5: Company headcount changes ---")

		companies := []string{"TechCorp", "CoolStartup", "BigCo"}
		for _, company := range companies {
			earliestQuery := fmt.Sprintf(`MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: "%s"}) AT TIME EARLIEST RETURN p.name`, company)
			currentQuery := fmt.Sprintf(`MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: "%s"}) RETURN p.name`, company)

			var earliestCount, currentCount int
			result, err = conn.Query(earliestQuery)
			if err == nil {
				earliestCount = len(result.Rows)
			}
			result, err = conn.Query(currentQuery)
			if err == nil {
				currentCount = len(result.Rows)
			}

			change := currentCount - earliestCount
			changeStr := ""
			if change > 0 {
				changeStr = fmt.Sprintf("+%d", change)
			} else if change < 0 {
				changeStr = fmt.Sprintf("%d", change)
			} else {
				changeStr = "no change"
			}
			log.Printf("  %s: %d -> %d (%s)", company, earliestCount, currentCount, changeStr)
		}

		// ---- Summary ----
		log.Println("")
		log.Println("========== TEMPORAL SUMMARY ==========")
		log.Printf("Timeline span: %v", timeAfterAllChanges.Sub(timeAfterSetup).Round(time.Millisecond))
		log.Println("Key events tracked:")
		log.Println("  - Alice: 3 promotions (Junior -> Senior -> Staff Engineer)")
		log.Println("  - Bob: Left TechCorp for CoolStartup")
		log.Println("  - David: Left BigCo for CoolStartup")
		log.Println("  - Eve: Left CoolStartup for TechCorp")
		log.Println("  - Grace: Left CoolStartup for BigCo")
		log.Println("  - Friendships: Alice-Bob ended, Eve-Alice formed")
		log.Println("")
		log.Println("All temporal data is queryable via AT TIME EARLIEST/LATEST or specific timestamps!")

		log.Println("")
		log.Println("Demo data loaded successfully - temporal tomfoolery complete!")
	}()
}
