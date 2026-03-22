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

		log.Println("Loading demo data with historical timestamps...")

		// Helper to run a query and log errors
		run := func(desc, query string) {
			_, err := conn.Query(query)
			if err != nil {
				log.Printf("  [FAIL] %s: %v", desc, err)
			}
		}

		// ============================================================
		// TIMELINE: A startup called TechCorp grows over ~2 years
		// ============================================================
		//
		// 2023-01-15  TechCorp founded, first hires (Alice, Bob, Carol)
		// 2023-03-01  First project kicks off, skills tracked
		// 2023-06-01  Company grows — David, Eve join
		// 2023-09-01  BigCo and CoolStartup enter the picture
		// 2024-01-15  Reorg — promotions, new teams
		// 2024-04-01  People switch companies
		// 2024-07-01  New projects, collaborations form
		// 2024-10-01  More hires, mentorship begins
		// 2025-01-15  Alice promoted to Staff, Eve joins TechCorp
		// 2025-06-01  Latest state — mature org
		// ============================================================

		// ---- Jan 2023: TechCorp founded, first 3 hires ----
		log.Println("  2023-01 — TechCorp founded...")
		run("TechCorp", `CREATE (tc:Company {name: "TechCorp", founded: 2023}) AT TIME 1673740800`)

		run("Alice", `CREATE (p:Person {name: "Alice", role: "backend engineer"}) AT TIME 1673827200`)
		run("Bob", `CREATE (p:Person {name: "Bob", role: "frontend developer"}) AT TIME 1673913600`)
		run("Carol", `CREATE (p:Person {name: "Carol", role: "engineering manager"}) AT TIME 1674000000`)

		// Initial employment at TechCorp
		run("Alice@TechCorp", `MATCH (p:Person {name: "Alice"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Junior Engineer"}]->(c) AT TIME 1673827200`)
		run("Bob@TechCorp", `MATCH (p:Person {name: "Bob"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Frontend Dev"}]->(c) AT TIME 1673913600`)
		run("Carol@TechCorp", `MATCH (p:Person {name: "Carol"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Engineering Manager"}]->(c) AT TIME 1674000000`)

		// Reporting structure
		run("Alice->Carol", `MATCH (a:Person {name: "Alice"}), (c:Person {name: "Carol"}) CREATE (a)-[:REPORTS_TO]->(c) AT TIME 1674000000`)
		run("Bob->Carol", `MATCH (b:Person {name: "Bob"}), (c:Person {name: "Carol"}) CREATE (b)-[:REPORTS_TO]->(c) AT TIME 1674000000`)

		// ---- Mar 2023: Skills and first project ----
		log.Println("  2023-03 — Skills tracked, first project...")
		run("Go skill", `CREATE (s:Skill {name: "Go", category: "backend"}) AT TIME 1677628800`)
		run("React skill", `CREATE (s:Skill {name: "React", category: "frontend"}) AT TIME 1677628800`)
		run("Python skill", `CREATE (s:Skill {name: "Python", category: "data"}) AT TIME 1677628800`)
		run("K8s skill", `CREATE (s:Skill {name: "Kubernetes", category: "infrastructure"}) AT TIME 1677628800`)
		run("ML skill", `CREATE (s:Skill {name: "Machine Learning", category: "data"}) AT TIME 1677628800`)

		run("Alice+Go", `MATCH (p:Person {name: "Alice"}), (s:Skill {name: "Go"}) CREATE (p)-[:HAS_SKILL {level: "expert"}]->(s) AT TIME 1677628800`)
		run("Bob+React", `MATCH (p:Person {name: "Bob"}), (s:Skill {name: "React"}) CREATE (p)-[:HAS_SKILL {level: "expert"}]->(s) AT TIME 1677628800`)

		run("Platform project", `CREATE (p:Project {name: "Platform Rewrite", status: "in progress"}) AT TIME 1677715200`)
		run("Alice on Platform", `MATCH (p:Person {name: "Alice"}), (proj:Project {name: "Platform Rewrite"}) CREATE (p)-[:WORKS_ON {role: "tech lead"}]->(proj) AT TIME 1677715200`)

		// Friendship forms early
		run("Alice-Bob friends", `MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:FRIENDS_WITH {since: 2023}]->(b) AT TIME 1677715200`)

		// ---- Jun 2023: Growth — David and Eve join ----
		log.Println("  2023-06 — David and Eve join...")
		run("David", `CREATE (p:Person {name: "David", role: "devops engineer"}) AT TIME 1685577600`)
		run("Eve", `CREATE (p:Person {name: "Eve", role: "product manager"}) AT TIME 1685664000`)

		run("David@TechCorp", `MATCH (p:Person {name: "David"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "DevOps Engineer"}]->(c) AT TIME 1685577600`)
		run("Eve@TechCorp", `MATCH (p:Person {name: "Eve"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Product Manager"}]->(c) AT TIME 1685664000`)

		run("David+K8s", `MATCH (p:Person {name: "David"}), (s:Skill {name: "Kubernetes"}) CREATE (p)-[:HAS_SKILL {level: "expert"}]->(s) AT TIME 1685577600`)
		run("David on Platform", `MATCH (p:Person {name: "David"}), (proj:Project {name: "Platform Rewrite"}) CREATE (p)-[:WORKS_ON {role: "infrastructure"}]->(proj) AT TIME 1685577600`)

		run("Bob-David friends", `MATCH (b:Person {name: "Bob"}), (d:Person {name: "David"}) CREATE (b)-[:FRIENDS_WITH {since: 2023}]->(d) AT TIME 1685664000`)

		// ---- Sep 2023: Other companies appear ----
		log.Println("  2023-09 — BigCo and CoolStartup enter...")
		run("BigCo", `CREATE (c:Company {name: "BigCo", founded: 2010}) AT TIME 1693526400`)
		run("CoolStartup", `CREATE (c:Company {name: "CoolStartup", founded: 2023}) AT TIME 1693526400`)

		// More people
		run("Frank", `CREATE (p:Person {name: "Frank", role: "data scientist"}) AT TIME 1693612800`)
		run("Grace", `CREATE (p:Person {name: "Grace", role: "UX designer"}) AT TIME 1693699200`)
		run("Henry", `CREATE (p:Person {name: "Henry", role: "QA engineer"}) AT TIME 1693785600`)

		run("Frank@BigCo", `MATCH (p:Person {name: "Frank"}), (c:Company {name: "BigCo"}) CREATE (p)-[:WORKS_AT {title: "Senior Data Scientist"}]->(c) AT TIME 1693612800`)
		run("Grace@CoolStartup", `MATCH (p:Person {name: "Grace"}), (c:Company {name: "CoolStartup"}) CREATE (p)-[:WORKS_AT {title: "Lead UX Designer"}]->(c) AT TIME 1693699200`)
		run("Henry@TechCorp", `MATCH (p:Person {name: "Henry"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "QA Lead"}]->(c) AT TIME 1693785600`)

		run("Henry->Carol", `MATCH (h:Person {name: "Henry"}), (c:Person {name: "Carol"}) CREATE (h)-[:REPORTS_TO]->(c) AT TIME 1693785600`)
		run("Frank+Python", `MATCH (p:Person {name: "Frank"}), (s:Skill {name: "Python"}) CREATE (p)-[:HAS_SKILL {level: "expert"}]->(s) AT TIME 1693612800`)
		run("Frank+ML", `MATCH (p:Person {name: "Frank"}), (s:Skill {name: "Machine Learning"}) CREATE (p)-[:HAS_SKILL {level: "intermediate"}]->(s) AT TIME 1693612800`)

		run("Frank-Maya friends", `MATCH (f:Person {name: "Frank"}), (g:Person {name: "Grace"}) CREATE (f)-[:FRIENDS_WITH {since: 2023}]->(g) AT TIME 1693699200`)

		// ---- Jan 2024: Reorg — Alice promoted, new hires ----
		log.Println("  2024-01 — Reorg and promotions...")
		run("Isabel", `CREATE (p:Person {name: "Isabel", role: "security engineer"}) AT TIME 1705276800`)
		run("Jack", `CREATE (p:Person {name: "Jack", role: "platform engineer"}) AT TIME 1705363200`)

		run("Isabel@TechCorp", `MATCH (p:Person {name: "Isabel"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Security Engineer"}]->(c) AT TIME 1705276800`)
		run("Jack@BigCo", `MATCH (p:Person {name: "Jack"}), (c:Company {name: "BigCo"}) CREATE (p)-[:WORKS_AT {title: "Senior Platform Engineer"}]->(c) AT TIME 1705363200`)

		run("Isabel-Henry friends", `MATCH (i:Person {name: "Isabel"}), (h:Person {name: "Henry"}) CREATE (i)-[:FRIENDS_WITH {since: 2024}]->(h) AT TIME 1705363200`)
		run("Jack-David friends", `MATCH (j:Person {name: "Jack"}), (d:Person {name: "David"}) CREATE (j)-[:FRIENDS_WITH {since: 2024}]->(d) AT TIME 1705363200`)

		// Alice promoted: delete old WORKS_AT, create new one
		run("Alice promo delete", `MATCH (p:Person {name: "Alice"})-[r:WORKS_AT]->(:Company {name: "TechCorp"}) DELETE r AT TIME 1705449600`)
		run("Alice promo", `MATCH (p:Person {name: "Alice"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Senior Engineer"}]->(c) AT TIME 1705449600`)
		run("Alice role update", `MATCH (p:Person {name: "Alice"}) SET p.role = "senior engineer"`)

		// Carol mentors Alice
		run("Carol mentors Alice", `MATCH (c:Person {name: "Carol"}), (a:Person {name: "Alice"}) CREATE (c)-[:MENTORS {started: 2024}]->(a) AT TIME 1705449600`)

		// ---- Apr 2024: People switch companies ----
		log.Println("  2024-04 — Bob and David move to CoolStartup...")

		// Bob leaves TechCorp for CoolStartup
		run("Bob leaves TechCorp", `MATCH (p:Person {name: "Bob"})-[r:WORKS_AT]->(:Company {name: "TechCorp"}) DELETE r AT TIME 1711929600`)
		run("Bob@CoolStartup", `MATCH (p:Person {name: "Bob"}), (c:Company {name: "CoolStartup"}) CREATE (p)-[:WORKS_AT {title: "Design Lead"}]->(c) AT TIME 1711929600`)

		// Alice-Bob friendship ends when Bob leaves
		run("Alice-Bob unfriend", `MATCH (a:Person {name: "Alice"})-[r:FRIENDS_WITH]->(b:Person {name: "Bob"}) DELETE r AT TIME 1711929600`)

		// David leaves TechCorp for CoolStartup
		run("David leaves TechCorp", `MATCH (p:Person {name: "David"})-[r:WORKS_AT]->(:Company {name: "TechCorp"}) DELETE r AT TIME 1711929600`)
		run("David@CoolStartup", `MATCH (p:Person {name: "David"}), (c:Company {name: "CoolStartup"}) CREATE (p)-[:WORKS_AT {title: "Lead DevOps"}]->(c) AT TIME 1711929600`)

		// ---- Jul 2024: New projects, collaborations ----
		log.Println("  2024-07 — New projects and collaborations...")
		run("Mobile App", `CREATE (p:Project {name: "Mobile App", status: "planning"}) AT TIME 1719792000`)
		run("Grace on Mobile", `MATCH (p:Person {name: "Grace"}), (proj:Project {name: "Mobile App"}) CREATE (p)-[:WORKS_ON {role: "design lead"}]->(proj) AT TIME 1719792000`)
		run("Bob on Platform", `MATCH (p:Person {name: "Bob"}), (proj:Project {name: "Platform Rewrite"}) CREATE (p)-[:WORKS_ON {role: "frontend"}]->(proj) AT TIME 1719792000`)

		run("Bob-David collab", `MATCH (b:Person {name: "Bob"}), (d:Person {name: "David"}) CREATE (b)-[:COLLABORATES {project: "Platform"}]->(d) AT TIME 1719792000`)
		run("Eve-Bob collab", `MATCH (e:Person {name: "Eve"}), (b:Person {name: "Bob"}) CREATE (e)-[:COLLABORATES {project: "Product"}]->(b) AT TIME 1719878400`)

		// More hires
		run("Kate", `CREATE (p:Person {name: "Kate", role: "engineering manager"}) AT TIME 1719878400`)
		run("Kate@CoolStartup", `MATCH (p:Person {name: "Kate"}), (c:Company {name: "CoolStartup"}) CREATE (p)-[:WORKS_AT {title: "Engineering Manager"}]->(c) AT TIME 1719878400`)

		run("Liam", `CREATE (p:Person {name: "Liam", role: "backend engineer"}) AT TIME 1719964800`)
		run("Liam@BigCo", `MATCH (p:Person {name: "Liam"}), (c:Company {name: "BigCo"}) CREATE (p)-[:WORKS_AT {title: "Backend Engineer"}]->(c) AT TIME 1719964800`)
		run("Liam+Go", `MATCH (p:Person {name: "Liam"}), (s:Skill {name: "Go"}) CREATE (p)-[:HAS_SKILL {level: "intermediate"}]->(s) AT TIME 1719964800`)

		run("Liam->Jack", `MATCH (l:Person {name: "Liam"}), (j:Person {name: "Jack"}) CREATE (l)-[:REPORTS_TO]->(j) AT TIME 1719964800`)

		// ---- Oct 2024: Mentorship, Maya joins ----
		log.Println("  2024-10 — Maya joins, mentorship grows...")
		run("Maya", `CREATE (p:Person {name: "Maya", role: "ML engineer"}) AT TIME 1727740800`)
		run("Maya@CoolStartup", `MATCH (p:Person {name: "Maya"}), (c:Company {name: "CoolStartup"}) CREATE (p)-[:WORKS_AT {title: "ML Engineer"}]->(c) AT TIME 1727740800`)
		run("Maya+Python", `MATCH (p:Person {name: "Maya"}), (s:Skill {name: "Python"}) CREATE (p)-[:HAS_SKILL {level: "expert"}]->(s) AT TIME 1727740800`)
		run("Maya+ML", `MATCH (p:Person {name: "Maya"}), (s:Skill {name: "Machine Learning"}) CREATE (p)-[:HAS_SKILL {level: "expert"}]->(s) AT TIME 1727740800`)
		run("Maya->Kate", `MATCH (m:Person {name: "Maya"}), (k:Person {name: "Kate"}) CREATE (m)-[:REPORTS_TO]->(k) AT TIME 1727740800`)

		run("Frank mentors Alice", `MATCH (f:Person {name: "Frank"}), (a:Person {name: "Alice"}) CREATE (f)-[:MENTORS {started: 2024}]->(a) AT TIME 1727827200`)
		run("Carol-Eve friends", `MATCH (c:Person {name: "Carol"}), (e:Person {name: "Eve"}) CREATE (c)-[:FRIENDS_WITH {since: 2024}]->(e) AT TIME 1727827200`)
		run("Frank-Maya friends", `MATCH (f:Person {name: "Frank"}), (m:Person {name: "Maya"}) CREATE (f)-[:FRIENDS_WITH {since: 2024}]->(m) AT TIME 1727827200`)

		// ---- Jan 2025: Alice staff promo, Eve switches to TechCorp ----
		log.Println("  2025-01 — Alice promoted to Staff, Eve joins TechCorp...")

		// Alice -> Staff Engineer
		run("Alice promo2 delete", `MATCH (p:Person {name: "Alice"})-[r:WORKS_AT]->(:Company {name: "TechCorp"}) DELETE r AT TIME 1736899200`)
		run("Alice staff", `MATCH (p:Person {name: "Alice"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Staff Engineer"}]->(c) AT TIME 1736899200`)
		run("Alice role staff", `MATCH (p:Person {name: "Alice"}) SET p.role = "staff engineer"`)

		// Eve promoted to Director of Product
		run("Eve promo delete", `MATCH (p:Person {name: "Eve"})-[r:WORKS_AT]->(:Company {name: "TechCorp"}) DELETE r AT TIME 1736985600`)
		run("Eve director", `MATCH (p:Person {name: "Eve"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {title: "Director of Product"}]->(c) AT TIME 1736985600`)

		// Grace moves from CoolStartup to BigCo
		run("Grace leaves CoolStartup", `MATCH (p:Person {name: "Grace"})-[r:WORKS_AT]->(:Company {name: "CoolStartup"}) DELETE r AT TIME 1736985600`)
		run("Grace@BigCo", `MATCH (p:Person {name: "Grace"}), (c:Company {name: "BigCo"}) CREATE (p)-[:WORKS_AT {title: "VP of Design"}]->(c) AT TIME 1736985600`)

		// New friendship
		run("Eve-Alice friends", `MATCH (e:Person {name: "Eve"}), (a:Person {name: "Alice"}) CREATE (e)-[:FRIENDS_WITH {since: 2025}]->(a) AT TIME 1736985600`)

		// ---- Jun 2025: Current state ----
		log.Println("  2025-06 — Current state (mature org)...")

		// Generate embeddings if embedder is available
		if embedder != nil {
			log.Println("  Generating embeddings for Person nodes...")
			_, err := conn.Query(`MATCH (p:Person) EMBED p.role RETURN p`)
			if err != nil {
				log.Printf("  [FAIL] generate embeddings: %v", err)
			} else {
				log.Println("  Embeddings generated successfully")
			}
		}

		// ---- Summary queries ----
		log.Println("")
		log.Println("========== DEMO DATA SUMMARY ==========")

		result, err := conn.Query(`MATCH (p:Person) RETURN COUNT(p) AS total`)
		if err == nil && len(result.Rows) > 0 {
			log.Printf("  People: %v", result.Rows[0]["total"])
		}

		result, err = conn.Query(`MATCH (c:Company) RETURN COUNT(c) AS total`)
		if err == nil && len(result.Rows) > 0 {
			log.Printf("  Companies: %v", result.Rows[0]["total"])
		}

		result, err = conn.Query(`MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, COUNT(p) AS headcount`)
		if err == nil {
			for _, row := range result.Rows {
				log.Printf("  %v: %v employees", row["c.name"], row["headcount"])
			}
		}

		log.Println("")
		log.Println("  Timeline spans Jan 2023 → Jun 2025")
		log.Println("  Use the time slider to explore how the org evolved!")
		log.Println("")
		log.Println("Demo data loaded successfully!")
	}()
}
