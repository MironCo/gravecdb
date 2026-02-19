#!/usr/bin/env python3
"""
Test script for GravecDB Bolt protocol implementation.
Requires: pip install neo4j
"""

from neo4j import GraphDatabase

def main():
    # Connect to GravecDB via Bolt protocol
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri)

    print("=" * 50)
    print("GravecDB Bolt Protocol Test")
    print("=" * 50)

    with driver.session() as session:
        # Test 1: Get all Person nodes
        print("\n[Test 1] MATCH (n:Person) RETURN n")
        print("-" * 40)
        result = session.run("MATCH (n:Person) RETURN n")
        for record in result:
            node = record["n"]
            print(f"  Node: {node}")

        # Test 2: Get all Company nodes
        print("\n[Test 2] MATCH (n:Company) RETURN n")
        print("-" * 40)
        result = session.run("MATCH (n:Company) RETURN n")
        for record in result:
            node = record["n"]
            print(f"  Node: {node}")

        # Test 3: Get specific properties
        print("\n[Test 3] MATCH (p:Person) RETURN p.name")
        print("-" * 40)
        result = session.run("MATCH (p:Person) RETURN p.name")
        for record in result:
            print(f"  Name: {record['p.name']}")

        # Test 4: Relationships
        print("\n[Test 4] MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b")
        print("-" * 40)
        result = session.run("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b")
        for record in result:
            print(f"  {record['a']} -[KNOWS]-> {record['b']}")

        # Test 5: Create a node
        print("\n[Test 5] CREATE (p:Person {name: 'BoltTest', age: 99})")
        print("-" * 40)
        result = session.run("CREATE (p:Person {name: 'BoltTest', age: 99})")
        print("  Node created!")

        # Test 6: Verify the created node
        print("\n[Test 6] MATCH (p:Person {name: 'BoltTest'}) RETURN p")
        print("-" * 40)
        result = session.run("MATCH (p:Person {name: 'BoltTest'}) RETURN p")
        for record in result:
            print(f"  Found: {record['p']}")

        # Test 7: DURATION on relationships
        print("\n[Test 7] DURATION(r) - how long each person has worked at their company")
        print("-" * 40)
        result = session.run(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p.name, c.name, DURATION(r) AS tenure_days ORDER BY tenure_days DESC"
        )
        rows = list(result)
        assert len(rows) > 0, "Expected WORKS_AT relationships"
        for record in rows:
            days = record["tenure_days"]
            assert days is not None, f"DURATION returned None for {record['p.name']}"
            assert days >= 0, f"DURATION returned negative value {days}"
            print(f"  {record['p.name']} @ {record['c.name']}: {days:.2f} days")
        print(f"  PASS ({len(rows)} rows)")

        # Test 8: DURATION on nodes
        print("\n[Test 8] DURATION(p) - how long each Person node has existed")
        print("-" * 40)
        result = session.run(
            "MATCH (p:Person) RETURN p.name, DURATION(p) AS age_days ORDER BY age_days DESC"
        )
        rows = list(result)
        assert len(rows) > 0, "Expected Person nodes"
        for record in rows:
            days = record["age_days"]
            assert days is not None, f"DURATION returned None for {record['p.name']}"
            assert days >= 0, f"DURATION returned negative value {days}"
            print(f"  {record['p.name']}: {days:.4f} days old")
        print(f"  PASS ({len(rows)} rows)")

        # Test 9: SIMILAR TO semantic search (top-level, not WHERE)
        print("\n[Test 9] SIMILAR TO - semantic search for 'software engineer'")
        print("-" * 40)
        result = session.run(
            'MATCH (p:Person) SIMILAR TO "software engineer" RETURN p.name, similarity'
        )
        rows = list(result)
        if len(rows) == 0:
            print("  SKIP (no embeddings - run EMBED first)")
        else:
            for record in rows:
                sim = record["similarity"]
                assert 0.0 <= sim <= 1.0, f"similarity {sim} out of range"
                print(f"  {record['p.name']}: {sim:.4f}")
            print(f"  PASS ({len(rows)} rows)")

        # Test 10: SIMILAR TO THROUGH TIME
        print("\n[Test 10] SIMILAR TO THROUGH TIME - historical semantic search")
        print("-" * 40)
        result = session.run(
            'MATCH (p:Person) SIMILAR TO "engineer" THROUGH TIME RETURN p.name, similarity, valid_from, valid_to'
        )
        rows = list(result)
        if len(rows) == 0:
            print("  SKIP (no embeddings - run EMBED first)")
        else:
            for record in rows:
                assert record["valid_from"] is not None, "valid_from should not be None"
                print(f"  {record['p.name']}: sim={record['similarity']:.4f} from={record['valid_from']}")
            print(f"  PASS ({len(rows)} rows)")

    driver.close()
    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
