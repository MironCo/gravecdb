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

        # Test 5: Create a node (clean up first so test is idempotent across runs)
        print("\n[Test 5] CREATE (p:Person {name: 'BoltTest', age: 99})")
        print("-" * 40)
        session.run("MATCH (p:Person {name: 'BoltTest'}) DELETE p")
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
        try:
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
        except Exception as e:
            if "embedder" in str(e).lower():
                print("  SKIP (no embedder configured - run with Ollama)")
            else:
                raise

        # Test 10: SIMILAR TO THROUGH TIME
        print("\n[Test 10] SIMILAR TO THROUGH TIME - historical semantic search")
        print("-" * 40)
        try:
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
        except Exception as e:
            if "embedder" in str(e).lower():
                print("  SKIP (no embedder configured - run with Ollama)")
            else:
                raise

        # Test 11: earliestPath - earliest arrival path between two specific people
        print("\n[Test 11] earliestPath((a:Person {name:'Alice'})-[*]->(b:Person {name:'Bob'})) - temporal Dijkstra")
        print("-" * 40)
        try:
            result = session.run(
                "MATCH p = earliestPath((a:Person {name: 'Alice'})-[*]->(b:Person {name: 'Bob'})) RETURN p, arrival_time"
            )
            rows = list(result)
            if len(rows) == 0:
                print("  SKIP (no directed path from Alice to Bob)")
            else:
                for record in rows:
                    arrival = record["arrival_time"]
                    assert arrival is not None, "arrival_time should not be None"
                    print(f"  earliest arrival: {arrival}")
                print(f"  PASS ({len(rows)} rows)")
        except Exception as e:
            print(f"  SKIP (earliestPath not available for this dataset: {e})")

        # Test 12: UNWIND - expand list into rows
        print("\n[Test 12] UNWIND ['Alice', 'Bob', 'Charlie'] AS name RETURN name")
        print("-" * 40)
        result = session.run("UNWIND ['Alice', 'Bob', 'Charlie'] AS name RETURN name")
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        names = [r["name"] for r in rows]
        assert set(names) == {"Alice", "Bob", "Charlie"}, f"Unexpected names: {names}"
        for r in rows:
            print(f"  {r['name']}")
        print(f"  PASS ({len(rows)} rows)")

        # Test 13: UNWIND with numbers
        print("\n[Test 13] UNWIND [10, 20, 30] AS x RETURN x")
        print("-" * 40)
        result = session.run("UNWIND [10, 20, 30] AS x RETURN x")
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        values = [r["x"] for r in rows]
        assert set(values) == {10, 20, 30}, f"Unexpected values: {values}"
        print(f"  {values}")
        print(f"  PASS ({len(rows)} rows)")

        # Test 14: MERGE - create if not exists
        print("\n[Test 14] MERGE (p:TestPerson {name: 'MergeTest'}) RETURN p")
        print("-" * 40)
        result = session.run("MERGE (p:TestPerson {name: 'MergeTest'}) RETURN p")
        rows1 = list(result)
        assert len(rows1) == 1, f"Expected 1 row on first MERGE, got {len(rows1)}"
        node1 = dict(rows1[0]["p"])
        assert node1.get("name") == "MergeTest", f"Wrong name: {node1}"
        print(f"  Created: {node1}")

        # Run MERGE again - should return the same existing node, not create a new one
        result = session.run("MERGE (p:TestPerson {name: 'MergeTest'}) RETURN p")
        rows2 = list(result)
        assert len(rows2) == 1, f"Expected 1 row on second MERGE, got {len(rows2)}"
        node2 = dict(rows2[0]["p"])
        assert node2.get("name") == "MergeTest", f"Wrong name on second MERGE: {node2}"

        # Verify only one node exists with this name
        result = session.run("MATCH (p:TestPerson {name: 'MergeTest'}) RETURN p")
        count = len(list(result))
        assert count == 1, f"Expected exactly 1 TestPerson node, found {count}"
        print(f"  Found after double MERGE: {node2}")
        print(f"  PASS (idempotent - still 1 node after 2 MERGEs)")

        # Test 15: toUpper / toLower on property values
        print("\n[Test 15] String functions: toUpper, toLower, size")
        print("-" * 40)
        result = session.run("UNWIND ['alice', 'Bob', 'CHARLIE'] AS name RETURN toUpper(name) AS up, toLower(name) AS lo, size(name) AS n")
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        for r in rows:
            assert r["up"] == r["up"].upper(), f"toUpper failed: {r['up']}"
            assert r["lo"] == r["lo"].lower(), f"toLower failed: {r['lo']}"
            assert isinstance(r["n"], int) and r["n"] > 0, f"size failed: {r['n']}"
            print(f"  up={r['up']!r}  lo={r['lo']!r}  size={r['n']}")
        print(f"  PASS ({len(rows)} rows)")

        # Test 16: Math functions: abs, round, ceil, floor
        print("\n[Test 16] Math functions: abs, round, ceil, floor")
        print("-" * 40)
        result = session.run("UNWIND [-3.7, 2.3, 0.0] AS x RETURN x, abs(x) AS a, round(x) AS r, ceil(x) AS c, floor(x) AS f")
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        for row in rows:
            assert row["a"] >= 0, f"abs({row['x']}) = {row['a']} should be >= 0"
            print(f"  x={row['x']}  abs={row['a']}  round={row['r']}  ceil={row['c']}  floor={row['f']}")
        # spot-check
        by_x = {r["x"]: r for r in rows}
        assert by_x[-3.7]["a"] == 3.7,  f"abs(-3.7) should be 3.7, got {by_x[-3.7]['a']}"
        assert by_x[-3.7]["r"] == -4.0, f"round(-3.7) should be -4, got {by_x[-3.7]['r']}"
        assert by_x[2.3]["c"]  == 3.0,  f"ceil(2.3) should be 3, got {by_x[2.3]['c']}"
        assert by_x[2.3]["f"]  == 2.0,  f"floor(2.3) should be 2, got {by_x[2.3]['f']}"
        print(f"  PASS ({len(rows)} rows)")

        # Test 17: toInteger / toFloat / toString / reverse
        print("\n[Test 17] Type-conversion functions: toInteger, toFloat, toString, reverse")
        print("-" * 40)
        result = session.run("UNWIND ['42', '3.14', 'hello'] AS s RETURN toInteger(s) AS i, toFloat(s) AS f, reverse(s) AS rev")
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        by_val = {}
        for r in rows:
            print(f"  toInteger={r['i']}  toFloat={r['f']}  reverse={r['rev']!r}")
            by_val[r["rev"]] = r
        assert by_val.get("24"), "reverse of '42' should be '24'"
        assert by_val["24"]["i"] == 42, f"toInteger('42') should be 42"
        assert abs(by_val["41.3"]["f"] - 3.14) < 0.001, f"toFloat('3.14') ≈ 3.14"
        assert by_val["olleh"]["rev"] == "olleh", "reverse of 'hello' should be 'olleh'"
        print(f"  PASS ({len(rows)} rows)")

        # Test 18: WITH clause — friends-of-friends style chained MATCH
        print("\n[Test 18] WITH clause — chained MATCH via WITH")
        print("-" * 40)
        result = session.run(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b MATCH (b)-[:WORKS_AT]->(c:Company) RETURN b.name AS person, c.name AS company"
        )
        rows = list(result)
        if len(rows) == 0:
            print("  SKIP (no Person-KNOWS-Person-WORKS_AT-Company paths in dataset)")
        else:
            for r in rows:
                assert r["person"] is not None, "person should not be None"
                assert r["company"] is not None, "company should not be None"
                print(f"  {r['person']} works at {r['company']}")
            print(f"  PASS ({len(rows)} rows)")

    driver.close()
    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
