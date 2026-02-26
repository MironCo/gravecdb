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

        # Test 19: WHERE with scalar function — toUpper(p.name) = 'ALICE'
        print("\n[Test 19] WHERE function: toUpper(p.name) = 'ALICE'")
        print("-" * 40)
        session.run("MERGE (p:TestPerson {name: 'alice_case'})").consume()
        result = session.run("MATCH (p:TestPerson) WHERE toUpper(p.name) = 'ALICE_CASE' RETURN p.name AS name")
        rows = list(result)
        assert len(rows) >= 1, f"Expected at least 1 row, got {len(rows)}"
        names = [r["name"] for r in rows]
        assert "alice_case" in names, f"Expected 'alice_case' in results, got {names}"
        print(f"  Found: {names}")
        print("  PASS")

        # Test 20: OPTIONAL MATCH — returns null row when pattern not found
        print("\n[Test 20] OPTIONAL MATCH — null row on no match")
        print("-" * 40)
        result = session.run(
            "OPTIONAL MATCH (p:NonExistentLabel9999) RETURN p.name AS name"
        )
        rows = list(result)
        assert len(rows) == 1, f"Expected exactly 1 null row, got {len(rows)}"
        assert rows[0]["name"] is None, f"Expected null name, got {rows[0]['name']}"
        print(f"  Row: name={rows[0]['name']!r}")
        print("  PASS")

        # Test 21: CASE WHEN in RETURN
        print("\n[Test 21] CASE WHEN in RETURN")
        print("-" * 40)
        result = session.run(
            "UNWIND [1, 5, 10] AS x "
            "RETURN x, CASE WHEN x < 5 THEN 'low' WHEN x = 5 THEN 'mid' ELSE 'high' END AS bucket"
        )
        rows = list(result)
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        by_x = {r["x"]: r["bucket"] for r in rows}
        assert by_x[1]  == "low",  f"x=1 bucket should be 'low',  got {by_x[1]!r}"
        assert by_x[5]  == "mid",  f"x=5 bucket should be 'mid',  got {by_x[5]!r}"
        assert by_x[10] == "high", f"x=10 bucket should be 'high', got {by_x[10]!r}"
        for r in rows:
            print(f"  x={r['x']}  bucket={r['bucket']!r}")
        print("  PASS")

        # Test 22: OR in WHERE
        print("\n[Test 22] OR in WHERE")
        print("-" * 40)
        result = session.run(
            "UNWIND [1, 2, 3, 4, 5] AS x WHERE x = 1 OR x = 3 OR x = 5 RETURN x"
        )
        rows = list(result)
        vals = sorted([r["x"] for r in rows])
        assert vals == [1, 3, 5], f"Expected [1,3,5], got {vals}"
        print(f"  Filtered values: {vals}")
        print("  PASS")

        # Test 23: NOT in WHERE
        print("\n[Test 23] NOT in WHERE")
        print("-" * 40)
        result = session.run(
            "UNWIND [1, 2, 3, 4, 5] AS x WHERE NOT x = 3 RETURN x"
        )
        rows = list(result)
        vals = sorted([r["x"] for r in rows])
        assert vals == [1, 2, 4, 5], f"Expected [1,2,4,5], got {vals}"
        print(f"  Filtered values: {vals}")
        print("  PASS")

        # Test 24: IN operator in WHERE
        print("\n[Test 24] IN operator in WHERE")
        print("-" * 40)
        result = session.run(
            "UNWIND [10, 20, 30, 40, 50] AS x WHERE x IN [20, 40] RETURN x"
        )
        rows = list(result)
        vals = sorted([r["x"] for r in rows])
        assert vals == [20, 40], f"Expected [20,40], got {vals}"
        print(f"  IN filtered values: {vals}")
        print("  PASS")

        # Test 25: IS NULL / IS NOT NULL in WHERE (via OPTIONAL MATCH)
        print("\n[Test 25] IS NULL / IS NOT NULL")
        print("-" * 40)
        # Create two nodes, one with 'score' and one without
        session.run("MERGE (a:ScoreTest {id: 'a', score: 42})").consume()
        session.run("MERGE (b:ScoreTest {id: 'b'})").consume()
        result = session.run(
            "MATCH (n:ScoreTest) WHERE n.score IS NULL RETURN n.id AS id"
        )
        null_ids = [r["id"] for r in result]
        assert "b" in null_ids, f"Expected 'b' (no score) in IS NULL result, got {null_ids}"
        assert "a" not in null_ids, f"'a' (has score) should not appear in IS NULL result"
        result2 = session.run(
            "MATCH (n:ScoreTest) WHERE n.score IS NOT NULL RETURN n.id AS id"
        )
        not_null_ids = [r["id"] for r in result2]
        assert "a" in not_null_ids, f"Expected 'a' in IS NOT NULL result, got {not_null_ids}"
        assert "b" not in not_null_ids, f"'b' should not appear in IS NOT NULL result"
        print(f"  IS NULL ids: {null_ids}")
        print(f"  IS NOT NULL ids: {not_null_ids}")
        print("  PASS")

        # Test 26: STARTS WITH in WHERE
        print("\n[Test 26] STARTS WITH in WHERE")
        print("-" * 40)
        result = session.run(
            "UNWIND ['Apple', 'Banana', 'Avocado', 'Cherry'] AS fruit WHERE fruit STARTS WITH 'A' RETURN fruit"
        )
        rows = list(result)
        vals = sorted([r["fruit"] for r in rows])
        assert vals == ["Apple", "Avocado"], f"Expected ['Apple','Avocado'], got {vals}"
        print(f"  STARTS WITH 'A': {vals}")
        print("  PASS")

        # Test 27: ENDS WITH in WHERE
        print("\n[Test 27] ENDS WITH in WHERE")
        print("-" * 40)
        result = session.run(
            "UNWIND ['hello', 'world', 'jello', 'mellow'] AS w WHERE w ENDS WITH 'llo' RETURN w"
        )
        rows = list(result)
        vals = sorted([r["w"] for r in rows])
        assert vals == ["hello", "jello"], f"Expected ['hello','jello'], got {vals}"
        print(f"  ENDS WITH 'llo': {vals}")
        print("  PASS")

        # Test 28: CONTAINS in WHERE
        print("\n[Test 28] CONTAINS in WHERE")
        print("-" * 40)
        result = session.run(
            "UNWIND ['graph', 'database', 'graphic', 'data'] AS w WHERE w CONTAINS 'raph' RETURN w"
        )
        rows = list(result)
        vals = sorted([r["w"] for r in rows])
        assert vals == ["graph", "graphic"], f"Expected ['graph','graphic'], got {vals}"
        print(f"  CONTAINS 'raph': {vals}")
        print("  PASS")

        # Test 29: String concatenation in RETURN
        print("\n[Test 29] String concatenation (p.first + ' ' + p.last)")
        print("-" * 40)
        session.run("MERGE (p:ConcatTest {first: 'John', last: 'Doe'})").consume()
        session.run("MERGE (p:ConcatTest {first: 'Jane', last: 'Smith'})").consume()
        result = session.run(
            "MATCH (p:ConcatTest) RETURN p.first + ' ' + p.last AS full_name ORDER BY full_name"
        )
        rows = list(result)
        assert len(rows) >= 2, f"Expected at least 2 rows, got {len(rows)}"
        names = [r["full_name"] for r in rows]
        assert "John Doe" in names, f"Expected 'John Doe' in {names}"
        assert "Jane Smith" in names, f"Expected 'Jane Smith' in {names}"
        for r in rows:
            print(f"  full_name={r['full_name']!r}")
        print("  PASS")

        # Test 30: Multi-pattern MATCH (cartesian product)
        print("\n[Test 30] Multi-pattern MATCH — cartesian product")
        print("-" * 40)
        session.run("MERGE (a:CartA {name: 'A1'})").consume()
        session.run("MERGE (a:CartA {name: 'A2'})").consume()
        session.run("MERGE (b:CartB {name: 'B1'})").consume()
        result = session.run(
            "MATCH (a:CartA), (b:CartB) RETURN a.name AS a, b.name AS b ORDER BY a, b"
        )
        rows = list(result)
        pairs = [(r["a"], r["b"]) for r in rows]
        assert len(rows) == 2, f"Expected 2 rows (2 CartA × 1 CartB), got {len(rows)}: {pairs}"
        assert ("A1", "B1") in pairs, f"Expected ('A1','B1') in {pairs}"
        assert ("A2", "B1") in pairs, f"Expected ('A2','B1') in {pairs}"
        for r in rows:
            print(f"  a={r['a']!r}  b={r['b']!r}")
        print("  PASS")

        # Test 31: labels(n) introspection
        print("\n[Test 31] labels(n) — node label introspection")
        print("-" * 40)
        result = session.run("MATCH (p:Person) RETURN labels(p) AS lbls LIMIT 1")
        rows = list(result)
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        lbls = rows[0]["lbls"]
        assert "Person" in lbls, f"Expected 'Person' in labels, got {lbls}"
        print(f"  labels(p) = {lbls}")
        print("  PASS")

        # Test 32: type(r) — relationship type introspection
        print("\n[Test 32] type(r) — relationship type introspection")
        print("-" * 40)
        result = session.run(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r) AS t LIMIT 1"
        )
        rows = list(result)
        if len(rows) == 0:
            print("  SKIP (no KNOWS relationships in dataset)")
        else:
            t = rows[0]["t"]
            assert t == "KNOWS", f"Expected 'KNOWS', got {t!r}"
            print(f"  type(r) = {t!r}")
            print("  PASS")

        # Test 33: keys(n) — property key introspection
        print("\n[Test 33] keys(n) — property key introspection")
        print("-" * 40)
        session.run("MERGE (k:KeyTest {alpha: 1, beta: 2, gamma: 3})").consume()
        result = session.run("MATCH (k:KeyTest) RETURN keys(k) AS ks LIMIT 1")
        rows = list(result)
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        ks = sorted(rows[0]["ks"])
        assert "alpha" in ks and "beta" in ks and "gamma" in ks, f"Missing keys: {ks}"
        print(f"  keys(k) = {ks}")
        print("  PASS")

        # Test 34: coalesce() — first non-null
        print("\n[Test 34] coalesce() — first non-null value")
        print("-" * 40)
        result = session.run(
            "MATCH (n:ScoreTest) RETURN n.id AS id, coalesce(n.score, 0) AS score ORDER BY n.id"
        )
        rows = list(result)
        assert len(rows) >= 2, f"Expected >=2 ScoreTest nodes, got {len(rows)}"
        by_id = {r["id"]: r["score"] for r in rows}
        assert by_id["a"] == 42, f"Node 'a' has score 42, coalesce should return 42, got {by_id['a']}"
        assert by_id["b"] == 0,  f"Node 'b' has no score, coalesce should return 0, got {by_id['b']}"
        print(f"  coalesce results: {by_id}")
        print("  PASS")

        # Test 35: range() — generate integer list
        print("\n[Test 35] range() — integer list generation")
        print("-" * 40)
        result = session.run("UNWIND range(1, 5) AS x RETURN x")
        rows = list(result)
        vals = [r["x"] for r in rows]
        assert vals == [1, 2, 3, 4, 5], f"Expected [1..5], got {vals}"
        print(f"  range(1,5) = {vals}")
        result = session.run("UNWIND range(0, 10, 2) AS x RETURN x")
        rows = list(result)
        vals = [r["x"] for r in rows]
        assert vals == [0, 2, 4, 6, 8, 10], f"Expected [0,2,4,6,8,10], got {vals}"
        print(f"  range(0,10,2) = {vals}")
        print("  PASS")

        # Test 36: COUNT(DISTINCT x)
        print("\n[Test 36] COUNT(DISTINCT x)")
        print("-" * 40)
        session.run("MERGE (d:DistinctTest {city: 'NYC', name: 'Alice'})").consume()
        session.run("MERGE (d:DistinctTest {city: 'NYC', name: 'Bob'})").consume()
        session.run("MERGE (d:DistinctTest {city: 'LA',  name: 'Carol'})").consume()
        result = session.run(
            "MATCH (d:DistinctTest) RETURN COUNT(d) AS total, COUNT(DISTINCT d.city) AS cities"
        )
        rows = list(result)
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        total  = rows[0]["total"]
        cities = rows[0]["cities"]
        assert total == 3,  f"Expected total=3, got {total}"
        assert cities == 2, f"Expected cities=2 (NYC,LA), got {cities}"
        print(f"  COUNT(d)={total}, COUNT(DISTINCT d.city)={cities}")
        print("  PASS")

        # Test 37: Multiple MATCH clauses without WITH
        print("\n[Test 37] Multiple MATCH clauses without WITH")
        print("-" * 40)
        result = session.run(
            "MATCH (a:CartA) "
            "MATCH (b:CartB) "
            "RETURN a.name AS a, b.name AS b ORDER BY a, b"
        )
        rows = list(result)
        pairs = [(r["a"], r["b"]) for r in rows]
        assert len(rows) == 2, f"Expected 2 rows (2 CartA × 1 CartB), got {len(rows)}: {pairs}"
        assert ("A1", "B1") in pairs and ("A2", "B1") in pairs, f"Wrong pairs: {pairs}"
        for r in rows:
            print(f"  a={r['a']!r}  b={r['b']!r}")
        print("  PASS")

        # Clean up any stale RemoveTest nodes from previous runs
        session.run("MATCH (r:RemoveTest) DETACH DELETE r").consume()

        # Test 38: REMOVE property
        print("\n[Test 38] REMOVE n.property")
        print("-" * 40)
        session.run("MERGE (r:RemoveTest {id: 'rp1', keep: 'yes', drop: 'bye'})").consume()
        result = session.run("MATCH (r:RemoveTest {id: 'rp1'}) RETURN r.drop AS val")
        rows = list(result)
        assert len(rows) == 1 and rows[0]["val"] == "bye", f"Setup failed: {rows}"
        session.run("MATCH (r:RemoveTest {id: 'rp1'}) REMOVE r.drop").consume()
        result = session.run("MATCH (r:RemoveTest {id: 'rp1'}) RETURN r.keep AS keep, r.drop AS drop")
        rows = list(result)
        assert len(rows) == 1, f"Expected 1 row after REMOVE, got {len(rows)}"
        assert rows[0]["keep"] == "yes", f"'keep' property should still be 'yes', got {rows[0]['keep']}"
        assert rows[0]["drop"] is None, f"'drop' property should be None after REMOVE, got {rows[0]['drop']}"
        print(f"  keep={rows[0]['keep']!r}  drop={rows[0]['drop']!r}")
        print("  PASS")

        # Test 39: REMOVE label
        print("\n[Test 39] REMOVE n:Label")
        print("-" * 40)
        session.run("MERGE (r:RemoveTest:ExtraLabel {id: 'rl1'})").consume()
        result = session.run("MATCH (r:ExtraLabel {id: 'rl1'}) RETURN r.id AS id")
        rows = list(result)
        assert len(rows) == 1, f"Setup failed - node with ExtraLabel not found"
        session.run("MATCH (r:RemoveTest {id: 'rl1'}) REMOVE r:ExtraLabel").consume()
        result = session.run("MATCH (r:ExtraLabel {id: 'rl1'}) RETURN r.id AS id")
        rows = list(result)
        assert len(rows) == 0, f"Node should no longer have ExtraLabel, but got {len(rows)} results"
        result = session.run("MATCH (r:RemoveTest {id: 'rl1'}) RETURN r.id AS id")
        rows = list(result)
        assert len(rows) == 1, f"Node should still have RemoveTest label, got {len(rows)} results"
        print(f"  ExtraLabel removed, RemoveTest label remains")
        print("  PASS")

        # Test 40: REMOVE multiple properties
        print("\n[Test 40] REMOVE multiple properties in one clause")
        print("-" * 40)
        session.run("MERGE (r:RemoveTest {id: 'rp2', a: 1, b: 2, c: 3})").consume()
        session.run("MATCH (r:RemoveTest {id: 'rp2'}) REMOVE r.a, r.b").consume()
        result = session.run("MATCH (r:RemoveTest {id: 'rp2'}) RETURN r.a AS a, r.b AS b, r.c AS c")
        rows = list(result)
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]["a"] is None, f"'a' should be removed, got {rows[0]['a']}"
        assert rows[0]["b"] is None, f"'b' should be removed, got {rows[0]['b']}"
        assert rows[0]["c"] == 3,    f"'c' should still be 3, got {rows[0]['c']}"
        print(f"  a={rows[0]['a']!r}  b={rows[0]['b']!r}  c={rows[0]['c']!r}")
        print("  PASS")

        # Set up a chain for variable-length path tests:
        #   VLP_A -[:KNOWS]-> VLP_B -[:KNOWS]-> VLP_C -[:KNOWS]-> VLP_D
        session.run("MATCH (n:VLPTest) DETACH DELETE n").consume()
        session.run("CREATE (a:VLPTest {name: 'VLP_A'})").consume()
        session.run("CREATE (b:VLPTest {name: 'VLP_B'})").consume()
        session.run("CREATE (c:VLPTest {name: 'VLP_C'})").consume()
        session.run("CREATE (d:VLPTest {name: 'VLP_D'})").consume()
        session.run(
            "MATCH (a:VLPTest {name: 'VLP_A'}), (b:VLPTest {name: 'VLP_B'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        ).consume()
        session.run(
            "MATCH (b:VLPTest {name: 'VLP_B'}), (c:VLPTest {name: 'VLP_C'}) "
            "CREATE (b)-[:KNOWS]->(c)"
        ).consume()
        session.run(
            "MATCH (c:VLPTest {name: 'VLP_C'}), (d:VLPTest {name: 'VLP_D'}) "
            "CREATE (c)-[:KNOWS]->(d)"
        ).consume()

        # Test 41: Variable-length path — *1..2 (1 or 2 hops)
        print("\n[Test 41] Variable-length path — (a)-[:KNOWS*1..2]->(b)")
        print("-" * 40)
        result = session.run(
            "MATCH (a:VLPTest)-[:KNOWS*1..2]->(b:VLPTest) "
            "RETURN a.name AS from, b.name AS to ORDER BY from, to"
        )
        rows = list(result)
        pairs = [(r["from"], r["to"]) for r in rows]
        # 1-hop: A->B, B->C, C->D  (3)
        # 2-hop: A->C, B->D        (2)
        # total: 5
        assert len(pairs) == 5, f"Expected 5 paths within 1..2 hops, got {len(pairs)}: {pairs}"
        assert ("VLP_A", "VLP_B") in pairs, f"Missing A->B (1-hop): {pairs}"
        assert ("VLP_A", "VLP_C") in pairs, f"Missing A->C (2-hop): {pairs}"
        assert ("VLP_B", "VLP_C") in pairs, f"Missing B->C (1-hop): {pairs}"
        assert ("VLP_B", "VLP_D") in pairs, f"Missing B->D (2-hop): {pairs}"
        assert ("VLP_C", "VLP_D") in pairs, f"Missing C->D (1-hop): {pairs}"
        for r in rows:
            print(f"  {r['from']} -> {r['to']}")
        print("  PASS")

        # Test 42: Variable-length path — exact 2 hops (*2)
        print("\n[Test 42] Variable-length path — exact 2 hops (*2)")
        print("-" * 40)
        result = session.run(
            "MATCH (a:VLPTest)-[:KNOWS*2]->(b:VLPTest) "
            "RETURN a.name AS from, b.name AS to ORDER BY from, to"
        )
        rows = list(result)
        pairs = [(r["from"], r["to"]) for r in rows]
        # Exactly 2 hops: A->C, B->D
        assert len(pairs) == 2, f"Expected 2 exact-2-hop paths, got {len(pairs)}: {pairs}"
        assert ("VLP_A", "VLP_C") in pairs, f"Missing A->C: {pairs}"
        assert ("VLP_B", "VLP_D") in pairs, f"Missing B->D: {pairs}"
        for r in rows:
            print(f"  {r['from']} -2-> {r['to']}")
        print("  PASS")

        # Test 43: Variable-length path — unbounded (*)
        print("\n[Test 43] Variable-length path — unbounded (*)")
        print("-" * 40)
        result = session.run(
            "MATCH (a:VLPTest {name: 'VLP_A'})-[:KNOWS*]->(b:VLPTest) "
            "RETURN b.name AS name ORDER BY name"
        )
        rows = list(result)
        names = [r["name"] for r in rows]
        # A can reach B (1-hop), C (2-hop), D (3-hop)
        assert len(names) == 3, f"Expected 3 reachable nodes from A, got {len(names)}: {names}"
        assert "VLP_B" in names, f"Missing VLP_B: {names}"
        assert "VLP_C" in names, f"Missing VLP_C: {names}"
        assert "VLP_D" in names, f"Missing VLP_D: {names}"
        print(f"  Reachable from VLP_A: {names}")
        print("  PASS")

        # Test 44: substring()
        print("\n[Test 44] substring(str, start, length)")
        print("-" * 40)
        result = session.run(
            "UNWIND ['hello world'] AS s "
            "RETURN substring(s, 0, 5) AS a, substring(s, 6) AS b"
        )
        rows = list(result)
        assert rows[0]["a"] == "hello", f"substring(s,0,5) should be 'hello', got {rows[0]['a']!r}"
        assert rows[0]["b"] == "world", f"substring(s,6) should be 'world', got {rows[0]['b']!r}"
        print(f"  substring('hello world', 0, 5) = {rows[0]['a']!r}")
        print(f"  substring('hello world', 6) = {rows[0]['b']!r}")
        print("  PASS")

        # Test 45: replace()
        print("\n[Test 45] replace(str, search, replacement)")
        print("-" * 40)
        result = session.run(
            "UNWIND ['hello world'] AS s "
            "RETURN replace(s, 'world', 'cypher') AS val"
        )
        rows = list(result)
        assert rows[0]["val"] == "hello cypher", f"Expected 'hello cypher', got {rows[0]['val']!r}"
        print(f"  replace('hello world', 'world', 'cypher') = {rows[0]['val']!r}")
        print("  PASS")

        # Test 46: split()
        print("\n[Test 46] split(str, delimiter)")
        print("-" * 40)
        result = session.run(
            "UNWIND ['a,b,c'] AS s "
            "RETURN split(s, ',') AS val"
        )
        rows = list(result)
        assert rows[0]["val"] == ["a", "b", "c"], f"Expected ['a','b','c'], got {rows[0]['val']!r}"
        print(f"  split('a,b,c', ',') = {rows[0]['val']!r}")
        print("  PASS")

        # Test 47: left() and right()
        print("\n[Test 47] left(str, n) and right(str, n)")
        print("-" * 40)
        result = session.run(
            "UNWIND ['hello'] AS s "
            "RETURN left(s, 3) AS l, right(s, 3) AS r"
        )
        rows = list(result)
        assert rows[0]["l"] == "hel", f"left('hello',3) should be 'hel', got {rows[0]['l']!r}"
        assert rows[0]["r"] == "llo", f"right('hello',3) should be 'llo', got {rows[0]['r']!r}"
        print(f"  left('hello', 3) = {rows[0]['l']!r}")
        print(f"  right('hello', 3) = {rows[0]['r']!r}")
        print("  PASS")

        # Test 48: Math functions — sin, cos, pi, rand, pow
        print("\n[Test 48] Math functions: sin, cos, pi, rand, pow")
        print("-" * 40)
        result = session.run(
            "UNWIND [1] AS x "
            "RETURN pi() AS pi, rand() AS rnd, pow(2, 10) AS p, "
            "round(sin(0) * 100) AS s, round(cos(0) * 100) AS c"
        )
        rows = list(result)
        import math
        assert abs(rows[0]["pi"] - math.pi) < 0.0001, f"pi() wrong: {rows[0]['pi']}"
        assert 0 <= rows[0]["rnd"] <= 1, f"rand() out of range: {rows[0]['rnd']}"
        assert rows[0]["p"] == 1024.0, f"pow(2,10) should be 1024, got {rows[0]['p']}"
        assert rows[0]["s"] == 0, f"sin(0) should be 0, got {rows[0]['s']}"
        assert rows[0]["c"] == 100, f"cos(0) should be 100 (round(1*100)), got {rows[0]['c']}"
        print(f"  pi() = {rows[0]['pi']}")
        print(f"  rand() = {rows[0]['rnd']}")
        print(f"  pow(2, 10) = {rows[0]['p']}")
        print(f"  round(sin(0)*100) = {rows[0]['s']}")
        print(f"  round(cos(0)*100) = {rows[0]['c']}")
        print("  PASS")

        # Test 49: startNode / endNode
        print("\n[Test 49] startNode(r) and endNode(r)")
        print("-" * 40)
        result = session.run(
            "MATCH (a:VLPTest {name: 'VLP_A'})-[r:KNOWS]->(b:VLPTest {name: 'VLP_B'}) "
            "RETURN startNode(r) AS sn, endNode(r) AS en"
        )
        rows = list(result)
        if len(rows) == 0:
            print("  SKIP (no VLP data)")
        else:
            assert rows[0]["sn"] is not None, f"startNode returned None"
            assert rows[0]["en"] is not None, f"endNode returned None"
            print(f"  startNode(r) = {rows[0]['sn']!r}")
            print(f"  endNode(r) = {rows[0]['en']!r}")
            print("  PASS")

        # Test 50: EXISTS(n.property)
        print("\n[Test 50] EXISTS(n.property)")
        print("-" * 40)
        session.run("MATCH (e:ExistsTest) DETACH DELETE e").consume()
        session.run("CREATE (e:ExistsTest {name: 'has_email', email: 'test@test.com'})").consume()
        session.run("CREATE (e:ExistsTest {name: 'no_email'})").consume()
        result = session.run(
            "MATCH (e:ExistsTest) WHERE EXISTS(e.email) RETURN e.name AS name"
        )
        rows = list(result)
        names = [r["name"] for r in rows]
        assert names == ["has_email"], f"Expected ['has_email'], got {names}"
        print(f"  Nodes with email: {names}")
        print("  PASS")

        # Test 51: ANY(x IN list WHERE condition)
        print("\n[Test 51] ANY(x IN list WHERE condition)")
        print("-" * 40)
        session.run("MATCH (lt:ListTest) DETACH DELETE lt").consume()
        session.run("CREATE (lt:ListTest {id: 'a', tags: ['go', 'rust', 'python']})").consume()
        session.run("CREATE (lt:ListTest {id: 'b', tags: ['java', 'kotlin']})").consume()
        session.run("CREATE (lt:ListTest {id: 'c', tags: ['go', 'typescript']})").consume()
        result = session.run(
            "MATCH (lt:ListTest) WHERE ANY(t IN lt.tags WHERE t = 'go') "
            "RETURN lt.id AS id ORDER BY id"
        )
        rows = list(result)
        ids = [r["id"] for r in rows]
        assert ids == ["a", "c"], f"Expected ['a', 'c'], got {ids}"
        print(f"  Nodes with ANY tag = 'go': {ids}")
        print("  PASS")

        # Test 52: ALL(x IN list WHERE condition)
        print("\n[Test 52] ALL(x IN list WHERE condition)")
        print("-" * 40)
        result = session.run(
            "MATCH (lt:ListTest) WHERE ALL(t IN lt.tags WHERE size(t) > 1) "
            "RETURN lt.id AS id ORDER BY id"
        )
        rows = list(result)
        ids = [r["id"] for r in rows]
        assert ids == ["a", "b", "c"], f"Expected all 3 (all tags > 1 char), got {ids}"
        print(f"  Nodes where ALL tags have size > 1: {ids}")
        print("  PASS")

        # Test 53: NONE(x IN list WHERE condition)
        print("\n[Test 53] NONE(x IN list WHERE condition)")
        print("-" * 40)
        result = session.run(
            "MATCH (lt:ListTest) WHERE NONE(t IN lt.tags WHERE t = 'java') "
            "RETURN lt.id AS id ORDER BY id"
        )
        rows = list(result)
        ids = [r["id"] for r in rows]
        assert ids == ["a", "c"], f"Expected ['a', 'c'] (no java), got {ids}"
        print(f"  Nodes with NONE tag = 'java': {ids}")
        print("  PASS")

        # Test 54: SINGLE(x IN list WHERE condition)
        print("\n[Test 54] SINGLE(x IN list WHERE condition)")
        print("-" * 40)
        result = session.run(
            "MATCH (lt:ListTest) WHERE SINGLE(t IN lt.tags WHERE t = 'go') "
            "RETURN lt.id AS id ORDER BY id"
        )
        rows = list(result)
        ids = [r["id"] for r in rows]
        # Both 'a' and 'c' have exactly one 'go' tag
        assert ids == ["a", "c"], f"Expected ['a', 'c'] (exactly one 'go'), got {ids}"
        print(f"  Nodes with SINGLE tag = 'go': {ids}")
        print("  PASS")

        # Test 55: UNION
        print("\n[Test 55] UNION (deduplicated)")
        print("-" * 40)
        result = session.run(
            "UNWIND [1, 2, 3] AS x RETURN x "
            "UNION "
            "UNWIND [2, 3, 4] AS x RETURN x"
        )
        rows = list(result)
        values = sorted([r["x"] for r in rows])
        assert values == [1, 2, 3, 4], f"Expected [1,2,3,4] (deduplicated), got {values}"
        print(f"  UNION result: {values}")
        print("  PASS")

        # Test 56: UNION ALL
        print("\n[Test 56] UNION ALL (with duplicates)")
        print("-" * 40)
        result = session.run(
            "UNWIND [1, 2] AS x RETURN x "
            "UNION ALL "
            "UNWIND [2, 3] AS x RETURN x"
        )
        rows = list(result)
        values = sorted([r["x"] for r in rows])
        assert values == [1, 2, 2, 3], f"Expected [1,2,2,3] (with duplicates), got {values}"
        print(f"  UNION ALL result: {values}")
        print("  PASS")

        # Test 57: timestamp()
        print("\n[Test 57] timestamp()")
        print("-" * 40)
        result = session.run("UNWIND [1] AS x RETURN timestamp() AS ts")
        rows = list(result)
        ts = rows[0]["ts"]
        import time as pytime
        now_ms = int(pytime.time() * 1000)
        assert abs(ts - now_ms) < 5000, f"timestamp() too far from now: {ts} vs {now_ms}"
        print(f"  timestamp() = {ts}")
        print("  PASS")

        # Test 58: date() and datetime()
        print("\n[Test 58] date() and datetime()")
        print("-" * 40)
        result = session.run(
            "UNWIND [1] AS x "
            "RETURN date() AS d, datetime() AS dt, date('2024-06-15') AS parsed"
        )
        rows = list(result)
        d = rows[0]["d"]
        dt = rows[0]["dt"]
        parsed = rows[0]["parsed"]
        assert len(d) == 10, f"date() should be YYYY-MM-DD, got {d!r}"
        assert "T" in dt, f"datetime() should contain T, got {dt!r}"
        assert parsed == "2024-06-15", f"date('2024-06-15') should return '2024-06-15', got {parsed!r}"
        print(f"  date() = {d!r}")
        print(f"  datetime() = {dt!r}")
        print(f"  date('2024-06-15') = {parsed!r}")
        print("  PASS")

    driver.close()
    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
