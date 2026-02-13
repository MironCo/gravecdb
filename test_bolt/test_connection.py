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

    driver.close()
    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
