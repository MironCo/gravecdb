#!/usr/bin/env python3
"""
Comprehensive test script for GravecDB Bolt protocol implementation.
Tests CRUD operations, relationships, transactions, WHERE clauses, and query patterns.

Requires: pip install neo4j
"""

from neo4j import GraphDatabase
import sys


def run_test(session, name, query, expect_results=True):
    """Helper to run a test and print results."""
    print(f"\n[{name}]")
    print(f"  Query: {query}")
    print("-" * 50)
    try:
        result = session.run(query)
        records = list(result)
        if expect_results:
            if records:
                for record in records:
                    print(f"  → {dict(record)}")
            else:
                print("  → (no results)")
        else:
            print("  → OK")
        return records
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


def test_basic_crud(session):
    """Test basic CRUD operations."""
    print("\n" + "=" * 60)
    print("BASIC CRUD OPERATIONS")
    print("=" * 60)

    # Create nodes
    run_test(session, "CREATE Person",
             "CREATE (p:Person {name: 'Alice', age: 30, role: 'Engineer'})",
             expect_results=False)

    run_test(session, "CREATE Person 2",
             "CREATE (p:Person {name: 'Bob', age: 25, role: 'Designer'})",
             expect_results=False)

    run_test(session, "CREATE Company",
             "CREATE (c:Company {name: 'TechCorp', industry: 'Software'})",
             expect_results=False)

    # Read nodes
    run_test(session, "MATCH all Persons",
             "MATCH (p:Person) RETURN p")

    run_test(session, "MATCH with property filter",
             "MATCH (p:Person {name: 'Alice'}) RETURN p")

    run_test(session, "MATCH with property access",
             "MATCH (p:Person) RETURN p.name, p.age")


def test_where_clause(session):
    """Test WHERE clause filtering."""
    print("\n" + "=" * 60)
    print("WHERE CLAUSE FILTERING")
    print("=" * 60)

    run_test(session, "WHERE equals",
             "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name, p.age")

    run_test(session, "WHERE greater than",
             "MATCH (p:Person) WHERE p.age > 26 RETURN p.name, p.age")

    run_test(session, "WHERE less than or equal",
             "MATCH (p:Person) WHERE p.age <= 25 RETURN p.name, p.role")


def test_relationships(session):
    """Test relationship creation and traversal."""
    print("\n" + "=" * 60)
    print("RELATIONSHIPS")
    print("=" * 60)

    # Create relationships using MATCH...CREATE
    run_test(session, "Create WORKS_AT relationship",
             """MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'})
                CREATE (p)-[r:WORKS_AT {since: 2020}]->(c)""",
             expect_results=False)

    run_test(session, "Create KNOWS relationship",
             """MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
                CREATE (a)-[r:KNOWS {since: 2019}]->(b)""",
             expect_results=False)

    # Query relationships
    run_test(session, "MATCH relationship pattern",
             "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p.name, c.name")

    run_test(session, "MATCH KNOWS relationship",
             "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r")


def test_set_operations(session):
    """Test SET (update) operations."""
    print("\n" + "=" * 60)
    print("SET OPERATIONS")
    print("=" * 60)

    run_test(session, "SET property",
             "MATCH (p:Person {name: 'Alice'}) SET p.level = 'Senior' RETURN p",
             expect_results=False)

    run_test(session, "Verify SET",
             "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.level")


def test_transactions(session, driver):
    """Test transaction support (BEGIN, COMMIT, ROLLBACK)."""
    print("\n" + "=" * 60)
    print("TRANSACTIONS")
    print("=" * 60)

    # Test commit
    print("\n[Transaction COMMIT test]")
    print("-" * 50)
    with driver.session() as tx_session:
        tx = tx_session.begin_transaction()
        try:
            tx.run("CREATE (p:Person {name: 'TxPerson', age: 99})")
            tx.commit()
            print("  → Transaction committed")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    # Verify committed data
    run_test(session, "Verify commit",
             "MATCH (p:Person {name: 'TxPerson'}) RETURN p.name, p.age")

    # Test rollback
    print("\n[Transaction ROLLBACK test]")
    print("-" * 50)
    with driver.session() as tx_session:
        tx = tx_session.begin_transaction()
        try:
            tx.run("CREATE (p:Person {name: 'RollbackPerson', age: 1})")
            tx.rollback()
            print("  → Transaction rolled back")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    # Verify rollback (should not find)
    records = run_test(session, "Verify rollback (should be empty)",
                       "MATCH (p:Person {name: 'RollbackPerson'}) RETURN p")
    if records is not None and len(records) == 0:
        print("  ✓ Rollback successful - node not found")


def test_delete_operations(session):
    """Test DELETE operations."""
    print("\n" + "=" * 60)
    print("DELETE OPERATIONS")
    print("=" * 60)

    # Create a node to delete
    run_test(session, "Create node for deletion",
             "CREATE (p:Person {name: 'ToDelete', age: 0})",
             expect_results=False)

    # Verify it exists
    run_test(session, "Verify node exists",
             "MATCH (p:Person {name: 'ToDelete'}) RETURN p")

    # Delete it
    run_test(session, "DELETE node",
             "MATCH (p:Person {name: 'ToDelete'}) DELETE p",
             expect_results=False)

    # Verify deletion
    records = run_test(session, "Verify deletion (should be empty)",
                       "MATCH (p:Person {name: 'ToDelete'}) RETURN p")
    if records is not None and len(records) == 0:
        print("  ✓ Deletion successful - node not found")


def test_complex_patterns(session):
    """Test complex query patterns."""
    print("\n" + "=" * 60)
    print("COMPLEX PATTERNS")
    print("=" * 60)

    # Multi-hop traversal
    run_test(session, "Multi-node pattern",
             """MATCH (p:Person), (c:Company)
                WHERE p.role = 'Engineer'
                RETURN p.name, c.name""")

    # Return multiple items
    run_test(session, "Return all Persons with all properties",
             "MATCH (p:Person) RETURN p")


def test_merge_operations(session):
    """Test MERGE operations."""
    print("\n" + "=" * 60)
    print("MERGE OPERATIONS")
    print("=" * 60)

    # MERGE should create if not exists
    run_test(session, "MERGE create new node",
             "MERGE (p:Person {name: 'MergeTest'})",
             expect_results=False)

    # Verify it was created
    records = run_test(session, "Verify MERGE created node",
                       "MATCH (p:Person {name: 'MergeTest'}) RETURN p.name")
    if records and len(records) == 1:
        print("  ✓ MERGE created node successfully")

    # MERGE again should not create duplicate
    run_test(session, "MERGE existing node (should not duplicate)",
             "MERGE (p:Person {name: 'MergeTest'})",
             expect_results=False)

    # Verify still only one
    records = run_test(session, "Verify no duplicate",
                       "MATCH (p:Person {name: 'MergeTest'}) RETURN p.name")
    if records and len(records) == 1:
        print("  ✓ MERGE did not create duplicate")
    elif records and len(records) > 1:
        print(f"  ✗ MERGE created duplicate! Found {len(records)} nodes")

    # Cleanup
    run_test(session, "Delete MergeTest node",
             "MATCH (p:Person {name: 'MergeTest'}) DELETE p",
             expect_results=False)


def test_remove_operations(session):
    """Test REMOVE operations."""
    print("\n" + "=" * 60)
    print("REMOVE OPERATIONS")
    print("=" * 60)

    # Create a node with properties to remove
    run_test(session, "Create node with properties",
             "CREATE (p:Person {name: 'RemoveTest', age: 30, temp: 'toremove'})",
             expect_results=False)

    # Verify properties exist
    records = run_test(session, "Verify properties exist",
                       "MATCH (p:Person {name: 'RemoveTest'}) RETURN p.name, p.age, p.temp")
    if records and len(records) == 1:
        print("  ✓ Node created with properties")

    # Remove a property
    run_test(session, "REMOVE property",
             "MATCH (p:Person {name: 'RemoveTest'}) REMOVE p.temp",
             expect_results=False)

    # Verify property was removed
    records = run_test(session, "Verify property removed",
                       "MATCH (p:Person {name: 'RemoveTest'}) RETURN p.name, p.age, p.temp")
    if records and len(records) == 1:
        rec = dict(records[0])
        if rec.get('p.temp') is None:
            print("  ✓ Property successfully removed")
        else:
            print(f"  ✗ Property still exists: {rec.get('p.temp')}")

    # Cleanup
    run_test(session, "Delete RemoveTest node",
             "MATCH (p:Person {name: 'RemoveTest'}) DELETE p",
             expect_results=False)


def test_unwind_operations(session):
    """Test UNWIND operations."""
    print("\n" + "=" * 60)
    print("UNWIND OPERATIONS")
    print("=" * 60)

    # UNWIND a list
    records = run_test(session, "UNWIND list",
                       "UNWIND [1, 2, 3] AS x RETURN x")
    if records and len(records) == 3:
        print("  ✓ UNWIND expanded list into 3 rows")
    else:
        print(f"  ✗ Expected 3 rows, got {len(records) if records else 0}")

    # UNWIND with strings
    records = run_test(session, "UNWIND string list",
                       "UNWIND ['a', 'b', 'c'] AS letter RETURN letter")
    if records and len(records) == 3:
        print("  ✓ UNWIND works with strings")


def test_parameter_binding(session):
    """Test query parameter binding ($param style)."""
    print("\n" + "=" * 60)
    print("PARAMETER BINDING")
    print("=" * 60)

    # Create a test node
    run_test(session, "Create node for param test",
             "CREATE (p:ParamTest {name: 'ParamPerson', age: 42})",
             expect_results=False)

    # Test string parameter in node pattern {name: $name}
    print("\n[Query with parameter in node pattern]")
    print("-" * 50)
    try:
        result = session.run(
            "MATCH (p:ParamTest {name: $name}) RETURN p.name, p.age",
            name="ParamPerson"
        )
        records = list(result)
        if records:
            for record in records:
                print(f"  → {dict(record)}")
            if dict(records[0]).get('p.name') == 'ParamPerson':
                print("  ✓ Parameter in node pattern works")
        else:
            print("  ✗ No results returned")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test string parameter in WHERE clause
    print("\n[Query with parameter in WHERE clause]")
    print("-" * 50)
    try:
        result = session.run(
            "MATCH (p:ParamTest) WHERE p.name = $name RETURN p.name, p.age",
            name="ParamPerson"
        )
        records = list(result)
        if records:
            for record in records:
                print(f"  → {dict(record)}")
            if dict(records[0]).get('p.name') == 'ParamPerson':
                print("  ✓ String parameter binding works")
        else:
            print("  ✗ No results returned")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test numeric parameter
    print("\n[Query with numeric parameter]")
    print("-" * 50)
    try:
        result = session.run(
            "MATCH (p:ParamTest) WHERE p.age = $age RETURN p.name, p.age",
            age=42
        )
        records = list(result)
        if records:
            for record in records:
                print(f"  → {dict(record)}")
            if dict(records[0]).get('p.age') == 42:
                print("  ✓ Numeric parameter binding works")
        else:
            print("  ✗ No results returned")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test multiple parameters
    print("\n[Query with multiple parameters]")
    print("-" * 50)
    try:
        result = session.run(
            "MATCH (p:ParamTest) WHERE p.name = $name AND p.age = $age RETURN p",
            name="ParamPerson",
            age=42
        )
        records = list(result)
        if records:
            for record in records:
                print(f"  → {dict(record)}")
            print("  ✓ Multiple parameter binding works")
        else:
            print("  ✗ No results returned")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test CREATE with parameters
    print("\n[CREATE with parameters]")
    print("-" * 50)
    try:
        session.run(
            "CREATE (p:ParamTest {name: $name, score: $score})",
            name="ParamCreated",
            score=99.5
        )
        result = session.run("MATCH (p:ParamTest {name: 'ParamCreated'}) RETURN p.name, p.score")
        records = list(result)
        if records:
            rec = dict(records[0])
            print(f"  → {rec}")
            if rec.get('p.name') == 'ParamCreated' and rec.get('p.score') == 99.5:
                print("  ✓ CREATE with parameters works")
        else:
            print("  ✗ Node not created")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Cleanup
    run_test(session, "Delete ParamTest nodes",
             "MATCH (p:ParamTest) DELETE p",
             expect_results=False)


def test_shortest_path(session):
    """Test shortestPath() and allShortestPaths() functions."""
    print("\n" + "=" * 60)
    print("SHORTEST PATH FUNCTIONS")
    print("=" * 60)

    # Create a graph for path testing:
    #   A --KNOWS--> B --KNOWS--> C
    #   |                         ^
    #   +-------KNOWS-------------+
    # So there are two paths from A to C: A->B->C (length 2) and A->C (length 1)

    run_test(session, "Create path node A",
             "CREATE (a:PathNode {name: 'A'})",
             expect_results=False)
    run_test(session, "Create path node B",
             "CREATE (b:PathNode {name: 'B'})",
             expect_results=False)
    run_test(session, "Create path node C",
             "CREATE (c:PathNode {name: 'C'})",
             expect_results=False)

    # Create relationships
    run_test(session, "Create A->B",
             "MATCH (a:PathNode {name: 'A'}), (b:PathNode {name: 'B'}) CREATE (a)-[:KNOWS]->(b)",
             expect_results=False)
    run_test(session, "Create B->C",
             "MATCH (b:PathNode {name: 'B'}), (c:PathNode {name: 'C'}) CREATE (b)-[:KNOWS]->(c)",
             expect_results=False)
    run_test(session, "Create A->C (direct)",
             "MATCH (a:PathNode {name: 'A'}), (c:PathNode {name: 'C'}) CREATE (a)-[:KNOWS]->(c)",
             expect_results=False)

    # Test shortestPath
    print("\n[shortestPath from A to C]")
    print("-" * 50)
    try:
        result = session.run(
            "MATCH p = shortestPath((a:PathNode {name: 'A'})-[:KNOWS*]->(c:PathNode {name: 'C'})) RETURN p"
        )
        records = list(result)
        if records:
            for record in records:
                path = record['p']
                if hasattr(path, 'nodes'):
                    print(f"  → Path with {len(path.nodes)} nodes, {len(path.relationships)} relationships")
                    node_names = [n.get('name', 'unknown') for n in path.nodes]
                    print(f"  → Node sequence: {' -> '.join(node_names)}")
                    if len(path.relationships) == 1:
                        print("  ✓ shortestPath found direct path A->C (length 1)")
                else:
                    print(f"  → {path}")
        else:
            print("  → No path found (this may be expected if shortestPath not fully implemented)")
    except Exception as e:
        print(f"  Note: shortestPath query returned: {e}")

    # Test allShortestPaths
    print("\n[allShortestPaths from A to C]")
    print("-" * 50)
    try:
        result = session.run(
            "MATCH p = allShortestPaths((a:PathNode {name: 'A'})-[:KNOWS*]->(c:PathNode {name: 'C'})) RETURN p"
        )
        records = list(result)
        if records:
            print(f"  → Found {len(records)} path(s)")
            for i, record in enumerate(records):
                path = record['p']
                if hasattr(path, 'nodes'):
                    node_names = [n.get('name', 'unknown') for n in path.nodes]
                    print(f"  → Path {i+1}: {' -> '.join(node_names)}")
        else:
            print("  → No paths found")
    except Exception as e:
        print(f"  Note: allShortestPaths query returned: {e}")

    # Cleanup
    run_test(session, "Delete PathNode nodes",
             "MATCH (n:PathNode) DETACH DELETE n",
             expect_results=False)


def test_label_removal(session):
    """Test REMOVE n:Label syntax for label removal."""
    print("\n" + "=" * 60)
    print("LABEL REMOVAL")
    print("=" * 60)

    # Create a node with multiple labels
    run_test(session, "Create node with multiple labels",
             "CREATE (p:Person:Employee:Manager {name: 'LabelTest'})",
             expect_results=False)

    # Verify initial labels
    print("\n[Verify initial labels]")
    print("-" * 50)
    try:
        result = session.run("MATCH (p:Person {name: 'LabelTest'}) RETURN p")
        records = list(result)
        if records:
            node = records[0]['p']
            labels = list(node.labels) if hasattr(node, 'labels') else []
            print(f"  → Labels: {labels}")
            if 'Manager' in labels:
                print("  ✓ Node has Manager label")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Remove a label using REMOVE n:Label syntax
    run_test(session, "Remove Manager label",
             "MATCH (p:Person {name: 'LabelTest'}) REMOVE p:Manager",
             expect_results=False)

    # Verify label was removed
    print("\n[Verify Manager label removed]")
    print("-" * 50)
    try:
        result = session.run("MATCH (p:Person {name: 'LabelTest'}) RETURN p")
        records = list(result)
        if records:
            node = records[0]['p']
            labels = list(node.labels) if hasattr(node, 'labels') else []
            print(f"  → Labels after removal: {labels}")
            if 'Manager' not in labels:
                print("  ✓ Manager label successfully removed")
            else:
                print("  ✗ Manager label still present")
            if 'Person' in labels and 'Employee' in labels:
                print("  ✓ Other labels preserved (Person, Employee)")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test that node is no longer matched by removed label
    print("\n[Verify node not matched by removed label]")
    print("-" * 50)
    try:
        result = session.run("MATCH (p:Manager {name: 'LabelTest'}) RETURN p")
        records = list(result)
        if not records:
            print("  ✓ Node no longer matched by :Manager label")
        else:
            print("  ✗ Node still matched by :Manager label")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Cleanup
    run_test(session, "Delete LabelTest node",
             "MATCH (p:Person {name: 'LabelTest'}) DETACH DELETE p",
             expect_results=False)


def test_variable_length_paths(session):
    """Test variable-length path patterns [*1..3]."""
    print("\n" + "=" * 60)
    print("VARIABLE-LENGTH PATHS")
    print("=" * 60)

    # Create a chain of nodes: A -> B -> C -> D
    run_test(session, "Create chain node A",
             "CREATE (a:Chain {name: 'A', level: 1})",
             expect_results=False)
    run_test(session, "Create chain node B",
             "CREATE (b:Chain {name: 'B', level: 2})",
             expect_results=False)
    run_test(session, "Create chain node C",
             "CREATE (c:Chain {name: 'C', level: 3})",
             expect_results=False)
    run_test(session, "Create chain node D",
             "CREATE (d:Chain {name: 'D', level: 4})",
             expect_results=False)

    # Create relationships
    run_test(session, "Create A->B",
             "MATCH (a:Chain {name: 'A'}), (b:Chain {name: 'B'}) CREATE (a)-[:NEXT]->(b)",
             expect_results=False)
    run_test(session, "Create B->C",
             "MATCH (b:Chain {name: 'B'}), (c:Chain {name: 'C'}) CREATE (b)-[:NEXT]->(c)",
             expect_results=False)
    run_test(session, "Create C->D",
             "MATCH (c:Chain {name: 'C'}), (d:Chain {name: 'D'}) CREATE (c)-[:NEXT]->(d)",
             expect_results=False)

    # Test variable-length path with exact range
    records = run_test(session, "Variable-length path [*1..2] from A",
                       "MATCH (a:Chain {name: 'A'})-[*1..2]->(x:Chain) RETURN x.name")
    if records:
        names = [dict(r)['x.name'] for r in records]
        print(f"  Found nodes: {names}")
        if 'B' in names and 'C' in names:
            print("  ✓ Found nodes 1-2 hops away (B, C)")
        if 'D' in names:
            print("  ✗ Should not find D (3 hops away)")

    # Test variable-length path with larger range
    records = run_test(session, "Variable-length path [*1..3] from A",
                       "MATCH (a:Chain {name: 'A'})-[*1..3]->(x:Chain) RETURN x.name")
    if records:
        names = [dict(r)['x.name'] for r in records]
        print(f"  Found nodes: {names}")
        if 'B' in names and 'C' in names and 'D' in names:
            print("  ✓ Found all nodes 1-3 hops away (B, C, D)")

    # Test with specific relationship type
    records = run_test(session, "Variable-length path [*1..3] with type NEXT",
                       "MATCH (a:Chain {name: 'A'})-[:NEXT*1..3]->(x:Chain) RETURN x.name")
    if records:
        names = [dict(r)['x.name'] for r in records]
        print(f"  Found nodes: {names}")
        if len(names) == 3:
            print("  ✓ Found all 3 reachable nodes via NEXT relationship")

    # Test returning the path variable (Bolt Path data type)
    records = run_test(session, "Return path variable p",
                       "MATCH (a:Chain {name: 'A'})-[p:NEXT*1..3]->(x:Chain) RETURN p, x.name")
    if records:
        for record in records:
            rec = dict(record)
            path = rec.get('p')
            node_name = rec.get('x.name')
            if path is not None:
                # Neo4j driver returns Path objects with nodes and relationships
                if hasattr(path, 'nodes') and hasattr(path, 'relationships'):
                    print(f"  Path to {node_name}: {len(path.nodes)} nodes, {len(path.relationships)} relationships")
                    print("  ✓ Path returned as proper Bolt Path type")
                else:
                    print(f"  Path to {node_name}: {type(path)} - {path}")

    # Cleanup chain nodes
    run_test(session, "Delete chain nodes",
             "MATCH (n:Chain) DETACH DELETE n",
             expect_results=False)


def cleanup(session):
    """Clean up test data."""
    print("\n" + "=" * 60)
    print("CLEANUP")
    print("=" * 60)

    # Use DETACH DELETE to remove nodes and their relationships
    run_test(session, "Delete TxPerson",
             "MATCH (p:Person {name: 'TxPerson'}) DETACH DELETE p",
             expect_results=False)

    run_test(session, "Delete test nodes with relationships",
             "MATCH (p:Person {name: 'Alice'}) DETACH DELETE p",
             expect_results=False)

    run_test(session, "Delete Bob",
             "MATCH (p:Person {name: 'Bob'}) DETACH DELETE p",
             expect_results=False)

    run_test(session, "Delete TechCorp",
             "MATCH (c:Company {name: 'TechCorp'}) DETACH DELETE c",
             expect_results=False)

    print("\n  Cleanup complete")


def main():
    uri = "bolt://localhost:7687"

    print("=" * 60)
    print("GravecDB Bolt Protocol Test Suite")
    print("=" * 60)
    print(f"Connecting to {uri}...")

    try:
        driver = GraphDatabase.driver(uri)
        # Verify connectivity
        driver.verify_connectivity()
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nMake sure GravecDB is running with Bolt protocol enabled:")
        print("  make run")
        sys.exit(1)

    with driver.session() as session:
        # Run all tests
        test_basic_crud(session)
        test_where_clause(session)
        test_relationships(session)
        test_set_operations(session)
        test_transactions(session, driver)
        test_delete_operations(session)
        test_complex_patterns(session)
        test_merge_operations(session)
        test_remove_operations(session)
        test_unwind_operations(session)
        test_variable_length_paths(session)

        # New feature tests
        test_parameter_binding(session)
        test_shortest_path(session)
        test_label_removal(session)

        # Cleanup
        cleanup(session)

    driver.close()

    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
