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

        # Cleanup
        cleanup(session)

    driver.close()

    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
