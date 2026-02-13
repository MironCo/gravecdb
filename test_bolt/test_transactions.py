#!/usr/bin/env python3
"""
Test script for GravecDB ACID transactions.
Requires: pip install neo4j
"""

from neo4j import GraphDatabase
import random

def main():
    # Connect to GravecDB via Bolt protocol
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri)

    print("=" * 50)
    print("GravecDB Transaction Test")
    print("=" * 50)

    # Test 1: Successful transaction (commit)
    print("\n[Test 1] Transaction with COMMIT")
    print("-" * 40)

    unique_id = random.randint(1000, 9999)
    with driver.session() as session:
        # Start explicit transaction
        with session.begin_transaction() as tx:
            # Create a node within the transaction
            tx.run(f"CREATE (p:Person {{name: 'TxTest{unique_id}', age: 42}})")
            print(f"  Created node TxTest{unique_id} in transaction")

            # Commit the transaction
            tx.commit()
            print("  Transaction committed!")

    # Verify the node exists after commit
    with driver.session() as session:
        result = session.run(f"MATCH (p:Person {{name: 'TxTest{unique_id}'}}) RETURN p")
        records = list(result)
        if records:
            print(f"  Verified: Node TxTest{unique_id} exists after commit")
        else:
            print(f"  ERROR: Node TxTest{unique_id} not found after commit!")

    # Test 2: Rolled back transaction
    print("\n[Test 2] Transaction with ROLLBACK")
    print("-" * 40)

    rollback_id = random.randint(1000, 9999)
    with driver.session() as session:
        # Start explicit transaction
        with session.begin_transaction() as tx:
            # Create a node within the transaction
            tx.run(f"CREATE (p:Person {{name: 'RollbackTest{rollback_id}', age: 99}})")
            print(f"  Created node RollbackTest{rollback_id} in transaction")

            # Rollback the transaction
            tx.rollback()
            print("  Transaction rolled back!")

    # Verify the node does NOT exist after rollback
    with driver.session() as session:
        result = session.run(f"MATCH (p:Person {{name: 'RollbackTest{rollback_id}'}}) RETURN p")
        records = list(result)
        if not records:
            print(f"  Verified: Node RollbackTest{rollback_id} does NOT exist (rollback worked!)")
        else:
            print(f"  ERROR: Node RollbackTest{rollback_id} exists but should have been rolled back!")

    # Test 3: Multiple operations in single transaction
    print("\n[Test 3] Multiple operations in single transaction")
    print("-" * 40)

    multi_id = random.randint(1000, 9999)
    with driver.session() as session:
        with session.begin_transaction() as tx:
            # Create multiple nodes
            tx.run(f"CREATE (a:Person {{name: 'Alice{multi_id}'}})")
            tx.run(f"CREATE (b:Person {{name: 'Bob{multi_id}'}})")
            print(f"  Created Alice{multi_id} and Bob{multi_id}")

            # All committed together
            tx.commit()
            print("  Both nodes committed together")

    # Verify both exist
    with driver.session() as session:
        result = session.run(f"MATCH (p:Person) WHERE p.name = 'Alice{multi_id}' OR p.name = 'Bob{multi_id}' RETURN p.name")
        names = [r["p.name"] for r in result]
        if len(names) == 2:
            print(f"  Verified: Both nodes exist: {names}")
        else:
            print(f"  WARNING: Expected 2 nodes, found {len(names)}: {names}")

    # Test 4: Auto-commit mode (no explicit transaction)
    print("\n[Test 4] Auto-commit mode (implicit transaction)")
    print("-" * 40)

    auto_id = random.randint(1000, 9999)
    with driver.session() as session:
        # No explicit transaction - should auto-commit
        session.run(f"CREATE (p:Person {{name: 'AutoCommit{auto_id}'}})")
        print(f"  Created AutoCommit{auto_id} without explicit transaction")

    # Verify it exists
    with driver.session() as session:
        result = session.run(f"MATCH (p:Person {{name: 'AutoCommit{auto_id}'}}) RETURN p")
        records = list(result)
        if records:
            print(f"  Verified: Node exists (auto-commit worked)")
        else:
            print(f"  ERROR: Node not found!")

    driver.close()
    print("\n" + "=" * 50)
    print("Transaction tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
