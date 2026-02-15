# Bolt Protocol Implementation

GravecDB implements the Neo4j Bolt protocol (v4.4), allowing connections from standard Neo4j drivers (Python, JavaScript, Java, etc.).

## Current Status

### Protocol Support

| Feature | Status | Notes |
|---------|--------|-------|
| Handshake | ✅ | Bolt 3.0+ accepted, responds as 4.4 |
| INIT/HELLO | ✅ | Basic auth fields parsed (not enforced) |
| GOODBYE | ✅ | Clean disconnect |
| RUN | ✅ | Query execution |
| PULL/PULL_ALL | ✅ | Result streaming |
| DISCARD/DISCARD_ALL | ✅ | Result discarding |
| RESET | ✅ | Connection state reset |
| ACK_FAILURE | ✅ | Error acknowledgment |
| BEGIN | ✅ | Transaction start |
| COMMIT | ✅ | Transaction commit |
| ROLLBACK | ✅ | Transaction rollback |

### Cypher Query Support

#### Clauses - Working

| Clause | Status | Example |
|--------|--------|---------|
| MATCH | ✅ | `MATCH (n:Person) RETURN n` |
| WHERE | ✅ | `MATCH (n) WHERE n.age > 25 RETURN n` |
| RETURN | ✅ | `RETURN n.name, n.age` |
| CREATE | ✅ | `CREATE (n:Person {name: 'Alice'})` |
| DELETE | ✅ | `MATCH (n) DELETE n` |
| DETACH DELETE | ✅ | `MATCH (n) DETACH DELETE n` |
| SET | ✅ | `MATCH (n) SET n.x = 1 RETURN n` |
| LIMIT | ✅ | `MATCH (n) RETURN n LIMIT 10` |
| SKIP | ✅ | `MATCH (n) RETURN n SKIP 5` |
| ORDER BY | ✅ | `MATCH (n) RETURN n ORDER BY n.name` |

#### Clauses - Partial/TODO

| Clause | Status | Notes |
|--------|--------|-------|
| OPTIONAL MATCH | ⚠️ | Parsed, may not behave correctly |
| MERGE | ❌ | Parsed but not implemented |
| WITH | ⚠️ | Basic support |
| UNWIND | ❌ | Parsed but not implemented |
| REMOVE | ❌ | Parsed but not implemented |
| FOREACH | ❌ | Not implemented |
| CALL/YIELD | ❌ | Procedures not supported |
| UNION | ❌ | Not implemented |
| LOAD CSV | ❌ | Not implemented |

#### Patterns & Expressions

| Feature | Status | Example |
|---------|--------|---------|
| Node patterns | ✅ | `(n:Label {prop: value})` |
| Relationship patterns | ✅ | `(a)-[r:TYPE]->(b)` |
| Undirected relationships | ✅ | `(a)-[r]-(b)` |
| Property access | ✅ | `n.name`, `r.since` |
| Comparison operators | ✅ | `=`, `<>`, `<`, `>`, `<=`, `>=` |
| Boolean operators | ✅ | `AND`, `OR`, `NOT`, `XOR` |
| Arithmetic | ✅ | `+`, `-`, `*`, `/`, `%`, `^` |
| String predicates | ✅ | `CONTAINS`, `STARTS WITH`, `ENDS WITH` |
| IN operator | ✅ | `n.status IN ['active', 'pending']` |
| IS NULL / IS NOT NULL | ✅ | `n.email IS NOT NULL` |
| List literals | ✅ | `[1, 2, 3]` |
| Map literals | ✅ | `{key: 'value'}` |
| CASE expressions | ✅ | `CASE WHEN ... THEN ... END` |
| Aggregate functions | ✅ | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT` |
| Variable-length paths | ❌ | `(a)-[*1..3]->(b)` |
| Path functions | ❌ | `shortestPath()`, `allShortestPaths()` |

#### GravecDB Extensions

| Feature | Status | Example |
|---------|--------|---------|
| EMBED | ✅ | `MATCH (n) EMBED n RETURN n` |
| SIMILAR TO | ✅ | `MATCH (n) SIMILAR TO "query" RETURN n` |
| THROUGH TIME | ✅ | `MATCH (n) SIMILAR TO "query" THROUGH TIME RETURN n` |
| DRIFT | ✅ | `MATCH (n) SIMILAR TO "query" DRIFT THROUGH TIME RETURN n` |
| AT TIME | ✅ | `MATCH (n) AT TIME $timestamp RETURN n` |

### Data Types

| Type | Status | Notes |
|------|--------|-------|
| Null | ✅ | |
| Boolean | ✅ | |
| Integer | ✅ | |
| Float | ✅ | |
| String | ✅ | |
| List | ✅ | |
| Map | ✅ | |
| Node | ✅ | Converted to Bolt Node structure |
| Relationship | ✅ | Converted to Bolt Relationship structure |
| Path | ❌ | Not yet implemented |
| Date/Time types | ❌ | Not yet implemented |
| Point (spatial) | ❌ | Not planned |

## Known Issues

1. **Authentication**: Auth fields are parsed but not enforced
2. **Multiple statements**: Only single statements supported per RUN

## TODO

### High Priority
- [x] ~~Fix SET clause to work with following clauses (RETURN, etc.)~~
- [ ] Implement MERGE clause
- [ ] Add variable-length path support `[*1..5]`
- [ ] Implement Path data type for Bolt responses

### Medium Priority
- [ ] Implement REMOVE clause
- [ ] Implement UNWIND clause
- [ ] Add UNION support
- [ ] Proper error codes and messages
- [ ] Connection authentication

### Low Priority
- [ ] LOAD CSV support
- [ ] CALL/YIELD for stored procedures
- [ ] EXPLAIN/PROFILE query plans
- [ ] Date/Time data types
- [ ] Multiple statements per transaction

## Testing

```bash
cd test_bolt
source venv/bin/activate
python test_connection.py
```

Requires the server running (`make run`) and `neo4j` Python package installed.

## Usage

The Bolt server starts automatically on port 7687 when running the main server:

```bash
make run
# Bolt server listening on :7687
```

Connect with any Neo4j driver:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (n:Person) RETURN n.name")
    for record in result:
        print(record["n.name"])
```

```javascript
const neo4j = require('neo4j-driver');

const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();
const result = await session.run('MATCH (n:Person) RETURN n.name');
```
