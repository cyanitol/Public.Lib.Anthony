# EXPLAIN and EXPLAIN QUERY PLAN Implementation

## Overview

The driver now fully supports both `EXPLAIN` and `EXPLAIN QUERY PLAN` statements, allowing users to analyze query execution plans and view the VDBE (Virtual Database Engine) bytecode.

## Features Implemented

### 1. EXPLAIN QUERY PLAN

Shows the high-level query execution plan that SQLite will use to execute a statement.

**Supported Statements:**
- SELECT (with/without WHERE, JOIN, GROUP BY, ORDER BY)
- INSERT (including INSERT...SELECT)
- UPDATE
- DELETE

**Output Format:**
- **Columns:** `id`, `parent`, `notused`, `detail`
- **id:** Unique identifier for each plan node
- **parent:** ID of the parent node (-1 for root nodes)
- **notused:** Reserved for compatibility (always 0)
- **detail:** Human-readable description of the plan step

**Example:**
```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 25;
```

Output:
```
id  parent  notused  detail
0   -1      0        QUERY PLAN
1   0       0          SEARCH users USING INDEX (POSSIBLE age)
```

### 2. EXPLAIN (VDBE Bytecode)

Shows the VDBE bytecode that will be executed. This is useful for debugging and understanding low-level query execution.

**Output Format:**
- **Columns:** `addr`, `opcode`, `p1`, `p2`, `p3`, `p4`, `p5`, `comment`
- **addr:** Instruction address (0-indexed)
- **opcode:** Operation code (e.g., Init, OpenRead, Rewind, etc.)
- **p1, p2, p3:** Integer parameters for the opcode
- **p4:** String/data parameter
- **p5:** Additional flags/options
- **comment:** Human-readable comment about the instruction

**Example:**
```sql
EXPLAIN SELECT * FROM users WHERE age > 25;
```

Output:
```
addr  opcode      p1  p2  p3  p4  p5  comment
0     Init        0   0   0       0
1     OpenRead    0   2   3       0
2     Rewind      0   13  0       0
3     Column      0   1   1       0   r[1]=users.age
4     Integer     25  2   0       0   INT 25
...
```

## Implementation Details

### Parser Support

The parser already supports EXPLAIN statements through:
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/parser/ast.go` - ExplainStmt AST node
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/parser/parser.go` - parseExplain() method

### Driver Implementation

**File:** `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/stmt.go`

Key functions:
1. **compileExplain()** - Main entry point for EXPLAIN compilation
2. **compileExplainQueryPlan()** - Generates query plan output
3. **compileExplainOpcodes()** - Generates VDBE bytecode output
4. **compileInnerStatement()** - Compiles the inner statement being explained

### Planner Integration

**File:** `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/planner/explain.go`

The planner provides query plan generation through:
- **GenerateExplain()** - Main entry point for generating explain plans
- **ExplainPlan** - Data structure representing the plan tree
- **FormatAsTable()** - Formats plan as rows for output

Supported plan operations:
- Table scans (SCAN tablename)
- Index searches (SEARCH tablename USING INDEX)
- JOIN operations (INNER JOIN, LEFT JOIN, etc.)
- Temporary B-trees (for GROUP BY, ORDER BY)
- Subquery handling

## Usage Examples

### Example 1: Simple Query Plan

```go
db, _ := sql.Open("sqlite_internal", "test.db")

rows, _ := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18")
defer rows.Close()

for rows.Next() {
    var id, parent, notused int
    var detail string
    rows.Scan(&id, &parent, &notused, &detail)
    fmt.Printf("%s\n", detail)
}
```

### Example 2: JOIN Query Plan

```go
query := `
    EXPLAIN QUERY PLAN
    SELECT u.name, o.total
    FROM users u
    JOIN orders o ON u.id = o.user_id
`

rows, _ := db.Query(query)
// Process results...
```

### Example 3: VDBE Bytecode

```go
rows, _ := db.Query("EXPLAIN SELECT * FROM users")
defer rows.Close()

for rows.Next() {
    var addr, p1, p2, p3, p5 int
    var opcode, p4, comment string
    rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment)
    fmt.Printf("%d: %s\n", addr, opcode)
}
```

## Testing

Tests are located in:
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/explain_test.go`
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/explain_subquery_test.go`

Run tests:
```bash
nix-shell --run "go test -v -run TestExplain ./internal/driver/"
```

## Demonstration

A comprehensive demonstration program is available at:
`/home/justin/Programming/Workspace/Public.Lib.Anthony/demo_explain.go`

Run it:
```bash
nix-shell --run "go run demo_explain.go"
```

## Key Benefits

1. **Performance Analysis:** Identify whether queries are using indexes or performing full table scans
2. **Query Optimization:** Understand the execution plan to optimize complex queries
3. **Debugging:** View the exact VDBE instructions being executed
4. **Learning:** Understand how SQLite translates SQL into low-level operations

## Limitations

1. The query plan is generated based on heuristics and doesn't execute actual table scans
2. Index usage is marked as "POSSIBLE" since we don't have actual index statistics
3. EXPLAIN doesn't execute the statement - it only shows what would be executed
4. Some advanced SQLite features may not be fully represented in the plan

## Future Enhancements

Potential improvements:
- Integration with actual index statistics for more accurate plans
- Cost estimates for different execution paths
- Support for ANALYZE command to gather table statistics
- More detailed subquery plan representation
- Optimization hints based on plan analysis
