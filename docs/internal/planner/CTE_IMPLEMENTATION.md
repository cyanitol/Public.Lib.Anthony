# Common Table Expressions (CTEs) Implementation

This document describes the implementation of Common Table Expressions (CTEs) in the Anthony SQLite clone.

## Overview

Common Table Expressions (CTEs) are temporary named result sets that can be referenced within a SELECT, INSERT, UPDATE, or DELETE statement. The Anthony implementation supports both **non-recursive** and **recursive** CTEs, following SQLite's semantics.

## Features

### Supported Features

1. **Non-recursive CTEs**: Simple WITH clauses that define temporary views
2. **Recursive CTEs**: WITH RECURSIVE clauses for hierarchical and iterative queries
3. **Multiple CTEs**: Multiple CTE definitions in a single WITH clause
4. **CTE Dependencies**: CTEs can reference other CTEs defined in the same WITH clause
5. **Column Lists**: Explicit column naming in CTE definitions
6. **CTE Expansion**: Automatic expansion of CTE references into virtual tables
7. **Materialization**: Optional materialization of CTEs into temporary tables

### SQL Syntax

#### Simple CTE
```sql
WITH cte_name AS (
    SELECT column1, column2 FROM table_name WHERE condition
)
SELECT * FROM cte_name;
```

#### CTE with Column List
```sql
WITH cte_name(col1, col2) AS (
    SELECT column1, column2 FROM table_name
)
SELECT * FROM cte_name;
```

#### Multiple CTEs
```sql
WITH
    cte1 AS (SELECT * FROM table1),
    cte2 AS (SELECT * FROM table2),
    cte3 AS (SELECT * FROM cte1 JOIN cte2)
SELECT * FROM cte3;
```

#### Recursive CTE
```sql
WITH RECURSIVE counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM counter WHERE n < 10
)
SELECT * FROM counter;
```

#### Hierarchical Query
```sql
WITH RECURSIVE org_chart(id, name, manager_id, level) AS (
    -- Anchor member: top-level employees
    SELECT id, name, manager_id, 0
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive member: employees with managers
    SELECT e.id, e.name, e.manager_id, oc.level + 1
    FROM employees e
    JOIN org_chart oc ON e.manager_id = oc.id
)
SELECT * FROM org_chart ORDER BY level;
```

## Architecture

### Components

1. **CTEContext** (`internal/planner/cte.go`)
   - Manages all CTEs in a query
   - Tracks dependencies between CTEs
   - Handles CTE expansion and materialization

2. **CTEDefinition** (`internal/planner/cte.go`)
   - Represents a single CTE definition
   - Contains the SELECT statement and metadata
   - Tracks recursive status and dependencies

3. **MaterializedCTE** (`internal/planner/cte.go`)
   - Represents a materialized CTE
   - Manages temporary table creation
   - Tracks materialization status

4. **Parser Integration** (`internal/parser/parser.go`)
   - Parses WITH clauses
   - Creates AST nodes for CTEs
   - Validates CTE syntax

5. **Planner Integration** (`internal/planner/planner.go`)
   - Integrates CTEs into query planning
   - Expands CTE references to virtual tables
   - Optimizes CTE execution

### Data Flow

```
SQL Query
    ↓
Parser (WITH clause parsing)
    ↓
AST (WithClause + CTE nodes)
    ↓
CTEContext creation
    ↓
Dependency Analysis
    ↓
Topological Sort
    ↓
Planner Integration
    ↓
CTE Expansion
    ↓
Query Execution
```

## Implementation Details

### CTE Context Creation

When a WITH clause is encountered, a `CTEContext` is created:

```go
ctx, err := planner.NewCTEContext(selectStmt.With)
```

This performs:
1. Parsing each CTE definition
2. Detecting recursive CTEs
3. Building dependency graph
4. Creating topological order

### Dependency Analysis

CTEs can reference other CTEs. The implementation:
1. Scans each CTE's SELECT statement
2. Identifies references to other CTEs
3. Builds a dependency graph
4. Detects circular dependencies
5. Creates execution order via topological sort

Example:
```sql
WITH
    a AS (SELECT 1),           -- Level 0
    b AS (SELECT * FROM a),    -- Level 1 (depends on a)
    c AS (SELECT * FROM b)     -- Level 2 (depends on b)
SELECT * FROM c;
```

Execution order: `a → b → c`

### Recursive CTE Processing

Recursive CTEs are detected and validated:

1. **Detection**: Check if CTE references itself
2. **Validation**: Ensure UNION/UNION ALL structure
3. **Execution**: Iterative evaluation
   - Execute anchor member (non-recursive part)
   - Execute recursive member repeatedly
   - Union results until convergence

### CTE Expansion

When a CTE is referenced in a query, it's expanded to a `TableInfo`:

```go
table, err := ctx.ExpandCTE("cte_name", cursor)
```

This creates a virtual table with:
- Column definitions (from CTE definition)
- Estimated row count
- Cursor assignment

### Materialization

For complex or frequently-used CTEs, materialization is supported:

```go
mat, err := ctx.MaterializeCTE("cte_name")
```

This:
1. Creates a temporary table
2. Executes the CTE query
3. Stores results in temp table
4. Returns materialized table info

## Query Planning Integration

The planner integrates CTEs in the planning phase:

```go
// Set CTE context
planner.SetCTEContext(ctx)

// Plan query (CTEs are automatically expanded)
info, err := planner.PlanQuery(tables, whereClause)
```

The planner:
1. Expands CTE references to virtual tables
2. Applies cost-based optimization
3. Determines materialization strategy
4. Generates execution plan

## Testing

Comprehensive tests are provided in `cte_test.go`:

1. **TestNewCTEContext**: CTE context creation
2. **TestCTEDependencies**: Dependency detection
3. **TestCTEDependencyOrder**: Topological sorting
4. **TestRecursiveCTEDetection**: Recursive CTE detection
5. **TestExpandCTE**: CTE expansion to TableInfo
6. **TestCTEColumnInference**: Column name inference
7. **TestCTEValidation**: CTE validation
8. **TestRewriteQueryWithCTEs**: Query rewriting
9. **TestPlannerWithCTEs**: Planner integration
10. **TestMultipleCTEs**: Multiple CTE handling
11. **TestRecursiveCTEStructure**: Recursive structure validation
12. **TestMaterializeCTE**: CTE materialization
13. **TestCTEsInSubqueries**: CTEs in subqueries

## Example Usage

### Simple CTE

```go
sql := "WITH users_cte AS (SELECT id, name FROM users) SELECT * FROM users_cte"

// Parse
p := parser.NewParser(sql)
stmts, _ := p.Parse()
selectStmt := stmts[0].(*parser.SelectStmt)

// Create CTE context
ctx, _ := planner.NewCTEContext(selectStmt.With)

// Use with planner
queryPlanner := planner.NewPlanner()
queryPlanner.SetCTEContext(ctx)
```

### Recursive CTE

```go
sql := `WITH RECURSIVE cnt(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM cnt WHERE n < 10
) SELECT * FROM cnt`

// Parse and create context
p := parser.NewParser(sql)
stmts, _ := p.Parse()
selectStmt := stmts[0].(*parser.SelectStmt)
ctx, _ := planner.NewCTEContext(selectStmt.With)

// Check recursive flag
cntDef, _ := ctx.GetCTE("cnt")
fmt.Printf("Is recursive: %v\n", cntDef.IsRecursive)
```

## Performance Considerations

### CTE Materialization Strategy

The implementation uses heuristics to decide when to materialize CTEs:

1. **Always materialize if**:
   - CTE is referenced multiple times
   - CTE is recursive
   - CTE result is large and complex

2. **Inline if**:
   - CTE is referenced once
   - CTE is simple (single table scan)
   - Result set is small

### Recursive CTE Limits

To prevent infinite loops, recursive CTEs have:
- Maximum iteration limit (default: 1000)
- Row count limit
- Timeout protection

## Limitations

Current limitations:
1. No support for MATERIALIZED/NOT MATERIALIZED hints
2. Recursive CTE optimization is basic
3. No parallel CTE execution
4. Limited cross-CTE optimization

## Future Enhancements

Planned improvements:
1. Advanced recursive CTE optimization
2. Bloom filters for large CTEs
3. Parallel CTE execution
4. Better cardinality estimation
5. CTE result caching
6. Support for CTEs in INSERT/UPDATE/DELETE

## References

- SQLite WITH clause: https://www.sqlite.org/lang_with.html
- SQL:1999 CTEs specification
- PostgreSQL CTE documentation
- Anthony parser: `internal/parser/parser.go`
- Anthony planner: `internal/planner/planner.go`

## Testing

Run CTE tests:
```bash
go test -v ./internal/planner -run TestCTE
go test -v ./internal/parser -run TestParseCTE
```

Run all tests:
```bash
go test -v ./internal/planner
go test -v ./internal/parser
```

## Contributing

When adding CTE features:
1. Update `cte.go` with new functionality
2. Add tests to `cte_test.go`
3. Update this documentation
4. Ensure backward compatibility
5. Add example usage to `cte_example_test.go`
