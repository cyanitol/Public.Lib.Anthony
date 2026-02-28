# CTE Quick Reference Guide

## SQL Syntax Examples

### Simple CTE
```sql
WITH users_cte AS (
    SELECT id, name FROM users WHERE active = 1
)
SELECT * FROM users_cte;
```

### CTE with Column List
```sql
WITH users_cte(user_id, user_name) AS (
    SELECT id, name FROM users
)
SELECT * FROM users_cte;
```

### Multiple CTEs
```sql
WITH
    active_users AS (SELECT id, name FROM users WHERE active = 1),
    user_orders AS (SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users))
SELECT * FROM user_orders;
```

### Recursive CTE - Counter
```sql
WITH RECURSIVE counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM counter WHERE n < 10
)
SELECT * FROM counter;
```

### Recursive CTE - Hierarchy
```sql
WITH RECURSIVE org_chart(id, name, manager_id, level) AS (
    -- Anchor: top-level employees
    SELECT id, name, manager_id, 0
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: subordinates
    SELECT e.id, e.name, e.manager_id, oc.level + 1
    FROM employees e
    JOIN org_chart oc ON e.manager_id = oc.id
)
SELECT * FROM org_chart ORDER BY level;
```

### Recursive CTE - Tree Traversal
```sql
WITH RECURSIVE tree(id, parent_id, name, path, depth) AS (
    -- Root nodes
    SELECT id, parent_id, name, name AS path, 0 AS depth
    FROM categories
    WHERE parent_id IS NULL

    UNION ALL

    -- Child nodes
    SELECT c.id, c.parent_id, c.name, t.path || ' > ' || c.name, t.depth + 1
    FROM categories c
    JOIN tree t ON c.parent_id = t.id
)
SELECT * FROM tree ORDER BY path;
```

## Code Examples

### Basic Usage
```go
import (
    "github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
)

// Parse SQL
sql := "WITH cte AS (SELECT * FROM users) SELECT * FROM cte"
p := parser.NewParser(sql)
stmts, _ := p.Parse()
selectStmt := stmts[0].(*parser.SelectStmt)

// Create CTE context
ctx, _ := planner.NewCTEContext(selectStmt.With)

// Check CTEs
fmt.Printf("CTEs: %d\n", len(ctx.CTEs))
```

### With Planner
```go
// Create planner
queryPlanner := planner.NewPlanner()
queryPlanner.SetCTEContext(ctx)

// Expand CTE
cteTable, _ := ctx.ExpandCTE("cte", 0)

// Plan query
info, _ := queryPlanner.PlanQuery([]*planner.TableInfo{cteTable}, nil)
```

### Check Recursive
```go
def, exists := ctx.GetCTE("org_chart")
if exists {
    fmt.Printf("Is recursive: %v\n", def.IsRecursive)
    fmt.Printf("Columns: %v\n", def.Columns)
}
```

### Validate CTEs
```go
err := ctx.ValidateCTEs()
if err != nil {
    fmt.Printf("Validation error: %v\n", err)
}
```

### Materialize CTE
```go
mat, _ := ctx.MaterializeCTE("expensive_cte")
fmt.Printf("Materialized to: %s\n", mat.TempTable)
```

## API Reference

### CTEContext Methods

| Method | Description |
|--------|-------------|
| `NewCTEContext(withClause)` | Create CTE context from WITH clause |
| `GetCTE(name)` | Get CTE definition by name |
| `HasCTE(name)` | Check if CTE exists |
| `ExpandCTE(name, cursor)` | Expand CTE to TableInfo |
| `MaterializeCTE(name)` | Materialize CTE to temp table |
| `RewriteQueryWithCTEs(tables)` | Rewrite query to use CTEs |
| `ValidateCTEs()` | Validate all CTEs |

### CTEDefinition Fields

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | CTE name |
| `Columns` | `[]string` | Column names |
| `Select` | `*SelectStmt` | SELECT definition |
| `IsRecursive` | `bool` | Recursive flag |
| `DependsOn` | `[]string` | Dependencies |
| `Level` | `int` | Dependency level |

### Planner Integration

| Method | Description |
|--------|-------------|
| `SetCTEContext(ctx)` | Set CTE context on planner |
| `GetCTEContext()` | Get current CTE context |
| `PlanQuery(tables, where)` | Plan query (auto-expands CTEs) |

## Common Patterns

### Pattern 1: Filter then Join
```sql
WITH
    active_users AS (SELECT id FROM users WHERE active = 1),
    recent_orders AS (SELECT * FROM orders WHERE date > '2024-01-01')
SELECT u.name, o.total
FROM active_users au
JOIN users u ON au.id = u.id
JOIN recent_orders o ON u.id = o.user_id;
```

### Pattern 2: Aggregation then Filter
```sql
WITH sales_summary AS (
    SELECT product_id, SUM(amount) as total
    FROM sales
    GROUP BY product_id
)
SELECT * FROM sales_summary WHERE total > 1000;
```

### Pattern 3: Recursive Path Finding
```sql
WITH RECURSIVE paths(id, name, path) AS (
    SELECT id, name, CAST(id AS TEXT)
    FROM nodes
    WHERE parent_id IS NULL

    UNION ALL

    SELECT n.id, n.name, p.path || '/' || CAST(n.id AS TEXT)
    FROM nodes n
    JOIN paths p ON n.parent_id = p.id
)
SELECT * FROM paths;
```

## Testing

### Run Parser Tests
```bash
go test -v ./internal/parser -run TestParseCTE
```

### Run Planner Tests
```bash
go test -v ./internal/planner -run TestCTE
go test -v ./internal/planner -run Example
```

### Run All Tests
```bash
go test -v ./internal/parser ./internal/planner
```

## Files Reference

| File | Description |
|------|-------------|
| `internal/parser/ast.go` | CTE AST nodes |
| `internal/parser/parser.go` | CTE parsing |
| `internal/parser/parser_cte_test.go` | Parser tests |
| `internal/planner/cte.go` | CTE implementation |
| `internal/planner/cte_test.go` | Planner tests |
| `internal/planner/cte_example_test.go` | Usage examples |
| `internal/planner/planner.go` | Planner integration |

## Limitations

1. No MATERIALIZED/NOT MATERIALIZED hints
2. Recursive CTEs limited to 1000 iterations (default)
3. CTEs must contain SELECT statements
4. No parallel CTE execution

## Tips

1. **Use column lists** for clarity:
   ```sql
   WITH cte(id, name) AS (...)  -- Clear
   ```

2. **Name CTEs descriptively**:
   ```sql
   WITH active_users AS (...)   -- Good
   WITH t1 AS (...)             -- Bad
   ```

3. **Order CTEs by dependency**:
   ```sql
   WITH
       base AS (...),           -- Level 0
       derived AS (... FROM base),  -- Level 1
       final AS (... FROM derived)  -- Level 2
   ```

4. **Use recursive CTEs for hierarchies**:
   - Organization charts
   - Bill of materials
   - Category trees
   - Path finding

5. **Validate before executing**:
   ```go
   err := ctx.ValidateCTEs()
   if err != nil {
       // Handle error
   }
   ```
