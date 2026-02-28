# CTE AST Node Structure

## Visual Representation

### Example SQL
```sql
WITH RECURSIVE cte1(id, name) AS (
    SELECT id, name FROM users
),
cte2 AS (
    SELECT * FROM orders
)
SELECT * FROM cte1 JOIN cte2
```

### Resulting AST Structure
```
SelectStmt
├── With: *WithClause
│   ├── Recursive: true
│   └── CTEs: []CTE
│       ├── [0] CTE
│       │   ├── Name: "cte1"
│       │   ├── Columns: []string{"id", "name"}
│       │   └── Select: *SelectStmt
│       │       ├── With: nil
│       │       ├── Distinct: false
│       │       ├── Columns: []ResultColumn
│       │       │   ├── IdentExpr{Name: "id"}
│       │       │   └── IdentExpr{Name: "name"}
│       │       └── From: *FromClause
│       │           └── Tables: []TableOrSubquery
│       │               └── {TableName: "users"}
│       └── [1] CTE
│           ├── Name: "cte2"
│           ├── Columns: []string{} (empty - no column list)
│           └── Select: *SelectStmt
│               ├── With: nil
│               ├── Distinct: false
│               ├── Columns: []ResultColumn
│               │   └── {Star: true}
│               └── From: *FromClause
│                   └── Tables: []TableOrSubquery
│                       └── {TableName: "orders"}
├── Distinct: false
├── Columns: []ResultColumn
│   └── {Star: true}
└── From: *FromClause
    ├── Tables: []TableOrSubquery
    │   └── {TableName: "cte1"}
    └── Joins: []JoinClause
        └── {Table: {TableName: "cte2"}}
```

## AST Node Definitions

### WithClause
```go
type WithClause struct {
    Recursive bool    // true if WITH RECURSIVE is used
    CTEs      []CTE   // List of Common Table Expressions
}
```

**Fields:**
- `Recursive`: Boolean flag indicating whether the RECURSIVE keyword was present
- `CTEs`: Slice of CTE structs, one for each CTE in the WITH clause

### CTE
```go
type CTE struct {
    Name    string        // Name of the CTE
    Columns []string      // Optional column list
    Select  *SelectStmt   // The SELECT statement defining the CTE
}
```

**Fields:**
- `Name`: The identifier name of the CTE (e.g., "cte1", "user_orders")
- `Columns`: Optional list of column names specified in parentheses after the CTE name
  - Empty slice if no column list was provided
  - Contains column identifiers if syntax like `cte(col1, col2)` was used
- `Select`: Pointer to the SelectStmt that defines the CTE's query
  - This SelectStmt can itself have a With clause (nested CTEs)
  - Fully parsed with all SELECT features (WHERE, JOIN, GROUP BY, etc.)

### SelectStmt (Modified)
```go
type SelectStmt struct {
    With     *WithClause     // Common Table Expressions (WITH clause)
    Distinct bool
    Columns  []ResultColumn
    From     *FromClause
    Where    Expression
    GroupBy  []Expression
    Having   Expression
    OrderBy  []OrderingTerm
    Limit    Expression
    Offset   Expression
    Compound *CompoundSelect // For UNION, EXCEPT, INTERSECT
}
```

**New Field:**
- `With`: Pointer to WithClause containing all CTEs for this SELECT
  - `nil` if no WITH clause is present
  - Points to WithClause when WITH syntax is used

## Example AST Traversals

### Example 1: Simple CTE
```sql
WITH users_active AS (SELECT * FROM users WHERE active = 1)
SELECT * FROM users_active
```

```
SelectStmt
  With: &WithClause{
    Recursive: false,
    CTEs: []CTE{
      {
        Name: "users_active",
        Columns: []string{},
        Select: &SelectStmt{
          Columns: []ResultColumn{{Star: true}},
          From: &FromClause{
            Tables: []TableOrSubquery{{TableName: "users"}},
          },
          Where: BinaryExpr{Op: OpGt, Left: IdentExpr{Name: "active"}, Right: LiteralExpr{Value: "1"}},
        },
      },
    },
  },
  Columns: []ResultColumn{{Star: true}},
  From: &FromClause{
    Tables: []TableOrSubquery{{TableName: "users_active"}},
  },
```

### Example 2: Multiple CTEs
```sql
WITH
  a AS (SELECT 1 AS x),
  b AS (SELECT 2 AS y)
SELECT * FROM a, b
```

```
SelectStmt
  With: &WithClause{
    Recursive: false,
    CTEs: []CTE{
      {
        Name: "a",
        Columns: []string{},
        Select: &SelectStmt{...},
      },
      {
        Name: "b",
        Columns: []string{},
        Select: &SelectStmt{...},
      },
    },
  },
  From: &FromClause{
    Tables: []TableOrSubquery{
      {TableName: "a"},
      {TableName: "b"},
    },
  },
```

### Example 3: CTE with Column List
```sql
WITH cte(id, name, email) AS (SELECT id, name, email FROM users)
SELECT id FROM cte
```

```
SelectStmt
  With: &WithClause{
    Recursive: false,
    CTEs: []CTE{
      {
        Name: "cte",
        Columns: []string{"id", "name", "email"},
        Select: &SelectStmt{
          Columns: []ResultColumn{
            {Expr: IdentExpr{Name: "id"}},
            {Expr: IdentExpr{Name: "name"}},
            {Expr: IdentExpr{Name: "email"}},
          },
          From: &FromClause{
            Tables: []TableOrSubquery{{TableName: "users"}},
          },
        },
      },
    },
  },
  Columns: []ResultColumn{
    {Expr: IdentExpr{Name: "id"}},
  },
  From: &FromClause{
    Tables: []TableOrSubquery{{TableName: "cte"}},
  },
```

### Example 4: Recursive CTE
```sql
WITH RECURSIVE nums(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM nums WHERE n < 10
)
SELECT * FROM nums
```

```
SelectStmt
  With: &WithClause{
    Recursive: true,  // RECURSIVE keyword present
    CTEs: []CTE{
      {
        Name: "nums",
        Columns: []string{"n"},
        Select: &SelectStmt{
          Columns: []ResultColumn{
            {Expr: LiteralExpr{Value: "1"}},
          },
          Compound: &CompoundSelect{
            Op: CompoundUnionAll,
            Left: &SelectStmt{...},  // SELECT 1
            Right: &SelectStmt{      // SELECT n+1 FROM nums WHERE n < 10
              Columns: []ResultColumn{
                {Expr: BinaryExpr{Op: OpPlus, Left: IdentExpr{Name: "n"}, Right: LiteralExpr{Value: "1"}}},
              },
              From: &FromClause{
                Tables: []TableOrSubquery{{TableName: "nums"}},
              },
              Where: BinaryExpr{Op: OpLt, Left: IdentExpr{Name: "n"}, Right: LiteralExpr{Value: "10"}},
            },
          },
        },
      },
    },
  },
  Columns: []ResultColumn{{Star: true}},
  From: &FromClause{
    Tables: []TableOrSubquery{{TableName: "nums"}},
  },
```

## Field Reference

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `WithClause.Recursive` | `bool` | True if RECURSIVE keyword present | `WITH RECURSIVE cte ...` → true |
| `WithClause.CTEs` | `[]CTE` | List of all CTEs in WITH clause | Two CTEs → length 2 |
| `CTE.Name` | `string` | CTE identifier | `WITH my_cte AS ...` → "my_cte" |
| `CTE.Columns` | `[]string` | Optional column names | `cte(a, b)` → []string{"a", "b"} |
| `CTE.Select` | `*SelectStmt` | The CTE's SELECT query | Full nested SELECT statement |
| `SelectStmt.With` | `*WithClause` | WITH clause for this SELECT | nil if no WITH clause |

## Notes

1. **Nested CTEs**: A CTE's `Select` field can itself contain a `With` clause, allowing nested CTEs
2. **Column List**: The `Columns` field is an empty slice (not nil) when no column list is specified
3. **Recursive Flag**: The `Recursive` flag is on the `WithClause`, not individual CTEs
4. **CTE Scoping**: CTEs defined in a WITH clause are scoped to that specific SELECT statement
5. **Multiple CTEs**: All CTEs in a single WITH clause share the same `Recursive` flag
