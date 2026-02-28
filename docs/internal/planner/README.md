# SQLite Query Planner

A pure Go implementation of SQLite's query planner and optimizer. This package analyzes WHERE clauses, evaluates index usage, estimates costs, and generates optimal query execution plans.

## Overview

The query planner is responsible for:

1. **WHERE Clause Analysis** - Breaking down WHERE expressions into individual terms
2. **Index Selection** - Identifying which indexes can be used for each constraint
3. **Cost Estimation** - Calculating the cost of different access paths
4. **Plan Generation** - Choosing the optimal sequence of table accesses
5. **Join Optimization** - Determining the best join order for multi-table queries

## Architecture

### Core Components

- **Planner** - Main entry point for query planning
- **WhereLoop** - Represents a single access path for a table
- **WhereLoopBuilder** - Generates all possible WhereLoop options
- **CostModel** - Estimates costs for different operations
- **IndexSelector** - Chooses the best index for given constraints

### Key Data Structures

```go
// WhereLoop represents one way to access a table
type WhereLoop struct {
    TabIndex  int          // Table position in FROM clause
    Setup     LogEst       // One-time setup cost
    Run       LogEst       // Cost per execution
    NOut      LogEst       // Estimated output rows
    Flags     WhereFlags   // Access method flags
    Index     *IndexInfo   // Index to use (nil for full scan)
    Terms     []*WhereTerm // WHERE constraints used
}

// WhereTerm represents a single WHERE constraint
type WhereTerm struct {
    Operator    WhereOperator // EQ, LT, GT, etc.
    LeftCursor  int           // Table cursor
    LeftColumn  int           // Column index
    RightValue  interface{}   // Comparison value
}
```

## Usage

### Basic Query Planning

```go
import "github.com/JustinHenderson98/JuniperBible/core/sqlite/internal/planner"

// Create planner
p := planner.NewPlanner()

// Define table with indexes
table := &planner.TableInfo{
    Name:      "users",
    RowCount:  10000,
    RowLogEst: planner.NewLogEst(10000),
    Columns: []planner.ColumnInfo{
        {Name: "id", Index: 0, Type: "INTEGER"},
        {Name: "name", Index: 1, Type: "TEXT"},
    },
    Indexes: []*planner.IndexInfo{
        {
            Name:      "pk_users",
            Unique:    true,
            Columns:   []planner.IndexColumn{{Name: "id", Index: 0}},
            ColumnStats: []planner.LogEst{0}, // Unique index
        },
    },
}

// Create WHERE clause: id = 5
whereClause := &planner.WhereClause{
    Terms: []*planner.WhereTerm{
        {
            Operator:   planner.WO_EQ,
            LeftCursor: 0,
            LeftColumn: 0,
            RightValue: 5,
        },
    },
}

// Generate plan
info, err := p.PlanQuery([]*planner.TableInfo{table}, whereClause)
if err != nil {
    panic(err)
}

// Explain plan
fmt.Println(p.ExplainPlan(info))
```

### Index Selection

```go
// Create table with multiple indexes
table := &planner.TableInfo{
    Name: "products",
    Indexes: []*planner.IndexInfo{
        {Name: "idx_category", Columns: []planner.IndexColumn{{Name: "category"}}},
        {Name: "idx_price", Columns: []planner.IndexColumn{{Name: "price"}}},
        {Name: "idx_cat_price", Columns: []planner.IndexColumn{
            {Name: "category"},
            {Name: "price"},
        }},
    },
}

// Define constraints
terms := []*planner.WhereTerm{
    {Operator: planner.WO_EQ, LeftColumn: 0}, // category = ?
    {Operator: planner.WO_LT, LeftColumn: 1}, // price < ?
}

// Select best index
selector := planner.NewIndexSelector(table, terms, planner.NewCostModel())
bestIndex := selector.SelectBestIndex()
// Returns idx_cat_price (compound index covers both columns)
```

### Join Planning

```go
// Define two tables
customers := &planner.TableInfo{Name: "customers", Cursor: 0}
orders := &planner.TableInfo{Name: "orders", Cursor: 1}

// WHERE: customers.id = 5 AND orders.customer_id = customers.id
whereClause := &planner.WhereClause{
    Terms: []*planner.WhereTerm{
        {
            Operator:   planner.WO_EQ,
            LeftCursor: 0,
            LeftColumn: 0,
            RightValue: 5,
        },
        {
            Operator:    planner.WO_EQ,
            LeftCursor:  1,
            LeftColumn:  1,
            PrereqRight: planner.Bitmask(1 << 0), // Depends on customers
        },
    },
}

// Plan will choose: customers first (has constant), then orders (using join key)
info, _ := p.PlanQuery([]*planner.TableInfo{customers, orders}, whereClause)
```

## Query Planning Stages

### 1. WHERE Clause Analysis

The planner splits WHERE expressions into individual terms:

- `a = 1 AND b > 2` → Two terms: `a = 1`, `b > 2`
- `(a = 1 OR a = 2) AND b > 2` → OR term + comparison term

### 2. Access Path Generation

For each table, generate all possible access methods:

- **Full table scan** - Sequential scan of all rows
- **Index scan** - Use index with range constraints
- **Index seek** - Use index for equality lookups
- **Covering index** - Index contains all needed columns
- **Primary key lookup** - Direct rowid access

### 3. Cost Estimation

Calculate cost for each access path:

```
Cost = SeekCost + (RowsScanned * ScanCost) + (RowsFetched * FetchCost)
```

Factors considered:

- Number of rows to examine
- Index vs table I/O
- Selectivity of constraints
- Whether index is covering

### 4. Plan Selection

For single table:

- Choose lowest-cost WhereLoop

For joins:

- Use dynamic programming to find optimal join order
- Consider prerequisites (which tables must be accessed first)
- Keep top N partial plans at each level

## Cost Model

### LogEst Values

Costs and row counts use `LogEst` - logarithmic estimates stored as `10 * log2(n)`:

```go
LogEst(0)   = 1 row
LogEst(10)  = 2 rows
LogEst(20)  = 4 rows
LogEst(100) = 1024 rows
```

Benefits:

- Compact representation (int16)
- Easy to combine (addition = multiplication)
- Handles large numbers without overflow

### Cost Components

```go
const (
    costFullScan     = 100  // Cost per row for table scan
    costIndexSeek    = 10   // Cost to seek in index
    costIndexNext    = 5    // Cost to read next index entry
    costRowidLookup  = 19   // Cost to fetch row by rowid
    costComparison   = 2    // Cost of single comparison
)
```

### Selectivity Estimates

```go
const (
    selectivityEq    = -10  // x = const: ~1/1024 rows
    selectivityRange = -3   // x > const: ~1/8 rows
    selectivityIn    = -7   // x IN (...): ~1/128 rows
    selectivityNull  = -20  // x IS NULL: very rare
)
```

## Access Path Types

### Full Table Scan

```
SCAN table
Cost: nRows * costFullScan
```

Used when:

- No usable indexes
- Very small tables
- Constraint has low selectivity

### Index Range Scan

```
INDEX idx (a=? AND b>?)
Cost: seek + (matchingRows * (indexScan + rowLookup))
```

Used for:

- Equality on prefix columns
- Range on last column
- e.g., `a = 1 AND b > 10`

### Index Seek (Unique)

```
INDEX UNIQUE (a=?)
Cost: seek + rowLookup
```

Used when:

- All columns of unique index are constrained
- Returns exactly 1 row

### Covering Index

```
INDEX COVERING (a=?)
Cost: seek + (matchingRows * indexScan)
```

No row lookups needed - index contains all required columns.

### Primary Key Lookup

```
PRIMARY KEY (rowid=?)
Cost: seek
```

Direct access by rowid - fastest possible access.

## WHERE Clause Optimization

### AND Splitting

```sql
WHERE a = 1 AND b = 2 AND c > 3
```

Split into three independent terms, each can use different index columns.

### OR Handling

```sql
WHERE a = 1 OR a = 2
```

Two strategies:

1. Scan both ranges and merge
2. Convert to `a IN (1, 2)` if possible

### Transitive Closure

```sql
WHERE a = b AND b = 5
```

Infer: `a = 5`, allowing index on `a` to be used.

### Range Merging

```sql
WHERE a > 5 AND a < 10
```

Combine into single range scan: `5 < a < 10`.

## Index Selection Algorithm

1. **Score each index** based on:
   - Number of constrained columns
   - Type of constraints (equality > range)
   - Uniqueness
   - Coverage

2. **Estimate cost** for each index:
   - Calculate seek cost
   - Estimate matching rows using statistics
   - Add row fetch cost if not covering

3. **Select best** by combined score and cost

## Advanced Features

### Skip-Scan Optimization

Use index even when first column is not constrained:

```sql
-- Index on (city, age)
WHERE age = 25  -- Skip scan over distinct cities
```

Cost: `distinctCities * (seek + scan(age=25))`

Only beneficial when first column has low cardinality.

### Automatic Index Creation

Planner can suggest automatic indexes:

```go
cost := planner.EstimateIndexBuildCost(table, []string{"col1", "col2"})
if cost < scanCost {
    // Creating temp index is worthwhile
}
```

### ORDER BY Optimization

Index can satisfy ORDER BY without sorting:

```sql
-- Index on (a, b)
WHERE a = 1 ORDER BY b
-- No sort needed!
```

### Bloom Filters

For joins, use Bloom filter on inner table to reduce outer table rows:

```sql
SELECT * FROM large_table t1
JOIN small_table t2 ON t1.key = t2.key
-- Build Bloom filter on t2.key, test t1 rows before join
```

## Testing

Run tests:
```bash
go test ./planner
```

Run benchmarks:
```bash
go test -bench=. ./planner
```

Example output:
```
BenchmarkPlanQuery-8          50000    25431 ns/op
BenchmarkWhereLoopBuilder-8  100000    12345 ns/op
```

## Limitations

This implementation includes:

- ✅ Single and multi-table queries
- ✅ Index selection and cost estimation
- ✅ JOIN order optimization
- ✅ WHERE clause analysis
- ✅ Range and equality constraints

Not yet implemented:

- ❌ Virtual table support
- ❌ Subquery optimization
- ❌ Window functions
- ❌ Common table expressions (CTEs)
- ❌ Full LIKE optimization

## References

Based on SQLite's query planner:

- `where.c` - Main planning logic
- `wherecode.c` - Code generation
- `whereexpr.c` - Expression analysis

## Performance Tips

1. **Use appropriate indexes**
   - Index commonly queried columns
   - Use compound indexes for multiple constraints
   - Consider covering indexes for frequent queries

2. **Help the planner**
   - Provide statistics via `ANALYZE`
   - Use unique indexes where applicable
   - Avoid functions on indexed columns

3. **Optimize join order**
   - Put tables with constant constraints first
   - Use indexes on join keys
   - Consider Bloom filters for large joins

## License

This implementation is part of the JuniperBible project.
Based on SQLite algorithms (public domain).
