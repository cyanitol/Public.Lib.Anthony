# EXPLAIN QUERY PLAN with Cost Estimates

## Overview

Phase 9.3 enhancement adds comprehensive cost estimation to EXPLAIN QUERY PLAN output. The query planner now provides:

- **Estimated rows**: Number of rows each operation will produce
- **Estimated cost**: Relative cost in arbitrary units (higher = more expensive)
- **Index usage**: Which indexes are selected and why
- **Join strategies**: How tables are joined and in what order

## Cost Estimation Model

### Table Scans

Full table scans have a base cost of `1.0 * row_count`:

```
SCAN users (cost=1000000.00 rows=1000000)
```

With a WHERE clause, estimated output rows are reduced (typically by 90%), but scan cost remains the same since all rows must be examined:

```
SCAN users (cost=1000000.00 rows=100000)
```

### Index Scans

Index scans are significantly cheaper than full table scans:

- **Unique index with equality**: `~10.0` (logarithmic lookup)
  ```
  SEARCH users USING INDEX idx_users_id (id=?) (cost=10.00 rows=1)
  ```

- **Non-unique index with equality**: `~0.5 * matching_rows`
  ```
  SEARCH users USING INDEX idx_users_status (cost=5000.00 rows=10000)
  ```

- **Index range scan**: `~0.7 * matching_rows`
  ```
  SEARCH users USING INDEX idx_users_age (cost=70000.00 rows=100000)
  ```

- **Covering index**: Additional 20% cost reduction (no table lookup needed)
  ```
  SCAN users USING COVERING INDEX idx_users_name (cost=40000.00 rows=100000)
  ```

### Joins

Join costs depend on the join type and whether indexes are available:

- **Nested loop with index**: `O(N * log M)` = `leftRows * (10 + rightRows * 0.01)`
  ```
  INNER JOIN (cost=11000.00 rows=100000)
  ```

- **Nested loop without index**: `O(N * M)` = `leftRows * rightRows`
  ```
  CROSS JOIN (cost=1000000000000.00 rows=1000000000000)
  ```

### Aggregation (GROUP BY, DISTINCT)

Aggregation requires sorting or hashing with `O(N log N)` complexity:

```
USE TEMP B-TREE FOR GROUP BY (cost=1200000.00 rows=1000)
```

Estimated output rows are typically `sqrt(input_rows)` or `input_rows / 100`.

### Sorting (ORDER BY)

Sorting has `O(N log N)` complexity with a factor of `1.5`:

```
USE TEMP B-TREE FOR ORDER BY (name, age) (cost=1500000.00 rows=1000000)
```

## Table Statistics

For more accurate cost estimates, table statistics can be provided:

```go
// Set table statistics
table.SetTableStats(&schema.TableStats{
    RowCount: 500000,
    AverageRowSize: 256,
})
```

When statistics are available, the cost estimator uses actual row counts instead of defaults.

## Example Queries

### Simple SELECT

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18;
```

Output:
```
id | parent | notused | detail
---|--------|---------|----------------------------------------------------------
0  | -1     | 0       | QUERY PLAN
1  | 0      | 0       |   SCAN users (cost=1000000.00 rows=100000)
```

### SELECT with Index

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 42;
```

Output (with index available):
```
id | parent | notused | detail
---|--------|---------|----------------------------------------------------------
0  | -1     | 0       | QUERY PLAN
1  | 0      | 0       |   SEARCH users USING INDEX idx_users_id (id=?) (cost=10.00 rows=1)
```

### SELECT with GROUP BY

```sql
EXPLAIN QUERY PLAN SELECT department, COUNT(*) FROM employees GROUP BY department;
```

Output:
```
id | parent | notused | detail
---|--------|---------|----------------------------------------------------------
0  | -1     | 0       | QUERY PLAN
1  | 0      | 0       |   SCAN employees (cost=1000000.00 rows=100000)
2  | 1      | 0       |     USE TEMP B-TREE FOR GROUP BY (cost=1200000.00 rows=1000)
```

### JOIN Query

```sql
EXPLAIN QUERY PLAN
SELECT u.name, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id;
```

Output:
```
id | parent | notused | detail
---|--------|---------|----------------------------------------------------------
0  | -1     | 0       | QUERY PLAN
1  | 0      | 0       |   SCAN users (cost=1000000.00 rows=1000000)
2  | 1      | 0       |     INNER JOIN (cost=11000000.00 rows=100000)
3  | 2      | 0       |       SCAN orders (cost=1000000.00 rows=1000000)
```

### UPDATE with WHERE

```sql
EXPLAIN QUERY PLAN UPDATE users SET status = 'active' WHERE age > 18;
```

Output:
```
id | parent | notused | detail
---|--------|---------|----------------------------------------------------------
0  | -1     | 0       | QUERY PLAN
1  | 0      | 0       |   UPDATE users (cost=1150000.00 rows=100000)
2  | 1      | 0       |     SCAN users (cost=1000000.00 rows=100000)
```

### DELETE with WHERE

```sql
EXPLAIN QUERY PLAN DELETE FROM logs WHERE created_at < '2024-01-01';
```

Output:
```
id | parent | notused | detail
---|--------|---------|----------------------------------------------------------
0  | -1     | 0       | QUERY PLAN
1  | 0      | 0       |   DELETE FROM logs (cost=1100000.00 rows=100000)
2  | 1      | 0       |     SCAN logs (cost=1000000.00 rows=100000)
```

## Cost Interpretation

Cost values are **relative**, not absolute:

- **Cost < 100**: Very fast (indexed lookups, small operations)
- **Cost 100-10,000**: Fast (small scans, indexed operations)
- **Cost 10,000-1,000,000**: Moderate (medium table scans)
- **Cost > 1,000,000**: Expensive (large table scans, complex joins)

Use costs to:
1. **Compare query plans**: Lower total cost = faster query
2. **Identify bottlenecks**: High-cost operations need optimization
3. **Evaluate indexes**: See cost reduction with proper indexing
4. **Optimize joins**: Understand join order and strategy

## Implementation Details

### Cost Estimator

The `CostEstimator` type in `internal/planner/explain.go` provides:

- `EstimateTableScan()`: Full table scan costs
- `EstimateIndexScan()`: Index-based access costs
- `EstimateJoinCost()`: Join operation costs
- `EstimateAggregateCost()`: GROUP BY/DISTINCT costs
- `EstimateSortCost()`: ORDER BY costs

### Default Assumptions

When statistics are unavailable, the estimator assumes:

- **Default table rows**: 1,000,000
- **WHERE selectivity**: 10% (filters 90% of rows)
- **Index uniqueness**: 1.0 for UNIQUE, varies for non-unique
- **Join selectivity**: 10% for INNER JOIN
- **Group cardinality**: sqrt(rows) or rows/100

### Schema Integration

To provide schema information for better estimates:

```go
import "github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
import "github.com/JuniperBible/Public.Lib.Anthony/internal/schema"

plan, err := planner.GenerateExplainWithSchema(stmt, schemaInfo)
```

The planner will use actual table statistics and index information when available.

## Future Enhancements

Potential improvements for future phases:

1. **Statistics collection**: Automatic ANALYZE command to gather table statistics
2. **Histogram-based estimates**: More accurate selectivity for range queries
3. **Cost calibration**: Tune cost factors based on actual hardware
4. **Execution feedback**: Compare estimated vs actual costs
5. **Multi-column statistics**: Better estimates for composite indexes
6. **Subquery cost tracking**: Detailed costs for nested queries

## References

- SQLite EXPLAIN QUERY PLAN: https://www.sqlite.org/eqp.html
- PostgreSQL Query Planning: https://www.postgresql.org/docs/current/using-explain.html
- Cost-based optimization: Standard database query optimization techniques
