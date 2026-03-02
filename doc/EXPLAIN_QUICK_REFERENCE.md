# EXPLAIN QUERY PLAN Quick Reference

## Basic Usage

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18;
```

## Output Columns

| Column   | Type    | Description                              |
|----------|---------|------------------------------------------|
| id       | INTEGER | Unique node identifier                   |
| parent   | INTEGER | Parent node ID (-1 for root)             |
| notused  | INTEGER | Reserved (always 0)                      |
| detail   | TEXT    | Plan description with cost estimates     |

## Reading the Output

```
id | parent | detail
---|--------|--------------------------------------------------
0  | -1     | QUERY PLAN
1  | 0      |   SCAN users (cost=1000000.00 rows=1000000)
2  | 1     |     USE TEMP B-TREE FOR ORDER BY (cost=1500000.00 rows=1000000)
```

- **id=0**: Root node (always "QUERY PLAN")
- **id=1, parent=0**: Child of root (main operation)
- **id=2, parent=1**: Child of id=1 (sub-operation)
- **Indentation**: Reflects hierarchy (2 spaces per level)

## Cost Interpretation

### Quick Guide

| Cost Range          | Performance   | Example                          |
|---------------------|---------------|----------------------------------|
| < 100               | Very Fast     | Unique index lookup              |
| 100 - 10,000        | Fast          | Small table scan, indexed access |
| 10,000 - 1,000,000  | Moderate      | Medium table scan                |
| > 1,000,000         | Expensive     | Large table scan, complex joins  |

### Row Estimates

| Operation           | Typical Rows           |
|---------------------|------------------------|
| Unique index lookup | 1                      |
| Index range scan    | 1-10% of table         |
| WHERE filter        | ~10% of input          |
| Full table scan     | All rows               |
| JOIN (INNER)        | ~10% of cartesian      |
| GROUP BY            | sqrt(input) to input/100 |

## Common Operations

### Table Scans

```
SCAN table_name (cost=C rows=R)
```
- **cost=C**: Proportional to total rows
- **rows=R**: Estimated output rows

### Index Scans

```
SEARCH table USING INDEX idx_name (col=?) (cost=C rows=R)
```
- **SEARCH**: Index used for specific lookup
- **cost=C**: Typically 10-1000x cheaper than table scan
- **rows=R**: Typically 1-1000 rows

```
SCAN table USING COVERING INDEX idx_name (cost=C rows=R)
```
- **COVERING**: All needed columns in index (no table lookup)
- **cost=C**: 20% cheaper than regular index scan

### Joins

```
INNER JOIN (cost=C rows=R)
  SCAN right_table (cost=C2 rows=R2)
```
- **cost=C**: Total join cost (depends on join strategy)
- With index: O(N * log M)
- Without index: O(N * M)

### Aggregation

```
USE TEMP B-TREE FOR GROUP BY (cost=C rows=R)
```
- **cost=C**: Sorting/hashing cost (~1.2x input rows)
- **rows=R**: Number of groups (typically << input rows)

### Sorting

```
USE TEMP B-TREE FOR ORDER BY (col1, col2) (cost=C rows=R)
```
- **cost=C**: Sort cost (~1.5x input rows)
- **rows=R**: Same as input (sorting doesn't filter)

## Optimization Tips

### High Cost Scan?

```
SCAN users (cost=1000000.00 rows=1000000)
```

**Solutions**:
1. Add WHERE clause to filter
2. Create index on filter column
3. Use LIMIT if only need few rows

### Missing Index?

```
SCAN users (cost=1000000.00 rows=100000)
```
(despite WHERE clause)

**Solutions**:
1. `CREATE INDEX idx_users_age ON users(age);`
2. Verify index exists: `SELECT * FROM sqlite_master WHERE type='index';`
3. Check index is usable for your query

### Expensive Join?

```
INNER JOIN (cost=1000000000000.00 rows=1000000000000)
```

**Solutions**:
1. Add index on join column
2. Reorder joins (smaller table first)
3. Add WHERE filters before join
4. Consider denormalization

### Unnecessary Sort?

```
USE TEMP B-TREE FOR ORDER BY (cost=1500000.00 rows=1000000)
```

**Solutions**:
1. Create index on ORDER BY columns
2. Remove ORDER BY if not needed
3. Use index to avoid explicit sort

## Examples by Query Type

### 1. Simple SELECT

```sql
EXPLAIN QUERY PLAN SELECT * FROM users;
```
```
SCAN users (cost=1000000.00 rows=1000000)
```

### 2. SELECT with WHERE

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 42;
```
```
SEARCH users USING INDEX idx_users_id (id=?) (cost=10.00 rows=1)
```

### 3. SELECT with JOIN

```sql
EXPLAIN QUERY PLAN
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id;
```
```
SCAN users (cost=1000000.00 rows=1000000)
  INNER JOIN (cost=11000000.00 rows=100000)
    SCAN orders (cost=1000000.00 rows=1000000)
```

### 4. SELECT with GROUP BY

```sql
EXPLAIN QUERY PLAN
SELECT department, COUNT(*)
FROM users
GROUP BY department;
```
```
SCAN users (cost=1000000.00 rows=1000000)
  USE TEMP B-TREE FOR GROUP BY (cost=1200000.00 rows=1000)
```

### 5. UPDATE

```sql
EXPLAIN QUERY PLAN
UPDATE users
SET status = 'active'
WHERE age > 18;
```
```
UPDATE users (cost=1150000.00 rows=100000)
  SCAN users (cost=1000000.00 rows=100000)
```

### 6. DELETE

```sql
EXPLAIN QUERY PLAN
DELETE FROM logs
WHERE created_at < '2024-01-01';
```
```
DELETE FROM logs (cost=1100000.00 rows=100000)
  SCAN logs (cost=1000000.00 rows=100000)
```

## Cost Formula Reference

| Operation | Formula | Example |
|-----------|---------|---------|
| Table Scan | 1.0 x total_rows | 1000000.00 |
| Index Unique Lookup | 10.0 | 10.00 |
| Index Scan (equality) | 0.5 x matching_rows | 5000.00 |
| Index Scan (range) | 0.7 x matching_rows | 7000.00 |
| Covering Index | 0.8 x index_cost | 4000.00 |
| Nested Loop (indexed) | left x (10 + 0.01xright) | 11000.00 |
| Nested Loop (no index) | left x right | 1000000000.00 |
| Aggregation | 1.2 x input_rows | 1200000.00 |
| Sort | 1.5 x input_rows | 1500000.00 |

## Comparing Query Plans

### Query A (No Index)
```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'john@example.com';
```
```
SCAN users (cost=1000000.00 rows=100000)
```
**Total Cost**: 1,000,000

### Query B (With Index)
```sql
CREATE INDEX idx_users_email ON users(email);
EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'john@example.com';
```
```
SEARCH users USING INDEX idx_users_email (email=?) (cost=10.00 rows=1)
```
**Total Cost**: 10

**Improvement**: 100,000x faster!

## Advanced: Reading Complex Plans

```sql
EXPLAIN QUERY PLAN
SELECT u.name, COUNT(o.id), SUM(o.total)
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.age > 18
GROUP BY u.name
HAVING COUNT(o.id) > 5
ORDER BY SUM(o.total) DESC
LIMIT 10;
```

Output (annotated):
```
id | parent | detail
0  | -1     | QUERY PLAN
1  | 0      |   SCAN users (cost=1000000.00 rows=100000)          <- Base scan with WHERE
2  | 1      |     INNER JOIN (cost=11000000.00 rows=100000)       <- Join orders
3  | 2      |       SCAN orders (cost=1000000.00 rows=1000000)    <- Right side of join
4  | 3      |     USE TEMP B-TREE FOR GROUP BY (cost=1200000.00)  <- Group aggregation
5  | 4      |     USE TEMP B-TREE FOR ORDER BY (cost=1500.00)     <- Sort results
```

**Reading order**: Bottom-up execution
1. Scan users (WHERE age > 18)
2. Scan orders
3. Join users + orders
4. Group by name, filter HAVING
5. Sort by SUM(total) DESC
6. Take LIMIT 10

**Total estimated cost**: ~13,201,500.00

## Troubleshooting

### "SCAN" instead of "SEARCH"?
- No index exists
- Index not usable for query
- Optimizer chose full scan (table too small)

### Very high cost (> 1 billion)?
- Cartesian product (missing JOIN condition)
- No index on join columns
- Check for cross joins

### Lower rows than expected?
- Default estimate (statistics not available)
- Run ANALYZE to update statistics (future feature)

### Cost seems wrong?
- Using default row estimates (1M rows)
- Set table statistics for accuracy
- Cost is relative, not absolute time

## See Also

- [EXPLAIN_QUERY_PLAN_COSTS.md](EXPLAIN_QUERY_PLAN_COSTS.md) - Detailed cost model
- [PHASE_9_3_IMPLEMENTATION.md](PHASE_9_3_IMPLEMENTATION.md) - Implementation details
- [examples/explain_query_plan_cost_example.go](../examples/explain_query_plan_cost_example.go) - Working examples
