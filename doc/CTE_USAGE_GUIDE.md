# CTE (Common Table Expressions) Usage Guide

## What are CTEs?

Common Table Expressions (CTEs) are temporary named result sets that exist only within the execution scope of a single SQL statement. They make complex queries more readable and maintainable by breaking them into logical components.

## Basic Syntax

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;
```

## Examples

### 1. Simple CTE

Replace a subquery with a named CTE for better readability:

**Before (with subquery):**
```sql
SELECT * FROM (
    SELECT id, name FROM users WHERE age >= 18
) AS adults
WHERE name LIKE 'A%';
```

**After (with CTE):**
```sql
WITH adults AS (
    SELECT id, name FROM users WHERE age >= 18
)
SELECT * FROM adults WHERE name LIKE 'A%';
```

### 2. Multiple CTEs

Chain multiple CTEs together, where later CTEs can reference earlier ones:

```sql
WITH
    active_users AS (
        SELECT id, name, email FROM users WHERE active = 1
    ),
    recent_orders AS (
        SELECT order_id, user_id, amount
        FROM orders
        WHERE order_date >= date('now', '-30 days')
    ),
    active_user_orders AS (
        SELECT o.order_id, u.name, o.amount
        FROM recent_orders o
        JOIN active_users u ON o.user_id = u.id
    )
SELECT * FROM active_user_orders ORDER BY amount DESC;
```

### 3. CTE with Explicit Column Names

Rename columns in the CTE for clarity:

```sql
WITH user_summary(user_id, full_name, order_count) AS (
    SELECT u.id, u.name, COUNT(o.id)
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.id, u.name
)
SELECT * FROM user_summary WHERE order_count > 5;
```

### 4. Recursive CTE - Number Sequence

Generate a sequence of numbers:

```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 100
)
SELECT * FROM numbers;
```

### 5. Recursive CTE - Hierarchical Data

Traverse a tree or hierarchy (e.g., organizational chart):

```sql
-- Table: employees(id, name, manager_id)

WITH RECURSIVE org_chart AS (
    -- Anchor: Start with the CEO
    SELECT id, name, manager_id, 0 as level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: Add subordinates
    SELECT e.id, e.name, e.manager_id, oc.level + 1
    FROM employees e
    JOIN org_chart oc ON e.manager_id = oc.id
)
SELECT
    id,
    REPEAT('  ', level) || name as name,
    level
FROM org_chart
ORDER BY level, name;
```

### 6. Recursive CTE - Graph Traversal

Find all connected nodes in a graph:

```sql
-- Table: connections(from_id, to_id)

WITH RECURSIVE reachable(node_id) AS (
    -- Anchor: Start with node 1
    SELECT 1 as node_id

    UNION

    -- Recursive: Add connected nodes
    SELECT c.to_id
    FROM connections c
    JOIN reachable r ON c.from_id = r.node_id
)
SELECT DISTINCT node_id FROM reachable;
```

### 7. Recursive CTE - Date Series

Generate a series of dates:

```sql
WITH RECURSIVE date_series(d) AS (
    SELECT date('2024-01-01')
    UNION ALL
    SELECT date(d, '+1 day')
    FROM date_series
    WHERE d < date('2024-12-31')
)
SELECT d as date, strftime('%w', d) as day_of_week
FROM date_series;
```

### 8. Recursive CTE - Fibonacci Sequence

Generate mathematical sequences:

```sql
WITH RECURSIVE fibonacci(n, fib_n, fib_n1) AS (
    -- Anchor: F(0) = 0, F(1) = 1
    SELECT 0, 0, 1

    UNION ALL

    -- Recursive: F(n) = F(n-1) + F(n-2)
    SELECT n + 1, fib_n1, fib_n + fib_n1
    FROM fibonacci
    WHERE n < 20
)
SELECT n, fib_n FROM fibonacci;
```

## Advanced Patterns

### CTE for Data Transformation Pipeline

```sql
WITH
    -- Step 1: Clean raw data
    cleaned_data AS (
        SELECT
            TRIM(name) as name,
            LOWER(email) as email,
            CAST(age AS INTEGER) as age
        FROM raw_users
        WHERE email IS NOT NULL
    ),
    -- Step 2: Deduplicate
    deduped_data AS (
        SELECT DISTINCT * FROM cleaned_data
    ),
    -- Step 3: Enrich with additional info
    enriched_data AS (
        SELECT
            d.*,
            c.country,
            c.timezone
        FROM deduped_data d
        LEFT JOIN countries c ON SUBSTR(d.email, -2) = c.code
    )
SELECT * FROM enriched_data;
```

### CTE for Pivot/Unpivot Operations

```sql
-- Unpivot example
WITH unpivoted AS (
    SELECT id, 'Q1' as quarter, q1_sales as sales FROM quarterly_sales
    UNION ALL
    SELECT id, 'Q2', q2_sales FROM quarterly_sales
    UNION ALL
    SELECT id, 'Q3', q3_sales FROM quarterly_sales
    UNION ALL
    SELECT id, 'Q4', q4_sales FROM quarterly_sales
)
SELECT quarter, SUM(sales) as total_sales
FROM unpivoted
GROUP BY quarter;
```

### Recursive CTE with Path Tracking

```sql
-- Track the full path in hierarchical data
WITH RECURSIVE paths AS (
    SELECT
        id,
        name,
        manager_id,
        name as path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT
        e.id,
        e.name,
        e.manager_id,
        p.path || ' > ' || e.name
    FROM employees e
    JOIN paths p ON e.manager_id = p.id
)
SELECT id, name, path FROM paths;
```

## Best Practices

### 1. Use CTEs for Readability
Break complex queries into logical steps that are easy to understand.

### 2. Name CTEs Descriptively
Use clear, descriptive names that explain what the CTE contains:
- [x] `active_customers`, `high_value_orders`, `recent_activity`
- [fail] `temp1`, `t`, `cte`

### 3. Order CTEs by Dependency
Place CTEs that others depend on first:
```sql
WITH
    base_data AS (...),           -- No dependencies
    filtered_data AS (             -- Depends on base_data
        SELECT * FROM base_data ...
    ),
    aggregated_data AS (           -- Depends on filtered_data
        SELECT ... FROM filtered_data ...
    )
SELECT * FROM aggregated_data;
```

### 4. Use Column Lists When Renaming
Be explicit about column names when the CTE's purpose is transformation:
```sql
WITH user_stats(user_id, total_spent, order_count) AS (
    SELECT user_id, SUM(amount), COUNT(*)
    FROM orders
    GROUP BY user_id
)
SELECT * FROM user_stats;
```

### 5. Limit Recursive CTE Depth
Always include a termination condition to prevent infinite loops:
```sql
WITH RECURSIVE hierarchy AS (
    SELECT id, parent_id, 0 as depth FROM nodes WHERE parent_id IS NULL
    UNION ALL
    SELECT n.id, n.parent_id, h.depth + 1
    FROM nodes n
    JOIN hierarchy h ON n.parent_id = h.id
    WHERE h.depth < 100  -- Limit depth
)
SELECT * FROM hierarchy;
```

### 6. Reference CTEs Multiple Times
A CTE can be referenced multiple times in the same query:
```sql
WITH expensive_items AS (
    SELECT * FROM products WHERE price > 1000
)
SELECT
    (SELECT COUNT(*) FROM expensive_items) as total_count,
    (SELECT AVG(price) FROM expensive_items) as avg_price,
    *
FROM expensive_items
LIMIT 10;
```

## Common Use Cases

### 1. Deduplication
```sql
WITH ranked_rows AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) as rn
    FROM users
)
SELECT * FROM ranked_rows WHERE rn = 1;
```

### 2. Running Totals
```sql
WITH daily_sales AS (
    SELECT
        date,
        SUM(amount) as daily_total,
        SUM(SUM(amount)) OVER (ORDER BY date) as running_total
    FROM orders
    GROUP BY date
)
SELECT * FROM daily_sales;
```

### 3. Gap Detection
```sql
WITH numbered_records AS (
    SELECT
        id,
        id - ROW_NUMBER() OVER (ORDER BY id) as gap_group
    FROM records
)
SELECT
    MIN(id) as gap_start,
    MAX(id) as gap_end
FROM numbered_records
GROUP BY gap_group;
```

### 4. Tree Flattening
```sql
WITH RECURSIVE flattened_tree AS (
    SELECT id, parent_id, name, CAST(id AS TEXT) as path
    FROM categories
    WHERE parent_id IS NULL

    UNION ALL

    SELECT c.id, c.parent_id, c.name, f.path || '/' || c.id
    FROM categories c
    JOIN flattened_tree f ON c.parent_id = f.id
)
SELECT * FROM flattened_tree;
```

## Performance Considerations

### Materialization
CTEs are typically materialized (computed once and stored temporarily), which can be good or bad:

**Good:** CTE used multiple times
```sql
WITH expensive_cte AS (
    SELECT ... FROM large_table ... -- Computed once
)
SELECT COUNT(*) FROM expensive_cte
UNION ALL
SELECT AVG(value) FROM expensive_cte;  -- Reuses materialized result
```

**Bad:** CTE adds overhead if used once
```sql
WITH simple_filter AS (
    SELECT * FROM users WHERE active = 1
)
SELECT * FROM simple_filter LIMIT 10;  -- Might be faster without CTE
```

### Recursive CTE Limits
- Maximum recursion depth: 1000 iterations (configurable)
- Use WHERE conditions to terminate early
- Monitor performance on large datasets

## Debugging CTEs

### View Intermediate Results
Test each CTE individually:
```sql
-- Test just the first CTE
WITH first_step AS (
    SELECT ...
)
SELECT * FROM first_step;  -- Verify this works

-- Then add more
WITH
    first_step AS (...),
    second_step AS (...)
SELECT * FROM second_step;  -- Verify second step
```

### Add Debug Columns
Include debugging information:
```sql
WITH RECURSIVE hierarchy AS (
    SELECT id, parent_id, 0 as level, id::TEXT as path
    FROM nodes
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
        n.id,
        n.parent_id,
        h.level + 1,
        h.path || ' > ' || n.id
    FROM nodes n
    JOIN hierarchy h ON n.parent_id = h.id
    WHERE h.level < 10
)
SELECT * FROM hierarchy;  -- The 'level' and 'path' help debug
```

## Conclusion

CTEs are a powerful tool for writing clear, maintainable SQL queries. Use them to:
- Break down complex queries into steps
- Improve query readability
- Reuse intermediate results
- Traverse hierarchical data
- Generate sequences and series

Start simple and build up complexity as needed. Your future self (and teammates) will thank you for the clarity!

## See Also

- [SQLite CTE Documentation](https://sqlite.org/lang_with.html) - Official SQLite WITH clause reference
