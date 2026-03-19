# CTE Quick Reference

A compact syntax reminder for Common Table Expressions.

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;
```

```sql
WITH RECURSIVE cte_name AS (
    SELECT ...
    UNION ALL
    SELECT ... FROM cte_name
)
SELECT * FROM cte_name;
```

## Next Steps

- [CTE Usage Guide](CTE_USAGE_GUIDE.md)
- [SQL Language](SQL_LANGUAGE.md)
- [SQLite WITH Reference](sqlite/WITH_CTE.md)
- [Documentation Index](INDEX.md)
