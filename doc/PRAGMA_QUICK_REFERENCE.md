# PRAGMA Quick Reference

This page provides a short list of common PRAGMAs. For full details, see
[PRAGMAS](PRAGMAS.md) and the [SQLite PRAGMA Reference](sqlite/PRAGMA_REFERENCE.md).

## Common PRAGMAs

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -65536;  -- 64MB
PRAGMA temp_store = MEMORY;
PRAGMA user_version = 1;
```

## Schema Inspection

```sql
PRAGMA table_info('my_table');
PRAGMA index_list('my_table');
PRAGMA foreign_key_list('my_table');
```

## Next Steps

- [PRAGMAS](PRAGMAS.md)
- [API Reference](API.md)
- [Documentation Index](INDEX.md)
