# Conflict Resolution in SQLite

> See [CONFLICT_HANDLING.md](CONFLICT_HANDLING.md) for the complete conflict handling reference,
> and [LANG_CONFLICT.md](LANG_CONFLICT.md) for the ON CONFLICT clause language reference.

SQLite supports five conflict resolution algorithms that specify what to do when a
uniqueness or NOT NULL constraint is violated:

| Algorithm | Behavior |
|---|---|
| `ROLLBACK` | Abort and rollback the entire transaction |
| `ABORT` | Abort the statement; prior changes in transaction preserved (default) |
| `FAIL` | Abort the statement; prior changes preserved; no rollback |
| `IGNORE` | Skip the offending row; continue processing |
| `REPLACE` | Delete conflicting row(s), then insert the new row |

## Usage

```sql
-- In table definition
CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT REPLACE, val TEXT NOT NULL ON CONFLICT IGNORE);

-- In INSERT/UPDATE
INSERT OR REPLACE INTO t (id, val) VALUES (1, 'hello');
INSERT OR IGNORE INTO t (id, val) VALUES (1, 'ignored');
```

Source: https://www.sqlite.org/lang_conflict.html
