# sqlite3_stmt — Prepared Statement Object

The `sqlite3_stmt` object represents a single compiled SQL statement.
It is created by `sqlite3_prepare_v2()` and destroyed by `sqlite3_finalize()`.

## Lifecycle

```c
sqlite3_stmt *stmt;
sqlite3_prepare_v2(db, "SELECT * FROM t WHERE id = ?", -1, &stmt, NULL);
sqlite3_bind_int(stmt, 1, 42);
while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *val = (const char *)sqlite3_column_text(stmt, 0);
}
sqlite3_finalize(stmt);
```

Source: https://www.sqlite.org/c3ref/stmt.html
