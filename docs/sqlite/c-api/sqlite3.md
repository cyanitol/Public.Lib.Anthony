# sqlite3 — Database Connection Object

The `sqlite3` object represents an open SQLite database connection.
It is created by `sqlite3_open()` or `sqlite3_open_v2()` and closed by `sqlite3_close()`.

## Usage

```c
sqlite3 *db;
int rc = sqlite3_open("my.db", &db);
if (rc != SQLITE_OK) {
    // handle error
}
// ... use db ...
sqlite3_close(db);
```

Source: https://www.sqlite.org/c3ref/sqlite3.html
