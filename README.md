# Public.Lib.Anthony

A pure Go SQLite implementation for the JuniperBible project. This library provides a complete SQLite database engine written entirely in Go with no CGO dependencies.

## Installation

```bash
go get github.com/JuniperBible/Public.Lib.Anthony
```

## Usage

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    db, err := sql.Open("sqlite_internal", "mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create a table
    _, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    _, err = db.Exec(`INSERT INTO users (name) VALUES (?)`, "Alice")
    if err != nil {
        log.Fatal(err)
    }

    // Query data
    rows, err := db.Query(`SELECT id, name FROM users`)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name string
        rows.Scan(&id, &name)
        log.Printf("User: %d, %s", id, name)
    }
}
```

## Package Structure

- `internal/btree` - B-tree storage engine
- `internal/driver` - database/sql driver interface
- `internal/engine` - Query execution engine
- `internal/expr` - Expression evaluation
- `internal/format` - SQLite file format utilities
- `internal/functions` - Built-in SQL functions (scalar, aggregate, date, math)
- `internal/pager` - Page cache, journal, and transaction management
- `internal/parser` - SQL lexer, parser, and AST
- `internal/planner` - Query optimizer
- `internal/schema` - Database schema management
- `internal/sql` - SQL statement compilation (SELECT, INSERT, UPDATE, DELETE, DDL)
- `internal/utf` - UTF-8/UTF-16 encoding and collation
- `internal/vdbe` - Virtual Database Engine (bytecode VM)

## Driver Name

The driver registers as `sqlite_internal` to avoid conflicts with other SQLite drivers.

## License

Public Domain (SQLite License)

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.
