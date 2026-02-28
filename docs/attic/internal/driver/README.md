# SQLite database/sql Driver Implementation

This package implements the Go `database/sql` driver interface for SQLite, integrating all internal SQLite components into a cohesive database engine.

## Architecture

The driver integrates the following internal packages:

- **pager**: Low-level page cache and file I/O
- **btree**: B-tree storage engine for tables and indexes
- **parser**: SQL parser that generates AST
- **vdbe**: Virtual Database Engine (bytecode interpreter)
- **planner**: Query planner and optimizer
- **expr**: Expression evaluation
- **functions**: Built-in SQL functions
- **utf**: UTF-8/UTF-16 and collation support

## Components

### Driver (`driver.go`)

- Implements `database/sql/driver.Driver`
- Registers with `sql.Register("sqlite", ...)`
- Manages connection pool
- Handles database file opening

### Connection (`conn.go`)

- Implements `database/sql/driver.Conn`
- Manages pager and btree instances
- Handles transaction lifecycle
- Prepares SQL statements

### Statement (`stmt.go`)

- Implements `database/sql/driver.Stmt`
- Compiles SQL to VDBE bytecode
- Binds parameters
- Executes queries and commands

### Rows (`rows.go`)

- Implements `database/sql/driver.Rows`
- Iterates over query results
- Converts VDBE memory cells to Go values

### Transaction (`tx.go`)

- Implements `database/sql/driver.Tx`
- Manages atomic commits and rollbacks
- Integrates with pager journaling

### Value (`value.go`)

- Type conversion utilities
- Converts Go types to SQLite values
- Implements `driver.Result`

## Execution Flow

### Opening a Database
```
sql.Open("sqlite", "database.db")
  ↓
Driver.Open(name)
  ↓
pager.Open(filename) → Creates/opens database file
  ↓
btree.NewBtree() → Initializes B-tree layer
  ↓
Returns Conn
```

### Preparing a Statement
```
db.Prepare("SELECT * FROM users WHERE id = ?")
  ↓
Conn.Prepare(query)
  ↓
parser.Parse(query) → Generates AST
  ↓
Returns Stmt
```

### Executing a Query
```
stmt.Query(args...)
  ↓
Stmt.compile(args) → Generates VDBE bytecode
  ↓
planner → Optimizes query plan
  ↓
codegen → Emits opcodes
  ↓
Returns Rows with VDBE instance
```

### Iterating Results
```
rows.Next()
  ↓
VDBE.Step() → Executes one instruction
  ↓
VDBE instructions:

  - OpOpenRead → Open B-tree cursor
  - OpRewind → Position at first record
  - OpColumn → Extract column values
  - OpResultRow → Return row
  - OpNext → Advance to next record
  ↓
Converts vdbe.Mem → driver.Value
  ↓
Returns values to application
```

## Current Implementation Status

### Implemented

- [x] Driver registration and initialization
- [x] Connection management
- [x] Statement preparation
- [x] Query execution framework
- [x] Transaction support
- [x] Type conversion

### In Progress

- [ ] Complete VDBE bytecode generation for all SQL statements
- [ ] B-tree cursor integration
- [ ] Schema table management
- [ ] Index support
- [ ] Parameter binding

### Future Work

- [ ] Aggregate functions
- [ ] Subqueries
- [ ] Joins
- [ ] Window functions
- [ ] Full-text search
- [ ] Virtual tables

## Usage Example

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/yourusername/JuniperBible/core/sqlite/internal/driver"
)

func main() {
    // Open database
    db, err := sql.Open("sqlite", "test.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create table
    _, err = db.Exec(`
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    result, err := db.Exec(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "John Doe",
        "john@example.com",
    )
    if err != nil {
        log.Fatal(err)
    }

    id, _ := result.LastInsertId()
    fmt.Printf("Inserted user with ID: %d\n", id)

    // Query data
    rows, err := db.Query("SELECT id, name, email FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name, email string
        if err := rows.Scan(&id, &name, &email); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("User: %d, %s, %s\n", id, name, email)
    }
}
```

## Testing

Run the driver tests:

```bash
go test -v ./internal/driver/...
```

Run integration tests:

```bash
go test -v ./internal/driver/integration/...
```

## Performance Considerations

1. **Connection Pooling**: Use `db.SetMaxOpenConns()` to control connection pool size
2. **Prepared Statements**: Reuse prepared statements for repeated queries
3. **Transactions**: Batch writes in transactions for better performance
4. **Indexes**: Create indexes on frequently queried columns

## Compatibility

This driver aims to be compatible with:

- SQLite 3.x file format
- Go 1.19+
- Standard `database/sql` package

## Differences from modernc.org/sqlite

- Pure Go implementation from scratch
- Educational and transparent codebase
- Modular architecture
- Extensible design
- May have different performance characteristics

## Contributing

To add support for new SQL features:

1. Update the parser to recognize the syntax
2. Add AST node types
3. Implement planner logic
4. Generate VDBE opcodes in stmt.go
5. Add tests

## License

See LICENSE file in the root directory.
