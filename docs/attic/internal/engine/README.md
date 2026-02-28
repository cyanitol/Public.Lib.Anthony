# Engine Package

The `engine` package provides the top-level integration for the SQLite database engine, tying together all core components into a cohesive whole.

## Overview

The engine package is the main entry point for using the SQLite database. It coordinates:

- **Pager**: Low-level page I/O and transaction management
- **B-tree**: Data structure for tables and indexes
- **Schema**: Table and index metadata
- **Parser**: SQL parsing
- **VDBE**: Virtual machine for bytecode execution
- **Functions**: Built-in SQL functions
- **Compiler**: SQL to VDBE bytecode compilation

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Engine                            │
│  ┌──────────────────────────────────────────────┐   │
│  │              SQL Input                       │   │
│  └────────────────┬─────────────────────────────┘   │
│                   │                                  │
│                   ▼                                  │
│  ┌──────────────────────────────────────────────┐   │
│  │              Parser                          │   │
│  │         (SQL → AST)                          │   │
│  └────────────────┬─────────────────────────────┘   │
│                   │                                  │
│                   ▼                                  │
│  ┌──────────────────────────────────────────────┐   │
│  │              Compiler                        │   │
│  │         (AST → VDBE Bytecode)                │   │
│  └────────────────┬─────────────────────────────┘   │
│                   │                                  │
│                   ▼                                  │
│  ┌──────────────────────────────────────────────┐   │
│  │              VDBE Executor                   │   │
│  │    (Execute Bytecode)                        │   │
│  └────────────┬─────────────────┬───────────────┘   │
│               │                 │                    │
│               ▼                 ▼                    │
│  ┌────────────────┐   ┌────────────────┐            │
│  │    B-tree      │   │   Functions    │            │
│  │  (Data Access) │   │   Registry     │            │
│  └────────┬───────┘   └────────────────┘            │
│           │                                          │
│           ▼                                          │
│  ┌────────────────┐                                 │
│  │     Pager      │                                 │
│  │  (Page I/O)    │                                 │
│  └────────────────┘                                 │
└─────────────────────────────────────────────────────┘
```

## Components

### Engine

The main database engine that coordinates all components.

**Key Methods:**

- `Open(filename)` - Open or create a database
- `Close()` - Close the database
- `Execute(sql)` - Execute a SQL statement
- `Query(sql)` - Execute a query and return rows
- `Exec(sql)` - Execute a statement and return affected rows
- `Begin()` - Start a transaction
- `Prepare(sql)` - Prepare a statement for reuse

### Compiler

Compiles SQL AST to VDBE bytecode.

**Compilation Pipeline:**

1. Parse SQL → AST (done by parser)
2. Analyze and validate AST
3. Generate VDBE bytecode
4. Optimize bytecode (future)

**Supported Statements:**

- `SELECT` - Query data
- `INSERT` - Insert rows
- `UPDATE` - Update rows
- `DELETE` - Delete rows
- `CREATE TABLE` - Create tables
- `CREATE INDEX` - Create indexes
- `DROP TABLE` - Drop tables
- `DROP INDEX` - Drop indexes
- `BEGIN/COMMIT/ROLLBACK` - Transactions

### Result

Represents the result of executing a SQL statement.

**Fields:**

- `Columns` - Column names
- `Rows` - Result rows (for SELECT)
- `RowsAffected` - Number of rows affected (for INSERT/UPDATE/DELETE)
- `LastInsertID` - Last inserted rowid

### Rows

Iterator over query results (similar to database/sql.Rows).

**Methods:**

- `Next()` - Advance to next row
- `Scan(dest...)` - Scan current row into variables
- `Close()` - Close the iterator
- `Columns()` - Get column names
- `Err()` - Get any error that occurred

### Tx

Represents a database transaction.

**Methods:**

- `Commit()` - Commit the transaction
- `Rollback()` - Rollback the transaction
- `Execute(sql)` - Execute within transaction
- `Query(sql)` - Query within transaction
- `Exec(sql)` - Execute within transaction

### PreparedStmt

Represents a prepared statement for reuse.

**Methods:**

- `Execute(params...)` - Execute with parameters
- `Query(params...)` - Query with parameters
- `Close()` - Close and release resources
- `SQL()` - Get the SQL text

## Usage Examples

### Basic Operations

```go
// Open database
db, err := engine.Open("mydb.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Create table
_, err = db.Execute(`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    )
`)

// Insert data
_, err = db.Execute(`
    INSERT INTO users (name, email)
    VALUES ('Alice', 'alice@example.com')
`)

// Query data
rows, err := db.Query(`SELECT id, name, email FROM users`)
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
    fmt.Printf("%d: %s <%s>\n", id, name, email)
}
```

### Transactions

```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute statements in transaction
_, err = tx.Execute(`INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')`)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Execute(`INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')`)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit transaction
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

### Prepared Statements

```go
// Prepare statement
stmt, err := db.Prepare(`INSERT INTO users (name, email) VALUES (?, ?)`)
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute multiple times
users := []struct{name, email string}{
    {"Dave", "dave@example.com"},
    {"Eve", "eve@example.com"},
}

for _, user := range users {
    _, err := stmt.Execute(user.name, user.email)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Query Single Row

```go
var count int
err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Total users: %d\n", count)
```

## Implementation Status

### Completed

- ✅ Basic engine structure
- ✅ Database open/close
- ✅ CREATE TABLE compilation
- ✅ INSERT compilation (basic)
- ✅ SELECT compilation (basic)
- ✅ DROP TABLE compilation
- ✅ CREATE/DROP INDEX compilation
- ✅ Transaction management (BEGIN/COMMIT/ROLLBACK)
- ✅ Result iteration (Rows)
- ✅ Prepared statements (basic)

### Partial

- ⚠️ SELECT with WHERE clause (needs expression compilation)
- ⚠️ UPDATE compilation (stub)
- ⚠️ DELETE compilation (stub)
- ⚠️ Schema persistence (in-memory only)
- ⚠️ Parameter binding (not implemented)

### TODO

- ❌ JOIN compilation
- ❌ Subquery compilation
- ❌ GROUP BY / HAVING compilation
- ❌ ORDER BY compilation
- ❌ LIMIT / OFFSET compilation
- ❌ Aggregate function compilation
- ❌ Index usage in queries
- ❌ Query optimization
- ❌ Schema loading from sqlite_master
- ❌ Savepoints
- ❌ Concurrent access control
- ❌ Write-ahead logging (WAL)

## Design Decisions

### 1. Separation of Concerns
Each component has a clear responsibility:

- **Engine**: Coordination and API
- **Compiler**: SQL to bytecode translation
- **VDBE**: Bytecode execution
- **Pager**: Storage management

### 2. Simplified Transaction Model
Currently implements a basic two-phase commit:

1. Modifications go to journal
2. Commit writes journal and syncs
3. Rollback restores from journal

Full MVCC and WAL are future enhancements.

### 3. In-Memory Schema
Schema is currently kept in memory only. Persistence to sqlite_master table is planned but not yet implemented.

### 4. Register-Based VDBE
The VDBE uses registers for intermediate values, similar to SQLite's implementation. This is more efficient than a stack-based design for complex queries.

### 5. Error Handling
Errors are propagated up the stack. The VDBE halts on error, and the error is returned to the caller.

## Testing

The package includes comprehensive integration tests covering:

- Database creation and opening
- Table creation and dropping
- Data insertion and querying
- Transactions (commit and rollback)
- Multiple tables
- Indexes
- Prepared statements
- Concurrent access
- Read-only mode

Run tests:
```bash
go test -v ./core/sqlite/internal/engine
```

## Performance Considerations

### Current Limitations

1. **No Query Optimization**: Queries use simple table scans
2. **No Index Usage**: Indexes are created but not used in queries
3. **Inefficient Sorting**: No sorter implementation yet
4. **Limited Caching**: Basic page cache only

### Future Optimizations

1. Query planning and optimization
2. Index selection and usage
3. Join algorithms (nested loop, hash, merge)
4. Statistics collection
5. Cost-based optimization

## Thread Safety

The engine uses a mutex to protect transaction state. Multiple readers can access the database concurrently, but writes are serialized.

**Note**: Full MVCC (Multi-Version Concurrency Control) is not yet implemented.

## Compatibility

This implementation aims for compatibility with SQLite 3 at the SQL level, but the internal format and wire protocol are custom. Future versions may add SQLite file format compatibility.

## References

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [SQLite VDBE Documentation](https://www.sqlite.org/opcode.html)
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
