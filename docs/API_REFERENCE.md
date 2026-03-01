# Anthony API Reference

Anthony is a pure Go implementation of SQLite that provides a fully compatible `database/sql` driver interface. This reference documents how to use Anthony in Go applications, mapping SQLite C API concepts to Go's `database/sql` patterns.

## Table of Contents

1. [Opening and Closing Databases](#opening-and-closing-databases)
2. [Executing Queries](#executing-queries)
3. [Prepared Statements](#prepared-statements)
4. [Transactions](#transactions)
5. [Row Scanning](#row-scanning)
6. [Parameter Binding](#parameter-binding)
7. [Error Handling](#error-handling)
8. [Connection Management](#connection-management)

---

## Opening and Closing Databases

### Opening a Database Connection

Anthony implements the standard Go `database/sql` interface. Use `sql.Open()` to create a database connection.

**Basic Usage:**

```go
import (
    "database/sql"
    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    db, err := sql.Open("sqlite_internal", "mydata.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Verify the connection
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
}
```

**Convenience Functions:**

Anthony provides helper functions for common use cases:

```go
import "github.com/JuniperBible/Public.Lib.Anthony"

// Open a database with default settings
db, err := anthony.Open("mydata.db")

// Open in read-only mode
db, err := anthony.OpenReadOnly("mydata.db")
```

**Database Modes:**

Anthony supports SQLite URI parameters for advanced configuration:

- **Read-only mode**: `mydata.db?mode=ro`
- **Read-write mode**: `mydata.db?mode=rw`
- **Read-write with create**: `mydata.db?mode=rwc` (default)
- **In-memory database**: `:memory:` or `file::memory:`

**Example with URI parameters:**

```sql
db, err := sql.Open("sqlite_internal", "mydata.db?mode=ro&cache=shared")
```

### Closing a Database Connection

Always close database connections when finished to release resources. Use `defer` to ensure cleanup:

```go
db, err := sql.Open("sqlite_internal", "mydata.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Use the database...
```

**Important Notes:**

- `db.Close()` will wait for all queries to finish before closing
- Prepared statements should be closed before closing the database
- Closing a `nil` database pointer is safe (no-op)
- Pending transactions are automatically rolled back on close

---

## Executing Queries

### Simple Query Execution (db.Exec)

Use `db.Exec()` for SQL statements that don't return rows (INSERT, UPDATE, DELETE, CREATE, etc.):

```go
// Create a table
_, err := db.Exec(`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    )
`)
if err != nil {
    log.Fatal(err)
}

// Insert data
result, err := db.Exec(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    "Alice", "alice@example.com",
)
if err != nil {
    log.Fatal(err)
}

// Get affected rows
rowsAffected, err := result.RowsAffected()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Inserted %d row(s)\n", rowsAffected)

// Get last insert ID
lastID, err := result.LastInsertId()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("New user ID: %d\n", lastID)
```

### Querying Multiple Rows (db.Query)

Use `db.Query()` to retrieve multiple rows:

```go
rows, err := db.Query("SELECT id, name, email FROM users WHERE id > ?", 10)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string

    err := rows.Scan(&id, &name, &email)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("User %d: %s (%s)\n", id, name, email)
}

// Check for errors during iteration
if err = rows.Err(); err != nil {
    log.Fatal(err)
}
```

### Querying a Single Row (db.QueryRow)

Use `db.QueryRow()` when you expect exactly one row:

```go
var count int
err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Total users: %d\n", count)

// Querying by ID
var name, email string
err = db.QueryRow(
    "SELECT name, email FROM users WHERE id = ?",
    42,
).Scan(&name, &email)
if err == sql.ErrNoRows {
    fmt.Println("User not found")
} else if err != nil {
    log.Fatal(err)
} else {
    fmt.Printf("User: %s (%s)\n", name, email)
}
```

---

## Prepared Statements

Prepared statements are compiled SQL queries that can be executed multiple times with different parameters, improving performance and security.

### Creating Prepared Statements

Use `db.Prepare()` to create a prepared statement:

```go
stmt, err := db.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute the statement multiple times
users := []struct{ name, email string }{
    {"Alice", "alice@example.com"},
    {"Bob", "bob@example.com"},
    {"Charlie", "charlie@example.com"},
}

for _, user := range users {
    _, err := stmt.Exec(user.name, user.email)
    if err != nil {
        log.Printf("Failed to insert %s: %v", user.name, err)
    }
}
```

### Querying with Prepared Statements

Prepared statements can execute queries that return rows:

```go
stmt, err := db.Prepare("SELECT name, email FROM users WHERE id = ?")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Query for multiple IDs
for _, id := range []int{1, 2, 3} {
    var name, email string
    err := stmt.QueryRow(id).Scan(&name, &email)
    if err == sql.ErrNoRows {
        fmt.Printf("User %d not found\n", id)
        continue
    } else if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("User %d: %s (%s)\n", id, name, email)
}
```

### Prepared Statements in Transactions

Prepared statements can be used within transactions:

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

stmt, err := tx.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}
defer stmt.Close()

for _, user := range users {
    _, err := stmt.Exec(user.name, user.email)
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
}

if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

**Best Practices:**

- Always close prepared statements with `defer stmt.Close()`
- Reuse prepared statements when executing the same query multiple times
- Prepare statements are bound to the connection/transaction that created them
- Parameter placeholders use `?` in SQLite (positional parameters)

---

## Transactions

Transactions group multiple SQL operations into an atomic unit. All operations succeed together, or all are rolled back.

### Basic Transaction Usage

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute operations within transaction
_, err = tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE user_id = ?", 1)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

### Transaction Rollback

Rollback a transaction to undo all changes:

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

defer func() {
    if p := recover(); p != nil {
        tx.Rollback()
        panic(p) // Re-throw panic after rollback
    }
}()

// Perform operations...
result, err := tx.Exec("DELETE FROM users WHERE inactive = 1")
if err != nil {
    tx.Rollback()
    return err
}

rowsDeleted, _ := result.RowsAffected()
if rowsDeleted > 1000 {
    // Safety check: don't delete too many rows
    tx.Rollback()
    return errors.New("too many rows would be deleted")
}

// All checks passed, commit
return tx.Commit()
```

### Transaction Isolation

Anthony supports SQLite transaction isolation modes:

```sql
// Start a transaction with deferred locking (default)
tx, err := db.Begin()

// For immediate locking:
tx, err := db.Begin()
_, err = tx.Exec("BEGIN IMMEDIATE")

// For exclusive locking:
tx, err := db.Begin()
_, err = tx.Exec("BEGIN EXCLUSIVE")
```

### Savepoints

Use savepoints for nested transaction-like behavior:

```sql
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback() // Safety net

// Create a savepoint
_, err = tx.Exec("SAVEPOINT sp1")
if err != nil {
    log.Fatal(err)
}

// Do some work
_, err = tx.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
if err != nil {
    // Rollback to savepoint
    tx.Exec("ROLLBACK TO sp1")
} else {
    // Release savepoint
    tx.Exec("RELEASE sp1")
}

// Commit entire transaction
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

**Transaction Best Practices:**

- Keep transactions short to minimize lock contention
- Always handle errors and rollback on failure
- Use `defer tx.Rollback()` as a safety net (harmless if already committed)
- Avoid long-running operations within transactions
- SQLite uses database-level locking (one writer at a time)

---

## Row Scanning

### Basic Scanning

The `Scan()` method reads column values from a result row into Go variables:

```go
var id int
var name string
var balance float64
var active bool

err := db.QueryRow(
    "SELECT id, name, balance, active FROM accounts WHERE id = ?",
    123,
).Scan(&id, &name, &balance, &active)

if err != nil {
    log.Fatal(err)
}
```

### Scanning NULL Values

Use `sql.Null*` types to handle nullable columns:

```go
var id int
var name string
var email sql.NullString  // May be NULL
var age sql.NullInt64     // May be NULL

err := db.QueryRow(
    "SELECT id, name, email, age FROM users WHERE id = ?",
    123,
).Scan(&id, &name, &email, &age)

if err != nil {
    log.Fatal(err)
}

if email.Valid {
    fmt.Printf("Email: %s\n", email.String)
} else {
    fmt.Println("Email: NULL")
}

if age.Valid {
    fmt.Printf("Age: %d\n", age.Int64)
} else {
    fmt.Println("Age: NULL")
}
```

**Available NULL types:**

- `sql.NullString` - for TEXT columns
- `sql.NullInt64` - for INTEGER columns
- `sql.NullFloat64` - for REAL columns
- `sql.NullBool` - for boolean INTEGER columns
- `sql.NullTime` - for timestamp columns

### Scanning BLOB Data

BLOBs are scanned as byte slices:

```go
var id int
var data []byte

err := db.QueryRow(
    "SELECT id, image_data FROM images WHERE id = ?",
    456,
).Scan(&id, &data)

if err != nil {
    log.Fatal(err)
}

fmt.Printf("Image ID: %d, Size: %d bytes\n", id, len(data))
```

### Scanning Multiple Rows

Use `rows.Next()` to iterate through result sets:

```go
rows, err := db.Query("SELECT id, name, email FROM users ORDER BY id")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string

    if err := rows.Scan(&id, &name, &email); err != nil {
        log.Fatal(err)
    }

    fmt.Printf("%d: %s <%s>\n", id, name, email)
}

// Check for errors during iteration
if err := rows.Err(); err != nil {
    log.Fatal(err)
}
```

### Column Information

Access metadata about result columns:

```go
rows, err := db.Query("SELECT * FROM users LIMIT 1")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Get column names
columns, err := rows.Columns()
if err != nil {
    log.Fatal(err)
}

fmt.Println("Columns:", columns)

// Get column types (if supported)
columnTypes, err := rows.ColumnTypes()
if err != nil {
    log.Fatal(err)
}

for _, ct := range columnTypes {
    fmt.Printf("Column: %s, Type: %s\n", ct.Name(), ct.DatabaseTypeName())
}
```

**Scanning Best Practices:**

- Always check `rows.Err()` after iterating
- Ensure the number of Scan arguments matches the number of columns
- Use appropriate types for SQLite data types:
  - INTEGER -> `int`, `int64`
  - REAL -> `float64`
  - TEXT -> `string`
  - BLOB -> `[]byte`
- Use `sql.Null*` types for nullable columns to avoid errors

---

## Parameter Binding

Parameter binding (using placeholders) prevents SQL injection and improves performance.

### Positional Parameters

SQLite uses `?` as a positional parameter placeholder:

```sql
// Single parameter
result, err := db.Exec(
    "DELETE FROM users WHERE id = ?",
    123,
)

// Multiple parameters
result, err := db.Exec(
    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
    "Alice", "alice@example.com", 30,
)

// Parameters in WHERE clause
rows, err := db.Query(
    "SELECT * FROM products WHERE price > ? AND category = ?",
    19.99, "electronics",
)
```

### Named Parameters

SQLite also supports named parameters with `:name`, `@name`, or `$name` syntax:

```sql
// Using named parameters with :name syntax
_, err := db.Exec(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    sql.Named("name", "Bob"),
    sql.Named("email", "bob@example.com"),
)

// Using @name syntax
_, err = db.Exec(
    "UPDATE users SET email = @email WHERE id = @id",
    sql.Named("email", "newemail@example.com"),
    sql.Named("id", 42),
)
```

### Parameter Types

Anthony automatically handles Go types and converts them to appropriate SQLite types:

```go
// Integer types
db.Exec("INSERT INTO data (int_col) VALUES (?)", 42)
db.Exec("INSERT INTO data (int_col) VALUES (?)", int64(1234567890))

// Float types
db.Exec("INSERT INTO data (real_col) VALUES (?)", 3.14159)
db.Exec("INSERT INTO data (real_col) VALUES (?)", float32(2.71))

// String types
db.Exec("INSERT INTO data (text_col) VALUES (?)", "Hello, World!")

// Boolean (stored as INTEGER 0 or 1)
db.Exec("INSERT INTO data (bool_col) VALUES (?)", true)

// Byte slice (BLOB)
db.Exec("INSERT INTO data (blob_col) VALUES (?)", []byte{0x00, 0x01, 0x02})

// NULL values
db.Exec("INSERT INTO data (nullable_col) VALUES (?)", nil)
db.Exec("INSERT INTO data (nullable_col) VALUES (?)", sql.NullString{})
```

### Binding in Prepared Statements

Parameters are bound when executing prepared statements:

```sql
stmt, err := db.Prepare("INSERT INTO logs (level, message, timestamp) VALUES (?, ?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute with different parameters
stmt.Exec("INFO", "Application started", time.Now())
stmt.Exec("WARN", "High memory usage", time.Now())
stmt.Exec("ERROR", "Connection failed", time.Now())
```

### Array/Slice Binding

Use the `IN` clause with multiple parameters:

```go
ids := []int{1, 2, 3, 4, 5}

// Build the query with the right number of placeholders
placeholders := make([]string, len(ids))
args := make([]interface{}, len(ids))
for i, id := range ids {
    placeholders[i] = "?"
    args[i] = id
}

query := fmt.Sprintf(
    "SELECT * FROM users WHERE id IN (%s)",
    strings.Join(placeholders, ","),
)

rows, err := db.Query(query, args...)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()
```

**Binding Best Practices:**

- Always use parameters instead of string concatenation
- Parameters prevent SQL injection attacks
- Bound values are properly escaped and quoted
- Use `?` for positional parameters in SQLite
- Named parameters improve code readability for complex queries
- Reuse prepared statements with different parameter values

---

## Error Handling

### Common Errors

Anthony returns standard Go errors and `database/sql` error types:

```go
// No rows found
err := db.QueryRow("SELECT * FROM users WHERE id = ?", 999).Scan(&user)
if err == sql.ErrNoRows {
    fmt.Println("User not found")
} else if err != nil {
    log.Fatal(err)
}

// Connection errors
db, err := sql.Open("sqlite_internal", "mydata.db")
if err != nil {
    log.Fatal("Failed to open database:", err)
}

// Constraint violations
_, err = db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", 1, "duplicate@example.com")
if err != nil {
    // Check for specific SQLite errors
    fmt.Println("Insert failed:", err)
}
```

### Transaction Error Handling

Proper error handling ensures transactions are rolled back on failure:

```go
func transferFunds(db *sql.DB, fromID, toID int, amount float64) error {
    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("begin transaction: %w", err)
    }

    // Ensure rollback on panic or error
    defer func() {
        if p := recover(); p != nil {
            tx.Rollback()
            panic(p)
        }
    }()

    // Debit source account
    _, err = tx.Exec(
        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
        amount, fromID,
    )
    if err != nil {
        tx.Rollback()
        return fmt.Errorf("debit failed: %w", err)
    }

    // Credit destination account
    _, err = tx.Exec(
        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
        amount, toID,
    )
    if err != nil {
        tx.Rollback()
        return fmt.Errorf("credit failed: %w", err)
    }

    // Commit transaction
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("commit failed: %w", err)
    }

    return nil
}
```

### Context-Based Cancellation

Use context for timeout and cancellation control:

```go
import "context"

// Query with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
if err != nil {
    if err == context.DeadlineExceeded {
        log.Println("Query timed out")
    }
    log.Fatal(err)
}
defer rows.Close()

// Execute with cancellation
ctx, cancel = context.WithCancel(context.Background())

go func() {
    time.Sleep(2 * time.Second)
    cancel() // Cancel after 2 seconds
}()

_, err = db.ExecContext(ctx, "INSERT INTO logs (message) VALUES (?)", "test")
if err == context.Canceled {
    log.Println("Operation was cancelled")
}
```

**Error Handling Best Practices:**

- Always check errors from database operations
- Use `sql.ErrNoRows` to distinguish "not found" from other errors
- Wrap errors with context using `fmt.Errorf` with `%w`
- Roll back transactions on any error
- Use context for timeout and cancellation
- Log errors with sufficient context for debugging

---

## Connection Management

### Connection Pooling

Anthony uses Go's built-in connection pooling:

```go
db, err := sql.Open("sqlite_internal", "mydata.db")
if err != nil {
    log.Fatal(err)
}

// Configure connection pool
db.SetMaxOpenConns(25)    // Maximum number of open connections
db.SetMaxIdleConns(5)     // Maximum number of idle connections
db.SetConnMaxLifetime(5 * time.Minute) // Maximum connection lifetime
db.SetConnMaxIdleTime(1 * time.Minute) // Maximum idle time

// For SQLite, typically use:
// - MaxOpenConns = 1 (SQLite has limited concurrency)
// - MaxIdleConns = 1
// This ensures consistent behavior with SQLite's locking model
```

### Connection Health

Check connection health with Ping:

```go
if err := db.Ping(); err != nil {
    log.Printf("Database connection is not healthy: %v", err)
}

// PingContext with timeout
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

if err := db.PingContext(ctx); err != nil {
    log.Printf("Database ping failed: %v", err)
}
```

### Database Statistics

Monitor connection pool statistics:

```go
stats := db.Stats()
fmt.Printf("Open connections: %d\n", stats.OpenConnections)
fmt.Printf("In use: %d\n", stats.InUse)
fmt.Printf("Idle: %d\n", stats.Idle)
fmt.Printf("Wait count: %d\n", stats.WaitCount)
fmt.Printf("Wait duration: %v\n", stats.WaitDuration)
```

### Concurrent Access

SQLite has specific concurrency characteristics:

```go
// SQLite allows multiple readers OR one writer
// For write-heavy applications, serialize writes:

var writeMu sync.Mutex

func writeToDatabase(db *sql.DB, data string) error {
    writeMu.Lock()
    defer writeMu.Unlock()

    _, err := db.Exec("INSERT INTO logs (data) VALUES (?)", data)
    return err
}

// Or use WAL mode for better concurrent performance:
db.Exec("PRAGMA journal_mode=WAL")
```

### Resource Cleanup

Proper cleanup prevents resource leaks:

```go
func processData(db *sql.DB) error {
    // Prepare statement
    stmt, err := db.Prepare("INSERT INTO data (value) VALUES (?)")
    if err != nil {
        return err
    }
    defer stmt.Close() // Always close prepared statements

    // Execute query
    rows, err := db.Query("SELECT id, value FROM source_table")
    if err != nil {
        return err
    }
    defer rows.Close() // Always close rows

    // Process rows
    for rows.Next() {
        var id int
        var value string
        if err := rows.Scan(&id, &value); err != nil {
            return err
        }

        if _, err := stmt.Exec(value); err != nil {
            return err
        }
    }

    return rows.Err()
}
```

**Connection Management Best Practices:**

- Set MaxOpenConns=1 for SQLite (single writer limitation)
- Use WAL mode for better read concurrency
- Always close `Rows`, `Stmt`, and `Tx` objects
- Use `defer` for cleanup to ensure resources are released
- Monitor connection pool statistics in production
- Handle connection errors with retry logic
- Use connection contexts for timeout control

---

## Additional Resources

### Example: Complete Application

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    // Open database
    db, err := sql.Open("sqlite_internal", "app.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Insert user
    result, err := db.Exec(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "Alice Smith", "alice@example.com",
    )
    if err != nil {
        log.Fatal(err)
    }

    userID, _ := result.LastInsertId()
    fmt.Printf("Created user with ID: %d\n", userID)

    // Query users
    rows, err := db.Query("SELECT id, name, email FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    fmt.Println("\nAll users:")
    for rows.Next() {
        var id int
        var name, email string
        if err := rows.Scan(&id, &name, &email); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("  %d: %s <%s>\n", id, name, email)
    }

    if err := rows.Err(); err != nil {
        log.Fatal(err)
    }
}
```

### SQLite Type Mapping

| SQLite Type | Go Type          | Notes                          |
|-------------|------------------|--------------------------------|
| INTEGER     | int, int64       | 64-bit signed integer          |
| REAL        | float64          | 64-bit floating point          |
| TEXT        | string           | UTF-8 text                     |
| BLOB        | []byte           | Binary data                    |
| NULL        | nil, sql.Null*   | Use sql.Null* for nullable    |

### Driver-Specific Features

Anthony is a pure Go implementation that supports:

- Full SQLite 3.51.2 compatibility
- Database file format compatibility
- All standard SQL features
- Transactions and savepoints
- Prepared statements
- BLOB support
- Foreign keys
- Triggers and views
- Full-text search (FTS)
- JSON functions
- Window functions
- Common Table Expressions (CTE)

### Performance Tips

1. **Use Transactions**: Batch writes in transactions for 10-100x speedup
2. **Prepared Statements**: Reuse for repeated queries
3. **WAL Mode**: Enable for better concurrency: `PRAGMA journal_mode=WAL`
4. **Indexes**: Create indexes on frequently queried columns
5. **Connection Pool**: Set MaxOpenConns=1 for SQLite
6. **ANALYZE**: Keep statistics up-to-date: `ANALYZE`

### Reference Links

- [Go database/sql documentation](https://pkg.go.dev/database/sql)
- [SQLite documentation](https://www.sqlite.org/docs.html)
- [Anthony GitHub repository](https://github.com/JuniperBible/Public.Lib.Anthony)

---

**Note**: This documentation focuses on the Go `database/sql` interface. Anthony implements the standard Go database driver interface, providing a familiar and idiomatic Go API for SQLite database operations. All examples use pure Go code - no C API knowledge required.
