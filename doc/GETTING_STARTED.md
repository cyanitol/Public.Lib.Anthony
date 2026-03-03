# Getting Started with Anthony

Welcome to Anthony, a pure Go implementation of SQLite. This guide will help you get up and running quickly with all the essential features you need to build database-powered applications in Go.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Opening Databases](#opening-databases)
- [Basic Operations](#basic-operations)
- [Transactions](#transactions)
- [Prepared Statements](#prepared-statements)
- [Error Handling](#error-handling)
- [Compatibility](#compatibility)
- [Next Steps](#next-steps)

## Introduction

**Anthony** is a pure Go implementation of SQLite that provides:

- **100% Pure Go** - No CGO dependencies, easy cross-compilation to any platform Go supports
- **SQLite Compatible** - Reads and writes standard SQLite database files
- **Standard Interface** - Uses Go's `database/sql` package for familiar, idiomatic Go code
- **Full ACID Support** - Complete transaction support with rollback journals and WAL mode
- **Advanced SQL** - CTEs, subqueries, window functions, triggers, views, and more

Anthony is perfect for:
- Applications requiring cross-platform database support without CGO
- Embedded databases in Go applications
- Testing and development (use in-memory databases)
- Learning SQLite internals through readable Go code

## Installation

Install Anthony using `go get`:

```bash
go get github.com/JuniperBible/Public.Lib.Anthony
```

**Requirements:**
- Go 1.26 or later
- No other dependencies required

## Quick Start

Here's a complete example to get you started:

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    // Open a database (creates if it doesn't exist)
    db, err := sql.Open("sqlite_internal", "myapp.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Verify the connection works
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }

    // Create a table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    id, _ := result.LastInsertId()
    fmt.Printf("Created user with ID: %d\n", id)

    // Query data
    var name, email string
    err = db.QueryRow(
        "SELECT name, email FROM users WHERE id = ?", id,
    ).Scan(&name, &email)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("User: %s (%s)\n", name, email)
}
```

**Output:**
```text
Created user with ID: 1
User: Alice (alice@example.com)
```

## Opening Databases

### File-Based Databases

Open or create a database file on disk:

```go
import (
    "database/sql"
    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

// Basic file database
db, err := sql.Open("sqlite_internal", "myapp.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// With path
db, err = sql.Open("sqlite_internal", "/var/lib/myapp/data.db")

// Relative path
db, err = sql.Open("sqlite_internal", "./data/database.db")
```

**Note:** The driver name is `"sqlite_internal"` to avoid conflicts with other SQLite drivers like `mattn/go-sqlite3`.

### In-Memory Databases

Create a temporary database that exists only in memory:

```go
// In-memory database (perfect for testing)
db, err := sql.Open("sqlite_internal", ":memory:")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Alternative: empty string also creates in-memory database
db, err = sql.Open("sqlite_internal", "")
```

**Important:** In-memory databases are destroyed when the connection closes. Each connection creates a separate database.

### Verifying the Connection

Always verify your connection is working:

```go
db, err := sql.Open("sqlite_internal", "myapp.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Test the connection
if err := db.Ping(); err != nil {
    log.Fatalf("Cannot connect to database: %v", err)
}

fmt.Println("Database connected successfully!")
```

### Connection Pool Configuration

Configure the connection pool for optimal performance:

```go
db, err := sql.Open("sqlite_internal", "myapp.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// SQLite serializes writes, so limit connections for write-heavy workloads
db.SetMaxOpenConns(10)        // Maximum open connections
db.SetMaxIdleConns(5)         // Maximum idle connections
db.SetConnMaxLifetime(5 * time.Minute)  // Maximum connection lifetime
db.SetConnMaxIdleTime(10 * time.Minute) // Maximum idle time
```

## Basic Operations

### Creating Tables

Define your database schema with CREATE TABLE:

```go
// Simple table
_, err := db.Exec(`
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        price REAL,
        quantity INTEGER DEFAULT 0
    )
`)
if err != nil {
    log.Fatal(err)
}

// Table with constraints
_, err = db.Exec(`
    CREATE TABLE customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        age INTEGER CHECK(age >= 18),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
`)
if err != nil {
    log.Fatal(err)
}

// Table with foreign key
_, err = db.Exec(`
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL,
        total REAL NOT NULL,
        status TEXT DEFAULT 'pending',
        FOREIGN KEY (customer_id) REFERENCES customers(id)
    )
`)
if err != nil {
    log.Fatal(err)
}

// Enable foreign key enforcement
_, err = db.Exec("PRAGMA foreign_keys = ON")
```

### Inserting Data

Add rows to your tables:

```go
// Insert single row
result, err := db.Exec(
    "INSERT INTO products (name, price, quantity) VALUES (?, ?, ?)",
    "Laptop", 999.99, 10,
)
if err != nil {
    log.Fatal(err)
}

// Get the auto-generated ID
id, err := result.LastInsertId()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Inserted product with ID: %d\n", id)

// Check rows affected
rowsAffected, err := result.RowsAffected()
fmt.Printf("Rows affected: %d\n", rowsAffected)

// Insert multiple rows
products := []struct {
    Name     string
    Price    float64
    Quantity int
}{
    {"Mouse", 29.99, 50},
    {"Keyboard", 79.99, 30},
    {"Monitor", 299.99, 15},
}

for _, p := range products {
    _, err := db.Exec(
        "INSERT INTO products (name, price, quantity) VALUES (?, ?, ?)",
        p.Name, p.Price, p.Quantity,
    )
    if err != nil {
        log.Fatal(err)
    }
}
```

### Querying Data

Retrieve data from your database:

```go
// Query multiple rows
rows, err := db.Query("SELECT id, name, price FROM products WHERE price > ?", 50.0)
if err != nil {
    log.Fatal(err)
}
defer rows.Close() // Always close rows!

fmt.Println("Products over $50:")
for rows.Next() {
    var id int
    var name string
    var price float64

    err := rows.Scan(&id, &name, &price)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("  %d: %s - $%.2f\n", id, name, price)
}

// Check for errors during iteration
if err = rows.Err(); err != nil {
    log.Fatal(err)
}

// Query single row
var productName string
var productPrice float64
err = db.QueryRow(
    "SELECT name, price FROM products WHERE id = ?", 1,
).Scan(&productName, &productPrice)

if err == sql.ErrNoRows {
    fmt.Println("Product not found")
} else if err != nil {
    log.Fatal(err)
} else {
    fmt.Printf("Product: %s - $%.2f\n", productName, productPrice)
}

// Query with aggregate functions
var count int
var avgPrice float64
err = db.QueryRow("SELECT COUNT(*), AVG(price) FROM products").Scan(&count, &avgPrice)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Total products: %d, Average price: $%.2f\n", count, avgPrice)
```

### Updating Data

Modify existing rows:

```go
// Update single row
result, err := db.Exec(
    "UPDATE products SET price = ? WHERE id = ?",
    899.99, 1,
)
if err != nil {
    log.Fatal(err)
}

rowsAffected, _ := result.RowsAffected()
fmt.Printf("Updated %d row(s)\n", rowsAffected)

// Update multiple columns
_, err = db.Exec(`
    UPDATE products
    SET price = ?, quantity = ?
    WHERE name = ?`,
    79.99, 25, "Keyboard",
)
if err != nil {
    log.Fatal(err)
}

// Update with conditions
_, err = db.Exec(
    "UPDATE products SET quantity = quantity - 1 WHERE id = ? AND quantity > 0",
    1,
)
if err != nil {
    log.Fatal(err)
}
```

### Deleting Data

Remove rows from tables:

```go
// Delete specific rows
result, err := db.Exec("DELETE FROM products WHERE quantity = 0")
if err != nil {
    log.Fatal(err)
}

rowsAffected, _ := result.RowsAffected()
fmt.Printf("Deleted %d row(s)\n", rowsAffected)

// Delete single row
_, err = db.Exec("DELETE FROM products WHERE id = ?", 5)
if err != nil {
    log.Fatal(err)
}

// Delete with complex conditions
_, err = db.Exec(`
    DELETE FROM products
    WHERE price < ? AND quantity < ?`,
    10.0, 5,
)
if err != nil {
    log.Fatal(err)
}
```

## Transactions

Transactions ensure multiple operations execute atomically (all or nothing).

### Basic Transaction Pattern

```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Use defer to ensure rollback on error
defer tx.Rollback() // Safe to call even after successful Commit

// Execute operations within transaction
_, err = tx.Exec("INSERT INTO customers (name, email, age) VALUES (?, ?, ?)",
    "Bob", "bob@example.com", 25)
if err != nil {
    // Transaction will be rolled back by defer
    log.Fatal(err)
}

_, err = tx.Exec("INSERT INTO orders (customer_id, total) VALUES (?, ?)",
    1, 150.00)
if err != nil {
    // Transaction will be rolled back by defer
    log.Fatal(err)
}

// Commit the transaction
if err = tx.Commit(); err != nil {
    log.Fatal(err)
}

fmt.Println("Transaction completed successfully!")
```

### Money Transfer Example

A classic example demonstrating ACID properties:

```go
func transferMoney(db *sql.DB, fromAccount, toAccount int, amount float64) error {
    // Begin transaction
    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback()

    // Deduct from source account
    result, err := tx.Exec(
        "UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?",
        amount, fromAccount, amount,
    )
    if err != nil {
        return fmt.Errorf("failed to deduct from account %d: %w", fromAccount, err)
    }

    // Check if update succeeded (sufficient balance)
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return fmt.Errorf("insufficient balance in account %d", fromAccount)
    }

    // Add to destination account
    _, err = tx.Exec(
        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
        amount, toAccount,
    )
    if err != nil {
        return fmt.Errorf("failed to add to account %d: %w", toAccount, err)
    }

    // Commit transaction
    if err = tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    return nil
}

// Usage
err := transferMoney(db, 1, 2, 100.00)
if err != nil {
    log.Printf("Transfer failed: %v", err)
} else {
    fmt.Println("Transfer successful!")
}
```

### Read-Only Transactions

Use read-only transactions for consistent snapshots:

```go
import "context"

// Begin read-only transaction
ctx := context.Background()
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    ReadOnly: true,
})
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Execute read operations
var count int
err = tx.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
if err != nil {
    log.Fatal(err)
}

rows, err := tx.Query("SELECT name, price FROM products ORDER BY price DESC LIMIT 10")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Process results...

// Commit (or rollback) read-only transaction
tx.Commit()
```

### Savepoints

Use savepoints for nested transaction-like behavior:

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Initial work
_, err = tx.Exec("INSERT INTO log (message) VALUES (?)", "Started")
if err != nil {
    log.Fatal(err)
}

// Create savepoint
_, err = tx.Exec("SAVEPOINT sp1")
if err != nil {
    log.Fatal(err)
}

// Additional work that might fail
_, err = tx.Exec("INSERT INTO risky_table (data) VALUES (?)", "risky data")
if err != nil {
    // Rollback to savepoint instead of entire transaction
    tx.Exec("ROLLBACK TO SAVEPOINT sp1")
    log.Printf("Rolled back to savepoint: %v", err)
} else {
    // Release savepoint if successful
    tx.Exec("RELEASE SAVEPOINT sp1")
}

// Commit overall transaction
tx.Commit()
```

## Prepared Statements

Prepared statements improve performance and security for repeated queries.

### Basic Prepared Statement

```go
// Prepare statement
stmt, err := db.Prepare("INSERT INTO products (name, price) VALUES (?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close() // Always close prepared statements

// Execute multiple times with different parameters
products := []struct {
    Name  string
    Price float64
}{
    {"Widget A", 19.99},
    {"Widget B", 24.99},
    {"Widget C", 29.99},
}

for _, p := range products {
    result, err := stmt.Exec(p.Name, p.Price)
    if err != nil {
        log.Fatal(err)
    }

    id, _ := result.LastInsertId()
    fmt.Printf("Inserted %s with ID %d\n", p.Name, id)
}
```

### Prepared Query Statement

```go
// Prepare query statement
stmt, err := db.Prepare("SELECT name, price FROM products WHERE price > ? ORDER BY price")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute with different parameters
priceThresholds := []float64{10.0, 20.0, 30.0}

for _, threshold := range priceThresholds {
    fmt.Printf("\nProducts over $%.2f:\n", threshold)

    rows, err := stmt.Query(threshold)
    if err != nil {
        log.Fatal(err)
    }

    for rows.Next() {
        var name string
        var price float64
        rows.Scan(&name, &price)
        fmt.Printf("  %s - $%.2f\n", name, price)
    }
    rows.Close()
}
```

### Prepared Statements in Transactions

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Prepare statement within transaction
stmt, err := tx.Prepare("INSERT INTO audit_log (action, details) VALUES (?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Use prepared statement multiple times
actions := []struct {
    Action  string
    Details string
}{
    {"LOGIN", "User logged in"},
    {"UPDATE", "Profile updated"},
    {"LOGOUT", "User logged out"},
}

for _, a := range actions {
    _, err := stmt.Exec(a.Action, a.Details)
    if err != nil {
        log.Fatal(err)
    }
}

tx.Commit()
```

## Error Handling

Proper error handling is essential for robust database applications.

### Checking for Specific Errors

```go
import "database/sql"

// Query single row with error handling
var name string
err := db.QueryRow("SELECT name FROM users WHERE id = ?", 999).Scan(&name)

if err == sql.ErrNoRows {
    // No matching row found - this is often not an error
    fmt.Println("User not found")
} else if err != nil {
    // Other error occurred
    log.Fatalf("Database error: %v", err)
} else {
    fmt.Printf("Found user: %s\n", name)
}
```

### Handling Constraint Violations

```go
import "strings"

_, err := db.Exec("INSERT INTO users (email) VALUES (?)", "test@example.com")
if err != nil {
    // Check for specific constraint errors
    if strings.Contains(err.Error(), "UNIQUE constraint failed") {
        fmt.Println("Email already exists")
        // Handle duplicate gracefully
    } else if strings.Contains(err.Error(), "CHECK constraint failed") {
        fmt.Println("Data doesn't meet requirements")
        // Handle validation error
    } else if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
        fmt.Println("Invalid reference")
        // Handle referential integrity error
    } else if strings.Contains(err.Error(), "NOT NULL constraint failed") {
        fmt.Println("Required field is missing")
        // Handle missing data
    } else {
        log.Fatalf("Database error: %v", err)
    }
}
```

### Error Wrapping

Use error wrapping to preserve context:

```go
import "fmt"

func createUser(db *sql.DB, name, email string) error {
    _, err := db.Exec(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        name, email,
    )
    if err != nil {
        // Wrap error with context using %w
        return fmt.Errorf("failed to create user %s: %w", name, err)
    }
    return nil
}

// Caller can check the wrapped error
err := createUser(db, "Alice", "alice@example.com")
if err != nil {
    log.Printf("Error: %v", err)
    // Output: Error: failed to create user Alice: UNIQUE constraint failed: users.email
}
```

### Resource Cleanup

Always clean up database resources:

```go
// Pattern 1: Defer immediately after successful open
rows, err := db.Query("SELECT * FROM users")
if err != nil {
    return err
}
defer rows.Close() // Cleanup even if error occurs below

for rows.Next() {
    // Process rows
}

// Check for errors after iteration
if err = rows.Err(); err != nil {
    return err
}

// Pattern 2: Prepared statements
stmt, err := db.Prepare("SELECT * FROM users WHERE id = ?")
if err != nil {
    return err
}
defer stmt.Close()

// Pattern 3: Transactions
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback() // Safe even after Commit
```

### Common Error Scenarios

```go
// 1. Database locked (concurrent write conflict)
_, err := db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
if err != nil && strings.Contains(err.Error(), "database is locked") {
    // Retry with backoff or use WAL mode
    log.Println("Database busy, retrying...")
}

// 2. Table doesn't exist
_, err = db.Exec("SELECT * FROM nonexistent_table")
if err != nil && strings.Contains(err.Error(), "no such table") {
    log.Println("Table needs to be created first")
}

// 3. Syntax error
_, err = db.Exec("SELCT * FROM users") // Typo
if err != nil && strings.Contains(err.Error(), "syntax error") {
    log.Println("Check your SQL syntax")
}

// 4. Connection closed
err = db.Ping()
if err != nil {
    log.Println("Database connection lost")
    // Reconnect or exit
}
```

## Compatibility

Anthony is designed to be compatible with SQLite 3.x.

### SQLite Feature Compatibility

**Fully Supported:**
- Standard SQL: SELECT, INSERT, UPDATE, DELETE
- DDL: CREATE/DROP TABLE, CREATE/DROP INDEX, ALTER TABLE
- Constraints: PRIMARY KEY, UNIQUE, NOT NULL, CHECK, FOREIGN KEY
- Transactions with ACID guarantees
- Joins: INNER, LEFT, CROSS
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT
- Subqueries and CTEs (Common Table Expressions)
- Window functions
- Triggers and Views
- PRAGMA statements for configuration
- JSON functions (basic support)

**File Format:**
- 100% compatible with SQLite database files
- Can read databases created by SQLite
- Databases created by Anthony can be read by SQLite

**Differences:**
- Pure Go implementation (no C dependencies)
- May have different performance characteristics
- Some advanced features still in development (see TODO.txt)

### Testing Compatibility

```go
// Test that a database works with both Anthony and SQLite
func TestCrossCompatibility(t *testing.T) {
    // Create database with Anthony
    dbPath := "test.db"
    defer os.Remove(dbPath)

    anthonyDB, _ := sql.Open("sqlite_internal", dbPath)
    anthonyDB.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
    anthonyDB.Exec("INSERT INTO test VALUES (1, 'hello')")
    anthonyDB.Close()

    // Read with SQLite (using mattn/go-sqlite3 or similar)
    sqliteDB, _ := sql.Open("sqlite3", dbPath)
    var value string
    sqliteDB.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
    sqliteDB.Close()

    if value != "hello" {
        t.Errorf("Expected 'hello', got '%s'", value)
    }
}
```

### Version Information

```go
// Get SQLite version
var version string
err := db.QueryRow("SELECT sqlite_version()").Scan(&version)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("SQLite version: %s\n", version)
```

For detailed compatibility information, see [COMPATIBILITY.md](COMPATIBILITY.md).

## Next Steps

Now that you understand the basics, explore more advanced features:

### Documentation

- **[API Reference](API.md)** - Complete API documentation
- **[Local SQLite Reference](sqlite/README.md)** - Detailed SQL syntax, functions, pragmas, and internals
- **[Quick Start Guide](QUICKSTART.md)** - More examples and patterns
- **[Architecture Overview](ARCHITECTURE.md)** - How Anthony works internally
- **[Security Guide](SECURITY.md)** - Security best practices
- **[Error Handling Guide](ERROR_HANDLING.md)** - Comprehensive error handling

### Advanced Features

- **[Common Table Expressions](CTE_USAGE_GUIDE.md)** - Learn about CTEs and recursive queries
- **[Subqueries](SUBQUERY_QUICK_REFERENCE.md)** - Master subquery patterns
- **[PRAGMA Commands](PRAGMA_QUICK_REFERENCE.md)** - Database configuration options
- **[ALTER TABLE](ALTER_TABLE_QUICK_REFERENCE.md)** - Schema migration guide
- **[VACUUM](VACUUM_USAGE.md)** - Database maintenance

### Example Applications

Check the test files for more examples:

```bash
# Driver examples
cat internal/driver/example_test.go

# Function examples
cat internal/functions/examples_test.go

# Engine examples
cat internal/engine/example_test.go
```

### Performance Optimization

```go
// Enable WAL mode for better concurrency
db.Exec("PRAGMA journal_mode=WAL")

// Increase cache size (default is 2000 pages)
db.Exec("PRAGMA cache_size=10000")

// Create indexes for frequently queried columns
db.Exec("CREATE INDEX idx_users_email ON users(email)")

// Use prepared statements for repeated queries
stmt, _ := db.Prepare("SELECT * FROM users WHERE id = ?")
defer stmt.Close()

// Batch inserts in transactions
tx, _ := db.Begin()
for i := 0; i < 1000; i++ {
    tx.Exec("INSERT INTO data (value) VALUES (?)", i)
}
tx.Commit()

// Analyze database for query optimizer
db.Exec("ANALYZE")
```

### Getting Help

- **SQLite Documentation:** [https://www.sqlite.org/docs.html](https://www.sqlite.org/docs.html)
- **Go database/sql:** [https://pkg.go.dev/database/sql](https://pkg.go.dev/database/sql)
- **Documentation Index:** [INDEX.md](INDEX.md)

### Contributing

Anthony is open source and welcomes contributions. See the main [README.md](../README.md) for contribution guidelines.

## License

Anthony is in the public domain (SQLite License). Use freely!

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.

---

**Happy coding with Anthony!**
