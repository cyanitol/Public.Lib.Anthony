# Anthony SQLite - User Guide

A comprehensive guide to using Anthony, a pure Go implementation of SQLite.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Connection Strings](#connection-strings)
- [Supported SQL Syntax](#supported-sql-syntax)
- [Data Types](#data-types)
- [Transactions](#transactions)
- [Prepared Statements](#prepared-statements)
- [Error Handling](#error-handling)
- [Common Operations](#common-operations)
- [Performance Tips](#performance-tips)
- [Next Steps](#next-steps)

## Introduction

Anthony is a pure Go implementation of SQLite that provides full compatibility with Go's `database/sql` package. It requires no CGO dependencies, making it easy to cross-compile and deploy anywhere Go runs.

**Key Benefits:**

- Pure Go - no CGO required
- Cross-platform compilation
- Drop-in replacement using standard `database/sql` interface
- Full ACID transaction support
- Compatible with SQLite file format and SQL syntax

**What You Can Do:**

- Store data in a local SQLite database
- Execute complex SQL queries with joins, subqueries, and CTEs
- Enforce data integrity with constraints and foreign keys
- Use transactions for atomic operations
- Create indexes for fast queries
- Attach multiple databases

## Installation

Install Anthony using Go modules:

```bash
go get github.com/JuniperBible/Public.Lib.Anthony
```

**Requirements:** Go 1.26 or later

**Verify Installation:**

```bash
go list -m github.com/JuniperBible/Public.Lib.Anthony
```

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
    // 1. Open database connection
    db, err := sql.Open("sqlite_internal", "myapp.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. Verify connection
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }

    // 3. Create a table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER CHECK(age >= 0)
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // 4. Insert data
    result, err := db.Exec(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        "Alice", "alice@example.com", 30,
    )
    if err != nil {
        log.Fatal(err)
    }

    // Get the inserted ID
    id, _ := result.LastInsertId()
    fmt.Printf("Inserted user with ID: %d\n", id)

    // 5. Query data
    rows, err := db.Query("SELECT id, name, email, age FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // 6. Process results
    fmt.Println("\nUsers:")
    for rows.Next() {
        var id, age int
        var name, email string

        err := rows.Scan(&id, &name, &email, &age)
        if err != nil {
            log.Fatal(err)
        }

        fmt.Printf("  [%d] %s (%s), age %d\n", id, name, email, age)
    }

    // 7. Check for errors during iteration
    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
}
```

**Output:**
```
Inserted user with ID: 1

Users:
  [1] Alice (alice@example.com), age 30
```

## Connection Strings

The connection string (DSN) determines how Anthony opens your database.

### File-Based Database

```go
// Basic file database
db, err := sql.Open("sqlite_internal", "myapp.db")

// With relative path
db, err := sql.Open("sqlite_internal", "./data/myapp.db")

// With absolute path
db, err := sql.Open("sqlite_internal", "/var/lib/myapp/data.db")
```

**Note:** The database file is created automatically if it doesn't exist.

### In-Memory Database

```go
// In-memory database (data lost when connection closes)
db, err := sql.Open("sqlite_internal", ":memory:")

// Or use empty string
db, err := sql.Open("sqlite_internal", "")
```

**Important:** Each in-memory connection creates a separate database. Data is not shared between connections.

### DSN Query Parameters

```go
// Read-only mode
db, err := sql.Open("sqlite_internal", "myapp.db?mode=ro")
```

**Supported Parameters:**

- `mode=ro` - Open database in read-only mode

## Supported SQL Syntax

Anthony supports a comprehensive subset of SQLite's SQL dialect.

### Data Definition Language (DDL)

**CREATE TABLE:**

```go
_, err := db.Exec(`
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        price REAL CHECK(price > 0),
        category TEXT DEFAULT 'general',
        stock INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(name, category)
    )
`)
```

**CREATE INDEX:**

```go
// Simple index
_, err := db.Exec("CREATE INDEX idx_products_name ON products(name)")

// Unique index
_, err := db.Exec("CREATE UNIQUE INDEX idx_products_sku ON products(sku)")

// Partial index (with WHERE clause)
_, err := db.Exec(`
    CREATE INDEX idx_products_active
    ON products(name)
    WHERE stock > 0
`)
```

**ALTER TABLE:**

```go
// Rename table
_, err := db.Exec("ALTER TABLE products RENAME TO items")

// Add column
_, err := db.Exec("ALTER TABLE products ADD COLUMN description TEXT")

// Rename column
_, err := db.Exec("ALTER TABLE products RENAME COLUMN price TO cost")

// Drop column
_, err := db.Exec("ALTER TABLE products DROP COLUMN old_field")
```

**See Also:** [ALTER_TABLE_QUICK_REFERENCE.md](ALTER_TABLE_QUICK_REFERENCE.md)

### Data Manipulation Language (DML)

**INSERT:**

```go
// Insert single row
_, err := db.Exec(
    "INSERT INTO products (name, price, stock) VALUES (?, ?, ?)",
    "Widget", 9.99, 100,
)

// Insert multiple rows
_, err := db.Exec(`
    INSERT INTO products (name, price, stock) VALUES
        ('Gadget', 19.99, 50),
        ('Gizmo', 14.99, 75),
        ('Doohickey', 24.99, 25)
`)

// Insert with UPSERT (update on conflict)
_, err := db.Exec(`
    INSERT INTO products (id, name, price, stock)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        price = excluded.price,
        stock = stock + excluded.stock
`, 1, "Widget", 9.99, 50)
```

**UPDATE:**

```go
// Update specific rows
result, err := db.Exec(
    "UPDATE products SET price = price * 1.1 WHERE category = ?",
    "electronics",
)

// Check rows affected
rowsAffected, _ := result.RowsAffected()
fmt.Printf("Updated %d products\n", rowsAffected)

// Update with subquery
_, err = db.Exec(`
    UPDATE products SET stock = (
        SELECT SUM(quantity) FROM inventory WHERE product_id = products.id
    )
`)
```

**DELETE:**

```go
// Delete specific rows
_, err := db.Exec("DELETE FROM products WHERE stock = 0")

// Delete all rows (but keep table structure)
_, err := db.Exec("DELETE FROM products")
```

### SELECT Queries

**Basic SELECT:**

```go
rows, err := db.Query(`
    SELECT id, name, price
    FROM products
    WHERE category = ? AND stock > 0
    ORDER BY price DESC
    LIMIT 10
`, "electronics")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name string
    var price float64

    err := rows.Scan(&id, &name, &price)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("%s: $%.2f\n", name, price)
}
```

**JOINs:**

```go
rows, err := db.Query(`
    SELECT p.name, c.name as category_name, p.price
    FROM products p
    INNER JOIN categories c ON p.category_id = c.id
    WHERE p.stock > 0
    ORDER BY c.name, p.name
`)
```

**Aggregate Functions:**

```go
// Count products
var count int
err := db.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
fmt.Printf("Total products: %d\n", count)

// Average price
var avgPrice float64
err = db.QueryRow("SELECT AVG(price) FROM products").Scan(&avgPrice)
fmt.Printf("Average price: $%.2f\n", avgPrice)

// Group by category
rows, err := db.Query(`
    SELECT category, COUNT(*) as count, AVG(price) as avg_price
    FROM products
    GROUP BY category
    HAVING COUNT(*) > 5
    ORDER BY count DESC
`)
```

**Subqueries:**

```go
// Scalar subquery
rows, err := db.Query(`
    SELECT name, price
    FROM products
    WHERE price > (SELECT AVG(price) FROM products)
`)

// IN subquery
rows, err := db.Query(`
    SELECT name
    FROM products
    WHERE category_id IN (SELECT id FROM categories WHERE active = 1)
`)

// EXISTS subquery
rows, err := db.Query(`
    SELECT p.name
    FROM products p
    WHERE EXISTS (
        SELECT 1 FROM orders o
        WHERE o.product_id = p.id
        AND o.created_at > date('now', '-30 days')
    )
`)
```

**See Also:** [SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md)

**Common Table Expressions (CTEs):**

```go
// Simple CTE
rows, err := db.Query(`
    WITH expensive_products AS (
        SELECT * FROM products WHERE price > 50
    )
    SELECT name, price
    FROM expensive_products
    WHERE stock > 0
    ORDER BY price DESC
`)

// Recursive CTE (generate series)
rows, err := db.Query(`
    WITH RECURSIVE cnt(x) AS (
        SELECT 1
        UNION ALL
        SELECT x + 1 FROM cnt WHERE x < 10
    )
    SELECT x FROM cnt
`)
```

**See Also:** [CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md)

**Compound Queries:**

```go
// UNION (remove duplicates)
rows, err := db.Query(`
    SELECT name FROM products WHERE category = 'electronics'
    UNION
    SELECT name FROM products WHERE price > 100
`)

// UNION ALL (keep duplicates)
rows, err := db.Query(`
    SELECT name FROM products WHERE stock > 0
    UNION ALL
    SELECT name FROM discontinued_products
`)

// INTERSECT (common rows)
rows, err := db.Query(`
    SELECT email FROM customers
    INTERSECT
    SELECT email FROM subscribers
`)

// EXCEPT (difference)
rows, err := db.Query(`
    SELECT email FROM all_users
    EXCEPT
    SELECT email FROM unsubscribed
`)
```

**See Also:** [COMPOUND_SELECT_QUICK_REFERENCE.md](COMPOUND_SELECT_QUICK_REFERENCE.md), [SQL_LANGUAGE.md](SQL_LANGUAGE.md)

## Data Types

SQLite uses a dynamic type system where the type is associated with the value, not the column.

### Storage Classes

Every value in SQLite has one of five storage classes:

1. **NULL** - Missing or unknown value
2. **INTEGER** - Signed integer (64-bit)
3. **REAL** - Floating point (64-bit IEEE 754)
4. **TEXT** - Text string (UTF-8, UTF-16LE, or UTF-16BE)
5. **BLOB** - Binary data

### Column Type Affinity

Column declarations suggest a preferred type (affinity):

```go
_, err := db.Exec(`
    CREATE TABLE examples (
        -- INTEGER affinity
        id INTEGER,
        count INT,
        age BIGINT,

        -- TEXT affinity
        name TEXT,
        email VARCHAR(255),
        description CHAR(100),

        -- REAL affinity
        price REAL,
        temperature FLOAT,
        ratio DOUBLE,

        -- NUMERIC affinity (tries to convert to number)
        value NUMERIC,
        amount DECIMAL(10,2),

        -- BLOB affinity (no conversion)
        data BLOB,
        image BINARY
    )
`)
```

### Type Conversions

```go
// INTEGER → REAL (automatic)
var result float64
db.QueryRow("SELECT 42").Scan(&result) // result = 42.0

// TEXT → INTEGER (if valid)
var num int
db.QueryRow("SELECT '123'").Scan(&num) // num = 123

// Go types map naturally:
var (
    i   int64   // INTEGER
    f   float64 // REAL
    s   string  // TEXT
    b   []byte  // BLOB
    ptr *string // NULL if nil, TEXT otherwise
)
```

### Boolean Values

SQLite doesn't have a native BOOLEAN type:

```go
// Use INTEGER: 0 = false, 1 = true
_, err := db.Exec(`
    CREATE TABLE settings (
        name TEXT,
        enabled INTEGER  -- 0 or 1
    )
`)

// Insert boolean
_, err = db.Exec("INSERT INTO settings (name, enabled) VALUES (?, ?)",
    "feature_x", 1) // 1 = enabled

// Query boolean
var enabled bool
err = db.QueryRow("SELECT enabled FROM settings WHERE name = ?",
    "feature_x").Scan(&enabled)
```

### Date and Time

SQLite stores dates/times as TEXT, REAL, or INTEGER:

```go
// TEXT (ISO 8601 format) - most readable
_, err := db.Exec(`
    CREATE TABLE events (
        name TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
`)

// Insert with current timestamp
_, err = db.Exec("INSERT INTO events (name) VALUES (?)", "user_login")

// Query with date functions
rows, err := db.Query(`
    SELECT name, created_at
    FROM events
    WHERE date(created_at) = date('now')
`)

// Date arithmetic
rows, err = db.Query(`
    SELECT name, datetime(created_at, '+1 day') as tomorrow
    FROM events
`)

// Format dates
rows, err = db.Query(`
    SELECT strftime('%Y-%m-%d %H:%M', created_at) as formatted
    FROM events
`)
```

**See Also:** [TYPE_SYSTEM.md](TYPE_SYSTEM.md)

## Transactions

Transactions ensure that a series of operations either all succeed or all fail together (atomicity).

### Basic Transactions

```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Use defer for automatic rollback on error
defer tx.Rollback()

// Execute operations
_, err = tx.Exec("INSERT INTO accounts (name, balance) VALUES (?, ?)",
    "Alice", 1000)
if err != nil {
    return err // Rollback happens via defer
}

_, err = tx.Exec("INSERT INTO accounts (name, balance) VALUES (?, ?)",
    "Bob", 500)
if err != nil {
    return err // Rollback happens via defer
}

// Commit if all operations succeeded
if err := tx.Commit(); err != nil {
    return err
}

// Note: Rollback after Commit is a no-op (safe)
```

### Read-Only Transactions

```go
// Begin read-only transaction (better concurrency)
tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
    ReadOnly: true,
})
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Can only execute SELECT queries
rows, err := tx.Query("SELECT * FROM products WHERE stock > 0")
if err != nil {
    return err
}
defer rows.Close()

// Process results...

// Commit (releases read lock)
tx.Commit()
```

### Transaction Example: Money Transfer

```go
func transferFunds(db *sql.DB, fromAccount, toAccount int, amount float64) error {
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

    // Commit both operations atomically
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    return nil
}
```

### Savepoints

Savepoints create checkpoints within a transaction:

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Create savepoint
_, err = tx.Exec("SAVEPOINT sp1")
if err != nil {
    return err
}

// Do some work
_, err = tx.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
if err != nil {
    return err
}

// Another savepoint
_, err = tx.Exec("SAVEPOINT sp2")
if err != nil {
    return err
}

// More work
_, err = tx.Exec("INSERT INTO users (name) VALUES (?)", "Bob")
if err != nil {
    // Rollback to sp2 (keeps Alice, discards Bob)
    tx.Exec("ROLLBACK TO SAVEPOINT sp2")
    return err
}

// Release savepoint (no longer needed)
_, err = tx.Exec("RELEASE SAVEPOINT sp2")

// Commit entire transaction
tx.Commit()
```

## Prepared Statements

Prepared statements improve performance and security for repeated queries.

### Basic Prepared Statement

```go
// Prepare statement
stmt, err := db.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close() // Always close statements

// Execute multiple times with different parameters
users := []struct {
    Name  string
    Email string
}{
    {"Alice", "alice@example.com"},
    {"Bob", "bob@example.com"},
    {"Charlie", "charlie@example.com"},
}

for _, user := range users {
    result, err := stmt.Exec(user.Name, user.Email)
    if err != nil {
        log.Printf("Failed to insert %s: %v", user.Name, err)
        continue
    }

    id, _ := result.LastInsertId()
    fmt.Printf("Inserted %s with ID %d\n", user.Name, id)
}
```

### Prepared Query

```go
// Prepare SELECT statement
stmt, err := db.Prepare("SELECT id, name, email FROM users WHERE age > ?")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Query with different parameters
for _, minAge := range []int{18, 25, 30} {
    rows, err := stmt.Query(minAge)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("\nUsers older than %d:\n", minAge)
    for rows.Next() {
        var id int
        var name, email string
        rows.Scan(&id, &name, &email)
        fmt.Printf("  %s (%s)\n", name, email)
    }
    rows.Close()
}
```

### Why Use Prepared Statements?

1. **Performance:** Query is parsed and planned once, executed many times
2. **Security:** Prevents SQL injection attacks
3. **Type Safety:** Parameters are properly escaped and typed

```go
// Bad: SQL injection vulnerability
userInput := "'; DROP TABLE users; --"
db.Exec("SELECT * FROM users WHERE name = '" + userInput + "'")

// Good: Prepared statement prevents injection
db.Exec("SELECT * FROM users WHERE name = ?", userInput)
```

## Error Handling

Proper error handling is critical for robust applications.

### Checking Errors

```go
// Always check errors
result, err := db.Exec("INSERT INTO users (name) VALUES (?)", name)
if err != nil {
    // Handle error appropriately
    log.Printf("Failed to insert user: %v", err)
    return err
}

// Check operation results
rowsAffected, err := result.RowsAffected()
if err != nil {
    return err
}
if rowsAffected == 0 {
    return fmt.Errorf("no rows were inserted")
}
```

### Constraint Violations

```go
_, err := db.Exec("INSERT INTO users (email) VALUES (?)", email)
if err != nil {
    if strings.Contains(err.Error(), "UNIQUE constraint failed") {
        return fmt.Errorf("email already exists: %s", email)
    }
    if strings.Contains(err.Error(), "CHECK constraint failed") {
        return fmt.Errorf("invalid data: %w", err)
    }
    if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
        return fmt.Errorf("invalid reference: %w", err)
    }
    return err
}
```

### Query Errors

```go
var name string
err := db.QueryRow("SELECT name FROM users WHERE id = ?", id).Scan(&name)
if err != nil {
    if err == sql.ErrNoRows {
        return fmt.Errorf("user not found: %d", id)
    }
    return fmt.Errorf("query failed: %w", err)
}
```

### Resource Cleanup

```go
// Use defer to ensure cleanup
rows, err := db.Query("SELECT * FROM users")
if err != nil {
    return err
}
defer rows.Close() // Critical: always close rows

for rows.Next() {
    // Process rows
}

// Check for errors during iteration
if err := rows.Err(); err != nil {
    return fmt.Errorf("iteration error: %w", err)
}
```

**See Also:** [ERROR_HANDLING.md](ERROR_HANDLING.md)

## Common Operations

### Creating Tables with Constraints

```go
_, err := db.Exec(`
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER CHECK(quantity > 0),
        total REAL CHECK(total >= 0),
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id),
        UNIQUE(user_id, product_id, created_at)
    )
`)
if err != nil {
    log.Fatal(err)
}

// Enable foreign key enforcement (per-connection setting)
_, err = db.Exec("PRAGMA foreign_keys = ON")
if err != nil {
    log.Fatal(err)
}
```

### Bulk Insert with Transaction

```go
func bulkInsert(db *sql.DB, users []User) error {
    tx, err := db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    stmt, err := tx.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()

    for _, user := range users {
        _, err := stmt.Exec(user.Name, user.Email)
        if err != nil {
            return err
        }
    }

    return tx.Commit()
}
```

### Pagination

```go
func getUsers(db *sql.DB, page, pageSize int) ([]User, error) {
    offset := (page - 1) * pageSize

    rows, err := db.Query(`
        SELECT id, name, email
        FROM users
        ORDER BY id
        LIMIT ? OFFSET ?
    `, pageSize, offset)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var users []User
    for rows.Next() {
        var u User
        err := rows.Scan(&u.ID, &u.Name, &u.Email)
        if err != nil {
            return nil, err
        }
        users = append(users, u)
    }

    return users, rows.Err()
}
```

### Using PRAGMA Statements

```go
// Check database configuration
var pageSize int
err := db.QueryRow("PRAGMA page_size").Scan(&pageSize)
fmt.Printf("Page size: %d bytes\n", pageSize)

// Get table structure
rows, err := db.Query("PRAGMA table_info(users)")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

fmt.Println("Table schema:")
for rows.Next() {
    var cid int
    var name, colType string
    var notNull, pk int
    var dfltValue sql.NullString

    err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("  %s %s", name, colType)
    if notNull == 1 {
        fmt.Print(" NOT NULL")
    }
    if pk == 1 {
        fmt.Print(" PRIMARY KEY")
    }
    fmt.Println()
}

// Enable WAL mode for better concurrency
_, err = db.Exec("PRAGMA journal_mode = WAL")
if err != nil {
    log.Fatal(err)
}

// Verify integrity
var result string
err = db.QueryRow("PRAGMA integrity_check").Scan(&result)
if err != nil {
    log.Fatal(err)
}
if result != "ok" {
    log.Fatalf("Database corruption detected: %s", result)
}
```

**See Also:** [PRAGMA_QUICK_REFERENCE.md](PRAGMA_QUICK_REFERENCE.md)

### Attaching Multiple Databases

```go
// Attach another database
_, err := db.Exec("ATTACH DATABASE 'analytics.db' AS analytics")
if err != nil {
    log.Fatal(err)
}

// Query across databases
rows, err := db.Query(`
    SELECT main.users.name, analytics.events.count
    FROM main.users
    LEFT JOIN analytics.events ON main.users.id = analytics.events.user_id
`)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Process results...

// Detach when done
_, err = db.Exec("DETACH DATABASE analytics")
if err != nil {
    log.Fatal(err)
}
```

**See Also:** [ATTACH_DETACH_IMPLEMENTATION.md](ATTACH_DETACH_IMPLEMENTATION.md)

## Performance Tips

### 1. Create Indexes

```go
// Create indexes on frequently queried columns
_, err := db.Exec("CREATE INDEX idx_users_email ON users(email)")
_, err = db.Exec("CREATE INDEX idx_orders_user_id ON orders(user_id)")
_, err = db.Exec("CREATE INDEX idx_orders_created ON orders(created_at)")

// Composite index for multi-column queries
_, err = db.Exec("CREATE INDEX idx_orders_user_status ON orders(user_id, status)")
```

### 2. Use Transactions for Bulk Operations

```go
// Slow: Each insert is a separate transaction
for i := 0; i < 10000; i++ {
    db.Exec("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("User%d", i))
}

// Fast: All inserts in one transaction
tx, _ := db.Begin()
for i := 0; i < 10000; i++ {
    tx.Exec("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("User%d", i))
}
tx.Commit()
```

### 3. Enable WAL Mode

```go
// Better concurrency: allows multiple readers + one writer
_, err := db.Exec("PRAGMA journal_mode = WAL")
if err != nil {
    log.Fatal(err)
}
```

### 4. Analyze Query Plans

```go
// Check if query uses indexes
rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = ?",
    "test@example.com")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id, parent, notused int
    var detail string
    rows.Scan(&id, &parent, &notused, &detail)
    fmt.Println(detail)

    // Look for "SEARCH ... USING INDEX" (good)
    // Avoid "SCAN TABLE" (bad - needs index)
}
```

### 5. Tune Connection Pool

```go
// For read-heavy workloads
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(5 * time.Minute)

// For write-heavy workloads (SQLite serializes writes)
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
```

### 6. Regular Maintenance

```go
// Update query planner statistics
_, err := db.Exec("ANALYZE")

// Compact database (reclaim space)
_, err = db.Exec("VACUUM")
```

**See Also:** [VACUUM_USAGE.md](VACUUM_USAGE.md)

## Next Steps

### Explore Advanced Features

- **Common Table Expressions (CTEs):** [CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md)
- **Subqueries:** [SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md)
- **Window Functions:** [SQL_LANGUAGE.md](SQL_LANGUAGE.md)
- **Triggers:** [TRIGGER_INTEGRATION_REPORT.md](TRIGGER_INTEGRATION_REPORT.md)
- **Virtual Tables:** [VIRTUAL_TABLES.md](VIRTUAL_TABLES.md)

### Learn the Architecture

- **System Overview:** [ARCHITECTURE.md](ARCHITECTURE.md)
- **File Format:** [FILE_FORMAT.md](FILE_FORMAT.md)
- **Type System:** [TYPE_SYSTEM.md](TYPE_SYSTEM.md)
- **Complete SQL Reference:** [SQL_LANGUAGE.md](SQL_LANGUAGE.md)

### Security Best Practices

- **Security Guide:** [SECURITY.md](SECURITY.md)
- **Error Handling:** [ERROR_HANDLING.md](ERROR_HANDLING.md)

### Testing and Development

- **Testing Guide:** [TESTING.md](TESTING.md)
- **API Reference:** [API.md](API.md)
- **Getting Started:** [GETTING_STARTED.md](GETTING_STARTED.md)

### Complete Documentation

Browse the [Documentation Index](INDEX.md) for all available guides.

## Getting Help

### Common Issues

**Database Locked:**
```go
// Use WAL mode for better concurrency
db.Exec("PRAGMA journal_mode = WAL")

// Set busy timeout
db.Exec("PRAGMA busy_timeout = 5000") // 5 seconds
```

**Constraint Violations:**
```go
// Check error messages
if strings.Contains(err.Error(), "UNIQUE constraint failed") {
    // Handle duplicate entry
}
```

**Performance Problems:**
```go
// Check for missing indexes
rows, _ := db.Query("EXPLAIN QUERY PLAN SELECT ...")
// Add indexes as needed
db.Exec("CREATE INDEX ...")
```

### Resources

- **SQLite Documentation:** https://www.sqlite.org/docs.html
- **Go database/sql:** https://pkg.go.dev/database/sql
- **Repository:** https://github.com/JuniperBible/Public.Lib.Anthony

## License

Anthony is in the public domain (SQLite License).

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.
