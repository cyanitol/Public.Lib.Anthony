# Anthony SQLite - API Documentation

This document provides comprehensive documentation for the public API of Anthony, a pure Go SQLite implementation.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Database/SQL Compatibility](#databasesql-compatibility)
- [Opening Connections](#opening-connections)
- [Configuration Options](#configuration-options)
- [Supported SQL Features](#supported-sql-features)
- [Common Operations](#common-operations)
- [Advanced Features](#advanced-features)
- [Security Configuration](#security-configuration)
- [Error Handling](#error-handling)
- [Performance Considerations](#performance-considerations)

## Overview

Anthony is a pure Go implementation of SQLite that provides full compatibility with Go's `database/sql` package. It requires no CGO dependencies and implements the SQLite file format and SQL dialect.

**Driver Name:** `sqlite_internal`

**Import Path:** `github.com/JuniperBible/Public.Lib.Anthony`

## Installation

```bash
go get github.com/JuniperBible/Public.Lib.Anthony
```

## Database/SQL Compatibility

Anthony implements the standard Go `database/sql/driver` interfaces:

- `driver.Driver` - Driver registration and connection creation
- `driver.Conn` - Connection management and statement preparation
- `driver.Stmt` - Prepared statement execution
- `driver.Tx` - Transaction management
- `driver.Rows` - Result set iteration
- `driver.Result` - Execution results (rows affected, last insert ID)

### Supported Interfaces

Anthony supports these optional `database/sql` interfaces:

- `driver.ConnBeginTx` - Transaction options (read-only transactions)
- `driver.ConnPrepareContext` - Context-aware statement preparation
- `driver.StmtExecContext` - Context-aware statement execution
- `driver.StmtQueryContext` - Context-aware query execution
- `driver.Pinger` - Connection health checks
- `driver.SessionResetter` - Connection session reset
- `driver.NamedValueChecker` - Parameter validation

## Opening Connections

### Method 1: Using database/sql (Recommended)

```go
import (
    "database/sql"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    // Open a file-based database
    db, err := sql.Open("sqlite_internal", "mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Verify connection is working
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
}
```

### Method 2: Using Convenience Functions

```go
import (
    "github.com/JuniperBible/Public.Lib.Anthony"
)

func main() {
    // Open a database (creates if doesn't exist)
    db, err := anthony.Open("mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Or open in read-only mode
    db, err := anthony.OpenReadOnly("mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
}
```

### Connection Strings

#### File Database

```go
// Basic file database
db, err := sql.Open("sqlite_internal", "path/to/database.sqlite")

// Current directory
db, err := sql.Open("sqlite_internal", "./database.db")

// Absolute path
db, err := sql.Open("sqlite_internal", "/var/lib/app/data.db")
```

#### In-Memory Database

```go
// Anonymous in-memory database (unique per connection)
db, err := sql.Open("sqlite_internal", ":memory:")

// Empty string also creates in-memory database
db, err := sql.Open("sqlite_internal", "")
```

**Note:** Each in-memory database connection creates a separate, independent database. Data is not shared between connections and is lost when the connection closes.

## Configuration Options

### Connection Pool Settings

Configure connection pooling using standard `database/sql` methods:

```go
db, err := sql.Open("sqlite_internal", "mydb.sqlite")

// Maximum number of open connections
db.SetMaxOpenConns(25)

// Maximum number of idle connections
db.SetMaxIdleConns(5)

// Maximum connection lifetime
db.SetConnMaxLifetime(5 * time.Minute)

// Maximum idle connection time
db.SetConnMaxIdleTime(10 * time.Minute)
```

**Important:** SQLite uses file-level locking, so multiple write connections provide limited benefit. For read-heavy workloads, multiple connections can improve concurrency.

### PRAGMA Configuration

Configure database behavior using PRAGMA statements:

```go
// Must be set before any operations
_, err = db.Exec("PRAGMA journal_mode=WAL")
_, err = db.Exec("PRAGMA synchronous=NORMAL")
_, err = db.Exec("PRAGMA foreign_keys=ON")
_, err = db.Exec("PRAGMA page_size=4096")

// Query PRAGMA values
var journalMode string
err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)

var foreignKeys bool
err = db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys)
```

See [PRAGMA Quick Reference](PRAGMA_QUICK_REFERENCE.md) for complete PRAGMA documentation.

## Supported SQL Features

Anthony supports a comprehensive subset of SQLite's SQL dialect:

### Data Definition Language (DDL)

#### CREATE TABLE

```go
_, err := db.Exec(`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER CHECK(age >= 0),
        balance REAL DEFAULT 0.0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
`)
```

**Supported Features:**
- All SQLite column types (INTEGER, TEXT, REAL, BLOB, NULL)
- PRIMARY KEY constraints (single and composite)
- AUTOINCREMENT
- UNIQUE constraints (column and table-level)
- NOT NULL constraints
- CHECK constraints
- DEFAULT values (constants and expressions)
- FOREIGN KEY constraints
- Column and table constraints

#### CREATE INDEX

```go
// Simple index
_, err := db.Exec("CREATE INDEX idx_email ON users(email)")

// Unique index
_, err := db.Exec("CREATE UNIQUE INDEX idx_username ON users(name)")

// Composite index
_, err := db.Exec("CREATE INDEX idx_age_name ON users(age, name)")

// Conditional index (partial index)
_, err := db.Exec("CREATE INDEX idx_active ON users(name) WHERE active = 1")
```

#### CREATE VIEW

```go
_, err := db.Exec(`
    CREATE VIEW adult_users AS
    SELECT id, name, email
    FROM users
    WHERE age >= 18
`)

// Query the view
rows, err := db.Query("SELECT * FROM adult_users")
```

#### CREATE TRIGGER

```sql
_, err := db.Exec(`
    CREATE TRIGGER update_timestamp
    AFTER UPDATE ON users
    FOR EACH ROW
    BEGIN
        UPDATE users SET modified_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END
`)
```

**Supported Trigger Types:**
- BEFORE/AFTER INSERT
- BEFORE/AFTER UPDATE
- BEFORE/AFTER DELETE
- FOR EACH ROW triggers
- WHEN conditions

#### ALTER TABLE

```go
// Rename table
_, err := db.Exec("ALTER TABLE users RENAME TO customers")

// Add column
_, err := db.Exec("ALTER TABLE users ADD COLUMN phone TEXT")

// Rename column
_, err := db.Exec("ALTER TABLE users RENAME COLUMN name TO full_name")

// Drop column
_, err := db.Exec("ALTER TABLE users DROP COLUMN phone")
```

See [ALTER TABLE Quick Reference](ALTER_TABLE_QUICK_REFERENCE.md) for details.

#### DROP Statements

```go
_, err := db.Exec("DROP TABLE users")
_, err := db.Exec("DROP TABLE IF EXISTS users")
_, err := db.Exec("DROP INDEX idx_email")
_, err := db.Exec("DROP VIEW adult_users")
_, err := db.Exec("DROP TRIGGER update_timestamp")
```

#### ATTACH/DETACH DATABASE

```sql
// Attach another database
_, err := db.Exec("ATTACH DATABASE 'other.db' AS other")

// Query across databases
rows, err := db.Query("SELECT * FROM other.users")

// Detach database
_, err := db.Exec("DETACH DATABASE other")
```

See [ATTACH/DETACH Implementation](ATTACH_DETACH_IMPLEMENTATION.md) for details.

### Data Manipulation Language (DML)

#### INSERT

```go
// Simple insert
_, err := db.Exec("INSERT INTO users (name, email) VALUES (?, ?)",
    "Alice", "alice@example.com")

// Insert with named columns
_, err := db.Exec(`
    INSERT INTO users (name, email, age)
    VALUES ('Bob', 'bob@example.com', 30)
`)

// Insert multiple rows
_, err := db.Exec(`
    INSERT INTO users (name, email) VALUES
        ('Charlie', 'charlie@example.com'),
        ('Diana', 'diana@example.com')
`)

// Get last insert ID
result, err := db.Exec("INSERT INTO users (name) VALUES (?)", "Eve")
lastID, err := result.LastInsertId()
fmt.Printf("Inserted row ID: %d\n", lastID)
```

**Upsert (INSERT OR REPLACE):**

```sql
_, err := db.Exec(`
    INSERT INTO users (id, name, email)
    VALUES (?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        name = excluded.name,
        email = excluded.email
`, 1, "Alice", "alice@example.com")
```

#### SELECT

```go
// Basic SELECT
rows, err := db.Query("SELECT id, name, email FROM users")
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string
    err := rows.Scan(&id, &name, &email)
    // Process row
}

// Check for errors
if err = rows.Err(); err != nil {
    log.Fatal(err)
}

// Single row query
var name string
var age int
err := db.QueryRow("SELECT name, age FROM users WHERE id = ?", 1).Scan(&name, &age)
if err == sql.ErrNoRows {
    // No matching row found
}
```

**WHERE Clauses:**

```sql
// Simple conditions
rows, err := db.Query("SELECT * FROM users WHERE age > ?", 18)

// Multiple conditions
rows, err := db.Query(`
    SELECT * FROM users
    WHERE age BETWEEN ? AND ?
    AND email LIKE ?
`, 18, 65, "%@example.com")

// IN clause
rows, err := db.Query(`
    SELECT * FROM users
    WHERE name IN (?, ?, ?)
`, "Alice", "Bob", "Charlie")
```

**ORDER BY:**

```sql
rows, err := db.Query("SELECT * FROM users ORDER BY name ASC")
rows, err := db.Query("SELECT * FROM users ORDER BY age DESC, name ASC")
```

**LIMIT and OFFSET:**

```sql
// First 10 rows
rows, err := db.Query("SELECT * FROM users LIMIT 10")

// Pagination: rows 11-20
rows, err := db.Query("SELECT * FROM users LIMIT 10 OFFSET 10")
```

**JOINs:**

```sql
rows, err := db.Query(`
    SELECT u.name, d.name as department
    FROM users u
    INNER JOIN departments d ON u.dept_id = d.id
`)

// LEFT JOIN
rows, err := db.Query(`
    SELECT u.name, d.name as department
    FROM users u
    LEFT JOIN departments d ON u.dept_id = d.id
`)
```

**Aggregate Functions:**

```sql
// COUNT
var count int
err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)

// SUM, AVG, MIN, MAX
var total, avg, min, max float64
err := db.QueryRow(`
    SELECT SUM(balance), AVG(balance), MIN(balance), MAX(balance)
    FROM users
`).Scan(&total, &avg, &min, &max)

// GROUP BY
rows, err := db.Query(`
    SELECT department, COUNT(*), AVG(salary)
    FROM employees
    GROUP BY department
    HAVING COUNT(*) > 5
`)
```

**Subqueries:**

```sql
// Scalar subquery
rows, err := db.Query(`
    SELECT name, age
    FROM users
    WHERE age > (SELECT AVG(age) FROM users)
`)

// IN subquery
rows, err := db.Query(`
    SELECT name
    FROM users
    WHERE id IN (SELECT user_id FROM active_sessions)
`)

// EXISTS subquery
rows, err := db.Query(`
    SELECT name
    FROM users u
    WHERE EXISTS (
        SELECT 1 FROM orders o WHERE o.user_id = u.id
    )
`)
```

See [Subquery Quick Reference](SUBQUERY_QUICK_REFERENCE.md) for details.

**Common Table Expressions (CTEs):**

```sql
rows, err := db.Query(`
    WITH adult_users AS (
        SELECT * FROM users WHERE age >= 18
    )
    SELECT name, email
    FROM adult_users
    WHERE email LIKE '%@example.com'
    ORDER BY name
`)

// Recursive CTE
rows, err := db.Query(`
    WITH RECURSIVE cnt(x) AS (
        SELECT 1
        UNION ALL
        SELECT x+1 FROM cnt WHERE x < 10
    )
    SELECT x FROM cnt
`)
```

See [CTE Usage Guide](CTE_USAGE_GUIDE.md) for details.

**Compound Queries:**

```sql
// UNION
rows, err := db.Query(`
    SELECT name FROM customers
    UNION
    SELECT name FROM suppliers
`)

// UNION ALL (includes duplicates)
rows, err := db.Query(`
    SELECT name FROM customers
    UNION ALL
    SELECT name FROM suppliers
`)

// INTERSECT
rows, err := db.Query(`
    SELECT name FROM customers
    INTERSECT
    SELECT name FROM suppliers
`)

// EXCEPT
rows, err := db.Query(`
    SELECT name FROM customers
    EXCEPT
    SELECT name FROM suppliers
`)
```

See [Compound SELECT Quick Reference](COMPOUND_SELECT_QUICK_REFERENCE.md) for details.

#### UPDATE

```go
// Simple update
result, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", 31, 1)

// Check rows affected
rowsAffected, err := result.RowsAffected()
fmt.Printf("Updated %d row(s)\n", rowsAffected)

// Update multiple columns
_, err := db.Exec(`
    UPDATE users
    SET name = ?, email = ?, modified_at = CURRENT_TIMESTAMP
    WHERE id = ?
`, "Alice Smith", "alice.smith@example.com", 1)

// Update with complex WHERE
_, err := db.Exec(`
    UPDATE users
    SET status = 'inactive'
    WHERE last_login < ? AND status = 'active'
`, time.Now().AddDate(0, -6, 0))
```

#### DELETE

```go
// Delete specific rows
result, err := db.Exec("DELETE FROM users WHERE id = ?", 1)

// Delete with conditions
_, err := db.Exec("DELETE FROM users WHERE age < ? AND status = ?", 13, "pending")

// Delete all rows (but keep table)
_, err := db.Exec("DELETE FROM users")

// Check rows deleted
rowsAffected, err := result.RowsAffected()
```

### Transaction Control

#### BEGIN/COMMIT/ROLLBACK

```go
// Standard transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute statements in transaction
_, err = tx.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE user = ?", "Alice")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
if err = tx.Commit(); err != nil {
    log.Fatal(err)
}
```

#### Read-Only Transactions

```go
// Begin read-only transaction
tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
    ReadOnly: true,
})
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Can only execute read operations
rows, err := tx.Query("SELECT * FROM users")
// ...

tx.Commit()
```

#### Transaction Isolation

Anthony uses SQLite's transaction model:
- Transactions provide snapshot isolation
- Write transactions are serialized (one at a time)
- Read transactions can run concurrently with one write transaction (in WAL mode)

### Built-in Functions

#### String Functions

```sql
// LENGTH, UPPER, LOWER, TRIM
rows, err := db.Query(`
    SELECT
        LENGTH(name) as name_len,
        UPPER(name) as upper_name,
        LOWER(email) as lower_email,
        TRIM(name) as trimmed
    FROM users
`)

// SUBSTR
rows, err := db.Query("SELECT SUBSTR(name, 1, 3) FROM users")

// REPLACE
rows, err := db.Query("SELECT REPLACE(email, '@', ' at ') FROM users")
```

#### Math Functions

```sql
// ABS, ROUND, CEIL, FLOOR
rows, err := db.Query(`
    SELECT
        ABS(balance) as abs_balance,
        ROUND(price, 2) as rounded_price,
        CEIL(value) as ceiling,
        FLOOR(value) as floor
    FROM accounts
`)

// MIN, MAX (can also be used as aggregate functions)
rows, err := db.Query("SELECT MIN(age), MAX(age) FROM users")
```

#### Date/Time Functions

```sql
// CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME
_, err := db.Exec(`
    INSERT INTO events (name, created_at)
    VALUES (?, CURRENT_TIMESTAMP)
`, "User Login")

// DATE, TIME, DATETIME
rows, err := db.Query(`
    SELECT
        DATE(created_at) as date_part,
        TIME(created_at) as time_part,
        DATETIME(created_at, '+1 day') as tomorrow
    FROM events
`)

// STRFTIME for custom formatting
rows, err := db.Query(`
    SELECT STRFTIME('%Y-%m-%d %H:%M', created_at)
    FROM events
`)
```

#### Aggregate Functions

```sql
// COUNT, SUM, AVG, MIN, MAX, TOTAL, GROUP_CONCAT
rows, err := db.Query(`
    SELECT
        department,
        COUNT(*) as employee_count,
        SUM(salary) as total_salary,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        GROUP_CONCAT(name, ', ') as names
    FROM employees
    GROUP BY department
`)
```

See [Functions Documentation](FUNCTIONS.md) for complete function reference.

### EXPLAIN

```go
// EXPLAIN query plan
rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18")
defer rows.Close()

for rows.Next() {
    var id, parent, detail int
    var plan string
    rows.Scan(&id, &parent, &detail, &plan)
    fmt.Printf("%s\n", plan)
}

// EXPLAIN bytecode
rows, err := db.Query("EXPLAIN SELECT * FROM users WHERE age > 18")
// Returns VDBE bytecode instructions
```

### VACUUM

```go
// Compact database and reclaim space
_, err := db.Exec("VACUUM")
if err != nil {
    log.Fatal(err)
}

// VACUUM into new file
_, err := db.Exec("VACUUM INTO 'compacted.db'")
```

See [VACUUM Usage](VACUUM_USAGE.md) for details.

## Common Operations

### Opening a Connection

```go
import (
    "database/sql"
    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    db, err := sql.Open("sqlite_internal", "mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Test the connection
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
}
```

### Running Queries

```go
// Query returning multiple rows
rows, err := db.Query("SELECT id, name, email FROM users WHERE age > ?", 18)
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

    fmt.Printf("User: %d, %s, %s\n", id, name, email)
}

// Check for errors during iteration
if err = rows.Err(); err != nil {
    log.Fatal(err)
}

// Query single row
var count int
err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
if err != nil {
    log.Fatal(err)
}
```

### Using Transactions

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Ensure rollback on panic or error
defer func() {
    if p := recover(); p != nil {
        tx.Rollback()
        panic(p)
    } else if err != nil {
        tx.Rollback()
    }
}()

// Execute operations
_, err = tx.Exec("INSERT INTO accounts (name, balance) VALUES (?, ?)", "Alice", 1000)
if err != nil {
    return err
}

_, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE name = ?", 100, "Alice")
if err != nil {
    return err
}

// Commit transaction
err = tx.Commit()
if err != nil {
    return err
}
```

### Prepared Statements

```go
// Prepare statement
stmt, err := db.Prepare("INSERT INTO users (name, email, age) VALUES (?, ?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute multiple times with different parameters
users := []struct {
    Name  string
    Email string
    Age   int
}{
    {"Alice", "alice@example.com", 30},
    {"Bob", "bob@example.com", 25},
    {"Charlie", "charlie@example.com", 35},
}

for _, u := range users {
    result, err := stmt.Exec(u.Name, u.Email, u.Age)
    if err != nil {
        log.Fatal(err)
    }

    id, _ := result.LastInsertId()
    fmt.Printf("Inserted user with ID: %d\n", id)
}
```

### Complete Example

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
    db, err := sql.Open("sqlite_internal", "example.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create table
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

    // Insert data
    result, err := db.Exec(`
        INSERT INTO users (name, email, age)
        VALUES (?, ?, ?)
    `, "Alice", "alice@example.com", 30)
    if err != nil {
        log.Fatal(err)
    }

    id, _ := result.LastInsertId()
    fmt.Printf("Inserted user with ID: %d\n", id)

    // Query data
    rows, err := db.Query("SELECT id, name, email, age FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    fmt.Println("\nUsers:")
    for rows.Next() {
        var id, age int
        var name, email string

        if err := rows.Scan(&id, &name, &email, &age); err != nil {
            log.Fatal(err)
        }

        fmt.Printf("  ID: %d, Name: %s, Email: %s, Age: %d\n",
            id, name, email, age)
    }

    if err = rows.Err(); err != nil {
        log.Fatal(err)
    }
}
```

## Advanced Features

### User-Defined Functions

Anthony allows registering custom SQL functions that can be used in queries.

#### Scalar Functions

```go
import (
    "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/functions"
)

// Define a custom function
type DoubleFunc struct{}

func (f *DoubleFunc) Invoke(args []functions.Value) (functions.Value, error) {
    if len(args) != 1 {
        return functions.Value{}, fmt.Errorf("double() requires 1 argument")
    }

    if args[0].IsNull() {
        return functions.NewNullValue(), nil
    }

    result := args[0].AsInt64() * 2
    return functions.NewIntValue(result), nil
}

// Register the function
// Note: This requires accessing the underlying driver connection
rawConn, err := db.Conn(context.Background())
if err != nil {
    log.Fatal(err)
}
defer rawConn.Close()

err = rawConn.Raw(func(driverConn interface{}) error {
    conn := driverConn.(*driver.Conn)
    return conn.CreateScalarFunction("double", 1, true, &DoubleFunc{})
})
if err != nil {
    log.Fatal(err)
}

// Use the function in queries
var result int
err = db.QueryRow("SELECT double(21)").Scan(&result)
fmt.Printf("double(21) = %d\n", result) // Output: 42
```

#### Aggregate Functions

```go
// Define a custom aggregate function
type ProductFunc struct {
    product  int64
    hasValue bool
}

func (f *ProductFunc) Step(args []functions.Value) error {
    if len(args) != 1 {
        return fmt.Errorf("product() requires 1 argument")
    }

    if !args[0].IsNull() {
        if !f.hasValue {
            f.product = 1
            f.hasValue = true
        }
        f.product *= args[0].AsInt64()
    }

    return nil
}

func (f *ProductFunc) Final() (functions.Value, error) {
    if !f.hasValue {
        return functions.NewNullValue(), nil
    }
    return functions.NewIntValue(f.product), nil
}

func (f *ProductFunc) Reset() {
    f.product = 1
    f.hasValue = false
}

// Register the aggregate function
err = rawConn.Raw(func(driverConn interface{}) error {
    conn := driverConn.(*driver.Conn)
    return conn.CreateAggregateFunction("product", 1, true, &ProductFunc{})
})

// Use in queries
var result int64
err = db.QueryRow("SELECT product(value) FROM numbers").Scan(&result)
```

### Multi-Database Support (ATTACH)

```sql
// Attach another database
_, err := db.Exec("ATTACH DATABASE 'other.db' AS other")
if err != nil {
    log.Fatal(err)
}

// Query across databases
rows, err := db.Query(`
    SELECT main.users.name, other.profiles.bio
    FROM main.users
    JOIN other.profiles ON main.users.id = other.profiles.user_id
`)

// Detach database when done
_, err = db.Exec("DETACH DATABASE other")
```

**Limits:**
- Default maximum: 10 attached databases (configurable via security settings)
- Main database doesn't count toward limit

### PRAGMA Introspection

```go
// Get table schema
rows, err := db.Query("PRAGMA table_info(users)")
defer rows.Close()

for rows.Next() {
    var cid int
    var name, colType string
    var notNull, pk int
    var dfltValue sql.NullString

    err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Column: %s, Type: %s, NotNull: %t, PK: %t\n",
        name, colType, notNull == 1, pk == 1)
}

// List all tables
rows, err = db.Query("SELECT name FROM sqlite_master WHERE type='table'")
defer rows.Close()

for rows.Next() {
    var tableName string
    rows.Scan(&tableName)
    fmt.Println(tableName)
}

// Get database statistics
var pageSize, pageCount int
db.QueryRow("PRAGMA page_size").Scan(&pageSize)
db.QueryRow("PRAGMA page_count").Scan(&pageCount)
fmt.Printf("Database size: %d pages * %d bytes = %d bytes\n",
    pageCount, pageSize, pageCount*pageSize)
```

## Security Configuration

Anthony includes a comprehensive security model with configurable protections.

### Default Security Settings

By default, Anthony enables:
- Path validation (blocks null bytes, path traversal)
- Sandbox enforcement (confines databases to specific directory)
- Symlink blocking
- Reasonable resource limits

### Security Config Structure

```go
import "github.com/JuniperBible/Public.Lib.Anthony/internal/security"

type SecurityConfig struct {
    // DatabaseRoot: Root directory for database files (sandbox)
    DatabaseRoot string

    // BlockNullBytes: Block null bytes in paths (always enabled)
    BlockNullBytes bool

    // BlockTraversal: Block ".." in paths (always enabled)
    BlockTraversal bool

    // BlockSymlinks: Prevent following symlinks
    BlockSymlinks bool

    // BlockAbsolutePaths: Reject absolute paths
    BlockAbsolutePaths bool

    // EnforceSandbox: Enforce DatabaseRoot sandbox
    EnforceSandbox bool

    // AllowedSubdirs: Restrict to specific subdirectories (optional)
    AllowedSubdirs []string

    // CreateMode: File permissions for new databases (default: 0600)
    CreateMode os.FileMode

    // DirMode: Directory permissions (default: 0700)
    DirMode os.FileMode

    // MaxAttachedDBs: Maximum number of attached databases (default: 10)
    MaxAttachedDBs int
}
```

### Configuring Security

**Note:** Security configuration is set at the driver level and applies to all connections. Individual connection security cannot be configured through the `database/sql` API.

### Security Best Practices

1. **Always use parameterized queries** to prevent SQL injection:
   ```sql
   // Good
   db.Exec("SELECT * FROM users WHERE id = ?", userID)

   // Bad - vulnerable to SQL injection
   db.Exec(fmt.Sprintf("SELECT * FROM users WHERE id = %s", userID))
   ```

2. **Set appropriate file permissions:**
   ```go
   // Restrict database file access
   os.Chmod("mydb.sqlite", 0600) // Owner read/write only
   ```

3. **Use read-only transactions when possible:**
   ```go
   tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
   ```

4. **Validate and sanitize user input** before using in queries

5. **Use CHECK constraints** to enforce data integrity:
   ```sql
   db.Exec(`
       CREATE TABLE users (
           age INTEGER CHECK(age >= 0 AND age <= 150),
           email TEXT CHECK(email LIKE '%@%.%')
       )
   `)
   ```

6. **Enable foreign keys** for referential integrity:
   ```go
   db.Exec("PRAGMA foreign_keys = ON")
   ```

See [Security Guide](SECURITY.md) for comprehensive security documentation.

## Error Handling

### Common Errors

```go
import "database/sql"

result, err := db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice")
if err != nil {
    // Check for specific error types
    if strings.Contains(err.Error(), "UNIQUE constraint failed") {
        // Handle duplicate key error
        fmt.Println("User already exists")
    } else if strings.Contains(err.Error(), "CHECK constraint failed") {
        // Handle constraint violation
        fmt.Println("Data violates CHECK constraint")
    } else {
        // Other errors
        log.Fatal(err)
    }
}

// Query errors
var name string
err := db.QueryRow("SELECT name FROM users WHERE id = ?", 999).Scan(&name)
if err == sql.ErrNoRows {
    // No matching row found
    fmt.Println("User not found")
} else if err != nil {
    log.Fatal(err)
}
```

### Transaction Error Handling

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

defer func() {
    if p := recover(); p != nil {
        tx.Rollback()
        panic(p) // Re-throw panic
    } else if err != nil {
        tx.Rollback()
    }
}()

// Perform operations
_, err = tx.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
if err != nil {
    return // Deferred rollback will execute
}

err = tx.Commit()
```

### Common Error Messages

| Error | Meaning | Solution |
|-------|---------|----------|
| `UNIQUE constraint failed: users.email` | Duplicate value in UNIQUE column | Check for existing value before insert, or use INSERT OR IGNORE/REPLACE |
| `CHECK constraint failed` | Data violates CHECK constraint | Validate data before insertion |
| `FOREIGN KEY constraint failed` | Referenced row doesn't exist | Ensure referenced row exists, or disable foreign keys temporarily |
| `database is locked` | Another transaction holds exclusive lock | Retry operation, use WAL mode for better concurrency |
| `no such table: tablename` | Table doesn't exist | Create table first, or check table name spelling |
| `no such column: colname` | Column doesn't exist | Check column name, or add column with ALTER TABLE |
| `near "token": syntax error` | SQL syntax error | Check SQL syntax |

## Performance Considerations

### Indexing

Create indexes on frequently queried columns:

```sql
// Create indexes for better performance
db.Exec("CREATE INDEX idx_users_email ON users(email)")
db.Exec("CREATE INDEX idx_users_age ON users(age)")

// Composite index for multi-column queries
db.Exec("CREATE INDEX idx_users_age_name ON users(age, name)")
```

### Batch Operations

Use transactions for bulk inserts:

```go
// Without transaction: Very slow (each insert is a transaction)
for i := 0; i < 10000; i++ {
    db.Exec("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("User%d", i))
}

// With transaction: Much faster
tx, _ := db.Begin()
for i := 0; i < 10000; i++ {
    tx.Exec("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("User%d", i))
}
tx.Commit()
```

### Prepared Statements

Reuse prepared statements for repeated queries:

```go
stmt, _ := db.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
defer stmt.Close()

for _, user := range users {
    stmt.Exec(user.Name, user.Age)
}
```

### Write-Ahead Logging (WAL)

Enable WAL mode for better concurrency:

```go
// Enable WAL mode
db.Exec("PRAGMA journal_mode=WAL")

// WAL mode allows:
// - Multiple concurrent readers
// - One writer + multiple readers
// - Better performance for write-heavy workloads
```

### Query Optimization

```sql
// Use EXPLAIN QUERY PLAN to understand query execution
rows, _ := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18")
for rows.Next() {
    var id, parent, detail int
    var plan string
    rows.Scan(&id, &parent, &detail, &plan)
    fmt.Println(plan)
}

// Optimize queries based on plan
// - Add indexes where full table scans occur
// - Rewrite queries to use indexes
// - Add ANALYZE to update query planner statistics
```

### Connection Pool Tuning

```go
// For read-heavy workloads
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(5)

// For write-heavy workloads (SQLite serializes writes)
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
```

### Database Optimization

```sql
// Analyze database for query optimizer
db.Exec("ANALYZE")

// Compact database and reclaim space
db.Exec("VACUUM")

// Set appropriate page size (must be set before first write)
db.Exec("PRAGMA page_size = 4096")

// Set synchronous mode for performance/durability tradeoff
db.Exec("PRAGMA synchronous = NORMAL") // Balanced
// db.Exec("PRAGMA synchronous = FULL")   // Maximum durability
// db.Exec("PRAGMA synchronous = OFF")    // Maximum performance (dangerous)
```

## See Also

- [QUICKSTART.md](QUICKSTART.md) - Getting started tutorial
- [USER_GUIDE.md](USER_GUIDE.md) - Complete user guide and examples
- [FUNCTIONS.md](FUNCTIONS.md) - Built-in SQL function reference
- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Error handling patterns and best practices
- [Architecture Overview](ARCHITECTURE.md) - Internal architecture
- [Package Documentation](INDEX.md) - Complete package documentation
- [Security Guide](SECURITY.md) - Security model and best practices
- [PRAGMA Reference](PRAGMA_QUICK_REFERENCE.md) - PRAGMA configuration
- [CTE Usage Guide](CTE_USAGE_GUIDE.md) - Common Table Expressions
- [Subquery Reference](SUBQUERY_QUICK_REFERENCE.md) - Subquery support
- [SQLite Documentation](https://www.sqlite.org/docs.html) - Official SQLite docs
- [C API Introduction (local)](sqlite/C_API_INTRO.md) - SQLite C API background
- [C API Reference Index (local)](sqlite/c-api/API_INDEX.md) - Full C API listing

## License

Anthony is in the public domain (SQLite License).

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.
