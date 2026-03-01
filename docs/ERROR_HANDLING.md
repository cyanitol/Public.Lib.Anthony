# Error Handling and Debugging Guide

## Table of Contents

- [Overview](#overview)
- [Error Types](#error-types)
  - [Driver Errors](#driver-errors)
  - [Security Errors](#security-errors)
  - [VDBE Errors](#vdbe-errors)
  - [Constraint Errors](#constraint-errors)
- [SQLite Error Codes](#sqlite-error-codes)
- [Common Errors and Solutions](#common-errors-and-solutions)
  - [Connection Errors](#connection-errors)
  - [Transaction Errors](#transaction-errors)
  - [Constraint Violations](#constraint-violations)
  - [Parse Errors](#parse-errors)
  - [Security Violations](#security-violations)
- [Error Handling Patterns](#error-handling-patterns)
  - [Basic Error Handling](#basic-error-handling)
  - [Wrapped Errors](#wrapped-errors)
  - [Transaction Rollback](#transaction-rollback)
  - [Resource Cleanup](#resource-cleanup)
- [Debugging Techniques](#debugging-techniques)
  - [PRAGMA integrity_check](#pragma-integrity_check)
  - [EXPLAIN Query Plans](#explain-query-plans)
  - [Statement Tracing](#statement-tracing)
  - [Debug Information](#debug-information)
- [Database Diagnostics](#database-diagnostics)
  - [Schema Inspection](#schema-inspection)
  - [Index Analysis](#index-analysis)
  - [Integrity Verification](#integrity-verification)
- [Troubleshooting Scenarios](#troubleshooting-scenarios)
  - [Performance Issues](#performance-issues)
  - [Concurrency Problems](#concurrency-problems)
  - [Data Corruption](#data-corruption)
  - [Memory Issues](#memory-issues)
- [Best Practices](#best-practices)

## Overview

The Anthony SQLite implementation provides comprehensive error handling with:

- Descriptive error messages with context
- Error wrapping for traceability
- Proper resource cleanup on errors
- Security-focused error checking
- Integration with Go's standard error patterns

All errors follow Go conventions and can be inspected using `errors.Is()`, `errors.As()`, and `fmt.Errorf()` wrapping.

## Error Types

### Driver Errors

Driver errors occur during connection management, statement preparation, and execution:

```go
import (
    "database/sql"
    "database/sql/driver"
    "errors"
)

// ErrBadConn - Connection is closed or invalid
if errors.Is(err, driver.ErrBadConn) {
    // Handle closed connection
    // Typically indicates connection was closed or became invalid
}

// ErrSkip - Use default driver behavior
// Returned by CheckNamedValue to indicate type should use default handling
```

**Common Driver Error Messages:**

- `"parse error: ..."` - SQL syntax error during statement preparation
- `"compile error: ..."` - Error compiling SQL to VDBE bytecode
- `"execution error: ..."` - Error during statement execution
- `"auto-commit error: ..."` - Error committing implicit transaction
- `"no statements found"` - Empty SQL string provided
- `"multiple statements not supported"` - Multiple SQL statements in single prepare
- `"transaction already in progress"` - Attempted to begin nested transaction
- `"failed to begin read/write transaction: ..."` - Transaction start failed
- `"cannot reset session with active transaction"` - Session reset attempted during transaction

### Security Errors

Security errors are defined in `internal/security/errors.go`:

```go
import "github.com/JuniperBible/Public.Lib.Anthony/internal/security"

var (
    // Path security errors
    ErrNullByte         = errors.New("security: path contains null byte")
    ErrTraversal        = errors.New("security: path traversal attempt detected")
    ErrEscapesSandbox   = errors.New("security: path escapes sandbox")
    ErrNotInAllowlist   = errors.New("security: path not in allowed directories")
    ErrSymlink          = errors.New("security: symlinks not allowed")
    ErrAbsolutePath     = errors.New("security: absolute paths not allowed")

    // Integer overflow errors
    ErrIntegerOverflow  = errors.New("security: integer overflow detected")
    ErrIntegerUnderflow = errors.New("security: integer underflow detected")

    // Buffer errors
    ErrBufferOverflow   = errors.New("security: buffer overflow")
)
```

**Example: Handling security errors**

```go
db, err := sql.Open("sqlite_internal", "/path/to/database.db")
if err != nil {
    if errors.Is(err, security.ErrTraversal) {
        log.Fatal("Path traversal attempt detected")
    }
    if errors.Is(err, security.ErrEscapesSandbox) {
        log.Fatal("Path escapes sandbox")
    }
    log.Fatal(err)
}
```

### VDBE Errors

VDBE (Virtual Database Engine) errors occur during bytecode execution:

**Common VDBE Error Scenarios:**

- Invalid cursor state
- Register access out of bounds
- Type conversion failures
- Arithmetic overflow
- Invalid opcode parameters
- Cursor EOF conditions

**Example: VDBE error handling**

```go
// VDBE errors are wrapped with context
rows, err := db.Query("SELECT * FROM users WHERE id = ?", userID)
if err != nil {
    // Error will include context like:
    // "execution error: invalid cursor state"
    // "execution error: register out of bounds"
    log.Printf("Query failed: %v", err)
    return err
}
defer rows.Close()
```

### Constraint Errors

Constraint violations occur when data doesn't meet schema requirements:

**Common Constraint Error Types:**

- `UNIQUE constraint failed` - Duplicate value in unique column
- `PRIMARY KEY constraint failed` - Duplicate primary key
- `NOT NULL constraint failed` - NULL value in NOT NULL column
- `FOREIGN KEY constraint failed` - Invalid foreign key reference
- `CHECK constraint failed` - CHECK condition not met

**Example: Handling constraint violations**

```go
_, err := db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", id, email)
if err != nil {
    if strings.Contains(err.Error(), "UNIQUE constraint failed") {
        // Handle duplicate entry
        return fmt.Errorf("email already exists: %w", err)
    }
    if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
        // Handle invalid reference
        return fmt.Errorf("invalid reference: %w", err)
    }
    return err
}
```

## SQLite Error Codes

While the pure Go implementation doesn't expose numeric error codes directly, error messages correspond to SQLite error conditions:

| Error Type | Message Pattern | SQLite Equivalent |
|------------|----------------|-------------------|
| SQLITE_ERROR | `"execution error: ..."` | General SQL error |
| SQLITE_BUSY | `"database is locked"` | Database locked |
| SQLITE_LOCKED | `"table is locked"` | Table locked |
| SQLITE_NOMEM | `"out of memory"` | Memory allocation failed |
| SQLITE_READONLY | `"attempt to write a readonly database"` | Read-only database |
| SQLITE_INTERRUPT | `"interrupted"` | Operation interrupted |
| SQLITE_IOERR | `"disk I/O error"` | I/O error |
| SQLITE_CORRUPT | `"database corruption"` | Database malformed |
| SQLITE_FULL | `"database or disk is full"` | Disk full |
| SQLITE_CANTOPEN | `"unable to open database file"` | Can't open file |
| SQLITE_CONSTRAINT | `"constraint failed"` | Constraint violation |
| SQLITE_MISMATCH | `"datatype mismatch"` | Type mismatch |
| SQLITE_MISUSE | `"bad parameter"` | API misuse |
| SQLITE_RANGE | `"bind parameter out of range"` | Parameter index error |

## Common Errors and Solutions

### Connection Errors

**Problem:** Connection fails to open

```go
db, err := sql.Open("sqlite_internal", "file.db")
if err != nil {
    // Handle error
}
```

**Solutions:**

1. **File permission denied:**
   ```bash
   chmod 644 file.db  # Ensure read/write permissions
   ```

2. **Directory doesn't exist:**
   ```go
   // Create directory first
   os.MkdirAll(filepath.Dir(dbPath), 0755)
   db, err := sql.Open("sqlite_internal", dbPath)
   ```

3. **Invalid path characters:**
   ```go
   // Avoid special characters, use clean paths
   dbPath := filepath.Clean(userPath)
   ```

### Transaction Errors

**Problem:** `"transaction already in progress"`

```go
tx1, _ := db.Begin()
tx2, err := db.Begin() // Error: transaction already in progress
```

**Solution:** Use proper transaction nesting or savepoints

```go
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback() // Safe to call even after commit

// Use savepoints for nested operations
_, err = tx.Exec("SAVEPOINT sp1")
// ... operations ...
_, err = tx.Exec("RELEASE SAVEPOINT sp1")

// Commit outer transaction
if err := tx.Commit(); err != nil {
    return fmt.Errorf("commit failed: %w", err)
}
```

**Problem:** `"cannot reset session with active transaction"`

**Solution:** Commit or rollback before closing connection

```go
// Always complete transactions
if tx != nil {
    tx.Rollback() // or Commit()
}
db.Close()
```

### Constraint Violations

**Problem:** UNIQUE constraint failed

```go
_, err := db.Exec("INSERT INTO users (email) VALUES (?)", "test@example.com")
// Error: UNIQUE constraint failed: users.email
```

**Solutions:**

1. **Use INSERT OR REPLACE:**
   ```go
   _, err := db.Exec("INSERT OR REPLACE INTO users (email, name) VALUES (?, ?)",
                     email, name)
   ```

2. **Use INSERT OR IGNORE:**
   ```go
   _, err := db.Exec("INSERT OR IGNORE INTO users (email) VALUES (?)", email)
   ```

3. **Check before insert:**
   ```go
   var count int
   err := db.QueryRow("SELECT COUNT(*) FROM users WHERE email = ?", email).Scan(&count)
   if err != nil {
       return err
   }
   if count > 0 {
       return fmt.Errorf("email already exists")
   }
   _, err = db.Exec("INSERT INTO users (email) VALUES (?)", email)
   ```

### Parse Errors

**Problem:** `"parse error: syntax error near ..."`

**Solutions:**

1. **Check SQL syntax:**
   ```go
   // Bad
   db.Exec("SELECT * FROM users WHERE")

   // Good
   db.Exec("SELECT * FROM users WHERE id = ?", 1)
   ```

2. **Use EXPLAIN to validate:**
   ```go
   rows, err := db.Query("EXPLAIN SELECT * FROM users WHERE id = ?", 1)
   if err != nil {
       log.Printf("Invalid SQL: %v", err)
   }
   defer rows.Close()
   ```

3. **Validate table/column names:**
   ```go
   // Check if table exists
   var name string
   err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                      tableName).Scan(&name)
   if err == sql.ErrNoRows {
       return fmt.Errorf("table %s does not exist", tableName)
   }
   ```

### Security Violations

**Problem:** Path traversal attempt

```go
path := "../../../etc/passwd"
db, err := sql.Open("sqlite_internal", path)
// Error: security: path traversal attempt detected
```

**Solution:** Use validated paths only

```go
import "github.com/JuniperBible/Public.Lib.Anthony/internal/security"

// Validate path before use
validPath, err := security.ValidatePath(userPath, config)
if err != nil {
    return fmt.Errorf("invalid path: %w", err)
}

db, err := sql.Open("sqlite_internal", validPath)
```

## Error Handling Patterns

### Basic Error Handling

```go
package main

import (
    "database/sql"
    "log"
    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    db, err := sql.Open("sqlite_internal", "test.db")
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    // Ping to verify connection
    if err := db.Ping(); err != nil {
        log.Fatalf("Failed to ping database: %v", err)
    }

    // Execute with error handling
    result, err := db.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    if err != nil {
        log.Fatalf("Failed to create table: %v", err)
    }

    rowsAffected, _ := result.RowsAffected()
    log.Printf("Rows affected: %d", rowsAffected)
}
```

### Wrapped Errors

Use error wrapping to preserve error context:

```go
func createUser(db *sql.DB, name, email string) error {
    // Validate input
    if name == "" {
        return fmt.Errorf("invalid input: name cannot be empty")
    }

    // Execute with wrapped errors
    _, err := db.Exec("INSERT INTO users (name, email) VALUES (?, ?)", name, email)
    if err != nil {
        return fmt.Errorf("failed to create user %s: %w", name, err)
    }

    return nil
}

// Calling code can unwrap
err := createUser(db, "John", "john@example.com")
if err != nil {
    // Error chain preserved: "failed to create user John: UNIQUE constraint failed"
    var constraint *ConstraintError
    if errors.As(err, &constraint) {
        // Handle constraint-specific error
    }
    log.Printf("Error: %v", err)
}
```

### Transaction Rollback

Always use defer for rollback safety:

```go
func transferFunds(db *sql.DB, fromID, toID int, amount float64) error {
    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }

    // Defer rollback - safe to call even after commit
    defer tx.Rollback()

    // Deduct from source account
    _, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", amount, fromID)
    if err != nil {
        return fmt.Errorf("failed to deduct from account %d: %w", fromID, err)
    }

    // Add to destination account
    _, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, toID)
    if err != nil {
        return fmt.Errorf("failed to add to account %d: %w", toID, err)
    }

    // Commit transaction
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    return nil
}
```

### Resource Cleanup

Proper cleanup prevents resource leaks:

```go
func queryUsers(db *sql.DB) ([]User, error) {
    // Prepare statement
    stmt, err := db.Prepare("SELECT id, name, email FROM users WHERE active = ?")
    if err != nil {
        return nil, fmt.Errorf("failed to prepare statement: %w", err)
    }
    defer stmt.Close() // Always close statements

    // Query with statement
    rows, err := stmt.Query(true)
    if err != nil {
        return nil, fmt.Errorf("failed to query users: %w", err)
    }
    defer rows.Close() // Always close rows

    var users []User
    for rows.Next() {
        var u User
        if err := rows.Scan(&u.ID, &u.Name, &u.Email); err != nil {
            return nil, fmt.Errorf("failed to scan row: %w", err)
        }
        users = append(users, u)
    }

    // Check for iteration errors
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("row iteration error: %w", err)
    }

    return users, nil
}
```

## Debugging Techniques

### PRAGMA integrity_check

Verify database integrity:

```go
// Basic integrity check
var result string
err := db.QueryRow("PRAGMA integrity_check").Scan(&result)
if err != nil {
    log.Fatalf("Integrity check failed: %v", err)
}

if result != "ok" {
    log.Fatalf("Database corruption detected: %s", result)
}

// Detailed integrity check (limit to 100 errors)
rows, err := db.Query("PRAGMA integrity_check(100)")
if err != nil {
    log.Fatalf("Integrity check failed: %v", err)
}
defer rows.Close()

for rows.Next() {
    var msg string
    if err := rows.Scan(&msg); err != nil {
        log.Fatal(err)
    }
    if msg != "ok" {
        log.Printf("Integrity error: %s", msg)
    }
}

// Quick check (faster, less thorough)
err = db.QueryRow("PRAGMA quick_check").Scan(&result)
if err != nil {
    log.Fatalf("Quick check failed: %v", err)
}
```

**Safe PRAGMA operations:**

```go
// The following PRAGMAs are whitelisted for security:

// Informational (read-only)
db.Query("PRAGMA table_info(users)")           // Table structure
db.Query("PRAGMA index_list(users)")           // Table indexes
db.Query("PRAGMA foreign_key_list(orders)")    // Foreign keys
db.Query("PRAGMA database_list")               // Attached databases
db.Query("PRAGMA compile_options")             // Compile options

// Configuration (safe to modify)
db.Exec("PRAGMA foreign_keys = ON")            // Enable FK checks
db.Exec("PRAGMA cache_size = 10000")           // Set cache size
db.Exec("PRAGMA journal_mode = WAL")           // Set journal mode
db.Exec("PRAGMA synchronous = NORMAL")         // Sync mode
db.Exec("PRAGMA user_version = 1")             // User version

// Note: Dangerous PRAGMAs are blocked:
// - writable_schema (allows schema corruption)
// - ignore_check_constraints (bypasses constraints)
// - trusted_schema (security bypass)
```

### EXPLAIN Query Plans

Understand query execution and optimize performance:

```go
// EXPLAIN shows execution plan
rows, err := db.Query("EXPLAIN SELECT * FROM users WHERE email = ?", "test@example.com")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

fmt.Println("Execution Plan:")
for rows.Next() {
    var addr, opcode, p1, p2, p3, p4, p5, comment interface{}
    if err := rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%v %v %v %v %v %v %v %v\n",
               addr, opcode, p1, p2, p3, p4, p5, comment)
}

// EXPLAIN QUERY PLAN shows high-level strategy
rows, err = db.Query("EXPLAIN QUERY PLAN SELECT u.name, o.total " +
                     "FROM users u JOIN orders o ON u.id = o.user_id " +
                     "WHERE u.email = ?", "test@example.com")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

fmt.Println("\nQuery Plan:")
for rows.Next() {
    var id, parent, notused int
    var detail string
    if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%d|%d|%s\n", id, parent, detail)
}
```

### Statement Tracing

Log SQL statements for debugging:

```go
type LoggingDB struct {
    *sql.DB
}

func (db *LoggingDB) Exec(query string, args ...interface{}) (sql.Result, error) {
    log.Printf("EXEC: %s %v", query, args)
    result, err := db.DB.Exec(query, args...)
    if err != nil {
        log.Printf("ERROR: %v", err)
    }
    return result, err
}

func (db *LoggingDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
    log.Printf("QUERY: %s %v", query, args)
    rows, err := db.DB.Query(query, args...)
    if err != nil {
        log.Printf("ERROR: %v", err)
    }
    return rows, err
}

// Usage
realDB, _ := sql.Open("sqlite_internal", "test.db")
db := &LoggingDB{realDB}
db.Exec("INSERT INTO users (name) VALUES (?)", "John")
// Output: EXEC: INSERT INTO users (name) VALUES (?) [John]
```

### Debug Information

Collect diagnostic information:

```go
func collectDiagnostics(db *sql.DB) {
    // Database version
    var version string
    db.QueryRow("SELECT sqlite_version()").Scan(&version)
    log.Printf("SQLite version: %s", version)

    // Page size and count
    var pageSize, pageCount int
    db.QueryRow("PRAGMA page_size").Scan(&pageSize)
    db.QueryRow("PRAGMA page_count").Scan(&pageCount)
    log.Printf("Database size: %d pages x %d bytes = %d MB",
               pageCount, pageSize, (pageCount*pageSize)/1024/1024)

    // Free pages
    var freePages int
    db.QueryRow("PRAGMA freelist_count").Scan(&freePages)
    log.Printf("Free pages: %d (%.1f%%)", freePages,
               float64(freePages)/float64(pageCount)*100)

    // Journal mode
    var journalMode string
    db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
    log.Printf("Journal mode: %s", journalMode)

    // Foreign keys
    var fkEnabled int
    db.QueryRow("PRAGMA foreign_keys").Scan(&fkEnabled)
    log.Printf("Foreign keys: %v", fkEnabled == 1)

    // Schema version
    var schemaVersion int
    db.QueryRow("PRAGMA schema_version").Scan(&schemaVersion)
    log.Printf("Schema version: %d", schemaVersion)
}
```

## Database Diagnostics

### Schema Inspection

```go
// List all tables
func listTables(db *sql.DB) ([]string, error) {
    rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var tables []string
    for rows.Next() {
        var name string
        if err := rows.Scan(&name); err != nil {
            return nil, err
        }
        tables = append(tables, name)
    }
    return tables, nil
}

// Get table schema
func getTableSchema(db *sql.DB, tableName string) error {
    rows, err := db.Query("PRAGMA table_info(?)", tableName)
    if err != nil {
        return err
    }
    defer rows.Close()

    fmt.Printf("Schema for table '%s':\n", tableName)
    for rows.Next() {
        var cid int
        var name, typ string
        var notnull, pk int
        var dfltValue *string

        err := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk)
        if err != nil {
            return err
        }

        fmt.Printf("  %d: %s %s", cid, name, typ)
        if notnull == 1 {
            fmt.Print(" NOT NULL")
        }
        if pk > 0 {
            fmt.Print(" PRIMARY KEY")
        }
        if dfltValue != nil {
            fmt.Printf(" DEFAULT %s", *dfltValue)
        }
        fmt.Println()
    }
    return nil
}
```

### Index Analysis

```go
// List indexes for a table
func listIndexes(db *sql.DB, tableName string) error {
    rows, err := db.Query("PRAGMA index_list(?)", tableName)
    if err != nil {
        return err
    }
    defer rows.Close()

    fmt.Printf("Indexes for table '%s':\n", tableName)
    for rows.Next() {
        var seq int
        var name string
        var unique int
        var origin string
        var partial int

        err := rows.Scan(&seq, &name, &unique, &origin, &partial)
        if err != nil {
            return err
        }

        fmt.Printf("  %s", name)
        if unique == 1 {
            fmt.Print(" (UNIQUE)")
        }
        fmt.Printf(" origin=%s", origin)
        if partial == 1 {
            fmt.Print(" (PARTIAL)")
        }
        fmt.Println()

        // Show index columns
        showIndexColumns(db, name)
    }
    return nil
}

func showIndexColumns(db *sql.DB, indexName string) {
    rows, err := db.Query("PRAGMA index_info(?)", indexName)
    if err != nil {
        return
    }
    defer rows.Close()

    fmt.Print("    Columns: ")
    var cols []string
    for rows.Next() {
        var seqno, cid int
        var name *string
        if err := rows.Scan(&seqno, &cid, &name); err != nil {
            return
        }
        if name != nil {
            cols = append(cols, *name)
        }
    }
    fmt.Printf("%v\n", cols)
}
```

### Integrity Verification

```go
// Comprehensive database check
func verifyDatabase(db *sql.DB) error {
    // 1. Integrity check
    var result string
    err := db.QueryRow("PRAGMA integrity_check").Scan(&result)
    if err != nil {
        return fmt.Errorf("integrity check failed: %w", err)
    }
    if result != "ok" {
        return fmt.Errorf("database corruption: %s", result)
    }

    // 2. Foreign key check
    rows, err := db.Query("PRAGMA foreign_key_check")
    if err != nil {
        return fmt.Errorf("foreign key check failed: %w", err)
    }
    defer rows.Close()

    var fkErrors []string
    for rows.Next() {
        var table, rowid, parent, fkid interface{}
        if err := rows.Scan(&table, &rowid, &parent, &fkid); err != nil {
            return err
        }
        fkErrors = append(fkErrors, fmt.Sprintf("table=%v rowid=%v parent=%v fk=%v",
                                                table, rowid, parent, fkid))
    }
    if len(fkErrors) > 0 {
        return fmt.Errorf("foreign key violations: %v", fkErrors)
    }

    // 3. Index verification
    tables, err := listTables(db)
    if err != nil {
        return err
    }

    for _, table := range tables {
        _, err := db.Exec(fmt.Sprintf("REINDEX %s", table))
        if err != nil {
            return fmt.Errorf("reindex failed for %s: %w", table, err)
        }
    }

    log.Println("Database verification passed")
    return nil
}
```

## Troubleshooting Scenarios

### Performance Issues

**Symptoms:**
- Slow queries
- High CPU usage
- Long wait times

**Diagnosis:**

```go
// 1. Analyze query plan
rows, _ := db.Query("EXPLAIN QUERY PLAN SELECT * FROM orders WHERE user_id = ?", 123)
// Look for "SCAN TABLE" (bad) vs "SEARCH TABLE USING INDEX" (good)

// 2. Check for missing indexes
rows, _ = db.Query("SELECT name FROM sqlite_master WHERE type='index'")

// 3. Analyze table statistics
db.Exec("ANALYZE")

// 4. Check cache settings
var cacheSize int
db.QueryRow("PRAGMA cache_size").Scan(&cacheSize)
log.Printf("Cache size: %d pages", cacheSize)
```

**Solutions:**

```go
// 1. Add indexes
db.Exec("CREATE INDEX idx_orders_user_id ON orders(user_id)")

// 2. Increase cache
db.Exec("PRAGMA cache_size = 10000") // 10000 pages

// 3. Use WAL mode for better concurrency
db.Exec("PRAGMA journal_mode = WAL")

// 4. Vacuum to reclaim space
db.Exec("VACUUM")
```

### Concurrency Problems

**Symptoms:**
- Database locked errors
- Deadlocks
- Unexpected data

**Diagnosis:**

```go
// Check locking mode
var lockingMode string
db.QueryRow("PRAGMA locking_mode").Scan(&lockingMode)
log.Printf("Locking mode: %s", lockingMode)

// Check journal mode
var journalMode string
db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
log.Printf("Journal mode: %s", journalMode)
```

**Solutions:**

```go
// 1. Use WAL mode for better read/write concurrency
db.Exec("PRAGMA journal_mode = WAL")

// 2. Set busy timeout
db.Exec("PRAGMA busy_timeout = 5000") // 5 seconds

// 3. Use proper transaction isolation
tx, _ := db.BeginTx(context.Background(), &sql.TxOptions{
    Isolation: sql.LevelSerializable,
    ReadOnly:  false,
})

// 4. Ensure transactions are short-lived
// Start transaction
// Do minimal work
// Commit immediately
```

### Data Corruption

**Symptoms:**
- Integrity check failures
- Malformed data
- Unexpected NULL values

**Diagnosis:**

```go
// Run full integrity check
rows, err := db.Query("PRAGMA integrity_check")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var msg string
    rows.Scan(&msg)
    if msg != "ok" {
        log.Printf("Corruption detected: %s", msg)
    }
}
```

**Recovery:**

```go
// 1. Try to dump and restore
// Export data
rows, _ := db.Query("SELECT * FROM users")
// Save to CSV or JSON

// 2. Create new database
newDB, _ := sql.Open("sqlite_internal", "recovered.db")

// 3. Recreate schema
schema, _ := db.Query("SELECT sql FROM sqlite_master WHERE type='table'")
// Apply to new database

// 4. Import data
// Load from CSV or JSON into new database

// 5. If corruption is severe, restore from backup
// This is why regular backups are critical!
```

### Memory Issues

**Symptoms:**
- Out of memory errors
- Growing memory usage
- Slow performance

**Diagnosis:**

```go
// Check database size
var pageCount, pageSize int
db.QueryRow("PRAGMA page_count").Scan(&pageCount)
db.QueryRow("PRAGMA page_size").Scan(&pageSize)
sizeBytes := pageCount * pageSize
log.Printf("Database size: %d MB", sizeBytes/1024/1024)

// Check cache usage
var cacheSize int
db.QueryRow("PRAGMA cache_size").Scan(&cacheSize)
cacheMB := (cacheSize * pageSize) / 1024 / 1024
log.Printf("Cache size: %d MB", cacheMB)
```

**Solutions:**

```go
// 1. Reduce cache size
db.Exec("PRAGMA cache_size = 2000") // Smaller cache

// 2. Process large result sets in batches
stmt, _ := db.Prepare("SELECT * FROM large_table LIMIT ? OFFSET ?")
defer stmt.Close()

batchSize := 1000
for offset := 0; ; offset += batchSize {
    rows, _ := stmt.Query(batchSize, offset)

    count := 0
    for rows.Next() {
        // Process row
        count++
    }
    rows.Close()

    if count < batchSize {
        break
    }
}

// 3. Use streaming for large blobs
var blob []byte
err := db.QueryRow("SELECT data FROM files WHERE id = ?", id).Scan(&blob)
// Consider storing large files outside database

// 4. Regular VACUUM to reclaim space
db.Exec("VACUUM")
```

## Best Practices

### 1. Always Check Errors

```go
// Bad
db.Exec("INSERT INTO users (name) VALUES (?)", name)

// Good
_, err := db.Exec("INSERT INTO users (name) VALUES (?)", name)
if err != nil {
    return fmt.Errorf("failed to insert user: %w", err)
}
```

### 2. Use Prepared Statements

```go
// Prevents SQL injection and improves performance
stmt, err := db.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
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
```

### 3. Handle Resources Properly

```go
// Always use defer for cleanup
rows, err := db.Query("SELECT * FROM users")
if err != nil {
    return err
}
defer rows.Close() // Critical!

// Check for errors after iteration
for rows.Next() {
    // ...
}
if err := rows.Err(); err != nil {
    return err
}
```

### 4. Use Transactions for Multiple Operations

```go
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback() // Safe even after commit

// Multiple operations
_, err = tx.Exec("INSERT INTO users ...")
_, err = tx.Exec("INSERT INTO audit_log ...")

// Commit only if all succeed
if err := tx.Commit(); err != nil {
    return err
}
```

### 5. Set Reasonable Timeouts

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
if err != nil {
    if errors.Is(err, context.DeadlineExceeded) {
        log.Println("Query timeout")
    }
    return err
}
```

### 6. Enable Foreign Key Checks

```go
// Enable on each connection
db.Exec("PRAGMA foreign_keys = ON")

// Verify
var fk int
db.QueryRow("PRAGMA foreign_keys").Scan(&fk)
if fk != 1 {
    log.Fatal("Foreign keys not enabled")
}
```

### 7. Regular Integrity Checks

```go
// Schedule periodic checks
func scheduledIntegrityCheck(db *sql.DB) {
    ticker := time.NewTicker(24 * time.Hour)
    defer ticker.Stop()

    for range ticker.C {
        var result string
        err := db.QueryRow("PRAGMA integrity_check").Scan(&result)
        if err != nil || result != "ok" {
            log.Printf("ALERT: Database integrity issue: %v %s", err, result)
            // Send notification, trigger backup, etc.
        }
    }
}
```

### 8. Meaningful Error Messages

```go
// Include context in error messages
func getUserByEmail(db *sql.DB, email string) (*User, error) {
    var user User
    err := db.QueryRow("SELECT id, name, email FROM users WHERE email = ?",
                       email).Scan(&user.ID, &user.Name, &user.Email)
    if err != nil {
        if err == sql.ErrNoRows {
            return nil, fmt.Errorf("user not found: %s", email)
        }
        return nil, fmt.Errorf("failed to query user %s: %w", email, err)
    }
    return &user, nil
}
```

### 9. Log Security Events

```go
import "github.com/JuniperBible/Public.Lib.Anthony/internal/security"

// Monitor security violations
_, err := db.Exec("ATTACH DATABASE ? AS backup", userPath)
if err != nil {
    if errors.Is(err, security.ErrTraversal) {
        log.Printf("SECURITY ALERT: Path traversal attempt: %s", userPath)
        // Log to security audit system
    }
    return err
}
```

### 10. Test Error Paths

```go
func TestConstraintViolation(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
    db.Exec("INSERT INTO users (id, email) VALUES (1, 'test@example.com')")

    // Test duplicate email
    _, err := db.Exec("INSERT INTO users (id, email) VALUES (2, 'test@example.com')")
    if err == nil {
        t.Fatal("Expected constraint error")
    }
    if !strings.Contains(err.Error(), "UNIQUE constraint failed") {
        t.Errorf("Wrong error: %v", err)
    }
}
```

---

## Additional Resources

- [PRAGMAS.md](PRAGMAS.md) - Complete PRAGMA reference
- [SECURITY.md](SECURITY.md) - Security best practices
- [TESTING.md](TESTING.md) - Testing guide
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Lock ordering and concurrency
- [API.md](API.md) - API documentation

For more help:
- Check error messages carefully - they include context
- Use EXPLAIN to understand query execution
- Run integrity checks regularly
- Keep transactions short
- Test error handling paths
- Monitor security events
