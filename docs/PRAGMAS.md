# PRAGMA Commands - Go Implementation Guide

This document describes the PRAGMA commands supported by Anthony, a pure Go SQLite implementation. PRAGMAs are special commands used to query and modify SQLite's operational parameters.

## Table of Contents

- [Overview](#overview)
- [PRAGMA Syntax](#pragma-syntax)
- [Database Configuration](#database-configuration)
- [Query Information](#query-information)
- [Performance Tuning](#performance-tuning)
- [Schema Inspection](#schema-inspection)
- [Database Integrity](#database-integrity)
- [Go Implementation](#go-implementation)

## Overview

PRAGMA statements are SQLite extensions for modifying database behavior and querying internal state. Unlike standard SQL, PRAGMAs are database-specific and may change between versions.

**Package Location:** `internal/parser`, `internal/sql`, `internal/pager`

### Key Characteristics

- Not part of standard SQL
- Some PRAGMAs have side effects
- Some execute during prepare(), others during step()
- Unknown PRAGMAs are silently ignored (no error)
- Can be used as table-valued functions

## PRAGMA Syntax

### Basic Forms

```sql
-- Query a PRAGMA value
PRAGMA pragma_name;

-- Set a PRAGMA (equals syntax)
PRAGMA pragma_name = value;

-- Set a PRAGMA (function syntax)
PRAGMA pragma_name(value);

-- Schema-qualified PRAGMA
PRAGMA schema.pragma_name;
PRAGMA schema.pragma_name = value;
PRAGMA schema.pragma_name(value);
```

### As Table-Valued Functions

Some read-only PRAGMAs can be used as table-valued functions:

```sql
-- PRAGMA form
PRAGMA table_info('users');

-- Table-valued function form (allows SELECT operations)
SELECT * FROM pragma_table_info('users');
SELECT name, type FROM pragma_table_info('users') WHERE pk = 1;
```

### Go Implementation

```go
// Package: internal/parser
type PragmaStmt struct {
    Schema string      // Optional: "main", "temp", or attached DB name
    Name   string      // Required: pragma name
    Value  Expression  // Optional: value for setting pragmas
}

// Parse PRAGMA statement
parser := NewParser("PRAGMA cache_size = 10000")
stmt, err := parser.Parse()
pragmaStmt := stmt.(*PragmaStmt)
```

## Database Configuration

### application_id

Application-specific identifier stored in database header.

```sql
-- Query application ID
PRAGMA application_id;

-- Set application ID
PRAGMA application_id = 0x12345678;
```

**Use Case:**
```sql
-- Mark database as belonging to your application
PRAGMA application_id = 0x4D594150;  -- "MYAP" in hex
```

**Go Implementation:**
```go
// Set during database creation
_, err := db.Exec("PRAGMA application_id = ?", appID)

// Verify at runtime
var id int64
db.QueryRow("PRAGMA application_id").Scan(&id)
if id != expectedAppID {
    return errors.New("wrong database format")
}
```

### encoding

Database text encoding (UTF-8, UTF-16le, UTF-16be).

```sql
-- Query encoding (can only be set at creation)
PRAGMA encoding;

-- Set encoding (only works on empty database)
PRAGMA encoding = 'UTF-8';
PRAGMA encoding = 'UTF-16le';
PRAGMA encoding = 'UTF-16be';
```

**Anthony Default:** UTF-8

**Go Implementation:**
```go
// Always UTF-8 in Anthony
var encoding string
db.QueryRow("PRAGMA encoding").Scan(&encoding)
// encoding == "UTF-8"
```

### user_version

User-defined version number for schema versioning.

```sql
-- Query version
PRAGMA user_version;

-- Set version (for migrations)
PRAGMA user_version = 5;
```

**Use Case - Database Migrations:**
```go
func MigrateDatabase(db *sql.DB) error {
    var version int
    db.QueryRow("PRAGMA user_version").Scan(&version)

    migrations := []func(*sql.DB) error{
        migrate1,  // Version 0 -> 1
        migrate2,  // Version 1 -> 2
        migrate3,  // Version 2 -> 3
    }

    for version < len(migrations) {
        if err := migrations[version](db); err != nil {
            return err
        }
        version++
        _, err := db.Exec("PRAGMA user_version = ?", version)
        if err != nil {
            return err
        }
    }

    return nil
}
```

## Query Information

### database_list

List all attached databases.

```sql
PRAGMA database_list;

-- Returns:
-- seq | name | file
-- 0   | main | /path/to/main.db
-- 2   | temp |
-- 3   | aux  | /path/to/aux.db
```

**As Table-Valued Function:**
```sql
SELECT name, file FROM pragma_database_list();
SELECT COUNT(*) FROM pragma_database_list();
```

### table_list

List all tables in the database.

```sql
PRAGMA table_list;

-- Returns:
-- schema | name   | type  | ncol | wr | strict
-- main   | users  | table | 5    | 0  | 0
-- main   | posts  | table | 7    | 0  | 0
```

**As Table-Valued Function:**
```sql
SELECT name FROM pragma_table_list();
SELECT name FROM pragma_table_list() WHERE type = 'table';
```

### table_info

Get column information for a table.

```sql
PRAGMA table_info('users');

-- Returns:
-- cid | name     | type    | notnull | dflt_value | pk
-- 0   | id       | INTEGER | 0       | NULL       | 1
-- 1   | username | TEXT    | 1       | NULL       | 0
-- 2   | email    | TEXT    | 1       | NULL       | 0
-- 3   | created  | INTEGER | 0       | NULL       | 0
```

**As Table-Valued Function:**
```sql
-- Get primary key columns
SELECT name FROM pragma_table_info('users') WHERE pk > 0;

-- Get NOT NULL columns
SELECT name, type FROM pragma_table_info('users') WHERE notnull = 1;

-- Check if column exists
SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'email';
```

**Go Implementation:**
```go
type ColumnInfo struct {
    CID        int
    Name       string
    Type       string
    NotNull    bool
    DefaultVal sql.NullString
    PK         int
}

func GetTableColumns(db *sql.DB, table string) ([]ColumnInfo, error) {
    rows, err := db.Query("PRAGMA table_info(?)", table)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var columns []ColumnInfo
    for rows.Next() {
        var col ColumnInfo
        err := rows.Scan(&col.CID, &col.Name, &col.Type,
                         &col.NotNull, &col.DefaultVal, &col.PK)
        if err != nil {
            return nil, err
        }
        columns = append(columns, col)
    }

    return columns, nil
}
```

### table_xinfo

Extended table info including hidden columns.

```sql
PRAGMA table_xinfo('users');

-- Like table_info, but includes:
-- - Generated columns
-- - Hidden columns (for WITHOUT ROWID tables)
```

### index_list

List all indexes on a table.

```sql
PRAGMA index_list('users');

-- Returns:
-- seq | name              | unique | origin | partial
-- 0   | idx_users_email   | 1      | c      | 0
-- 1   | idx_users_name    | 0      | c      | 1
```

**As Table-Valued Function:**
```sql
SELECT name FROM pragma_index_list('users') WHERE unique = 1;
```

### index_info

Get column information for an index.

```sql
PRAGMA index_info('idx_users_email');

-- Returns:
-- seqno | cid | name
-- 0     | 2   | email
```

**As Table-Valued Function:**
```sql
SELECT name FROM pragma_index_info('idx_users_email');
```

### index_xinfo

Extended index info.

```sql
PRAGMA index_xinfo('idx_users_email');

-- Like index_info, but includes:
-- - Expression indexes
-- - DESC information
```

### foreign_key_list

List foreign keys for a table.

```sql
PRAGMA foreign_key_list('orders');

-- Returns:
-- id | seq | table    | from       | to  | on_update   | on_delete   | match
-- 0  | 0   | users    | user_id    | id  | NO ACTION   | CASCADE     | NONE
-- 1  | 0   | products | product_id | id  | NO ACTION   | RESTRICT    | NONE
```

**As Table-Valued Function:**
```sql
SELECT * FROM pragma_foreign_key_list('orders');
```

## Performance Tuning

### cache_size

Number of database pages to cache in memory.

```sql
-- Query cache size (in pages)
PRAGMA cache_size;

-- Set cache size
PRAGMA cache_size = 10000;      -- 10,000 pages
PRAGMA cache_size = -64000;     -- 64MB (negative = KB)
```

**Default:** -2000 (2MB)

**Calculation:**
```sql
-- For 4KB pages, 10000 pages = 40MB
PRAGMA page_size;       -- Get page size
PRAGMA cache_size;      -- Get cache size in pages
-- Memory = page_size * cache_size

-- Or use negative for KB:
PRAGMA cache_size = -65536;  -- 64MB regardless of page size
```

**Go Implementation:**
```go
// Set cache to 64MB
_, err := db.Exec("PRAGMA cache_size = -65536")

// Calculate in pages
pageSize := 4096
cachePages := 64 * 1024 * 1024 / pageSize  // 16384 pages
_, err = db.Exec("PRAGMA cache_size = ?", cachePages)
```

### page_size

Size of database pages (must be set before data is added).

```sql
-- Query page size
PRAGMA page_size;

-- Set page size (only works on empty database)
PRAGMA page_size = 4096;   -- 4KB (default)
PRAGMA page_size = 8192;   -- 8KB
PRAGMA page_size = 16384;  -- 16KB
```

**Valid Values:** 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536

**Go Implementation:**
```go
// Must be set before creating tables
db, _ := sql.Open("anthony", "newdb.db")
_, err := db.Exec("PRAGMA page_size = 8192")
if err != nil {
    return err
}
// Now create tables...
```

### mmap_size

Memory-mapped I/O size.

```sql
-- Query mmap size
PRAGMA mmap_size;

-- Set mmap size (in bytes)
PRAGMA mmap_size = 268435456;  -- 256MB
PRAGMA mmap_size = 0;          -- Disable mmap
```

**Benefits:**
- Faster reads (bypasses OS page cache)
- Reduced system calls
- Better performance for read-heavy workloads

**Go Implementation:**
```go
// Enable 256MB memory-mapped I/O
_, err := db.Exec("PRAGMA mmap_size = 268435456")
```

### temp_store

Where temporary tables and indexes are stored.

```sql
-- Query temp storage
PRAGMA temp_store;

-- Set temp storage
PRAGMA temp_store = 0;  -- DEFAULT (use compile-time setting)
PRAGMA temp_store = 1;  -- FILE (use temp files)
PRAGMA temp_store = 2;  -- MEMORY (use RAM)
```

**Go Implementation:**
```go
// Store temp tables in memory for better performance
_, err := db.Exec("PRAGMA temp_store = MEMORY")
```

### threads

Number of helper threads for parallel operations.

```sql
-- Query thread count
PRAGMA threads;

-- Set thread count
PRAGMA threads = 4;
```

## Database Integrity

### integrity_check

Comprehensive database integrity check.

```sql
-- Check entire database
PRAGMA integrity_check;

-- Limit number of errors reported
PRAGMA integrity_check(100);

-- Returns:
-- "ok" if no problems
-- List of errors if problems found
```

**Go Implementation:**
```go
func CheckDatabaseIntegrity(db *sql.DB) error {
    var result string
    err := db.QueryRow("PRAGMA integrity_check").Scan(&result)
    if err != nil {
        return err
    }

    if result != "ok" {
        return fmt.Errorf("integrity check failed: %s", result)
    }

    return nil
}
```

### quick_check

Faster integrity check (doesn't verify index contents).

```sql
-- Quick check
PRAGMA quick_check;

-- Limit errors
PRAGMA quick_check(100);
```

### foreign_key_check

Check foreign key constraints.

```sql
-- Check all tables
PRAGMA foreign_key_check;

-- Check specific table
PRAGMA foreign_key_check('orders');

-- Returns:
-- table | rowid | parent | fkid
-- (empty if no violations)
```

**As Table-Valued Function:**
```sql
SELECT * FROM pragma_foreign_key_check('orders');
```

## Transaction Control

### journal_mode

Transaction journal mode.

```sql
-- Query journal mode
PRAGMA journal_mode;

-- Set journal mode
PRAGMA journal_mode = DELETE;    -- Delete journal after commit (default)
PRAGMA journal_mode = TRUNCATE;  -- Truncate journal after commit
PRAGMA journal_mode = PERSIST;   -- Keep journal file
PRAGMA journal_mode = MEMORY;    -- Journal in memory
PRAGMA journal_mode = WAL;       -- Write-Ahead Log
PRAGMA journal_mode = OFF;       -- No journal (dangerous!)
```

**WAL Mode Benefits:**
```sql
-- Enable WAL mode for better concurrency
PRAGMA journal_mode = WAL;

-- Readers don't block writers
-- Writers don't block readers
-- Better performance for read-heavy workloads
```

**Go Implementation:**
```go
// Enable WAL mode at database open
db, _ := sql.Open("anthony", "mydb.db")
_, err := db.Exec("PRAGMA journal_mode = WAL")
if err != nil {
    return err
}
```

### synchronous

How aggressively SQLite syncs data to disk.

```sql
-- Query synchronous mode
PRAGMA synchronous;

-- Set synchronous mode
PRAGMA synchronous = 0;     -- OFF (fastest, dangerous)
PRAGMA synchronous = 1;     -- NORMAL (good balance)
PRAGMA synchronous = 2;     -- FULL (safest, slowest)
PRAGMA synchronous = 3;     -- EXTRA (paranoid)

-- Or use names:
PRAGMA synchronous = OFF;
PRAGMA synchronous = NORMAL;
PRAGMA synchronous = FULL;
```

**Recommendations:**
- **FULL**: Maximum durability, slower writes
- **NORMAL**: Good balance (recommended with WAL)
- **OFF**: Fast but risk of corruption on power loss

**Go Implementation:**
```go
// For WAL mode, NORMAL is safe and fast
_, err := db.Exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
`)
```

### wal_autocheckpoint

Automatically checkpoint WAL after N frames.

```sql
-- Query autocheckpoint threshold
PRAGMA wal_autocheckpoint;

-- Set threshold (in pages)
PRAGMA wal_autocheckpoint = 1000;  -- Checkpoint after 1000 pages
PRAGMA wal_autocheckpoint = 0;     -- Disable autocheckpoint
```

### wal_checkpoint

Manually checkpoint the WAL file.

```sql
-- Passive checkpoint (don't block readers/writers)
PRAGMA wal_checkpoint;

-- Full checkpoint (wait for readers)
PRAGMA wal_checkpoint(FULL);

-- Restart checkpoint (wait for readers, reset WAL)
PRAGMA wal_checkpoint(RESTART);

-- Truncate checkpoint (reset and truncate WAL)
PRAGMA wal_checkpoint(TRUNCATE);
```

**Go Implementation:**
```go
// Checkpoint periodically
func CheckpointWAL(db *sql.DB) error {
    _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
    return err
}

// Call during idle time or before backup
```

## Foreign Keys

### foreign_keys

Enable or disable foreign key constraint enforcement.

```sql
-- Query foreign key enforcement
PRAGMA foreign_keys;

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- Disable foreign keys
PRAGMA foreign_keys = OFF;
```

**Default:** OFF (for SQLite compatibility)

**Go Implementation:**
```go
// Enable foreign keys at connection open
db, _ := sql.Open("anthony", "mydb.db")
_, err := db.Exec("PRAGMA foreign_keys = ON")

// Or use connection hook
func init() {
    sql.Register("anthony-fk", &Driver{
        ConnectHook: func(conn *Conn) error {
            _, err := conn.Exec("PRAGMA foreign_keys = ON", nil)
            return err
        },
    })
}
```

### defer_foreign_keys

Defer foreign key constraint checking until commit.

```sql
-- Query defer status
PRAGMA defer_foreign_keys;

-- Enable deferred checking
PRAGMA defer_foreign_keys = ON;

-- Disable deferred checking (immediate)
PRAGMA defer_foreign_keys = OFF;
```

**Use Case:**
```sql
BEGIN;
PRAGMA defer_foreign_keys = ON;

-- Can temporarily violate FK constraints
INSERT INTO orders (user_id) VALUES (9999);  -- User doesn't exist yet
INSERT INTO users (id, name) VALUES (9999, 'New User');  -- Now it exists

COMMIT;  -- FK check happens here
```

## Optimization

### optimize

Run ANALYZE with optimizations.

```sql
-- Optimize database
PRAGMA optimize;

-- Run before closing database
-- Updates query planner statistics
```

**Go Implementation:**
```go
// Run optimize before closing
func CloseDatabase(db *sql.DB) error {
    _, err := db.Exec("PRAGMA optimize")
    if err != nil {
        log.Printf("Warning: optimize failed: %v", err)
    }
    return db.Close()
}
```

### analysis_limit

Limit ANALYZE operations to N rows per index.

```sql
-- Query limit
PRAGMA analysis_limit;

-- Set limit
PRAGMA analysis_limit = 1000;  -- Analyze 1000 rows per index
PRAGMA analysis_limit = 0;     -- Unlimited (default)
```

**Use Case:**
```sql
-- Fast ANALYZE on large database
PRAGMA analysis_limit = 1000;
ANALYZE;
```

## Vacuum

### auto_vacuum

Automatic database file shrinking.

```sql
-- Query auto-vacuum mode
PRAGMA auto_vacuum;

-- Set auto-vacuum (only works on empty database)
PRAGMA auto_vacuum = 0;  -- NONE (manual VACUUM needed)
PRAGMA auto_vacuum = 1;  -- FULL (automatic)
PRAGMA auto_vacuum = 2;  -- INCREMENTAL

-- Or use names:
PRAGMA auto_vacuum = NONE;
PRAGMA auto_vacuum = FULL;
PRAGMA auto_vacuum = INCREMENTAL;
```

### incremental_vacuum

Incrementally vacuum the database.

```sql
-- Vacuum up to N pages
PRAGMA incremental_vacuum(1000);

-- Vacuum all free pages
PRAGMA incremental_vacuum;
```

**Use with INCREMENTAL auto-vacuum:**
```go
// Setup (empty database)
_, err := db.Exec("PRAGMA auto_vacuum = INCREMENTAL")

// Later, free up space periodically
_, err = db.Exec("PRAGMA incremental_vacuum(1000)")
```

## Security

### trusted_schema

Control whether schema is trusted (allows SQL in CHECK, triggers, etc.).

```sql
-- Query trusted schema setting
PRAGMA trusted_schema;

-- Enable trusted schema (default)
PRAGMA trusted_schema = ON;

-- Disable trusted schema (paranoid mode)
PRAGMA trusted_schema = OFF;
```

## Go Implementation Examples

### Complete Database Setup

```go
func OpenDatabase(path string) (*sql.DB, error) {
    db, err := sql.Open("anthony", path)
    if err != nil {
        return nil, err
    }

    // Configure database
    pragmas := []string{
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -64000",      // 64MB cache
        "PRAGMA foreign_keys = ON",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 268435456",    // 256MB mmap
    }

    for _, pragma := range pragmas {
        if _, err := db.Exec(pragma); err != nil {
            db.Close()
            return nil, fmt.Errorf("pragma failed: %v", err)
        }
    }

    return db, nil
}
```

### Health Check

```go
func DatabaseHealthCheck(db *sql.DB) error {
    // Integrity check
    var result string
    err := db.QueryRow("PRAGMA integrity_check").Scan(&result)
    if err != nil {
        return err
    }
    if result != "ok" {
        return fmt.Errorf("integrity check failed: %s", result)
    }

    // Foreign key check
    rows, err := db.Query("PRAGMA foreign_key_check")
    if err != nil {
        return err
    }
    defer rows.Close()

    if rows.Next() {
        return errors.New("foreign key violations found")
    }

    return nil
}
```

### Schema Version Management

```go
type Migration struct {
    Version int
    Name    string
    SQL     string
}

func Migrate(db *sql.DB, migrations []Migration) error {
    var currentVersion int
    db.QueryRow("PRAGMA user_version").Scan(&currentVersion)

    for _, m := range migrations {
        if m.Version <= currentVersion {
            continue
        }

        log.Printf("Applying migration %d: %s", m.Version, m.Name)

        tx, err := db.Begin()
        if err != nil {
            return err
        }

        _, err = tx.Exec(m.SQL)
        if err != nil {
            tx.Rollback()
            return fmt.Errorf("migration %d failed: %v", m.Version, err)
        }

        _, err = tx.Exec("PRAGMA user_version = ?", m.Version)
        if err != nil {
            tx.Rollback()
            return err
        }

        if err := tx.Commit(); err != nil {
            return err
        }

        currentVersion = m.Version
    }

    return nil
}
```

## Quick Reference

### Read-Only PRAGMAs (Safe)

```sql
PRAGMA application_id;
PRAGMA cache_size;
PRAGMA database_list;
PRAGMA encoding;
PRAGMA foreign_keys;
PRAGMA journal_mode;
PRAGMA page_count;
PRAGMA page_size;
PRAGMA synchronous;
PRAGMA table_info('table_name');
PRAGMA table_list;
PRAGMA user_version;
```

### Write PRAGMAs (Use with Caution)

```sql
PRAGMA cache_size = value;
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA user_version = version;
```

### Dangerous PRAGMAs (Avoid)

```sql
PRAGMA journal_mode = OFF;     -- Risk of corruption
PRAGMA synchronous = OFF;      -- Risk of corruption
PRAGMA writable_schema = ON;   -- Can corrupt database
```

## References

- **Package:** `internal/parser` - PRAGMA parsing
- **Package:** `internal/sql` - PRAGMA execution
- **Package:** `internal/pager` - Page management
- **SQLite Docs:** [PRAGMA Statements](https://www.sqlite.org/pragma.html)
- **Local SQLite Ref:** [PRAGMA Reference (local)](sqlite/PRAGMA_REFERENCE.md) -- complete official PRAGMA syntax

---

## See Also

- [PRAGMA_QUICK_REFERENCE.md](PRAGMA_QUICK_REFERENCE.md) - Quick syntax reference and common usage patterns
- [PRAGMA_IMPLEMENTATION_SUMMARY.md](PRAGMA_IMPLEMENTATION_SUMMARY.md) - Implementation details and architecture
- [API.md](API.md) - Complete API documentation including PRAGMA usage
