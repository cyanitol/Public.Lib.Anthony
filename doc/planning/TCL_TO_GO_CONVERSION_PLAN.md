# TCL to Go Test Conversion Plan

**Version:** 1.0
**Date:** 2026-02-28
**Status:** Planning Document

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [TCL Test Structure Analysis](#tcl-test-structure-analysis)
3. [Go Test Equivalents](#go-test-equivalents)
4. [Pattern Translation Examples](#pattern-translation-examples)
5. [Priority Test Files](#priority-test-files)
6. [Automation Tools](#automation-tools)
7. [Quality Assurance](#quality-assurance)
8. [Implementation Roadmap](#implementation-roadmap)
9. [References](#references)

---

## Executive Summary

This document outlines the comprehensive strategy for converting SQLite's TCL test suite to idiomatic Go tests for the Anthony SQLite driver implementation. The SQLite project contains over 150,000 lines of TCL test code covering every aspect of SQL functionality, making it the gold standard for database testing.

**Goals:**
- Convert critical SQLite TCL tests to idiomatic Go tests
- Ensure feature parity and compatibility with SQLite behavior
- Maintain test readability and maintainability
- Enable automated conversion where possible
- Build a comprehensive test suite covering 90%+ of SQLite functionality

**Approach:**
- Pattern-based conversion using established Go testing idioms
- Table-driven tests for parametric TCL tests
- Helper functions for common TCL test utilities
- Automated parsing and code generation for repetitive patterns
- Manual conversion for complex logic and edge cases

---

## TCL Test Structure Analysis

### Common TCL Test Patterns

SQLite's TCL test suite uses a consistent structure based on several key testing utilities. Understanding these patterns is essential for effective conversion.

#### 1. Basic Test Structure

**TCL Pattern:**
```tcl
# test-name
do_test test-1.1 {
  execsql {SELECT 1+1}
} {2}

do_test test-1.2 {
  execsql {
    CREATE TABLE t1(a, b, c);
    INSERT INTO t1 VALUES(1, 2, 3);
    SELECT * FROM t1;
  }
} {1 2 3}
```

**Characteristics:**
- Test name follows hierarchical numbering (test-1.1, test-1.2, etc.)
- Code block executes SQL or TCL commands
- Expected result block for assertion
- Simple space-separated output format

#### 2. Error Testing

**TCL Pattern:**
```tcl
do_catchsql_test test-2.1 {
  SELECT * FROM nonexistent_table;
} {1 {no such table: nonexistent_table}}

do_test test-2.2 {
  catchsql {INSERT INTO t1 VALUES(1, 2)}
} {1 {table t1 has 3 columns but 2 values were supplied}}
```

**Characteristics:**
- `catchsql` returns `{code message}` list
- Code 0 = success, 1 = error
- Error message must match expected string
- Tests SQL error handling and validation

#### 3. Execution-Only Tests

**TCL Pattern:**
```tcl
do_execsql_test test-3.1 {
  CREATE TABLE t2(x INTEGER PRIMARY KEY, y TEXT);
  INSERT INTO t2 VALUES(1, 'hello');
  INSERT INTO t2 VALUES(2, 'world');
  SELECT y FROM t2 ORDER BY x;
} {hello world}

do_execsql_test test-3.2 {
  SELECT count(*) FROM t2;
} {2}
```

**Characteristics:**
- Simplified wrapper around `execsql`
- Combines SQL execution and result verification
- Most common pattern in SQLite tests
- Results are flattened into space-separated values

#### 4. File I/O and Database Operations

**TCL Pattern:**
```tcl
do_test test-4.1 {
  forcedelete test.db
  sqlite3 db test.db
  execsql {
    CREATE TABLE t1(a, b);
    INSERT INTO t1 VALUES(1, 2);
  }
  db close
  file exists test.db
} {1}
```

**Characteristics:**
- Database lifecycle management
- File system operations
- Connection handling
- Cleanup and setup operations

#### 5. Multi-Database Tests

**TCL Pattern:**
```tcl
do_test test-5.1 {
  sqlite3 db1 test1.db
  sqlite3 db2 test2.db
  db1 eval {CREATE TABLE t1(x)}
  db2 eval {CREATE TABLE t2(y)}
  db1 close
  db2 close
} {}
```

**Characteristics:**
- Multiple database handles
- Named connections (db1, db2)
- Parallel operations
- Resource management

### Test Categories

SQLite's test suite is organized by functionality. Key categories include:

#### Core SQL Operations
- **select*.test** (select1.test through select9.test): SELECT statement variations
- **insert*.test**: INSERT statement testing
- **update*.test**: UPDATE operations
- **delete*.test**: DELETE operations
- **join*.test**: JOIN operations (INNER, LEFT, CROSS, etc.)

#### Expression and Function Tests
- **expr.test**: Expression evaluation and type coercion
- **func.test**: Built-in SQL functions
- **func2.test**: Additional function tests
- **func3.test**: Window functions and advanced features

#### Schema and DDL
- **index*.test**: Index creation, usage, and optimization
- **table.test**: Table creation and modification
- **view*.test**: View creation and querying
- **trigger*.test**: Trigger functionality
- **fkey*.test**: Foreign key constraints

#### Data Types and Encoding
- **types.test**: Type affinity and conversion
- **collate*.test**: Collation sequences
- **utf*.test**: UTF-8 and UTF-16 encoding

#### Transaction and Concurrency
- **trans.test**: Transaction boundaries
- **lock*.test**: Locking mechanisms
- **corrupt*.test**: Corruption detection and recovery

#### Advanced Features
- **cte.test**: Common Table Expressions (WITH clause)
- **window*.test**: Window functions
- **json*.test**: JSON functions
- **fts*.test**: Full-text search

### Test Utilities

SQLite's TCL test framework provides numerous utilities:

#### Primary Test Utilities

1. **do_test** - Generic test execution
   ```tcl
   do_test name { code } {expected}
   ```

2. **do_execsql_test** - SQL execution test
   ```tcl
   do_execsql_test name { sql } {expected}
   ```

3. **do_catchsql_test** - Error catching test
   ```tcl
   do_catchsql_test name { sql } {code message}
   ```

4. **do_eqp_test** - EXPLAIN QUERY PLAN test
   ```tcl
   do_eqp_test name { sql } {plan}
   ```

5. **do_faultsim_test** - Fault injection test
   ```tcl
   do_faultsim_test name -prep { setup } -body { test } -test { verify }
   ```

#### Helper Utilities

- **execsql** - Execute SQL, return results as list
- **catchsql** - Execute SQL, catch errors
- **db eval** - Execute SQL with row callback
- **db transaction** - Execute code in transaction
- **forcedelete** - Delete file if exists
- **integrity_check** - Run PRAGMA integrity_check

---

## Go Test Equivalents

### Table-Driven Tests for do_test

The most common pattern in SQLite tests is `do_test`. This maps naturally to Go's table-driven test pattern.

**TCL:**
```tcl
do_test select-1.1 {
  execsql {SELECT 1+1}
} {2}

do_test select-1.2 {
  execsql {SELECT 2*3}
} {6}

do_test select-1.3 {
  execsql {SELECT 'hello' || ' ' || 'world'}
} {hello world}
```

**Go Equivalent:**
```go
func TestSelect1_BasicExpressions(t *testing.T) {
    tests := []struct {
        name     string
        sql      string
        expected [][]interface{}
    }{
        {
            name:     "select-1.1: addition",
            sql:      "SELECT 1+1",
            expected: [][]interface{}{{int64(2)}},
        },
        {
            name:     "select-1.2: multiplication",
            sql:      "SELECT 2*3",
            expected: [][]interface{}{{int64(6)}},
        },
        {
            name:     "select-1.3: string concatenation",
            sql:      "SELECT 'hello' || ' ' || 'world'",
            expected: [][]interface{}{{"hello world"}},
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            defer db.Close()

            rows, err := db.Query(tt.sql)
            if err != nil {
                t.Fatalf("query failed: %v", err)
            }
            defer rows.Close()

            got := scanAllRows(t, rows)
            if !equalRows(got, tt.expected) {
                t.Errorf("got %v, want %v", got, tt.expected)
            }
        })
    }
}
```

### Subtests for Test Organization

Go's `t.Run()` allows hierarchical test organization matching TCL's numbering scheme.

**TCL:**
```tcl
# select-2.* tests
do_test select-2.1 {...}
do_test select-2.2 {...}
do_test select-2.3 {...}

# select-3.* tests
do_test select-3.1 {...}
do_test select-3.2 {...}
```

**Go Equivalent:**
```go
func TestSelect2_WhereClause(t *testing.T) {
    t.Run("select-2.1", func(t *testing.T) {
        // Test implementation
    })

    t.Run("select-2.2", func(t *testing.T) {
        // Test implementation
    })

    t.Run("select-2.3", func(t *testing.T) {
        // Test implementation
    })
}

func TestSelect3_OrderBy(t *testing.T) {
    t.Run("select-3.1", func(t *testing.T) {
        // Test implementation
    })

    t.Run("select-3.2", func(t *testing.T) {
        // Test implementation
    })
}
```

### Helper Functions for Common Patterns

Create reusable helper functions matching TCL utilities.

#### 1. setupTestDB Helper

```go
// setupTestDB creates a new test database with automatic cleanup
func setupTestDB(t *testing.T) *sql.DB {
    t.Helper()

    tmpDir := t.TempDir() // Automatically cleaned up
    dbPath := filepath.Join(tmpDir, "test.db")

    db, err := sql.Open("anthony", dbPath)
    if err != nil {
        t.Fatalf("failed to open database: %v", err)
    }

    t.Cleanup(func() {
        db.Close()
    })

    return db
}
```

#### 2. execSQL Helper (equivalent to execsql)

```go
// execSQL executes SQL and returns results as [][]interface{}
func execSQL(t *testing.T, db *sql.DB, query string, args ...interface{}) [][]interface{} {
    t.Helper()

    rows, err := db.Query(query, args...)
    if err != nil {
        t.Fatalf("query failed: %v", err)
    }
    defer rows.Close()

    return scanAllRows(t, rows)
}

// scanAllRows scans all rows into [][]interface{}
func scanAllRows(t *testing.T, rows *sql.Rows) [][]interface{} {
    t.Helper()

    cols, err := rows.Columns()
    if err != nil {
        t.Fatalf("failed to get columns: %v", err)
    }

    var results [][]interface{}
    for rows.Next() {
        values := make([]interface{}, len(cols))
        valuePtrs := make([]interface{}, len(cols))
        for i := range values {
            valuePtrs[i] = &values[i]
        }

        if err := rows.Scan(valuePtrs...); err != nil {
            t.Fatalf("scan failed: %v", err)
        }

        results = append(results, values)
    }

    if err := rows.Err(); err != nil {
        t.Fatalf("rows error: %v", err)
    }

    return results
}
```

#### 3. catchSQL Helper (equivalent to catchsql)

```go
// catchSQL executes SQL and captures error information
func catchSQL(t *testing.T, db *sql.DB, query string, args ...interface{}) (int, string, [][]interface{}) {
    t.Helper()

    rows, err := db.Query(query, args...)
    if err != nil {
        // Return error code and message
        return 1, err.Error(), nil
    }
    defer rows.Close()

    results := scanAllRows(t, rows)
    return 0, "", results
}

// expectError asserts that SQL execution produces an error
func expectError(t *testing.T, db *sql.DB, query string, expectedMsg string) {
    t.Helper()

    _, err := db.Exec(query)
    if err == nil {
        t.Fatalf("expected error but got success")
    }

    if !strings.Contains(err.Error(), expectedMsg) {
        t.Errorf("expected error containing %q, got %q", expectedMsg, err.Error())
    }
}
```

#### 4. mustExec Helper (equivalent to db eval)

```go
// mustExec executes SQL that must succeed
func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) sql.Result {
    t.Helper()

    result, err := db.Exec(query, args...)
    if err != nil {
        t.Fatalf("exec failed: %v\nSQL: %s", err, query)
    }

    return result
}
```

#### 5. Row Comparison Helper

```go
// equalRows compares two result sets
func equalRows(got, want [][]interface{}) bool {
    if len(got) != len(want) {
        return false
    }

    for i := range got {
        if len(got[i]) != len(want[i]) {
            return false
        }

        for j := range got[i] {
            if !equalValues(got[i][j], want[i][j]) {
                return false
            }
        }
    }

    return true
}

// equalValues compares two SQL values (handling type conversions)
func equalValues(got, want interface{}) bool {
    // Handle nil
    if got == nil && want == nil {
        return true
    }
    if got == nil || want == nil {
        return false
    }

    // Handle []byte vs string
    if gb, ok := got.([]byte); ok {
        if wb, ok := want.([]byte); ok {
            return bytes.Equal(gb, wb)
        }
        if ws, ok := want.(string); ok {
            return string(gb) == ws
        }
    }

    // Handle int64 vs int
    if gi, ok := got.(int64); ok {
        if wi, ok := want.(int); ok {
            return gi == int64(wi)
        }
    }

    // Direct comparison
    return reflect.DeepEqual(got, want)
}
```

---

## Pattern Translation Examples

### Example 1: do_test -> t.Run with assertions

**TCL (select1.test):**
```tcl
do_test select-1.1 {
  execsql {
    CREATE TABLE t1(a INTEGER, b TEXT, c REAL);
    INSERT INTO t1 VALUES(1, 'hello', 3.14);
    SELECT * FROM t1;
  }
} {1 hello 3.14}
```

**Go:**
```go
func TestSelect1_BasicSelect(t *testing.T) {
    t.Run("select-1.1: create and select from table", func(t *testing.T) {
        db := setupTestDB(t)

        // Setup
        mustExec(t, db, "CREATE TABLE t1(a INTEGER, b TEXT, c REAL)")
        mustExec(t, db, "INSERT INTO t1 VALUES(1, 'hello', 3.14)")

        // Execute
        rows := execSQL(t, db, "SELECT * FROM t1")

        // Verify
        expected := [][]interface{}{
            {int64(1), "hello", 3.14},
        }

        if !equalRows(rows, expected) {
            t.Errorf("got %v, want %v", rows, expected)
        }
    })
}
```

### Example 2: do_execsql_test -> Query execution helpers

**TCL (insert.test):**
```tcl
do_execsql_test insert-1.1 {
  CREATE TABLE t1(x, y);
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
  SELECT x, y FROM t1 ORDER BY x;
} {1 2 3 4}

do_execsql_test insert-1.2 {
  INSERT INTO t1 SELECT x+10, y+10 FROM t1;
  SELECT count(*) FROM t1;
} {4}
```

**Go:**
```go
func TestInsert1_BasicInsert(t *testing.T) {
    tests := []struct {
        name     string
        setup    []string
        query    string
        expected [][]interface{}
    }{
        {
            name: "insert-1.1: basic insert and select",
            setup: []string{
                "CREATE TABLE t1(x, y)",
                "INSERT INTO t1 VALUES(1, 2)",
                "INSERT INTO t1 VALUES(3, 4)",
            },
            query: "SELECT x, y FROM t1 ORDER BY x",
            expected: [][]interface{}{
                {int64(1), int64(2)},
                {int64(3), int64(4)},
            },
        },
        {
            name: "insert-1.2: insert from select",
            setup: []string{
                "CREATE TABLE t1(x, y)",
                "INSERT INTO t1 VALUES(1, 2)",
                "INSERT INTO t1 VALUES(3, 4)",
                "INSERT INTO t1 SELECT x+10, y+10 FROM t1",
            },
            query: "SELECT count(*) FROM t1",
            expected: [][]interface{}{
                {int64(4)},
            },
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)

            // Execute setup
            for _, sql := range tt.setup {
                mustExec(t, db, sql)
            }

            // Execute query
            got := execSQL(t, db, tt.query)

            // Verify
            if !equalRows(got, tt.expected) {
                t.Errorf("got %v, want %v", got, tt.expected)
            }
        })
    }
}
```

### Example 3: catchsql -> Error checking patterns

**TCL (error.test):**
```tcl
do_catchsql_test error-1.1 {
  SELECT * FROM nonexistent;
} {1 {no such table: nonexistent}}

do_catchsql_test error-1.2 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES(1, 2);
} {1 {table t1 has 1 columns but 2 values were supplied}}
```

**Go:**
```go
func TestError1_BasicErrors(t *testing.T) {
    tests := []struct {
        name        string
        setup       []string
        sql         string
        expectError bool
        errorMsg    string
    }{
        {
            name:        "error-1.1: no such table",
            sql:         "SELECT * FROM nonexistent",
            expectError: true,
            errorMsg:    "no such table: nonexistent",
        },
        {
            name: "error-1.2: column count mismatch",
            setup: []string{
                "CREATE TABLE t1(x)",
            },
            sql:         "INSERT INTO t1 VALUES(1, 2)",
            expectError: true,
            errorMsg:    "table t1 has 1 columns but 2 values were supplied",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)

            // Execute setup
            for _, sql := range tt.setup {
                mustExec(t, db, sql)
            }

            // Execute test SQL
            _, err := db.Exec(tt.sql)

            // Verify error expectation
            if tt.expectError {
                if err == nil {
                    t.Fatalf("expected error but got success")
                }
                if !strings.Contains(err.Error(), tt.errorMsg) {
                    t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
                }
            } else {
                if err != nil {
                    t.Fatalf("unexpected error: %v", err)
                }
            }
        })
    }
}
```

### Example 4: Multi-step test with state

**TCL (update.test):**
```tcl
do_test update-1.1 {
  execsql {
    CREATE TABLE t1(a, b, c);
    INSERT INTO t1 VALUES(1, 2, 3);
    INSERT INTO t1 VALUES(4, 5, 6);
  }
  execsql {SELECT count(*) FROM t1}
} {2}

do_test update-1.2 {
  execsql {UPDATE t1 SET b=99 WHERE a=1}
  execsql {SELECT * FROM t1 ORDER BY a}
} {1 99 3 4 5 6}
```

**Go:**
```go
func TestUpdate1_BasicUpdate(t *testing.T) {
    db := setupTestDB(t)

    // Setup
    mustExec(t, db, "CREATE TABLE t1(a, b, c)")
    mustExec(t, db, "INSERT INTO t1 VALUES(1, 2, 3)")
    mustExec(t, db, "INSERT INTO t1 VALUES(4, 5, 6)")

    t.Run("update-1.1: verify initial data", func(t *testing.T) {
        got := execSQL(t, db, "SELECT count(*) FROM t1")
        expected := [][]interface{}{{int64(2)}}

        if !equalRows(got, expected) {
            t.Errorf("got %v, want %v", got, expected)
        }
    })

    t.Run("update-1.2: update and verify", func(t *testing.T) {
        mustExec(t, db, "UPDATE t1 SET b=99 WHERE a=1")

        got := execSQL(t, db, "SELECT * FROM t1 ORDER BY a")
        expected := [][]interface{}{
            {int64(1), int64(99), int64(3)},
            {int64(4), int64(5), int64(6)},
        }

        if !equalRows(got, expected) {
            t.Errorf("got %v, want %v", got, expected)
        }
    })
}
```

### Example 5: Testing with multiple databases

**TCL (attach.test):**
```tcl
do_test attach-1.1 {
  forcedelete test1.db test2.db
  sqlite3 db1 test1.db
  sqlite3 db2 test2.db
  db1 eval {CREATE TABLE t1(x)}
  db2 eval {CREATE TABLE t2(y)}
  db1 close
  db2 close
} {}
```

**Go:**
```go
func TestAttach1_MultipleConnections(t *testing.T) {
    t.Run("attach-1.1: independent databases", func(t *testing.T) {
        tmpDir := t.TempDir()

        // Create first database
        db1Path := filepath.Join(tmpDir, "test1.db")
        db1, err := sql.Open("anthony", db1Path)
        if err != nil {
            t.Fatalf("failed to open db1: %v", err)
        }
        defer db1.Close()

        // Create second database
        db2Path := filepath.Join(tmpDir, "test2.db")
        db2, err := sql.Open("anthony", db2Path)
        if err != nil {
            t.Fatalf("failed to open db2: %v", err)
        }
        defer db2.Close()

        // Create tables independently
        mustExec(t, db1, "CREATE TABLE t1(x)")
        mustExec(t, db2, "CREATE TABLE t2(y)")

        // Verify tables exist in respective databases
        rows1 := execSQL(t, db1, "SELECT name FROM sqlite_master WHERE type='table'")
        if len(rows1) != 1 || rows1[0][0] != "t1" {
            t.Errorf("db1 should have t1 table, got %v", rows1)
        }

        rows2 := execSQL(t, db2, "SELECT name FROM sqlite_master WHERE type='table'")
        if len(rows2) != 1 || rows2[0][0] != "t2" {
            t.Errorf("db2 should have t2 table, got %v", rows2)
        }
    })
}
```

---

## Priority Test Files

### Phase 1: Core SELECT Operations (Weeks 1-2)

**Priority: CRITICAL**

These tests validate the most fundamental database operation - querying data.

1. **select1.test** - Basic SELECT statements
   - Single column selection
   - Multi-column selection
   - Literal values
   - Basic expressions
   - Estimated effort: 3 days
   - Test count: ~50 tests

2. **select2.test** - WHERE clause
   - Comparison operators
   - Logical operators (AND, OR, NOT)
   - NULL handling
   - Pattern matching (LIKE)
   - Estimated effort: 3 days
   - Test count: ~80 tests

3. **select3.test** - ORDER BY and DISTINCT
   - Ascending/descending order
   - Multi-column ordering
   - NULL ordering
   - DISTINCT keyword
   - Estimated effort: 2 days
   - Test count: ~40 tests

4. **select4.test** - Aggregate functions
   - COUNT, SUM, AVG, MIN, MAX
   - GROUP BY clause
   - HAVING clause
   - NULL handling in aggregates
   - Estimated effort: 3 days
   - Test count: ~60 tests

### Phase 2: Data Modification (Weeks 3-4)

**Priority: HIGH**

5. **insert.test** - INSERT operations
   - INSERT VALUES
   - INSERT SELECT
   - Multi-row insert
   - Default values
   - Estimated effort: 2 days
   - Test count: ~50 tests

6. **insert2.test** - Advanced INSERT
   - INSERT OR REPLACE
   - INSERT OR IGNORE
   - AUTOINCREMENT
   - Constraint violations
   - Estimated effort: 2 days
   - Test count: ~40 tests

7. **update.test** - UPDATE operations
   - Basic UPDATE
   - UPDATE with WHERE
   - Multi-column UPDATE
   - Correlated subqueries
   - Estimated effort: 2 days
   - Test count: ~45 tests

8. **delete.test** - DELETE operations
   - DELETE with WHERE
   - DELETE all rows
   - Foreign key cascades
   - Trigger interactions
   - Estimated effort: 2 days
   - Test count: ~35 tests

### Phase 3: Expressions and Functions (Week 5)

**Priority: HIGH**

9. **expr.test** - Expression evaluation
   - Arithmetic operators
   - String operators
   - Comparison operators
   - Type coercion rules
   - Estimated effort: 4 days
   - Test count: ~100 tests

10. **func.test** - Built-in functions
    - String functions (length, substr, etc.)
    - Numeric functions (abs, round, etc.)
    - Date/time functions
    - Type conversion functions
    - Estimated effort: 4 days
    - Test count: ~120 tests

### Phase 4: Indexes and Optimization (Week 6)

**Priority: MEDIUM**

11. **index.test** - Index basics
    - CREATE INDEX
    - DROP INDEX
    - Unique indexes
    - Multi-column indexes
    - Estimated effort: 2 days
    - Test count: ~40 tests

12. **index2.test** - Index usage
    - Query optimization with indexes
    - Index selection
    - Covering indexes
    - Partial indexes
    - Estimated effort: 3 days
    - Test count: ~50 tests

### Phase 5: Advanced Features (Weeks 7-8)

**Priority: MEDIUM**

13. **join.test** - JOIN operations
    - INNER JOIN
    - LEFT JOIN
    - CROSS JOIN
    - Multiple joins
    - Estimated effort: 3 days
    - Test count: ~60 tests

14. **cte.test** - Common Table Expressions
    - Non-recursive CTEs
    - Recursive CTEs
    - Multiple CTEs
    - CTE optimization
    - Estimated effort: 3 days
    - Test count: ~50 tests

15. **trigger.test** - Triggers
    - BEFORE/AFTER triggers
    - INSERT/UPDATE/DELETE triggers
    - Trigger conditions
    - Recursive triggers
    - Estimated effort: 4 days
    - Test count: ~70 tests

### Phase 6: Data Types and Constraints (Week 9)

**Priority: MEDIUM**

16. **types.test** - Type affinity
    - INTEGER affinity
    - TEXT affinity
    - REAL affinity
    - NUMERIC affinity
    - BLOB affinity
    - Estimated effort: 2 days
    - Test count: ~50 tests

17. **collate.test** - Collation
    - BINARY collation
    - NOCASE collation
    - RTRIM collation
    - Custom collations
    - Estimated effort: 2 days
    - Test count: ~30 tests

### Phase 7: Transactions and Concurrency (Week 10)

**Priority: LOW**

18. **trans.test** - Transactions
    - BEGIN/COMMIT/ROLLBACK
    - Savepoints
    - Nested transactions
    - Transaction isolation
    - Estimated effort: 3 days
    - Test count: ~40 tests

19. **lock.test** - Locking
    - Shared locks
    - Exclusive locks
    - Lock escalation
    - Deadlock detection
    - Estimated effort: 3 days
    - Test count: ~35 tests

### Summary of Priority Files

| File | Priority | Days | Tests | Phase |
|------|----------|------|-------|-------|
| select1.test | CRITICAL | 3 | 50 | 1 |
| select2.test | CRITICAL | 3 | 80 | 1 |
| select3.test | CRITICAL | 2 | 40 | 1 |
| select4.test | CRITICAL | 3 | 60 | 1 |
| insert.test | HIGH | 2 | 50 | 2 |
| insert2.test | HIGH | 2 | 40 | 2 |
| update.test | HIGH | 2 | 45 | 2 |
| delete.test | HIGH | 2 | 35 | 2 |
| expr.test | HIGH | 4 | 100 | 3 |
| func.test | HIGH | 4 | 120 | 3 |
| index.test | MEDIUM | 2 | 40 | 4 |
| index2.test | MEDIUM | 3 | 50 | 4 |
| join.test | MEDIUM | 3 | 60 | 5 |
| cte.test | MEDIUM | 3 | 50 | 5 |
| trigger.test | MEDIUM | 4 | 70 | 5 |
| types.test | MEDIUM | 2 | 50 | 6 |
| collate.test | MEDIUM | 2 | 30 | 6 |
| trans.test | LOW | 3 | 40 | 7 |
| lock.test | LOW | 3 | 35 | 7 |
| **TOTAL** | | **50** | **1045** | **10 weeks** |

---

## Automation Tools

### TCL Parser for Test Extraction

Create a Go-based TCL parser to automatically extract test cases from SQLite's TCL test files.

#### Parser Architecture

```go
// Package tclparse provides TCL test file parsing for SQLite tests
package tclparse

// TestCase represents a single test case
type TestCase struct {
    Name        string   // e.g., "select-1.1"
    Description string   // Optional comment
    Type        string   // "do_test", "do_execsql_test", "do_catchsql_test"
    SQL         []string // SQL statements to execute
    Expected    string   // Expected result
    ExpectError bool     // Whether error is expected
    ErrorCode   int      // Expected error code (0 or 1)
}

// TestFile represents a parsed TCL test file
type TestFile struct {
    Filename string
    Tests    []TestCase
}

// Parse parses a TCL test file
func Parse(filename string) (*TestFile, error) {
    // Implementation
}
```

#### Parser Implementation

```go
package tclparse

import (
    "bufio"
    "fmt"
    "os"
    "regexp"
    "strings"
)

var (
    // Regular expressions for matching test patterns
    doTestRegex       = regexp.MustCompile(`do_test\s+([\w\-\.]+)\s+{`)
    doExecSQLRegex    = regexp.MustCompile(`do_execsql_test\s+([\w\-\.]+)\s+{`)
    doCatchSQLRegex   = regexp.MustCompile(`do_catchsql_test\s+([\w\-\.]+)\s+{`)
    commentRegex      = regexp.MustCompile(`^\s*#\s*(.+)`)
)

// Parse parses a TCL test file and extracts test cases
func Parse(filename string) (*TestFile, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    tf := &TestFile{
        Filename: filename,
        Tests:    make([]TestCase, 0),
    }

    scanner := bufio.NewScanner(file)
    var currentTest *TestCase
    var inBlock bool
    var braceCount int
    var blockContent strings.Builder

    for scanner.Scan() {
        line := scanner.Text()

        // Skip empty lines
        if strings.TrimSpace(line) == "" {
            continue
        }

        // Check for test start
        if matches := doExecSQLRegex.FindStringSubmatch(line); matches != nil {
            currentTest = &TestCase{
                Name: matches[1],
                Type: "do_execsql_test",
            }
            inBlock = true
            braceCount = 1
            blockContent.Reset()
            continue
        }

        if matches := doCatchSQLRegex.FindStringSubmatch(line); matches != nil {
            currentTest = &TestCase{
                Name:        matches[1],
                Type:        "do_catchsql_test",
                ExpectError: true,
            }
            inBlock = true
            braceCount = 1
            blockContent.Reset()
            continue
        }

        if matches := doTestRegex.FindStringSubmatch(line); matches != nil {
            currentTest = &TestCase{
                Name: matches[1],
                Type: "do_test",
            }
            inBlock = true
            braceCount = 1
            blockContent.Reset()
            continue
        }

        // Accumulate block content
        if inBlock {
            blockContent.WriteString(line)
            blockContent.WriteString("\n")

            // Count braces to find block end
            braceCount += strings.Count(line, "{")
            braceCount -= strings.Count(line, "}")

            if braceCount == 0 {
                // Block complete, parse it
                if currentTest != nil {
                    parseTestBlock(currentTest, blockContent.String())
                    tf.Tests = append(tf.Tests, *currentTest)
                }
                inBlock = false
                currentTest = nil
            }
        }
    }

    return tf, scanner.Err()
}

// parseTestBlock parses the SQL and expected result from a test block
func parseTestBlock(test *TestCase, content string) {
    // Split into SQL block and expected result
    // This is a simplified version - real implementation needs robust parsing
    parts := splitTestBlock(content)

    if len(parts) >= 1 {
        test.SQL = extractSQL(parts[0])
    }

    if len(parts) >= 2 {
        test.Expected = strings.TrimSpace(parts[1])
    }
}

// splitTestBlock splits test content into SQL and expected parts
func splitTestBlock(content string) []string {
    // Simplified implementation
    // Real version needs proper brace matching
    return strings.Split(content, "} {")
}

// extractSQL extracts individual SQL statements
func extractSQL(sqlBlock string) []string {
    // Remove outer braces and split by semicolon
    sqlBlock = strings.TrimSpace(sqlBlock)
    sqlBlock = strings.Trim(sqlBlock, "{}")

    statements := strings.Split(sqlBlock, ";")
    result := make([]string, 0)

    for _, stmt := range statements {
        stmt = strings.TrimSpace(stmt)
        if stmt != "" {
            result = append(result, stmt)
        }
    }

    return result
}
```

### Code Generation Templates

Generate Go test code from parsed TCL tests.

#### Template Structure

```go
package codegen

import (
    "bytes"
    "fmt"
    "go/format"
    "strings"
    "text/template"
)

// GenerateGoTest generates Go test code from TCL test cases
func GenerateGoTest(tf *tclparse.TestFile) (string, error) {
    tmpl := template.Must(template.New("test").Funcs(funcMap).Parse(testTemplate))

    var buf bytes.Buffer
    if err := tmpl.Execute(&buf, tf); err != nil {
        return "", err
    }

    // Format the generated code
    formatted, err := format.Source(buf.Bytes())
    if err != nil {
        // Return unformatted if formatting fails
        return buf.String(), nil
    }

    return string(formatted), nil
}

var funcMap = template.FuncMap{
    "funcName":    generateFuncName,
    "testName":    generateTestName,
    "formatSQL":   formatSQL,
    "formatValue": formatValue,
}

func generateFuncName(filename string) string {
    // Convert "select1.test" to "TestSelect1"
    name := strings.TrimSuffix(filename, ".test")
    return "Test" + strings.Title(name)
}

func generateTestName(tc tclparse.TestCase) string {
    return tc.Name
}

func formatSQL(sql []string) string {
    if len(sql) == 0 {
        return ""
    }
    return strings.Join(sql, ";\n")
}

func formatValue(v string) string {
    // Convert TCL result format to Go format
    // e.g., "1 2 3" -> [][]interface{}{{1, 2, 3}}
    return v // Simplified
}

const testTemplate = `package driver

import (
    "database/sql"
    "testing"
)

{{$filename := .Filename}}
func {{funcName $filename}}(t *testing.T) {
    tests := []struct {
        name     string
        sql      string
        expected [][]interface{}
        wantErr  bool
    }{
{{range .Tests}}
        {
            name: "{{.Name}}",
            sql: ` + "`{{formatSQL .SQL}}`" + `,
            expected: parseResult("{{.Expected}}"),
            wantErr: {{.ExpectError}},
        },
{{end}}
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            defer db.Close()

            rows, err := db.Query(tt.sql)
            if (err != nil) != tt.wantErr {
                t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
            }

            if err != nil {
                return
            }
            defer rows.Close()

            got := scanAllRows(t, rows)
            if !equalRows(got, tt.expected) {
                t.Errorf("got %v, want %v", got, tt.expected)
            }
        })
    }
}
`
```

### Command-Line Tool

Create a CLI tool for test conversion.

```go
// cmd/tcl2go/main.go
package main

import (
    "flag"
    "fmt"
    "os"
    "path/filepath"

    "github.com/cyanitol/Public.Lib.Anthony/internal/tclparse"
    "github.com/cyanitol/Public.Lib.Anthony/internal/codegen"
)

func main() {
    var (
        inputFile  = flag.String("input", "", "TCL test file to convert")
        outputFile = flag.String("output", "", "Output Go test file")
        outputDir  = flag.String("dir", "", "Output directory for generated tests")
    )
    flag.Parse()

    if *inputFile == "" {
        fmt.Fprintln(os.Stderr, "Error: -input required")
        flag.Usage()
        os.Exit(1)
    }

    // Parse TCL test file
    tf, err := tclparse.Parse(*inputFile)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Parse error: %v\n", err)
        os.Exit(1)
    }

    // Generate Go test code
    code, err := codegen.GenerateGoTest(tf)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Code generation error: %v\n", err)
        os.Exit(1)
    }

    // Determine output file
    output := *outputFile
    if output == "" {
        base := filepath.Base(*inputFile)
        base = strings.TrimSuffix(base, ".test")
        output = base + "_test.go"

        if *outputDir != "" {
            output = filepath.Join(*outputDir, output)
        }
    }

    // Write output
    if err := os.WriteFile(output, []byte(code), 0644); err != nil {
        fmt.Fprintf(os.Stderr, "Write error: %v\n", err)
        os.Exit(1)
    }

    fmt.Printf("Generated %s (%d tests)\n", output, len(tf.Tests))
}
```

### Usage Examples

```bash
# Convert single file
./tcl2go -input select1.test -output internal/driver/select1_test.go

# Convert multiple files
for f in select*.test; do
    ./tcl2go -input $f -dir internal/driver
done

# Dry run (print to stdout)
./tcl2go -input insert.test -output /dev/stdout

# Batch conversion with verification
./tcl2go -input select1.test -output test.go && go test -run TestSelect1
```

---

## Quality Assurance

### Ensuring Test Fidelity

Converting tests is not enough - we must ensure converted tests accurately verify the same behavior as the original TCL tests.

#### 1. Parallel Execution Verification

**Strategy:** Run both TCL and Go tests against SQLite and Anthony respectively, comparing results.

```bash
# Run SQLite TCL test
sqlite3 :memory: < select1.test > tcl_results.txt

# Run Go test with detailed output
go test -v -run TestSelect1 > go_results.txt

# Compare results
diff <(normalize_output tcl_results.txt) <(normalize_output go_results.txt)
```

#### 2. Result Normalization

Different output formats need normalization for comparison.

```go
// normalizeResult converts various result formats to canonical form
func normalizeResult(result string) [][]interface{} {
    // Handle TCL space-separated format: "1 2 3"
    // Convert to [][]interface{}{{1, 2, 3}}

    // Handle newline-separated rows
    // Handle NULL representation
    // Handle type conversions
}
```

#### 3. Test Coverage Metrics

Track which SQLite features are covered by converted tests.

```go
type CoverageTracker struct {
    TotalTCLTests     int
    ConvertedTests    int
    ManualTests       int
    SkippedTests      int

    FeatureCoverage   map[string]int // e.g., "SELECT" -> 150
    OperatorCoverage  map[string]int // e.g., "+" -> 25
    FunctionCoverage  map[string]int // e.g., "length" -> 10
}

func (ct *CoverageTracker) Report() string {
    // Generate coverage report
}
```

#### 4. Regression Testing

Ensure converted tests catch the same bugs as original tests.

**Process:**
1. Introduce known bug in implementation
2. Verify TCL test fails
3. Verify converted Go test also fails
4. Fix bug
5. Verify both tests pass

**Example:**
```go
func TestRegressionVerification(t *testing.T) {
    // This test should fail if we remove type coercion
    // Original TCL test: do_test select-1.5 {execsql {SELECT '5' > 4}} {1}

    db := setupTestDB(t)
    rows := execSQL(t, db, "SELECT '5' > 4")
    expected := [][]interface{}{{int64(1)}}

    if !equalRows(rows, expected) {
        t.Errorf("Type coercion broken: got %v, want %v", rows, expected)
    }
}
```

#### 5. Edge Case Verification

Ensure edge cases are preserved in conversion.

**Critical Edge Cases:**
- NULL handling
- Empty strings vs NULL
- Integer overflow
- Float precision
- Unicode/UTF-8
- Case sensitivity
- Collation sequences

**Verification Test:**
```go
func TestEdgeCases(t *testing.T) {
    tests := []struct {
        name     string
        sql      string
        expected [][]interface{}
    }{
        {
            name:     "NULL vs empty string",
            sql:      "SELECT NULL = ''",
            expected: [][]interface{}{{nil}}, // NULL, not 0
        },
        {
            name:     "Integer overflow",
            sql:      "SELECT 9223372036854775807 + 1",
            expected: [][]interface{}{{int64(-9223372036854775808)}},
        },
        {
            name:     "Float precision",
            sql:      "SELECT 0.1 + 0.2",
            expected: [][]interface{}{{0.30000000000000004}},
        },
        {
            name:     "Unicode handling",
            sql:      "SELECT length('hello world')",
            expected: [][]interface{}{{int64(11)}},
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            got := execSQL(t, db, tt.sql)
            if !equalRows(got, tt.expected) {
                t.Errorf("got %v, want %v", got, tt.expected)
            }
        })
    }
}
```

#### 6. Automated Verification Pipeline

Create CI pipeline to verify test conversions.

```yaml
# .github/workflows/test-conversion.yml
name: TCL Test Conversion Verification

on: [push, pull_request]

jobs:
  verify-conversion:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v2

      - name: Install SQLite with TCL
        run: |
          sudo apt-get install -y sqlite3 tcl

      - name: Setup Go
        uses: actions/setup-go@v2
        with:
          go-version: 1.21

      - name: Run TCL tests
        run: |
          cd sqlite-tests
          make test > tcl-results.txt

      - name: Run Go tests
        run: |
          go test -v ./internal/driver/... > go-results.txt

      - name: Compare results
        run: |
          python3 scripts/compare-results.py tcl-results.txt go-results.txt
```

#### 7. Test Quality Checklist

Before marking a test conversion complete:

- [ ] All test cases from TCL file converted
- [ ] Test names match original numbering
- [ ] Expected results verified against SQLite
- [ ] Error cases produce correct error messages
- [ ] Edge cases explicitly tested
- [ ] Helper functions used appropriately
- [ ] Tests are idiomatic Go (table-driven where appropriate)
- [ ] Tests use t.Helper() for helper functions
- [ ] Tests use t.Cleanup() for resource cleanup
- [ ] Tests pass with `go test -race`
- [ ] Tests documented with comments explaining non-obvious behavior
- [ ] Coverage report shows >90% line coverage for tested code

---

## Implementation Roadmap

### Timeline Overview

**Total Duration:** 10 weeks (50 working days)
**Team Size:** 2-3 developers
**Test Target:** 1000+ test cases converted

### Week-by-Week Plan

#### Week 1: Foundation and Tooling

**Days 1-2: Infrastructure Setup**
- Create `internal/tclparse` package
- Implement basic TCL parser
- Create test helper functions package
- Set up test file structure

**Days 3-4: Code Generation**
- Implement `internal/codegen` package
- Create test templates
- Build `tcl2go` CLI tool
- Write tool documentation

**Day 5: Validation**
- Test parser on sample files
- Verify code generation
- Run generated tests
- Fix any issues

**Deliverables:**
- Working TCL parser
- Code generation tool
- Helper function library
- 10-20 sample generated tests

#### Week 2: Select Tests (Part 1)

**Days 1-2: select1.test**
- Parse and convert select1.test
- Review generated code
- Manual refinement
- Verify against SQLite

**Days 3-4: select2.test**
- Convert WHERE clause tests
- Add comparison operator tests
- Verify NULL handling
- Add LIKE pattern tests

**Day 5: select3.test**
- Convert ORDER BY tests
- Add DISTINCT tests
- Verify sorting behavior
- Handle NULL ordering

**Deliverables:**
- select1_test.go (50 tests)
- select2_test.go (80 tests)
- select3_test.go (40 tests)
- Coverage report

#### Week 3: Select Tests (Part 2) and Insert Tests

**Days 1-2: select4.test**
- Convert aggregate function tests
- GROUP BY tests
- HAVING clause tests
- Verify edge cases

**Days 3-4: insert.test**
- Basic INSERT tests
- INSERT SELECT tests
- Multi-row inserts
- Default value handling

**Day 5: insert2.test**
- INSERT OR REPLACE
- INSERT OR IGNORE
- Constraint handling
- AUTOINCREMENT tests

**Deliverables:**
- select4_test.go (60 tests)
- insert_test.go (50 tests)
- insert2_test.go (40 tests)

#### Week 4: Update and Delete Tests

**Days 1-2: update.test**
- Basic UPDATE tests
- UPDATE with WHERE
- Multi-column updates
- Subquery updates

**Days 3-4: delete.test**
- DELETE with WHERE
- DELETE all rows
- Cascade deletes
- Trigger interactions

**Day 5: Integration Testing**
- Cross-test INSERT/UPDATE/DELETE
- Transaction tests
- Constraint tests
- Performance baseline

**Deliverables:**
- update_test.go (45 tests)
- delete_test.go (35 tests)
- Integration test suite

#### Week 5: Expressions and Functions

**Days 1-3: expr.test**
- Arithmetic operators
- String operators
- Comparison operators
- Type coercion
- Operator precedence

**Days 4-5: func.test (Part 1)**
- String functions
- Numeric functions
- Basic date/time

**Deliverables:**
- expr_test.go (100 tests)
- func_test.go (60 tests)

#### Week 6: Functions and Indexes

**Days 1-2: func.test (Part 2)**
- Advanced functions
- Type conversion
- Aggregate functions
- Edge cases

**Days 3-4: index.test**
- CREATE INDEX
- DROP INDEX
- Unique indexes
- Multi-column indexes

**Day 5: index2.test**
- Index usage verification
- Query plan tests
- Covering indexes
- Partial indexes

**Deliverables:**
- func_test.go complete (120 tests)
- index_test.go (40 tests)
- index2_test.go (50 tests)

#### Week 7: Join Operations

**Days 1-3: join.test**
- INNER JOIN
- LEFT JOIN
- CROSS JOIN
- Multi-table joins
- Self-joins

**Days 4-5: Complex Join Tests**
- Join with subqueries
- Join with aggregates
- Join optimization
- Edge cases

**Deliverables:**
- join_test.go (60 tests)
- join_complex_test.go (30 tests)

#### Week 8: Advanced Features (Part 1)

**Days 1-3: cte.test**
- Non-recursive CTEs
- Recursive CTEs
- Multiple CTEs
- CTE with joins

**Days 4-5: trigger.test (Part 1)**
- BEFORE triggers
- AFTER triggers
- INSERT triggers
- UPDATE triggers

**Deliverables:**
- cte_test.go (50 tests)
- trigger_test.go (35 tests)

#### Week 9: Advanced Features (Part 2)

**Days 1-3: trigger.test (Part 2)**
- DELETE triggers
- Complex trigger logic
- Recursive triggers
- Trigger edge cases

**Days 4-5: types.test and collate.test**
- Type affinity tests
- Type conversion
- Collation sequences
- Case sensitivity

**Deliverables:**
- trigger_test.go complete (70 tests)
- types_test.go (50 tests)
- collate_test.go (30 tests)

#### Week 10: Transactions and Polish

**Days 1-2: trans.test**
- BEGIN/COMMIT/ROLLBACK
- Savepoints
- Nested transactions
- Isolation levels

**Days 3-4: lock.test**
- Lock acquisition
- Lock conflicts
- Deadlock scenarios
- Lock timeouts

**Day 5: Final Review**
- Review all converted tests
- Fix failing tests
- Generate coverage report
- Update documentation

**Deliverables:**
- trans_test.go (40 tests)
- lock_test.go (35 tests)
- Final coverage report
- Conversion documentation

### Milestones

1. **Milestone 1 (Week 2):** Core SELECT tests complete - 170 tests
2. **Milestone 2 (Week 4):** All DML tests complete - 390 tests
3. **Milestone 3 (Week 6):** Functions and indexes complete - 700 tests
4. **Milestone 4 (Week 8):** Advanced features started - 870 tests
5. **Milestone 5 (Week 10):** All priority tests complete - 1045 tests

### Resource Requirements

**Development Team:**
- 1 Senior Go Developer (TCL parsing, code generation, complex tests)
- 1-2 Go Developers (test conversion, verification, documentation)

**Tools and Infrastructure:**
- SQLite source code with TCL tests
- Go 1.21+ development environment
- CI/CD pipeline (GitHub Actions or similar)
- Code coverage tools
- Test result comparison tools

**Time Allocation:**
- 60% - Test conversion and refinement
- 20% - Tooling and automation
- 10% - Verification and debugging
- 10% - Documentation and review

---

## References

### SQLite Resources

- [SQLite Test Suite](https://www.sqlite.org/testing.html) - Overview of SQLite's testing approach
- [SQLite TCL Test Scripts](https://github.com/sqlite/sqlite/tree/master/test) - Original TCL tests
- [SQLite Test Coverage](https://www.sqlite.org/testing.html#coverage) - Coverage methodology

### Go Testing Resources

- [Go Testing Package](https://pkg.go.dev/testing) - Official documentation
- [Table Driven Tests](https://github.com/golang/go/wiki/TableDrivenTests) - Best practices
- [Advanced Testing](https://go.dev/blog/subtests) - Subtests and parallel testing
- [Test Fixtures](https://go.dev/blog/using-go-modules) - Test data management

### Related Documentation

- [TESTING.md](./TESTING.md) - Anthony driver testing guide
- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture overview
- [SECURITY.md](./SECURITY.md) - Security testing practices

### Example Converted Tests

See the following files for examples of converted tests:
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/insert_select_test.go`
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/orderby_test.go`
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver/integration_test.go`

---

## Appendix A: TCL Test Utility Reference

### Complete do_test Family

```tcl
# Basic test execution
do_test name { code } { expected }

# SQL execution test
do_execsql_test name { sql } { expected }

# Error catching test
do_catchsql_test name { sql } { code message }

# EXPLAIN QUERY PLAN test
do_eqp_test name { sql } { plan }

# Fault simulation test
do_faultsim_test name -prep { setup } -body { test } -test { verify }

# Pattern matching test
do_test_regexp name { code } { pattern }

# File operations test
do_file_test name { operation } { expected }
```

### Common TCL Utilities

```tcl
# SQL execution
execsql { sql }                    # Execute SQL, return results
catchsql { sql }                   # Execute SQL, catch errors
db eval { sql }                    # Execute with callback
db transaction { code }            # Execute in transaction

# Database operations
sqlite3 db filename                # Open database
db close                           # Close database
forcedelete filename               # Delete file if exists
file exists filename               # Check file existence

# Data verification
integrity_check                    # PRAGMA integrity_check
explain { sql }                    # EXPLAIN output
explain_query_plan { sql }         # EXPLAIN QUERY PLAN

# Utility functions
lsort list                         # Sort list
llength list                       # List length
lindex list index                  # Get list element
```

### Expected Result Formats

```tcl
# Single value
{42}

# Multiple values (space-separated)
{1 2 3}

# Multiple rows
{1 2 3 4 5 6}  # Two rows of 3 columns each

# Error result
{1 {error message}}  # Code 1 with message

# Empty result
{}
```

## Appendix B: Go Test Helper Reference

### Complete Helper Function Set

```go
// Database setup
func setupTestDB(t *testing.T) *sql.DB
func setupTestDBWithPath(t *testing.T, path string) *sql.DB
func setupMemoryDB(t *testing.T) *sql.DB

// SQL execution
func execSQL(t *testing.T, db *sql.DB, query string, args ...interface{}) [][]interface{}
func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) sql.Result
func catchSQL(t *testing.T, db *sql.DB, query string, args ...interface{}) (int, string, [][]interface{})

// Error handling
func expectError(t *testing.T, db *sql.DB, query string, expectedMsg string)
func expectNoError(t *testing.T, db *sql.DB, query string)

// Result scanning
func scanAllRows(t *testing.T, rows *sql.Rows) [][]interface{}
func scanSingleRow(t *testing.T, rows *sql.Rows) []interface{}
func scanSingleValue(t *testing.T, rows *sql.Rows) interface{}

// Result comparison
func equalRows(got, want [][]interface{}) bool
func equalValues(got, want interface{}) bool
func containsError(err error, substring string) bool

// Schema helpers
func tableExists(t *testing.T, db *sql.DB, tableName string) bool
func columnCount(t *testing.T, db *sql.DB, tableName string) int
func getTableSchema(t *testing.T, db *sql.DB, tableName string) string
```

---

**Document Version:** 1.0
**Last Updated:** 2026-02-28
**Maintained By:** Anthony SQLite Driver Team
**Status:** Active Planning Document
