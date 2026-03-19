# Are We There Yet?

**Anthony vs SQLite C Reference Implementation**

This document tracks feature parity between Anthony (pure Go SQLite) and the reference C implementation.

| Legend | Meaning |
|--------|---------|
| :white_check_mark: | Fully implemented and tested |
| :large_orange_diamond: | Partially implemented or has known issues |
| :x: | Not implemented |
| :construction: | In progress |

---

## Test Status

| Metric | Count |
|--------|-------|
| **Passing Tests** | 14,072+ |
| **Skipped Tests** | 736 |
| **Trinity Tests** | 1,257 passing, 0 skipped |
| **Pass Rate** | 100% (0 failures) |
| **Race Detector** | Clean (all packages) |
| **Coverage Target** | 100% trinity parity |

---

## SQL Statements

### Data Manipulation Language (DML)

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT | :white_check_mark: | Full support including joins, subqueries |
| INSERT | :white_check_mark: | Including INSERT OR REPLACE, INSERT OR IGNORE on UNIQUE columns |
| UPDATE | :white_check_mark: | Including UPDATE...FROM syntax |
| DELETE | :white_check_mark: | Basic DELETE with WHERE |
| REPLACE | :white_check_mark: | Via INSERT OR REPLACE |
| UPSERT (ON CONFLICT) | :large_orange_diamond: | Basic support, complex cases untested |
| INSERT value count validation | :white_check_mark: | Proper error for column/value count mismatch |
| RETURNING clause | :white_check_mark: | INSERT/UPDATE/DELETE RETURNING with * expansion and expressions |

### Data Definition Language (DDL)

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE TABLE | :white_check_mark: | Including constraints |
| CREATE TABLE AS | :white_check_mark: | |
| CREATE INDEX | :white_check_mark: | Including UNIQUE indexes |
| CREATE VIEW | :white_check_mark: | |
| CREATE TRIGGER | :white_check_mark: | Runtime execution complete - BEFORE/AFTER x INSERT/UPDATE/DELETE all working |
| CREATE VIRTUAL TABLE | :white_check_mark: | SQL parsing complete, FTS5/R-Tree modules ready |
| ALTER TABLE | :white_check_mark: | RENAME TABLE, RENAME COLUMN, DROP COLUMN all implemented |
| DROP TABLE | :white_check_mark: | |
| DROP INDEX | :white_check_mark: | |
| DROP VIEW | :white_check_mark: | |
| DROP TRIGGER | :white_check_mark: | |

### Transaction Control

| Feature | Status | Notes |
|---------|--------|-------|
| BEGIN TRANSACTION | :white_check_mark: | |
| COMMIT | :white_check_mark: | |
| ROLLBACK | :white_check_mark: | |
| SAVEPOINT | :white_check_mark: | |
| RELEASE SAVEPOINT | :white_check_mark: | |
| ROLLBACK TO SAVEPOINT | :white_check_mark: | |

### Database Operations

| Feature | Status | Notes |
|---------|--------|-------|
| ATTACH DATABASE | :white_check_mark: | Fully implemented with cross-database CREATE TABLE, INSERT, SELECT |
| DETACH DATABASE | :white_check_mark: | Fully implemented |
| ANALYZE | :white_check_mark: | sqlite_stat1 table creation, row counts, index selectivity stats |
| REINDEX | :white_check_mark: | Parser complete, basic execution |
| VACUUM | :large_orange_diamond: | 30 tests skipped - schema persistence issues |
| EXPLAIN | :white_check_mark: | Basic output format |
| EXPLAIN QUERY PLAN | :large_orange_diamond: | Improved — SCAN/SEARCH/JOIN nodes, subquery reporting |

---

## Query Features

### SELECT Clauses

| Feature | Status | Notes |
|---------|--------|-------|
| WHERE | :white_check_mark: | |
| ORDER BY | :white_check_mark: | SELECT * with ORDER BY fixed |
| GROUP BY | :white_check_mark: | AVG returns float correctly, NULL-safe group comparison |
| HAVING | :white_check_mark: | Including aggregate expressions |
| LIMIT | :white_check_mark: | |
| OFFSET | :white_check_mark: | |
| DISTINCT | :white_check_mark: | |
| ALL | :white_check_mark: | |

### Joins

| Feature | Status | Notes |
|---------|--------|-------|
| INNER JOIN | :white_check_mark: | Including with aggregates via sorter pipeline |
| LEFT JOIN | :white_check_mark: | Including unmatched row NULL padding |
| RIGHT JOIN | :white_check_mark: | Table-swap compilation to LEFT JOIN with column reordering |
| FULL OUTER JOIN | :large_orange_diamond: | Basic support via combined LEFT JOIN + anti-join |
| CROSS JOIN | :white_check_mark: | |
| NATURAL JOIN | :white_check_mark: | |
| USING clause | :white_check_mark: | |
| ON clause | :white_check_mark: | |
| JOIN with CTE cursors | :white_check_mark: | Fixed cursor index handling for CTE temp tables |

### Subqueries

| Feature | Status | Notes |
|---------|--------|-------|
| Scalar subqueries | :white_check_mark: | |
| EXISTS | :white_check_mark: | |
| IN (subquery) | :white_check_mark: | |
| NOT IN (subquery) | :white_check_mark: | |
| Correlated subqueries | :white_check_mark: | Including correlated TVF cross-joins |
| Derived tables (FROM subquery) | :white_check_mark: | B-tree temp table materialization |

### Set Operations

| Feature | Status | Notes |
|---------|--------|-------|
| UNION | :white_check_mark: | |
| UNION ALL | :white_check_mark: | |
| INTERSECT | :white_check_mark: | Correct SQL-standard precedence (INTERSECT before UNION) |
| EXCEPT | :white_check_mark: | |

### Common Table Expressions (CTEs)

| Feature | Status | Notes |
|---------|--------|-------|
| Non-recursive CTE | :white_check_mark: | |
| Recursive CTE | :large_orange_diamond: | Base case + iterative step with depth limiting; edge cases remain |
| Multiple CTEs | :white_check_mark: | Including CTEs with JOINs |
| CTE with column list | :white_check_mark: | |

---

## Expressions & Logic

| Feature | Status | Notes |
|---------|--------|-------|
| AND/OR short-circuit | :white_check_mark: | Correct NULL handling (NULL AND 0 = 0) |
| Comparison operators | :white_check_mark: | |
| Arithmetic operators | :white_check_mark: | Integer division returns integer per SQLite |
| BETWEEN | :white_check_mark: | |
| IN (list) | :white_check_mark: | |
| CASE / WHEN | :white_check_mark: | |
| CAST | :white_check_mark: | |
| IS DISTINCT FROM | :white_check_mark: | NULL-safe inequality comparison |
| IS NOT DISTINCT FROM | :white_check_mark: | NULL-safe equality comparison |

---

## Constraints

| Feature | Status | Notes |
|---------|--------|-------|
| PRIMARY KEY | :white_check_mark: | |
| NOT NULL | :white_check_mark: | |
| UNIQUE | :white_check_mark: | INSERT OR IGNORE/REPLACE works on non-PK UNIQUE columns |
| CHECK | :white_check_mark: | |
| DEFAULT | :white_check_mark: | Proper type affinity for default values |
| FOREIGN KEY (syntax) | :white_check_mark: | Parsed correctly |
| FOREIGN KEY (runtime) | :white_check_mark: | 83/83 passing - deferred, collation, affinity, SET DEFAULT all working |
| COLLATE | :white_check_mark: | BINARY, NOCASE, RTRIM + custom collations |
| AUTOINCREMENT | :white_check_mark: | |
| GENERATED columns | :white_check_mark: | STORED/VIRTUAL semantics, direct write prevention |

---

## Table Types

| Feature | Status | Notes |
|---------|--------|-------|
| Regular tables | :white_check_mark: | |
| WITHOUT ROWID tables | :large_orange_diamond: | 45 passing, 6 skipped - JOINs fixed, ROLLBACK/CASCADE in progress |
| Temporary tables | :large_orange_diamond: | Basic support |
| Virtual tables | :white_check_mark: | json_each/json_tree/generate_series TVFs with correlated cross-joins |

---

## Built-in Functions

### Aggregate Functions

| Function | Status |
|----------|--------|
| COUNT | :white_check_mark: |
| SUM | :white_check_mark: |
| AVG | :white_check_mark: |
| MIN | :white_check_mark: |
| MAX | :white_check_mark: |
| GROUP_CONCAT | :white_check_mark: |
| TOTAL | :white_check_mark: |

### String Functions

| Function | Status |
|----------|--------|
| length | :white_check_mark: |
| substr | :white_check_mark: |
| upper | :white_check_mark: |
| lower | :white_check_mark: |
| trim | :white_check_mark: |
| ltrim | :white_check_mark: |
| rtrim | :white_check_mark: |
| replace | :white_check_mark: |
| instr | :white_check_mark: |
| hex | :white_check_mark: |
| quote | :white_check_mark: |
| printf | :white_check_mark: |
| format | :white_check_mark: |
| char | :white_check_mark: |
| unicode | :white_check_mark: |
| like | :white_check_mark: |
| glob | :white_check_mark: |

### Math Functions

| Function | Status |
|----------|--------|
| abs | :white_check_mark: |
| round | :white_check_mark: |
| trunc | :white_check_mark: |
| random | :white_check_mark: |
| max | :white_check_mark: |
| min | :white_check_mark: |
| sign | :white_check_mark: |
| ceil/ceiling | :white_check_mark: |
| floor | :white_check_mark: |
| sqrt | :white_check_mark: |
| power/pow | :white_check_mark: |
| exp | :white_check_mark: |
| ln/log | :white_check_mark: |
| log10 | :white_check_mark: |
| log2 | :white_check_mark: |
| mod | :white_check_mark: |
| pi | :white_check_mark: |
| radians | :white_check_mark: |
| degrees | :white_check_mark: |
| sin/cos/tan | :white_check_mark: |
| asin/acos/atan/atan2 | :white_check_mark: |
| sinh/cosh/tanh | :white_check_mark: |
| asinh/acosh/atanh | :white_check_mark: |

### Date/Time Functions

| Function | Status | Notes |
|----------|--------|-------|
| date | :white_check_mark: | |
| time | :white_check_mark: | |
| datetime | :white_check_mark: | |
| julianday | :white_check_mark: | |
| unixepoch | :white_check_mark: | |
| strftime | :white_check_mark: | All format specifiers including %w, %u, %W, %j |

### JSON Functions

| Function | Status |
|----------|--------|
| json | :white_check_mark: |
| json_array | :white_check_mark: |
| json_object | :white_check_mark: |
| json_extract | :white_check_mark: |
| json_insert | :white_check_mark: |
| json_replace | :white_check_mark: |
| json_set | :white_check_mark: |
| json_remove | :white_check_mark: |
| json_type | :white_check_mark: |
| json_valid | :white_check_mark: |
| json_quote | :white_check_mark: |
| json_each | :white_check_mark: |
| json_tree | :white_check_mark: |
| json_group_array | :white_check_mark: |
| json_group_object | :white_check_mark: |

### Table-Valued Functions

| Function | Status | Notes |
|----------|--------|-------|
| json_each | :white_check_mark: | Correlated cross-joins supported |
| json_tree | :white_check_mark: | Correlated cross-joins supported |
| generate_series | :white_check_mark: | 1-3 args (start, stop, step), ascending/descending |

### Other Functions

| Function | Status |
|----------|--------|
| coalesce | :white_check_mark: |
| ifnull | :white_check_mark: |
| nullif | :white_check_mark: |
| iif | :white_check_mark: |
| typeof | :white_check_mark: |
| cast | :white_check_mark: |
| zeroblob | :white_check_mark: |
| likelihood | :white_check_mark: |
| likely | :white_check_mark: |
| unlikely | :white_check_mark: |

---

## Window Functions

| Feature | Status | Notes |
|---------|--------|-------|
| ROW_NUMBER | :white_check_mark: | Working with streaming and partition modes |
| RANK | :white_check_mark: | Working with OpWindowRank opcode |
| DENSE_RANK | :white_check_mark: | |
| NTILE | :white_check_mark: | |
| LAG | :white_check_mark: | |
| LEAD | :white_check_mark: | |
| FIRST_VALUE | :white_check_mark: | |
| LAST_VALUE | :white_check_mark: | |
| NTH_VALUE | :white_check_mark: | |
| PERCENT_RANK | :white_check_mark: | (rank-1)/(partition_size-1), 0.0 for single-row |
| CUME_DIST | :white_check_mark: | rows_with_value_lte/partition_size |
| OVER clause | :white_check_mark: | Parser and basic execution |
| PARTITION BY | :white_check_mark: | Working |
| WINDOW clause | :white_check_mark: | Named windows working |

---

## Extensions & Virtual Tables

| Feature | Status | Notes |
|---------|--------|-------|
| FTS5 (Full-Text Search) | :large_orange_diamond: | Module complete (128 tests), needs SQL parser integration |
| R-Tree (Spatial) | :large_orange_diamond: | Module complete (all tests pass), needs SQL parser integration |
| JSON1 | :white_check_mark: | Core + aggregate functions (json_group_array/object) |
| Custom functions | :large_orange_diamond: | Infrastructure exists |
| Custom collations | :white_check_mark: | Global and per-connection registration via RegisterCollation / Conn.CreateCollation |
| Loadable extensions | :x: | Not planned (Go limitation) |

---

## PRAGMA Statements

| PRAGMA | Status |
|--------|--------|
| table_info | :white_check_mark: |
| index_list | :white_check_mark: |
| index_info | :white_check_mark: |
| foreign_key_list | :white_check_mark: |
| database_list | :white_check_mark: |
| compile_options | :white_check_mark: |
| journal_mode | :white_check_mark: |
| synchronous | :large_orange_diamond: |
| cache_size | :large_orange_diamond: |
| page_size | :white_check_mark: |
| user_version | :white_check_mark: |
| schema_version | :white_check_mark: |
| integrity_check | :large_orange_diamond: |
| quick_check | :large_orange_diamond: |

---

## Storage & I/O

| Feature | Status | Notes |
|---------|--------|-------|
| B-Tree storage | :white_check_mark: | |
| Page-based I/O | :white_check_mark: | Race-free page writes |
| Overflow pages | :white_check_mark: | |
| Free page management | :white_check_mark: | |
| Journal mode (DELETE) | :white_check_mark: | |
| Journal mode (WAL) | :white_check_mark: | Write path, checkpoint modes (PASSIVE/FULL/RESTART/TRUNCATE), recovery |
| Memory databases | :white_check_mark: | |
| File locking (Unix) | :white_check_mark: | |
| File locking (Windows) | :large_orange_diamond: | Implemented via LockFileEx/UnlockFileEx, not yet tested on Windows |
| Online backup | :white_check_mark: | Page-by-page copy with progress callbacks |

---

## Security & Quality

| Feature | Status | Notes |
|---------|--------|-------|
| File permissions | :white_check_mark: | 0600 for all created files |
| No unsafe package in hot paths | :white_check_mark: | Only in syscall interop (mmap, Windows locks) |
| Race-free concurrent access | :white_check_mark: | Pager, busy handler, codegen all thread-safe |
| Go version | :white_check_mark: | 1.26.1 |
| Cyclomatic complexity | :white_check_mark: | ≤11 across all packages |
| GitHub Actions pinned | :white_check_mark: | SHA-pinned releases |

---

## Platform Support

| Platform | Status |
|----------|--------|
| Linux | :white_check_mark: |
| macOS | :white_check_mark: |
| Windows | :large_orange_diamond: | File locking implemented, not yet tested on Windows |
| WebAssembly | :white_check_mark: | Pure Go, no CGO |

---

## Summary

### What Works Well
- Core SQL (SELECT, INSERT, UPDATE, DELETE) with proper validation
- INSERT/UPDATE/DELETE RETURNING clause
- UPDATE...FROM for join-based updates
- INSERT OR IGNORE/REPLACE on UNIQUE columns (not just PK)
- Compound query precedence (INTERSECT before UNION per SQL standard)
- AND/OR short-circuit with correct NULL semantics
- IS DISTINCT FROM / IS NOT DISTINCT FROM operators
- AVG always returns float (regular and GROUP BY)
- LENGTH() returns string representation length for numbers
- Transactions and savepoints
- Indexes and query optimization
- All built-in functions (string, math, date/time, JSON, pattern matching)
- trunc(), likelihood/likely/unlikely, generate_series TVF
- B-tree storage engine with race-free page writes
- Memory and file databases
- Foreign keys (83/83 tests passing, including SET DEFAULT and deferred)
- WITHOUT ROWID tables (JOIN queries fixed)
- Triggers - full runtime execution
- Generated column enforcement (STORED/VIRTUAL)
- ALTER TABLE (RENAME TABLE, RENAME COLUMN, DROP COLUMN)
- ATTACH/DETACH DATABASE with cross-database queries
- Window functions (all 11: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST)
- JSON table-valued functions (json_each, json_tree)
- generate_series table-valued function
- CTE with JOINs (fixed cursor index handling)
- FTS5 module (API level - 128 tests)
- R-Tree module (API level - all tests)
- 1,257 Trinity (DO-178C trace) tests passing (100% parity)
- JSON aggregate functions (json_group_array, json_group_object)
- NULL-safe GROUP BY comparison
- Trigger expression substitution (CAST, BETWEEN, IN, CASE)
- JOIN+aggregate compilation pipeline
- View WHERE filtering after materialization
- ANALYZE with sqlite_stat1 statistics
- Custom collations (global + per-connection)
- WAL mode (write path, all checkpoint modes, recovery)
- Online backup API with progress callbacks
- Windows file locking (LockFileEx/UnlockFileEx)

### Known Gaps
- Recursive CTEs (iterative step works, complex edge cases remain)
- VACUUM operations (schema persistence issues, 30 tests skipped)
- FTS5/R-Tree SQL parser integration (modules work at API level)
- R-Tree persistent storage to database file

---

*Last updated: 2026-03-16*
*Reference: [SQLite Documentation](https://sqlite.org/docs.html)*
