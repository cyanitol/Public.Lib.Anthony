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
| **Passing Tests** | 17,443 |
| **Skipped Tests** | 0 |
| **Trinity Tests** | 1,257 passing, 0 skipped |
| **Pass Rate** | 100% (0 failures, 0 skips) |
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
| UPSERT (ON CONFLICT) | :white_check_mark: | INSERT OR REPLACE, INSERT OR IGNORE, ON CONFLICT DO UPDATE |
| INSERT value count validation | :white_check_mark: | Proper error for column/value count mismatch |
| RETURNING clause | :white_check_mark: | INSERT/UPDATE/DELETE RETURNING with * expansion and expressions |

### Data Definition Language (DDL)

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE TABLE | :white_check_mark: | Including constraints |
| CREATE TABLE AS | :white_check_mark: | |
| CREATE INDEX | :white_check_mark: | Including UNIQUE, partial (WHERE), and expression indexes |
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
| VACUUM | :white_check_mark: | All tests passing (page rebuilds, indexes, schema cookie, views, incremental) |
| EXPLAIN | :white_check_mark: | Basic output format |
| EXPLAIN QUERY PLAN | :white_check_mark: | SCAN/SEARCH/USE TEMP B-TREE nodes, index usage, subquery plans |

---

## Query Features

### SELECT Clauses

| Feature | Status | Notes |
|---------|--------|-------|
| WHERE | :white_check_mark: | |
| ORDER BY | :white_check_mark: | SELECT * with ORDER BY fixed, NULLS FIRST/LAST |
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
| FULL OUTER JOIN | :white_check_mark: | LEFT JOIN + anti-join with column reordering |
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
| Recursive CTE | :white_check_mark: | Iterative VDBE loop, dedup, cycle detection, depth limiting |
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
| WITHOUT ROWID tables | :white_check_mark: | 68 tests passing, conflict resolution (INSERT OR REPLACE) working |
| Temporary tables | :white_check_mark: | CREATE TEMP TABLE, in-memory storage |
| Virtual tables | :white_check_mark: | FTS5/R-Tree SQL integration + json_each/json_tree/generate_series TVFs |

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
| unhex | :white_check_mark: |
| soundex | :white_check_mark: |

### Math Functions

| Function | Status |
|----------|--------|
| abs | :white_check_mark: |
| round | :white_check_mark: |
| trunc | :white_check_mark: |
| random | :white_check_mark: |
| randomblob | :white_check_mark: |
| max | :white_check_mark: |
| min | :white_check_mark: |
| sign | :white_check_mark: |
| ceil/ceiling | :white_check_mark: |
| floor | :white_check_mark: |
| sqrt | :white_check_mark: |
| power/pow | :white_check_mark: |
| exp | :white_check_mark: |
| ln/log | :white_check_mark: | Including two-argument log(B,X) |
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
| current_date | :white_check_mark: | |
| current_time | :white_check_mark: | |
| current_timestamp | :white_check_mark: | |

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
| json_array_length | :white_check_mark: |
| json_group_array | :white_check_mark: |
| json_group_object | :white_check_mark: |
| -> (arrow operator) | :white_check_mark: |
| ->> (arrow operator) | :white_check_mark: |

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
| last_insert_rowid | :white_check_mark: |
| changes | :white_check_mark: |
| total_changes | :white_check_mark: |
| sqlite_version | :white_check_mark: |

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
| Frame EXCLUDE | :white_check_mark: | EXCLUDE NO OTHERS, CURRENT ROW, GROUP, TIES |
| FILTER clause | :white_check_mark: | Aggregate FILTER(WHERE ...) in both GROUP BY and non-GROUP BY paths |

---

## Extensions & Virtual Tables

| Feature | Status | Notes |
|---------|--------|-------|
| FTS5 (Full-Text Search) | :white_check_mark: | Module + SQL integration (CREATE VIRTUAL TABLE, INSERT, SELECT, MATCH, DELETE) |
| R-Tree (Spatial) | :white_check_mark: | Module + SQL integration (CREATE VIRTUAL TABLE, INSERT, spatial range queries, DELETE) |
| JSON1 | :white_check_mark: | Core + aggregate functions (json_group_array/object) |
| Custom functions | :white_check_mark: | UDF registration infrastructure |
| Custom collations | :white_check_mark: | Global and per-connection registration via RegisterCollation / Conn.CreateCollation |
| Loadable extensions | :x: | Not planned (Go limitation) |

---

## PRAGMA Statements

| PRAGMA | Status | Notes |
|--------|--------|-------|
| table_info | :white_check_mark: | |
| index_list | :white_check_mark: | Including partial flag |
| index_info | :white_check_mark: | seqno, cid, name |
| foreign_keys | :white_check_mark: | GET/SET |
| foreign_key_list | :white_check_mark: | |
| foreign_key_check | :white_check_mark: | Per-table and whole-database |
| database_list | :white_check_mark: | |
| compile_options | :white_check_mark: | |
| journal_mode | :white_check_mark: | GET/SET |
| synchronous | :white_check_mark: | GET/SET (OFF/NORMAL/FULL/EXTRA) |
| cache_size | :white_check_mark: | GET/SET |
| page_size | :white_check_mark: | GET (SET before DB creation only) |
| page_count | :white_check_mark: | |
| user_version | :white_check_mark: | GET/SET |
| schema_version | :white_check_mark: | GET/SET |
| auto_vacuum | :white_check_mark: | GET/SET (NONE/FULL/INCREMENTAL) |
| incremental_vacuum | :white_check_mark: | PRAGMA incremental_vacuum(N) |
| integrity_check | :white_check_mark: | Free list verification |
| quick_check | :white_check_mark: | Alias for integrity_check |

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
| File locking (Windows) | :white_check_mark: | Implemented via LockFileEx/UnlockFileEx |
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
| GitHub Actions | :white_check_mark: | checkout@v6, setup-go@v6, gh CLI (Node.js 24) |

---

## Platform Support

| Platform | Status |
|----------|--------|
| Linux | :white_check_mark: |
| macOS | :white_check_mark: |
| Windows | :white_check_mark: | File locking implemented, pure Go |
| WebAssembly | :white_check_mark: | Pure Go, no CGO |

---

## Summary

### What Works Well
- Core SQL (SELECT, INSERT, UPDATE, DELETE) with full validation
- All JOIN types (INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL)
- INSERT/UPDATE/DELETE RETURNING clause
- UPDATE...FROM for join-based updates
- UPSERT (INSERT OR REPLACE/IGNORE, ON CONFLICT DO UPDATE)
- Compound queries (UNION/INTERSECT/EXCEPT with correct precedence)
- Subqueries (scalar, EXISTS, IN, correlated, derived tables)
- Common Table Expressions (non-recursive and recursive with cycle detection)
- Transactions and savepoints (SAVEPOINT/RELEASE/ROLLBACK TO)
- All built-in functions (string, math, date/time, JSON, pattern matching)
- All 11 window functions with PARTITION BY, named WINDOW clauses, frame EXCLUDE, and FILTER clause
- Table-valued functions (json_each, json_tree, generate_series)
- JSON aggregate functions (json_group_array, json_group_object)
- FTS5 module (MATCH queries, INSERT/DELETE via SQL)
- R-Tree module (spatial range queries, INSERT/DELETE via SQL)
- B-tree storage engine with race-free page writes and correct split logic
- VACUUM (page rebuild, indexes, schema cookie, views)
- WITHOUT ROWID tables (68 tests, conflict resolution working)
- Foreign keys (83/83 tests, including SET DEFAULT and deferred)
- Triggers (full runtime: BEFORE/AFTER x INSERT/UPDATE/DELETE, cascading)
- Generated columns (STORED/VIRTUAL semantics, write prevention)
- ALTER TABLE (RENAME TABLE, RENAME COLUMN, DROP COLUMN)
- ATTACH/DETACH DATABASE with cross-database queries
- ANALYZE with sqlite_stat1 statistics
- EXPLAIN QUERY PLAN (SCAN/SEARCH/JOIN nodes, index usage)
- Partial indexes (CREATE INDEX ... WHERE) with planner integration
- Expression indexes (CREATE INDEX ... (expr))
- Incremental vacuum (PRAGMA auto_vacuum=INCREMENTAL, PRAGMA incremental_vacuum(N))
- All 19 PRAGMAs fully implemented (GET/SET)
- Connection state functions (last_insert_rowid, changes, total_changes, sqlite_version)
- NULLS FIRST/LAST ordering across all sort paths (regular, compound, TVF, vtab)
- JSON -> / ->> arrow operators (desugared to json_extract/json_extract_text)
- Custom collations (global + per-connection registration)
- WAL mode (write path, all checkpoint modes, recovery)
- Online backup API with progress callbacks
- Memory and file databases
- Pager reference counting and cache eviction under pressure
- Race detector clean across all packages
- Cyclomatic complexity ≤11 for all functions

### Intentional Exclusions
- Loadable extensions (.so/.dll) - not possible in pure Go without CGO

---

*Last updated: 2026-03-19*
*Reference: [SQLite Documentation](https://sqlite.org/docs.html)*
