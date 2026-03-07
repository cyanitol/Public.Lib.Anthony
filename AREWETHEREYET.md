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
| **Passing Tests** | ~655 |
| **Skipped Tests** | ~476 |
| **Coverage Target** | 58% feature parity |

---

## SQL Statements

### Data Manipulation Language (DML)

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT | :white_check_mark: | Full support including joins, subqueries |
| INSERT | :white_check_mark: | Including INSERT OR REPLACE, INSERT OR IGNORE |
| UPDATE | :white_check_mark: | Basic UPDATE with WHERE |
| DELETE | :white_check_mark: | Basic DELETE with WHERE |
| REPLACE | :white_check_mark: | Via INSERT OR REPLACE |
| UPSERT (ON CONFLICT) | :large_orange_diamond: | Basic support, complex cases untested |

### Data Definition Language (DDL)

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE TABLE | :white_check_mark: | Including constraints |
| CREATE TABLE AS | :white_check_mark: | |
| CREATE INDEX | :white_check_mark: | Including UNIQUE indexes |
| CREATE VIEW | :white_check_mark: | |
| CREATE TRIGGER | :large_orange_diamond: | Parsed but runtime execution incomplete |
| CREATE VIRTUAL TABLE | :large_orange_diamond: | Infrastructure exists, limited vtab support |
| ALTER TABLE | :large_orange_diamond: | ADD COLUMN only |
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
| ATTACH DATABASE | :large_orange_diamond: | Partially implemented |
| DETACH DATABASE | :large_orange_diamond: | Partially implemented |
| ANALYZE | :x: | Planned for v0.2.0 |
| REINDEX | :white_check_mark: | Parser complete, basic execution |
| VACUUM | :large_orange_diamond: | 30 tests skipped - schema persistence issues |
| EXPLAIN | :white_check_mark: | Basic output format |
| EXPLAIN QUERY PLAN | :large_orange_diamond: | Partial support |

---

## Query Features

### SELECT Clauses

| Feature | Status | Notes |
|---------|--------|-------|
| WHERE | :white_check_mark: | |
| ORDER BY | :large_orange_diamond: | Complex cases incomplete |
| GROUP BY | :white_check_mark: | |
| HAVING | :white_check_mark: | |
| LIMIT | :white_check_mark: | |
| OFFSET | :white_check_mark: | |
| DISTINCT | :white_check_mark: | |
| ALL | :white_check_mark: | |

### Joins

| Feature | Status | Notes |
|---------|--------|-------|
| INNER JOIN | :white_check_mark: | |
| LEFT JOIN | :white_check_mark: | |
| RIGHT JOIN | :white_check_mark: | |
| CROSS JOIN | :white_check_mark: | |
| NATURAL JOIN | :white_check_mark: | |
| USING clause | :white_check_mark: | |
| ON clause | :white_check_mark: | |

### Subqueries

| Feature | Status | Notes |
|---------|--------|-------|
| Scalar subqueries | :white_check_mark: | |
| EXISTS | :white_check_mark: | |
| IN (subquery) | :white_check_mark: | |
| NOT IN (subquery) | :white_check_mark: | |
| Correlated subqueries | :large_orange_diamond: | Some edge cases cause issues |
| Derived tables (FROM subquery) | :white_check_mark: | |

### Set Operations

| Feature | Status | Notes |
|---------|--------|-------|
| UNION | :white_check_mark: | |
| UNION ALL | :white_check_mark: | |
| INTERSECT | :white_check_mark: | |
| EXCEPT | :white_check_mark: | |

### Common Table Expressions (CTEs)

| Feature | Status | Notes |
|---------|--------|-------|
| Non-recursive CTE | :white_check_mark: | |
| Recursive CTE | :large_orange_diamond: | 17 tests skipped - bytecode incomplete |
| Multiple CTEs | :white_check_mark: | |
| CTE with column list | :white_check_mark: | |

---

## Constraints

| Feature | Status | Notes |
|---------|--------|-------|
| PRIMARY KEY | :white_check_mark: | |
| NOT NULL | :white_check_mark: | |
| UNIQUE | :white_check_mark: | |
| CHECK | :white_check_mark: | |
| DEFAULT | :white_check_mark: | |
| FOREIGN KEY (syntax) | :white_check_mark: | Parsed correctly |
| FOREIGN KEY (runtime) | :construction: | 38 tests skipped - FK manager not integrated |
| COLLATE | :white_check_mark: | BINARY, NOCASE, RTRIM |
| AUTOINCREMENT | :white_check_mark: | |

---

## Table Types

| Feature | Status | Notes |
|---------|--------|-------|
| Regular tables | :white_check_mark: | |
| WITHOUT ROWID tables | :construction: | 23 tests skipped - uses OpNewRowid incorrectly |
| Temporary tables | :large_orange_diamond: | Basic support |
| Virtual tables | :large_orange_diamond: | Infrastructure only |

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
| random | :white_check_mark: |
| max | :white_check_mark: |
| min | :white_check_mark: |
| sign | :white_check_mark: |

### Date/Time Functions

| Function | Status | Notes |
|----------|--------|-------|
| date | :white_check_mark: | |
| time | :white_check_mark: | |
| datetime | :white_check_mark: | |
| julianday | :white_check_mark: | |
| unixepoch | :white_check_mark: | |
| strftime | :large_orange_diamond: | Some format specifiers incomplete |

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
| json_each | :x: |
| json_tree | :x: |

### Other Functions

| Function | Status |
|----------|--------|
| coalesce | :white_check_mark: |
| ifnull | :white_check_mark: |
| nullif | :white_check_mark: |
| typeof | :white_check_mark: |
| cast | :white_check_mark: |
| zeroblob | :white_check_mark: |
| likelihood | :x: |
| likely | :x: |
| unlikely | :x: |

---

## Window Functions

| Feature | Status | Notes |
|---------|--------|-------|
| ROW_NUMBER | :x: | Not implemented |
| RANK | :x: | Not implemented |
| DENSE_RANK | :x: | Not implemented |
| NTILE | :x: | Not implemented |
| LAG | :x: | Not implemented |
| LEAD | :x: | Not implemented |
| FIRST_VALUE | :x: | Not implemented |
| LAST_VALUE | :x: | Not implemented |
| NTH_VALUE | :x: | Not implemented |
| OVER clause | :x: | Not implemented |
| PARTITION BY | :x: | Not implemented |
| WINDOW clause | :x: | Not implemented |

---

## Extensions & Virtual Tables

| Feature | Status | Notes |
|---------|--------|-------|
| FTS5 (Full-Text Search) | :x: | Planned for v0.2.0 |
| R-Tree (Spatial) | :x: | Scaffolding exists, planned for v0.2.0 |
| JSON1 | :white_check_mark: | Core functions implemented |
| Custom functions | :large_orange_diamond: | Infrastructure exists |
| Custom collations | :x: | Planned |
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
| journal_mode | :large_orange_diamond: |
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
| Page-based I/O | :white_check_mark: | |
| Overflow pages | :white_check_mark: | |
| Free page management | :white_check_mark: | |
| Journal mode (DELETE) | :white_check_mark: | |
| Journal mode (WAL) | :large_orange_diamond: | Infrastructure exists |
| Memory databases | :white_check_mark: | |
| File locking (Unix) | :white_check_mark: | |
| File locking (Windows) | :x: | Not implemented |

---

## Platform Support

| Platform | Status |
|----------|--------|
| Linux | :white_check_mark: |
| macOS | :white_check_mark: |
| Windows | :large_orange_diamond: | No file locking |
| WebAssembly | :white_check_mark: | Pure Go, no CGO |

---

## Summary

### What Works Well
- Core SQL (SELECT, INSERT, UPDATE, DELETE)
- Transactions and savepoints
- Indexes and query optimization
- Most built-in functions
- B-tree storage engine
- Memory and file databases

### Known Gaps (v0.1.x blockers)
- Foreign key runtime enforcement (38 tests)
- WITHOUT ROWID tables (23 tests)
- Recursive CTEs (17 tests)
- VACUUM operations (30 tests)

### Major Missing Features (v0.2.0+)
- Window functions
- ANALYZE (query statistics)
- FTS5 (full-text search)
- R-Tree (spatial indexing)
- Trigger runtime execution

---

*Last updated: 2026-03-06*
*Reference: [SQLite Documentation](https://sqlite.org/docs.html)*
