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
| **Passing Tests** | 4951 |
| **Skipped Tests** | 466 |
| **Pass Rate** | 91.4% |
| **Coverage Target** | 70% feature parity |

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
| CREATE VIRTUAL TABLE | :white_check_mark: | SQL parsing complete, FTS5/R-Tree modules ready |
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
| FOREIGN KEY (runtime) | :white_check_mark: | 83/83 passing - deferred, collation, affinity all working |
| COLLATE | :white_check_mark: | BINARY, NOCASE, RTRIM |
| AUTOINCREMENT | :white_check_mark: | |

---

## Table Types

| Feature | Status | Notes |
|---------|--------|-------|
| Regular tables | :white_check_mark: | |
| WITHOUT ROWID tables | :large_orange_diamond: | 45 passing, 6 skipped - JOINs fixed, ROLLBACK/CASCADE in progress |
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
| ROW_NUMBER | :white_check_mark: | Working with streaming and partition modes |
| RANK | :white_check_mark: | Working with OpWindowRank opcode |
| DENSE_RANK | :construction: | Needs implementation |
| NTILE | :large_orange_diamond: | Opcode exists, needs compiler integration |
| LAG | :large_orange_diamond: | Opcode exists, needs compiler integration |
| LEAD | :large_orange_diamond: | Opcode exists, needs compiler integration |
| FIRST_VALUE | :large_orange_diamond: | Opcode exists, needs compiler integration |
| LAST_VALUE | :large_orange_diamond: | Opcode exists, needs compiler integration |
| NTH_VALUE | :x: | Not implemented |
| OVER clause | :white_check_mark: | Parser and basic execution |
| PARTITION BY | :large_orange_diamond: | Infrastructure exists |
| WINDOW clause | :x: | Not implemented |

---

## Extensions & Virtual Tables

| Feature | Status | Notes |
|---------|--------|-------|
| FTS5 (Full-Text Search) | :large_orange_diamond: | Module complete (128 tests), needs SQL parser integration |
| R-Tree (Spatial) | :large_orange_diamond: | Module complete (all tests pass), needs SQL parser integration |
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
- Foreign keys (83/83 tests passing)
- WITHOUT ROWID tables (JOIN queries fixed)
- Window functions RANK() with opcodes
- CREATE VIRTUAL TABLE SQL parsing
- FTS5 module (API level - 128 tests)
- R-Tree module (API level - all tests)

### Known Gaps (v0.1.x blockers)
- WITHOUT ROWID ROLLBACK (cache sync - 1 test)
- Recursive CTEs (17 tests)
- VACUUM operations (30 tests)
- Window function DENSE_RANK (not implemented)

### Major Missing Features (v0.2.0+)
- ANALYZE (query statistics)
- Trigger runtime execution
- Advanced window functions (LAG, LEAD, etc.)

---

*Last updated: 2026-03-09*
*Reference: [SQLite Documentation](https://sqlite.org/docs.html)*
