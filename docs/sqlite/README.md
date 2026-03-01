# SQLite Documentation Index

Converted from the SQLite 3.51.2 HTML documentation using a custom HTML-to-Markdown
converter. Source: `contrib/sqlite/sqlite-doc-3510200/`.

74 files converted; 1 skipped (`indexedby.html` not present in source).

---

## Overview and Introduction

| File | Description |
| --- | --- |
| [ABOUT.md](ABOUT.md) | About SQLite — history, design goals, and use cases |
| [FEATURES.md](FEATURES.md) | Feature list and capabilities summary |
| [FAQ.md](FAQ.md) | Frequently Asked Questions |
| [HOW_IT_WORKS.md](HOW_IT_WORKS.md) | High-level explanation of how SQLite works |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Internal architecture and component overview |

---

## SQL Language Reference

### Core Language

| File | Description |
| --- | --- |
| [SQL_LANGUAGE_OVERVIEW.md](SQL_LANGUAGE_OVERVIEW.md) | SQL language overview and grammar index |
| [EXPRESSIONS.md](EXPRESSIONS.md) | Expression syntax and operators |
| [DATATYPES.md](DATATYPES.md) | Type system, storage classes, and affinity rules |
| [TYPE_AFFINITY.md](TYPE_AFFINITY.md) | Short reference for type affinity |
| [COMMENTS.md](COMMENTS.md) | SQL comment syntax |
| [FULL_SQL.md](FULL_SQL.md) | Complete SQL syntax diagram |

### Data Manipulation

| File | Description |
| --- | --- |
| [SELECT.md](SELECT.md) | SELECT statement |
| [INSERT.md](INSERT.md) | INSERT statement |
| [UPDATE.md](UPDATE.md) | UPDATE statement |
| [DELETE.md](DELETE.md) | DELETE statement |
| [WITH_CTE.md](WITH_CTE.md) | WITH clause and Common Table Expressions (CTEs) |

### Data Definition

| File | Description |
| --- | --- |
| [CREATE_TABLE.md](CREATE_TABLE.md) | CREATE TABLE statement |
| [CREATE_INDEX.md](CREATE_INDEX.md) | CREATE INDEX statement |
| [CREATE_VIEW.md](CREATE_VIEW.md) | CREATE VIEW statement |
| [CREATE_TRIGGER.md](CREATE_TRIGGER.md) | CREATE TRIGGER statement |
| [ALTER_TABLE.md](ALTER_TABLE.md) | ALTER TABLE statement |
| [DROP_TABLE.md](DROP_TABLE.md) | DROP TABLE statement |
| [DROP_INDEX.md](DROP_INDEX.md) | DROP INDEX statement |
| [DROP_VIEW.md](DROP_VIEW.md) | DROP VIEW statement |

### Transaction and Session Control

| File | Description |
| --- | --- |
| [TRANSACTIONS.md](TRANSACTIONS.md) | BEGIN / COMMIT / ROLLBACK |
| [ATTACH.md](ATTACH.md) | ATTACH and DETACH database |
| [ANALYZE.md](ANALYZE.md) | ANALYZE statement |
| [VACUUM.md](VACUUM.md) | VACUUM statement |
| [REINDEX.md](REINDEX.md) | REINDEX statement |

### Conflict Handling

| File | Description |
| --- | --- |
| [CONFLICT_RESOLUTION.md](CONFLICT_RESOLUTION.md) | ON CONFLICT clauses and behavior |
| [LANG_CONFLICT.md](LANG_CONFLICT.md) | Conflict-resolution syntax in DML statements |

---

## Built-in Functions

| File | Description |
| --- | --- |
| [CORE_FUNCTIONS.md](CORE_FUNCTIONS.md) | Core scalar functions |
| [AGGREGATE_FUNCTIONS.md](AGGREGATE_FUNCTIONS.md) | Aggregate functions |
| [DETERMINISTIC_FUNCTIONS.md](DETERMINISTIC_FUNCTIONS.md) | Deterministic vs. non-deterministic functions |

---

## Pragma Reference

| File | Description |
| --- | --- |
| [PRAGMA_REFERENCE.md](PRAGMA_REFERENCE.md) | Complete PRAGMA command reference |

---

## Advanced SQL Features

| File | Description |
| --- | --- |
| [FOREIGN_KEYS.md](FOREIGN_KEYS.md) | Foreign key support and enforcement |
| [GENERATED_COLUMNS.md](GENERATED_COLUMNS.md) | Generated (computed) columns |
| [EXPRESSION_INDEXES.md](EXPRESSION_INDEXES.md) | Indexes on expressions |
| [AUTOINCREMENT.md](AUTOINCREMENT.md) | AUTOINCREMENT behavior |
| [IN_MEMORY_DB.md](IN_MEMORY_DB.md) | In-memory databases |
| [FLOATING_POINT.md](FLOATING_POINT.md) | Floating-point arithmetic behavior |
| [INVALID_UTF.md](INVALID_UTF.md) | Handling of invalid UTF-8/UTF-16 text |

---

## Extensions

### Full-Text Search

| File | Description |
| --- | --- |
| [FTS3.md](FTS3.md) | FTS3 and FTS4 full-text search extension |
| [FTS5.md](FTS5.md) | FTS5 full-text search extension |

### JSON

| File | Description |
| --- | --- |
| [JSON1.md](JSON1.md) | JSON1 extension — JSON functions and operators |

### Geospatial / Spatial

| File | Description |
| --- | --- |
| [RTREE.md](RTREE.md) | R-Tree spatial index extension |
| [GEOPOLY.md](GEOPOLY.md) | Geopoly polygon extension |

### Virtual Tables and Table-Valued Functions

| File | Description |
| --- | --- |
| [VIRTUAL_TABLES_SPEC.md](VIRTUAL_TABLES_SPEC.md) | Virtual table interface specification |
| [CSV.md](CSV.md) | CSV virtual table extension |
| [CARRAY.md](CARRAY.md) | CARRAY table-valued function |
| [DBSTAT.md](DBSTAT.md) | DBSTAT virtual table |
| [DBPAGE.md](DBPAGE.md) | DBPAGE virtual table |
| [DBHASH.md](DBHASH.md) | DBHASH utility for database checksums |

---

## Internals and File Format

| File | Description |
| --- | --- |
| [FILE_FORMAT_SPEC.md](FILE_FORMAT_SPEC.md) | On-disk file format specification |
| [WAL_SPEC.md](WAL_SPEC.md) | Write-Ahead Log (WAL) format and behavior |
| [VDBE_OPCODES.md](VDBE_OPCODES.md) | Virtual Database Engine (VDBE) opcode reference |
| [ATOMIC_COMMIT.md](ATOMIC_COMMIT.md) | How atomic commits are implemented |
| [ISOLATION.md](ISOLATION.md) | Transaction isolation and concurrency |
| [LIMITS.md](LIMITS.md) | Compile-time and run-time limits |

---

## VFS and I/O

| File | Description |
| --- | --- |
| [BACKUP_API.md](BACKUP_API.md) | Online backup API |
| [ASYNC_VFS.md](ASYNC_VFS.md) | Asynchronous VFS extension |
| [CHECKSUM_VFS.md](CHECKSUM_VFS.md) | Checksum VFS shim |
| [BIND_POINTER.md](BIND_POINTER.md) | Pointer passing via sqlite3_bind_pointer |

---

## C API

| File | Description |
| --- | --- |
| [C_INTRO.md](C_INTRO.md) | Introduction to the C/C++ API |
| [IMPOSTER.md](IMPOSTER.md) | Imposter tables for schema manipulation |

---

## Query Planning and Performance

| File | Description |
| --- | --- |
| [EXPLAIN_QUERY_PLAN.md](EXPLAIN_QUERY_PLAN.md) | EXPLAIN QUERY PLAN output interpretation |

---

## Building SQLite

| File | Description |
| --- | --- |
| [COMPILE.md](COMPILE.md) | How to compile SQLite |
| [CUSTOM_BUILD.md](CUSTOM_BUILD.md) | Custom build options and compile-time flags |

---

## Operations, Testing, and Security

| File | Description |
| --- | --- |
| [SECURITY.md](SECURITY.md) | Security considerations |
| [TESTING.md](TESTING.md) | SQLite testing methodology |
| [DEBUGGING.md](DEBUGGING.md) | Debugging techniques |
| [ERROR_LOG.md](ERROR_LOG.md) | Error logging interface |

---

## Skipped Files

The following source file was not found in the source directory and was not converted:

- `indexedby.html` (INDEXED_BY.md)
