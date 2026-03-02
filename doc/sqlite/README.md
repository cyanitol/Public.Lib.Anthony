# SQLite 3.51.2 Reference Documentation

This directory contains the complete SQLite 3.51.2 official documentation,
converted from HTML to Markdown for offline reference.

**305 files total** -- 141 root docs, 71 syntax diagrams, 51 session extension, 42 C API.

---

## SQL Language Reference

Core SQL statement syntax:

| File | Description |
|---|---|
| [SQL_LANGUAGE_OVERVIEW.md](SQL_LANGUAGE_OVERVIEW.md) | SQL language overview |
| [SELECT.md](SELECT.md) | SELECT statement |
| [INSERT.md](INSERT.md) | INSERT statement |
| [UPDATE.md](UPDATE.md) | UPDATE statement |
| [DELETE.md](DELETE.md) | DELETE statement |
| [WITH_CTE.md](WITH_CTE.md) | WITH clause / Common Table Expressions |
| [UPSERT.md](UPSERT.md) | INSERT OR ... / ON CONFLICT DO UPDATE |
| [RETURNING.md](RETURNING.md) | RETURNING clause |
| [WINDOW_FUNCTIONS.md](WINDOW_FUNCTIONS.md) | Window functions reference |
| [WINDOW_FUNCTIONS_LANG.md](WINDOW_FUNCTIONS_LANG.md) | Window function SQL syntax |
| [SAVEPOINTS.md](SAVEPOINTS.md) | SAVEPOINT, RELEASE, ROLLBACK TO |
| [TRANSACTIONS.md](TRANSACTIONS.md) | BEGIN / COMMIT / ROLLBACK |
| [EXPRESSIONS.md](EXPRESSIONS.md) | Expression syntax |
| [EXPLAIN.md](EXPLAIN.md) | EXPLAIN statement |
| [ANALYZE.md](ANALYZE.md) | ANALYZE statement |
| [VACUUM.md](VACUUM.md) | VACUUM statement |
| [REINDEX.md](REINDEX.md) | REINDEX statement |
| [VALUES.md](VALUES.md) | VALUES clause |
| [COMMENTS.md](COMMENTS.md) | SQL comment syntax |
| [INDEXED_BY.md](INDEXED_BY.md) | INDEXED BY hint |

## DDL -- Data Definition

| File | Description |
|---|---|
| [CREATE_TABLE.md](CREATE_TABLE.md) | CREATE TABLE |
| [CREATE_INDEX.md](CREATE_INDEX.md) | CREATE INDEX |
| [CREATE_VIEW.md](CREATE_VIEW.md) | CREATE VIEW |
| [CREATE_TRIGGER.md](CREATE_TRIGGER.md) | CREATE TRIGGER |
| [CREATE_VTAB.md](CREATE_VTAB.md) | CREATE VIRTUAL TABLE |
| [ALTER_TABLE.md](ALTER_TABLE.md) | ALTER TABLE |
| [DROP_TABLE.md](DROP_TABLE.md) | DROP TABLE |
| [DROP_INDEX.md](DROP_INDEX.md) | DROP INDEX |
| [DROP_VIEW.md](DROP_VIEW.md) | DROP VIEW |
| [DROP_TRIGGER.md](DROP_TRIGGER.md) | DROP TRIGGER |
| [DETACH.md](DETACH.md) | DETACH DATABASE |
| [ATTACH.md](ATTACH.md) | ATTACH DATABASE |

## Data Types & Expressions

| File | Description |
|---|---|
| [DATATYPES.md](DATATYPES.md) | SQLite type system (primary reference) |
| [DATATYPES_LEGACY.md](DATATYPES_LEGACY.md) | Legacy datatypes doc |
| [TYPE_AFFINITY.md](TYPE_AFFINITY.md) | Type affinity overview |
| [TYPE_AFFINITY_SHORT.md](TYPE_AFFINITY_SHORT.md) | Affinity quick reference |
| [TYPE_AFFINITY_CASE1.md](TYPE_AFFINITY_CASE1.md) | Affinity case examples |
| [FLEXIBLE_TYPING.md](FLEXIBLE_TYPING.md) | Why flexible typing is good |
| [FLOATING_POINT.md](FLOATING_POINT.md) | Floating point behavior |
| [NULLS.md](NULLS.md) | NULL handling |

## Built-in Functions

| File | Description |
|---|---|
| [CORE_FUNCTIONS.md](CORE_FUNCTIONS.md) | Core scalar functions |
| [AGGREGATE_FUNCTIONS.md](AGGREGATE_FUNCTIONS.md) | Aggregate functions |
| [LANG_MATHFUNC.md](LANG_MATHFUNC.md) | Math functions |
| [LANG_DATEFUNC.md](LANG_DATEFUNC.md) | Date/time functions |
| [DETERMINISTIC_FUNCTIONS.md](DETERMINISTIC_FUNCTIONS.md) | Deterministic function semantics |

## Configuration (PRAGMA)

| File | Description |
|---|---|
| [PRAGMA_REFERENCE.md](PRAGMA_REFERENCE.md) | Complete PRAGMA reference |

## Advanced SQL Features

| File | Description |
|---|---|
| [FOREIGN_KEYS.md](FOREIGN_KEYS.md) | Foreign key support |
| [EXPRESSION_INDEXES.md](EXPRESSION_INDEXES.md) | Expression-based indexes |
| [PARTIAL_INDEXES.md](PARTIAL_INDEXES.md) | Partial (filtered) indexes |
| [WITHOUT_ROWID.md](WITHOUT_ROWID.md) | WITHOUT ROWID tables |
| [ROWID_TABLE.md](ROWID_TABLE.md) | Rowid tables |
| [STRICT_TABLES.md](STRICT_TABLES.md) | STRICT mode tables |
| [GENERATED_COLUMNS.md](GENERATED_COLUMNS.md) | Generated/computed columns |
| [AUTOINCREMENT.md](AUTOINCREMENT.md) | AUTOINCREMENT behavior |
| [CONFLICT_HANDLING.md](CONFLICT_HANDLING.md) | ON CONFLICT clauses |
| [CONFLICT_RESOLUTION.md](CONFLICT_RESOLUTION.md) | Conflict resolution algorithms |
| [LANG_CONFLICT.md](LANG_CONFLICT.md) | Conflict clause language ref |
| [LANG_REPLACE.md](LANG_REPLACE.md) | REPLACE statement |

## Extensions

| File | Description |
|---|---|
| [FTS5.md](FTS5.md) | Full-text search (FTS5) |
| [FTS3.md](FTS3.md) | Full-text search (FTS3/FTS4) |
| [JSON1.md](JSON1.md) | JSON1 extension |
| [RTREE.md](RTREE.md) | R-Tree spatial index |
| [GEOPOLY.md](GEOPOLY.md) | GeoPoly extension |
| [CSV.md](CSV.md) | CSV virtual table |
| [CARRAY.md](CARRAY.md) | Carray extension |
| [SWARMVTAB.md](SWARMVTAB.md) | Swarmvtab extension |
| [COMPLETION_VTAB.md](COMPLETION_VTAB.md) | Completion virtual table |
| [BYTECODE_VTAB.md](BYTECODE_VTAB.md) | Bytecode virtual table |
| [TCL_INTERFACE.md](TCL_INTERFACE.md) | Tcl interface |
| [BASE64.md](BASE64.md) | Base64 extension |
| [BASE85.md](BASE85.md) | Base85 extension |
| [VIRTUAL_TABLES_SPEC.md](VIRTUAL_TABLES_SPEC.md) | Virtual table architecture |

## Internals

| File | Description |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | SQLite architecture |
| [HOW_IT_WORKS.md](HOW_IT_WORKS.md) | How SQLite works |
| [FILE_FORMAT.md](FILE_FORMAT.md) | Database file format |
| [FILE_FORMAT_SPEC.md](FILE_FORMAT_SPEC.md) | File format specification |
| [WAL_SPEC.md](WAL_SPEC.md) | Write-ahead logging |
| [VDBE_OPCODES.md](VDBE_OPCODES.md) | VDBE opcode reference |
| [LOCKING.md](LOCKING.md) | File locking and concurrency |
| [ISOLATION.md](ISOLATION.md) | Transaction isolation |
| [ATOMIC_COMMIT.md](ATOMIC_COMMIT.md) | Atomic commit mechanism |
| [SCHEMA_TABLE.md](SCHEMA_TABLE.md) | sqlite_schema table |
| [DBPAGE.md](DBPAGE.md) | dbpage virtual table |
| [DBSTAT.md](DBSTAT.md) | dbstat virtual table |
| [DBHASH.md](DBHASH.md) | dbhash utility |
| [SHARED_CACHE.md](SHARED_CACHE.md) | Shared cache mode |
| [TEMP_FILES.md](TEMP_FILES.md) | Temporary files |
| [UNLOCK_NOTIFY.md](UNLOCK_NOTIFY.md) | Unlock notification API |

## Operations

| File | Description |
|---|---|
| [BACKUP_API.md](BACKUP_API.md) | Online backup API |
| [CLI.md](CLI.md) | Command-line shell |
| [IN_MEMORY_DB.md](IN_MEMORY_DB.md) | In-memory databases |
| [URI_FILENAMES.md](URI_FILENAMES.md) | URI filename format |
| [ASYNC_VFS.md](ASYNC_VFS.md) | Asynchronous VFS |
| [CHECKSUM_VFS.md](CHECKSUM_VFS.md) | Checksum VFS |
| [IMPOSTER.md](IMPOSTER.md) | Imposter tables |

## Reference

| File | Description |
|---|---|
| [RESULT_CODES.md](RESULT_CODES.md) | Error/result codes |
| [LIMITS.md](LIMITS.md) | Compile-time limits |
| [QUIRKS.md](QUIRKS.md) | SQLite quirks and gotchas |
| [SQLITE_DIFFERENCES.md](SQLITE_DIFFERENCES.md) | How SQLite differs from other SQL |
| [FAQ.md](FAQ.md) | Frequently asked questions |
| [FEATURES.md](FEATURES.md) | Feature list |
| [FULL_SQL.md](FULL_SQL.md) | Full SQL feature set |
| [KEYWORD_INDEX.md](KEYWORD_INDEX.md) | Keyword index |
| [LANG_KEYWORDS.md](LANG_KEYWORDS.md) | Reserved keywords |
| [GLOSSARY.md](GLOSSARY.md) | Glossary |
| [ERROR_LOG.md](ERROR_LOG.md) | Error logging |
| [EXPLAIN_QUERY_PLAN.md](EXPLAIN_QUERY_PLAN.md) | EXPLAIN QUERY PLAN |
| [QUERY_OPTIMIZER.md](QUERY_OPTIMIZER.md) | Query optimizer overview |
| [QUERY_PLANNER.md](QUERY_PLANNER.md) | Query planner details |
| [VERSION.md](VERSION.md) | Version information |
| [VERSION_NUMBERS.md](VERSION_NUMBERS.md) | Version numbering |
| [SECURITY.md](SECURITY.md) | Security considerations |
| [CVES.md](CVES.md) | CVE history |
| [DEBUGGING.md](DEBUGGING.md) | Debugging SQLite |
| [TESTING.md](TESTING.md) | SQLite testing |
| [TRANSACTIONAL.md](TRANSACTIONAL.md) | Transactional behavior |
| [SERVERLESS.md](SERVERLESS.md) | Serverless usage |
| [WHEN_TO_USE.md](WHEN_TO_USE.md) | When to use SQLite |
| [FASTER_THAN_FS.md](FASTER_THAN_FS.md) | SQLite faster than filesystem |
| [ABOUT.md](ABOUT.md) | About SQLite |

## Build & Deployment

| File | Description |
|---|---|
| [COMPILE.md](COMPILE.md) | Compilation guide |
| [AMALGAMATION.md](AMALGAMATION.md) | Amalgamation build |
| [CUSTOM_BUILD.md](CUSTOM_BUILD.md) | Custom build options |
| [FOOTPRINT.md](FOOTPRINT.md) | Memory footprint |
| [THREAD_SAFETY.md](THREAD_SAFETY.md) | Thread safety modes |
| [GET_THE_CODE.md](GET_THE_CODE.md) | Obtaining source code |
| [APP_FILE_FORMAT.md](APP_FILE_FORMAT.md) | Using SQLite as app file format |
| [BIND_POINTER.md](BIND_POINTER.md) | Pointer binding |
| [C_API_INTRO.md](C_API_INTRO.md) | C API introduction |
| [C_API_REFERENCE.md](C_API_REFERENCE.md) | C API reference |
| [C_INTERFACE.md](C_INTERFACE.md) | C interface docs |
| [C_INTRO.md](C_INTRO.md) | C intro (summary) |

## C API Reference

The [c-api/](c-api/) directory contains 42 individual C API function pages:

- [c-api/API_INDEX.md](c-api/API_INDEX.md) -- complete API listing
- Individual function pages: `open.md`, `close.md`, `exec.md`, `prepare.md`, `step.md`, `finalize.md`, `bind_blob.md`, `column_text.md`, `errcode.md`, `create_function.md`, and more.

## Session Extension

The [session/](session/) directory contains **51 files** covering the SQLite session extension
for changesets, patchsets, and conflict resolution for database synchronization.

## SQL Syntax Diagrams

The [syntax/](syntax/) directory contains **71 files** with railroad diagram syntax for
every SQL construct: expressions, statements, clauses, and grammar rules.

---

*SQLite 3.51.2 documentation -- converted from official HTML docs.*
*Source: https://www.sqlite.org/docs.html*
