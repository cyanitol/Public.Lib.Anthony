# Anthony SQLite - Documentation

A pure Go implementation of SQLite, providing a complete SQL database engine without CGO dependencies.

## Getting Started

- [GETTING_STARTED.md](GETTING_STARTED.md) - Installation and first steps
- [USER_GUIDE.md](USER_GUIDE.md) - Comprehensive user guide
- [API.md](API.md) - Public API documentation

## Architecture

- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture and design
- [FILE_FORMAT.md](FILE_FORMAT.md) - SQLite file format
- [WAL_MODE.md](WAL_MODE.md) - Write-Ahead Logging
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency and lock hierarchy
- [INTERNAL_API.md](INTERNAL_API.md) - Internal package APIs

## SQL Features

- [SQL_LANGUAGE.md](SQL_LANGUAGE.md) - SQL syntax overview
- [CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md) - Common Table Expressions
- [SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md) - Subqueries
- [COMPOUND_SELECT_QUICK_REFERENCE.md](COMPOUND_SELECT_QUICK_REFERENCE.md) - UNION/INTERSECT/EXCEPT
- [DDL_FEATURES.md](DDL_FEATURES.md) - CREATE, ALTER, DROP
- [TRIGGERS.md](TRIGGERS.md) - Trigger support
- [VIRTUAL_TABLES.md](VIRTUAL_TABLES.md) - Virtual tables
- [EXPRESSION_INDEXES.md](EXPRESSION_INDEXES.md) - Expression indexes
- [VACUUM_USAGE.md](VACUUM_USAGE.md) - Database maintenance
- [EXPLAIN_QUICK_REFERENCE.md](EXPLAIN_QUICK_REFERENCE.md) - Query analysis
- [ATTACH_DETACH_IMPLEMENTATION.md](ATTACH_DETACH_IMPLEMENTATION.md) - Multi-database

## Reference

- [FUNCTIONS.md](FUNCTIONS.md) - Built-in SQL functions
- [JSON_FUNCTIONS.md](JSON_FUNCTIONS.md) - JSON functions
- [PRAGMAS.md](PRAGMAS.md) - PRAGMA commands
- [TYPE_SYSTEM.md](TYPE_SYSTEM.md) - Type system and affinity
- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Error handling
- [COMPATIBILITY.md](COMPATIBILITY.md) - SQLite compatibility

## Development

- [TESTING.md](TESTING.md) - Testing strategy
- [TESTING_INFRASTRUCTURE.md](TESTING_INFRASTRUCTURE.md) - Test infrastructure
- [SECURITY.md](SECURITY.md) - Security model

Development roadmaps are in [planning/](planning/).

## SQLite Reference

SQL language reference from SQLite 3.51.2 is in [sqlite/](sqlite/).

## License

Public domain (SQLite License).
