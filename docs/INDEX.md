# Anthony SQLite - Documentation Index

This is the complete documentation index for the Anthony pure Go SQLite implementation.

## Getting Started

- **[QUICKSTART.md](QUICKSTART.md)** - Quick start guide with code examples
- **[API.md](API.md)** - Complete public API documentation and usage guide
- **[README.md](README.md)** - Documentation overview and navigation guide
- **[README (root)](../README.md)** - Project overview and installation
- **[CONTRIBUTING (root)](../CONTRIBUTING.md)** - Contributor guidelines and development workflow

## Architecture

Core architecture and system design documentation:

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - High-level architecture, component overview, and design decisions
- **[LOCK_ORDERING.md](LOCK_ORDERING.md)** - Concurrency control and lock ordering hierarchy for thread safety

## Security

Security model, controls, and best practices:

- **[SECURITY.md](SECURITY.md)** - Comprehensive security guide
  - Layered security architecture (Pattern blocking, Sandbox, Allowlist, File permissions)
  - SecurityConfig configuration and examples
  - Input validation and resource limits
  - Integer safety and buffer safety
  - Concurrency safety and best practices
  - Security testing and vulnerability reporting
- **[SECURITY_AUDIT_PLAN.md](SECURITY_AUDIT_PLAN.md)** - Security audit planning document
- **[SECURITY_AUDIT_IMPLEMENTATION.md](SECURITY_AUDIT_IMPLEMENTATION.md)** - Security implementation details and audit results

## SQL Features

### Query Processing Features

#### Common Table Expressions (CTEs)
- **[CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md)** - User guide with examples and best practices
- **[CTE_IMPLEMENTATION_SUMMARY.md](CTE_IMPLEMENTATION_SUMMARY.md)** - Complete implementation details
- **[CTE_QUICK_REFERENCE.md](CTE_QUICK_REFERENCE.md)** - Quick syntax reference
- **[CTE_AST_STRUCTURE.md](CTE_AST_STRUCTURE.md)** - AST structure and parsing details

#### Subqueries
- **[SUBQUERY_ARCHITECTURE.md](SUBQUERY_ARCHITECTURE.md)** - Architecture and design patterns
- **[SUBQUERY_IMPLEMENTATION.md](SUBQUERY_IMPLEMENTATION.md)** - Implementation guide and technical details
- **[SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md)** - Quick syntax reference
- **[SUBQUERY_CHECKLIST.md](SUBQUERY_CHECKLIST.md)** - Development checklist and status

#### Compound SELECT Operations
- **[COMPOUND_SELECT_QUICK_REFERENCE.md](COMPOUND_SELECT_QUICK_REFERENCE.md)** - UNION, INTERSECT, EXCEPT reference
- **[COMPOUND_SELECT_BYTECODE_FLOW.md](COMPOUND_SELECT_BYTECODE_FLOW.md)** - Bytecode implementation and execution flow
- **[INTERSECT_EXCEPT_IMPLEMENTATION.md](INTERSECT_EXCEPT_IMPLEMENTATION.md)** - INTERSECT/EXCEPT implementation details

### DDL Operations

- **[DDL_IMPLEMENTATION_REPORT.md](DDL_IMPLEMENTATION_REPORT.md)** - DDL operations implementation and status
- **[ALTER_TABLE_QUICK_REFERENCE.md](ALTER_TABLE_QUICK_REFERENCE.md)** - ALTER TABLE syntax and operations
- **[ATTACH_DETACH_IMPLEMENTATION.md](ATTACH_DETACH_IMPLEMENTATION.md)** - Multi-database support (ATTACH/DETACH)

### Database Operations

- **[PRAGMA_QUICK_REFERENCE.md](PRAGMA_QUICK_REFERENCE.md)** - PRAGMA quick reference and configuration
- **[PRAGMA_IMPLEMENTATION_SUMMARY.md](PRAGMA_IMPLEMENTATION_SUMMARY.md)** - PRAGMA implementation details
- **[VACUUM_USAGE.md](VACUUM_USAGE.md)** - VACUUM usage guide and best practices
- **[VACUUM_IMPLEMENTATION.md](VACUUM_IMPLEMENTATION.md)** - VACUUM implementation details

### Advanced Features

- **[TRIGGER_INTEGRATION_REPORT.md](TRIGGER_INTEGRATION_REPORT.md)** - Trigger support implementation and status

## Testing

- **[TESTING.md](TESTING.md)** - Comprehensive testing guide
  - Unit, integration, and security tests
  - Race detection and fuzz testing
  - Coverage goals and reporting
  - Running specific tests
  - Continuous integration guidelines
  - Troubleshooting and best practices

## Development

### Contributing

See **[CONTRIBUTING.md](../CONTRIBUTING.md)** for complete contributor guidelines.

When adding new features or fixing bugs:

1. Read the relevant package documentation
2. Understand the SQLite specification
3. Write tests first (TDD approach)
4. Keep cyclomatic complexity <= 10
5. Document all public APIs
6. Update this INDEX.md if adding new documentation
7. Follow security best practices in [SECURITY.md](SECURITY.md)
8. Review lock ordering requirements in [LOCK_ORDERING.md](LOCK_ORDERING.md)

### Documentation Organization

Documentation files follow these naming patterns:

- `*_QUICK_REFERENCE.md` - Concise syntax references
- `*_USAGE.md` - User guides with examples
- `*_IMPLEMENTATION*.md` - Technical implementation details
- `*_SUMMARY.md` - High-level overviews
- `*_CHECKLIST.md` - Development task lists
- `*_REPORT.md` - Detailed status reports
- `*_ARCHITECTURE.md` - Architectural documentation
- `*_STRUCTURE.md` - Structure and format documentation
- `*_FLOW.md` - Process and data flow documentation

### Internal Package Documentation

Note: Detailed package READMEs that were previously in docs/internal/ have been moved to the attic. For the most up-to-date package documentation, refer to the godoc comments in the source code or generate documentation using:

```bash
# Generate and view package documentation
go doc -all github.com/JuniperBible/Public.Lib.Anthony/internal/driver
go doc -all github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe
# etc.
```

**Core Packages:**
- `internal/driver` - database/sql driver interface
- `internal/engine` - Query execution engine
- `internal/parser` - SQL lexer, parser, and AST
- `internal/planner` - Query optimizer
- `internal/sql` - SQL statement compilation
- `internal/vdbe` - Virtual Database Engine (bytecode VM)
- `internal/expr` - Expression evaluation
- `internal/functions` - Built-in SQL functions
- `internal/btree` - B-tree storage engine
- `internal/pager` - Page cache and transaction management
- `internal/schema` - Database schema management
- `internal/constraint` - SQL constraint enforcement
- `internal/format` - SQLite file format utilities
- `internal/utf` - UTF-8/UTF-16 encoding and collation
- `internal/collation` - Collation sequences
- `internal/vtab` - Virtual table support
- `internal/security` - Security controls and validation

## Quick Reference by Use Case

### For Users

**Getting Started:**
- [QUICKSTART.md](QUICKSTART.md) - Start here for basic usage
- [API.md](API.md) - Complete API reference
- [COMPATIBILITY.md](COMPATIBILITY.md) - SQLite compatibility guide

**SQL Core Concepts:**
- [SQL_LANGUAGE.md](SQL_LANGUAGE.md) - Complete SQL syntax reference
- [TYPE_SYSTEM.md](TYPE_SYSTEM.md) - Type system and affinities
- [FILE_FORMAT.md](FILE_FORMAT.md) - Database file format
- [PRAGMAS.md](PRAGMAS.md) - PRAGMA commands guide

**SQL Features:**
- [CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md) - Common Table Expressions
- [VACUUM_USAGE.md](VACUUM_USAGE.md) - Database maintenance
- [CTE_QUICK_REFERENCE.md](CTE_QUICK_REFERENCE.md) - CTE syntax
- [SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md) - Subquery syntax
- [COMPOUND_SELECT_QUICK_REFERENCE.md](COMPOUND_SELECT_QUICK_REFERENCE.md) - UNION/INTERSECT/EXCEPT
- [ALTER_TABLE_QUICK_REFERENCE.md](ALTER_TABLE_QUICK_REFERENCE.md) - ALTER TABLE syntax
- [PRAGMA_QUICK_REFERENCE.md](PRAGMA_QUICK_REFERENCE.md) - PRAGMA quick reference

### For Developers

**Understanding the System:**
- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture overview
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency and thread safety
- [FILE_FORMAT.md](FILE_FORMAT.md) - Database file format and storage
- [TYPE_SYSTEM.md](TYPE_SYSTEM.md) - Type system implementation

**SQL Implementation:**
- [SQL_LANGUAGE.md](SQL_LANGUAGE.md) - SQL language implementation
- [PRAGMAS.md](PRAGMAS.md) - PRAGMA implementation details
- [COMPATIBILITY.md](COMPATIBILITY.md) - SQLite compatibility

**Feature Implementation Details:**
- [CTE_IMPLEMENTATION_SUMMARY.md](CTE_IMPLEMENTATION_SUMMARY.md) - CTE implementation
- [SUBQUERY_IMPLEMENTATION.md](SUBQUERY_IMPLEMENTATION.md) - Subquery implementation
- [INTERSECT_EXCEPT_IMPLEMENTATION.md](INTERSECT_EXCEPT_IMPLEMENTATION.md) - Set operations
- [ATTACH_DETACH_IMPLEMENTATION.md](ATTACH_DETACH_IMPLEMENTATION.md) - Multi-database support
- [PRAGMA_IMPLEMENTATION_SUMMARY.md](PRAGMA_IMPLEMENTATION_SUMMARY.md) - PRAGMA parsing
- [VACUUM_IMPLEMENTATION.md](VACUUM_IMPLEMENTATION.md) - VACUUM implementation
- [DDL_IMPLEMENTATION_REPORT.md](DDL_IMPLEMENTATION_REPORT.md) - DDL feature status

**Architecture Details:**
- [SUBQUERY_ARCHITECTURE.md](SUBQUERY_ARCHITECTURE.md) - Subquery design patterns
- [CTE_AST_STRUCTURE.md](CTE_AST_STRUCTURE.md) - CTE AST structure
- [COMPOUND_SELECT_BYTECODE_FLOW.md](COMPOUND_SELECT_BYTECODE_FLOW.md) - Compound query execution

**Development Tracking:**
- [SUBQUERY_CHECKLIST.md](SUBQUERY_CHECKLIST.md) - Subquery development checklist
- [TRIGGER_INTEGRATION_REPORT.md](TRIGGER_INTEGRATION_REPORT.md) - Trigger implementation status

### For Contributors

**Essential Reading:**
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contribution guidelines
- [SECURITY.md](SECURITY.md) - Security requirements
- [TESTING.md](TESTING.md) - Testing practices
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency rules

**Security:**
- [SECURITY_AUDIT_PLAN.md](SECURITY_AUDIT_PLAN.md) - Security audit scope
- [SECURITY_AUDIT_IMPLEMENTATION.md](SECURITY_AUDIT_IMPLEMENTATION.md) - Security implementation

## SQLite Core Documentation

Comprehensive documentation on SQLite fundamentals as implemented in Anthony:

- **[FILE_FORMAT.md](FILE_FORMAT.md)** - SQLite database file format for Go implementation
  - Database structure and organization
  - Page layout and B-tree storage
  - Record encoding and serialization
  - Header format and metadata
  - Transaction files (journal and WAL)
  - Go-specific implementation notes

- **[TYPE_SYSTEM.md](TYPE_SYSTEM.md)** - Type affinity and storage classes
  - Storage classes (NULL, INTEGER, REAL, TEXT, BLOB)
  - Type affinity system and rules
  - Type conversions and coercion
  - Column affinity determination
  - STRICT tables
  - Comparison and sorting behavior

- **[SQL_LANGUAGE.md](SQL_LANGUAGE.md)** - Supported SQL syntax reference
  - Data Definition Language (DDL)
  - Data Manipulation Language (DML)
  - Query language (SELECT, JOIN, etc.)
  - Expressions and operators
  - Built-in functions
  - Transactions
  - Go-specific usage examples

- **[PRAGMAS.md](PRAGMAS.md)** - PRAGMA commands reference
  - Database configuration PRAGMAs
  - Performance tuning options
  - Schema inspection commands
  - Integrity checking
  - Transaction control
  - Complete Go usage examples

- **[COMPATIBILITY.md](COMPATIBILITY.md)** - SQLite compatibility status
  - Feature compatibility matrix
  - Known differences from SQLite
  - File format compatibility
  - Migration guide
  - Testing compatibility
  - Version compatibility matrix

## References

### SQLite Documentation
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite SQL Syntax](https://www.sqlite.org/lang.html)
- [SQLite Datatypes](https://www.sqlite.org/datatype3.html)
- [SQLite Pragma Statements](https://www.sqlite.org/pragma.html)
- [SQLite Opcodes](https://www.sqlite.org/opcode.html)
- [SQLite Internals](https://www.sqlite.org/arch.html)

### Books and Papers
- Knuth, "The Art of Computer Programming, Volume 3"
- Bayer & McCreight, "Organization and Maintenance of Large Ordered Indexes"
- SQLite source code (public domain)

### Go Documentation
- [Go database/sql package](https://pkg.go.dev/database/sql)
- [Go testing package](https://pkg.go.dev/testing)
- [Go fuzzing](https://go.dev/doc/fuzz/)

## SQLite Reference Documentation

Complete SQLite 3.51.2 reference, converted to Markdown from official HTML docs.
See [sqlite/README.md](sqlite/README.md) for the full index (305 files).

### SQL Language

[SELECT](sqlite/SELECT.md) · [INSERT](sqlite/INSERT.md) · [UPDATE](sqlite/UPDATE.md) ·
[DELETE](sqlite/DELETE.md) · [WITH](sqlite/WITH_CTE.md) · [UPSERT](sqlite/UPSERT.md) ·
[RETURNING](sqlite/RETURNING.md) · [Window Functions](sqlite/WINDOW_FUNCTIONS.md) ·
[Savepoints](sqlite/SAVEPOINTS.md) · [Transactions](sqlite/TRANSACTIONS.md)

### Reference

[PRAGMA](sqlite/PRAGMA_REFERENCE.md) · [Functions](sqlite/CORE_FUNCTIONS.md) ·
[Aggregate](sqlite/AGGREGATE_FUNCTIONS.md) · [Math](sqlite/LANG_MATHFUNC.md) ·
[Datatypes](sqlite/DATATYPES.md) · [Result Codes](sqlite/RESULT_CODES.md) ·
[Limits](sqlite/LIMITS.md) · [Quirks](sqlite/QUIRKS.md) · [NULL Handling](sqlite/NULLS.md)

### Extensions

[FTS5](sqlite/FTS5.md) · [FTS3](sqlite/FTS3.md) · [JSON1](sqlite/JSON1.md) ·
[RTree](sqlite/RTREE.md) · [GeoPoly](sqlite/GEOPOLY.md)

### Internals

[Architecture](sqlite/ARCHITECTURE.md) · [File Format](sqlite/FILE_FORMAT_SPEC.md) ·
[WAL](sqlite/WAL_SPEC.md) · [VDBE Opcodes](sqlite/VDBE_OPCODES.md) ·
[Locking](sqlite/LOCKING.md) · [Isolation](sqlite/ISOLATION.md)

### C API & Syntax

[C API Reference](sqlite/c-api/) · [Session Extension](sqlite/session/) ·
[SQL Syntax Diagrams](sqlite/syntax/)

## License

This project is in the public domain (SQLite License).

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.
