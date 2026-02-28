# Anthony SQLite - Documentation Index

This is the complete documentation index for the Anthony pure Go SQLite implementation.

## Getting Started

- **[QUICKSTART.md](QUICKSTART.md)** - Quick start guide for using Anthony SQLite
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Overall architecture and design
- **[README (root)](../README.md)** - Project overview and installation

## Core Package Documentation

### Storage Layer

- **[btree](internal/btree/README.md)** - B-tree storage engine
  - Variable-length integer encoding
  - Page structure and cell parsing
  - Cursor operations and navigation
  - Page splitting and tree growth
  - Integrity checking

- **[pager](internal/pager/README.md)** - Page cache, journal, and transaction management
  - Page caching and I/O
  - Transaction support
  - Write-Ahead Logging (WAL)
  - Lock management

- **[format](internal/format/README.md)** - SQLite file format utilities
  - Database file header
  - Page types and structures
  - Format validation

### Query Processing

- **[parser](internal/parser/README.md)** - SQL lexer, parser, and AST
  - Lexical analysis
  - Syntax parsing
  - Abstract Syntax Tree (AST)
  - SQL statement support

- **[planner](internal/planner/README.md)** - Query optimizer
  - Query plan generation
  - Cost-based optimization
  - Common Table Expressions (CTEs)
  - Join optimization

- **[sql](internal/sql/README.md)** - SQL statement compilation
  - SELECT, INSERT, UPDATE, DELETE
  - DDL operations (CREATE TABLE, DROP TABLE, ALTER TABLE)
  - Compound queries (UNION, INTERSECT, EXCEPT)
  - Subqueries

### Execution Engine

- **[vdbe](internal/vdbe/README.md)** - Virtual Database Engine (bytecode VM)
  - Bytecode instruction set
  - Memory cells and type system
  - Cursor operations
  - Program execution

- **[expr](internal/expr/README.md)** - Expression evaluation
  - Expression compilation to bytecode
  - Type conversions
  - Operator support
  - Code generation

- **[functions](internal/functions/README.md)** - Built-in SQL functions
  - Scalar functions (string, math, date)
  - Aggregate functions (SUM, COUNT, AVG, etc.)
  - Function registration and execution

### Database Interface

- **[driver](internal/driver/README.md)** - database/sql driver interface
  - Go database/sql compatibility
  - Connection management
  - Statement preparation and execution
  - Transaction support

- **[engine](internal/engine/README.md)** - Query execution engine
  - Query coordination
  - Result set management
  - Error handling

### Data Management

- **[schema](internal/schema/README.md)** - Database schema management
  - Table and column definitions
  - Type affinity
  - sqlite_master table
  - Index management

- **[constraint](internal/constraint/README.md)** - SQL constraint enforcement
  - CHECK constraints
  - UNIQUE constraints
  - Collation sequences
  - Constraint validation

### Utilities

- **[utf](internal/utf/README.md)** - UTF-8/UTF-16 encoding and collation
  - Character encoding conversion
  - String comparison
  - Unicode support

- **[collation](internal/collation/README.md)** - Collation sequences
  - Built-in collations (BINARY, NOCASE, RTRIM)
  - Custom collation registration
  - String comparison functions

- **[vtab](internal/vtab/README.md)** - Virtual table support
  - Virtual table modules
  - Cursor interface
  - Index planning

## Feature Documentation

### SQL Features

- **[Common Table Expressions](CTE_USAGE_GUIDE.md)** - WITH clause and CTEs
- **[Subqueries](SUBQUERY_ARCHITECTURE.md)** - Subquery implementation
- **[Triggers](TRIGGER_INTEGRATION_REPORT.md)** - Trigger support
- **[VACUUM](VACUUM_USAGE.md)** - Database compaction

### DDL Operations

- **[ALTER TABLE](ALTER_TABLE_QUICK_REFERENCE.md)** - ALTER TABLE operations
- **[ATTACH/DETACH](ATTACH_DETACH_IMPLEMENTATION.md)** - Multi-database support
- **[PRAGMA](PRAGMA_QUICK_REFERENCE.md)** - Database configuration

### DML Operations

- **[Compound SELECT](COMPOUND_SELECT_QUICK_REFERENCE.md)** - UNION, INTERSECT, EXCEPT
- **[INSERT/UPDATE/DELETE](DDL_IMPLEMENTATION_REPORT.md)** - Data modification

### System Operations

- **[EXPLAIN](PHASE2_EXPLAIN_SUMMARY.md)** - Query plan explanation
- **[Lock Ordering](LOCK_ORDERING.md)** - Concurrency control

## Implementation Status

### Phase 1 - Core Infrastructure
- B-tree storage engine
- Page management
- Basic SQL parsing
- VDBE bytecode engine

### Phase 2 - SQL Features (Current)
- Comprehensive constraint support
- Advanced SELECT features (CTEs, subqueries)
- Transaction management
- Full DDL support

### Phase 3 - Advanced Features (Planned)
- Triggers
- Views
- Full-text search
- Virtual tables

## Development Documentation

### Implementation Guides
- **[CTE Implementation Status](CTE_IMPLEMENTATION_STATUS.md)**
- **[Subquery Implementation](SUBQUERY_IMPLEMENTATION.md)**
- **[Compound SELECT Implementation](COMPOUND_SELECT_BYTECODE_FLOW.md)**

### Checklists
- **[Subquery Checklist](SUBQUERY_CHECKLIST.md)**
- **[DDL Implementation Report](DDL_IMPLEMENTATION_REPORT.md)**

## Testing

Each package includes comprehensive tests. Run all tests:

```bash
# All tests
go test ./...

# Specific package
go test ./internal/btree/...
go test ./internal/vdbe/...

# With coverage
go test -cover ./...

# Verbose output
go test -v ./internal/parser/...
```

## Contributing

When adding new features or fixing bugs:

1. Read the relevant package documentation
2. Understand the SQLite specification
3. Write tests first (TDD approach)
4. Keep cyclomatic complexity ≤ 10
5. Document all public APIs
6. Update this INDEX.md if adding new documentation

## References

### SQLite Documentation
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite SQL Syntax](https://www.sqlite.org/lang.html)
- [SQLite Opcodes](https://www.sqlite.org/opcode.html)
- [SQLite Internals](https://www.sqlite.org/arch.html)

### Books and Papers
- Knuth, "The Art of Computer Programming, Volume 3"
- Bayer & McCreight, "Organization and Maintenance of Large Ordered Indexes"
- SQLite source code (public domain)

## License

This project is in the public domain (SQLite License).

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.
