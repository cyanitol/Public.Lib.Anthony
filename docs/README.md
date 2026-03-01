# Anthony SQLite - Documentation

Welcome to the Anthony SQLite documentation. This is a pure Go implementation of SQLite, providing a complete SQL database engine without CGO dependencies.

## Getting Started

Start here if you're new to Anthony:

- [QUICKSTART.md](QUICKSTART.md) - Quick start guide with code examples
- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture and design overview
- [INDEX.md](INDEX.md) - Complete documentation index with all package references

## Architecture

Core architecture and system design documentation:

- [ARCHITECTURE.md](ARCHITECTURE.md) - High-level architecture, data flow, and design decisions
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency control and lock ordering hierarchy

### Internal Package Documentation

Detailed documentation for each internal package:

- [internal/btree/](internal/btree/) - B-tree storage engine
- [internal/pager/](internal/pager/) - Page cache and transaction management
- [internal/parser/](internal/parser/) - SQL lexer, parser, and AST
- [internal/planner/](internal/planner/) - Query optimizer
- [internal/sql/](internal/sql/) - SQL statement compilation
- [internal/vdbe/](internal/vdbe/) - Virtual Database Engine (bytecode VM)
- [internal/expr/](internal/expr/) - Expression evaluation
- [internal/functions/](internal/functions/) - Built-in SQL functions
- [internal/driver/](internal/driver/) - database/sql driver interface
- [internal/engine/](internal/engine/) - Query execution engine
- [internal/schema/](internal/schema/) - Database schema management
- [internal/constraint/](internal/constraint/) - SQL constraint enforcement
- [internal/format/](internal/format/) - SQLite file format utilities
- [internal/utf/](internal/utf/) - UTF-8/UTF-16 encoding and collation
- [internal/collation/](internal/collation/) - Collation sequences
- [internal/vtab/](internal/vtab/) - Virtual table support

## Security

Security model, controls, and best practices:

- [SECURITY.md](SECURITY.md) - Comprehensive security guide
  - Layered security architecture (Pattern blocking, Sandbox, Allowlist, File permissions)
  - SecurityConfig configuration and examples
  - Input validation and resource limits
  - Integer safety and buffer safety
  - Concurrency safety and best practices
  - Security testing and vulnerability reporting
- [SECURITY_AUDIT_PLAN.md](SECURITY_AUDIT_PLAN.md) - Security audit planning document
- [SECURITY_AUDIT_IMPLEMENTATION.md](SECURITY_AUDIT_IMPLEMENTATION.md) - Security implementation details
- [SECURITY_AUDIT_REPORT.md](SECURITY_AUDIT_REPORT.md) - Security audit report and findings
- [SECURITY_FIXES_SUMMARY.md](SECURITY_FIXES_SUMMARY.md) - Summary of security fixes implemented

## Code Quality

Code quality reports and analysis:

- [CYCLOMATIC_COMPLEXITY_REPORT.md](CYCLOMATIC_COMPLEXITY_REPORT.md) - Cyclomatic complexity analysis and reduction efforts

## Development

Guides for contributors and developers:

### SQL Feature Implementation

#### Common Table Expressions (CTEs)
- [CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md) - User guide with examples
- [CTE_IMPLEMENTATION_SUMMARY.md](CTE_IMPLEMENTATION_SUMMARY.md) - Complete implementation details
- [CTE_IMPLEMENTATION_STATUS.md](CTE_IMPLEMENTATION_STATUS.md) - Implementation status
- [CTE_QUICK_REFERENCE.md](CTE_QUICK_REFERENCE.md) - Quick syntax reference
- [CTE_AST_STRUCTURE.md](CTE_AST_STRUCTURE.md) - AST structure details

#### Subqueries
- [SUBQUERY_ARCHITECTURE.md](SUBQUERY_ARCHITECTURE.md) - Architecture and design
- [SUBQUERY_IMPLEMENTATION.md](SUBQUERY_IMPLEMENTATION.md) - Implementation guide
- [SUBQUERY_SUMMARY.md](SUBQUERY_SUMMARY.md) - Implementation summary
- [SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md) - Quick syntax reference
- [SUBQUERY_CHECKLIST.md](SUBQUERY_CHECKLIST.md) - Development checklist

#### Compound SELECT Operations
- [COMPOUND_SELECT_QUICK_REFERENCE.md](COMPOUND_SELECT_QUICK_REFERENCE.md) - UNION, INTERSECT, EXCEPT reference
- [COMPOUND_SELECT_BYTECODE_FLOW.md](COMPOUND_SELECT_BYTECODE_FLOW.md) - Bytecode implementation
- [INTERSECT_EXCEPT_IMPLEMENTATION.md](INTERSECT_EXCEPT_IMPLEMENTATION.md) - INTERSECT/EXCEPT implementation
- [INTERSECT_EXCEPT_SUMMARY.md](INTERSECT_EXCEPT_SUMMARY.md) - Implementation summary

### DDL and Database Operations

- [DDL_IMPLEMENTATION_REPORT.md](DDL_IMPLEMENTATION_REPORT.md) - DDL operations implementation
- [ALTER_TABLE_QUICK_REFERENCE.md](ALTER_TABLE_QUICK_REFERENCE.md) - ALTER TABLE reference
- [ATTACH_DETACH_IMPLEMENTATION.md](ATTACH_DETACH_IMPLEMENTATION.md) - Multi-database support
- [PRAGMA_IMPLEMENTATION_SUMMARY.md](PRAGMA_IMPLEMENTATION_SUMMARY.md) - PRAGMA implementation
- [PRAGMA_QUICK_REFERENCE.md](PRAGMA_QUICK_REFERENCE.md) - PRAGMA quick reference

### Advanced Features

- [TRIGGER_INTEGRATION_REPORT.md](TRIGGER_INTEGRATION_REPORT.md) - Trigger support implementation
- [VACUUM_IMPLEMENTATION.md](VACUUM_IMPLEMENTATION.md) - VACUUM implementation details
- [VACUUM_USAGE.md](VACUUM_USAGE.md) - VACUUM usage guide

## API Reference

For detailed API documentation, see:

- [INDEX.md](INDEX.md) - Complete package and feature index
- Individual package READMEs in [internal/*/README.md](internal/)

## Documentation Categories

### By Topic

#### Query Processing
- CTEs: CTE_*.md files
- Subqueries: SUBQUERY_*.md files
- Compound SELECT: COMPOUND_SELECT_*.md, INTERSECT_EXCEPT_*.md

#### Data Definition
- ALTER TABLE: ALTER_TABLE_QUICK_REFERENCE.md
- PRAGMA: PRAGMA_*.md
- ATTACH/DETACH: ATTACH_DETACH_IMPLEMENTATION.md

#### System Operations
- VACUUM: VACUUM_*.md
- Triggers: TRIGGER_INTEGRATION_REPORT.md

#### Security
- Security model: SECURITY.md
- Security audit: SECURITY_AUDIT_*.md
- Lock ordering: LOCK_ORDERING.md

### By Audience

#### Users
- QUICKSTART.md - Get started quickly
- CTE_USAGE_GUIDE.md - Learn CTE syntax
- VACUUM_USAGE.md - Database maintenance
- *_QUICK_REFERENCE.md - Syntax references

#### Developers
- ARCHITECTURE.md - Understand the system
- *_IMPLEMENTATION*.md - Implementation details
- *_CHECKLIST.md - Development checklists
- Package READMEs - API documentation

#### Contributors
- SECURITY.md - Security guidelines
- LOCK_ORDERING.md - Concurrency rules
- DDL_IMPLEMENTATION_REPORT.md - Feature status

## Document Types

### Quick References
Concise syntax and usage guides:
- ALTER_TABLE_QUICK_REFERENCE.md
- CTE_QUICK_REFERENCE.md
- COMPOUND_SELECT_QUICK_REFERENCE.md
- PRAGMA_QUICK_REFERENCE.md
- SUBQUERY_QUICK_REFERENCE.md

### Usage Guides
Detailed user documentation with examples:
- QUICKSTART.md
- CTE_USAGE_GUIDE.md
- VACUUM_USAGE.md

### Implementation Documents
Technical implementation details:
- CTE_IMPLEMENTATION_SUMMARY.md
- SUBQUERY_IMPLEMENTATION.md
- INTERSECT_EXCEPT_IMPLEMENTATION.md
- ATTACH_DETACH_IMPLEMENTATION.md
- PRAGMA_IMPLEMENTATION_SUMMARY.md
- VACUUM_IMPLEMENTATION.md

### Status Reports
Implementation status and checklists:
- CTE_IMPLEMENTATION_STATUS.md
- DDL_IMPLEMENTATION_REPORT.md
- SUBQUERY_CHECKLIST.md
- TRIGGER_INTEGRATION_REPORT.md

### Architecture Documents
System design and structure:
- ARCHITECTURE.md
- SUBQUERY_ARCHITECTURE.md
- CTE_AST_STRUCTURE.md
- COMPOUND_SELECT_BYTECODE_FLOW.md

### Summary Documents
High-level overviews:
- CTE_IMPLEMENTATION_SUMMARY.md
- SUBQUERY_SUMMARY.md
- INTERSECT_EXCEPT_SUMMARY.md
- PRAGMA_IMPLEMENTATION_SUMMARY.md

## Naming Conventions

Documentation files follow these naming patterns:

- `*_QUICK_REFERENCE.md` - Concise syntax references
- `*_USAGE*.md` - User guides with examples
- `*_IMPLEMENTATION*.md` - Technical implementation details
- `*_SUMMARY.md` - High-level overviews
- `*_CHECKLIST.md` - Development task lists
- `*_STATUS.md` - Implementation status tracking
- `*_REPORT.md` - Detailed status reports
- `*_ARCHITECTURE.md` - Architectural documentation
- `*_STRUCTURE.md` - Structure and format documentation
- `*_FLOW.md` - Process and data flow documentation

## Testing

Each package includes comprehensive tests. See individual package READMEs for testing details.

Run all tests:
```bash
go test ./...
go test -race ./...  # With race detector
go test -cover ./...  # With coverage
```

## Contributing

When contributing to Anthony:

1. Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system
2. Follow the security guidelines in [SECURITY.md](SECURITY.md)
3. Understand the lock ordering in [LOCK_ORDERING.md](LOCK_ORDERING.md)
4. Write tests for all new features
5. Keep cyclomatic complexity <= 10
6. Document all public APIs
7. Update relevant documentation

## License

This project is in the public domain (SQLite License).

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.

## Local SQLite Reference Documentation

Complete SQLite 3.51.2 documentation is available offline in [sqlite/README.md](sqlite/README.md).
Over 200 reference files covering SQL language, functions, pragmas, extensions, and internals.

Quick links:
- [SQL Language Reference](sqlite/SQL_LANGUAGE_OVERVIEW.md)
- [PRAGMA Reference](sqlite/PRAGMA_REFERENCE.md)
- [Core Functions](sqlite/CORE_FUNCTIONS.md)
- [WAL Specification](sqlite/WAL_SPEC.md)
- [File Format](sqlite/FILE_FORMAT_SPEC.md)
- [Full index →](sqlite/README.md)

## External Resources

- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite SQL Syntax](https://www.sqlite.org/lang.html)
- [SQLite Opcodes](https://www.sqlite.org/opcode.html)
- [Go database/sql](https://pkg.go.dev/database/sql)
