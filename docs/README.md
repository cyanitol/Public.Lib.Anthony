# Anthony SQLite - Documentation

Welcome to the Anthony SQLite documentation. This is a pure Go implementation of SQLite, providing a complete SQL database engine without CGO dependencies.

## Table of Contents

- [Introduction](#introduction)
- [Getting Started](#getting-started)
- [Core Documentation](#core-documentation)
- [Feature Documentation](#feature-documentation)
- [Reference Documentation](#reference-documentation)
- [Testing and Development](#testing-and-development)
- [Security Documentation](#security-documentation)
- [SQLite Reference Documentation](#sqlite-reference-documentation)
- [Contributing](#contributing)
- [License](#license)

## Introduction

Anthony is a pure Go implementation of SQLite 3, providing a complete SQL database engine without CGO dependencies. It includes a SQL parser, query planner, bytecode virtual machine (VDBE), B-tree storage engine, and transaction manager.

Key features:
- Pure Go implementation (no CGO)
- SQLite-compatible file format
- Support for transactions, indexes, triggers, and views
- Common Table Expressions (CTEs) and subqueries
- Virtual table support
- Comprehensive security controls

For a quick overview of the system, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Getting Started

New to Anthony? Start here:

- [QUICKSTART.md](QUICKSTART.md) - Quick start guide with code examples
- [GETTING_STARTED.md](GETTING_STARTED.md) - Detailed getting started guide
- [USER_GUIDE.md](USER_GUIDE.md) - Comprehensive user guide
- [INDEX.md](INDEX.md) - Complete documentation index

## Core Documentation

Essential documentation for understanding Anthony's architecture and design:

### Architecture and Design

- [ARCHITECTURE.md](ARCHITECTURE.md) - High-level architecture, data flow, and design decisions
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency control and lock ordering hierarchy
- [FILE_FORMAT.md](FILE_FORMAT.md) - SQLite file format documentation
- [WAL_MODE.md](WAL_MODE.md) - Write-Ahead Logging mode

### API Documentation

- [API.md](API.md) - Core API documentation
- [API_REFERENCE.md](API_REFERENCE.md) - Complete API reference
- [INTERNAL_API.md](INTERNAL_API.md) - Internal API documentation

### Type System and Error Handling

- [TYPE_SYSTEM.md](TYPE_SYSTEM.md) - Type system and type affinity
- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Error handling patterns
- [COMPATIBILITY.md](COMPATIBILITY.md) - SQLite compatibility notes

### SQL Language

- [SQL_LANGUAGE.md](SQL_LANGUAGE.md) - SQL language support and syntax

## Feature Documentation

Documentation for specific SQL features and operations:

### Common Table Expressions (CTEs)

- [CTE_USAGE_GUIDE.md](CTE_USAGE_GUIDE.md) - User guide with examples
- [CTE_QUICK_REFERENCE.md](CTE_QUICK_REFERENCE.md) - Quick syntax reference
- [CTE_IMPLEMENTATION_SUMMARY.md](CTE_IMPLEMENTATION_SUMMARY.md) - Implementation details
- [CTE_AST_STRUCTURE.md](CTE_AST_STRUCTURE.md) - AST structure details

### Subqueries

- [SUBQUERY_QUICK_REFERENCE.md](SUBQUERY_QUICK_REFERENCE.md) - Quick syntax reference
- [SUBQUERY_ARCHITECTURE.md](SUBQUERY_ARCHITECTURE.md) - Architecture and design
- [SUBQUERY_IMPLEMENTATION.md](SUBQUERY_IMPLEMENTATION.md) - Implementation guide
- [SUBQUERY_CHECKLIST.md](SUBQUERY_CHECKLIST.md) - Development checklist

### Compound SELECT Operations

- [COMPOUND_SELECT_QUICK_REFERENCE.md](COMPOUND_SELECT_QUICK_REFERENCE.md) - UNION, INTERSECT, EXCEPT
- [COMPOUND_SELECT_BYTECODE_FLOW.md](COMPOUND_SELECT_BYTECODE_FLOW.md) - Bytecode implementation
- [INTERSECT_EXCEPT_IMPLEMENTATION.md](INTERSECT_EXCEPT_IMPLEMENTATION.md) - INTERSECT/EXCEPT details

### DDL and Schema Operations

- [ALTER_TABLE_QUICK_REFERENCE.md](ALTER_TABLE_QUICK_REFERENCE.md) - ALTER TABLE reference
- [DDL_IMPLEMENTATION_REPORT.md](DDL_IMPLEMENTATION_REPORT.md) - DDL operations status
- [ATTACH_DETACH_IMPLEMENTATION.md](ATTACH_DETACH_IMPLEMENTATION.md) - Multi-database support

### VACUUM Operations

- [VACUUM_USAGE.md](VACUUM_USAGE.md) - User guide for VACUUM
- [VACUUM_IMPLEMENTATION.md](VACUUM_IMPLEMENTATION.md) - Implementation details

### Triggers

- [TRIGGER_INTEGRATION_REPORT.md](TRIGGER_INTEGRATION_REPORT.md) - Trigger support and implementation

### Virtual Tables

- [VIRTUAL_TABLES.md](VIRTUAL_TABLES.md) - Virtual table support
- [VTABLE_QUICK_START.md](VTABLE_QUICK_START.md) - Quick start guide for virtual tables

### EXPLAIN Support

- [EXPLAIN_QUICK_REFERENCE.md](EXPLAIN_QUICK_REFERENCE.md) - EXPLAIN query syntax
- [EXPLAIN_IMPLEMENTATION.md](EXPLAIN_IMPLEMENTATION.md) - Implementation details
- [EXPLAIN_QUERY_PLAN_COSTS.md](EXPLAIN_QUERY_PLAN_COSTS.md) - Query plan cost analysis

## Reference Documentation

### Functions

- [FUNCTIONS.md](FUNCTIONS.md) - Built-in SQL functions
- [JSON_FUNCTIONS.md](JSON_FUNCTIONS.md) - JSON functions
- [EXPRESSION_INDEXES.md](EXPRESSION_INDEXES.md) - Expression-based indexes

### PRAGMA Commands

- [PRAGMAS.md](PRAGMAS.md) - PRAGMA commands reference
- [PRAGMA_QUICK_REFERENCE.md](PRAGMA_QUICK_REFERENCE.md) - Quick reference
- [PRAGMA_IMPLEMENTATION_SUMMARY.md](PRAGMA_IMPLEMENTATION_SUMMARY.md) - Implementation details

## Testing and Development

Documentation for testing, development, and code quality:

### Testing

- [TESTING.md](TESTING.md) - Testing strategy and guidelines
- [TESTING_INFRASTRUCTURE.md](TESTING_INFRASTRUCTURE.md) - Test infrastructure
- [TEST_REPORT.md](TEST_REPORT.md) - Test results and coverage
- [TEST_OPTIMIZATION.md](TEST_OPTIMIZATION.md) - Test optimization strategies
- [SQLITE_TESTING_PLAN.md](SQLITE_TESTING_PLAN.md) - SQLite test suite integration plan
- [TCL_TO_GO_CONVERSION_PLAN.md](TCL_TO_GO_CONVERSION_PLAN.md) - TCL test conversion plan

### Code Quality

- [CYCLOMATIC_COMPLEXITY_REPORT.md](CYCLOMATIC_COMPLEXITY_REPORT.md) - Complexity analysis

### Development Plans

- [SQLITE_SOURCE_INTEGRATION_PLAN.md](SQLITE_SOURCE_INTEGRATION_PLAN.md) - SQLite source integration

## Security Documentation

Security model, controls, and best practices:

### Security Guides

- [SECURITY.md](SECURITY.md) - Comprehensive security guide
  - Layered security architecture
  - SecurityConfig configuration
  - Input validation and resource limits
  - Integer and buffer safety
  - Concurrency safety
  - Security testing and vulnerability reporting

### Security Audits

- [SECURITY_AUDIT_PLAN.md](SECURITY_AUDIT_PLAN.md) - Security audit planning
- [SECURITY_AUDIT_IMPLEMENTATION.md](SECURITY_AUDIT_IMPLEMENTATION.md) - Implementation details
- [SECURITY_AUDIT_REPORT.md](SECURITY_AUDIT_REPORT.md) - Audit findings
- [SECURITY_FIXES_SUMMARY.md](SECURITY_FIXES_SUMMARY.md) - Summary of fixes

## SQLite Reference Documentation

Complete SQLite 3.51.2 reference documentation is available offline in the [sqlite/](sqlite/) subdirectory. This includes over 200 reference files covering SQL language, functions, pragmas, extensions, and internals.

For a complete index and quick links, see [sqlite/README.md](sqlite/README.md).

Key SQLite documentation files:
- SQL Language Reference
- PRAGMA Reference
- Core Functions
- WAL Specification
- File Format Specification

## Contributing

When contributing to Anthony:

1. Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system
2. Review [GETTING_STARTED.md](GETTING_STARTED.md) for development setup
3. Follow security guidelines in [SECURITY.md](SECURITY.md)
4. Understand lock ordering in [LOCK_ORDERING.md](LOCK_ORDERING.md)
5. Write tests for all new features (see [TESTING.md](TESTING.md))
6. Keep cyclomatic complexity <= 10
7. Document all public APIs
8. Update relevant documentation

Run all tests before submitting:
```bash
go test ./...
go test -race ./...  # With race detector
go test -cover ./...  # With coverage
```

## License

This project is in the public domain (SQLite License).

The authors disclaim copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.

## External Resources

- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite SQL Syntax](https://www.sqlite.org/lang.html)
- [SQLite Opcodes](https://www.sqlite.org/opcode.html)
- [Go database/sql](https://pkg.go.dev/database/sql)
