# Anthony - Pure Go SQLite Implementation

[![Go Version](https://img.shields.io/badge/go-1.26+-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/license-Tri--License-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#testing)

A pure Go SQLite implementation for the JuniperBible project. This library provides a complete SQLite database engine written entirely in Go with no CGO dependencies, making it easy to cross-compile and deploy.

## Features

- **Pure Go Implementation** - No CGO dependencies, easy cross-compilation
- **SQLite Compatible** - Implements SQLite file format and SQL syntax
- **Full ACID Support** - Transactions with journal and Write-Ahead Logging (WAL)
- **Standard database/sql Interface** - Drop-in replacement using Go's standard library
- **Advanced SQL Features**:
  - Common Table Expressions (CTEs) with recursive queries
  - Subqueries (scalar, IN, EXISTS)
  - Compound queries (UNION, INTERSECT, EXCEPT)
  - Joins (INNER, LEFT, CROSS)
  - Aggregate functions (SUM, COUNT, AVG, MIN, MAX, GROUP_CONCAT)
  - Window functions
  - Triggers and views
- **Complete DDL Support** - CREATE, ALTER, DROP for tables and indexes
- **Constraint Enforcement** - PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY, NOT NULL
- **Built-in Functions** - String, math, date/time, and aggregate functions
- **Virtual Table Support** - Extensible virtual table interface
- **Concurrent Access** - Multiple readers with thread-safe page cache

## Installation

```bash
go get github.com/JuniperBible/Public.Lib.Anthony
```

**Requirements**: Go 1.26 or later

## Quick Start

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    // Open database (creates if it doesn't exist)
    db, err := sql.Open("sqlite_internal", "mydb.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create a table
    _, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    _, err = db.Exec(`INSERT INTO users (name) VALUES (?)`, "Alice")
    if err != nil {
        log.Fatal(err)
    }

    // Query data
    rows, err := db.Query(`SELECT id, name FROM users`)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name string
        rows.Scan(&id, &name)
        log.Printf("User: %d, %s", id, name)
    }
}
```

The driver registers as `sqlite_internal` to avoid conflicts with other SQLite drivers.

For more examples, see [docs/QUICKSTART.md](docs/QUICKSTART.md).

## Documentation

### Core Documentation
- **[Quick Start Guide](docs/QUICKSTART.md)** - Get started with code examples
- **[Architecture Overview](docs/ARCHITECTURE.md)** - System design and internals
- **[Documentation Index](docs/INDEX.md)** - Complete documentation index
- **[Security Guide](docs/SECURITY.md)** - Security model and best practices

### Feature Documentation
- **[Common Table Expressions (CTEs)](docs/CTE_USAGE_GUIDE.md)** - Recursive and non-recursive CTEs
- **[Subqueries](docs/SUBQUERY_ARCHITECTURE.md)** - Scalar, IN, and EXISTS subqueries
- **[PRAGMA Statements](docs/PRAGMA_QUICK_REFERENCE.md)** - Database configuration
- **[ALTER TABLE](docs/ALTER_TABLE_QUICK_REFERENCE.md)** - Schema modifications
- **[VACUUM](docs/VACUUM_USAGE.md)** - Database compaction
- **[Triggers](docs/TRIGGER_INTEGRATION_REPORT.md)** - Trigger support

### Package Documentation
- **[btree](internal/btree)** - B-tree storage engine
- **[pager](internal/pager)** - Page cache and transaction management
- **[parser](internal/parser)** - SQL lexer and parser
- **[vdbe](internal/vdbe)** - Virtual Database Engine (bytecode VM)
- **[functions](internal/functions)** - Built-in SQL functions
- **[driver](internal/driver)** - database/sql driver interface

See [docs/INDEX.md](docs/INDEX.md) for the complete documentation index.

## Package Structure

```
internal/
├── btree/       - B-tree storage engine
├── pager/       - Page cache, journal, and transaction management
├── format/      - SQLite file format utilities
├── parser/      - SQL lexer, parser, and AST
├── planner/     - Query optimizer
├── sql/         - SQL statement compilation
├── vdbe/        - Virtual Database Engine (bytecode VM)
├── expr/        - Expression evaluation
├── functions/   - Built-in SQL functions
├── engine/      - Query execution engine
├── driver/      - database/sql driver interface
├── schema/      - Database schema management
├── constraint/  - Constraint enforcement
├── utf/         - UTF-8/UTF-16 encoding and collation
└── vtab/        - Virtual table support
```

## Development

### Using Nix Shell

This project uses Nix for reproducible development environments:

```bash
# Enter the development shell
nix-shell

# All Go commands work inside nix-shell
go build ./...
go test ./...
```

### Pre-Commit Checks

Before committing, run the validation checks:

```bash
# Run all pre-commit checks
make commit
```

This runs:
1. **Format check** - Ensures code is `gofmt` formatted
2. **SPDX check** - All `.go` files must have SPDX license headers
3. **Complexity check** - No function may have cyclomatic complexity > 10
4. **go vet** - Static analysis for bugs
5. **Build** - Ensures project compiles
6. **Tests** - All tests must pass

Individual checks:
```bash
make check-fmt         # Check formatting
make check-spdx        # Check license headers
make check-complexity  # Check function complexity
make vet               # Run go vet
```

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run with race detector
go test -race ./...

# Run specific package tests
go test ./internal/driver/...
go test ./internal/btree/...

# Using Make
make test         # All tests
make test-fast    # Parallel tests
make test-short   # Skip slow tests
make test-cover   # With coverage
```

## Known Limitations

- **Windows File Locking** - File locking on Windows is not yet implemented (Phase 2.1)
- **ORDER BY Execution** - ORDER BY bytecode generation is incomplete (Phase 3.3)
- **UPDATE/DELETE Operations** - Full WHERE clause support is in progress (Phase 3.4)
- **Trigger Execution** - Trigger integration with DML is partial (Phase 3.5)
- **Memory Limits** - No spill-to-disk for large sort operations yet (Phase 4.4)
- **Performance** - No prepared statement caching yet (Phase 4.1)

See [TODO.txt](TODO.txt) for the complete roadmap across 9 phases.

## Roadmap

### Completed
- Phase 1: Core Infrastructure (B-tree, Pager, VDBE, Parser)
- Phase 2: SQL Features (Constraints, CTEs, Subqueries, DDL)
- Phase 3: Functions, Query Optimization, and Integration

### Current Focus
- Completing feature implementations (CTEs, Subqueries, Triggers)
- Storage layer hardening (WAL, journal recovery)
- Code architecture improvements

### Planned
- Performance optimization (caching, pooling)
- Security enhancements (input validation, resource limits)
- Testing expansion (fuzzing, benchmarks)
- Configuration and extensibility (DSN parsing, custom collations)

See [CHANGELOG.md](CHANGELOG.md) for version history and [TODO.txt](TODO.txt) for detailed task breakdown.

## Contributing

Contributions are welcome! When contributing:

1. **Read the documentation** - Understand the architecture before making changes
2. **Follow coding standards**:
   - Keep cyclomatic complexity ≤ 10
   - Add comprehensive tests for new features
   - Document all public APIs
   - Use the standard Go formatting (`gofmt`)
3. **Write tests first** - Use Test-Driven Development (TDD)
4. **Follow the lock hierarchy** - See [docs/LOCK_ORDERING.md](docs/LOCK_ORDERING.md)
5. **Run tests with race detector** - `go test -race ./...`
6. **Update documentation** - Keep docs synchronized with code changes

### Code Review Checklist
- [ ] All database paths validated
- [ ] Integer casts use safe conversion functions
- [ ] Buffer access includes bounds checks
- [ ] Locks acquired in correct order
- [ ] Tests cover the new functionality
- [ ] Documentation updated

For security vulnerabilities, please see [docs/SECURITY.md](docs/SECURITY.md) for responsible disclosure.

## Performance Tips

- **Create indexes** on frequently queried columns
- **Use prepared statements** for repeated queries
- **Enable WAL mode** for better concurrency: `PRAGMA journal_mode=WAL`
- **Configure connection pool** settings appropriately
- **Use transactions** for bulk inserts and updates

## Development status

This project is open source, but we are **not accepting external code
contributions at this time**. Issues and feedback are welcome.

## License

Licensed under your choice of: **Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0**.

See `LICENSE` and the `LICENSE-*` files for the full text.

The model and patterns of this software are heavily influenced by SQLite. The
author of this code recognizes the gift given by the SQLite creators and wishes
to continue that tradition.

Here is the SQLite Blessing:

* May you do good and not evil.
* May you find forgiveness for yourself and forgive others.
* May you share freely, never taking more than you give.


## Acknowledgments

This implementation is inspired by SQLite's design and architecture. We are grateful to the SQLite team for their excellent documentation and for placing their work in the public domain.

## Links

- **Repository**: [github.com/JuniperBible/Public.Lib.Anthony](https://github.com/JuniperBible/Public.Lib.Anthony)
- **SQLite Documentation**: [sqlite.org/docs.html](https://www.sqlite.org/docs.html)
- **Go database/sql**: [pkg.go.dev/database/sql](https://pkg.go.dev/database/sql)
