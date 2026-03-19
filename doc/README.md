# Anthony - Pure Go SQLite Implementation

[![Go Version](https://img.shields.io/badge/go-1.26.1+-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/license-Quad--License-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#testing)

A pure Go SQLite implementation for the cyanitol project. This library provides a complete SQLite database engine written entirely in Go with no CGO dependencies, making it easy to cross-compile and deploy.

## Features

- **Pure Go Implementation** - No CGO dependencies, easy cross-compilation
- **SQLite Compatible** - Implements SQLite file format and SQL syntax
- **Full ACID Support** - Transactions with journal and Write-Ahead Logging (WAL)
- **Standard database/sql Interface** - Drop-in replacement using Go's standard library
- **Advanced SQL Features**:
  - Common Table Expressions (CTEs) with recursive queries
  - Subqueries (scalar, IN, EXISTS)
  - Compound queries (UNION, INTERSECT, EXCEPT)
  - Joins (INNER, LEFT, CROSS, RIGHT, NATURAL)
  - Aggregate functions (SUM, COUNT, AVG, MIN, MAX, GROUP_CONCAT, TOTAL)
  - Window functions (ROW_NUMBER, RANK, NTH_VALUE, named WINDOW clauses)
  - Triggers (BEFORE/AFTER, WHEN clause, RAISE, cascading, UPDATE OF columns)
  - Table-valued functions (json_each, json_tree)
  - ATTACH/DETACH with cross-database queries
- **Complete DDL Support** - CREATE, ALTER (RENAME TABLE/COLUMN, DROP COLUMN), DROP for tables and indexes
- **Constraint Enforcement** - PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY, NOT NULL
- **Built-in Functions** - String, math, date/time, and aggregate functions
- **Virtual Table Support** - Extensible virtual table interface
- **Concurrent Access** - Multiple readers with thread-safe page cache

## Installation

```bash
go get github.com/cyanitol/Public.Lib.Anthony
```

**Requirements**: Go 1.26.1 or later

## Quick Start

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
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

For more examples, see [QUICKSTART.md](QUICKSTART.md).

## Documentation

### Core Documentation
- **[Quick Start Guide](QUICKSTART.md)** - Get started with code examples
- **[Architecture Overview](ARCHITECTURE.md)** - System design and internals
- **[Documentation Index](INDEX.md)** - Complete documentation index
- **[Security Guide](SECURITY.md)** - Security model and best practices
- **Development Environment** - Nix-based for reproducibility; run `nix-shell` for a fully configured toolchain

### Feature Documentation
- **[Common Table Expressions (CTEs)](CTE_USAGE_GUIDE.md)** - Recursive and non-recursive CTEs
- **[Subqueries](SUBQUERY_ARCHITECTURE.md)** - Scalar, IN, and EXISTS subqueries
- **[PRAGMA Statements](PRAGMA_QUICK_REFERENCE.md)** - Database configuration
- **[ALTER TABLE](ALTER_TABLE_QUICK_REFERENCE.md)** - Schema modifications
- **[VACUUM](VACUUM_USAGE.md)** - Database compaction
- **[Triggers](TRIGGER_INTEGRATION_REPORT.md)** - Trigger support
- **[Roadmap](ROADMAP.md)** - Current priorities and completion plan
- **[WITHOUT ROWID Plan](WITHOUT_ROWID_PLAN.md)** - Composite key design and remaining work

### Package Documentation
- **[btree](../internal/btree)** - B-tree storage engine
- **[pager](../internal/pager)** - Page cache and transaction management
- **[parser](../internal/parser)** - SQL lexer and parser
- **[vdbe](../internal/vdbe)** - Virtual Database Engine (bytecode VM)
- **[functions](../internal/functions)** - Built-in SQL functions
- **[driver](../internal/driver)** - database/sql driver interface

See [INDEX.md](INDEX.md) for the complete documentation index.

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
├── expr/        - Expression evaluation and code generation
├── functions/   - Built-in SQL functions (scalar, aggregate, date/time, JSON)
├── engine/      - Query execution engine and trigger framework
├── driver/      - database/sql driver interface and SQL compiler
├── schema/      - Database schema management (tables, indexes, views, triggers)
├── constraint/  - Constraint enforcement (PK, FK, UNIQUE, CHECK, NOT NULL)
├── collation/   - Collation sequences (BINARY, NOCASE, RTRIM)
├── security/    - Security model (path validation, resource limits)
├── utf/         - UTF-8/UTF-16 encoding
├── withoutrowid/ - WITHOUT ROWID table support
└── vtab/        - Virtual table support (FTS5, R-Tree)
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
3. **Complexity check** - No function may have cyclomatic complexity > 9
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

# Run all tests including trinity
go test -tags alltests ./internal/driver/...

# Using Make
make test         # All tests
make test-fast    # Parallel tests
make test-short   # Skip slow tests
make test-cover   # With coverage
```

## Repository Guidelines

### Project Structure & Module Organization
- Core library resides in `internal/` with major components such as `btree/`, `pager/`, `parser/`, `vdbe/`, `engine/`, and `driver/`; tests live alongside code in the same packages.
- CLI helpers for debugging and demonstrations are under `cmd/test_dsn` and `cmd/test_explain`.
- Documentation is in `doc/` (architecture, testing, security); examples are in `example/`; repository entrypoint wiring sits in `anthony.go`.

### Build, Test, and Development Commands
- Preferred workflow uses the reproducible `nix-shell`.
- Quick build: `make build` (or `go build ./...`).
- Test suite: `make test` (CGO disabled); faster iterations `make test-fast TEST_PKG_PARALLEL=16 TEST_PARALLEL=8`; skip slow cases with `make test-short`.
- Coverage and race: `make test-cover`, `make test-race`, or `make test-cover-report` to emit `coverage.html`.
- Full pre-commit gate: `make commit` (runs fmt, SPDX check, complexity, vet, build, test). Run this before opening review.

### Coding Style & Naming Conventions
- Go code must be formatted with `gofmt`; no diffs should appear from `gofmt -w .`.
- Every `.go` file requires an `SPDX-License-Identifier` header; `make check-spdx` enforces this.
- Maintain cyclomatic complexity ≤ 9 for non-test functions (`make check-complexity`); refactor or split functions if they drift higher.
- Follow idiomatic Go naming; keep exported APIs minimal. Use clear package prefixes for helpers instead of stuttered names (e.g., `pager.Cache`, not `pager.PagerCache`).
- Tests favor table-driven layouts with underscore-separated scenario names (e.g., `TestPlanBuilder_NestedCTE`).

### Testing Guidelines
- Primary framework is the standard `go test`. Run `go test ./...` for quick local validation when outside `nix-shell`.
- Coverage targets: critical packages (driver, vdbe, security) 80%+, core packages (parser, pager, btree) 75%+, overall 75%+. Use `go tool cover -func=coverage.out` after `go test -coverprofile=coverage.out ./...`.
- Use `-short` for slow paths and parallelize with `-p`/`-parallel` (mirrored by `TEST_PKG_PARALLEL` and `TEST_PARALLEL` in Makefile).
- Keep examples as `ExampleType_Method` and add fuzz/benchmarks where they provide regression protection.

### Commit & Pull Request Guidelines
- External PRs are currently declined (see `CONTRIBUTING.md`), but internal changes should still follow review discipline.
- Commit messages in history are short and imperative (e.g., “Add AREWETHEREYET.md feature comparison document”); use similar style and keep commits scoped.
- PRs should describe the behavior change, list test commands run (`make commit` expected), and link issues or design notes. Include coverage or performance notes for critical areas and screenshots only when UI tooling is involved.

### Security & Configuration Tips
- Default builds and tests run with `CGO_ENABLED=0`; avoid adding CGO dependencies without approval.
- Review `SECURITY.md` before modifying pager, btree, or WAL paths; highlight any changes that affect file formats, locking, or crash safety in your PR description.

## Known Limitations

- **Windows File Locking** - File locking on Windows is not yet implemented
- **Recursive CTEs** - Cursor architecture being reworked for correct recursive member execution
- **Window Function Edge Cases** - 42 trinity tests skipped for advanced window function scenarios
- **Performance** - No prepared statement caching yet

## Roadmap

### Completed
- Phase 1: Core Infrastructure (B-tree, Pager, VDBE, Parser)
- Phase 2: SQL Features (Constraints, CTEs, Subqueries, DDL)
- Phase 3: Functions, Query Optimization, and Integration
- Phase 4: Triggers (BEFORE/AFTER/INSTEAD OF, WHEN clause, RAISE, cascading, UPDATE OF)
- Phase 5: ALTER TABLE (RENAME TABLE, RENAME COLUMN, DROP COLUMN)
- Phase 6: ATTACH/DETACH (cross-database queries, PRAGMA database_list)
- Phase 7: Table-Valued Functions (json_each, json_tree)
- Phase 8: Window Functions (ROW_NUMBER, RANK, NTH_VALUE, named WINDOW clauses)
- Phase 9: Date/Time (strftime %w/%u/%W/%j), AUTOINCREMENT, schema persistence

### Current Focus
- Recursive CTE cursor architecture fix
- Window function edge cases (42 trinity tests skipped)
- OLD row extraction from cursors for DELETE/UPDATE triggers

### Planned
- Performance optimization (caching, pooling)
- WAL concurrent operations
- Fuzzing and sqllogictest conformance
- Platform-specific enhancements (Windows file locking)

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Contributing

Contributions are welcome! When contributing:

1. **Read the documentation** - Understand the architecture before making changes
2. **Follow coding standards**:
   - Keep cyclomatic complexity ≤ 9
   - Add comprehensive tests for new features
   - Document all public APIs
   - Use the standard Go formatting (`gofmt`)
3. **Write tests first** - Use Test-Driven Development (TDD)
4. **Follow the lock hierarchy** - See [LOCK_ORDERING.md](LOCK_ORDERING.md)
5. **Run tests with race detector** - `go test -race ./...`
6. **Update documentation** - Keep docs synchronized with code changes

### Code Review Checklist
- [ ] All database paths validated
- [ ] Integer casts use safe conversion functions
- [ ] Buffer access includes bounds checks
- [ ] Locks acquired in correct order
- [ ] Tests cover the new functionality
- [ ] Documentation updated

For security vulnerabilities, please see [SECURITY.md](SECURITY.md) for responsible disclosure.

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

Licensed under your choice of: **Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause**.

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

- **Repository**: [github.com/cyanitol/Public.Lib.Anthony](https://github.com/cyanitol/Public.Lib.Anthony)
- **SQLite Documentation**: [sqlite.org/docs.html](https://www.sqlite.org/docs.html)
- **Go database/sql**: [pkg.go.dev/database/sql](https://pkg.go.dev/database/sql)
