# Contributing to Public.Lib.Anthony

Thank you for your interest in contributing to the Anthony SQLite Driver! This document provides guidelines for contributing to this pure Go SQLite implementation.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Style Guidelines](#code-style-guidelines)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Commit Message Format](#commit-message-format)
- [Security Considerations](#security-considerations)
- [Documentation Requirements](#documentation-requirements)
- [Resources](#resources)

## Code of Conduct

This project follows the SQLite blessing:

> May you do good and not evil.
> May you find forgiveness for yourself and forgive others.
> May you share freely, never taking more than you give.

We expect all contributors to be respectful, constructive, and collaborative.

## Getting Started

### Prerequisites

- Go 1.26 or later
- Git
- Make (optional, but recommended)

### Initial Setup

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Public.Lib.Anthony.git
   cd Public.Lib.Anthony
   ```

3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/JuniperBible/Public.Lib.Anthony.git
   ```

4. Install development dependencies:
   ```bash
   go mod download
   ```

5. Verify your setup:
   ```bash
   make test
   ```

## Development Workflow

1. **Sync with upstream** before starting work:
   ```bash
   git checkout main
   git fetch upstream
   git merge upstream/main
   ```

2. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes** following the code style guidelines

4. **Run tests** frequently during development:
   ```bash
   make test
   make test-race
   ```

5. **Commit your changes** using the commit message format

6. **Push to your fork** and create a pull request

## Code Style Guidelines

### General Go Style

- Follow the [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Use `gofmt` for formatting (run `make fmt`)
- Use `go vet` to catch common mistakes (run `make vet`)
- Keep cyclomatic complexity at or below 10 (run `make complexity`)
- Write clear, self-documenting code with meaningful variable names

### Package Organization

The project follows a modular architecture:

```
internal/
├── btree/      - B-tree storage engine
├── driver/     - database/sql driver interface
├── engine/     - Query execution engine
├── expr/       - Expression evaluation
├── format/     - SQLite file format utilities
├── functions/  - Built-in SQL functions
├── pager/      - Page cache, journal, transactions
├── parser/     - SQL lexer, parser, AST
├── planner/    - Query optimizer
├── schema/     - Database schema management
├── security/   - Security controls and validation
├── sql/        - SQL statement compilation
├── utf/        - UTF-8/UTF-16 encoding
└── vdbe/       - Virtual Database Engine (bytecode VM)
```

### Naming Conventions

- **Exported types/functions**: Use CamelCase (e.g., `OpenDatabase`)
- **Unexported types/functions**: Use camelCase (e.g., `parseQuery`)
- **Constants**: Use CamelCase or UPPER_CASE for readability
- **Error variables**: Prefix with `Err` (e.g., `ErrDatabaseLocked`)
- **Interfaces**: Suffix with `Interface` when needed (e.g., `PagerInterface`)

### Error Handling

- Always check and handle errors explicitly
- Return errors rather than panicking (except for programmer errors)
- Use `fmt.Errorf` with `%w` for error wrapping
- Define sentinel errors for common conditions

Example:
```go
if err != nil {
    return fmt.Errorf("failed to parse query: %w", err)
}
```

### Comments

- Use complete sentences with proper punctuation
- Document all exported types, functions, and constants
- Include package-level documentation in `doc.go` files
- Explain WHY, not just WHAT, in complex code sections

Example:
```go
// SafeCastUint32ToUint16 converts a uint32 to uint16 with overflow checking.
// This prevents integer overflow vulnerabilities in page number calculations.
func SafeCastUint32ToUint16(val uint32) (uint16, error) {
    if val > 0xFFFF {
        return 0, ErrIntegerOverflow
    }
    return uint16(val), nil
}
```

## Testing Requirements

All contributions must include appropriate tests. We aim for high test coverage while maintaining meaningful tests.

### Test Types

1. **Unit Tests**: Test individual functions in isolation
2. **Integration Tests**: Test component interactions
3. **Concurrent Tests**: Test thread safety with `-race`
4. **Regression Tests**: Prevent previously fixed bugs from recurring

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-cover

# Generate coverage report
make test-cover-report

# Run with race detector (REQUIRED before submitting PR)
make test-race

# Run specific package tests
go test ./internal/btree/...
```

### Test Guidelines

- Test file naming: `*_test.go`
- Test function naming: `TestFunctionName` or `TestFeature_Scenario`
- Use table-driven tests for multiple scenarios
- Include edge cases and error conditions
- Test concurrent access for thread-safe code
- Mock external dependencies when appropriate

Example table-driven test:
```go
func TestSafeCast(t *testing.T) {
    tests := []struct {
        name    string
        input   uint32
        want    uint16
        wantErr bool
    }{
        {"zero", 0, 0, false},
        {"max valid", 0xFFFF, 0xFFFF, false},
        {"overflow", 0x10000, 0, true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := SafeCastUint32ToUint16(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("got error %v, wantErr %v", err, tt.wantErr)
            }
            if got != tt.want {
                t.Errorf("got %v, want %v", got, tt.want)
            }
        })
    }
}
```

### Coverage Requirements

- New code should have >80% test coverage
- Security-critical code should have >90% coverage
- Run `make test-cover-func` to check per-function coverage

## Pull Request Process

### Before Submitting

1. **Run all tests**:
   ```bash
   make test
   make test-race
   make complexity
   ```

2. **Format and lint**:
   ```bash
   make fmt
   make vet
   ```

3. **Update documentation** if needed

4. **Review the security checklist** (see below)

### PR Requirements

- Clear, descriptive title summarizing the change
- Detailed description explaining:
  - What changed and why
  - How it was tested
  - Any breaking changes or migration steps
- Reference related issues (e.g., "Fixes #123")
- All CI checks must pass
- At least one maintainer approval required

### PR Template

```markdown
## Summary
Brief description of the changes

## Motivation
Why is this change needed?

## Changes
- List of specific changes made

## Testing
How was this tested?

## Security Considerations
Any security implications? (See docs/SECURITY.md checklist)

## Breaking Changes
Any breaking changes?

## Related Issues
Fixes #123
```

### Review Process

- Maintainers will review within 3-5 business days
- Address review comments promptly
- Push new commits for changes (don't force-push during review)
- Once approved, maintainers will merge

## Commit Message Format

We follow a consistent commit message format for clarity and automated changelog generation.

### Format

```
<type>: <summary>

<optional body>

<optional footer>
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or modifying tests
- `refactor`: Code refactoring (no functional changes)
- `perf`: Performance improvements
- `security`: Security fixes or improvements
- `build`: Build system or dependency changes
- `chore`: Maintenance tasks

### Examples

```
feat: Add support for INTERSECT and EXCEPT operators

Implement INTERSECT and EXCEPT compound SELECT operators with
proper duplicate handling and NULL comparison semantics.

Closes #45
```

```
fix: Prevent deadlock in connection close path

Use two-phase close pattern to release Conn.mu before acquiring
Driver.mu, following lock ordering hierarchy.

Fixes #89
```

```
security: Add integer overflow checks to page number calculations

All page number arithmetic now uses SafeCast* functions to prevent
integer overflow vulnerabilities.
```

### Commit Guidelines

- Keep the summary line under 72 characters
- Use imperative mood ("Add feature" not "Added feature")
- Capitalize the summary line
- No period at the end of the summary
- Separate summary from body with a blank line
- Wrap body at 72 characters
- Reference issues and PRs in the footer

## Security Considerations

Security is critical for a database driver. All contributors must follow security best practices.

### Security Checklist for Contributors

When submitting code, review this checklist:

- [ ] All database paths validated through `ValidateDatabasePath()`
- [ ] Integer casts use `SafeCast*()` functions
- [ ] Buffer access includes bounds checks
- [ ] Locks acquired in correct order (see docs/LOCK_ORDERING.md)
- [ ] Resource limits enforced (SQL length, expression depth, etc.)
- [ ] New code has security tests
- [ ] No hardcoded paths or filesystem assumptions
- [ ] Error messages don't leak sensitive information
- [ ] PRAGMA statements validated against whitelist
- [ ] Concurrent access properly synchronized

See [docs/SECURITY.md](docs/SECURITY.md) for the complete security guide.

### Critical Security Rules

1. **Path Validation**: ALL database paths MUST be validated through `security.ValidateDatabasePath()`
2. **Integer Safety**: ALL integer casts MUST use `SafeCast*()` functions
3. **Buffer Safety**: ALL buffer access MUST include bounds checking
4. **Lock Ordering**: ALL lock acquisition MUST follow the hierarchy in [docs/LOCK_ORDERING.md](docs/LOCK_ORDERING.md)
5. **Resource Limits**: ALL user input MUST be validated against defined limits

### Reporting Security Vulnerabilities

**DO NOT** open public issues for security vulnerabilities. See [docs/SECURITY.md](docs/SECURITY.md#reporting-security-vulnerabilities) for the responsible disclosure process.

## Documentation Requirements

### Code Documentation

- All exported types, functions, and constants must have godoc comments
- Complex algorithms should have explanatory comments
- Security-critical code must document security implications

### Package Documentation

When adding new packages, include a `doc.go` file:

```go
// Package btree implements a B-tree storage engine for SQLite database pages.
//
// The B-tree provides efficient key-value storage with support for:
//   - Ordered iteration
//   - Range queries
//   - Multi-level page caching
//
// Thread Safety: All operations are protected by appropriate locks.
package btree
```

### Documentation Files

Major features or systems should have documentation in `docs/`:

- Architecture overviews
- Implementation details
- Usage guides
- Quick reference guides

Examples:
- `docs/ARCHITECTURE.md` - System architecture
- `docs/SECURITY.md` - Security guide
- `docs/LOCK_ORDERING.md` - Concurrency documentation

### README Updates

Update the main README.md when:
- Adding new features visible to users
- Changing the driver interface
- Modifying installation or usage instructions

## Resources

### Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - System architecture overview
- [docs/SECURITY.md](docs/SECURITY.md) - Complete security guide
- [docs/LOCK_ORDERING.md](docs/LOCK_ORDERING.md) - Lock ordering and concurrency
- [docs/QUICKSTART.md](docs/QUICKSTART.md) - Getting started guide
- [docs/INDEX.md](docs/INDEX.md) - Documentation index

### External Resources

- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [Effective Go](https://golang.org/doc/effective_go.html)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

### Building and Testing

```bash
# Show all available make targets
make help

# Common workflows
make all                  # Test and build
make test                 # Run tests
make test-race            # Run with race detector
make test-cover-report    # Generate coverage report
make complexity           # Check cyclomatic complexity
make fmt                  # Format code
make clean               # Clean generated files
```

## Questions?

- Open an issue for bugs or feature requests
- Check existing documentation in `docs/`
- Review closed issues for similar questions
- Ask maintainers in your pull request

## License

By contributing, you agree to place your contributions in the public domain under the SQLite blessing:

> May you do good and not evil.
> May you find forgiveness for yourself and forgive others.
> May you share freely, never taking more than you give.

---

Thank you for contributing to Public.Lib.Anthony!
