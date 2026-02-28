# Test Optimization Project Charter

## Overview

This document outlines the test optimization strategy for the Anthony SQLite driver. The goal is to significantly reduce test execution time while maintaining test quality and coverage.

## Current State

- **Test Functions:** 5,085 across 366 test files
- **Parallel Tests:** 0 (no `t.Parallel()` usage)
- **Estimated Full Suite Time:** 2-5 minutes
- **Test Caching:** Disabled by `-count=1` in some contexts

## Optimization Goals

| Priority | Optimization | Target Impact |
|----------|-------------|---------------|
| 1 | Add `t.Parallel()` to tests | 50-70% reduction |
| 2 | Enable test caching | 80-90% on unchanged code |
| 3 | Parallel package execution | 20-30% reduction |
| 4 | Disable CGO | 10-15% reduction |
| 5 | Short mode for slow tests | Variable |
| 6 | JSON output for CI | Faster parsing |
| 7 | Connection pooling | 10-20% reduction |

## Implementation Plan

### Phase 1: Infrastructure (Makefile)

Add optimized test targets:

```makefile
# Fast test (default for development)
test-fast:
	CGO_ENABLED=0 go test -p 8 -parallel 4 ./...

# CI test with JSON output
test-ci:
	CGO_ENABLED=0 go test -p 8 -parallel 4 -trimpath -json ./...

# Short mode (skip slow tests)
test-short:
	CGO_ENABLED=0 go test -p 8 -parallel 4 -short ./...
```

### Phase 2: Add t.Parallel() to Tests

Pattern for table-driven tests:

```go
func TestExample(t *testing.T) {
    t.Parallel()  // Top-level parallelism

    tests := []struct {
        name string
        // ...
    }{
        // test cases
    }

    for _, tt := range tests {
        tt := tt  // Capture range variable
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()  // Subtest parallelism
            // test code
        })
    }
}
```

**Packages to parallelize (in order):**
1. `internal/parser` - Pure parsing, no side effects
2. `internal/expr` - Expression evaluation
3. `internal/collation` - String comparison
4. `internal/format` - Formatting utilities
5. `internal/schema` - Schema operations
6. `internal/vdbe` - Virtual machine (careful with state)
7. `internal/driver` - Integration tests (careful with DB state)
8. `internal/pager` - Page management (careful with files)
9. `internal/btree` - B-tree operations (careful with files)

### Phase 3: Short Mode for Slow Tests

Add short mode checks to slow tests:

```go
func TestSlowOperation(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping slow test in short mode")
    }
    // slow test code
}
```

**Candidates for short mode:**
- Stress tests
- Large data set tests
- Integration tests
- Concurrent tests with many iterations

### Phase 4: Connection Pooling

Create test helper for database connection reuse:

```go
// internal/testing/pool.go
var testDBPool = sync.Pool{
    New: func() interface{} {
        db, _ := sql.Open("sqlite", ":memory:")
        return db
    },
}

func GetTestDB(t *testing.T) *sql.DB {
    t.Helper()
    db := testDBPool.Get().(*sql.DB)
    t.Cleanup(func() {
        // Reset and return to pool
        db.Exec("PRAGMA writable_schema = RESET")
        testDBPool.Put(db)
    })
    return db
}
```

### Phase 5: Benchmarks

Add benchmark scaffolding for critical paths:

```go
func BenchmarkParseSelect(b *testing.B) {
    sql := "SELECT * FROM users WHERE id = ?"
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        parser := NewParser(sql)
        parser.Parse()
    }
}
```

## Test Commands Reference

### Development

```bash
# Quick test (uses cache)
make test

# Fast test with parallelism
make test-fast

# Short mode (skip slow tests)
make test-short
```

### CI/CD

```bash
# CI with JSON output
make test-ci

# Full test with coverage
make test-cover

# Race detection
make test-race
```

### Debugging

```bash
# Force re-run without cache
go test -count=1 ./...

# Verbose single package
go test -v ./internal/driver

# Profile slow tests
go test -v ./... 2>&1 | grep -E "--- (PASS|FAIL):" | sort -t'(' -k2 -rn | head -20
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CGO_ENABLED` | Disable CGO for faster builds | `0` |
| `GOMAXPROCS` | Max parallel processes | CPU count |
| `GOTESTFLAGS` | Additional test flags | `` |

### Makefile Variables

```makefile
# Customize parallelism
TEST_PARALLEL ?= 4
TEST_PKG_PARALLEL ?= 8

# Example usage
test-custom:
	go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) ./...
```

## Metrics

Track these metrics to measure improvement:

1. **Full Suite Time** - Time to run all tests
2. **Cached Run Time** - Time when tests are cached
3. **Short Mode Time** - Time with `-short` flag
4. **CI Pipeline Time** - End-to-end CI duration

## Related Documentation

- [TESTING.md](./TESTING.md) - Complete testing guide
- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture
- [SECURITY.md](./SECURITY.md) - Security testing

---

*Last Updated: February 2026*
