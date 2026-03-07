# Testing Guide

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Running Tests](#running-tests)
  - [Fast Testing Commands](#fast-testing-commands)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)
  - [Race Detection Tests](#race-detection-tests)
  - [Fuzz Tests](#fuzz-tests)
  - [Security Tests](#security-tests)
  - [Benchmark Tests](#benchmark-tests)
- [Test Coverage](#test-coverage)
  - [Coverage Goals](#coverage-goals)
  - [Generating Coverage Reports](#generating-coverage-reports)
  - [Coverage Analysis](#coverage-analysis)
- [Test Organization](#test-organization)
  - [Test File Structure](#test-file-structure)
  - [Test Naming Conventions](#test-naming-conventions)
  - [Test Categories](#test-categories)
- [Test Parallelization](#test-parallelization)
  - [Using t.Parallel()](#using-tparallel)
  - [Writing Parallel-Safe Tests](#writing-parallel-safe-tests)
  - [Parallel Execution Levels](#parallel-execution-levels)
  - [Short Mode for Slow Tests](#short-mode-for-slow-tests)
- [Connection Pooling](#connection-pooling)
  - [Using Pooled Connections](#using-pooled-connections)
  - [GetTestDB vs GetFreshDB](#gettestdb-vs-getfreshdb)
  - [Connection Pool Benefits](#connection-pool-benefits)
- [Running Specific Tests](#running-specific-tests)
  - [By Package](#by-package)
  - [By Name Pattern](#by-name-pattern)
  - [By Build Tag](#by-build-tag)
- [Continuous Integration](#continuous-integration)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

The Anthony SQLite driver has comprehensive test coverage across all components. The test suite includes unit tests, integration tests, security tests, fuzz tests, and benchmarks. Tests are organized by component and follow Go testing conventions.

**Test Statistics:**
- 300+ test files across all packages
- Comprehensive coverage of parser, VDBE, pager, btree, and driver layers
- Security-focused tests for attack vectors and edge cases
- Fuzz tests for parser and record decoder
- Integration tests for end-to-end scenarios

## Quick Start

```bash
# Run all tests (recommended for most development)
make test

# Fast parallel testing (optimized for speed)
make test-fast

# Short mode (skip slow tests)
make test-short

# Run tests with coverage
make test-cover

# Run tests with race detection
make test-race

# Run all quality checks
make all
```

## Running Tests

### Fast Testing Commands

The project includes several optimized test targets for different use cases:

```bash
# Standard testing (recommended for CI and validation)
make test

# Fast parallel testing (optimized for development)
make test-fast

# CI testing with JSON output
make test-ci

# Short mode - skip slow tests (fastest option)
make test-short
```

**Command Details:**

- **`make test`** - Standard test run with CGO disabled
- **`make test-fast`** - Parallel execution with 8 package workers and 4 test workers
- **`make test-ci`** - Same as test-fast but with JSON output for CI parsers
- **`make test-short`** - Uses Go's `-short` flag to skip slow tests

**Customizing Parallelism:**

You can customize the parallelism levels:

```bash
# Adjust package-level parallelism (default: 8)
make test-fast TEST_PKG_PARALLEL=16

# Adjust test-level parallelism (default: 4)
make test-fast TEST_PARALLEL=8

# Combine both
make test-fast TEST_PKG_PARALLEL=16 TEST_PARALLEL=8
```

### Unit Tests

Unit tests verify individual functions and components in isolation.

```bash
# Run all unit tests
go test ./...

# Run tests for a specific package
go test ./internal/driver
go test ./internal/pager
go test ./internal/vdbe

# Run tests verbosely to see individual test output
go test -v ./internal/driver

# Run tests with output for passing tests
go test -v ./internal/parser
```

**Using Make:**
```bash
# Run all tests
make test

# Build and test
make all
```

### Integration Tests

Integration tests verify component interactions and end-to-end functionality.

**Location:** Integration tests are marked with descriptive names containing "Integration" or test complete workflows.

```bash
# Run integration tests in driver package
go test -v ./internal/driver -run Integration

# Examples of integration test files:
# - internal/driver/integration_test.go
# - internal/driver/cte_integration_test.go
# - internal/pager/lru_integration_test.go

# Run specific integration test
go test -v ./internal/driver -run TestFullIntegration
go test -v ./internal/driver -run TestCTEIntegration
```

**Key Integration Test Areas:**
- Full SQL query execution (driver/integration_test.go)
- CTE (Common Table Expressions) integration
- Transaction lifecycle
- Schema loading and validation
- Multi-connection scenarios
- WAL mode operations

### Race Detection Tests

Race detection tests identify data races and concurrency issues using Go's race detector.

```bash
# Run all tests with race detection
go test -race ./...

# Run race detection for specific package
go test -race ./internal/driver
go test -race ./internal/pager

# Using Make
make test-race
```

**Important Notes:**
- Race detection significantly slows down test execution (5-10x slower)
- Essential for testing concurrent operations
- Run before committing changes that affect concurrency
- Some tests skip in race mode if they're too slow

**Key Race Detection Test Areas:**
- Concurrent connection close operations (driver/concurrent_security_test.go)
- Statement execution races
- Schema access synchronization
- Pager cache operations
- Lock ordering validation

**Example Race Detection Test:**
```bash
# Test concurrent database operations
go test -race -v ./internal/driver -run TestConcurrentClose

# Test pager concurrency
go test -race -v ./internal/pager
```

### Fuzz Tests

Fuzz tests use Go's built-in fuzzing (Go 1.18+) to test with random inputs.

**Available Fuzz Tests:**
1. **Parser Fuzzing** - Tests SQL parser with random inputs
2. **Record Decoder Fuzzing** - Tests VDBE record decoding
3. **Memory Pager Fuzzing** - Tests pager operations

```bash
# Run parser fuzz test (30 seconds)
go test -fuzz=FuzzParser -fuzztime=30s ./internal/parser/

# Run record decoder fuzz test (30 seconds)
go test -fuzz=FuzzDecodeRecord -fuzztime=30s ./internal/vdbe/

# Run memory pager fuzz test (30 seconds)
go test -fuzz=FuzzMemoryPager -fuzztime=30s ./internal/pager/

# Extended fuzzing (1 hour - for CI or deep testing)
go test -fuzz=FuzzParser -fuzztime=1h ./internal/parser/
go test -fuzz=FuzzDecodeRecord -fuzztime=1h ./internal/vdbe/

# Run with parallel workers
go test -fuzz=FuzzParser -fuzztime=30s -parallel=4 ./internal/parser/
```

**Fuzz Test Corpus:**
- Seed corpus is included in testdata directories
- Fuzzer automatically generates new test cases
- Crashes are saved for regression testing

**Example Fuzz Test Usage:**
```bash
# Quick fuzz test during development (10 seconds)
go test -fuzz=FuzzParser -fuzztime=10s ./internal/parser/

# Standard fuzz test (30 seconds)
go test -fuzz=FuzzDecodeRecord -fuzztime=30s ./internal/vdbe/

# Deep fuzz test for CI (5 minutes)
go test -fuzz=FuzzParser -fuzztime=5m ./internal/parser/
```

### Security Tests

Security tests verify protection against attack vectors and validate security controls.

```bash
# Run all security tests
go test -v ./internal/security/...

# Run security tests with race detection
go test -race -v ./internal/security/...

# Run all tests including security (with race detection)
go test -race ./...

# Run concurrent security tests
go test -v ./internal/driver -run Concurrent

# Run specific security test categories
go test -v ./internal/security -run TestSafeCast
go test -v ./internal/security -run Arithmetic
```

**Security Test Categories:**

1. **Attack Vector Tests**
   - Path traversal attacks (../, absolute paths, null bytes)
   - SQL injection patterns
   - Integer overflow/underflow
   - Buffer overflow attempts

2. **Boundary Tests**
   - Maximum values for integers, sizes, counts
   - Empty inputs and null handling
   - Resource limit enforcement

3. **Concurrency Tests**
   - Concurrent statement execution
   - Connection/statement lifecycle races
   - Lock ordering validation

4. **Integer Safety Tests**
   - Safe type casting (internal/security/arithmetic_test.go)
   - Overflow detection
   - Boundary value testing

**Example Security Test Commands:**
```bash
# Test arithmetic safety
go test -v ./internal/security -run TestSafeCast

# Test concurrent operations
go test -race -v ./internal/driver -run TestConcurrent

# All security tests with coverage
go test -cover -v ./internal/security/...
```

### Benchmark Tests

Benchmark tests measure performance of critical operations.

```bash
# Run all benchmarks
go test -bench=. ./...

# Run benchmarks for specific package
go test -bench=. ./internal/btree
go test -bench=. ./internal/pager

# Run specific benchmark
go test -bench=BenchmarkInsert ./internal/btree

# Run benchmarks with memory stats
go test -bench=. -benchmem ./internal/pager

# Run benchmarks multiple times for accuracy
go test -bench=. -benchtime=10s -count=5 ./internal/btree
```

**Key Benchmark Areas:**
- B-tree operations (insert, search, delete)
- Pager cache operations
- Record encoding/decoding
- Parser performance
- Function execution

**Example Benchmark Commands:**
```bash
# Benchmark btree operations with memory stats
go test -bench=Benchmark -benchmem ./internal/btree

# Benchmark pager with extended time
go test -bench=. -benchtime=30s ./internal/pager

# Compare benchmarks (save baseline first)
go test -bench=. ./internal/btree > old.txt
# ... make changes ...
go test -bench=. ./internal/btree > new.txt
benchcmp old.txt new.txt
```

**Running Benchmarks in CI:**

For CI environments, use shorter benchmark times:

```bash
# Quick benchmark run (3 seconds per benchmark)
go test -bench=. -benchtime=3s ./...

# With memory stats for performance tracking
go test -bench=. -benchtime=3s -benchmem ./... > benchmark-results.txt
```

## Test Coverage

### Coverage Goals

**Target Coverage Levels:**
- **Critical packages** (security, driver, vdbe): 80%+ coverage
- **Core packages** (parser, pager, btree): 75%+ coverage
- **Supporting packages** (schema, functions): 70%+ coverage
- **Overall project**: 75%+ coverage

**Security-Critical Coverage:**
- Path validation: 90%+ (50+ attack vectors tested)
- Integer arithmetic: 95%+ (boundary cases and overflow scenarios)
- Buffer operations: 85%+ (underflow, overflow, edge cases)
- Concurrency: 80%+ (race conditions and deadlock scenarios)

### Generating Coverage Reports

```bash
# Simple coverage summary
make test-cover

# Generate coverage profile
go test -coverprofile=coverage.out ./...

# View coverage in terminal
go tool cover -func=coverage.out

# Generate HTML coverage report
make test-cover-report
# Opens coverage.html in browser

# Per-function coverage analysis
make test-cover-func
```

**Detailed Coverage Commands:**
```bash
# Coverage for specific package
go test -coverprofile=coverage.out ./internal/driver
go tool cover -html=coverage.out -o driver-coverage.html

# Coverage with race detection
go test -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# Coverage with detailed output
go test -cover -coverprofile=coverage.out -v ./...
go tool cover -func=coverage.out | sort -k3 -n
```

### Coverage Analysis

**View Coverage by Package:**
```bash
# Generate and analyze coverage
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | grep -E '^github.com/.*/internal' | sort -k3 -n

# Find packages with low coverage
go tool cover -func=coverage.out | awk '$3 < 70'

# Show total coverage
go tool cover -func=coverage.out | tail -1
```

**HTML Coverage Report:**
```bash
# Generate interactive HTML report
make test-cover-report

# Or manually
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
# Open coverage.html in browser
```

## Test Organization

### Test File Structure

Tests follow Go conventions:
- `*_test.go` files in the same package as code under test
- `example_test.go` for example code (package `*_test`)
- `integration_test.go` for integration tests
- `security_test.go` for security-focused tests

**Example Structure:**
```bash
internal/driver/
+-- driver.go              # Implementation
+-- driver_test.go         # Unit tests
+-- example_test.go        # Examples
+-- integration_test.go    # Integration tests
+-- concurrent_security_test.go  # Concurrent/security tests
+-- trigger_test.go        # Feature-specific tests
+-- view_test.go           # Feature-specific tests
```

### Test Naming Conventions

**Test Function Names:**
```go
// Unit test
func TestFunctionName(t *testing.T)

// Table-driven test
func TestFunctionName_Scenario(t *testing.T)

// Integration test
func TestFeatureIntegration(t *testing.T)

// Concurrent test
func TestConcurrentOperation(t *testing.T)

// Example
func ExampleType_Method()

// Benchmark
func BenchmarkOperation(b *testing.B)

// Fuzz test
func FuzzOperation(f *testing.F)
```

**Test Case Names:**
- Descriptive of what is being tested
- Use underscores to separate words
- Include scenario or edge case description

### Test Categories

Tests are categorized by purpose:

1. **Unit Tests** - Test individual functions
   - Most `*_test.go` files
   - Fast execution
   - Isolated components

2. **Integration Tests** - Test component interactions
   - `*_integration_test.go` files
   - May be slower
   - Test complete workflows

3. **Example Tests** - Provide usage examples
   - `*_example_test.go` files
   - Must produce verifiable output
   - Documentation and testing

4. **Security Tests** - Test security controls
   - Files in `internal/security/`
   - Concurrent tests with race detector
   - Attack vector validation

5. **Fuzz Tests** - Random input testing
   - `*_fuzz_test.go` files
   - Require Go 1.18+
   - Corpus-based testing

## Test Parallelization

The test suite is optimized for parallel execution to significantly reduce test run time.

### Using t.Parallel()

Most tests in the codebase use `t.Parallel()` to enable concurrent execution:

```go
func TestExample(t *testing.T) {
    t.Parallel()  // Top-level parallelism

    tests := []struct {
        name string
        input string
        want string
    }{
        {"case 1", "input1", "output1"},
        {"case 2", "input2", "output2"},
    }

    for _, tt := range tests {
        tt := tt  // Capture range variable (Go < 1.22)
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()  // Subtest parallelism
            // test code
        })
    }
}
```

### Writing Parallel-Safe Tests

When writing new tests, follow these guidelines:

1. **Add t.Parallel() by default** - Unless the test has shared state
2. **Capture range variables** - Use `tt := tt` pattern (Go < 1.22)
3. **Avoid shared state** - Use isolated resources or proper synchronization
4. **Use pooled connections** - See Connection Pooling section below

**When NOT to use t.Parallel():**

- Tests that modify global state
- Tests that require specific execution order
- Tests with filesystem operations that conflict
- Tests that set environment variables

### Parallel Execution Levels

Go provides two levels of parallelism:

```bash
# Package-level parallelism (number of packages tested concurrently)
go test -p 8 ./...

# Test-level parallelism (number of tests per package running concurrently)
go test -parallel 4 ./...

# Combine both for maximum speed
go test -p 8 -parallel 4 ./...
```

The `make test-fast` target uses optimized settings: `-p 8 -parallel 4`

### Short Mode for Slow Tests

Use Go's `-short` flag to skip slow tests during development:

```go
func TestSlowIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping slow test in short mode")
    }
    // slow test code
}
```

Run with short mode:

```bash
# Skip slow tests
go test -short ./...

# Or use the Makefile target
make test-short
```

**When to use testing.Short():**

- Integration tests with large datasets
- Stress tests with many iterations
- Tests that take more than 1 second
- Tests with network or external dependencies

## Connection Pooling

The `internal/testing` package provides helper functions for efficient database connection management.

### Using Pooled Connections

```go
import (
    "testing"
    testutil "github.com/cyanitol/Public.Lib.Anthony/internal/testing"
)

func TestDatabaseOperation(t *testing.T) {
    t.Parallel()

    // Get a pooled database connection
    db := testutil.GetTestDB(t)

    // Use the connection
    _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY)")
    if err != nil {
        t.Fatal(err)
    }

    // Connection is automatically cleaned and returned to pool
}
```

### GetTestDB vs GetFreshDB

The testing package provides two functions:

**GetTestDB(t) - Pooled Connection (Recommended)**

- Reuses connections from a pool
- Automatically cleaned and reset after test
- Faster for parallel tests
- Use for most unit tests

```go
db := testutil.GetTestDB(t)
```

**GetFreshDB(t) - New Connection**

- Creates a new connection
- Completely isolated
- Use when you need guaranteed clean state
- Use for tests that modify SQLite internals

```go
db := testutil.GetFreshDB(t)
```

### Connection Pool Benefits

Using pooled connections provides:

- **10-20% faster test execution** - Reduces connection overhead
- **Better parallelism** - Multiple tests can run concurrently
- **Automatic cleanup** - No manual Close() required
- **Consistent state** - Connections are reset between tests

## Running Specific Tests

### By Package

```bash
# Test single package
go test ./internal/driver

# Test package and subpackages
go test ./internal/...

# Test multiple specific packages
go test ./internal/driver ./internal/pager ./internal/vdbe

# Test with verbose output
go test -v ./internal/driver
```

### By Name Pattern

```bash
# Run tests matching pattern
go test -run TestInsert ./...
go test -run "Test.*Integration" ./...

# Run specific test
go test -run TestFullIntegration ./internal/driver

# Run multiple test patterns
go test -run "TestConcurrent|TestRace" ./...

# Case-insensitive pattern
go test -run "(?i)insert" ./...
```

**Common Patterns:**
```bash
# All integration tests
go test -run Integration ./...

# All concurrent tests
go test -run Concurrent ./...

# All security tests
go test -run "Security|Attack|Safe" ./...

# Specific feature tests
go test -run CTE ./internal/driver
go test -run Trigger ./internal/driver
go test -run Window ./internal/vdbe
```

### By Build Tag

While this project doesn't currently use build tags extensively, you can add them:

```bash
# Skip slow tests
go test -short ./...

# Run only with specific tag
go test -tags=integration ./...
```

**Testing Short Mode:**
Some tests check `testing.Short()` and skip if true:
```bash
# Skip slow/concurrent tests
go test -short ./...
```

## Continuous Integration

### Pre-commit Checks

Before committing, run:
```bash
# Format code
make fmt

# Run linting
make vet

# Check complexity
make complexity

# Run all tests (fast mode for development)
make test-fast

# Or run standard tests
make test

# Run tests with race detection
make test-race

# Full check
make fmt && make vet && make complexity && make test-race
```

### CI Pipeline

Recommended CI pipeline using optimized targets:

```bash
# 1. Code quality
make fmt
make vet
make complexity

# 2. Fast parallel tests with JSON output
make test-ci > test-results.json

# 3. Coverage (if needed)
make test-cover-report

# 4. Race detection (on critical packages)
go test -race ./internal/driver ./internal/pager ./internal/vdbe

# 5. Security tests
go test -v ./internal/security/...

# 6. Fuzz tests (short duration for CI)
go test -fuzz=FuzzParser -fuzztime=30s ./internal/parser/
go test -fuzz=FuzzDecodeRecord -fuzztime=30s ./internal/vdbe/

# 7. Build verification
make build
```

**Fast CI Configuration:**

For faster CI pipelines, use the short mode:

```bash
# Quick validation (skips slow tests)
make test-short

# With JSON output for CI systems
make test-ci | tee test-results.json
```

**Traditional CI Pipeline:**

If you prefer explicit commands:

```bash
# 1. Code quality
go fmt ./...
go vet ./...

# 2. Unit tests with coverage and parallelism
CGO_ENABLED=0 go test -p 8 -parallel 4 -cover -coverprofile=coverage.out ./...

# 3. Race detection
go test -race ./...

# 4. Security tests
go test -v ./internal/security/...

# 5. Fuzz tests (short duration for CI)
go test -fuzz=FuzzParser -fuzztime=30s ./internal/parser/
go test -fuzz=FuzzDecodeRecord -fuzztime=30s ./internal/vdbe/

# 6. Build verification
go build ./...
```

## Troubleshooting

### Common Issues

#### Test Failures

**Problem:** Tests fail with "database locked" or timeout errors

**Solution:**
```bash
# Clean test cache
go clean -testcache

# Or use make
make clean

# For persistent issues
make clean-all
```

**Problem:** Race detector reports data races

**Solution:**
1. Run specific test with race detection:
   ```bash
   go test -race -v ./internal/driver -run TestProblemTest
   ```
2. Review the race report for involved goroutines
3. Check lock ordering (see doc/LOCK_ORDERING.md)
4. Ensure proper synchronization

#### Coverage Issues

**Problem:** Coverage report not generating

**Solution:**
```bash
# Remove old coverage files
rm -f coverage.out coverage.html

# Regenerate
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

**Problem:** Low coverage in package

**Solution:**
```bash
# Identify uncovered code
go test -coverprofile=coverage.out ./internal/package
go tool cover -html=coverage.out

# Focus on critical paths first
# Add table-driven tests for edge cases
```

#### Fuzz Test Issues

**Problem:** Fuzz test finds crash

**Solution:**
1. Fuzz test automatically saves failing input
2. Run regular test with saved corpus:
   ```bash
   go test ./internal/parser
   ```
3. Fix the issue
4. Verify fix with fuzz test:
   ```bash
   go test -fuzz=FuzzParser -fuzztime=30s ./internal/parser/
   ```

**Problem:** Fuzz test too slow

**Solution:**
```bash
# Reduce fuzz time
go test -fuzz=FuzzParser -fuzztime=10s ./internal/parser/

# Reduce parallel workers
go test -fuzz=FuzzParser -parallel=1 -fuzztime=30s ./internal/parser/
```

#### Performance Issues

**Problem:** Tests running very slowly

**Solution:**
```bash
# Skip slow tests
go test -short ./...

# Run tests in parallel (default is GOMAXPROCS)
go test -parallel=8 ./...

# Test specific package
go test ./internal/driver

# Skip integration tests
go test -run "^Test[^I]" ./...  # Skip TestI* patterns
```

**Problem:** Race detector makes tests too slow

**Solution:**
```bash
# Test specific packages with race detection
go test -race ./internal/driver
go test -race ./internal/pager

# Skip race detection for full suite
go test ./...
```

### Test Debugging

**Enable verbose output:**
```bash
# See all test output
go test -v ./internal/driver

# See test names only
go test -v ./internal/driver | grep -E "^=== RUN|PASS|FAIL"
```

**Debug specific test:**
```bash
# Run with verbose and show all logging
go test -v ./internal/driver -run TestSpecificTest

# Add temporary debug prints in test code
# Use t.Log() instead of fmt.Println()
```

**Analyze test timing:**
```bash
# Show test duration
go test -v ./internal/driver | grep -E "PASS|FAIL" | grep -E "[0-9]+\.[0-9]+s"

# Find slow tests
go test -v ./... 2>&1 | grep -E "[0-9]+\.[0-9]+s" | sort -k2 -n
```

### Environment Issues

**Problem:** Tests fail only on CI or specific environment

**Solution:**
```bash
# Check for file permission issues
ls -la testdata/

# Check for temp directory issues
TMPDIR=/custom/tmp go test ./...

# Run with specific working directory
cd /specific/path && go test ./...

# Check for environment-specific settings
env | grep GO
```

**Problem:** Tests fail intermittently

**Solution:**
```bash
# Run multiple times to reproduce
go test -count=10 ./internal/driver -run TestFlaky

# Use race detector
go test -race ./internal/driver -run TestFlaky

# Increase timeout for slow environments
go test -timeout=5m ./...
```

## Best Practices

### Writing Tests

1. **Use Table-Driven Tests with Parallelism** for multiple scenarios:
   ```go
   func TestFunction(t *testing.T) {
       t.Parallel()  // Enable top-level parallelism

       tests := []struct {
           name    string
           input   string
           want    string
           wantErr bool
       }{
           {"valid input", "test", "result", false},
           {"empty input", "", "", true},
       }
       for _, tt := range tests {
           tt := tt  // Capture range variable (Go < 1.22)
           t.Run(tt.name, func(t *testing.T) {
               t.Parallel()  // Enable subtest parallelism
               got, err := Function(tt.input)
               if (err != nil) != tt.wantErr {
                   t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
               }
               if got != tt.want {
                   t.Errorf("got %v, want %v", got, tt.want)
               }
           })
       }
   }
   ```

2. **Use t.Helper()** in test utilities:
   ```go
   func setupTest(t *testing.T) *Database {
       t.Helper()
       // setup code
   }
   ```

3. **Use t.Cleanup()** for resource cleanup:
   ```go
   func TestFeature(t *testing.T) {
       t.Parallel()

       db := openDatabase(t)
       t.Cleanup(func() {
           db.Close()
       })
   }
   ```

4. **Use Connection Pooling** for database tests:
   ```go
   import testutil "github.com/cyanitol/Public.Lib.Anthony/internal/testing"

   func TestDatabaseFeature(t *testing.T) {
       t.Parallel()

       db := testutil.GetTestDB(t)  // Pooled connection
       // test code - cleanup is automatic
   }
   ```

5. **Mark Slow Tests** with testing.Short():
   ```go
   func TestSlowOperation(t *testing.T) {
       if testing.Short() {
           t.Skip("skipping slow test in short mode")
       }
       t.Parallel()
       // slow test code
   }
   ```

6. **Test error cases**:
   - Happy path
   - Edge cases
   - Error conditions
   - Boundary values

7. **Use descriptive test names**:
   - What is being tested
   - Under what conditions
   - What is expected

8. **Parallel Testing Guidelines**:
   - Add `t.Parallel()` to all tests by default
   - Capture range variables in loops: `tt := tt`
   - Avoid shared state between tests
   - Use fresh resources (pooled connections, temp files, etc.)
   - Don't use `t.Parallel()` if test modifies global state

### Running Tests Efficiently

1. **Use optimized test commands during development:**
   ```bash
   # Fast parallel testing
   make test-fast

   # Skip slow tests for quick feedback
   make test-short

   # Test specific package
   go test ./internal/driver
   ```

2. **Use test caching:**
   ```bash
   # Go automatically caches test results
   # Force re-run with:
   go test -count=1 ./internal/driver

   # Clear test cache if needed
   make clean
   ```

3. **Write parallel-safe tests:**
   ```bash
   # Always add t.Parallel() to new tests
   func TestNewFeature(t *testing.T) {
       t.Parallel()
       // test code
   }

   # Use pooled connections for database tests
   db := testutil.GetTestDB(t)
   ```

4. **Use short mode for quick feedback:**
   ```bash
   # Skip slow tests
   make test-short

   # Or directly with go test
   go test -short ./...
   ```

5. **Optimize parallelism for your system:**
   ```bash
   # Default settings (8 packages, 4 tests per package)
   make test-fast

   # High-end system (more parallelism)
   make test-fast TEST_PKG_PARALLEL=16 TEST_PARALLEL=8

   # Low-end system (less parallelism)
   make test-fast TEST_PKG_PARALLEL=4 TEST_PARALLEL=2
   ```

### Code Coverage

1. **Aim for meaningful coverage:**
   - Focus on critical paths
   - Test error handling
   - Cover edge cases

2. **Don't chase 100% coverage:**
   - Some code is difficult to test
   - Focus on value over percentage

3. **Use coverage to find gaps:**
   ```bash
   go test -cover ./internal/driver
   go test -coverprofile=coverage.out ./internal/driver
   go tool cover -html=coverage.out
   ```

### Security Testing

1. **Always run race detector before committing:**
   ```bash
   make test-race
   ```

2. **Test concurrent scenarios:**
   - Multiple goroutines
   - Shared state access
   - Lock ordering

3. **Run fuzz tests periodically:**
   ```bash
   # Short fuzzing during development
   go test -fuzz=FuzzParser -fuzztime=30s ./internal/parser/

   # Extended fuzzing before release
   go test -fuzz=FuzzParser -fuzztime=1h ./internal/parser/
   ```

4. **Test attack vectors:**
   - Invalid inputs
   - Boundary conditions
   - Resource exhaustion

### Continuous Testing

1. **Run tests before committing:**
   ```bash
   make test
   ```

2. **Run full suite before pushing:**
   ```bash
   make test-race
   ```

3. **Use pre-commit hooks:**
   ```bash
   # .git/hooks/pre-commit
   #!/bin/sh
   make test
   ```

4. **Monitor coverage trends:**
   ```bash
   # Track coverage over time
   go test -coverprofile=coverage.out ./...
   go tool cover -func=coverage.out | tail -1
   ```

## References

- [Go Testing Documentation](https://pkg.go.dev/testing)
- [Go Fuzzing](https://go.dev/doc/fuzz/)
- [Table Driven Tests](https://github.com/golang/go/wiki/TableDrivenTests)
- [Test Optimization Charter](./TEST_OPTIMIZATION.md) - Detailed optimization strategy and project plan
- [Security Guide](./SECURITY.md) - Security testing details
- [Lock Ordering](./LOCK_ORDERING.md) - Concurrency testing guidelines
- [Architecture Guide](./ARCHITECTURE.md) - Component overview

---

**Related Documentation:**
- [TESTING_INFRASTRUCTURE.md](./TESTING_INFRASTRUCTURE.md) - Testing infrastructure details
- [TEST_OPTIMIZATION.md](./TEST_OPTIMIZATION.md) - Test optimization strategy and implementation details
- [SECURITY.md](./SECURITY.md) - Security model and testing
- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture
- [LOCK_ORDERING.md](./LOCK_ORDERING.md) - Concurrency guidelines
- [QUICKSTART.md](./QUICKSTART.md) - Getting started guide

---

## See Also

- [TESTING_INFRASTRUCTURE.md](TESTING_INFRASTRUCTURE.md) - Testing infrastructure and helper utilities
- [../CONTRIBUTING.md](../CONTRIBUTING.md) - Contributing guidelines and development workflow
- [SECURITY.md](SECURITY.md) - Security testing and best practices
