# Testing Guide

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Running Tests](#running-tests)
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

# Run tests with coverage
make test-cover

# Run tests with race detection
make test-race

# Run all quality checks
make all
```

## Running Tests

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
```
internal/driver/
├── driver.go              # Implementation
├── driver_test.go         # Unit tests
├── example_test.go        # Examples
├── integration_test.go    # Integration tests
├── concurrent_security_test.go  # Concurrent/security tests
├── trigger_test.go        # Feature-specific tests
└── view_test.go           # Feature-specific tests
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

# Run all tests
make test

# Run tests with race detection
make test-race

# Full check
make fmt && make vet && make complexity && make test-race
```

### CI Pipeline

Recommended CI pipeline:
```bash
# 1. Code quality
go fmt ./...
go vet ./...

# 2. Unit tests with coverage
go test -cover -coverprofile=coverage.out ./...

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
3. Check lock ordering (see docs/LOCK_ORDERING.md)
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

1. **Use Table-Driven Tests** for multiple scenarios:
   ```go
   func TestFunction(t *testing.T) {
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
           t.Run(tt.name, func(t *testing.T) {
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
       db := openDatabase(t)
       t.Cleanup(func() {
           db.Close()
       })
   }
   ```

4. **Test error cases**:
   - Happy path
   - Edge cases
   - Error conditions
   - Boundary values

5. **Use descriptive test names**:
   - What is being tested
   - Under what conditions
   - What is expected

### Running Tests Efficiently

1. **Run relevant tests during development:**
   ```bash
   # Test package you're working on
   go test ./internal/driver
   ```

2. **Use test caching:**
   ```bash
   # Go automatically caches test results
   # Force re-run with:
   go test -count=1 ./internal/driver
   ```

3. **Run parallel tests when possible:**
   ```bash
   # Default parallelism
   go test ./...

   # Specify parallel workers
   go test -parallel=4 ./...
   ```

4. **Use short mode for quick feedback:**
   ```bash
   go test -short ./...
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
- [Security Guide](./SECURITY.md) - Security testing details
- [Lock Ordering](./LOCK_ORDERING.md) - Concurrency testing guidelines
- [Architecture Guide](./ARCHITECTURE.md) - Component overview

---

**Related Documentation:**
- [SECURITY.md](./SECURITY.md) - Security model and testing
- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture
- [LOCK_ORDERING.md](./LOCK_ORDERING.md) - Concurrency guidelines
- [QUICKSTART.md](./QUICKSTART.md) - Getting started guide
