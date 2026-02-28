# Testing Infrastructure

This document describes the comprehensive testing infrastructure for the Public.Lib.Anthony SQLite implementation.

## Overview

The testing infrastructure consists of two major components:

1. **Fuzz Testing** - Automated testing with random inputs to discover edge cases and crashes
2. **SQL Logic Test (SLT) Runner** - Compatibility testing using SQLite's official test format

Both systems help ensure correctness, robustness, and compatibility with SQLite.

## Fuzz Testing

Fuzz testing uses Go 1.18+ native fuzzing to automatically generate test inputs and discover bugs, panics, and edge cases.

### Location

- `/internal/driver/fuzz_test.go` - SQL execution fuzzing
- `/internal/parser/fuzz_test.go` - SQL parser fuzzing
- `/internal/vdbe/fuzz_test.go` - Record encoding/decoding fuzzing

### Running Fuzz Tests

```bash
# Run all fuzz tests (runs until stopped with Ctrl+C)
nix-shell --run "go test -fuzz=. ./internal/driver"
nix-shell --run "go test -fuzz=. ./internal/parser"
nix-shell --run "go test -fuzz=. ./internal/vdbe"

# Run specific fuzz test
nix-shell --run "go test -fuzz=FuzzSQL ./internal/driver"
nix-shell --run "go test -fuzz=FuzzParse ./internal/parser"
nix-shell --run "go test -fuzz=FuzzDecodeRecord ./internal/vdbe"

# Run with time limit (e.g., 30 seconds)
nix-shell --run "go test -fuzz=FuzzSQL -fuzztime=30s ./internal/driver"

# Run with minimum time (e.g., until 1000 new inputs tested)
nix-shell --run "go test -fuzz=FuzzParse -fuzzminimizetime=1000x ./internal/parser"
```

### Driver Fuzz Tests

Located in `/internal/driver/fuzz_test.go`:

#### FuzzSQL
Tests the full database driver with random SQL statements including:
- DDL (CREATE, DROP, ALTER)
- DML (INSERT, UPDATE, DELETE)
- Queries (SELECT with various clauses)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- PRAGMA statements
- Malformed SQL
- SQL injection attempts

**Goal**: Ensure the driver never panics, even with invalid SQL.

#### FuzzPreparedStatement
Tests prepared statements with random SQL and parameters:
- Parameter binding
- Type conversions
- Edge cases with placeholders
- Statement reuse

**Goal**: Verify prepared statement safety and correctness.

#### FuzzTransaction
Tests transaction handling with random SQL sequences:
- Nested transactions
- Commit/rollback behavior
- Error handling during transactions

**Goal**: Ensure ACID properties are maintained.

#### FuzzConcurrentAccess
Tests concurrent database access from multiple goroutines:
- Race condition detection
- Lock ordering
- Concurrent reads/writes

**Goal**: Verify thread-safety and prevent deadlocks.

### Parser Fuzz Tests

Located in `/internal/parser/fuzz_test.go`:

#### FuzzParse
Tests the SQL parser with random input including:
- All SQL statement types
- Various expression forms
- Nested structures
- Deeply nested parentheses
- Very long inputs
- Special characters
- Unicode
- Malformed SQL

**Goal**: Parser should never panic, always return valid AST or error.

#### FuzzLexer
Tests the lexer/tokenizer independently:
- All token types
- String literals with escapes
- Comments
- Whitespace handling
- Invalid characters

**Goal**: Lexer should handle any byte sequence without panicking.

#### FuzzParseExpression
Focused fuzzing of expression parsing:
- Binary operators
- Function calls
- CASE expressions
- Subqueries
- Column references

**Goal**: Expression parser robustness.

#### FuzzParseTableName
Tests table name parsing:
- Schema-qualified names
- Quoted identifiers
- Aliases
- Special characters

**Goal**: Identifier parsing correctness.

#### FuzzParseCreateTable
Tests CREATE TABLE statement parsing:
- Column definitions
- Constraints
- Table options
- CREATE TABLE AS

**Goal**: DDL parsing robustness.

### VDBE Fuzz Tests

Located in `/internal/vdbe/fuzz_test.go`:

#### FuzzDecodeRecord
Tests record decoding with random binary data:
- Valid record structures
- Truncated records
- Invalid header sizes
- Invalid serial types
- Buffer overflow protection

**Goal**: Decoder should never panic or cause memory issues.

#### FuzzEncodeRecord
Tests record encoding with random values:
- All supported types (NULL, int, float, text, blob)
- Edge case values
- Large values

**Goal**: Encoder produces valid records.

#### FuzzEncodeDecodeRoundTrip
Tests that encode->decode is a valid round trip:
- Value preservation
- Type preservation
- No data loss

**Goal**: Verify encoding/decoding correctness.

#### FuzzVarint
Tests varint encoding/decoding:
- All sizes (1-9 bytes)
- Edge cases
- Round-trip correctness

**Goal**: Varint codec correctness.

### Fuzz Test Corpus

Fuzz tests maintain a corpus of interesting inputs in:
- `testdata/fuzz/<FuzzTestName>/`

These are automatically generated and minimized by the fuzzer. They serve as:
1. Regression tests (run during normal `go test`)
2. Seed inputs for future fuzzing
3. Documentation of discovered edge cases

### Best Practices

1. **Don't commit large corpora** - Only commit minimal reproducing cases
2. **Run regularly** - Integrate into CI/CD for continuous fuzzing
3. **Investigate failures** - Each crash/panic should be analyzed and fixed
4. **Update seeds** - Add new seed cases when implementing new features

## SQL Logic Test (SLT) Runner

The SLT runner executes tests in SQLite's SQL Logic Test format, ensuring compatibility with SQLite.

### Location

- `/internal/testing/slt/runner.go` - SLT test runner
- `/internal/testing/slt/runner_test.go` - Tests for the runner

### SLT File Format

SQL Logic Test files (`.test`) contain:

```
# Comments start with #

# Set hash threshold for large result sets
hash-threshold 100

# Statement tests (expect success or error)
statement ok
CREATE TABLE t (id INTEGER, name TEXT)

statement error
DROP TABLE nonexistent

# Query tests with expected results
query IT
SELECT * FROM t ORDER BY id
----
1	Alice
2	Bob

# Query with sorting
query IT rowsort
SELECT * FROM t
----
1	Alice
2	Bob
```

### Directives

#### hash-threshold
```
hash-threshold <number>
```
Sets the threshold for using MD5 hash comparison. For queries returning more than this many rows, results are compared by hash instead of line-by-line.

#### statement
```
statement [ok|error] [label]
```
Executes a SQL statement and expects success (`ok`) or error (`error`).

Examples:
```
statement ok
CREATE TABLE t (id INT)

statement error
DROP TABLE nonexistent

statement ok label1
INSERT INTO t VALUES (1)
```

#### query
```
query <types> [sortmode] [label]
----
<expected results>
```

Executes a query and compares results.

**Types**: One character per column:
- `I` - Integer
- `T` - Text
- `R` - Real (float)

**Sort modes**:
- `nosort` - Compare results in exact order (default)
- `rowsort` - Sort rows before comparing
- `valuesort` - Sort all values before comparing

Examples:
```
query IT
SELECT id, name FROM t
----
1	Alice
2	Bob

query I rowsort
SELECT id FROM t
----
1
2
3

query ITR
SELECT id, name, score FROM t
----
1	Alice	95.5
```

### Using the Runner

#### Programmatic Usage

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/testing/slt"
)

func main() {
    // Open database
    db, err := sql.Open("sqlite_internal", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create runner
    runner := slt.NewRunner(db)
    runner.SetVerbose(true)
    runner.SetHashThreshold(100)

    // Run test file
    results, err := runner.RunFile("test.test")
    if err != nil {
        log.Fatal(err)
    }

    // Check results
    for _, result := range results {
        if !result.Passed {
            fmt.Printf("FAIL: %s:%d - %v\n",
                result.File, result.Line, result.Error)
        }
    }

    // Print summary
    runner.PrintSummary()
}
```

#### Runner Configuration

```go
runner := slt.NewRunner(db)

// Set hash threshold (default: 100)
runner.SetHashThreshold(50)

// Enable verbose output
runner.SetVerbose(true)

// Skip remaining tests after error
runner.SetSkipOnError(true)
```

#### Test Results

Each test returns a `TestResult`:

```go
type TestResult struct {
    File       string  // Test file name
    Line       int     // Line number in file
    TestType   string  // "statement", "query", "hash-threshold"
    SQL        string  // SQL statement
    Expected   string  // Expected result
    Actual     string  // Actual result
    Passed     bool    // Whether test passed
    Error      error   // Error if failed
}
```

#### Statistics

```go
total, passed, failed, skipped := runner.GetStats()
fmt.Printf("Passed: %d/%d\n", passed, total)

// Reset statistics
runner.ResetStats()
```

### Test Example

Complete example test file:

```
# Example SLT test file

# Create schema
statement ok
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  age INTEGER
)

# Insert data
statement ok
INSERT INTO users VALUES
  (1, 'Alice', 'alice@example.com', 30),
  (2, 'Bob', 'bob@example.com', 25),
  (3, 'Charlie', 'charlie@example.com', 35)

# Query with sorting
query IT rowsort
SELECT name, email FROM users WHERE age > 25
----
Alice	alice@example.com
Charlie	charlie@example.com

# Aggregate query
query I
SELECT COUNT(*) FROM users
----
3

# Error case
statement error
INSERT INTO users VALUES (1, 'Duplicate', 'test@example.com', 40)
```

### Creating Test Files

1. **Start with comments** - Describe what the test covers
2. **Set up schema** - CREATE TABLE statements
3. **Insert test data** - Use statement ok
4. **Run queries** - Use appropriate column types and sort modes
5. **Test edge cases** - NULL values, empty results, errors
6. **Clean up** - DROP statements if needed

### Running SLT Tests

```bash
# Run the SLT runner tests
nix-shell --run "go test -v ./internal/testing/slt"

# Run with coverage
nix-shell --run "go test -cover ./internal/testing/slt"
```

### Integration with SQLite Test Suite

The SLT runner is compatible with SQLite's official test format from https://www.sqlite.org/sqllogictest/

To use SQLite tests:

1. Download .test files from SQLite project
2. Place in a test directory
3. Run with the runner:

```go
files, _ := filepath.Glob("sqlite-tests/*.test")
for _, file := range files {
    results, err := runner.RunFile(file)
    // Check results...
}
```

**Note**: Some SQLite-specific features may not be implemented, causing expected failures.

## Continuous Testing

### In CI/CD

```yaml
# Example GitHub Actions workflow
- name: Run fuzz tests
  run: |
    nix-shell --run "go test -fuzz=. -fuzztime=60s ./internal/driver"
    nix-shell --run "go test -fuzz=. -fuzztime=60s ./internal/parser"
    nix-shell --run "go test -fuzz=. -fuzztime=60s ./internal/vdbe"

- name: Run SLT tests
  run: |
    nix-shell --run "go test -v ./internal/testing/slt"
```

### Local Development

```bash
# Quick verification (runs seed corpus)
nix-shell --run "go test ./internal/driver ./internal/parser ./internal/vdbe"

# Extended fuzzing session (run overnight)
nix-shell --run "go test -fuzz=FuzzSQL -fuzztime=8h ./internal/driver"

# Run all tests including SLT
nix-shell --run "go test ./..."
```

## Troubleshooting

### Fuzz Test Failures

When a fuzz test finds a crash:

1. **Reproduce**: The failing input is saved in `testdata/fuzz/`
2. **Debug**: Run the specific test case with `-v` flag
3. **Fix**: Address the root cause (bounds check, nil check, etc.)
4. **Verify**: Run the fuzz test again to confirm fix
5. **Commit**: Include the minimal reproducing input in corpus

### SLT Test Failures

When an SLT test fails:

1. **Check expected vs actual**: Review the diff
2. **Verify SQL**: Ensure the SQL is valid for your implementation
3. **Check features**: Some features may not be implemented yet
4. **Update tests**: If behavior is intentionally different, update expected results
5. **File issues**: Document known incompatibilities

## Test Coverage

To check test coverage:

```bash
# Coverage for all packages
nix-shell --run "go test -coverprofile=coverage.out ./..."
nix-shell --run "go tool cover -html=coverage.out"

# Coverage for specific package
nix-shell --run "go test -coverprofile=coverage.out ./internal/parser"
nix-shell --run "go tool cover -func=coverage.out"
```

## Contributing Tests

### Adding Fuzz Tests

1. Identify untested code paths
2. Create new fuzz functions following existing patterns
3. Add diverse seed corpus
4. Run locally to verify
5. Submit with PR

### Adding SLT Tests

1. Create .test file in appropriate location
2. Follow SLT format conventions
3. Test both success and error cases
4. Include comments explaining test purpose
5. Verify tests pass locally
6. Submit with PR

## References

- [Go Fuzzing Documentation](https://go.dev/doc/fuzz/)
- [SQLite Test Suite](https://www.sqlite.org/testing.html)
- [SQL Logic Test Format](https://www.sqlite.org/sqllogictest/)
- [SQLite SLT Tests Repository](https://www.sqlite.org/sqllogictest/dir?ci=tip)

## Summary

The testing infrastructure provides:

1. **Automated bug discovery** via fuzz testing
2. **Compatibility verification** via SLT tests
3. **Regression prevention** via corpus management
4. **Continuous quality** via CI/CD integration

Use both systems together for comprehensive testing:
- Fuzz tests find unexpected crashes and edge cases
- SLT tests verify correct behavior and SQLite compatibility

Regular use of both systems ensures a robust, reliable SQLite implementation.
