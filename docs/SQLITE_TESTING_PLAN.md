# SQLite Testing Plan for Anthony (Go Implementation)

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Test Coverage Categories](#test-coverage-categories)
3. [Test Infrastructure](#test-infrastructure)
4. [SQLite Test Port Strategy](#sqlite-test-port-strategy)
5. [Implementation Phases](#implementation-phases)
6. [Test Metrics and Reporting](#test-metrics-and-reporting)
7. [Continuous Testing and Automation](#continuous-testing-and-automation)

---

## 1. Testing Philosophy

### 1.1 Principles

Following SQLite's rigorous testing standards, this project adopts a **testing-first approach** where:

1. **Reliability Over Features** - Every line of code must be tested under normal and exceptional conditions
2. **Quality Through Coverage** - Aim for 100% branch coverage in core paths
3. **Defense in Depth** - Multiple independent testing strategies catch different classes of bugs
4. **Testing As Documentation** - Tests demonstrate correct behavior and serve as usage examples
5. **Regression Prevention** - Every bug fix includes a test that would have caught it

### 1.2 Testing Goals

| Goal | Target | SQLite Baseline |
|------|--------|-----------------|
| Test-to-Code Ratio | ≥ 300:1 | 590:1 |
| Branch Coverage | ≥ 95% | 100% |
| Test Cases | ≥ 50,000 | 51,445+ |
| Fuzz Cases Daily | ≥ 10M | 500M |
| Assertion Density | 1 per 20 LOC | 1 per 23 LOC |

### 1.3 Go-Specific Considerations

- **Leverage Go's Testing Tools** - Use `go test`, `go test -race`, `go test -cover`
- **Table-Driven Tests** - Exploit Go's struct-based test patterns
- **Benchmark Integration** - Use Go's built-in benchmarking for performance regression
- **Example Tests** - Provide godoc-executable examples
- **Subtests** - Use `t.Run()` for hierarchical test organization

---

## 2. Test Coverage Categories

### 2.1 Logic Tests (SQL Correctness)

**Objective**: Verify correct SQL execution for all supported features.

#### 2.1.1 Query Logic Tests

**Coverage Areas**:
- SELECT statements (simple, joins, subqueries, CTEs)
- WHERE clause evaluation (operators, functions, NULL handling)
- ORDER BY, GROUP BY, HAVING
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT)
- Window functions (OVER, PARTITION BY, frame specifications)
- Set operations (UNION, INTERSECT, EXCEPT)

**Test Pattern**:
```go
func TestSelectLogic(t *testing.T) {
    tests := []struct {
        name     string
        setup    []string          // SQL setup statements
        query    string            // Query to test
        expected [][]interface{}   // Expected result rows
        err      error             // Expected error (nil if success)
    }{
        {
            name:  "simple_select",
            setup: []string{
                "CREATE TABLE t1(id INTEGER, name TEXT)",
                "INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')",
            },
            query: "SELECT id, name FROM t1 ORDER BY id",
            expected: [][]interface{}{
                {int64(1), "Alice"},
                {int64(2), "Bob"},
            },
        },
        // ... hundreds of test cases
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            defer db.Close()

            // Execute setup
            for _, stmt := range tt.setup {
                if _, err := db.Exec(stmt); err != nil {
                    t.Fatalf("setup failed: %v", err)
                }
            }

            // Execute query
            rows, err := db.Query(tt.query)
            if err != tt.err {
                t.Fatalf("expected error %v, got %v", tt.err, err)
            }
            if err != nil {
                return // Expected error case
            }
            defer rows.Close()

            // Verify results
            verifyRows(t, rows, tt.expected)
        })
    }
}
```

#### 2.1.2 DML Logic Tests

**Coverage Areas**:
- INSERT (single, multi-value, INSERT OR REPLACE, UPSERT)
- UPDATE (simple, with WHERE, with subquery)
- DELETE (simple, with WHERE, with subquery)
- Transaction semantics (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)

#### 2.1.3 DDL Logic Tests

**Coverage Areas**:
- CREATE TABLE (columns, constraints, WITHOUT ROWID)
- CREATE INDEX (single, multi-column, unique, partial)
- ALTER TABLE (RENAME, ADD COLUMN)
- DROP TABLE, DROP INDEX
- CREATE VIEW, CREATE TRIGGER

#### 2.1.4 Type Affinity and Conversion Tests

**Coverage Areas**:
- Storage classes (NULL, INTEGER, REAL, TEXT, BLOB)
- Type affinity (TEXT, NUMERIC, INTEGER, REAL, BLOB)
- Automatic type conversion
- CAST operator
- Comparison operators with mixed types

**Test Pattern**:
```go
func TestTypeAffinity(t *testing.T) {
    tests := []struct {
        colType  string
        insert   interface{}
        stored   interface{}
        affinity string
    }{
        {"INTEGER", "123", int64(123), "INTEGER"},
        {"TEXT", 123, "123", "TEXT"},
        {"NUMERIC", "3.14", 3.14, "NUMERIC"},
        {"REAL", "2.5", 2.5, "REAL"},
        {"BLOB", []byte{1,2,3}, []byte{1,2,3}, "BLOB"},
    }

    for _, tt := range tests {
        t.Run(fmt.Sprintf("%s_%v", tt.colType, tt.insert), func(t *testing.T) {
            // Test type affinity conversion
        })
    }
}
```

### 2.2 Fault Injection Testing

**Objective**: Verify graceful degradation under resource constraints and I/O failures.

#### 2.2.1 Out-of-Memory (OOM) Injection

**Implementation Strategy**:
```go
// memory/limiter.go
type MemoryLimiter struct {
    maxBytes    int64
    used        int64
    mu          sync.Mutex
    failAfter   int    // Fail after N allocations
    allocCount  int
}

func (m *MemoryLimiter) Alloc(size int) ([]byte, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.allocCount++
    if m.failAfter > 0 && m.allocCount >= m.failAfter {
        return nil, ErrOutOfMemory
    }

    if m.used + int64(size) > m.maxBytes {
        return nil, ErrOutOfMemory
    }

    m.used += int64(size)
    return make([]byte, size), nil
}
```

**Test Pattern**:
```go
func TestOOM_PageCache(t *testing.T) {
    tests := []struct {
        name        string
        maxMemory   int64
        operation   func(*sql.DB) error
        expectError bool
    }{
        {
            name:      "insufficient_cache",
            maxMemory: 4096, // Only 1 page
            operation: func(db *sql.DB) error {
                // Try to load 100 pages
                _, err := db.Query("SELECT * FROM large_table")
                return err
            },
            expectError: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            limiter := &MemoryLimiter{maxBytes: tt.maxMemory}
            db := setupDBWithMemoryLimiter(t, limiter)
            defer db.Close()

            err := tt.operation(db)
            if (err != nil) != tt.expectError {
                t.Errorf("expected error=%v, got error=%v", tt.expectError, err)
            }
        })
    }
}
```

**Coverage Areas**:
- Page cache allocation failures
- B-tree node split during OOM
- Sort buffer allocation for ORDER BY
- Hash table allocation for GROUP BY
- Temporary table creation during OOM
- Transaction rollback during OOM

#### 2.2.2 I/O Error Injection

**Implementation Strategy**:
```go
// testutil/failingfs.go
type FailingFileSystem struct {
    real        afero.Fs
    failReads   int  // Fail after N reads
    failWrites  int  // Fail after N writes
    failSyncs   int  // Fail after N syncs
}

func (fs *FailingFileSystem) ReadAt(p []byte, off int64) (int, error) {
    if fs.failReads > 0 {
        fs.failReads--
        if fs.failReads == 0 {
            return 0, ErrIORead
        }
    }
    return fs.real.ReadAt(p, off)
}
```

**Test Pattern**:
```go
func TestIO_ReadFailure(t *testing.T) {
    tests := []struct {
        name         string
        failAfter    int
        operation    string
        shouldRecover bool
    }{
        {
            name:      "read_failure_during_select",
            failAfter: 5,
            operation: "SELECT * FROM t1",
            shouldRecover: true, // Should return error, not crash
        },
        {
            name:      "write_failure_during_commit",
            failAfter: 1,
            operation: "INSERT INTO t1 VALUES (1); COMMIT",
            shouldRecover: true, // Should rollback cleanly
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            fs := &FailingFileSystem{failReads: tt.failAfter}
            db := setupDBWithFS(t, fs)
            defer db.Close()

            _, err := db.Exec(tt.operation)
            if err == nil && !tt.shouldRecover {
                t.Error("expected operation to fail")
            }

            // Verify database is still usable after error
            verifyDatabaseIntegrity(t, db)
        })
    }
}
```

**Coverage Areas**:
- Read failures during page load
- Write failures during transaction commit
- Sync failures during fsync
- Journal write failures
- WAL write failures
- Lock acquisition failures

#### 2.2.3 Database Corruption Detection

**Test Pattern**:
```go
func TestCorruption_DetectionAndRecovery(t *testing.T) {
    corruptions := []struct {
        name        string
        corruptFunc func(dbPath string) error
        shouldDetect bool
    }{
        {
            name: "invalid_page_header",
            corruptFunc: func(dbPath string) error {
                f, _ := os.OpenFile(dbPath, os.O_RDWR, 0644)
                defer f.Close()
                // Corrupt page type byte
                f.WriteAt([]byte{0xFF}, 4096) // Page 2, byte 0
                return nil
            },
            shouldDetect: true,
        },
        {
            name: "invalid_btree_structure",
            corruptFunc: func(dbPath string) error {
                // Corrupt cell pointer array
                f, _ := os.OpenFile(dbPath, os.O_RDWR, 0644)
                defer f.Close()
                f.WriteAt([]byte{0xFF, 0xFF}, 4096+8) // Invalid pointer
                return nil
            },
            shouldDetect: true,
        },
    }

    for _, tc := range corruptions {
        t.Run(tc.name, func(t *testing.T) {
            db := createTestDB(t)
            db.Close()

            // Corrupt the database
            tc.corruptFunc(db.Path())

            // Reopen and verify corruption is detected
            db2, err := sql.Open("sqlite_internal", db.Path())
            defer db2.Close()

            _, err = db2.Query("SELECT * FROM sqlite_master")
            if tc.shouldDetect && err == nil {
                t.Error("expected corruption to be detected")
            }
        })
    }
}
```

### 2.3 Fuzz Testing

**Objective**: Discover edge cases, crashes, and security vulnerabilities through randomized inputs.

#### 2.3.1 SQL Parser Fuzzing

**Implementation**:
```go
// fuzz_test.go
func FuzzSQLParser(f *testing.F) {
    // Seed corpus with valid SQL
    seeds := []string{
        "SELECT * FROM t1",
        "INSERT INTO t1 VALUES (1)",
        "CREATE TABLE t1(x INTEGER)",
        "SELECT x FROM t1 WHERE x > 10 ORDER BY x",
    }

    for _, seed := range seeds {
        f.Add(seed)
    }

    f.Fuzz(func(t *testing.T, sql string) {
        // Parser should never crash, even on invalid input
        defer func() {
            if r := recover(); r != nil {
                t.Errorf("parser panicked on input: %q", sql)
            }
        }()

        parser := parser.New()
        _, err := parser.Parse(sql)
        // Error is fine, panic is not
        _ = err
    })
}
```

**Coverage Areas**:
- Malformed SQL syntax
- Extremely long identifiers
- Deeply nested expressions
- Unicode and special characters
- SQL injection patterns
- Comment edge cases

#### 2.3.2 Record Format Fuzzing

**Implementation**:
```go
func FuzzRecordDecoder(f *testing.F) {
    // Seed with valid record formats
    validRecords := [][]byte{
        encodeRecord([]interface{}{int64(1), "test"}),
        encodeRecord([]interface{}{nil, 3.14, []byte{1,2,3}}),
    }

    for _, rec := range validRecords {
        f.Add(rec)
    }

    f.Fuzz(func(t *testing.T, data []byte) {
        defer func() {
            if r := recover(); r != nil {
                t.Errorf("record decoder panicked: %v\nInput: %x", r, data)
            }
        }()

        decoder := vdbe.NewRecordDecoder(data)
        _, err := decoder.Decode()
        // Should handle gracefully
        _ = err
    })
}
```

#### 2.3.3 Database File Fuzzing

**Implementation**:
```go
func FuzzDatabaseFile(f *testing.F) {
    // Seed with minimal valid database
    validDB := createMinimalDatabase()
    f.Add(validDB)

    f.Fuzz(func(t *testing.T, dbData []byte) {
        tmpFile := filepath.Join(t.TempDir(), "fuzz.db")
        if err := os.WriteFile(tmpFile, dbData, 0644); err != nil {
            return
        }

        defer func() {
            if r := recover(); r != nil {
                t.Errorf("database open panicked: %v", r)
            }
        }()

        // Should not crash on malformed database
        db, err := sql.Open("sqlite_internal", tmpFile)
        if err != nil {
            return // Expected for invalid databases
        }
        defer db.Close()

        // Try basic operations
        db.Query("SELECT * FROM sqlite_master")
    })
}
```

#### 2.3.4 Pager/Cache Fuzzing

**Test concurrent access patterns**:
```go
func FuzzPagerConcurrency(f *testing.F) {
    f.Add(uint64(12345)) // Random seed

    f.Fuzz(func(t *testing.T, seed uint64) {
        rng := rand.New(rand.NewSource(int64(seed)))

        pager := createTestPager(t)
        defer pager.Close()

        // Spawn random operations
        var wg sync.WaitGroup
        for i := 0; i < 10; i++ {
            wg.Add(1)
            go func() {
                defer wg.Done()
                for j := 0; j < 100; j++ {
                    pageNum := uint32(rng.Intn(100) + 1)

                    switch rng.Intn(3) {
                    case 0: // Read
                        pager.Get(pageNum)
                    case 1: // Write
                        if page, err := pager.Get(pageNum); err == nil {
                            pager.Write(page)
                        }
                    case 2: // Unref
                        if page, err := pager.Get(pageNum); err == nil {
                            pager.Unref(page)
                        }
                    }
                }
            }()
        }
        wg.Wait()
    })
}
```

### 2.4 Boundary Value Testing

**Objective**: Test system limits and edge cases.

#### 2.4.1 Size Limits

**Test Cases**:
```go
func TestBoundaries_SizeLimits(t *testing.T) {
    limits := []struct {
        name     string
        test     func(*testing.T)
    }{
        {
            name: "max_sql_length",
            test: func(t *testing.T) {
                // SQLite max SQL length: 1,000,000 bytes
                sql := "SELECT " + strings.Repeat("1,", 250000) + "1"
                db := setupTestDB(t)
                defer db.Close()
                _, err := db.Query(sql)
                // Should handle or error gracefully
                _ = err
            },
        },
        {
            name: "max_column_count",
            test: func(t *testing.T) {
                // SQLite max: 2000 columns
                cols := make([]string, 2001)
                for i := range cols {
                    cols[i] = fmt.Sprintf("col%d INTEGER", i)
                }
                sql := "CREATE TABLE t1(" + strings.Join(cols, ",") + ")"

                db := setupTestDB(t)
                defer db.Close()
                _, err := db.Exec(sql)
                if err == nil {
                    t.Error("should reject table with > 2000 columns")
                }
            },
        },
        {
            name: "max_row_size",
            test: func(t *testing.T) {
                // Test inserting row larger than page size
                largeData := strings.Repeat("x", 100000)
                db := setupTestDB(t)
                defer db.Close()

                db.Exec("CREATE TABLE t1(data TEXT)")
                _, err := db.Exec("INSERT INTO t1 VALUES (?)", largeData)
                if err != nil {
                    t.Errorf("failed to insert large row: %v", err)
                }
            },
        },
        {
            name: "max_page_count",
            test: func(t *testing.T) {
                // Maximum database size
                // Test database with billions of pages (may skip in practice)
                t.Skip("max page count test requires TB of storage")
            },
        },
    }

    for _, lim := range limits {
        t.Run(lim.name, lim.test)
    }
}
```

#### 2.4.2 Numeric Boundaries

```go
func TestBoundaries_NumericLimits(t *testing.T) {
    tests := []struct {
        name  string
        value interface{}
        sql   string
    }{
        {"int64_min", int64(-9223372036854775808), "INSERT INTO t1 VALUES (?)"},
        {"int64_max", int64(9223372036854775807), "INSERT INTO t1 VALUES (?)"},
        {"float64_min", math.SmallestNonzeroFloat64, "INSERT INTO t1 VALUES (?)"},
        {"float64_max", math.MaxFloat64, "INSERT INTO t1 VALUES (?)"},
        {"float64_inf", math.Inf(1), "INSERT INTO t1 VALUES (?)"},
        {"float64_nan", math.NaN(), "INSERT INTO t1 VALUES (?)"},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            defer db.Close()

            db.Exec("CREATE TABLE t1(x)")
            _, err := db.Exec(tt.sql, tt.value)
            if err != nil {
                t.Errorf("failed to insert %v: %v", tt.value, err)
            }

            // Verify round-trip
            var result interface{}
            db.QueryRow("SELECT x FROM t1").Scan(&result)
            // Handle special float cases (NaN != NaN)
            if !valuesEqual(tt.value, result) {
                t.Errorf("round-trip failed: %v != %v", tt.value, result)
            }
        })
    }
}
```

### 2.5 Regression Testing

**Objective**: Ensure historical bugs never return.

#### 2.5.1 Bug Database

**Structure**:
```go
// testdata/regressions/
// Each bug gets a unique ID and test file
// Format: bug_NNNN_short_description_test.go

// testdata/regressions/bug_0001_null_comparison_test.go
func TestRegression_0001_NullComparison(t *testing.T) {
    // Bug: NULL = NULL returned true instead of NULL
    db := setupTestDB(t)
    defer db.Close()

    var result interface{}
    db.QueryRow("SELECT NULL = NULL").Scan(&result)

    if result != nil {
        t.Errorf("NULL = NULL should return NULL, got %v", result)
    }
}

// testdata/regressions/bug_0042_overflow_in_aggregate_test.go
func TestRegression_0042_AggregateOverflow(t *testing.T) {
    // Bug: SUM() overflowed silently for large integers
    db := setupTestDB(t)
    defer db.Close()

    db.Exec("CREATE TABLE t1(x INTEGER)")
    db.Exec("INSERT INTO t1 VALUES (?), (?)",
        int64(9223372036854775807), int64(1))

    var result sql.NullInt64
    db.QueryRow("SELECT SUM(x) FROM t1").Scan(&result)

    // Should handle overflow gracefully (return REAL or NULL)
    // Exact behavior TBD, but shouldn't crash or wrap around
}
```

#### 2.5.2 Regression Test Automation

```go
// Automatically run all regression tests
func TestAllRegressions(t *testing.T) {
    // Run all tests in testdata/regressions/
    regressionDir := "testdata/regressions"

    files, err := filepath.Glob(filepath.Join(regressionDir, "bug_*_test.go"))
    if err != nil {
        t.Fatal(err)
    }

    t.Logf("Running %d regression tests", len(files))

    for _, file := range files {
        // Tests run automatically via go test
        // This is just tracking
    }
}
```

### 2.6 Concurrent/Stress Testing

**Objective**: Verify thread safety and race conditions.

#### 2.6.1 Concurrent Reader/Writer Tests

```go
func TestConcurrency_MultipleReaders(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    // Setup test data
    db.Exec("CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)")
    for i := 0; i < 1000; i++ {
        db.Exec("INSERT INTO t1 VALUES (?, ?)", i, fmt.Sprintf("value%d", i))
    }

    // Spawn multiple readers
    const numReaders = 20
    var wg sync.WaitGroup
    errors := make(chan error, numReaders)

    for i := 0; i < numReaders; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()

            for j := 0; j < 100; j++ {
                rows, err := db.Query("SELECT id, value FROM t1 WHERE id < ?", 100)
                if err != nil {
                    errors <- fmt.Errorf("reader %d: %w", id, err)
                    return
                }

                count := 0
                for rows.Next() {
                    var id int
                    var value string
                    rows.Scan(&id, &value)
                    count++
                }
                rows.Close()

                if count != 100 {
                    errors <- fmt.Errorf("reader %d: expected 100 rows, got %d", id, count)
                }
            }
        }(i)
    }

    wg.Wait()
    close(errors)

    for err := range errors {
        t.Error(err)
    }
}

func TestConcurrency_ReadersAndWriters(t *testing.T) {
    // Test with WAL mode enabled for concurrent access
    db := setupTestDB(t)
    defer db.Close()

    db.Exec("PRAGMA journal_mode=WAL")
    db.Exec("CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)")

    var wg sync.WaitGroup
    stopChan := make(chan struct{})

    // Spawn writers
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func(writerID int) {
            defer wg.Done()
            for {
                select {
                case <-stopChan:
                    return
                default:
                    db.Exec("INSERT INTO t1(value) VALUES (?)", writerID)
                    time.Sleep(time.Millisecond)
                }
            }
        }(i)
    }

    // Spawn readers
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                select {
                case <-stopChan:
                    return
                default:
                    rows, _ := db.Query("SELECT COUNT(*) FROM t1")
                    if rows != nil {
                        rows.Close()
                    }
                    time.Sleep(time.Millisecond)
                }
            }
        }()
    }

    // Run for 5 seconds
    time.Sleep(5 * time.Second)
    close(stopChan)
    wg.Wait()

    // Verify database integrity
    var count int
    db.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
    t.Logf("Final row count: %d", count)
}
```

#### 2.6.2 Lock Ordering Tests

```go
func TestConcurrency_LockOrdering(t *testing.T) {
    // Verify lock hierarchy is maintained
    // See docs/LOCK_ORDERING.md

    db := setupTestDB(t)
    defer db.Close()

    // Enable lock debugging
    lockTracer := &LockOrderTracer{}
    db.SetLockTracer(lockTracer)

    // Run complex operations
    runComplexWorkload(db)

    // Verify no lock order violations
    violations := lockTracer.GetViolations()
    if len(violations) > 0 {
        t.Errorf("Lock order violations detected:\n%s", violations)
    }
}
```

#### 2.6.3 Deadlock Detection

```go
func TestConcurrency_DeadlockAvoidance(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()

    db.Exec("CREATE TABLE t1(id INTEGER PRIMARY KEY)")
    db.Exec("CREATE TABLE t2(id INTEGER PRIMARY KEY)")

    var wg sync.WaitGroup
    deadlockDetected := false

    // Scenario: Two transactions trying to lock in opposite order
    wg.Add(2)

    go func() {
        defer wg.Done()
        tx, _ := db.Begin()
        tx.Exec("INSERT INTO t1 VALUES (1)")
        time.Sleep(100 * time.Millisecond)
        _, err := tx.Exec("INSERT INTO t2 VALUES (1)")
        if err != nil {
            deadlockDetected = true
        }
        tx.Rollback()
    }()

    go func() {
        defer wg.Done()
        tx, _ := db.Begin()
        tx.Exec("INSERT INTO t2 VALUES (2)")
        time.Sleep(100 * time.Millisecond)
        _, err := tx.Exec("INSERT INTO t1 VALUES (2)")
        if err != nil {
            deadlockDetected = true
        }
        tx.Rollback()
    }()

    wg.Wait()

    // System should detect and handle deadlock
    if !deadlockDetected {
        t.Error("deadlock scenario should be detected and handled")
    }
}
```

---

## 3. Test Infrastructure

### 3.1 Table-Driven Test Patterns

**Core Pattern**:
```go
type SQLTestCase struct {
    Name        string
    Setup       []string
    Query       string
    Args        []interface{}
    Expected    [][]interface{}
    ExpectError bool
    ErrorMsg    string
    Skip        bool
    SkipReason  string
}

func runSQLTestCases(t *testing.T, tests []SQLTestCase) {
    for _, tt := range tests {
        t.Run(tt.Name, func(t *testing.T) {
            if tt.Skip {
                t.Skip(tt.SkipReason)
            }

            db := setupTestDB(t)
            defer db.Close()

            // Execute setup
            for i, stmt := range tt.Setup {
                if _, err := db.Exec(stmt); err != nil {
                    t.Fatalf("setup[%d] failed: %v\nSQL: %s", i, err, stmt)
                }
            }

            // Execute test query
            rows, err := db.Query(tt.Query, tt.Args...)
            if tt.ExpectError {
                if err == nil {
                    t.Error("expected error, got nil")
                }
                if tt.ErrorMsg != "" && !strings.Contains(err.Error(), tt.ErrorMsg) {
                    t.Errorf("error message mismatch\nExpected: %s\nGot: %s",
                        tt.ErrorMsg, err.Error())
                }
                return
            }

            if err != nil {
                t.Fatalf("unexpected error: %v", err)
            }
            defer rows.Close()

            // Verify results
            verifyResults(t, rows, tt.Expected)
        })
    }
}
```

### 3.2 Test Helpers and Fixtures

#### 3.2.1 Database Setup Helpers

```go
// testutil/helpers.go

// TestDB wraps sql.DB with test-specific functionality
type TestDB struct {
    *sql.DB
    path      string
    cleanup   func()
}

// setupTestDB creates a temporary database for testing
func setupTestDB(t *testing.T) *TestDB {
    t.Helper()

    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.db")

    db, err := sql.Open("sqlite_internal", dbPath)
    if err != nil {
        t.Fatalf("failed to open database: %v", err)
    }

    return &TestDB{
        DB:   db,
        path: dbPath,
        cleanup: func() {
            db.Close()
        },
    }
}

// setupTestDBWithOptions allows customization
func setupTestDBWithOptions(t *testing.T, opts TestDBOptions) *TestDB {
    t.Helper()

    db := setupTestDB(t)

    if opts.WALMode {
        db.Exec("PRAGMA journal_mode=WAL")
    }

    if opts.ForeignKeys {
        db.Exec("PRAGMA foreign_keys=ON")
    }

    if opts.PageSize > 0 {
        db.Exec(fmt.Sprintf("PRAGMA page_size=%d", opts.PageSize))
    }

    return db
}

type TestDBOptions struct {
    WALMode     bool
    ForeignKeys bool
    PageSize    int
    MemoryMode  bool
}
```

#### 3.2.2 Result Verification Helpers

```go
// verifyResults compares query results with expected values
func verifyResults(t *testing.T, rows *sql.Rows, expected [][]interface{}) {
    t.Helper()

    cols, err := rows.Columns()
    if err != nil {
        t.Fatalf("failed to get columns: %v", err)
    }

    var actual [][]interface{}
    for rows.Next() {
        row := make([]interface{}, len(cols))
        rowPtrs := make([]interface{}, len(cols))
        for i := range row {
            rowPtrs[i] = &row[i]
        }

        if err := rows.Scan(rowPtrs...); err != nil {
            t.Fatalf("scan error: %v", err)
        }

        actual = append(actual, row)
    }

    if err := rows.Err(); err != nil {
        t.Fatalf("rows iteration error: %v", err)
    }

    if len(actual) != len(expected) {
        t.Errorf("row count mismatch: got %d, want %d", len(actual), len(expected))
        return
    }

    for i := range actual {
        for j := range actual[i] {
            if !compareValues(actual[i][j], expected[i][j]) {
                t.Errorf("row %d, col %d: got %v (%T), want %v (%T)",
                    i, j, actual[i][j], actual[i][j], expected[i][j], expected[i][j])
            }
        }
    }
}

// compareValues handles type-aware comparison
func compareValues(actual, expected interface{}) bool {
    // Handle nil
    if expected == nil {
        return actual == nil
    }
    if actual == nil {
        return false
    }

    // Handle byte slices
    if exp, ok := expected.([]byte); ok {
        act, ok := actual.([]byte)
        if !ok {
            return false
        }
        return bytes.Equal(act, exp)
    }

    // Handle float comparison with epsilon
    if exp, ok := expected.(float64); ok {
        act, ok := actual.(float64)
        if !ok {
            return false
        }
        return math.Abs(act-exp) < 1e-9
    }

    // Default comparison
    return reflect.DeepEqual(actual, expected)
}
```

#### 3.2.3 Data Generation Helpers

```go
// generateTestData creates realistic test data
func generateTestData(t *testing.T, db *sql.DB, spec DataSpec) {
    t.Helper()

    switch spec.Type {
    case "sequential_integers":
        for i := spec.Start; i < spec.Start+spec.Count; i++ {
            db.Exec("INSERT INTO "+spec.Table+" VALUES (?)", i)
        }

    case "random_strings":
        for i := 0; i < spec.Count; i++ {
            db.Exec("INSERT INTO "+spec.Table+" VALUES (?)", randomString(spec.Length))
        }

    case "datetime_series":
        start := time.Now()
        for i := 0; i < spec.Count; i++ {
            ts := start.Add(time.Duration(i) * spec.Interval)
            db.Exec("INSERT INTO "+spec.Table+" VALUES (?)", ts.Unix())
        }
    }
}

type DataSpec struct {
    Type     string
    Table    string
    Count    int
    Start    int
    Length   int
    Interval time.Duration
}
```

### 3.3 Coverage Measurement

#### 3.3.1 Branch Coverage Tracking

```go
// Run tests with coverage
// $ go test -coverprofile=coverage.out ./...
// $ go tool cover -html=coverage.out -o coverage.html

// coverage_test.go
func TestCoverageReport(t *testing.T) {
    // Generate coverage report
    cmd := exec.Command("go", "test", "-coverprofile=coverage.out", "./...")
    output, err := cmd.CombinedOutput()
    if err != nil {
        t.Logf("Test output:\n%s", output)
        t.Fatalf("coverage test failed: %v", err)
    }

    // Parse coverage
    coverage := parseCoverageProfile("coverage.out")

    // Verify minimum coverage thresholds
    thresholds := map[string]float64{
        "internal/btree":     90.0,
        "internal/pager":     90.0,
        "internal/parser":    85.0,
        "internal/vdbe":      90.0,
        "internal/driver":    80.0,
    }

    for pkg, minCov := range thresholds {
        if cov := coverage[pkg]; cov < minCov {
            t.Errorf("package %s coverage %.1f%% below threshold %.1f%%",
                pkg, cov, minCov)
        }
    }
}
```

#### 3.3.2 Assertion Coverage

```go
// Track assertion execution
type AssertionTracker struct {
    mu         sync.Mutex
    assertions map[string]int
}

func (at *AssertionTracker) Assert(condition bool, id string) {
    at.mu.Lock()
    at.assertions[id]++
    at.mu.Unlock()

    if !condition {
        panic(fmt.Sprintf("assertion failed: %s", id))
    }
}

// Report assertions never executed
func (at *AssertionTracker) Report() {
    for id, count := range at.assertions {
        if count == 0 {
            fmt.Printf("WARNING: assertion %s never executed\n", id)
        }
    }
}
```

---

## 4. SQLite Test Port Strategy

### 4.1 Understanding SQLite TCL Tests

**SQLite Test Structure**:
```tcl
# test/select1.test
do_test select1-1.1 {
  execsql {
    CREATE TABLE t1(a,b,c);
    INSERT INTO t1 VALUES(1,2,3);
    SELECT * FROM t1;
  }
} {1 2 3}

do_test select1-1.2 {
  execsql {
    SELECT a, b FROM t1 WHERE a=1;
  }
} {1 2}
```

### 4.2 Mapping TCL to Go

**Translation Pattern**:
```go
// Equivalent Go test
func TestSelect1(t *testing.T) {
    tests := []SQLTestCase{
        {
            Name: "select1-1.1",
            Setup: []string{
                "CREATE TABLE t1(a,b,c)",
                "INSERT INTO t1 VALUES(1,2,3)",
            },
            Query: "SELECT * FROM t1",
            Expected: [][]interface{}{
                {int64(1), int64(2), int64(3)},
            },
        },
        {
            Name: "select1-1.2",
            Setup: []string{
                "CREATE TABLE t1(a,b,c)",
                "INSERT INTO t1 VALUES(1,2,3)",
            },
            Query: "SELECT a, b FROM t1 WHERE a=1",
            Expected: [][]interface{}{
                {int64(1), int64(2)},
            },
        },
    }

    runSQLTestCases(t, tests)
}
```

### 4.3 TCL Test Utilities Translation

**Common TCL Commands → Go Helpers**:

| TCL Command | Go Equivalent |
|-------------|---------------|
| `execsql {SQL}` | `db.Exec(sql)` or `db.Query(sql)` |
| `catchsql {SQL}` | Check `err != nil` |
| `do_test name {...} {expected}` | Table-driven test case |
| `integrity_check` | `PRAGMA integrity_check` |
| `finish_test` | `t.Cleanup()` |

**Implementation**:
```go
// execsql executes SQL and returns results as interface{} slice
func execsql(t *testing.T, db *sql.DB, query string) []interface{} {
    t.Helper()

    rows, err := db.Query(query)
    if err != nil {
        t.Fatalf("execsql failed: %v\nSQL: %s", err, query)
    }
    defer rows.Close()

    var results []interface{}
    cols, _ := rows.Columns()

    for rows.Next() {
        row := make([]interface{}, len(cols))
        rowPtrs := make([]interface{}, len(cols))
        for i := range row {
            rowPtrs[i] = &row[i]
        }
        rows.Scan(rowPtrs...)

        // Flatten to single slice (SQLite TCL style)
        results = append(results, row...)
    }

    return results
}

// catchsql executes SQL and returns [result, error] tuple
func catchsql(db *sql.DB, query string) ([]interface{}, error) {
    rows, err := db.Query(query)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var results []interface{}
    cols, _ := rows.Columns()

    for rows.Next() {
        row := make([]interface{}, len(cols))
        rowPtrs := make([]interface{}, len(cols))
        for i := range row {
            rowPtrs[i] = &row[i]
        }
        rows.Scan(rowPtrs...)
        results = append(results, row...)
    }

    return results, nil
}
```

### 4.4 SQL Logic Test (SLT) Integration

**SLT Format**:
```
# Test case from SLT suite
statement ok
CREATE TABLE t1(a INTEGER, b TEXT)

statement ok
INSERT INTO t1 VALUES (1, 'one'), (2, 'two')

query II rowsort
SELECT * FROM t1
----
1 one
2 two
```

**Go Parser**:
```go
type SLTTest struct {
    Type     string   // "statement" or "query"
    Expected string   // "ok" or "error"
    Query    string
    Results  []string
    Sort     bool     // rowsort flag
}

func parseSLTFile(path string) ([]SLTTest, error) {
    content, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }

    lines := strings.Split(string(content), "\n")
    var tests []SLTTest

    for i := 0; i < len(lines); i++ {
        line := strings.TrimSpace(lines[i])

        if strings.HasPrefix(line, "#") || line == "" {
            continue
        }

        if strings.HasPrefix(line, "statement ") {
            test := SLTTest{
                Type:     "statement",
                Expected: strings.TrimPrefix(line, "statement "),
            }

            // Read SQL (next lines until blank)
            i++
            for i < len(lines) && strings.TrimSpace(lines[i]) != "" {
                test.Query += lines[i] + "\n"
                i++
            }

            tests = append(tests, test)
        } else if strings.HasPrefix(line, "query ") {
            parts := strings.Fields(line)
            test := SLTTest{
                Type: "query",
                Sort: len(parts) > 2 && parts[2] == "rowsort",
            }

            // Read query
            i++
            for i < len(lines) && lines[i] != "----" {
                test.Query += lines[i] + "\n"
                i++
            }

            // Read expected results
            i++ // Skip "----"
            for i < len(lines) && strings.TrimSpace(lines[i]) != "" {
                test.Results = append(test.Results, lines[i])
                i++
            }

            tests = append(tests, test)
        }
    }

    return tests, nil
}

func runSLTTests(t *testing.T, tests []SLTTest) {
    db := setupTestDB(t)
    defer db.Close()

    for i, test := range tests {
        t.Run(fmt.Sprintf("slt_%d", i), func(t *testing.T) {
            if test.Type == "statement" {
                _, err := db.Exec(test.Query)
                if test.Expected == "ok" && err != nil {
                    t.Errorf("expected ok, got error: %v", err)
                } else if test.Expected == "error" && err == nil {
                    t.Error("expected error, got ok")
                }
            } else {
                rows, err := db.Query(test.Query)
                if err != nil {
                    t.Fatal(err)
                }
                defer rows.Close()

                var results []string
                for rows.Next() {
                    // Scan and format results
                    // Compare with test.Results
                }

                if test.Sort {
                    sort.Strings(results)
                }

                if !reflect.DeepEqual(results, test.Results) {
                    t.Errorf("results mismatch\nExpected: %v\nGot: %v",
                        test.Results, results)
                }
            }
        })
    }
}
```

### 4.5 Test Organization

**Directory Structure**:
```
tests/
├── ported/                 # Tests ported from SQLite
│   ├── select/
│   │   ├── select1_test.go
│   │   ├── select2_test.go
│   │   └── select_join_test.go
│   ├── insert/
│   ├── update/
│   ├── delete/
│   ├── expr/
│   ├── aggregate/
│   └── ...
├── slt/                    # SQL Logic Tests
│   ├── select1.test
│   ├── select2.test
│   └── ...
├── regression/             # Bug regression tests
├── fuzz/                   # Fuzz tests
└── benchmark/              # Performance benchmarks
```

---

## 5. Implementation Phases

### Phase 1: Foundation (Months 1-2)

**Goal**: Establish test infrastructure and cover existing functionality.

#### Milestones:

1. **Test Infrastructure Setup**
   - [ ] Create test helper package (`testutil/`)
   - [ ] Implement `setupTestDB()` and result verification helpers
   - [ ] Set up table-driven test framework
   - [ ] Configure coverage reporting

2. **Core Component Tests**
   - [ ] B-tree tests: 100 test cases
   - [ ] Pager tests: 80 test cases
   - [ ] Parser tests: 200 test cases
   - [ ] VDBE tests: 150 test cases

3. **Integration Tests**
   - [ ] End-to-end SELECT tests: 50 cases
   - [ ] End-to-end INSERT tests: 30 cases
   - [ ] Transaction tests: 20 cases

**Deliverable**: 630+ test cases, 75%+ coverage on core components

### Phase 2: SQL Logic Coverage (Months 3-4)

**Goal**: Port SQLite test suite for SQL correctness.

#### Milestones:

1. **SELECT Statement Coverage**
   - [ ] Basic SELECT: 200 tests
   - [ ] WHERE clause: 150 tests
   - [ ] JOIN operations: 100 tests
   - [ ] Subqueries: 80 tests
   - [ ] Aggregate functions: 60 tests
   - [ ] Window functions: 40 tests

2. **DML Statement Coverage**
   - [ ] INSERT variants: 80 tests
   - [ ] UPDATE operations: 60 tests
   - [ ] DELETE operations: 50 tests
   - [ ] UPSERT: 30 tests

3. **DDL Statement Coverage**
   - [ ] CREATE TABLE: 60 tests
   - [ ] CREATE INDEX: 40 tests
   - [ ] ALTER TABLE: 30 tests
   - [ ] Views and triggers: 40 tests

**Deliverable**: 1,020+ SQL test cases, 10,000+ test instances (via parameterization)

### Phase 3: Fault Injection (Months 5-6)

**Goal**: Harden system against failures.

#### Milestones:

1. **OOM Injection Framework**
   - [ ] Implement memory limiter interface
   - [ ] Page cache OOM tests: 30 cases
   - [ ] Sort buffer OOM tests: 20 cases
   - [ ] B-tree split OOM tests: 15 cases

2. **I/O Error Injection**
   - [ ] Implement FailingFileSystem
   - [ ] Read error tests: 40 cases
   - [ ] Write error tests: 40 cases
   - [ ] Sync error tests: 20 cases

3. **Corruption Scenarios**
   - [ ] Database file corruption: 50 cases
   - [ ] Journal corruption: 20 cases
   - [ ] WAL corruption: 20 cases

**Deliverable**: 255+ fault injection tests, all crash-free

### Phase 4: Fuzz Testing (Months 7-8)

**Goal**: Discover edge cases through randomization.

#### Milestones:

1. **Fuzzer Setup**
   - [ ] SQL parser fuzzer
   - [ ] Record decoder fuzzer
   - [ ] Database file fuzzer
   - [ ] Pager fuzzer
   - [ ] Set up continuous fuzzing with oss-fuzz

2. **Fuzz Corpus Development**
   - [ ] Seed corpus: 1,000+ valid SQL statements
   - [ ] Seed corpus: 100+ valid database files
   - [ ] Seed corpus: 500+ valid record formats

3. **Fuzz Bug Resolution**
   - [ ] Triage and fix discovered crashes
   - [ ] Add regression tests for fuzzer findings
   - [ ] Achieve 24-hour clean fuzz run

**Deliverable**: 4+ active fuzzers, 1,600+ seed corpus, 10M+ cases/day

### Phase 5: Concurrency & Stress (Months 9-10)

**Goal**: Verify thread safety and scalability.

#### Milestones:

1. **Concurrent Access Tests**
   - [ ] Multiple readers: 20 tests
   - [ ] Readers + writers: 20 tests
   - [ ] WAL mode concurrency: 15 tests

2. **Lock Testing**
   - [ ] Lock ordering verification: 30 tests
   - [ ] Deadlock scenarios: 10 tests
   - [ ] Lock hierarchy enforcement

3. **Stress Testing**
   - [ ] Large database (10GB+): 5 tests
   - [ ] High transaction rate (1000 TPS): 5 tests
   - [ ] Long-running transactions: 5 tests

**Deliverable**: 110+ concurrency tests, race-free under `-race` detector

### Phase 6: Boundary & Regression (Months 11-12)

**Goal**: Test limits and capture all historical bugs.

#### Milestones:

1. **Boundary Value Tests**
   - [ ] Size limits: 30 tests
   - [ ] Numeric limits: 40 tests
   - [ ] String limits: 20 tests
   - [ ] Nesting limits: 15 tests

2. **Regression Test Database**
   - [ ] Set up regression test tracking
   - [ ] Port all historical bug fixes: 100+ tests
   - [ ] Automated regression runs

3. **SLT Integration**
   - [ ] Implement SLT parser
   - [ ] Import official SLT test suite (7M+ queries)
   - [ ] Achieve 95%+ pass rate on SLT

**Deliverable**: 205+ boundary tests, 100+ regression tests, SLT integration

### Phase 7: Performance & Benchmarks (Month 13)

**Goal**: Establish performance baselines and detect regressions.

#### Milestones:

1. **Benchmark Suite**
   - [ ] SELECT benchmarks: 20 benchmarks
   - [ ] INSERT benchmarks: 15 benchmarks
   - [ ] UPDATE/DELETE benchmarks: 10 benchmarks
   - [ ] Transaction benchmarks: 10 benchmarks

2. **Performance Regression Detection**
   - [ ] Set up continuous benchmarking
   - [ ] Establish baseline metrics
   - [ ] Alert on >10% regressions

**Deliverable**: 55+ benchmarks, performance CI integration

### Phase 8: Documentation & Tooling (Month 14)

**Goal**: Make tests maintainable and discoverable.

#### Milestones:

1. **Test Documentation**
   - [ ] Document test organization
   - [ ] Create test writing guide
   - [ ] Add godoc examples for all public APIs

2. **Test Tooling**
   - [ ] Create test data generators
   - [ ] Build coverage visualization tools
   - [ ] Implement test result dashboard

**Deliverable**: Complete test documentation, developer guide

### Phase 9: Continuous Improvement (Ongoing)

**Goal**: Maintain and expand test coverage.

#### Ongoing Tasks:

- Add test for every bug fix
- Review coverage reports monthly
- Update tests for new features
- Rotate fuzz seed corpus
- Monitor test execution time
- Refactor slow tests

---

## 6. Test Metrics and Reporting

### 6.1 Coverage Metrics

**Track Multiple Coverage Types**:

```go
type CoverageReport struct {
    Package        string
    TotalLines     int
    CoveredLines   int
    LineCoverage   float64
    TotalBranches  int
    CoveredBranches int
    BranchCoverage float64
    Functions      []FunctionCoverage
}

type FunctionCoverage struct {
    Name           string
    Complexity     int
    Coverage       float64
    UncoveredLines []int
}
```

**Generate Reports**:
```bash
# Coverage report generation
$ ./scripts/coverage_report.sh

# Outputs:
# - coverage.html (browsable HTML)
# - coverage.json (machine-readable)
# - coverage_summary.txt (human-readable summary)
```

### 6.2 Test Statistics

**Track Test Execution Metrics**:

```go
type TestStats struct {
    TotalTests      int
    PassedTests     int
    FailedTests     int
    SkippedTests    int
    ExecutionTime   time.Duration
    Coverage        float64
    FuzzCases       int64
    MemoryPeak      uint64
}

func generateTestReport() TestStats {
    // Collect test statistics
    // Generate report
}
```

### 6.3 Quality Gates

**Enforce Minimum Standards**:

```yaml
# .github/quality_gates.yml
coverage:
  minimum: 90.0
  per_package:
    internal/btree: 95.0
    internal/pager: 95.0
    internal/vdbe: 90.0
    internal/parser: 85.0

test_count:
  minimum: 10000

fuzz:
  daily_cases: 10000000
  max_crashes: 0

performance:
  regression_threshold: 10  # percent
```

**CI Integration**:
```go
func TestQualityGates(t *testing.T) {
    gates := loadQualityGates()

    // Check coverage
    coverage := getCoverageStats()
    if coverage < gates.Coverage.Minimum {
        t.Errorf("Coverage %.1f%% below minimum %.1f%%",
            coverage, gates.Coverage.Minimum)
    }

    // Check test count
    testCount := getTestCount()
    if testCount < gates.TestCount.Minimum {
        t.Errorf("Test count %d below minimum %d",
            testCount, gates.TestCount.Minimum)
    }
}
```

---

## 7. Continuous Testing and Automation

### 7.1 CI/CD Pipeline

**GitHub Actions Workflow**:

```yaml
# .github/workflows/tests.yml
name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.26'

      - name: Run unit tests
        run: go test -v -race -cover ./...

      - name: Generate coverage report
        run: |
          go test -coverprofile=coverage.out ./...
          go tool cover -html=coverage.out -o coverage.html

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.out

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4

      - name: Run integration tests
        run: go test -v -tags=integration ./tests/integration/...

  fuzz-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4

      - name: Run fuzz tests (short)
        run: go test -fuzz=. -fuzztime=1m ./...

  slt-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4

      - name: Download SLT suite
        run: ./scripts/download_slt.sh

      - name: Run SLT tests
        run: go test -v ./tests/slt/...

  quality-gates:
    runs-on: ubuntu-latest
    needs: [unit-tests, integration-tests]
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4

      - name: Check quality gates
        run: go test -v ./tests/quality/...
```

### 7.2 Continuous Fuzzing

**OSS-Fuzz Integration**:

```dockerfile
# projects/anthony/Dockerfile
FROM gcr.io/oss-fuzz-base/base-builder-go
RUN git clone https://github.com/JuniperBible/Public.Lib.Anthony anthony
WORKDIR anthony
COPY build.sh $SRC/
```

```bash
# projects/anthony/build.sh
#!/bin/bash -eu

go get github.com/AdamKorcz/go-118-fuzz-build/testing

# Compile fuzzers
compile_go_fuzzer github.com/JuniperBible/Public.Lib.Anthony/internal/parser FuzzParser fuzz_parser
compile_go_fuzzer github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe FuzzRecordDecoder fuzz_record
compile_go_fuzzer github.com/JuniperBible/Public.Lib.Anthony/internal/pager FuzzDatabaseFile fuzz_database
```

### 7.3 Nightly Test Runs

**Extended Test Suite**:

```yaml
# .github/workflows/nightly.yml
name: Nightly Tests

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM daily

jobs:
  extended-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 360  # 6 hours

    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4

      - name: Run all tests
        run: go test -v -timeout=6h ./...

      - name: Run stress tests
        run: go test -v -tags=stress ./tests/stress/...

      - name: Run long fuzz tests
        run: go test -fuzz=. -fuzztime=4h ./...

      - name: Generate comprehensive report
        run: ./scripts/nightly_report.sh

      - name: Upload artifacts
        uses: actions/upload-artifact@v3
        with:
          name: nightly-results
          path: |
            coverage.html
            fuzz_results/
            test_report.md
```

### 7.4 Performance Benchmarking

**Benchmark Tracking**:

```yaml
# .github/workflows/benchmark.yml
name: Benchmark

on: [push, pull_request]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4

      - name: Run benchmarks
        run: go test -bench=. -benchmem ./... > new.txt

      - name: Download baseline
        run: gh run download -n baseline-benchmarks

      - name: Compare benchmarks
        run: benchstat old.txt new.txt

      - name: Check for regressions
        run: |
          if ./scripts/check_benchmark_regression.sh; then
            echo "No performance regressions detected"
          else
            echo "Performance regression detected!"
            exit 1
          fi
```

---

## Summary

This comprehensive testing plan adapts SQLite's rigorous methodology to Go, targeting:

- **50,000+ test cases** across all categories
- **90%+ branch coverage** in core components
- **10M+ daily fuzz cases** for continuous testing
- **Zero crashes** under fault injection
- **SLT suite integration** for cross-database validation

By following this phased approach over 14 months, the Anthony SQLite implementation will achieve SQLite-level reliability while leveraging Go's testing ecosystem.

**Key Success Metrics**:
- Test-to-code ratio > 300:1
- All tests pass with `-race` detector
- 95%+ SLT compatibility
- Clean 24-hour fuzz runs
- Sub-10% performance variance

The testing infrastructure will be **maintainable**, **discoverable**, and **automated**, ensuring long-term quality as the codebase evolves.
