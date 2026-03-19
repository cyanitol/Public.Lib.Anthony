# Trinity-Equivalent Test Plan for Anthony (Pure Go SQLite)

## DO-178C Compliance Target: DAL C (with DAL A structural coverage goals)

This test plan aligns with RTCA DO-178C *Software Considerations in Airborne Systems
and Equipment Certification* and its supplements. While Anthony is not avionics
software, adopting DO-178C processes gives us a rigorous, auditable verification
framework that exceeds industry norms for database engines.

---

## 0. DO-178C Overview and Mapping

### 0.1 Design Assurance Levels (DAL)

| DAL | Failure Effect | Structural Coverage Required | Objectives |
|-----|---------------|------------------------------|-----------|
| A | Catastrophic | MC/DC + Decision + Statement | 71 (all with independence) |
| B | Hazardous | Decision + Statement | 69 |
| C | Major | Statement | 62 |
| D | Minor | None required | 26 |
| E | No effect | None required | 0 |

**Anthony targets**: DAL C process rigour with **DAL A structural coverage** (MC/DC).
We apply the full verification process from DO-178C Section 6 but relax the
independence requirement (self-verification is acceptable for non-safety software).

### 0.2 DO-178C Process Mapping

| DO-178C Process | Anthony Implementation |
|----------------|----------------------|
| **Planning** (§4) | This document + `ROADMAP.md` + `CHANGELOG.md` |
| **Development** (§5) | Source in `internal/`, reviewed via PR |
| **Verification** (§6) | Trinity test suite (this plan) |
| **Config Management** (§7) | Git, tagged releases, `go.sum` |
| **Quality Assurance** (§8) | CI pipeline, complexity checks, linting |

### 0.3 DO-178C Supplement Mapping

| Supplement | Standard | Anthony Application |
|-----------|----------|-------------------|
| **DO-330** | Tool Qualification | `go test` + `go tool cover` + `gocyclo` are the verification tools; qualified by widespread use |
| **DO-331** | Model-Based Dev | N/A |
| **DO-332** | Object-Oriented Tech | Go is not OO but we apply interface segregation and composition principles |
| **DO-333** | Formal Methods | Invariant assertions (`Assert`/`Always`/`Never`) as lightweight formal methods |

### 0.4 Verification Objectives (DO-178C §6.3)

| Objective | How We Meet It |
|-----------|---------------|
| Requirements-based test cases | Every `sqlTestCase` traces to a requirement ID |
| Requirements-based test procedures | `runSQLTests` / `runSQLTestsFreshDB` runners |
| Requirements coverage analysis | Traceability matrix in generated code comments |
| Structural coverage analysis | `go tool cover` with `-coverprofile`, target 100% statement, decision, MC/DC |
| Structural coverage supplementary | Dead code analysis via `go vet` + unused function detection |

### 0.5 Traceability Model

Every generated test case carries a requirement ID:

```
REQ-{MODULE}-{NNN}
```

Examples:
- `REQ-EXPR-001` — Binary addition with INTEGER operands
- `REQ-NULL-042` — NULL propagation through COALESCE
- `REQ-FKEY-017` — ON DELETE CASCADE removes child rows

The code generator embeds these IDs in test names and comments, enabling
automated traceability reports:

```bash
nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -v ./internal/driver/ 2>&1 | grep "REQ-"'
```

---

## 1. Build Tag: `trinity`

All Trinity tests are gated behind a build tag so they never run in normal `go test`:

```go
//go:build trinity
```

| Command | What Runs |
|---------|----------|
| `nix-shell --run 'CGO_ENABLED=0 go test ./...'` | Normal tests only |
| `nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -run "^TestTrinity" ./internal/driver/'` | Trinity only |
| `nix-shell --run 'CGO_ENABLED=0 go test -tags trinity ./internal/driver/'` | Both |
| `nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -coverprofile=trinity.out ./internal/driver/'` | Trinity + coverage |

---

## 2. Code Generation Architecture

### 2.1 Design Principles

Tests are **generated, not hand-written**. The generator is a standalone Go program
(`cmd/trinitygen/main.go`) that reads declarative specification files and emits
`_test.go` files. This gives us:

- **Single source of truth**: specs are small TOML/Go tables; test files are derived
- **Combinatorial explosion without code explosion**: 1,200+ cases from ~300 lines of spec
- **Easy maintenance**: change a spec, re-run generator, all tests update
- **DO-178C traceability**: generator auto-assigns REQ-IDs and embeds them
- **Complexity compliance**: generated functions are always ≤ 5 cyclomatic complexity

### 2.2 Generator Tool: `cmd/trinitygen`

```
cmd/trinitygen/
    main.go              -- Entry point, orchestrates generation
    spec.go              -- Spec parsing (Go structs as specs)
    emit.go              -- Go source emitter (writes _test.go files)
    emit_helpers.go      -- Shared emit utilities
    specs/
        expr.go          -- Expression operator × type matrix
        types.go         -- Type affinity specs
        func.go          -- Function coverage specs
        join.go          -- JOIN variant specs
        boundary.go      -- Boundary value specs
        null.go          -- NULL propagation specs
        insert.go        -- INSERT conflict specs
        select.go        -- SELECT feature specs
        ddl.go           -- DDL operation specs
        trans.go         -- Transaction specs
        fkey.go          -- Foreign key specs
        trigger.go       -- Trigger specs
        window.go        -- Window function specs
        collation.go     -- Collation specs
        cte.go           -- CTE specs
        compound.go      -- UNION/INTERSECT/EXCEPT specs
        pragma.go        -- PRAGMA specs
        json.go          -- JSON function specs
        fault.go         -- Fault injection specs
```

### 2.3 Spec Format

Specs are plain Go structs — no external format needed, no parser to maintain:

```go
// specs/expr.go
package specs

// BinaryOp defines one binary operator to test.
type BinaryOp struct {
    Symbol   string   // e.g. "+"
    ReqBase  string   // e.g. "REQ-EXPR" -> REQ-EXPR-001, REQ-EXPR-002, ...
    Module   string   // e.g. "expr"
}

// TypeValue defines a typed SQL literal for cross-product testing.
type TypeValue struct {
    Label   string // e.g. "int_pos"
    Literal string // e.g. "42"
    GoType  string // e.g. "int64"
    GoValue string // e.g. "int64(84)"  (expected result of 42+42)
}

var BinaryOps = []BinaryOp{
    {"+", "REQ-EXPR", "expr"},
    {"-", "REQ-EXPR", "expr"},
    {"*", "REQ-EXPR", "expr"},
    // ... 15 total
}

var TypeValues = []TypeValue{
    {"int_pos",    "42",    "int64",  ""},
    {"int_neg",    "-7",    "int64",  ""},
    {"int_zero",   "0",     "int64",  ""},
    {"float",      "3.14",  "float64",""},
    {"text",       "'abc'", "string", ""},
    // ... 25 total
}
```

### 2.4 Generator Output

The generator produces one `_test.go` file per module. Each file:
1. Has `//go:build trinity` and SPDX header
2. Contains one `TestTrinity_{Module}` function
3. Calls a generated `buildTrinity{Module}Tests()` that returns `[]sqlTestCase`
4. Every test case name includes the REQ-ID: `REQ-EXPR-001_add_int_pos_int_pos`

Example generated output (abbreviated):

```go
//go:build trinity
// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Code generated by cmd/trinitygen. DO NOT EDIT.
package driver

import "testing"

func TestTrinity_Expr(t *testing.T) {
    runSQLTestsFreshDB(t, buildTrinityExprTests())
}

func buildTrinityExprTests() []sqlTestCase {
    return []sqlTestCase{
        {name: "REQ-EXPR-001_add_int_pos_int_pos", query: "SELECT 42 + 42", wantRows: [][]interface{}{{int64(84)}}},
        {name: "REQ-EXPR-002_add_int_pos_int_neg", query: "SELECT 42 + (-7)", wantRows: [][]interface{}{{int64(35)}}},
        // ... hundreds more
    }
}
```

### 2.5 Running the Generator

```bash
# Generate all Trinity test files
nix-shell --run 'CGO_ENABLED=0 go run ./cmd/trinitygen/'

# Generate a single module
nix-shell --run 'CGO_ENABLED=0 go run ./cmd/trinitygen/ -module expr'

# Dry-run (print to stdout, don't write files)
nix-shell --run 'CGO_ENABLED=0 go run ./cmd/trinitygen/ -dry-run'

# Verify generated files compile and pass
nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -run "^TestTrinity" ./internal/driver/'
```

### 2.6 Generated File Inventory

| Generated File | Spec Source | Est. Cases | DO-178C Coverage |
|---------------|------------|-----------|-----------------|
| `trinity_expr_test.go` | `specs/expr.go` | 375 | Statement + Decision + MC/DC for expr eval |
| `trinity_types_test.go` | `specs/types.go` | 25 | Type affinity matrix |
| `trinity_null_test.go` | `specs/null.go` | 80 | NULL propagation paths |
| `trinity_func_test.go` | `specs/func.go` | 120 | All 50+ built-in functions |
| `trinity_join_test.go` | `specs/join.go` | 60 | JOIN algorithm branches |
| `trinity_boundary_test.go` | `specs/boundary.go` | 50 | Boundary value analysis |
| `trinity_select_test.go` | `specs/select.go` | 80 | SELECT variations |
| `trinity_insert_test.go` | `specs/insert.go` | 60 | INSERT/conflict handling |
| `trinity_update_test.go` | `specs/select.go` | 40 | UPDATE/DELETE |
| `trinity_ddl_test.go` | `specs/ddl.go` | 45 | CREATE/ALTER/DROP |
| `trinity_trans_test.go` | `specs/trans.go` | 35 | Transactions/savepoints |
| `trinity_fkey_test.go` | `specs/fkey.go` | 40 | Foreign key enforcement |
| `trinity_trigger_test.go` | `specs/trigger.go` | 45 | Trigger execution paths |
| `trinity_window_test.go` | `specs/window.go` | 72 | Window function frames |
| `trinity_cte_test.go` | `specs/cte.go` | 30 | CTE and recursive CTE |
| `trinity_compound_test.go` | `specs/compound.go` | 25 | UNION/INTERSECT/EXCEPT |
| `trinity_collation_test.go` | `specs/collation.go` | 45 | Collation sequences |
| `trinity_pragma_test.go` | `specs/pragma.go` | 25 | PRAGMA statements |
| `trinity_json_test.go` | `specs/json.go` | 50 | JSON functions |
| `trinity_fault_test.go` | `specs/fault.go` | ~100 | Fault injection |
| **Total** | | **~1,400** | |

---

## 3. DO-178C Structural Coverage Strategy

### 3.1 Coverage Levels

DO-178C defines three structural coverage levels. We target all three:

| Level | Definition | Go Tooling | Target |
|-------|-----------|-----------|--------|
| **Statement** | Every statement executed at least once | `go tool cover -func` | 100% of reachable code |
| **Decision** | Every branch (if/switch/for) taken both ways | `go tool cover` branch mode | 100% of decisions |
| **MC/DC** | Each condition in a decision independently affects the outcome | Custom analysis tool | 100% of conditions in critical packages |

### 3.2 MC/DC Analysis

MC/DC (Modified Condition/Decision Coverage) is the highest structural coverage
criterion, required for DAL A. For a decision with N conditions, MC/DC requires
2N test cases (not 2^N).

Example: `if a && b || c` has 3 conditions. MC/DC requires 6 test cases showing
each condition independently flipping the decision outcome.

**Implementation**: The generator produces MC/DC test vectors for critical
decision points in:
- `expr` — all comparison and logical operators
- `vdbe` — opcode dispatch (each opcode branch)
- `parser` — token classification
- `constraint` — constraint checking decisions
- `btree` — page split/merge decisions

### 3.3 Coverage Measurement Pipeline

```bash
# 1. Generate Trinity tests
nix-shell --run 'CGO_ENABLED=0 go run ./cmd/trinitygen/'

# 2. Run with coverage
nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -coverprofile=trinity.out -coverpkg=./internal/... ./internal/driver/'

# 3. Statement coverage report
nix-shell --run 'go tool cover -func=trinity.out | sort -t: -k3 -n'

# 4. HTML coverage visualization
nix-shell --run 'go tool cover -html=trinity.out -o trinity_coverage.html'

# 5. Identify uncovered lines (0% functions)
nix-shell --run 'go tool cover -func=trinity.out | grep "0.0%"'
```

### 3.4 Dead Code Elimination

DO-178C §6.4.4.2(c) requires analysis of dead code. Our approach:
- `go vet ./...` — catches unreachable code
- `staticcheck ./...` — identifies unused functions and dead branches
- Coverage gaps from Trinity run → investigate whether code is dead or undertested
- Dead code is removed, not annotated

---

## 4. Spec-Driven Combinatorial Generators

### 4.1 Expression Operator × Type Matrix

The largest generator. Produces the Cartesian product of operators and type pairs.

**Spec** (`specs/expr.go`):

```go
var Operators = []struct {
    Sym     string
    Name    string
    Eval    func(a, b string) string // SQL expression template
}{
    {"+",  "add",  func(a, b string) string { return a + " + " + b }},
    {"-",  "sub",  func(a, b string) string { return a + " - " + b }},
    {"*",  "mul",  func(a, b string) string { return a + " * " + b }},
    {"/",  "div",  func(a, b string) string { return a + " / " + b }},
    {"%",  "mod",  func(a, b string) string { return a + " % " + b }},
    {"||", "cat",  func(a, b string) string { return a + " || " + b }},
    {"=",  "eq",   func(a, b string) string { return a + " = " + b }},
    {"!=", "ne",   func(a, b string) string { return a + " != " + b }},
    {"<",  "lt",   func(a, b string) string { return a + " < " + b }},
    {">",  "gt",   func(a, b string) string { return a + " > " + b }},
    {"<=", "le",   func(a, b string) string { return a + " <= " + b }},
    {">=", "ge",   func(a, b string) string { return a + " >= " + b }},
    {"AND","and",  func(a, b string) string { return a + " AND " + b }},
    {"OR", "or",   func(a, b string) string { return a + " OR " + b }},
    {"IS", "is",   func(a, b string) string { return a + " IS " + b }},
}

var Values = []struct {
    Label   string
    SQL     string
    Affinity string
}{
    {"int_pos",      "42",         "INTEGER"},
    {"int_neg",      "-7",         "INTEGER"},
    {"int_zero",     "0",          "INTEGER"},
    {"int_max",      "9223372036854775807", "INTEGER"},
    {"int_min",      "-9223372036854775808","INTEGER"},
    {"float_pos",    "3.14",       "REAL"},
    {"float_neg",    "-2.718",     "REAL"},
    {"float_zero",   "0.0",        "REAL"},
    {"text_alpha",   "'hello'",    "TEXT"},
    {"text_empty",   "''",         "TEXT"},
    {"text_numeric", "'42'",       "TEXT"},
    {"blob_hex",     "X'DEADBEEF'","BLOB"},
    {"null",         "NULL",       "NULL"},
    // ... more
}
```

**Generator** produces: `SELECT {op(a,b)}` for every (op, a, b) triple.
Expected results are computed by a reference function or marked `wantErr: true`
for type mismatches.

**Output**: ~375 test cases, ~15 lines of spec.

### 4.2 Function Coverage Matrix

**Spec** (`specs/func.go`):

```go
var ScalarFuncs = []struct {
    Name    string
    Args    []string   // representative argument sets
    Results []string   // expected results (parallel with Args)
}{
    {"abs",     []string{"(-5)", "(5)", "(0)", "(NULL)"},
                []string{"5",    "5",   "0",   "NULL"}},
    {"length",  []string{"('hello')", "('')", "(NULL)", "(42)"},
                []string{"5",         "0",    "NULL",   "2"}},
    {"upper",   []string{"('hello')", "('')", "(NULL)"},
                []string{"HELLO",     "",     "NULL"}},
    // ... 50+ functions
}
```

**Output**: Each function × each arg set = one test case. ~120 cases from ~50 lines.

### 4.3 Boundary Value Generator

**Spec** (`specs/boundary.go`):

```go
var IntBoundaries = []struct {
    Label string
    Value string
    Desc  string
}{
    {"i64_max",       "9223372036854775807",  "INT64 maximum"},
    {"i64_max_minus1","9223372036854775806",  "INT64 max - 1"},
    {"i64_min",       "-9223372036854775808", "INT64 minimum"},
    {"i64_min_plus1", "-9223372036854775807", "INT64 min + 1"},
    {"i32_max",       "2147483647",           "INT32 maximum"},
    {"i32_min",       "-2147483648",          "INT32 minimum"},
    {"zero",          "0",                    "zero"},
    {"one",           "1",                    "one"},
    {"neg_one",       "-1",                   "negative one"},
}

var BoundaryTests = []struct {
    Expr    string  // SQL expression using $V placeholder
    Desc    string
}{
    {"$V + 0",     "identity addition"},
    {"$V + 1",     "increment (overflow at max)"},
    {"$V - 1",     "decrement (underflow at min)"},
    {"$V * 1",     "identity multiplication"},
    {"$V * -1",    "negation"},
    {"$V * 0",     "zero product"},
    {"CAST($V AS TEXT)",  "integer to text cast"},
    {"TYPEOF($V)", "type checking"},
}
```

**Output**: Each boundary × each test template = one case. ~50 cases from ~20 lines.

### 4.4 NULL Propagation Generator

**Spec** (`specs/null.go`):

```go
var NullContexts = []struct {
    Template string
    Desc     string
    Expected string
}{
    {"NULL + 42",            "null arithmetic",     "NULL"},
    {"NULL = NULL",          "null equality",       "NULL"},
    {"NULL IS NULL",         "null IS check",       "1"},
    {"NULL IS NOT NULL",     "null IS NOT check",   "0"},
    {"COALESCE(NULL, 42)",   "coalesce skip null",  "42"},
    {"COALESCE(NULL, NULL)", "coalesce all null",   "NULL"},
    {"IFNULL(NULL, 'x')",   "ifnull substitution", "x"},
    {"NULLIF(42, 42)",       "nullif match",        "NULL"},
    {"NULLIF(42, 7)",        "nullif no match",     "42"},
    {"CASE WHEN NULL THEN 1 ELSE 0 END", "null in case", "0"},
    // ... more
}
```

### 4.5 JOIN Variant Generator

```go
var JoinTypes = []string{"INNER JOIN", "LEFT JOIN", "LEFT OUTER JOIN", "CROSS JOIN", "NATURAL JOIN"}
var JoinPredicates = []struct {
    On     string
    Desc   string
}{
    {"a.id = b.ref",       "equijoin"},
    {"a.id > b.ref",       "theta join"},
    {"a.id = b.ref AND a.val > 0", "compound predicate"},
    {"1=1",                "cartesian"},
}
```

---

## 5. Fault Injection Framework

### 5.1 VFS Interface for I/O Error Injection

```go
// internal/pager/vfs.go
type VFS interface {
    Open(name string, flags int) (VFSFile, error)
    Delete(name string) error
    Access(name string) (bool, error)
}

type VFSFile interface {
    ReadAt(p []byte, off int64) (int, error)
    WriteAt(p []byte, off int64) (int, error)
    Sync() error
    Truncate(size int64) error
    Close() error
    Size() (int64, error)
}
```

### 5.2 Fault Injection Spec

```go
// specs/fault.go
var FaultScenarios = []struct {
    Setup     []string // SQL setup
    Operation string   // SQL to execute under fault
    FaultType string   // "read", "write", "sync"
    Desc      string
}{
    {[]string{"CREATE TABLE t(x)"}, "INSERT INTO t VALUES(1)", "write", "write fault during insert"},
    {[]string{"CREATE TABLE t(x)", "INSERT INTO t VALUES(1)"}, "SELECT * FROM t", "read", "read fault during select"},
    // ...
}
```

The generator produces iterating tests: for each scenario, try fault at point 1,
then 2, ..., until the operation succeeds without hitting the fault. Each point
verifies: no panic, error or success, integrity_check passes after.

### 5.3 OOM Simulation

- `PRAGMA cache_size = 1` forces pager eviction on every page load
- Large INSERT sequences with minimal cache test spill-to-disk paths
- VDBE pool with configured byte limits

### 5.4 Crash Recovery

- Write to temp file, copy journal mid-transaction, simulate crash
- Reopen database, verify journal replay
- Truncated writes at various offsets

---

## 6. Current State Assessment

### 6.1 Project Metrics

| Metric | Value |
|--------|-------|
| Source code (non-test) | ~97,000 lines across 22 packages |
| Test code | ~257,000 lines across 427 test files |
| Test-to-source ratio | ~2.6:1 (target: 15:1 via generated tests) |
| Existing test functions | ~5,378 across 410 files |
| Skipped tests | ~426 ("pre-existing failure") |

### 6.2 Package Priority (mapped to SQLite subsystems)

| Anthony Package | SQLite Equivalent | Source Lines | Test Ratio | Trinity Priority |
|----------------|-------------------|-------------|-----------|--------------|
| `vdbe` | vdbe.c | 13,759 | 1.2:1 | **Critical** |
| `pager` | pager.c, wal.c | 10,100 | 1.9:1 | **Critical** |
| `btree` | btree.c | 6,877 | 2.5:1 | **Critical** |
| `driver` | main.c | 15,201 | 6.2:1 | High |
| `parser` | tokenize.c | 6,824 | 3.0:1 | High |
| `planner` | where.c | 7,629 | 2.0:1 | High |
| `sql` | select.c | 6,927 | 1.6:1 | High |
| `functions` | func.c | 5,573 | 2.1:1 | Medium |
| `expr` | expr.c | 4,299 | 1.9:1 | Medium |
| `schema` | build.c | 3,485 | 1.6:1 | Medium |
| `constraint` | fkey.c | 3,217 | 2.7:1 | Medium |

---

## 7. Phased Implementation

### Phase 0: Generator Tool (Prerequisite)

Build `cmd/trinitygen/` — the code generator that reads specs and emits `_test.go` files.

**Deliverables:**
- `cmd/trinitygen/main.go` — CLI entry point
- `cmd/trinitygen/emit.go` — Go source emitter
- `cmd/trinitygen/specs/*.go` — all spec definitions
- `Makefile` target: `trinity-gen`

**Verification:** `go run ./cmd/trinitygen/ -dry-run` produces valid Go source.

### Phase 1: Core Coverage (Est. ~900 generated cases)

| Spec | Generated File | Cases | DO-178C Objective |
|------|---------------|-------|------------------|
| `expr.go` | `trinity_expr_test.go` | 375 | MC/DC for expression evaluation |
| `types.go` | `trinity_types_test.go` | 25 | Statement coverage for type affinity |
| `null.go` | `trinity_null_test.go` | 80 | Decision coverage for NULL paths |
| `func.go` | `trinity_func_test.go` | 120 | Statement coverage for all functions |
| `boundary.go` | `trinity_boundary_test.go` | 50 | Boundary value analysis per DO-178C §6.4.3 |
| `select.go` | `trinity_select_test.go` | 80 | Decision coverage for SELECT paths |
| `insert.go` | `trinity_insert_test.go` | 60 | Statement coverage for INSERT paths |
| `collation.go` | `trinity_collation_test.go` | 45 | Decision coverage for comparison |

### Phase 2: SQL Feature Coverage (Est. ~400 generated cases)

| Spec | Generated File | Cases |
|------|---------------|-------|
| `ddl.go` | `trinity_ddl_test.go` | 45 |
| `join.go` | `trinity_join_test.go` | 60 |
| `compound.go` | `trinity_compound_test.go` | 25 |
| `trans.go` | `trinity_trans_test.go` | 35 |
| `fkey.go` | `trinity_fkey_test.go` | 40 |
| `trigger.go` | `trinity_trigger_test.go` | 45 |
| `window.go` | `trinity_window_test.go` | 72 |
| `cte.go` | `trinity_cte_test.go` | 30 |
| `pragma.go` | `trinity_pragma_test.go` | 25 |
| `json.go` | `trinity_json_test.go` | 50 |

### Phase 3: Fault Injection & Resilience (Est. ~100 specs × N iterations)

- VFS abstraction in `internal/pager/`
- `trinity_fault_test.go` — I/O fault iteration
- `trinity_oom_test.go` — memory pressure
- `trinity_corrupt_test.go` — corruption recovery
- `trinity_io_test.go` — I/O error paths

### Phase 4: Unskip & Fix (Ongoing)

- Triage 426 skipped "pre-existing failure" tests
- Fix underlying issues, unskip in batches
- Each unskipped test adds to structural coverage

---

## 8. Later Additions

### 8.1 sqllogictest Expansion
- Integrate canonical SLT corpus in `testdata/slt/`
- Track pass rate per file
- Target: run against 7.2M SLT test cases

### 8.2 Fuzz Testing Expansion
Expand from 6 to 13+ fuzz targets:
- `FuzzRecordDecode`, `FuzzBtreeInsert`, `FuzzPagerCorrupt`
- `FuzzDatabaseFile`, `FuzzExprEval`, `FuzzCollation`
- `FuzzJSONParse`, `FuzzVarint`

### 8.3 Invariant Assertions (DO-333 Formal Methods Supplement)
- `Assert()`, `Always()`, `Never()` gated behind `//go:build debug`
- Target: 1 assertion per ~20 lines of source in critical packages
- Run full suite with debug tag to verify invariants

---

## 9. CI Pipeline

```yaml
# .github/workflows/trinity.yml
name: Trinity Verification
on: [push, pull_request]

jobs:
  trinity-generate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: cachix/install-nix-action@v24
      - name: Generate Trinity tests
        run: nix-shell --run 'CGO_ENABLED=0 go run ./cmd/trinitygen/'
      - name: Verify no diff (generated files committed)
        run: git diff --exit-code internal/driver/trinity_*_test.go

  trinity-tests:
    runs-on: ubuntu-latest
    needs: trinity-generate
    steps:
      - uses: actions/checkout@v4
      - uses: cachix/install-nix-action@v24
      - name: Run Trinity suite
        run: nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -run "^TestTrinity" -coverprofile=trinity.out -coverpkg=./internal/... ./internal/driver/'
      - name: Coverage report
        run: nix-shell --run 'go tool cover -func=trinity.out'
      - name: Traceability check (all REQ-IDs present)
        run: nix-shell --run 'CGO_ENABLED=0 go test -tags trinity -v ./internal/driver/ 2>&1 | grep -c "REQ-"'

  complexity:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: cachix/install-nix-action@v24
      - name: Cyclomatic complexity check
        run: nix-shell --run 'gocyclo -over 11 internal/'
```

---

## 10. Complexity Constraints

- All generated test functions: cyclomatic complexity ≤ 5 (generated code is flat)
- Generator tool functions: cyclomatic complexity ≤ 11
- All files: SPDX header `(Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)`
- All files: `gofmt` compliant
- All commands: `nix-shell --run '...'` with `CGO_ENABLED=0`

---

## 11. Traceability Report Format

The generator produces a traceability matrix as a Go comment block at the top of
each generated file:

```go
// Traceability Matrix (DO-178C §6.4.4.1):
//
// REQ-EXPR-001  Binary addition, INTEGER+INTEGER        -> Statement, Decision
// REQ-EXPR-002  Binary addition, INTEGER+REAL            -> Statement, Decision
// REQ-EXPR-003  Binary addition, INTEGER+NULL            -> Statement, Decision, MC/DC
// ...
//
// Coverage targets:
//   Statement: 100% of expr.go
//   Decision:  100% of binary op branches
//   MC/DC:     100% of comparison operator conditions
```

Additionally, `cmd/trinitygen/ -report` generates a standalone `Trinity_TRACEABILITY.md`
mapping every REQ-ID to its test case, source location, and coverage level.

*Last updated: 2026-03-15*
