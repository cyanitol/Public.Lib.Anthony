# Remediation Plan: 726 Skipped Tests

## Summary

726 tests are currently skipped across the Go SQLite implementation. Priority order:
1. **Quick wins** - tests that may pass now or need minor fixes
2. **Core functionality** - DISTINCT, Foreign Keys, basic query features
3. **Maximum coverage** - CTE, triggers, complex features, extensions

## Test Categories

| Category | Count | Action |
|----------|-------|--------|
| Acceptable Skips (short mode, platform) | ~77 | Keep |
| Quick Wins (may pass now or need minor fixes) | ~60 | **Priority 1** |
| Core Features (DISTINCT, FK, WHERE, etc.) | ~200 | **Priority 2** |
| Complex Features (CTE, triggers, transactions) | ~100 | **Priority 3** |
| Extensions (FTS, R-Tree, BACKUP) | ~6 | **Implement** |
| Pre-existing failures | ~133 | Add skips, fix incrementally |

## Phase 1: Quick Wins & Triage (Week 1)

### 1.1 Re-run Pre-existing Failures
Many tests marked "pre-existing failure" may pass after recent fixes. Remove skips and test:
- BETWEEN expression tests (33) - codegen exists at `expr/codegen.go:854`
- ORDER BY tests (8) - recent fixes may have resolved
- ROWID handling tests (11)

### 1.2 Windows Lock Tests (12 tests)
Windows file locking was recently implemented. Re-enable and verify.

### 1.3 Extensions (FTS, R-Tree, BACKUP) - Keep in Scope
These will be implemented in Phase 5 (not removed).

## Phase 2: High-Impact Bug Fixes (Week 2)

### 2.1 IS NULL/IS NOT NULL VDBE Loop (~45 tests)
**Root cause**: VDBE opcode handling for NULL comparison causes infinite loop
**Files**: `internal/vdbe/exec.go`
**Impact**: Single fix enables ~45 tests

### 2.2 GROUP BY Aggregate Loop (~10 tests)
**Root cause**: CTE/subquery with GROUP BY causes infinite loop
**Files**: `internal/driver/stmt_cte.go`, `internal/vdbe/exec.go`

### 2.3 Simple CASE Expression (~6 tests)
**Files**: `internal/expr/codegen.go`, `internal/vdbe/exec.go`

## Phase 3: Core Feature Implementation (Weeks 3-6)

Priority order based on tests unblocked per effort:

### 3.1 DISTINCT Clause (62 tests) - Week 3
- Detect `stmt.Distinct` in `compile_select.go`
- Use ephemeral table with unique index for deduplication
- Handle `COUNT(DISTINCT col)` in aggregate handler

**Files**:
- `internal/driver/compile_select.go`
- `internal/vdbe/exec.go` (SorterInsert dedup)

### 3.2 Foreign Key Enforcement (39 tests) - Week 3-4
- ForeignKeyManager exists at `internal/constraint/foreign_key.go`
- Wire `ValidateInsert/Update/Delete` into VDBE execution
- Implement RowReader interface using btree cursor

**Files**:
- `internal/constraint/foreign_key.go` (complete)
- `internal/driver/conn.go` (initialize FKManager)
- `internal/vdbe/exec.go` (hook into DML opcodes)

### 3.3 CHECK Constraint Enforcement (22 tests) - Week 4
- Check constraint framework exists at `internal/constraint/check.go`
- Wire into INSERT/UPDATE execution

### 3.4 WHERE Clause Complex Conditions (14 tests) - Week 4
- Nested AND/OR with short-circuit
- WHERE with function calls

### 3.5 Pager Rollback (10 tests) - Week 5
- Complete journal rollback in `internal/pager/journal.go`
- Wire into transaction abort path

### 3.6 REINDEX Command (10 tests) - Week 5
- Drop and recreate index entries

## Phase 4: Complex Features (Weeks 7-10)

### 4.1 Engine Transaction Support (17 tests)
**Depends on**: Pager rollback
- Complete savepoint stack
- Wire driver Tx to VDBE transaction opcodes

### 4.2 ATTACH/DETACH (24 tests)
- Extend VDBEContext to hold map of schemas
- Implement schema-qualified name resolution
- Add database index to transaction opcodes

### 4.3 CTE Bytecode Execution (6 tests)
- Fix jump target adjustment in `stmt_cte.go`
- Fix cursor allocation conflicts
- Implement coroutine pattern for complex CTEs

### 4.4 Trigger Execution (6 tests)
- Add callback hooks in VDBE OpInsert/OpDelete/OpUpdate
- Re-enable OLD/NEW substitution
- Connect trigger context to transaction

## Phase 5: Extensions (Weeks 11-14)

### 5.1 Full-Text Search (FTS5) (2+ tests)
- FTS5 module exists at `internal/vtab/fts5/`
- Wire into query execution for MATCH queries
- Implement ranking functions (BM25)

### 5.2 R-Tree Spatial Index (2+ tests)
- R-Tree module exists at `internal/vtab/rtree/`
- Wire into query execution for spatial queries

### 5.3 BACKUP Command (2+ tests)
- Implement database backup API
- Page-by-page copy with proper locking

## Implementation Strategy

### Parallel Tracks

**Track A: Expression/Query** (can run independently)
- BETWEEN verification
- DISTINCT implementation
- WHERE complex conditions

**Track B: Constraints** (can run independently)
- Foreign Keys
- CHECK constraints

**Track C: Storage/Transaction** (sequential)
- Pager rollback → Engine transactions → ATTACH/DETACH

**Track D: Advanced** (depends on A, B, C)
- CTE execution
- Trigger execution

## Critical Files

| File | Purpose |
|------|---------|
| `internal/vdbe/exec.go` | VDBE execution - NULL fix, FK hooks, trigger hooks |
| `internal/driver/compile_select.go` | SELECT compilation - DISTINCT, WHERE |
| `internal/expr/codegen.go` | Expression codegen - BETWEEN, CASE |
| `internal/constraint/foreign_key.go` | FK infrastructure (complete, needs wiring) |
| `internal/pager/journal.go` | Journal rollback |
| `internal/driver/stmt_cte.go` | CTE bytecode inlining |

## Success Metrics

| Milestone | Tests Fixed | Cumulative |
|-----------|-------------|------------|
| Quick wins & triage | ~60 | ~60 |
| Bug fixes (NULL, GROUP BY, CASE) | ~61 | ~121 |
| DISTINCT + Foreign Keys | ~101 | ~222 |
| CHECK + WHERE + Rollback | ~46 | ~268 |
| Transactions + ATTACH | ~41 | ~309 |
| CTE + Triggers | ~12 | ~321 |
| Extensions (FTS, R-Tree, BACKUP) | ~6 | ~327 |
| Pre-existing failures (incremental) | ~133 | ~460 |

**Final target**: ~460+ tests fixed, ~77 acceptable skips (short mode + platform), ~189 edge cases

## Acceptable Permanent Skips (~77)

- Short mode stress tests (53) - Performance tests, skip with `-short`
- Platform-specific tests (17 Unix on Windows, 7 Windows on Unix after verification)

## Pre-existing Failures Strategy

For the ~133 tests marked "pre-existing failure":
1. Keep skips in place initially
2. After each phase completion, batch-remove skips for related tests
3. Let them fail, then fix incrementally
4. Track progress separately from feature implementation
