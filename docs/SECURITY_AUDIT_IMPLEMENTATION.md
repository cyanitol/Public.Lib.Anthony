# Security Audit Implementation Plan

## Executive Summary

Comprehensive security audit of the Public.Lib.Anthony SQLite implementation identified **23 security issues** across three categories: Input Validation (10), Memory Safety (7), and Concurrency (6). This plan addresses all findings with an 11-agent implementation team.

## Security Model: Layered + Sandbox

This implementation uses a combined **Layered Defaults** and **Sandbox** security model:

1. **Layer 1 - Always Block**: Dangerous patterns (null bytes, `..`, etc.) are always rejected
2. **Layer 2 - Sandbox**: All paths resolved relative to a configurable root directory
3. **Layer 3 - Allowlist**: Optional subdirectory restrictions within the sandbox
4. **Layer 4 - Symlink Protection**: Symlinks blocked by default to prevent escapes

---

## Critical Findings Summary

### CRITICAL Severity (4 issues)

| # | Issue | Location | Description |
|---|-------|----------|-------------|
| 1 | Race Condition | stmt.go:81-92 | Unprotected `inTx` checks allow concurrent modification |
| 2 | Deadlock | stmt.go:28-48 | Lock ordering violation in Stmt.Close() |
| 3 | Use-After-Free Risk | stmt.go:63-100 | TOCTOU vulnerability in ExecContext |
| 4 | Path Traversal | stmt_attach.go:37 | ATTACH DATABASE accepts arbitrary paths |

### HIGH Severity (8 issues)

| # | Issue | Location | Description |
|---|-------|----------|-------------|
| 1 | Path Injection | stmt_vacuum.go:234 | VACUUM INTO accepts arbitrary paths |
| 2 | Integer Overflow | cell.go:272 | uint32->uint16 cast without bounds check |
| 3 | Integer Overflow | overflow.go:287 | uint32->uint16 cast without bounds check |
| 4 | Schema Race | stmt.go:517+ | Schema access without synchronization |
| 5 | Deadlock | conn.go:79-120 | Lock ordering violation in Conn.Close() |
| 6 | Buffer Overflow | record.go:89 | decodeInt24Value lacks bounds check |
| 7 | Buffer Overflow | record.go:98 | decodeInt48Value lacks bounds check |
| 8 | Missing Whitelist | parser.go:2244 | No PRAGMA whitelist enforcement |

### MEDIUM Severity (11 issues)

| # | Issue | Location | Description |
|---|-------|----------|-------------|
| 1 | Memory Exhaustion | memory_pager.go | No page count limits |
| 2 | Missing Validation | stmt.go | Parameter count not validated |
| 3 | Cache Race | btree.go:51-75 | Double-check pattern missing |
| 4 | State Race | pager.go | Pager state read without lock |
| 5 | Integer Underflow | cell.go:265 | calculateMinLocal edge case |
| 6 | Unsafe Cast | cell.go:88,94,160,167 | CellSize casts without validation |
| 7 | Missing Limits | record.go | No record size maximum |
| 8 | Query Complexity | stmt.go | Expression depth unbounded |
| 9 | File Permissions | pager.go | No permission mode enforcement |
| 10 | Reference Underflow | btree.go | Page reference count edge cases |
| 11 | Varint Edge Cases | varint.go | Incomplete bounds validation |

---

## Implementation Architecture

### New Package: internal/security

```
internal/security/
+-- config.go      # SecurityConfig struct and defaults
+-- path.go        # Path validation functions
+-- limits.go      # Resource limit constants
+-- arithmetic.go  # Safe arithmetic helpers
+-- errors.go      # Security-specific error types
+-- security_test.go # Security test suite
```

### SecurityConfig Structure

```go
// SecurityConfig provides layered security with sandbox isolation
type SecurityConfig struct {
    // Sandbox root - all paths resolved relative to this
    // Empty string disables sandbox (not recommended)
    DatabaseRoot string

    // Layer 1: Dangerous pattern blocking (always enforced)
    BlockNullBytes     bool // default: true - block null bytes in paths
    BlockTraversal     bool // default: true - block ".." sequences
    BlockSymlinks      bool // default: true - block symlink targets
    BlockAbsolutePaths bool // default: true - require relative paths

    // Layer 2: Sandbox enforcement
    EnforceSandbox bool // default: true - require DatabaseRoot prefix

    // Layer 3: Allowlist (within sandbox)
    AllowedSubdirs []string // empty = allow all within sandbox

    // File creation settings
    CreateMode os.FileMode // default: 0600 - rw owner only
    DirMode    os.FileMode // default: 0700 - rwx owner only

    // Resource limits
    MaxDatabaseSize int64 // default: 0 (unlimited)
    MaxAttachedDBs  int   // default: 10
}

func DefaultSecurityConfig() *SecurityConfig {
    return &SecurityConfig{
        BlockNullBytes:     true,
        BlockTraversal:     true,
        BlockSymlinks:      true,
        BlockAbsolutePaths: true,
        EnforceSandbox:     true,
        CreateMode:         0600,
        DirMode:            0700,
        MaxAttachedDBs:     10,
    }
}
```

### Path Validation Flow

```
User Input Path
       |
       |
+------------------+
| Layer 1: Block   |  Reject: null bytes, "..", control chars
| Dangerous Chars  |
+--------+---------+
         |
         |
+------------------+
| Layer 2: Resolve |  filepath.Clean() + Join with DatabaseRoot
| Within Sandbox   |  Verify: strings.HasPrefix(resolved, root)
+--------+---------+
         |
         |
+------------------+
| Layer 3: Check   |  If AllowedSubdirs configured,
| Allowlist        |  verify path is within allowed dirs
+--------+---------+
         |
         |
+------------------+
| Layer 4: Verify  |  Lstat() to detect symlinks
| No Symlinks      |  Follow links and verify still in sandbox
+--------+---------+
         |
         |
    Validated Path
```

---

## Agent Team Assignments

### Agent 1: Path Traversal Prevention
**Files**: `internal/security/*.go`, `internal/driver/stmt_attach.go`, `internal/driver/stmt_vacuum.go`

**Tasks**:
1. Create `internal/security/config.go` with SecurityConfig
2. Create `internal/security/path.go` with validation functions
3. Create `internal/security/errors.go` with error types
4. Add `WithSecurityConfig()` option to driver
5. Integrate validation in stmt_attach.go (ATTACH DATABASE)
6. Integrate validation in stmt_vacuum.go (VACUUM INTO)
7. Add secure file creation with mode enforcement

**Validation Functions**:
```go
func ValidateDatabasePath(cfg *SecurityConfig, path string) (string, error)
func ValidateAttachPath(cfg *SecurityConfig, path string) (string, error)
func ValidateVacuumIntoPath(cfg *SecurityConfig, path string) (string, error)
func IsPathInSandbox(root, path string) bool
func ContainsDangerousPatterns(path string) error
```

---

### Agent 2: Statement Concurrency Fixes
**Files**: `internal/driver/stmt.go`

**Issues Addressed**:
- CRITICAL: Race condition in inTx checks (lines 81-92)
- CRITICAL: Deadlock in Stmt.Close() (lines 28-48)
- CRITICAL: TOCTOU in ExecContext (lines 63-100)

**Tasks**:
1. Hold Conn.mu during transaction state checks in ExecContext()
2. Apply same pattern to QueryContext()
3. Fix Stmt.Close() lock ordering:
   - Don't call conn.removeStmt() while holding stmt.mu
   - Use atomic closed flag + deferred cleanup
4. Add TOCTOU protection by validating conn under lock
5. Add comprehensive concurrent execution tests

**Pattern**:
```go
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        return nil, ErrStmtClosed
    }

    // Hold conn lock for transaction state check
    s.conn.mu.RLock()
    inTx := s.conn.inTransaction
    connClosed := s.conn.closed
    s.conn.mu.RUnlock()

    if connClosed {
        return nil, ErrConnClosed
    }
    // ... proceed with execution
}
```

---

### Agent 3: Connection Concurrency Fixes
**Files**: `internal/driver/conn.go`, `internal/driver/driver.go`

**Issues Addressed**:
- HIGH: Lock ordering violation in Conn.Close() (lines 79-120)
- Reference counting for active operations

**Tasks**:
1. Fix Conn.Close() two-phase close pattern:
   - Phase 1: Mark closed under conn.mu, collect statements
   - Phase 2: Release conn.mu, close statements, then acquire driver.mu
2. Add connection reference counting for in-flight operations
3. Protect all shared state access
4. Update docs/LOCK_ORDERING.md
5. Add deadlock detection tests using runtime.SetMutexProfileFraction

**Pattern**:
```go
func (c *Conn) Close() error {
    // Phase 1: Mark closed and collect cleanup items
    c.mu.Lock()
    if c.closed {
        c.mu.Unlock()
        return ErrConnClosed
    }
    c.closed = true
    stmts := make([]*Stmt, len(c.statements))
    copy(stmts, c.statements)
    c.statements = nil
    c.mu.Unlock()

    // Phase 2: Close statements (no lock held)
    for _, stmt := range stmts {
        stmt.Close()
    }

    // Phase 3: Remove from driver (driver lock, not conn lock)
    c.driver.mu.Lock()
    delete(c.driver.conns, c)
    c.driver.mu.Unlock()

    return c.pager.Close()
}
```

---

### Agent 4: Integer Safety
**Files**: `internal/btree/cell.go`, `internal/btree/overflow.go`, `internal/security/arithmetic.go`

**Issues Addressed**:
- HIGH: Integer overflow in uint32->uint16 casts
- MEDIUM: Integer underflow in calculateMinLocal
- MEDIUM: Unsafe casts in CellSize calculations

**Tasks**:
1. Create `internal/security/arithmetic.go`:
```go
func SafeCastUint32ToUint16(v uint32) (uint16, error) {
    if v > math.MaxUint16 {
        return 0, ErrIntegerOverflow
    }
    return uint16(v), nil
}

func SafeAdd[T Integer](a, b T) (T, error)
func SafeSub[T Integer](a, b T) (T, error)
func SafeMul[T Integer](a, b T) (T, error)
```

2. Fix calculateLocalPayload() in cell.go:272,274
3. Fix CalculateLocalPayload() in overflow.go:287,296
4. Fix calculateMinLocal() to validate usableSize >= 35
5. Fix all CellSize casts (lines 88, 94, 160, 167, 217, 224)
6. Add boundary tests for all edge cases

---

### Agent 5: Buffer Safety
**Files**: `internal/vdbe/record.go`, `internal/btree/varint.go`

**Issues Addressed**:
- HIGH: Buffer overflow in decodeInt24Value
- HIGH: Buffer overflow in decodeInt48Value
- MEDIUM: Missing record size limits
- MEDIUM: Varint parsing edge cases

**Tasks**:
1. Add bounds checking to decodeInt24Value():
```go
func decodeInt24Value(data []byte, offset int) (int32, error) {
    if offset+3 > len(data) {
        return 0, ErrBufferOverflow
    }
    // ... existing logic
}
```

2. Add bounds checking to decodeInt48Value()
3. Validate offset after each increment in DecodeRecord
4. Add MaxRecordSize constant and enforce it
5. Fix varint edge cases for maximum values
6. Add fuzz tests for record parsing

---

### Agent 6: Resource Limits
**Files**: `internal/security/limits.go`, `internal/parser/parser.go`, `internal/pager/memory_pager.go`

**Issues Addressed**:
- MEDIUM: Memory exhaustion in memory_pager
- MEDIUM: Query complexity unbounded
- HIGH: Expression depth unbounded

**Tasks**:
1. Create `internal/security/limits.go`:
```go
const (
    // SQL parsing limits
    MaxSQLLength     = 1_000_000  // 1MB
    MaxTokens        = 10_000
    MaxExprDepth     = 100
    MaxQueryTables   = 64
    MaxParameters    = 32_767

    // Memory limits
    MaxMemoryDBPages = 100_000    // ~400MB at 4KB pages
    MaxRecordSize    = 1_000_000_000  // 1GB
    MaxBlobSize      = 1_000_000_000  // 1GB

    // Operation limits
    MaxAttachedDBs   = 10
    MaxTriggerDepth  = 32
)
```

2. Add SQL length check at parser entry
3. Track and limit expression depth during parsing
4. Add page count limit to memory_pager
5. Create PRAGMA whitelist (safe pragmas only)
6. Add parameter count validation

---

### Agent 7: Schema Safety
**Files**: `internal/schema/*.go`, `internal/driver/stmt.go`

**Issues Addressed**:
- HIGH: Schema access without synchronization

**Tasks**:
1. Add sync.RWMutex to Schema struct
2. Wrap all schema reads with RLock():
```go
func (s *Schema) GetTable(name string) *Table {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.tables[name]
}
```

3. Wrap all schema modifications with Lock()
4. Update stmt.go schema access (lines 517, 545, 1779, 2178)
5. Add schema validation for reserved names
6. Add concurrent schema access tests

---

### Agent 8: Btree Safety
**Files**: `internal/btree/btree.go`, `internal/btree/cursor.go`

**Issues Addressed**:
- MEDIUM: Cache race in GetPage()
- MEDIUM: Page reference count edge cases

**Tasks**:
1. Fix GetPage() double-check pattern:
```go
func (b *Btree) GetPage(pgno uint32) (*Page, error) {
    b.mu.RLock()
    if page, ok := b.pages[pgno]; ok {
        b.mu.RUnlock()
        return page, nil
    }
    b.mu.RUnlock()

    b.mu.Lock()
    defer b.mu.Unlock()

    // Double-check after acquiring write lock
    if page, ok := b.pages[pgno]; ok {
        return page, nil
    }

    // Load page...
}
```

2. Add page reference counting for cursor stability
3. Add integrity checks on page reads (magic numbers, checksums)
4. Validate page numbers against database size
5. Add cursor state validation

---

### Agent 9: Pager Safety
**Files**: `internal/pager/pager.go`, `internal/pager/wal_index.go`

**Issues Addressed**:
- MEDIUM: Pager state race conditions
- File permission enforcement

**Tasks**:
1. Add CRC32 checksum validation before journal rollback
2. Strengthen state transition validation (state machine pattern)
3. Synchronize all state reads
4. Add file permission checks on database creation:
```go
func createDatabase(path string, cfg *SecurityConfig) (*os.File, error) {
    f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, cfg.CreateMode)
    if err != nil {
        return nil, err
    }
    return f, nil
}
```
5. Validate lock state transitions
6. Add checksums to WAL frames

---

### Agent 10: Security Tests
**Files**: `internal/security/*_test.go`, `internal/parser/fuzz_test.go`, `internal/vdbe/fuzz_test.go`

**Tasks**:
1. Create `internal/security/security_test.go`:
   - Path traversal attack tests (50+ vectors)
   - SQL injection pattern tests
   - Integer overflow boundary tests
   - Buffer overflow tests
   - Symlink escape tests

2. Create `internal/security/path_test.go`:
```go
func TestPathTraversal(t *testing.T) {
    vectors := []string{
        "../../../etc/passwd",
        "foo/../../../etc/passwd",
        "foo/..\\..\\..\\windows\\system32",
        "foo%00bar.db",
        "foo\x00bar.db",
        "/absolute/path",
        "symlink_to_outside",
    }
    // ... test all vectors
}
```

3. Create fuzz tests:
```go
func FuzzParser(f *testing.F) {
    f.Add("SELECT * FROM t")
    f.Fuzz(func(t *testing.T, sql string) {
        _, _ = parser.Parse(sql)
    })
}
```

4. Create concurrency tests with race detector
5. Create resource exhaustion tests

---

### Agent 11: Security Documentation
**Files**: `docs/SECURITY.md`

**Tasks**:
1. Create comprehensive `docs/SECURITY.md`:
```markdown
# Security Guide

## Security Model
## Input Validation
## Concurrency Safety
## Resource Limits
## Configuration Options
## Security Checklist
## Reporting Vulnerabilities
```

2. Update `docs/LOCK_ORDERING.md` with complete hierarchy
3. Create security advisory template
4. Add security checklist for contributors
5. Document all security configuration options

---

## File Change Summary

### Files to Modify

| File | Changes |
|------|---------|
| `internal/driver/stmt.go` | Concurrency fixes, lock ordering, TOCTOU protection |
| `internal/driver/conn.go` | Two-phase Close(), reference counting |
| `internal/driver/driver.go` | SecurityConfig integration |
| `internal/driver/stmt_attach.go` | Path validation before ATTACH |
| `internal/driver/stmt_vacuum.go` | Path validation for VACUUM INTO |
| `internal/btree/btree.go` | Double-check cache pattern, page validation |
| `internal/btree/cell.go` | Integer overflow protection, safe casts |
| `internal/btree/overflow.go` | Integer overflow protection |
| `internal/btree/cursor.go` | State validation, reference counting |
| `internal/vdbe/record.go` | Buffer bounds checking, size limits |
| `internal/btree/varint.go` | Edge case handling |
| `internal/pager/pager.go` | State synchronization, checksums, permissions |
| `internal/pager/memory_pager.go` | Page count limits |
| `internal/pager/wal_index.go` | Checksum validation |
| `internal/parser/parser.go` | Size limits, depth tracking, PRAGMA whitelist |
| `internal/schema/schema.go` | RWMutex synchronization |
| `docs/LOCK_ORDERING.md` | Updated hierarchy |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/security/config.go` | SecurityConfig struct and defaults |
| `internal/security/path.go` | Path validation functions |
| `internal/security/errors.go` | Security error types |
| `internal/security/limits.go` | Resource limit constants |
| `internal/security/arithmetic.go` | Safe arithmetic helpers |
| `internal/security/security_test.go` | Comprehensive security tests |
| `internal/security/path_test.go` | Path validation tests |
| `internal/parser/fuzz_test.go` | Parser fuzzing |
| `internal/vdbe/fuzz_test.go` | VDBE/record fuzzing |
| `docs/SECURITY.md` | Security documentation |

---

## Success Criteria

1. **All CRITICAL issues resolved** (4/4)
2. **All HIGH issues resolved** (8/8)
3. **All MEDIUM issues addressed** (11/11)
4. **Security test suite passing**: 100+ test cases
5. **Race detector clean**: `go test -race ./...` passes
6. **Fuzz testing**: No panics after 10M iterations
7. **Documentation complete**: SECURITY.md published
8. **Code review**: All changes reviewed

---

## Testing Commands

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run security tests only
go test ./internal/security/...

# Run fuzz tests (short)
go test -fuzz=FuzzParser -fuzztime=30s ./internal/parser/

# Run fuzz tests (full)
go test -fuzz=FuzzParser -fuzztime=1h ./internal/parser/
go test -fuzz=FuzzRecord -fuzztime=1h ./internal/vdbe/

# Check for remaining issues
grep -r "TODO.*security" internal/
grep -r "FIXME.*security" internal/
```

---

## Rollback Plan

If issues are discovered post-implementation:

1. Each agent's changes are in separate commits
2. Revert specific agent commits if needed
3. SecurityConfig has flags to disable individual protections
4. Resource limits can be increased via configuration

---

## Timeline

| Phase | Agents | Focus |
|-------|--------|-------|
| 1 | 1-3 | Path security, concurrency (CRITICAL fixes) |
| 2 | 4-6 | Integer/buffer safety, resource limits (HIGH fixes) |
| 3 | 7-9 | Schema/btree/pager safety (MEDIUM fixes) |
| 4 | 10-11 | Tests and documentation |

All 11 agents can run in parallel for maximum efficiency.
