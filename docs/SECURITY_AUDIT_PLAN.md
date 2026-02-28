# Security Audit Plan

## Overview

This document outlines a comprehensive security audit plan for the Public.Lib.Anthony SQLite implementation. The audit focuses on identifying and remediating security vulnerabilities across all components.

## Audit Scope

### 1. Input Validation & Injection Prevention
- **SQL Injection**: Parser input handling, parameter binding
- **Path Traversal**: Database file paths, ATTACH/DETACH operations
- **Buffer Overflow**: Binary data handling, record parsing
- **Integer Overflow**: Page calculations, size computations

### 2. Memory Safety
- **Use-After-Free**: Cursor lifecycle, connection cleanup
- **Double-Free**: Resource deallocation patterns
- **Memory Leaks**: Long-running operations, error paths
- **Uninitialized Memory**: Record parsing, value handling

### 3. Concurrency & Race Conditions
- **Lock Ordering**: Deadlock prevention
- **Atomic Operations**: Shared state access
- **Transaction Isolation**: Read/write conflicts
- **Connection Pooling**: State isolation

### 4. Cryptographic Security
- **Checksum Validation**: Journal and WAL integrity
- **Random Number Generation**: Rowid generation
- **Secure Defaults**: Configuration options

### 5. Resource Management
- **Denial of Service**: Query complexity limits
- **Resource Exhaustion**: Memory limits, file handles
- **Timeout Handling**: Busy handler, lock acquisition

### 6. File System Security
- **Permission Handling**: Database file creation
- **Symlink Attacks**: Path resolution
- **Temporary Files**: Journal, WAL cleanup
- **Atomic Operations**: Commit integrity

## Component-Specific Audits

### Parser (internal/parser)
| Risk Area | Priority | Description |
|-----------|----------|-------------|
| SQL Injection | HIGH | Validate all parsed input, prevent injection via string literals |
| Stack Overflow | MEDIUM | Limit expression nesting depth |
| Resource Exhaustion | MEDIUM | Limit query complexity, token count |

### Pager (internal/pager)
| Risk Area | Priority | Description |
|-----------|----------|-------------|
| Path Traversal | HIGH | Sanitize all file paths |
| Lock Bypass | HIGH | Validate lock state transitions |
| Journal Corruption | HIGH | Validate checksums before rollback |
| Resource Leaks | MEDIUM | Ensure cleanup on all error paths |

### B-tree (internal/btree)
| Risk Area | Priority | Description |
|-----------|----------|-------------|
| Integer Overflow | HIGH | Validate page numbers, cell sizes |
| Buffer Overflow | HIGH | Bounds checking on cell access |
| Corruption Detection | MEDIUM | Integrity checks on page reads |

### VDBE (internal/vdbe)
| Risk Area | Priority | Description |
|-----------|----------|-------------|
| Stack Overflow | HIGH | Limit program size and recursion |
| Type Confusion | MEDIUM | Validate register types |
| Resource Exhaustion | MEDIUM | Limit instruction count |

### Driver (internal/driver)
| Risk Area | Priority | Description |
|-----------|----------|-------------|
| Connection State | HIGH | Validate state before operations |
| Transaction Isolation | HIGH | Prevent cross-connection leaks |
| Resource Cleanup | MEDIUM | Ensure proper finalization |

### Functions (internal/functions)
| Risk Area | Priority | Description |
|-----------|----------|-------------|
| Integer Overflow | MEDIUM | Math function bounds |
| Null Pointer | MEDIUM | Argument validation |
| Resource Exhaustion | LOW | Limit string operations |

## Implementation Phases

### Phase 1: Critical Fixes (HIGH Priority)
1. Path traversal prevention in pager
2. Integer overflow protection in btree
3. SQL injection prevention in parser
4. Lock state validation in pager
5. Buffer overflow protection in record parsing

### Phase 2: Important Fixes (MEDIUM Priority)
1. Expression depth limits in parser
2. Query complexity limits
3. Memory leak fixes on error paths
4. Checksum validation improvements
5. Concurrent access hardening

### Phase 3: Defensive Measures (LOW Priority)
1. Additional input validation
2. Enhanced logging for security events
3. Configuration hardening
4. Documentation of security practices

## Security Controls to Implement

### 1. Input Validation
```go
// Path validation
func ValidateDatabasePath(path string) error {
    // Check for null bytes
    // Check for path traversal (../)
    // Check for absolute path requirements
    // Validate file extension
}

// SQL size limits
const (
    MaxSQLLength     = 1000000  // 1MB
    MaxTokens        = 10000
    MaxExprDepth     = 100
    MaxQueryTables   = 64
)
```

### 2. Integer Overflow Protection
```go
// Safe arithmetic
func SafeAdd(a, b int64) (int64, error) {
    if a > 0 && b > math.MaxInt64-a {
        return 0, ErrOverflow
    }
    if a < 0 && b < math.MinInt64-a {
        return 0, ErrOverflow
    }
    return a + b, nil
}
```

### 3. Resource Limits
```go
type SecurityLimits struct {
    MaxPageCount     int64
    MaxCacheSize     int64
    MaxBlobSize      int64
    MaxRecordSize    int64
    MaxVDBEOps       int64
    MaxQueryTime     time.Duration
}
```

### 4. Checksum Validation
```go
// CRC32 for journal/WAL
func ValidateChecksum(data []byte, expected uint32) bool {
    return crc32.ChecksumIEEE(data) == expected
}
```

## Testing Requirements

### Security Test Categories
1. **Fuzzing Tests**: Random input to parser, record parser
2. **Boundary Tests**: Maximum values, edge cases
3. **Injection Tests**: SQL injection patterns
4. **Concurrency Tests**: Race condition detection
5. **Resource Tests**: Memory/file handle limits

### Test Coverage Targets
- All security-critical paths: 100%
- Input validation functions: 100%
- Error handling paths: 95%+

## Implementation Status

### Completed Components

#### Path Security (Agent 1)
- [x] Created `internal/security/config.go` with SecurityConfig
- [x] Created `internal/security/path.go` with 4-layer validation
- [x] Created `internal/security/errors.go` with error types
- [x] Implemented path validation in ATTACH DATABASE
- [x] Implemented path validation in VACUUM INTO
- [x] Added file permission enforcement (0600/0700)

#### Integer Safety (Agent 4)
- [x] Created `internal/security/arithmetic.go` with SafeCast functions
- [x] Fixed integer overflow in btree cell calculations
- [x] Fixed integer overflow in overflow calculations
- [x] Added bounds checking for all casts
- [x] Added validation for usable size calculations

#### Resource Limits (Agent 6)
- [x] Created `internal/security/limits.go` with constants
- [x] Added SQL length limits (MaxSQLLength = 1MB)
- [x] Added expression depth limits (MaxExprDepth = 100)
- [x] Added table count limits (MaxQueryTables = 64)
- [x] Added memory database page limits (MaxMemoryDBPages = 100,000)
- [x] Added PRAGMA whitelist

#### Concurrency Safety (Agents 2 & 3)
- [x] Fixed deadlock in Conn.Close() with two-phase pattern
- [x] Fixed deadlock in Stmt.Close() with proper lock ordering
- [x] Fixed race conditions in transaction state checks
- [x] Created comprehensive LOCK_ORDERING.md documentation
- [x] Implemented reference counting for active operations

#### Security Testing (Agent 10)
- [x] Created `internal/security/path_test.go` with attack vectors
- [x] Created `internal/security/limits_test.go`
- [x] Created `internal/security/arithmetic_test.go`
- [x] Added fuzz testing for parser
- [x] Added fuzz testing for memory pager
- [x] Added race detector tests

#### Documentation (Agent 11)
- [x] Created comprehensive `docs/SECURITY.md`
- [x] Updated `docs/LOCK_ORDERING.md` with security context
- [x] Updated `docs/SECURITY_AUDIT_PLAN.md` with status
- [x] Documented all security controls and best practices
- [x] Created security vulnerability reporting process

### Remaining Work

#### Buffer Safety (Agent 5)
- [ ] Add bounds checking to decodeInt24Value()
- [ ] Add bounds checking to decodeInt48Value()
- [ ] Validate offsets in DecodeRecord
- [ ] Add record size enforcement
- [ ] Fix varint edge cases

#### Schema Safety (Agent 7)
- [ ] Add sync.RWMutex to Schema struct
- [ ] Protect schema reads with RLock
- [ ] Protect schema writes with Lock
- [ ] Add schema name validation
- [ ] Add concurrent schema access tests

#### Btree Safety (Agent 8)
- [ ] Fix GetPage() double-check pattern
- [ ] Add page reference counting
- [ ] Add page integrity checks
- [ ] Validate page numbers against DB size
- [ ] Add cursor state validation

#### Pager Safety (Agent 9)
- [ ] Add CRC32 checksum validation
- [ ] Strengthen state transition validation
- [ ] Synchronize all state reads
- [ ] Add WAL frame checksums
- [ ] Validate lock state transitions

## Audit Checklist

### Pre-Implementation
- [x] Review OWASP guidelines for database security
- [x] Identify all external input entry points
- [x] Map data flow through components
- [x] Document current security controls

### Implementation
- [x] Add path traversal prevention
- [x] Add integer overflow checks
- [x] Add input size limits
- [x] Add resource limits
- [x] Add lock state validation
- [ ] Add checksum validation (in progress)
- [ ] Add memory bounds checking (in progress)

### Post-Implementation
- [x] Run security test suite
- [x] Perform fuzz testing
- [x] Conduct code review
- [x] Update security documentation
- [x] Create security advisory process

## File Locations for Security Fixes

| Component | Files | Security Focus |
|-----------|-------|----------------|
| Parser | `internal/parser/*.go` | Input validation, limits |
| Pager | `internal/pager/*.go` | Path traversal, locks, checksums |
| B-tree | `internal/btree/*.go` | Integer overflow, bounds checking |
| VDBE | `internal/vdbe/*.go` | Resource limits, type safety |
| Driver | `internal/driver/*.go` | Connection safety, isolation |
| Schema | `internal/schema/*.go` | Name validation |
| Functions | `internal/functions/*.go` | Overflow protection |
| SQL | `internal/sql/*.go` | Query limits |
| Engine | `internal/engine/*.go` | Transaction safety |
| Expr | `internal/expr/*.go` | Expression limits |
| Constraint | `internal/constraint/*.go` | Validation logic |

## Success Criteria

1. No HIGH priority vulnerabilities remaining
2. All MEDIUM priority vulnerabilities addressed
3. Security test suite passing
4. Fuzz testing completes without crashes
5. Code review completed by security team
6. Documentation updated with security practices
