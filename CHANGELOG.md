# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- Comprehensive security audit implementation addressing 23 vulnerabilities
- New `internal/security` package with defense-in-depth security model
- Path traversal prevention with 4-layer validation (Layered + Sandbox model)
  - Layer 1: Dangerous pattern blocking (null bytes, `..`, control chars)
  - Layer 2: Sandbox enforcement with path resolution
  - Layer 3: Optional allowlist for subdirectory restrictions
  - Layer 4: Symlink detection and blocking
- Integer overflow protection with safe arithmetic functions
  - `SafeCastUint32ToUint16()` for safe downcasting
  - `SafeAddUint32()` and `SafeSubUint32()` with overflow/underflow detection
  - `ValidateUsableSize()` for B-tree size validation
- Buffer overflow protection with bounds checking
  - Safe record decoding functions with length validation
  - Maximum record size enforcement (1GB limit)
- Resource limits to prevent exhaustion attacks
  - SQL length limit: 1MB (MaxSQLLength)
  - Token limit: 10,000 (MaxTokens)
  - Expression depth limit: 100 (MaxExprDepth)
  - Query table limit: 64 (MaxQueryTables)
  - Parameter limit: 32,767 (MaxParameters)
  - Memory database page limit: 100,000 pages (~400MB at 4KB pages)
  - Attached database limit: 10 (MaxAttachedDBs)
  - Trigger depth limit: 32 (MaxTriggerDepth)
- PRAGMA whitelist with 40+ safe pragmas
  - Informational pragmas (table_info, index_list, etc.)
  - Safe configuration pragmas (cache_size, foreign_keys, etc.)
  - Dangerous pragmas blocked (writable_schema, ignore_check_constraints, etc.)
  - Expanded whitelist for better usability while maintaining security
- SecurityConfig with comprehensive security controls
  - Configurable sandbox root directory
  - File permission enforcement (default: 0600 for files, 0700 for dirs)
  - Toggleable security layers for flexibility
- Comprehensive security documentation
  - `docs/SECURITY.md` with security model, configuration, and best practices
  - `docs/SECURITY_AUDIT_IMPLEMENTATION.md` with implementation details
  - Security checklist for code review
  - `docs/LOCK_ORDERING.md` for concurrency safety
- Extensive security test suite
  - Path traversal attack tests (50+ vectors)
  - Integer overflow/underflow boundary tests
  - Buffer overflow tests
  - Symlink escape tests
  - Comprehensive attack scenario tests
  - Null byte injection prevention tests
- Test coverage improvements across all packages
  - 7 packages at 97%+ coverage: format, rtree, fts5, vtab, functions, utf, vtab/builtin
  - 5 packages at 94-97%: expr, schema, parser, constraint, collation
  - 4 packages at 90-94%: engine, planner, sql
  - 3 packages at 85-90%: vdbe, driver, pager
  - 1 package at 81%: btree
- Development infrastructure
  - `.gitignore` for coverage files, build artifacts, IDE files
  - `Makefile` with common development targets
- Comprehensive TODO.txt with 36 improvement tasks across 9 phases
- Best practices improvement plan based on 11-agent architectural review

### Changed
- Reduced all function cyclomatic complexity to ≤10 for improved maintainability
- Refactored connection and statement close operations with two-phase pattern
  - Prevents lock ordering violations and deadlocks
  - Phase 1: Mark closed and collect cleanup items under lock
  - Phase 2: Release lock and perform cleanup
  - Phase 3: Acquire parent lock for registration cleanup
- Enhanced B-tree page validation with integrity checks
  - Page number validation (reject page 0)
  - Page type validation (only known types accepted)
  - Cell pointer array bounds checking
  - Content area overlap detection
  - Double-check locking pattern for cache access
- Improved statement execution with TOCTOU protection
  - Connection state checks under lock
  - Atomic closed flags
  - Safe concurrent access patterns
- Improved concurrent connection safety
  - Better synchronization for connection state
  - Atomic operations for critical paths
  - Enhanced race condition prevention
- SQL injection hardening
  - Stricter parameter validation
  - Enhanced input sanitization
  - Improved prepared statement handling
- Documentation consolidation
  - Consolidated security documentation
  - Moved deprecated documentation to `attic/` directory
  - Updated all security-related documentation for consistency

### Fixed
- **CRITICAL**: Race condition in statement transaction checks
  - Fixed by holding connection lock during transaction state reads
  - Applied to both `ExecContext()` and `QueryContext()`
- **CRITICAL**: Deadlock in `Stmt.Close()` lock ordering
  - Fixed with two-phase close: release stmt lock before acquiring conn lock
  - Deferred cleanup pattern prevents deadlock
- **CRITICAL**: Use-after-free risk (TOCTOU) in statement execution
  - Fixed by validating connection state under lock before execution
  - Returns `driver.ErrBadConn` if connection closed
- **CRITICAL**: Path traversal vulnerability in ATTACH DATABASE
  - Fixed with 4-layer path validation before attaching databases
  - Sandbox enforcement prevents access outside database root
  - Null byte injection prevention
- **HIGH**: Path injection in VACUUM INTO
  - Fixed with same 4-layer validation as ATTACH DATABASE
- **HIGH**: Integer overflow in uint32→uint16 casts
  - Fixed in `btree/cell.go` and `btree/overflow.go`
  - Uses `SafeCastUint32ToUint16()` with bounds checking
- **HIGH**: Schema race conditions
  - Added `sync.RWMutex` to Schema struct
  - All schema access now synchronized
- **HIGH**: Lock ordering violation in `Conn.Close()`
  - Fixed with three-phase close respecting lock hierarchy
  - Driver.mu acquired after releasing Conn.mu
- **HIGH**: Buffer overflows in record decoding
  - Fixed `decodeInt24Value()` and `decodeInt48Value()` with bounds checks
  - Validates offset before each buffer access
- **MEDIUM**: Memory exhaustion in memory pager
  - Added page count limit (100,000 pages)
  - Prevents unbounded memory growth
- **MEDIUM**: Missing parameter count validation
  - Added validation against MaxParameters (32,767)
- **MEDIUM**: Cache race in `btree.GetPage()`
  - Fixed with double-check locking pattern
  - Prevents cache corruption from concurrent access
- **MEDIUM**: Pager state races
  - Synchronized all state reads/writes
  - Atomic state transitions
- **MEDIUM**: Integer underflow in `calculateMinLocal()`
  - Added `ValidateUsableSize()` check (minimum 35 bytes)
- **MEDIUM**: Unsafe casts in CellSize calculations
  - Added bounds checking for all cell size casts
- **MEDIUM**: Missing record size limits
  - Enforced MaxRecordSize (1GB) and MaxBlobSize (1GB)
- **MEDIUM**: Unbounded query complexity
  - Added MaxExprDepth (100) and MaxQueryTables (64) limits
- **MEDIUM**: File permission issues
  - Enforced secure file creation modes (0600/0700)
- **MEDIUM**: Page reference count edge cases
  - Added reference counting for cursor stability
  - Validates page numbers against database size
- **MEDIUM**: Varint parsing edge cases
  - Improved bounds validation for maximum values
- Race condition in `Conn.Close()` - added mutex protection to Stmt struct
- B-tree Pages map synchronization - added `sync.RWMutex` to Btree struct
- memoryCount atomic operations - changed to int64 with `atomic.AddInt64()`
- All test failures from previous versions

### Security
- Implemented complete security audit addressing all 23 identified issues:
  - 4 CRITICAL severity issues resolved
  - 8 HIGH severity issues resolved
  - 11 MEDIUM severity issues resolved
- Defense-in-depth security model with layered protections
  - 4-layer path validation system
  - PRAGMA whitelist expansion for safe operations
  - Path traversal protection with sandbox enforcement
  - Null byte injection prevention
  - Symlink detection and blocking
  - Sandbox enforcement with configurable root directory
- Concurrent connection safety improvements
  - Enhanced lock ordering to prevent deadlocks
  - Atomic operations for critical state changes
  - TOCTOU protection in execution paths
- SQL injection hardening with stricter validation
- All security changes pass race detector (`go test -race ./...`)
- Comprehensive test coverage for attack vectors

---

## [0.3.0] - 2026-02-27

### Added
- Phase 3: Functions, Query Optimization, and Integration

### Fixed
- All test failures: btree splits, pager locks, driver timeouts, VDBE comparisons

---

## [0.2.0] - 2026-02-26

### Added
- Phase 1: Core ACID & Storage implementation
- B-tree with page management
- Pager with journal and transaction support
- VDBE bytecode VM

---

## [0.1.0] - 2026-02-25

### Added
- Initial implementation of pure Go SQLite database driver
- SQL parser and lexer
- Schema management
- Basic query execution
