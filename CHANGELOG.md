# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- Comprehensive TODO.txt with 36 improvement tasks across 9 phases
- Best practices improvement plan based on 11-agent architectural review
- Lock ordering documentation (docs/LOCK_ORDERING.md)

### Changed
- Reduced all function cyclomatic complexity to ≤10

### Fixed
- Race condition in Conn.Close() - added mutex protection to Stmt struct
- Btree Pages map synchronization - added sync.RWMutex to Btree struct
- memoryCount atomic operations - changed to int64 with atomic.AddInt64()

### Security
- (Pending) Path traversal prevention
- (Pending) Integer overflow protection
- (Pending) Input size limits

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
