# Lock Ordering Documentation

## Overview

This document establishes the lock acquisition hierarchy for the Anthony SQLite Driver to prevent deadlocks. All code must acquire locks in the order specified below.

## Lock Hierarchy (Top to Bottom)

Locks must be acquired in this order. Never acquire a higher-numbered lock while holding a lower-numbered lock.

```
1. Driver.mu          (sync.Mutex)     - Global driver state
2. Conn.mu            (sync.Mutex)     - Connection state, statements map
3. Stmt.mu            (sync.Mutex)     - Statement state (closed, vdbe)
4. Pager locks        (file locks)     - Database file locks
5. Btree.mu           (sync.RWMutex)   - Pages cache map
```

## Lock Details

### 1. Driver.mu (`internal/driver/driver.go`)

Protects:
- `conns` map (active connections by filename)
- `dbs` map (shared database state per file)

Acquisition:
- During `Open()` / `OpenConnector()`
- During connection cleanup

### 2. Conn.mu (`internal/driver/conn.go`)

Protects:
- `stmts` map (prepared statements)
- `closed` flag
- `inTx` transaction state

Acquisition:
- Before any statement map modification
- Before checking/setting connection state

### 3. Stmt.mu (`internal/driver/stmt.go`)

Protects:
- `closed` flag
- `vdbe` pointer

Acquisition:
- In `Close()`, `ExecContext()`, `QueryContext()`
- When Conn.Close() closes all statements

### 4. Pager Locks (`internal/pager/`)

File-level locks for transaction isolation:
- `SHARED` lock for read transactions
- `RESERVED` lock for write intent
- `EXCLUSIVE` lock for writes

### 5. Btree.mu (`internal/btree/btree.go`)

Protects:
- `Pages` map (in-memory page cache)

Acquisition:
- `RLock` for cache reads (when page exists)
- `Lock` for cache writes (when caching from provider)

## Common Patterns

### Pattern 1: Closing a Connection

```go
func (c *Conn) Close() error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Close all statements with their locks
    for stmt := range c.stmts {
        stmt.mu.Lock()
        stmt.closed = true
        if stmt.vdbe != nil {
            stmt.vdbe.Finalize()
            stmt.vdbe = nil
        }
        stmt.mu.Unlock()
    }

    // Then access pager (lower in hierarchy)
    c.pager.Rollback()
    c.pager.Close()

    // Finally update driver state
    c.driver.mu.Lock()
    delete(c.driver.conns, c.filename)
    c.driver.mu.Unlock()
}
```

Note: Driver.mu is acquired last here because we're removing from the map, not iterating. This is safe as long as no other code holds driver.mu and tries to acquire conn.mu.

### Pattern 2: Preparing a Statement

```go
func (c *Conn) PrepareContext(...) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Create stmt (no stmt.mu needed - not visible yet)
    stmt := &Stmt{...}
    c.stmts[stmt] = struct{}{}
    return stmt, nil
}
```

### Pattern 3: Closing a Statement

```go
func (s *Stmt) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.closed = true
    // Finalize VDBE...

    // Remove from connection (conn.mu is same level, OK via removeStmt)
    s.conn.removeStmt(s)
}

func (c *Conn) removeStmt(stmt *Stmt) {
    c.mu.Lock()
    defer c.mu.Unlock()
    delete(c.stmts, stmt)
}
```

### Pattern 4: Page Cache Access

```go
func (bt *Btree) GetPage(pageNum uint32) ([]byte, error) {
    // Try read lock first
    bt.mu.RLock()
    if page, ok := bt.Pages[pageNum]; ok {
        bt.mu.RUnlock()
        return page, nil
    }
    bt.mu.RUnlock()

    // Need write lock to cache
    data, err := bt.Provider.GetPageData(pageNum)
    if err != nil {
        return nil, err
    }

    bt.mu.Lock()
    bt.Pages[pageNum] = data
    bt.mu.Unlock()
    return data, nil
}
```

## Anti-Patterns (DO NOT DO)

### Holding Conn.mu While Calling Stmt.Close()

```go
// BAD - can deadlock
c.mu.Lock()
for stmt := range c.stmts {
    stmt.Close()  // Close() tries to call removeStmt() which needs c.mu
}
c.mu.Unlock()
```

### Holding Btree.mu During Provider Calls

```go
// BAD - provider may do I/O or acquire other locks
bt.mu.Lock()
data, err := bt.Provider.GetPageData(pageNum)  // Long operation under lock
bt.mu.Unlock()
```

## Thread Safety Notes

1. **Atomic Operations**: `Driver.memoryCount` uses `atomic.AddInt64()` - no lock needed.

2. **Immutable After Creation**: `Stmt.conn`, `Stmt.query`, `Stmt.ast` are set once and never modified.

3. **Single-Writer**: The pager ensures only one write transaction at a time.

4. **RWMutex Usage**: `Btree.mu` is a RWMutex. Use RLock for read-only access, Lock for writes.

## Testing Race Conditions

Run tests with race detector:

```bash
go test -race ./...
```

## Version History

- 2026-02-27: Initial documentation created as part of Phase 1.4 stability fixes
