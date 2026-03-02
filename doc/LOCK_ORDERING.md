# Lock Ordering Documentation

## Overview

This document establishes the lock acquisition hierarchy for the Anthony SQLite Driver to prevent deadlocks. All code must acquire locks in the order specified below.

## Lock Hierarchy (Top to Bottom)

Locks must be acquired in this order. Never acquire a higher-numbered lock while holding a lower-numbered lock.

**CRITICAL RULE: Never hold multiple locks simultaneously unless required by the hierarchy.**

```
1. Driver.mu          (sync.Mutex)     - Global driver state (conns, dbs maps)
2. Conn.mu            (sync.Mutex)     - Connection state (stmts map, closed, inTx)
3. Stmt.mu            (sync.Mutex)     - Statement state (closed, vdbe)
4. Schema.mu          (sync.RWMutex)   - Schema tables, indexes, views, triggers
5. Pager.mu           (sync.RWMutex)   - Pager state, cache, transactions
6. Btree.mu           (sync.RWMutex)   - B-tree page cache (Pages map)
```

### Lock Ordering Rules

1. **Never acquire Driver.mu while holding Conn.mu** - This causes deadlock
2. **Never acquire Driver.mu while holding Stmt.mu** - This violates hierarchy
3. **Always release locks before calling methods that may acquire other locks**
4. **Use two-phase patterns for cleanup operations** (see below)
5. **Prefer RLock for read-only operations** on RWMutex locks

## Lock Details

### 1. Driver.mu (`internal/driver/driver.go`)

**Type**: `sync.Mutex`

**Protects**:
- `conns` map - Active connections by filename
- `dbs` map - Shared database state per file (dbState structures)
- Connection creation and cleanup

**Acquisition**:
- During `Open()` / `OpenConnector()`
- During connection cleanup (when removing from driver)

**Thread Safety Note**: `memoryCount` uses `atomic.AddInt64()` - no lock needed

### 2. Conn.mu (`internal/driver/conn.go`)

**Type**: `sync.Mutex`

**Protects**:
- `stmts` map - Prepared statements belonging to this connection
- `closed` flag - Connection closed state
- `inTx` - Transaction state (in transaction or not)

**Acquisition**:
- `PrepareContext()` - Before adding statement to map
- `BeginTx()` - Before checking/setting transaction state
- `Ping()`, `ResetSession()` - Before checking closed state
- `removeStmt()` - Before removing statement from map
- `CreateScalarFunction()`, `CreateAggregateFunction()`, `UnregisterFunction()` - Before modifying function registry

**Critical**: Released before acquiring Driver.mu in Close() to prevent deadlock

### 3. Stmt.mu (`internal/driver/stmt.go`)

**Type**: `sync.Mutex`

**Protects**:
- `closed` flag - Statement closed state
- `vdbe` pointer - Reference to VDBE execution engine

**Acquisition**:
- `Close()` - Before checking/setting closed state and finalizing VDBE
- `ExecContext()` - Before checking closed state
- `QueryContext()` - Before checking closed state

**Important**: Lock is released before calling `conn.removeStmt()` to avoid deadlock

### 4. Schema.mu (`internal/schema/schema.go`)

**Type**: `sync.RWMutex`

**Protects**:
- `Tables` map - Table definitions
- `Indexes` map - Index definitions
- `Views` map - View definitions
- `Triggers` map - Trigger definitions

**Acquisition**:
- `GetTable()`, `GetIndex()` - RLock for read-only access
- `ListTables()`, `ListIndexes()` - RLock for iteration
- `CreateTable()`, `CreateIndex()` - Lock for modifications
- `DropTable()`, `DropIndex()` - Lock for deletions
- `RenameTable()` - Lock for updates

**Performance Note**: Use RLock for queries, Lock only for DDL operations

### 5. Pager.mu (`internal/pager/pager.go`)

**Type**: `sync.RWMutex`

**Protects**:
- Pager state (`state`, `lockState`)
- Database size (`dbSize`, `dbOrigSize`)
- Cache operations
- Transaction state

**Acquisition**:
- `Get()`, `getLocked()` - For page retrieval
- `Write()`, `writeLocked()` - For page modifications
- `Commit()` - For transaction commit
- `Rollback()`, `rollbackLocked()` - For transaction rollback
- `Close()` - For cleanup
- `PageSize()`, `PageCount()` - RLock for read-only operations

**File Locks** (not mutex locks):
- `SHARED` lock - Read transactions
- `RESERVED` lock - Write intent
- `EXCLUSIVE` lock - Active writes

### 6. Btree.mu (`internal/btree/btree.go`)

**Type**: `sync.RWMutex`

**Protects**:
- `Pages` map - In-memory page cache (pageNum -> page data)

**Acquisition**:
- `GetPage()` - RLock for cache read (if page exists), Lock for cache write (when loading from provider)
- `SetPage()` - Lock for cache modification
- `ClearCache()` - Lock for clearing cache
- `AllocatePage()` - Lock when adding new page to cache
- `DropTable()` - Lock when removing page from cache
- `String()` - RLock for reading page count

**Performance Pattern**: Double-checked locking
- Try RLock first to check cache
- Release RLock
- Acquire Lock if page needs loading
- Re-check cache (another goroutine may have loaded it)

## Common Patterns

### Pattern 1: Two-Phase Close Pattern (REQUIRED for Conn.Close)

**Problem**: Acquiring Driver.mu while holding Conn.mu violates lock ordering and causes deadlock.

**Solution**: Use a two-phase close pattern that releases Conn.mu before acquiring Driver.mu.

```go
func (c *Conn) Close() error {
    // Phase 1: Mark closed and collect cleanup items under conn lock
    c.mu.Lock()
    if c.closed {
        c.mu.Unlock()
        return nil // Already closed
    }
    c.closed = true

    // Collect statements to close (make a slice copy)
    stmts := make([]*Stmt, 0, len(c.stmts))
    for stmt := range c.stmts {
        stmts = append(stmts, stmt)
    }
    c.stmts = nil

    // Collect transaction state and pager reference
    inTx := c.inTx
    pager := c.pager
    c.mu.Unlock() // RELEASE LOCK BEFORE PHASE 2

    // Phase 2: Close statements without holding conn lock
    for _, stmt := range stmts {
        stmt.mu.Lock()
        stmt.closed = true
        if stmt.vdbe != nil {
            stmt.vdbe.Finalize()
            stmt.vdbe = nil
        }
        stmt.mu.Unlock()
    }

    // Phase 3: Remove from driver (only driver lock needed)
    // This respects the lock hierarchy: Driver.mu before Conn.mu
    if c.driver != nil {
        c.driver.mu.Lock()
        delete(c.driver.conns, c.filename)
        c.driver.mu.Unlock()
    }

    // Phase 4: Close pager (no locks needed)
    if pager != nil {
        if inTx {
            pager.Rollback()
        }
        pager.Close()
    }

    return nil
}
```

**Key Points**:
- Phase 1 sets `c.closed = true` under lock, preventing new operations
- Conn.mu is **released** before acquiring Driver.mu
- Statements are closed without holding Conn.mu (safe because we copied the slice)
- This pattern prevents the deadlock: Thread A (Conn.Close holding Conn.mu waiting for Driver.mu) vs Thread B (Driver.Open holding Driver.mu waiting for Conn.mu)

### Pattern 2: Preparing a Statement

```go
func (c *Conn) PrepareContext(...) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.closed {
        return nil, driver.ErrBadConn
    }

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
    if s.closed {
        s.mu.Unlock()
        return nil
    }
    s.closed = true

    // Finalize VDBE if it exists
    if s.vdbe != nil {
        s.vdbe.Finalize()
        s.vdbe = nil
    }

    // Save connection reference before unlocking
    conn := s.conn
    s.mu.Unlock() // RELEASE LOCK BEFORE CALLING removeStmt

    // Remove from connection (conn.mu is same level, OK via removeStmt)
    if conn != nil {
        conn.removeStmt(s)
    }

    return nil
}

func (c *Conn) removeStmt(stmt *Stmt) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.stmts != nil {
        delete(c.stmts, stmt)
    }
}
```

### Pattern 4: Page Cache Access (Double-Checked Locking)

```go
func (bt *Btree) GetPage(pageNum uint32) ([]byte, error) {
    // Fast path: try read lock first
    bt.mu.RLock()
    if page, ok := bt.Pages[pageNum]; ok {
        bt.mu.RUnlock()
        return page, nil
    }
    bt.mu.RUnlock()

    // Slow path: need write lock to cache
    if bt.Provider != nil {
        bt.mu.Lock()
        defer bt.mu.Unlock()

        // CRITICAL: Double-check after acquiring write lock
        // Another goroutine may have loaded the page
        if page, ok := bt.Pages[pageNum]; ok {
            return page, nil
        }

        // Load page from provider
        data, err := bt.Provider.GetPageData(pageNum)
        if err != nil {
            return nil, err
        }

        // Validate page before caching
        if err := bt.validatePage(data, pageNum); err != nil {
            return nil, err
        }

        // Cache the validated page
        bt.Pages[pageNum] = data
        return data, nil
    }

    return nil, fmt.Errorf("page %d not found", pageNum)
}
```

**Why this works**:
- Most reads hit cache (fast path with RLock)
- Only cache misses need write lock
- Double-check prevents race where two goroutines both miss cache
- First goroutine loads, second goroutine finds it on re-check

### Pattern 5: Schema Read-Heavy Operations

```go
func (s *Schema) GetTable(name string) (*Table, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    lowerName := strings.ToLower(name)
    for tableName, table := range s.Tables {
        if strings.ToLower(tableName) == lowerName {
            return table, true
        }
    }
    return nil, false
}
```

**Performance Optimization**: Use RLock for reads (allows concurrent readers)

### Pattern 6: TOCTOU (Time-of-Check-Time-of-Use) Prevention

```go
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
    // Check statement state
    s.mu.Lock()
    if s.closed {
        s.mu.Unlock()
        return nil, driver.ErrBadConn
    }
    s.mu.Unlock()

    // Check connection state under lock to avoid TOCTOU race
    s.conn.mu.Lock()
    inTx := s.conn.inTx
    connClosed := s.conn.closed
    s.conn.mu.Unlock()

    if connClosed {
        return nil, driver.ErrBadConn
    }

    // ... rest of execution
}
```

**Why this is important**: Captures state atomically to prevent race conditions

## Anti-Patterns (DO NOT DO)

### Anti-Pattern 1: Acquiring Driver.mu While Holding Conn.mu

```go
// BAD - violates lock ordering hierarchy (causes deadlock)
c.mu.Lock()
// ... do some work ...
c.driver.mu.Lock()  // WRONG: Driver.mu must be acquired before Conn.mu
delete(c.driver.conns, c.filename)
c.driver.mu.Unlock()
c.mu.Unlock()
```

**Why this is bad**: Thread A can hold Conn.mu waiting for Driver.mu, while Thread B holds Driver.mu waiting for Conn.mu (classic deadlock).

**Fix**: Use two-phase close pattern (see Pattern 1 above).

### Anti-Pattern 2: Holding Conn.mu While Calling Stmt.Close()

```go
// BAD - can deadlock
c.mu.Lock()
for stmt := range c.stmts {
    stmt.Close()  // Close() tries to call removeStmt() which needs c.mu
}
c.mu.Unlock()
```

**Why this is bad**: Stmt.Close() calls removeStmt(), which tries to acquire Conn.mu (already held).

**Fix**: Either finalize statements directly under lock, or use two-phase pattern (as in Pattern 1).

### Anti-Pattern 3: Holding Btree.mu During Provider Calls

```go
// BAD - provider may do I/O or acquire other locks
bt.mu.Lock()
data, err := bt.Provider.GetPageData(pageNum)  // Long operation under lock
bt.Pages[pageNum] = data
bt.mu.Unlock()
```

**Why this is bad**: Provider operations may do I/O (slow) or acquire other locks, causing unnecessary contention.

**Fix**: Release lock before calling provider, then re-acquire (see Pattern 4).

### Anti-Pattern 4: Not Releasing Stmt.mu Before Conn Operations

```go
// BAD - holding stmt lock while calling conn method
s.mu.Lock()
s.closed = true
s.conn.removeStmt(s)  // This acquires conn.mu while holding stmt.mu
s.mu.Unlock()
```

**Why this is bad**: Creates unnecessary lock ordering dependency.

**Fix**: Release stmt.mu before calling conn.removeStmt() (see Pattern 3).

## Lock Hierarchy Diagram

```
+-------------------------------------------------------------+
|                        Driver.mu                            |
|  (Global driver state, conns map, dbs map)                  |
|  Type: sync.Mutex                                           |
+-------------------------------------------------------------+
                            |
                            |
+-------------------------------------------------------------+
|                         Conn.mu                             |
|  (Connection state, stmts map, closed, inTx)                |
|  Type: sync.Mutex                                           |
+-------------------------------------------------------------+
                            |
                            |
+-------------------------------------------------------------+
|                         Stmt.mu                             |
|  (Statement state, closed, vdbe)                            |
|  Type: sync.Mutex                                           |
+-------------------------------------------------------------+
                            |
                            |
+-------------------------------------------------------------+
|                        Schema.mu                            |
|  (Schema tables, indexes, views, triggers)                  |
|  Type: sync.RWMutex (RLock for reads, Lock for writes)     |
+-------------------------------------------------------------+
                            |
                            |
+-------------------------------------------------------------+
|                         Pager.mu                            |
|  (Pager state, cache, transactions)                         |
|  Type: sync.RWMutex                                         |
+-------------------------------------------------------------+
                            |
                            |
+-------------------------------------------------------------+
|                         Btree.mu                            |
|  (B-tree page cache, Pages map)                             |
|  Type: sync.RWMutex (RLock for reads, Lock for writes)     |
+-------------------------------------------------------------+

Legend:
  |  = Can acquire lower lock while holding higher lock
     = NEVER acquire higher lock while holding lower lock
```

## Thread Safety Notes

1. **Atomic Operations**:
   - `Driver.memoryCount` uses `atomic.AddInt64()` - no lock needed
   - No mutex required for atomic counters

2. **Immutable After Creation**:
   - `Stmt.conn`, `Stmt.query`, `Stmt.ast` are set once and never modified
   - No locking needed for immutable fields

3. **Single-Writer**:
   - The pager ensures only one write transaction at a time
   - File locks enforce this at the OS level

4. **RWMutex Usage**:
   - `Btree.mu`, `Schema.mu`, `Pager.mu` are RWMutex
   - Use RLock for read-only access (allows concurrent readers)
   - Use Lock for writes (exclusive access)

5. **Double-Checked Locking**:
   - Pattern used in Btree.GetPage()
   - Minimizes write lock contention
   - Prevents duplicate work by multiple goroutines

## Testing Race Conditions

Run tests with race detector to verify lock safety:

```bash
go test -race ./...
```

Run specific package tests:

```bash
go test -race ./internal/driver
go test -race ./internal/btree
go test -race ./internal/pager
go test -race ./internal/schema
```

## Deadlock Prevention Checklist

Before committing code that acquires multiple locks:

- [ ] Do I acquire locks in hierarchy order (Driver.mu -> Conn.mu -> Stmt.mu -> Schema.mu -> Pager.mu -> Btree.mu)?
- [ ] Do I release all locks before calling external methods?
- [ ] If closing resources, do I use a two-phase pattern?
- [ ] Have I tested with `go test -race`?
- [ ] Can any code path lead to acquiring a higher lock while holding a lower one?
- [ ] Am I using RLock for read-only operations on RWMutex locks?
- [ ] Have I avoided holding locks during I/O operations?
- [ ] Do I capture state atomically to avoid TOCTOU races?

## Security Implications

### Deadlock Prevention

Deadlocks can lead to denial-of-service conditions where the application becomes unresponsive. The lock ordering hierarchy prevents deadlocks by ensuring all threads acquire locks in the same order.

**Example Deadlock Scenario (prevented by hierarchy)**:
- Thread A: Holds Conn.mu, waits for Driver.mu
- Thread B: Holds Driver.mu, waits for Conn.mu
- Result: Both threads blocked forever

**Solution**: Two-phase close pattern ensures Conn.mu is released before acquiring Driver.mu.

### Race Condition Prevention

Race conditions can lead to:
- Data corruption
- Use-after-free vulnerabilities
- Inconsistent state
- Security bypasses
- TOCTOU (Time-of-Check-Time-of-Use) vulnerabilities

All shared state must be protected by appropriate locks according to the hierarchy.

### Performance vs Safety

**When to use RWMutex**:
- Read-heavy workloads (Schema, Btree cache)
- Multiple concurrent readers are safe
- Writers need exclusive access

**When to use Mutex**:
- Write-heavy workloads
- Simple lock/unlock semantics
- State changes are frequent

## Integration with Security Model

The lock ordering hierarchy is part of the overall security model:

1. **Concurrency Safety Layer**: Prevents race conditions and deadlocks
2. **Resource Protection**: Ensures atomic state transitions
3. **Isolation**: Maintains connection and statement independence
4. **Transaction Safety**: Guarantees ACID properties
5. **DoS Prevention**: Prevents deadlock-based denial of service

See [SECURITY.md](SECURITY.md) for the complete security model.

## SQLite Reference

- [SQLite Locking Reference (local)](sqlite/LOCKING.md)
- [SQLite Isolation Reference (local)](sqlite/ISOLATION.md)

## Version History

- 2026-02-28: Added Schema.mu and Pager.mu to hierarchy, expanded patterns, added diagrams (Agent 2)
- 2026-02-28: Added security implications and integration documentation (Security Agent 11)
- 2026-02-28: Added two-phase close pattern and anti-patterns (Security fixes - Agent 3)
- 2026-02-27: Initial documentation created as part of Phase 1.4 stability fixes
