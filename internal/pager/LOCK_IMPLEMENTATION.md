# File Locking Implementation for Anthony SQLite Clone

## Overview

This document describes the file locking implementation for concurrent access control in the Anthony SQLite clone. The implementation follows SQLite's five-level locking protocol to enable concurrent readers with a single writer.

## Files Implemented

1. **internal/pager/lock.go** - Core lock manager and lock level definitions
2. **internal/pager/lock_unix.go** - Unix/Linux-specific implementation using fcntl
3. **internal/pager/lock_windows.go** - Windows stub implementation (to be completed)
4. **internal/pager/lock_test.go** - Comprehensive test suite
5. **internal/pager/lock_example_test.go** - Example usage

## Lock Levels

SQLite uses a five-level locking hierarchy (now implemented in Anthony):

### LockNone (0)
- No locks are held
- The database is completely unlocked
- Default state when no operations are in progress

### LockShared (1)
- A SHARED lock allows reading from the database
- Multiple connections can hold SHARED locks simultaneously
- Prevents other connections from modifying the database
- Used during read transactions

### LockReserved (2)
- A RESERVED lock indicates intent to write
- Only one connection can hold a RESERVED lock at a time
- Other connections can continue to read (hold SHARED locks)
- Used when beginning a write transaction but still reading

### LockPending (3)
- A PENDING lock means the connection wants to write ASAP
- Waiting for all current SHARED locks to clear
- No new SHARED locks are allowed while PENDING is held
- Existing SHARED locks can continue
- Transitional state before EXCLUSIVE

### LockExclusive (4)
- An EXCLUSIVE lock is required to write to the database
- Only one connection can hold an EXCLUSIVE lock
- No other locks of any kind can coexist with EXCLUSIVE
- Used during the actual database write phase

## Lock Transition Rules

### Valid Upgrades
- NONE → SHARED (start reading)
- NONE → EXCLUSIVE (direct exclusive access)
- SHARED → RESERVED (plan to write)
- SHARED → EXCLUSIVE (direct upgrade to write)
- RESERVED → PENDING (prepare to write)
- RESERVED → EXCLUSIVE (commit write)
- PENDING → EXCLUSIVE (finalize write)

### Invalid Upgrades (Rejected)
- NONE → RESERVED (must acquire SHARED first)
- NONE → PENDING (must go through proper states)
- SHARED → PENDING (must acquire RESERVED first)
- EXCLUSIVE → anything (already at max level)

### Downgrades (Always Valid)
Any lock level can be downgraded to any lower level:
- EXCLUSIVE → SHARED, RESERVED, PENDING, or NONE
- PENDING → SHARED, RESERVED, or NONE
- RESERVED → SHARED or NONE
- SHARED → NONE

## Platform-Specific Implementation

### Unix/Linux (lock_unix.go)

Uses POSIX fcntl byte-range locking with the following byte ranges:

```
Byte Range              Lock Type    Lock Name
-----------             ---------    ---------
0x40000000             Exclusive    PENDING lock
0x40000001             Exclusive    RESERVED lock
0x40000002-0x400001FF  Shared       SHARED lock range (510 bytes)
```

#### Key Features:
- Uses `syscall.FcntlFlock()` for non-blocking lock operations
- SHARED locks use one byte from a 510-byte range (randomized per connection)
- RESERVED and PENDING use exclusive locks on specific bytes
- EXCLUSIVE locks the entire SHARED range to block all readers
- Lock conflicts return `ErrLockBusy` immediately (non-blocking)

### Windows (lock_windows.go)

Stub implementation with TODO markers for future development:
- Designed to use `LockFileEx`/`UnlockFileEx` APIs
- Same byte range strategy as Unix
- Currently returns "not yet implemented" errors
- Can be completed by implementing the stub functions

## API

### NewLockManager(file *os.File) (*LockManager, error)
Creates a new lock manager for the given file.

**Parameters:**
- `file`: Open file handle (must remain open for lock manager lifetime)

**Returns:**
- Lock manager instance
- Error if file is nil or platform initialization fails

### AcquireLock(level LockLevel) error
Acquires the specified lock level, following SQLite's escalation rules.

**Parameters:**
- `level`: Target lock level (SHARED, RESERVED, PENDING, or EXCLUSIVE)

**Returns:**
- `nil` on success
- `ErrLockBusy` if lock is held by another process
- `ErrInvalidLock` if the transition is invalid

### ReleaseLock(level LockLevel) error
Releases locks back to the specified level (downgrade).

**Parameters:**
- `level`: Target lock level (must be lower than current)

**Returns:**
- `nil` on success
- Error if platform unlock fails

### GetLockState() LockLevel
Returns the current lock level held by this manager.

**Returns:**
- Current lock level (NONE, SHARED, RESERVED, PENDING, or EXCLUSIVE)

### Close() error
Releases all locks and cleans up resources.

**Returns:**
- `nil` on success
- Error if cleanup fails

### Helper Methods

- **IsLockHeld(level LockLevel) bool**: Check if a specific lock level is held
- **CanAcquire(level LockLevel) bool**: Check if a lock transition would be valid
- **TryAcquireLock(level LockLevel) error**: Non-blocking lock acquisition

## Integration with Pager

The lock manager is designed to integrate with the existing pager implementation:

1. **Modified pager.go**: Added `LockPending` to the lock state constants
2. **Lock levels as int constants**: Maintained compatibility with existing code
3. **LockLevel type**: Provides type safety while using existing constants
4. **Thread-safe**: All operations use mutex protection

## Typical Write Transaction Sequence

```go
// 1. Begin read
lm.AcquireLock(LockShared)

// 2. Prepare to write
lm.AcquireLock(LockReserved)

// 3. Journal modifications (while still allowing readers)
// ... write to journal ...

// 4. Ready to commit - block new readers
lm.AcquireLock(LockPending)

// 5. Wait for readers, then get exclusive access
lm.AcquireLock(LockExclusive)

// 6. Write all changes to database
// ... write dirty pages ...

// 7. Release all locks
lm.ReleaseLock(LockNone)
```

## Concurrent Access Patterns

### Multiple Readers
- Multiple connections can hold SHARED locks simultaneously
- All can read from the database concurrently
- No writers can modify the database while readers exist

### Single Writer with Readers
- Writer acquires RESERVED lock (can coexist with SHARED)
- Writer journals changes while readers continue
- Writer acquires PENDING to block new readers
- Writer waits for existing readers to finish
- Writer acquires EXCLUSIVE and commits changes

### Writer-Writer Conflict
- Only one RESERVED lock can exist
- Second writer attempting RESERVED gets `ErrLockBusy`
- Second writer must wait and retry

## Testing

Comprehensive test coverage includes:

- **Lock level transitions**: Valid and invalid state changes
- **Concurrent readers**: Multiple SHARED locks
- **Reader-writer conflicts**: EXCLUSIVE blocks readers
- **Writer-writer conflicts**: Only one RESERVED lock
- **Lock sequences**: Typical transaction patterns
- **Thread safety**: Concurrent lock operations
- **Edge cases**: Invalid transitions, error handling

### Running Tests

```bash
# Run all lock tests
go test -v ./internal/pager -run TestLock

# Run specific test
go test -v ./internal/pager -run TestAcquireReleaseLock

# Run example
go test -v ./internal/pager -run ExampleLockManager
```

**Note**: Tests are skipped on Windows until the Windows implementation is completed.

## Error Handling

The lock manager defines specific errors:

- **ErrLockBusy**: Lock cannot be acquired due to conflict
- **ErrLockTimeout**: Lock acquisition timed out (future enhancement)
- **ErrInvalidLock**: Invalid lock state transition
- **ErrLockNotHeld**: Attempted to release a lock not held
- **ErrFileNotOpen**: File handle is nil

## Performance Considerations

1. **Non-blocking locks**: All lock operations return immediately
2. **Minimal syscalls**: Only one fcntl call per lock operation
3. **Thread-safe**: RWMutex protects lock state
4. **No polling**: Locks fail immediately if busy
5. **Byte-range locking**: Enables true multi-process concurrency

## Future Enhancements

1. **Complete Windows implementation**: Implement LockFileEx-based locking
2. **Lock timeouts**: Add configurable timeout for blocking operations
3. **Lock monitoring**: Add metrics for lock contention
4. **Lock escalation hints**: Optimize lock upgrade patterns
5. **WAL mode support**: Adapt locking for Write-Ahead Logging mode

## Compatibility

- **Platform**: Linux, Unix, macOS (Windows stub ready)
- **Go version**: 1.26.0+
- **SQLite compatibility**: Follows SQLite 3 locking protocol
- **POSIX**: Uses standard fcntl locking

## References

- SQLite Locking Protocol: https://www.sqlite.org/lockingv3.html
- SQLite Architecture: https://www.sqlite.org/arch.html
- POSIX File Locking: `fcntl(2)` man page
- SQLite Source: src/os_unix.c, src/os_win.c

## Implementation Status

- ✅ Core lock manager (lock.go)
- ✅ Unix/Linux implementation (lock_unix.go)
- ✅ Windows stub (lock_windows.go)
- ✅ Comprehensive tests (lock_test.go)
- ✅ Example code (lock_example_test.go)
- ✅ Integration with pager constants
- ⏳ Windows full implementation (pending)
- ⏳ Pager integration (pending)
- ⏳ ACID compliance testing (Phase 1 goal)

## Summary

This implementation provides a solid foundation for concurrent access control in the Anthony SQLite clone. It follows SQLite's proven five-level locking protocol and provides platform-specific implementations for Unix/Linux systems. The code is well-tested, documented, and ready for integration with the pager's transaction management system.
