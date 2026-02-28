# SQLite Pager Implementation

This package implements a pure Go SQLite database pager, which is responsible for reading and writing pages from/to the database file, managing the page cache, and providing atomic commit/rollback through journaling.

## Overview

The pager is a critical component of the SQLite architecture. It sits between the B-tree layer and the operating system's file I/O layer, providing:

1. **Page-based I/O**: All database access is done in fixed-size pages
2. **Caching**: Frequently accessed pages are kept in memory
3. **Atomic Commits**: Using a rollback journal to ensure ACID properties
4. **Concurrency Control**: File locking to prevent corruption

## Architecture

Based on the SQLite C reference implementation (`pager.c` and `pager.h`), this Go implementation provides:

### Files

- **format.go**: SQLite database file format constants and header parsing
  - Database header structure (100 bytes)
  - Magic header string validation
  - Page size validation and encoding
  - Header serialization/deserialization

- **page.go**: Page structure and cache management
  - `DbPage`: Individual page with reference counting
  - `PageCache`: LRU-style cache with dirty page tracking
  - Thread-safe operations with mutexes

- **pager.go**: Main pager implementation
  - Page I/O (read/write from/to disk)
  - Transaction management (begin/commit/rollback)
  - Journal file management
  - State machine implementation

## Database File Format

The SQLite database file format is defined by a 100-byte header at the beginning of the file:

```
Offset  Size  Description
------  ----  -----------
0       16    Magic header string: "SQLite format 3\0"
16      2     Page size (big-endian, 1 = 65536)
18      1     File format write version
19      1     File format read version
20      1     Reserved space at end of each page
21      1     Maximum embedded payload fraction (64)
22      1     Minimum embedded payload fraction (32)
23      1     Leaf payload fraction (32)
24      4     File change counter
28      4     Database size in pages
32      4     First freelist trunk page
36      4     Total freelist pages
40      4     Schema cookie
44      4     Schema format number (1-4)
48      4     Default page cache size
52      4     Largest root page (auto-vacuum)
56      4     Text encoding (1=UTF-8, 2=UTF-16le, 3=UTF-16be)
60      4     User version
64      4     Incremental vacuum mode
68      4     Application ID
72      20    Reserved space (must be zero)
92      4     Version-valid-for number
96      4     SQLite version number
```

## Page Management

### Page Structure

Each page has:

- **Page number** (Pgno): 1-based page identifier
- **Data**: Raw page content (size = database page size)
- **Flags**: State flags (clean, dirty, writeable, etc.)
- **Reference count**: Number of active users

### Page States

Pages can be in different states:

- **Clean**: Not modified since last disk write
- **Dirty**: Modified but not yet written to disk
- **Writeable**: Journaled and ready to be modified

### Page Cache

The page cache maintains:

- Hash map of page number → page
- Dirty page list for efficient commit
- LRU eviction policy for clean pages
- Reference counting to prevent premature eviction

## Pager States

The pager implements a state machine based on SQLite's design:

1. **OPEN**: No transaction active, file may not be locked
2. **READER**: Read transaction active, shared lock held
3. **WRITER_LOCKED**: Write transaction started, locks acquired
4. **WRITER_CACHEMOD**: Cache modified, journal opened
5. **WRITER_DBMOD**: Database file modified
6. **WRITER_FINISHED**: Ready to commit
7. **ERROR**: Error state, rollback required

## Transaction Management

### Write Transactions

1. **Begin**: Acquire reserved lock, open journal
2. **Journal**: Write original page content to journal before modification
3. **Modify**: Update pages in cache
4. **Commit**:
   - Write all dirty pages to disk
   - Sync database file
   - Delete/truncate journal
   - Release locks

### Rollback

1. Read journal entries
2. Restore original page content to database
3. Sync database file
4. Delete journal
5. Clear cache

## Journal File Format

The journal file records original page content before modification:

```
[4 bytes: page size]
[4 bytes: page number][page size bytes: page data]
[4 bytes: page number][page size bytes: page data]
...
```

## Usage Examples

### Opening a Database

```go
// Open existing or create new database
pager, err := pager.Open("mydb.db", false)
if err != nil {
    log.Fatal(err)
}
defer pager.Close()

// Open with specific page size
pager, err := pager.OpenWithPageSize("mydb.db", false, 8192)
```

### Reading a Page

```go
// Get page 1
page, err := pager.Get(1)
if err != nil {
    log.Fatal(err)
}
defer pager.Put(page)

// Read data from page
data, err := page.Read(offset, length)
```

### Writing a Page

```go
// Get page and mark for writing
page, err := pager.Get(1)
if err != nil {
    log.Fatal(err)
}
defer pager.Put(page)

// Mark page as writeable (journals original content)
if err := pager.Write(page); err != nil {
    log.Fatal(err)
}

// Modify page content
if err := page.Write(offset, data); err != nil {
    log.Fatal(err)
}

// Commit transaction
if err := pager.Commit(); err != nil {
    log.Fatal(err)
}
```

### Rollback

```go
page, _ := pager.Get(1)
pager.Write(page)
page.Write(0, []byte("test"))

// Undo changes
if err := pager.Rollback(); err != nil {
    log.Fatal(err)
}
```

## Thread Safety

All public operations on the pager and pages are thread-safe:

- Pager uses RWMutex for state protection
- Pages use RWMutex for data access
- Reference counts use atomic operations
- Cache uses RWMutex for map access

## Testing

Comprehensive tests are provided in `*_test.go` files:

```bash
# Run all tests
go test -v

# Run specific test
go test -v -run TestPager_WriteAndCommit

# Run benchmarks
go test -bench=. -benchmem

# Check test coverage
go test -cover
go test -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Test Coverage

Tests cover:

- Database header parsing and validation
- Page creation, modification, and reference counting
- Page cache operations and eviction
- Pager open/close operations
- Read and write transactions
- Commit and rollback operations
- Multi-page transactions
- Error conditions
- Concurrent access

## Performance Considerations

1. **Page Cache**: Adjust `DefaultCacheSize` based on available memory
2. **Page Size**: Larger pages reduce I/O overhead but increase memory usage
3. **Journal Mode**: Different modes offer different performance/safety tradeoffs
4. **Sync Operations**: File syncing is expensive but necessary for durability

## Limitations

This implementation is a simplified version of SQLite's pager:

1. **No WAL Support**: Only rollback journal mode is implemented
2. **Simplified Locking**: File locking is stubbed (would need OS-specific implementation)
3. **No Memory-Mapped I/O**: All I/O goes through standard file operations
4. **No Hot Journal Recovery**: Crash recovery is not implemented
5. **No Shared Cache**: Each pager has its own cache
6. **No Savepoints**: Nested transactions not supported

## Future Enhancements

Potential improvements:

1. Implement WAL (Write-Ahead Logging) mode
2. Add proper file locking (flock/fcntl)
3. Support memory-mapped I/O for read performance
4. Implement hot journal recovery
5. Add savepoint support for nested transactions
6. Optimize cache eviction algorithms
7. Add statistics and monitoring

## References

- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite Pager Documentation](https://www.sqlite.org/arch.html)
- SQLite Source Code: `pager.c`, `pager.h`, `pcache.h`
- [How SQLite Database Works](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)

## License

This implementation is based on the public domain SQLite source code and follows the same blessing:

```
May you do good and not evil.
May you find forgiveness for yourself and forgive others.
May you share freely, never taking more than you give.
```
