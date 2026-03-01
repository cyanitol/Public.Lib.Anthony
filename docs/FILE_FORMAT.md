# SQLite File Format - Go Implementation Guide

This document describes the SQLite database file format as implemented in Anthony, a pure Go SQLite implementation. This guide focuses on Go-specific implementation details and omits C references.

## Table of Contents

- [Overview](#overview)
- [Database File Structure](#database-file-structure)
- [Pages](#pages)
- [Database Header](#database-header)
- [B-tree Pages](#b-tree-pages)
- [Record Format](#record-format)
- [Schema Layer](#schema-layer)
- [Transaction Files](#transaction-files)
- [Go Implementation Notes](#go-implementation-notes)

## Overview

An SQLite database consists of a single main database file containing all data, indexes, and schema information. The file is organized into fixed-size pages, with the first page containing a 100-byte database header.

**Package Location:** `internal/format`, `internal/btree`, `internal/pager`

### Key Characteristics

- **Page-based storage**: All I/O operates on page boundaries
- **B-tree organization**: Tables and indexes stored as B-trees
- **Self-contained**: Single file contains complete database state
- **Cross-platform**: Big-endian integers ensure portability

## Database File Structure

The main database file is the primary component of an SQLite database:

```
┌─────────────────────────────────┐
│  Page 1: Database Header + Data │ ← 100-byte header + page content
├─────────────────────────────────┤
│  Page 2: Data                   │
├─────────────────────────────────┤
│  Page 3: Data                   │
├─────────────────────────────────┤
│  ...                            │
├─────────────────────────────────┤
│  Page N: Data                   │
└─────────────────────────────────┘
```

### Supporting Files

- **Rollback Journal**: `<database>-journal` (traditional mode)
- **Write-Ahead Log**: `<database>-wal` (WAL mode)
- **Shared Memory**: `<database>-shm` (WAL mode)

## Pages

### Page Characteristics

**Page Size:**
- Power of two between 512 and 65,536 bytes
- Default: 4,096 bytes
- Specified at database creation (offset 16-17 in header)
- All pages in database are same size

**Page Numbering:**
- Pages numbered starting at 1 (not 0)
- Maximum page number: 4,294,967,294 (2³² - 2)
- Page 1 is special: contains database header

**Maximum Database Size:**
- Theoretical: ~281 terabytes (2³² pages × 65,536 bytes)
- Practical: Limited by filesystem

### Page Types

Every page has exactly one type:

1. **B-tree Pages**
   - Table B-tree interior page
   - Table B-tree leaf page
   - Index B-tree interior page
   - Index B-tree leaf page

2. **Freelist Pages**
   - Freelist trunk page
   - Freelist leaf page

3. **Overflow Pages**
   - Store large cell payloads

4. **Pointer Map Pages**
   - Used in auto-vacuum mode

5. **Lock-byte Page**
   - Reserved for locking (page 1,073,741,824 for 1KB pages)

## Database Header

The first 100 bytes of page 1 contain the database header. All multi-byte integers are stored big-endian.

### Header Format

```go
type DatabaseHeader struct {
    Magic              [16]byte  // Offset 0: "SQLite format 3\x00"
    PageSize           uint16    // Offset 16: Page size (512-65536, or 1=65536)
    WriteVersion       uint8     // Offset 18: 1=legacy, 2=WAL
    ReadVersion        uint8     // Offset 19: 1=legacy, 2=WAL
    ReservedSpace      uint8     // Offset 20: Unused bytes per page
    MaxEmbedPayload    uint8     // Offset 21: Must be 64
    MinEmbedPayload    uint8     // Offset 22: Must be 32
    LeafPayloadFrac    uint8     // Offset 23: Must be 32
    FileChangeCounter  uint32    // Offset 24: Incremented on modification
    DatabaseSize       uint32    // Offset 28: Size in pages
    FirstFreelistPage  uint32    // Offset 32: First freelist trunk page
    FreelistCount      uint32    // Offset 36: Total freelist pages
    SchemaCookie       uint32    // Offset 40: Schema change counter
    SchemaFormat       uint32    // Offset 44: Schema format (1-4)
    DefaultCacheSize   uint32    // Offset 48: Suggested cache size
    LargestRootPage    uint32    // Offset 52: For auto-vacuum
    TextEncoding       uint32    // Offset 56: 1=UTF-8, 2=UTF-16le, 3=UTF-16be
    UserVersion        uint32    // Offset 60: User version
    IncrementalVacuum  uint32    // Offset 64: Non-zero for incremental vacuum
    ApplicationID      uint32    // Offset 68: Application identifier
    Reserved           [20]byte  // Offset 72: Reserved for expansion
    VersionValidFor    uint32    // Offset 92: Version validation
    VersionNumber      uint32    // Offset 96: SQLite version number
}
```

### Key Header Fields

**Magic Header String (Offset 0, 16 bytes)**
```go
magicHeader := []byte{'S', 'Q', 'L', 'i', 't', 'e', ' ',
                      'f', 'o', 'r', 'm', 'a', 't', ' ', '3', 0}
```

Every valid SQLite database begins with this exact sequence.

**Page Size (Offset 16, 2 bytes)**
```go
// Read page size
pageSize := binary.BigEndian.Uint16(header[16:18])
if pageSize == 1 {
    pageSize = 65536 // Special case for 64KB pages
}
```

**File Format Versions (Offsets 18-19)**
- `1`: Rollback journal mode
- `2`: Write-ahead log (WAL) mode

**Schema Cookie (Offset 40)**

Incremented whenever the database schema changes. Used to invalidate prepared statements:

```go
// Check if schema changed
if currentSchemaCookie != preparedSchemaCookie {
    // Reprepare statement
}
```

**Text Encoding (Offset 56)**
- `1`: UTF-8 (recommended)
- `2`: UTF-16 little-endian
- `3`: UTF-16 big-endian

**User Version (Offset 60)**

Application-defined version number, accessible via:
```sql
PRAGMA user_version;
PRAGMA user_version = 5;
```

**Application ID (Offset 68)**

Identifies the application that created the database:
```sql
PRAGMA application_id;
PRAGMA application_id = 0x12345678;
```

## B-tree Pages

SQLite stores all tables and indexes as B-trees. Each B-tree page contains a header followed by cells.

### B-tree Page Header

**Interior Table Page (type 0x05):**
```go
type BTreeInteriorTablePage struct {
    PageType           uint8   // 0x05
    FirstFreeblock     uint16  // Offset to first freeblock
    CellCount          uint16  // Number of cells
    CellContentOffset  uint16  // Offset to cell content area
    FragmentedBytes    uint8   // Fragmented free bytes
    RightmostPointer   uint32  // Page number of right child
    // Cell pointer array follows
    // Cell content area at end
}
```

**Leaf Table Page (type 0x0D):**
```go
type BTreeLeafTablePage struct {
    PageType           uint8   // 0x0D
    FirstFreeblock     uint16  // Offset to first freeblock
    CellCount          uint16  // Number of cells
    CellContentOffset  uint16  // Offset to cell content area
    FragmentedBytes    uint8   // Fragmented free bytes
    // Cell pointer array follows
    // Cell content area at end
}
```

**Interior Index Page (type 0x02):**
```go
type BTreeInteriorIndexPage struct {
    PageType           uint8   // 0x02
    FirstFreeblock     uint16
    CellCount          uint16
    CellContentOffset  uint16
    FragmentedBytes    uint8
    RightmostPointer   uint32
}
```

**Leaf Index Page (type 0x0A):**
```go
type BTreeLeafIndexPage struct {
    PageType           uint8   // 0x0A
    FirstFreeblock     uint16
    CellCount          uint16
    CellContentOffset  uint16
    FragmentedBytes    uint8
}
```

### Cell Format

Each cell contains a record. The structure varies by page type:

**Table Leaf Cell:**
```
┌──────────────┬────────┬────────┐
│ Payload Size │ RowID  │ Payload│
│  (varint)    │(varint)│ (blob) │
└──────────────┴────────┴────────┘
```

**Table Interior Cell:**
```
┌─────────────┬────────┐
│ Left Child  │ RowID  │
│  (4 bytes)  │(varint)│
└─────────────┴────────┘
```

**Index Leaf Cell:**
```
┌──────────────┬────────┐
│ Payload Size │ Payload│
│  (varint)    │ (blob) │
└──────────────┴────────┘
```

**Index Interior Cell:**
```
┌─────────────┬──────────────┬────────┐
│ Left Child  │ Payload Size │ Payload│
│  (4 bytes)  │  (varint)    │ (blob) │
└─────────────┴──────────────┴────────┘
```

## Record Format

Records in SQLite use a space-efficient format with variable-length encoding.

### Record Structure

```
┌──────────────┬────────────┬──────────────┬─────────────┐
│ Header Size  │ Type Code₁ │ Type Code₂...│ Value₁, ... │
│  (varint)    │  (varint)  │  (varint)    │  (binary)   │
└──────────────┴────────────┴──────────────┴─────────────┘
```

### Serial Type Codes

```go
const (
    SerialNull      = 0   // NULL value
    Serial8bitInt   = 1   // 1-byte signed integer
    Serial16bitInt  = 2   // 2-byte signed integer (big-endian)
    Serial24bitInt  = 3   // 3-byte signed integer (big-endian)
    Serial32bitInt  = 4   // 4-byte signed integer (big-endian)
    Serial48bitInt  = 5   // 6-byte signed integer (big-endian)
    Serial64bitInt  = 6   // 8-byte signed integer (big-endian)
    SerialFloat     = 7   // 8-byte IEEE 754 float (big-endian)
    SerialZero      = 8   // Integer 0 (schema format 4+)
    SerialOne       = 9   // Integer 1 (schema format 4+)
    // 10 and 11 are reserved
)

// For serial type N >= 12:
// Even N (N&1 == 0): BLOB of length (N-12)/2
// Odd N  (N&1 == 1): TEXT of length (N-13)/2
```

### Varint Encoding

Variable-length integers (varints) save space for small numbers:

```go
// WriteVarint writes an unsigned 64-bit integer as a varint
// Returns the number of bytes written (1-9 bytes)
func WriteVarint(buf []byte, value uint64) int {
    if value <= 240 {
        buf[0] = byte(value)
        return 1
    }
    // ... more complex encoding for larger values
}

// ReadVarint reads a varint from buf
// Returns the value and number of bytes consumed
func ReadVarint(buf []byte) (uint64, int) {
    if buf[0] <= 240 {
        return uint64(buf[0]), 1
    }
    // ... decoding logic
}
```

### Example Record Encoding

**SQL:**
```sql
CREATE TABLE users (id INTEGER, name TEXT, active INTEGER);
INSERT INTO users VALUES (42, 'Alice', 1);
```

**Encoded Record:**
```
Header Size: 4 (varint)
Type Codes: [4, 19, 9]  (varints)
  - 4: 32-bit integer (id)
  - 19: TEXT of length (19-13)/2 = 3 bytes (name)
  - 9: Integer constant 1 (active)

Values:
  - 0x0000002A (42 as 4-byte big-endian)
  - 0x416c696365 ("Alice" as UTF-8)
  - (no bytes, encoded in type code)

Full record: [04 04 13 09 00 00 00 2A 41 6C 69 63 65]
```

## Schema Layer

### Schema Storage

The database schema is stored in the `sqlite_schema` table (also known as `sqlite_master`):

```sql
CREATE TABLE sqlite_schema (
    type     TEXT,    -- 'table', 'index', 'view', 'trigger'
    name     TEXT,    -- Object name
    tbl_name TEXT,    -- Associated table name
    rootpage INTEGER, -- Root B-tree page
    sql      TEXT     -- CREATE statement
);
```

**Go Access:**
```go
// Query schema
rows, err := db.Query(`
    SELECT type, name, sql
    FROM sqlite_schema
    WHERE type = 'table'
`)
```

### Table Representation

**Regular Tables (with ROWID):**
- Stored as B-tree with ROWID as key
- ROWID is 64-bit signed integer
- Table data stored in leaf pages

**WITHOUT ROWID Tables:**
- Primary key is the B-tree key
- More space-efficient for certain schemas
- Requires explicit PRIMARY KEY

```sql
-- Regular table (has implicit ROWID)
CREATE TABLE regular (id INTEGER, name TEXT);

-- WITHOUT ROWID table (primary key is tree key)
CREATE TABLE compact (
    id INTEGER PRIMARY KEY,
    name TEXT
) WITHOUT ROWID;
```

### Index Representation

Indexes are stored as separate B-trees:

```sql
CREATE INDEX idx_name ON users(name);
```

**Index Record Format:**
```
┌─────────────┬─────────────┬────────┐
│ Indexed     │ ROWID       │        │
│ Column(s)   │ (for table) │ (key)  │
└─────────────┴─────────────┴────────┘
```

For WITHOUT ROWID tables, the ROWID is replaced with primary key columns.

### Internal Tables

**sqlite_sequence:**
```sql
-- Tracks AUTOINCREMENT counters
CREATE TABLE sqlite_sequence (
    name TEXT,  -- Table name
    seq  INTEGER -- Next sequence value
);
```

**sqlite_stat1:**
```sql
-- Index statistics for query optimizer
CREATE TABLE sqlite_stat1 (
    tbl  TEXT,    -- Table name
    idx  TEXT,    -- Index name
    stat TEXT     -- Statistics blob
);
```

**sqlite_stat4:**
```sql
-- Detailed index statistics
CREATE TABLE sqlite_stat4 (
    tbl     TEXT,
    idx     TEXT,
    neq     TEXT,    -- Number of rows with same key prefix
    nlt     TEXT,    -- Number of rows less than this sample
    ndlt    TEXT,    -- Number of distinct keys less than sample
    sample  BLOB     -- Sample row
);
```

## Transaction Files

### Rollback Journal

**Format:**
```
┌──────────────┬──────────────┬─────────────┐
│ Journal      │ Page Records │ Commit      │
│ Header       │ (original    │ Record      │
│              │  pages)      │             │
└──────────────┴──────────────┴─────────────┘
```

**Journal Header:**
```go
type JournalHeader struct {
    Magic          [8]byte   // Journal magic number
    PageCount      uint32    // Number of pages in journal
    RandomNonce    uint32    // Random nonce for checksum
    InitialPages   uint32    // Original database size
    SectorSize     uint32    // Sector size
    PageSize       uint32    // Database page size
}
```

### Write-Ahead Log (WAL)

**WAL File Format:**
```
┌──────────────┬──────────────┬──────────────┬─────┐
│ WAL Header   │ Frame 1      │ Frame 2      │ ... │
│ (32 bytes)   │ (hdr + page) │ (hdr + page) │     │
└──────────────┴──────────────┴──────────────┴─────┘
```

**WAL Frame:**
```go
type WALFrame struct {
    PageNumber    uint32    // Page number
    DatabaseSize  uint32    // Database size after commit
    Salt1         uint32    // Checksum salt
    Salt2         uint32    // Checksum salt
    Checksum1     uint32    // Cumulative checksum
    Checksum2     uint32    // Cumulative checksum
    // Followed by page data
}
```

**Advantages of WAL:**
- Readers don't block writers
- Writers don't block readers
- Faster commits (append-only)
- Better concurrency

## Go Implementation Notes

### Memory Management

```go
// Use sync.Pool for page buffers
var pagePool = sync.Pool{
    New: func() interface{} {
        return make([]byte, pageSize)
    },
}

// Acquire page buffer
page := pagePool.Get().([]byte)
defer pagePool.Put(page)
```

### Endianness Handling

```go
import "encoding/binary"

// Always use BigEndian for SQLite format
var bo = binary.BigEndian

// Read 32-bit integer
value := bo.Uint32(buf[offset:])

// Write 32-bit integer
bo.PutUint32(buf[offset:], value)
```

### File I/O

```go
// Use proper file flags for database files
file, err := os.OpenFile(path,
    os.O_RDWR|os.O_CREATE,
    0644)

// Use pread/pwrite for concurrent access
_, err = unix.Pread(fd, buf, offset)
_, err = unix.Pwrite(fd, buf, offset)
```

### Locking

```go
// File locking for concurrent access
import "golang.org/x/sys/unix"

// Acquire shared lock
err := unix.Flock(fd, unix.LOCK_SH)

// Acquire exclusive lock
err := unix.Flock(fd, unix.LOCK_EX)

// Release lock
err := unix.Flock(fd, unix.LOCK_UN)
```

### Validation

```go
// Validate database header
func ValidateHeader(header []byte) error {
    magic := header[0:16]
    if !bytes.Equal(magic, magicHeader) {
        return errors.New("invalid SQLite header")
    }

    pageSize := binary.BigEndian.Uint16(header[16:18])
    if pageSize == 1 {
        pageSize = 65536
    }
    if pageSize < 512 || pageSize > 65536 ||
       (pageSize & (pageSize - 1)) != 0 {
        return errors.New("invalid page size")
    }

    return nil
}
```

## Performance Considerations

### Page Size Selection

**Larger pages (8KB-16KB):**
- Better for sequential scans
- Fewer I/O operations
- More memory usage

**Smaller pages (1KB-2KB):**
- Better for random access
- Less memory per page
- More I/O operations

**Recommended:** 4KB (matches most OS page sizes)

### Caching Strategy

```go
// Implement LRU cache for pages
type PageCache struct {
    mu       sync.RWMutex
    capacity int
    pages    map[uint32]*Page
    lru      *list.List
}
```

### Write Optimization

```go
// Batch writes using transactions
tx, _ := db.Begin()
for _, record := range records {
    tx.Exec("INSERT INTO ...", record)
}
tx.Commit() // Single fsync
```

## References

- **Package:** `internal/format` - File format utilities
- **Package:** `internal/btree` - B-tree implementation
- **Package:** `internal/pager` - Page cache and I/O
- **SQLite Docs (local):** [File Format Specification (local)](sqlite/FILE_FORMAT_SPEC.md) ([sqlite.org](https://www.sqlite.org/fileformat.html))

## See Also

- [TYPE_SYSTEM.md](TYPE_SYSTEM.md) - Type affinity and storage classes
- [ARCHITECTURE.md](ARCHITECTURE.md) - Overall system architecture
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency and locking
