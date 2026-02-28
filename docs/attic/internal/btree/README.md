# B-Tree Implementation

This package provides a pure Go implementation of the SQLite B-tree data structure, based on SQLite 3.51.2.

## Overview

SQLite uses a B-tree data structure to organize data on disk. Each database file contains one or more B-trees, where each table or index is stored as a separate B-tree.

### B-Tree Types

SQLite uses four types of B-tree pages:

1. **Interior Table Pages** (0x05): Internal nodes of table B-trees, contain integer keys (rowids) and child page pointers
2. **Leaf Table Pages** (0x0d): Leaf nodes of table B-trees, contain integer keys (rowids) and data payloads
3. **Interior Index Pages** (0x02): Internal nodes of index B-trees, contain arbitrary keys and child page pointers
4. **Leaf Index Pages** (0x0a): Leaf nodes of index B-trees, contain arbitrary keys (no separate data)

## Architecture

### Core Components

#### 1. Variable-Length Integer Encoding (`varint.go`)

SQLite uses a custom variable-length integer format:
- 1-9 bytes per integer
- Lower 7 bits of each byte store data
- High bit (0x80) set on all bytes except the last
- Big-endian (most significant byte first)
- 9th byte uses all 8 bits

**Functions:**
- `PutVarint(p []byte, v uint64) int` - Encode a 64-bit integer
- `GetVarint(p []byte) (uint64, int)` - Decode a 64-bit integer
- `GetVarint32(p []byte) (uint32, int)` - Decode a 32-bit integer (optimized)
- `VarintLen(v uint64) int` - Calculate encoded length without encoding

#### 2. Page Structure (`page.go`)

Each B-tree page has a header followed by cell pointers and cell content.

**Page Header Format:**
```
Leaf Pages (8 bytes):
  Byte 0:   Page type
  Bytes 1-2: First freeblock offset
  Bytes 3-4: Number of cells
  Bytes 5-6: Cell content area start
  Byte 7:   Fragmented free bytes

Interior Pages (12 bytes):
  Same as leaf, plus:
  Bytes 8-11: Right-most child page number
```

#### 3. Cell Parsing (`cell.go`)

Cells contain the actual key-value data stored in the B-tree.

**Cell Formats:**

*Table Leaf Cell:*
```
varint: payload_size
varint: rowid (integer key)
bytes:  payload (up to maxLocal bytes)
uint32: overflow page number (if payload > maxLocal)
```

*Table Interior Cell:*
```
uint32: left child page number
varint: rowid (integer key)
```

*Index Leaf Cell:*
```
varint: payload_size (also serves as key)
bytes:  payload (up to maxLocal bytes)
uint32: overflow page number (if payload > maxLocal)
```

*Index Interior Cell:*
```
uint32: left child page number
varint: payload_size
bytes:  payload (up to maxLocal bytes)
uint32: overflow page number (if payload > maxLocal)
```

#### 4. B-Tree Management (`btree.go`)

Main B-tree structure providing page management and cell iteration.

**Key Functions:**
- `NewBtree(pageSize uint32) *Btree` - Create new B-tree
- `GetPage(pageNum uint32) ([]byte, error)` - Retrieve page
- `SetPage(pageNum uint32, data []byte) error` - Store page
- `ParsePage(pageNum uint32) (*PageHeader, []*CellInfo, error)` - Parse entire page
- `IteratePage(pageNum uint32, visitor func(int, *CellInfo) error) error` - Iterate cells

#### 5. Cursor Operations (`cursor.go`)

Cursors provide sequential access to B-tree entries.

**Cursor States:**
- `CursorValid` - Positioned at valid entry
- `CursorInvalid` - Not positioned at valid entry
- `CursorSkipNext` - Next operation should be no-op
- `CursorRequireSeek` - Position needs restoration
- `CursorFault` - Unrecoverable error

**Key Functions:**
- `NewCursor(bt *Btree, rootPage uint32) *BtCursor` - Create cursor
- `MoveToFirst() error` - Move to first (leftmost) entry
- `MoveToLast() error` - Move to last (rightmost) entry
- `Next() error` - Move to next entry in order
- `Previous() error` - Move to previous entry in order
- `Insert(key int64, payload []byte) error` - Insert new row
- `Delete() error` - Delete current row

#### 6. Page Splitting (`split.go`)

Handles B-tree page splits when pages become full.

**Split Algorithm:**

For leaf pages:
1. Collect all cells plus new cell, sorted by key
2. Find median index to divide cells
3. Allocate new sibling page
4. Distribute cells between left (original) and right (new) pages
5. Update parent with divider key
6. Create new root if splitting root page

For interior pages:
1. Collect all cells plus new divider
2. Find median key to promote to parent
3. Distribute cells, promoting median
4. Update child pointers
5. Recursively handle parent splits

**Key Functions:**
- `splitLeafPage(cursor, key, payload)` - Split full leaf page
- `splitInteriorPage(cursor, key, childPgno)` - Split full interior page
- `updateParentAfterSplit()` - Handle cascading splits
- `createNewRoot()` - Create new root when old root splits

#### 7. Integrity Checking (`integrity.go`)

Comprehensive B-tree integrity verification.

**Checks Performed:**
- Page format validation (headers, cell pointers, content areas)
- Cell overlap detection
- Key ordering verification
- Parent-child pointer validation
- Cycle detection
- Depth limit enforcement
- Free block list validation
- Orphan page detection

**Key Functions:**
- `CheckIntegrity(bt *Btree, rootPage uint32) *IntegrityResult` - Full recursive check
- `CheckPageIntegrity(bt *Btree, pageNum uint32) *IntegrityResult` - Single page check
- `ValidateFreeBlockList(bt *Btree, pageNum uint32) *IntegrityResult` - Free block validation

**Error Types Detected:** 26 different integrity error types including corruption, invalid structures, and consistency violations.

## Usage Examples

### Basic B-Tree Operations

```go
// Create B-tree with 4KB pages
bt := btree.NewBtree(4096)
rootPage, _ := bt.CreateTable()
cursor := btree.NewCursor(bt, rootPage)

// Insert data
for i := 1; i <= 1000; i++ {
    key := int64(i)
    payload := []byte(fmt.Sprintf("Row %d", i))
    err := cursor.Insert(key, payload)
    if err != nil {
        log.Fatal(err)
    }
}

// Iterate through all entries
cursor.MoveToFirst()
for cursor.IsValid() {
    key := cursor.GetKey()
    payload := cursor.GetPayload()
    fmt.Printf("Key: %d, Data: %s\n", key, string(payload))
    cursor.Next()
}
```

### Integrity Checking

```go
// Check B-tree integrity
result := btree.CheckIntegrity(bt, rootPage)

if result.OK() {
    fmt.Printf("B-tree is healthy: %d pages, %d rows\n",
        result.PagesVisited, result.RowCount)
} else {
    fmt.Println("B-tree has errors:")
    for _, err := range result.Errors {
        fmt.Printf("  Page %d: %s - %s\n",
            err.PageNum, err.Type, err.Description)
    }
}
```

### Page Parsing

```go
// Parse a B-tree page
header, cells, err := bt.ParsePage(pageNum)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Page type: %s\n", header)
fmt.Printf("Number of cells: %d\n", len(cells))

for i, cell := range cells {
    fmt.Printf("Cell %d: key=%d, size=%d bytes\n",
        i, cell.Key, len(cell.Payload))
}
```

## Implementation Details

### Payload Overflow

When a cell's payload is too large to fit on the page, the excess is stored in overflow pages:

1. **maxLocal** is calculated based on page size and type
2. If `payload_size <= maxLocal`, entire payload is on the page
3. Otherwise:
   - **minLocal** bytes minimum stored locally
   - Surplus calculated to optimize overflow page usage
   - Overflow pages form a linked list
   - Each overflow page has 4-byte next pointer + data

### Tree Navigation

The cursor maintains a stack of (page, index) pairs representing the path from root to current position:

```go
type BtCursor struct {
    PageStack  [MaxBtreeDepth]uint32  // Page numbers
    IndexStack [MaxBtreeDepth]int     // Cell indices
    Depth      int                     // Current depth
    // ...
}
```

Navigation operations (Next/Previous) use this stack to efficiently traverse the tree without re-reading parent pages.

### Page Layout

```
┌─────────────────────────────────────────────┐
│ File Header (100 bytes, page 1 only)       │
├─────────────────────────────────────────────┤
│ Page Header (8-12 bytes)                    │
├─────────────────────────────────────────────┤
│ Cell Pointer Array (2 bytes × numCells)    │
│ ↓ grows downward                            │
├─────────────────────────────────────────────┤
│ Unallocated Space                           │
├─────────────────────────────────────────────┤
│ ↑ grows upward                              │
│ Cell Content Area (variable size)           │
└─────────────────────────────────────────────┘
```

## Performance Characteristics

- **Page access**: O(1) with in-memory cache
- **Cell parsing**: O(1) per cell
- **Cursor navigation**: O(log N) worst case (tree height)
- **Sequential iteration**: O(1) amortized per entry
- **Insert**: O(log N) search + O(N) split (amortized O(1) split cost)
- **Delete**: O(log N) search + O(N) cell removal

## Testing

Run all tests:
```bash
go test -v ./internal/btree/...
```

Run with coverage:
```bash
go test -cover ./internal/btree/...
```

Run specific test categories:
```bash
go test -v ./internal/btree -run Split      # Split tests
go test -v ./internal/btree -run Integrity  # Integrity tests
go test -v ./internal/btree -run Cursor     # Cursor tests
```

## Implementation Status

### Completed
- Variable-length integer encoding/decoding
- Page header parsing for all page types
- Cell parsing for all cell formats
- B-tree cursor with full navigation
- Insert operations with automatic splitting
- Delete operations
- Page splitting (leaf and interior)
- Root splitting and tree growth
- Integrity checking (26 error types)
- Comprehensive test coverage

### Limitations
- Index B-trees support incomplete (only table B-trees fully tested)
- Overflow pages supported but not extensively tested
- No page merging/rebalancing for underfull pages
- No auto-vacuum support
- In-memory page cache only (no disk-based paging)

## References

- [SQLite File Format Documentation](https://www.sqlite.org/fileformat.html)
- [SQLite B-tree Module](https://www.sqlite.org/btree.html)
- SQLite source: `btree.c`, `btree.h`, `btreeInt.h`
- Knuth, "The Art of Computer Programming, Volume 3: Sorting and Searching"
- Bayer & McCreight (1972) - Original B-tree paper

## License

This implementation is based on the SQLite source code, which is in the public domain.
The Go implementation follows the same public domain dedication.
