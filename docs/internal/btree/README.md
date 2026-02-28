# SQLite B-Tree Implementation in Go

This package provides a pure Go implementation of the SQLite B-tree data structure, based on the reference C implementation from SQLite 3.51.2.

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

**Page Types:**

- `0x02` - Interior index B-tree page
- `0x05` - Interior table B-tree page
- `0x0a` - Leaf index B-tree page
- `0x0d` - Leaf table B-tree page

**Functions:**

- `ParsePageHeader(data []byte, pageNum uint32) (*PageHeader, error)` - Parse page header
- `GetCellPointer(data []byte, cellIndex int) (uint16, error)` - Get cell offset
- `GetCellPointers(data []byte) ([]uint16, error)` - Get all cell offsets

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

**Functions:**

- `ParseCell(pageType byte, cellData []byte, usableSize uint32) (*CellInfo, error)` - Parse any cell type

#### 4. B-Tree Management (`btree.go`)

Main B-tree structure providing page management and cell iteration.

**Structures:**

- `Btree` - B-tree instance with page cache
- `BtShared` - Shared B-tree metadata (page sizes, transaction state)

**Functions:**

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

**Functions:**

- `NewCursor(bt *Btree, rootPage uint32) *BtCursor` - Create cursor
- `MoveToFirst() error` - Move to first (leftmost) entry
- `MoveToLast() error` - Move to last (rightmost) entry
- `Next() error` - Move to next entry in order
- `Previous() error` - Move to previous entry in order
- `IsValid() bool` - Check if cursor is valid
- `GetKey() int64` - Get current key
- `GetPayload() []byte` - Get current payload

## Usage Examples

### Example 1: Parse a B-Tree Page

```go
package main

import (
    "fmt"
    "github.com/yourusername/btree"
)

func main() {
    // Create B-tree with 4KB pages
    bt := btree.NewBtree(4096)

    // Load page data (from file, etc.)
    pageData := loadPageFromDisk(1)
    bt.SetPage(1, pageData)

    // Parse page
    header, cells, err := bt.ParsePage(1)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Page type: %s\n", header)
    fmt.Printf("Number of cells: %d\n", len(cells))

    for i, cell := range cells {
        fmt.Printf("Cell %d: key=%d, payload=%q\n",
            i, cell.Key, string(cell.Payload))
    }
}
```

### Example 2: Iterate Through a B-Tree

```go
func iterateTable(bt *btree.Btree, rootPage uint32) error {
    cursor := btree.NewCursor(bt, rootPage)

    // Move to first entry
    if err := cursor.MoveToFirst(); err != nil {
        return err
    }

    // Iterate through all entries
    for cursor.IsValid() {
        key := cursor.GetKey()
        payload := cursor.GetPayload()

        fmt.Printf("Rowid: %d, Data: %q\n", key, string(payload))

        if err := cursor.Next(); err != nil {
            if err.Error() == "end of btree" {
                break
            }
            return err
        }
    }

    return nil
}
```

### Example 3: Encode/Decode Varints

```go
func varintExample() {
    // Encode a value
    var buf [9]byte
    value := uint64(12345678)
    n := btree.PutVarint(buf[:], value)
    fmt.Printf("Encoded %d in %d bytes: %x\n", value, n, buf[:n])

    // Decode the value
    decoded, m := btree.GetVarint(buf[:])
    fmt.Printf("Decoded %d from %d bytes\n", decoded, m)

    // Check length without encoding
    length := btree.VarintLen(value)
    fmt.Printf("Varint length: %d\n", length)
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

## Testing

Run all tests:
```bash
go test -v ./...
```

Run with coverage:
```bash
go test -cover ./...
```

Run benchmarks:
```bash
go test -bench=. -benchmem ./...
```

## Performance Characteristics

- **Page access**: O(1) with in-memory cache
- **Cell parsing**: O(1) per cell
- **Cursor navigation**: O(log N) worst case (tree height)
- **Sequential iteration**: O(1) amortized per entry

## Limitations

This implementation currently:

- Does not handle overflow pages (reads only local payload)
- Does not support write operations (read-only)
- Does not implement auto-vacuum
- Does not support WAL (write-ahead logging)
- Keeps all pages in memory (no paging to disk)

## References

- [SQLite File Format Documentation](https://www.sqlite.org/fileformat.html)
- SQLite source: `btree.c`, `btree.h`, `btreeInt.h`
- Donald E. Knuth, "The Art of Computer Programming, Volume 3: Sorting and Searching"

## License

This implementation is based on the SQLite source code, which is in the public domain.
