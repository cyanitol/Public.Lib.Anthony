# Format Package

This package defines SQLite file format constants and structures.

## Overview

The format package provides comprehensive definitions for the SQLite database file format, including header structures, page types, and validation functions. It serves as the foundational layer for the pure Go SQLite engine implementation.

## Database File Header

Every SQLite database file begins with a 100-byte header containing metadata about the database.

### Header Contents

- Magic string ("SQLite format 3\x00")
- Page size (512 to 65536 bytes, must be power of 2)
- File format versions
- Text encoding (UTF-8, UTF-16LE, UTF-16BE)
- Schema metadata
- Freelist information
- User-defined metadata

### Usage

**Parse an existing database header:**
```go
data := make([]byte, format.HeaderSize)
_, err := file.Read(data)
if err != nil {
    log.Fatal(err)
}

header := &format.Header{}
if err := header.Parse(data); err != nil {
    log.Fatal(err)
}

fmt.Printf("Page size: %d\n", header.GetPageSize())
fmt.Printf("Text encoding: %d\n", header.TextEncoding)
```

**Create a new database header:**
```go
header := format.NewHeader(4096)
header.UserVersion = 1
header.AppID = 0x12345678

data := header.Serialize()
// Write data to file...
```

## Page Types

SQLite uses B-tree pages to store data. There are four page types:

1. **Interior Index** (0x02): Interior nodes of index B-trees
2. **Interior Table** (0x05): Interior nodes of table B-trees
3. **Leaf Index** (0x0a): Leaf nodes of index B-trees
4. **Leaf Table** (0x0d): Leaf nodes of table B-trees

### Page Header Structure

Each page begins with a page header containing:
- Page type (1 byte)
- First freeblock offset (2 bytes)
- Number of cells (2 bytes)
- Cell content start (2 bytes)
- Fragmented bytes (1 byte)
- Right-most pointer (4 bytes, interior pages only)

## Text Encoding

SQLite supports three text encoding schemes:

1. **UTF-8** (encoding value 1): Default, most common
2. **UTF-16 Little-Endian** (encoding value 2)
3. **UTF-16 Big-Endian** (encoding value 3)

The text encoding is set when the database is created and cannot be changed.

## Validation

The package provides validation functions to ensure database headers and page sizes conform to SQLite specifications:

```go
if !format.IsValidPageSize(pageSize) {
    log.Fatalf("Invalid page size: %d", pageSize)
}

if err := header.Validate(); err != nil {
    log.Fatalf("Invalid header: %v", err)
}
```

## Constants

All SQLite file format constants are defined as package-level constants:

- Header offsets (OffsetMagic, OffsetPageSize, etc.)
- Page type values (PageTypeLeafTable, PageTypeInteriorIndex, etc.)
- B-tree header offsets (BtreePageType, BtreeCellCount, etc.)
- Text encoding values (EncodingUTF8, EncodingUTF16LE, etc.)
- Size limits (MinPageSize, MaxPageSize, HeaderSize, etc.)

## Thread Safety

All functions and methods in this package are thread-safe and can be called concurrently from multiple goroutines.

## References

- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [SQLite Documentation](https://www.sqlite.org/docs.html)

## License

This implementation is based on the SQLite source code, which is in the public domain.
