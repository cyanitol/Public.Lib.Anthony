# Pager Quick Start Guide

## Installation

The pager is part of the JuniperBible SQLite implementation:

```bash
go get github.com/JuniperBible/juniper/core/sqlite/internal/pager
```

## Basic Usage

### Creating a New Database

```go
package main

import (
    "log"
    "github.com/JuniperBible/juniper/core/sqlite/internal/pager"
)

func main() {
    // Create a new database
    p, err := pager.Open("mydb.db", false)
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    // Database is ready to use
    log.Println("Database created successfully")
}
```

### Writing Data

```go
// Get a page
page, err := p.Get(1)
if err != nil {
    log.Fatal(err)
}
defer p.Put(page)

// Mark page for writing
if err := p.Write(page); err != nil {
    log.Fatal(err)
}

// Write data to the page (after the 100-byte header)
data := []byte("Hello, World!")
if err := page.Write(pager.DatabaseHeaderSize, data); err != nil {
    log.Fatal(err)
}

// Commit the transaction
if err := p.Commit(); err != nil {
    log.Fatal(err)
}
```

### Reading Data

```go
// Get the page
page, err := p.Get(1)
if err != nil {
    log.Fatal(err)
}
defer p.Put(page)

// Read data from the page
data, err := page.Read(pager.DatabaseHeaderSize, 13)
if err != nil {
    log.Fatal(err)
}

log.Printf("Read: %s\n", string(data))
```

### Rollback Transaction

```go
// Start transaction
page, _ := p.Get(1)
p.Write(page)
page.Write(pager.DatabaseHeaderSize, []byte("test"))

// Changed your mind? Rollback!
if err := p.Rollback(); err != nil {
    log.Fatal(err)
}
p.Put(page)
```

## Common Patterns

### Working with Multiple Pages

```go
// Write to multiple pages in one transaction
for i := 1; i <= 10; i++ {
    page, err := p.Get(pager.Pgno(i))
    if err != nil {
        log.Fatal(err)
    }

    if err := p.Write(page); err != nil {
        log.Fatal(err)
    }

    offset := pager.DatabaseHeaderSize
    if i > 1 {
        offset = 0 // Page 1 has the header
    }

    data := []byte(fmt.Sprintf("Page %d data", i))
    if err := page.Write(offset, data); err != nil {
        log.Fatal(err)
    }

    p.Put(page)
}

// Commit all changes at once
if err := p.Commit(); err != nil {
    log.Fatal(err)
}
```

### Custom Page Size

```go
// Create database with 8KB pages
p, err := pager.OpenWithPageSize("mydb.db", false, 8192)
if err != nil {
    log.Fatal(err)
}
defer p.Close()
```

### Read-Only Mode

```go
// Open existing database in read-only mode
p, err := pager.Open("mydb.db", true)
if err != nil {
    log.Fatal(err)
}
defer p.Close()

// Can read but not write
page, _ := p.Get(1)
data, _ := page.Read(0, 100)
p.Put(page)

// This will fail:
// p.Write(page) // Error: pager is read-only
```

### Accessing the Database Header

```go
header := p.GetHeader()

fmt.Printf("Page size: %d\n", header.GetPageSize())
fmt.Printf("Database size: %d pages\n", header.DatabaseSize)
fmt.Printf("Text encoding: %d\n", header.TextEncoding)
fmt.Printf("User version: %d\n", header.UserVersion)
```

## API Reference

### Pager Functions

| Function | Description |
|----------|-------------|
| `Open(filename, readOnly)` | Open or create database |
| `OpenWithPageSize(filename, readOnly, pageSize)` | Open with specific page size |
| `Close()` | Close database |
| `Get(pgno)` | Get page by number |
| `Put(page)` | Release page reference |
| `Write(page)` | Mark page for writing |
| `Commit()` | Commit transaction |
| `Rollback()` | Rollback transaction |
| `PageSize()` | Get page size |
| `PageCount()` | Get number of pages |
| `IsReadOnly()` | Check if read-only |
| `GetHeader()` | Get database header |

### Page Functions

| Function | Description |
|----------|-------------|
| `Read(offset, length)` | Read data from page |
| `Write(offset, data)` | Write data to page |
| `IsDirty()` | Check if page is modified |
| `IsClean()` | Check if page is clean |
| `IsWriteable()` | Check if page is writeable |
| `MakeDirty()` | Mark page as dirty |
| `MakeClean()` | Mark page as clean |
| `Ref()` | Increment reference count |
| `Unref()` | Decrement reference count |
| `GetRefCount()` | Get reference count |
| `Size()` | Get page size |
| `Zero()` | Zero out page content |
| `Clone()` | Create deep copy |

## Constants

```go
// Database header
pager.DatabaseHeaderSize  // 100 bytes
pager.MagicHeaderString  // "SQLite format 3\0"

// Page sizes
pager.DefaultPageSize    // 4096 bytes
pager.MinPageSize        // 512 bytes
pager.MaxPageSize        // 65536 bytes

// Text encodings
pager.EncodingUTF8       // 1
pager.EncodingUTF16LE    // 2
pager.EncodingUTF16BE    // 3
```

## Error Handling

```go
page, err := p.Get(1)
if err != nil {
    switch err {
    case pager.ErrInvalidPageNum:
        log.Println("Invalid page number")
    case pager.ErrPageNotFound:
        log.Println("Page not found")
    case pager.ErrReadOnly:
        log.Println("Database is read-only")
    default:
        log.Printf("Error: %v\n", err)
    }
    return
}
defer p.Put(page)
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ErrInvalidPageSize` | Invalid page size | Use power of 2 between 512-65536 |
| `ErrInvalidPageNum` | Page number 0 or too large | Use valid page numbers (1+) |
| `ErrReadOnly` | Write on read-only database | Open with readOnly=false |
| `ErrNoTransaction` | Commit/rollback without transaction | Start transaction with Write() |
| `ErrCacheFull` | Cache full, can't evict dirty pages | Commit or increase cache size |

## Best Practices

### 1. Always Release Pages

```go
page, err := p.Get(1)
if err != nil {
    return err
}
defer p.Put(page)  // ALWAYS release!
```

### 2. Commit or Rollback

```go
// Either commit...
if err := p.Commit(); err != nil {
    return err
}

// ...or rollback
if err := p.Rollback(); err != nil {
    return err
}
```

### 3. Handle Errors

```go
if err := p.Write(page); err != nil {
    p.Rollback()  // Clean up on error
    return err
}
```

### 4. Close Database

```go
p, err := pager.Open("mydb.db", false)
if err != nil {
    return err
}
defer p.Close()  // Ensures cleanup
```

## Performance Tips

### 1. Batch Writes

```go
// Good: Multiple writes in one transaction
for i := 1; i <= 100; i++ {
    page, _ := p.Get(pager.Pgno(i))
    p.Write(page)
    page.Write(0, data)
    p.Put(page)
}
p.Commit()  // One commit for all

// Bad: Commit after each write
for i := 1; i <= 100; i++ {
    page, _ := p.Get(pager.Pgno(i))
    p.Write(page)
    page.Write(0, data)
    p.Commit()  // Slow!
    p.Put(page)
}
```

### 2. Reuse Pages

```go
// Cache helps automatically, but you can also:
page, _ := p.Get(1)
// Use page multiple times
data1, _ := page.Read(0, 100)
data2, _ := page.Read(100, 100)
p.Put(page)  // Release once when done
```

### 3. Choose Appropriate Page Size

```go
// Small files: 1-4KB pages
p, _ := pager.OpenWithPageSize("small.db", false, 1024)

// Large files: 8-16KB pages
p, _ := pager.OpenWithPageSize("large.db", false, 8192)

// Very large files: 32-64KB pages
p, _ := pager.OpenWithPageSize("huge.db", false, 32768)
```

## Debugging

### Enable Logging

```go
// Read header to see database info
header := p.GetHeader()
log.Printf("Database info:")
log.Printf("  Page size: %d", header.GetPageSize())
log.Printf("  Pages: %d", header.DatabaseSize)
log.Printf("  Change counter: %d", header.FileChangeCounter)
```

### Check Page State

```go
page, _ := p.Get(1)
log.Printf("Page 1:")
log.Printf("  Dirty: %v", page.IsDirty())
log.Printf("  Writeable: %v", page.IsWriteable())
log.Printf("  Ref count: %d", page.GetRefCount())
p.Put(page)
```

### Check Pager State

```go
log.Printf("Pager info:")
log.Printf("  Page size: %d", p.PageSize())
log.Printf("  Page count: %d", p.PageCount())
log.Printf("  Read-only: %v", p.IsReadOnly())
```

## Testing

Run the included tests:

```bash
# All tests
go test -v

# Specific test
go test -v -run TestPager_WriteAndCommit

# With coverage
go test -cover

# Benchmarks
go test -bench=. -benchmem
```

## More Information

- See `README.md` for complete documentation
- See `example_test.go` for more examples
- See `IMPLEMENTATION.md` for implementation details
- See `doc.go` for package documentation
