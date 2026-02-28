# Write-Ahead Logging (WAL) Mode

## Overview

Write-Ahead Logging (WAL) is an alternative journaling mode that provides better concurrency and performance characteristics compared to traditional rollback journaling. In WAL mode, changes are written to a separate log file (`.db-wal`) before being applied to the main database file.

## Benefits of WAL Mode

1. **Concurrent Readers and Writers**: Multiple readers can access the database while a write transaction is in progress
2. **Better Performance**: Faster write operations as changes are appended to a log file instead of overwriting database pages
3. **Reduced I/O**: Changes are written sequentially to the WAL file, reducing random I/O
4. **Atomic Commits**: All changes in a transaction are committed together
5. **Crash Recovery**: Database can be recovered from the WAL file after a crash

## How WAL Mode Works

### File Structure

When WAL mode is enabled, three files are involved:

- **`.db`**: Main database file (read-only during transactions)
- **`.db-wal`**: Write-Ahead Log file (contains recent changes)
- **`.db-shm`**: Shared memory file (coordinates access between readers and writers)

### Write Process

1. When a write transaction begins, changes are written to the WAL file
2. Each change is stored as a "frame" containing:
   - Page number
   - Page data
   - Database size after this frame
   - Salt values for checksum
   - Checksums for integrity validation

3. On commit, the WAL file is synchronized to disk
4. Readers can still access the database using a combination of:
   - Committed pages from the main database file
   - Recent changes from the WAL file

### Checkpointing

Periodically, the WAL file is "checkpointed" - its contents are transferred back to the main database file. This happens:

- Automatically when the WAL file reaches a certain size (default: 1000 frames)
- When switching from WAL mode to another journal mode
- When explicitly requested via `PRAGMA wal_checkpoint`

## Usage

### Enabling WAL Mode

```go
import (
    "database/sql"
    _ "github.com/JuniperBible/Public.Lib.Anthony"
)

db, err := sql.Open("sqlite_internal", "database.db")
if err != nil {
    log.Fatal(err)
}

// Switch to WAL mode
var mode string
err = db.QueryRow("PRAGMA journal_mode = WAL").Scan(&mode)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Journal mode: %s\n", mode) // Output: wal
```

### Checking Current Mode

```go
var mode string
err := db.QueryRow("PRAGMA journal_mode").Scan(&mode)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Current journal mode: %s\n", mode)
```

### Switching Back to DELETE Mode

```go
// This will checkpoint the WAL and switch back to traditional journaling
var mode string
err := db.QueryRow("PRAGMA journal_mode = DELETE").Scan(&mode)
if err != nil {
    log.Fatal(err)
}
```

### Manual Checkpointing

```go
// Checkpoint the WAL file manually
_, err := db.Exec("PRAGMA wal_checkpoint")
if err != nil {
    log.Fatal(err)
}

// Or with specific checkpoint mode:
// PASSIVE - Checkpoint as much as possible without blocking
// FULL - Wait for readers to finish, then checkpoint all frames
// RESTART - Like FULL, but also reset the WAL
// TRUNCATE - Like RESTART, but also truncate the WAL file to zero bytes
_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
```

## Implementation Details

### WAL File Format

The WAL file consists of:

1. **Header (32 bytes)**:
   - Magic number: `0x377f0682` (big-endian)
   - File format version: `3007000`
   - Page size
   - Checkpoint sequence number
   - Salt values (2 × 32-bit)
   - Checksums (2 × 32-bit)

2. **Frames** (variable number):
   Each frame contains:
   - Page number (4 bytes)
   - Database size in pages (4 bytes)
   - Salt values (2 × 4 bytes)
   - Checksums (2 × 4 bytes)
   - Page data (page size bytes)

### WAL Index (Shared Memory)

The `.db-shm` file provides fast lookup of pages in the WAL:

- **Header**: Contains metadata about WAL state
- **Hash Table**: Maps page numbers to frame numbers
- **Read Marks**: Track which frames each reader has consumed

The WAL index is memory-mapped for efficient concurrent access.

### Concurrency Model

```
┌─────────────┐
│   Reader 1  │────┐
└─────────────┘    │
                   ├──→ ┌─────────────┐      ┌──────────┐
┌─────────────┐    │    │  WAL Index  │◄────►│ WAL File │
│   Reader 2  │────┤    │   (.db-shm) │      │ (.db-wal)│
└─────────────┘    │    └─────────────┘      └──────────┘
                   │           ▲                    ▲
┌─────────────┐    │           │                    │
│   Reader 3  │────┘           │                    │
└─────────────┘                │                    │
                               │                    │
┌─────────────┐                │                    │
│   Writer    │────────────────┴────────────────────┘
└─────────────┘
        │
        └──→ ┌─────────────┐
             │ Database    │
             │   (.db)     │
             └─────────────┘
```

- Multiple readers can access simultaneously
- One writer can proceed while readers are active
- Writers append to the WAL file
- Readers combine data from main database + WAL
- Checkpointing merges WAL back to main database

## Performance Considerations

### When to Use WAL Mode

**Best for:**
- Applications with many concurrent readers
- Write-heavy workloads with frequent small transactions
- Applications requiring better write performance
- Systems where readers should not be blocked by writers

**Not ideal for:**
- Databases on network filesystems (NFS, SMB)
- Applications requiring strict ACID guarantees at all times
- Very large databases (WAL file can grow large)
- Systems with limited disk space

### Tuning Parameters

```go
// Auto-checkpoint threshold (default: 1000 frames)
db.Exec("PRAGMA wal_autocheckpoint = 1000")

// Synchronous mode (affects durability vs performance)
db.Exec("PRAGMA synchronous = NORMAL") // Recommended for WAL mode

// Page size (set before enabling WAL)
db.Exec("PRAGMA page_size = 4096")
```

## Limitations and Caveats

1. **Network Filesystems**: WAL mode requires support for shared memory locking, which may not work reliably on network filesystems

2. **Database Size**: All readers must be able to fit the WAL index in memory

3. **Checkpoint Delays**: Very busy databases may delay checkpointing, causing the WAL file to grow large

4. **File Count**: WAL mode creates additional files (`.db-wal` and `.db-shm`)

5. **Read-Only Databases**: Cannot enable WAL mode on read-only databases

## Troubleshooting

### WAL File Growing Too Large

```go
// Force a checkpoint
db.Exec("PRAGMA wal_checkpoint(RESTART)")

// Reduce auto-checkpoint threshold
db.Exec("PRAGMA wal_autocheckpoint = 500")
```

### "Database is Locked" Errors

WAL mode should reduce these, but if they still occur:

```go
// Increase busy timeout
db.Exec("PRAGMA busy_timeout = 5000") // 5 seconds

// Consider implementing retry logic
for retries := 0; retries < 3; retries++ {
    if err := db.Exec(query); err == nil {
        break
    }
    time.Sleep(100 * time.Millisecond)
}
```

### Verifying WAL Mode is Active

```go
// Check for WAL files
if _, err := os.Stat("database.db-wal"); err == nil {
    fmt.Println("WAL file exists")
}

// Query journal mode
var mode string
db.QueryRow("PRAGMA journal_mode").Scan(&mode)
fmt.Printf("Journal mode: %s\n", mode)
```

## Complete Example

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony"
)

func main() {
    // Open database
    db, err := sql.Open("sqlite_internal", "myapp.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Enable WAL mode
    var mode string
    err = db.QueryRow("PRAGMA journal_mode = WAL").Scan(&mode)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Enabled WAL mode: %s\n", mode)

    // Set synchronous mode for better performance
    _, err = db.Exec("PRAGMA synchronous = NORMAL")
    if err != nil {
        log.Fatal(err)
    }

    // Create table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }

    for i := 1; i <= 100; i++ {
        _, err = tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)",
            fmt.Sprintf("User%d", i),
            fmt.Sprintf("user%d@example.com", i))
        if err != nil {
            tx.Rollback()
            log.Fatal(err)
        }
    }

    if err = tx.Commit(); err != nil {
        log.Fatal(err)
    }

    fmt.Println("Inserted 100 users")

    // Query data (can happen concurrently with writes)
    rows, err := db.Query("SELECT name, email FROM users WHERE id <= 5")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    fmt.Println("First 5 users:")
    for rows.Next() {
        var name, email string
        if err := rows.Scan(&name, &email); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("  %s <%s>\n", name, email)
    }

    // Checkpoint the WAL
    _, err = db.Exec("PRAGMA wal_checkpoint(PASSIVE)")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Checkpointed WAL file")
}
```

## References

- [SQLite Write-Ahead Logging](https://www.sqlite.org/wal.html)
- [SQLite WAL File Format](https://www.sqlite.org/walformat.html)
- [PRAGMA journal_mode](https://www.sqlite.org/pragma.html#pragma_journal_mode)
- [PRAGMA wal_checkpoint](https://www.sqlite.org/pragma.html#pragma_wal_checkpoint)

## See Also

- [PRAGMAS.md](PRAGMAS.md) - Complete PRAGMA documentation
- [FILE_FORMAT.md](FILE_FORMAT.md) - Database file format documentation
- [API_REFERENCE.md](API_REFERENCE.md) - Full API documentation
