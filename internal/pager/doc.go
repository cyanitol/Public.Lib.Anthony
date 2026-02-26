/*
Package pager implements a pure Go SQLite database pager for file I/O and page management.

This package is a pure Go implementation based on SQLite source code.
SQLite is in the public domain: https://sqlite.org/copyright.html

The pager is responsible for reading and writing database pages from/to disk,
managing an in-memory page cache, and providing atomic commit/rollback through
journaling.

# Overview

The pager sits between the B-tree layer and the operating system's file I/O layer,
providing page-based I/O, caching, atomic commits, and concurrency control.

This implementation is based on SQLite's pager.c and pager.h reference code,
adapted to use Go idioms and patterns.

# Database File Format

SQLite databases begin with a 100-byte header containing metadata:
  - Magic string: "SQLite format 3\0"
  - Page size (512 to 65536 bytes, power of 2)
  - File format versions
  - Database size in pages
  - Schema information
  - Text encoding
  - User-defined metadata

All database access is done in fixed-size pages. The first page contains the
database header followed by the root page of the schema table.

# Page Management

Pages are the fundamental unit of database I/O:
  - Each page has a unique page number (1-based)
  - Pages can be clean (unchanged) or dirty (modified)
  - Reference counting prevents premature eviction from cache
  - Dirty pages are tracked for efficient commits

The page cache maintains frequently accessed pages in memory:
  - Hash map for O(1) page lookup
  - Dirty page list for commit operations
  - LRU eviction for clean, unreferenced pages
  - Thread-safe with mutex protection

# Transaction Management

Write transactions use a rollback journal for atomicity:

 1. Begin: Acquire locks, open journal file
 2. Journal: Record original page content before modification
 3. Modify: Update pages in cache
 4. Commit: Write dirty pages, sync file, delete journal
 5. Rollback: Restore pages from journal, delete journal

This ensures atomic, durable commits even in the event of crashes or
power failures.

# Pager States

The pager implements a state machine:

	OPEN -> READER -> WRITER_LOCKED -> WRITER_CACHEMOD ->
	WRITER_DBMOD -> WRITER_FINISHED -> OPEN

Error conditions transition to the ERROR state, requiring rollback.

# Usage

Basic usage pattern:

	// Open database
	p, err := pager.Open("mydb.db", false)
	if err != nil {
	    return err
	}
	defer p.Close()

	// Get page
	page, err := p.Get(1)
	if err != nil {
	    return err
	}
	defer p.Put(page)

	// Modify page
	if err := p.Write(page); err != nil {
	    return err
	}

	data := []byte("Hello, World!")
	if err := page.Write(100, data); err != nil {
	    return err
	}

	// Commit changes
	if err := p.Commit(); err != nil {
	    return err
	}

See the example tests for more usage patterns.

# Thread Safety

All public operations are thread-safe:
  - Pager uses RWMutex for state protection
  - Pages use RWMutex for data access
  - Reference counts use atomic operations
  - Cache operations are mutex-protected

# Limitations

This is a simplified implementation compared to full SQLite:
  - No WAL (Write-Ahead Logging) support
  - Simplified file locking (OS-specific locking not implemented)
  - No memory-mapped I/O
  - No hot journal recovery
  - No savepoints (nested transactions)

These features may be added in future versions.

# Performance

Performance considerations:
  - Larger page sizes reduce I/O overhead but use more memory
  - Cache size affects hit rate (default: 2000 pages)
  - File sync operations are expensive but required for durability
  - Page reference counting allows safe concurrent access

# References

  - SQLite File Format: https://www.sqlite.org/fileformat.html
  - SQLite Architecture: https://www.sqlite.org/arch.html
  - SQLite Source Code: src/pager.c, src/pager.h

# Implementation Notes

This implementation closely follows the SQLite C reference code while
using Go idioms:
  - Errors instead of return codes
  - Interfaces for extensibility
  - Goroutine-safe by default
  - Explicit resource management with defer

The core algorithms and state machine are preserved from the original
SQLite implementation to ensure correctness and compatibility.
*/
package pager
