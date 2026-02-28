# VACUUM Implementation for Anthony SQLite Clone

## Overview

This document describes the implementation of the VACUUM command for the Anthony SQLite clone. VACUUM is a maintenance operation that rebuilds the database file from scratch, removing unused pages, defragmenting the database, and reclaiming disk space.

## Features Implemented

### 1. VACUUM Statement Parsing
- **Location**: `internal/parser/ast.go`, `internal/parser/parser.go`
- **Syntax Support**:
  - `VACUUM` - Basic vacuum of the main database
  - `VACUUM schema_name` - Vacuum a specific attached database
  - `VACUUM INTO 'filename'` - Vacuum into a new file (backup + compact)
  - `VACUUM schema_name INTO 'filename'` - Combine both options

### 2. Database Compaction
- **Location**: `internal/pager/vacuum.go`
- **Features**:
  - Removes all free pages from the free list
  - Rebuilds the database file sequentially
  - Consolidates fragmented free space
  - Maintains data integrity throughout the process
  - Updates database header to reflect new state

### 3. VACUUM INTO Support
- **Purpose**: Creates a compacted copy of the database in a new file
- **Use Cases**:
  - Database backup
  - Database migration
  - Creating a clean copy without affecting the original
- **Behavior**:
  - Source database remains unchanged
  - Target file is created with compacted data
  - All live data is copied to the new file

### 4. Page Reorganization
- **Location**: `internal/pager/vacuum.go`
- **Process**:
  1. Build a map of all free pages from the free list
  2. Copy all live (non-free) pages to a new temporary database
  3. Pages are copied in sequential order (compaction)
  4. Database header is updated to reflect zero free pages
  5. Original database file is replaced with the new one

## Implementation Details

### Parser Integration

The VACUUM statement is added to the parser with the following AST node:

```go
type VacuumStmt struct {
    Schema string // Database schema name (optional)
    Into   string // Target filename for VACUUM INTO (optional)
}
```

The parser recognizes VACUUM as a top-level statement and handles the optional INTO clause.

### Pager Integration

The core vacuum logic is implemented in `internal/pager/vacuum.go`:

#### Key Functions:

1. **Vacuum(opts *VacuumOptions)** - Main entry point
   - Validates preconditions (not read-only, not in transaction)
   - Creates a temporary file for the vacuumed database
   - Calls vacuumToFile to perform the operation
   - Replaces the original file or copies to target (for VACUUM INTO)

2. **vacuumToFile(targetFilename)** - Core vacuum logic
   - Opens a new pager for the target file
   - Copies the database header
   - Copies all live pages in order
   - Updates header to reflect zero free pages

3. **copyLivePages(target)** - Page-by-page copying
   - Identifies free pages using collectFreePages
   - Copies only non-free pages to the target
   - Maintains data integrity during copy

4. **collectFreePages(freePages)** - Free list traversal
   - Walks the free list trunk pages
   - Collects all free page numbers into a map
   - Used to skip free pages during copy

### Driver Integration

The driver executes VACUUM by calling the pager's Vacuum method directly:

```go
func (s *Stmt) compileVacuum(vm *vdbe.VDBE, stmt *parser.VacuumStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
    opts := &pager.VacuumOptions{
        Schema:   stmt.Schema,
        IntoFile: stmt.Into,
    }

    if err := s.conn.pager.Vacuum(opts); err != nil {
        return nil, fmt.Errorf("VACUUM failed: %w", err)
    }

    // Schema reload for consistency
    s.conn.schema.Reload()

    // Return empty VM
    return vm, nil
}
```

VACUUM is special in that it doesn't use VDBE bytecode - it executes directly at the pager level.

## Safety Features

### 1. Transaction Safety
- VACUUM cannot run during an active transaction
- Returns `ErrTransactionOpen` if attempted during a transaction
- Ensures atomicity of the database state

### 2. Read-Only Protection
- VACUUM cannot run on read-only databases
- Returns `ErrReadOnly` if attempted on read-only database

### 3. Data Integrity
- Uses a temporary file during the vacuum process
- Original database is only replaced after successful vacuum
- If vacuum fails, original database is unchanged
- All page data is verified during copy

### 4. Page Cache Management
- Page cache is cleared after vacuum
- Ensures no stale cached pages
- Database header is reloaded
- Database size is recalculated

## Testing

### Parser Tests (`internal/parser/vacuum_test.go`)
- Basic VACUUM parsing
- VACUUM with schema name
- VACUUM INTO parsing
- Case sensitivity
- Multi-statement parsing
- AST structure verification

### Pager Tests (`internal/pager/vacuum_test.go`)
- Basic vacuum operation
- VACUUM after many deletes
- VACUUM INTO functionality
- Read-only database protection
- Transaction protection
- Data integrity verification
- Empty database handling

### Integration Tests (`internal/driver/vacuum_test.go`)
- End-to-end VACUUM through SQL interface
- VACUUM with table data
- VACUUM after DELETE operations
- VACUUM INTO with verification
- Multiple sequential VACUUMs
- VACUUM with indexes (if supported)

## Usage Examples

### Basic VACUUM
```sql
-- Compact the main database
VACUUM;
```

### VACUUM INTO (Backup + Compact)
```sql
-- Create a compacted backup
VACUUM INTO 'backup.db';
```

### VACUUM with Schema
```sql
-- Vacuum an attached database
VACUUM mydb;
```

### Combined
```sql
-- Vacuum attached database into a file
VACUUM mydb INTO 'mydb_backup.db';
```

## Performance Considerations

1. **Space Requirements**: VACUUM requires enough disk space to temporarily hold two copies of the database
2. **Time Complexity**: O(n) where n is the number of live pages
3. **Lock Duration**: VACUUM holds an exclusive lock for the entire operation
4. **I/O Intensity**: VACUUM is I/O intensive as it reads and writes all pages

## Limitations

1. **No concurrent access**: Database is locked during VACUUM
2. **Temporary disk space**: Requires additional disk space during operation
3. **Schema reload**: May require schema reload after VACUUM (implemented)
4. **No partial vacuum**: Cannot vacuum individual tables (SQLite behavior)

## Future Enhancements

Potential improvements for future versions:

1. **Incremental VACUUM**: Support for incremental vacuum mode
2. **AUTO VACUUM**: Automatic vacuum on DELETE/UPDATE
3. **Progress callbacks**: Report vacuum progress to caller
4. **Parallel page copying**: Multi-threaded page copying for large databases
5. **Vacuum analysis**: Report space savings before performing vacuum

## Files Modified/Created

### Created:
- `internal/pager/vacuum.go` - Core vacuum implementation
- `internal/pager/vacuum_test.go` - Pager-level tests
- `internal/driver/vacuum_test.go` - Integration tests
- `internal/parser/vacuum_test.go` - Parser tests
- `VACUUM_IMPLEMENTATION.md` - This documentation

### Modified:
- `internal/parser/ast.go` - Added VacuumStmt AST node
- `internal/parser/parser.go` - Added VACUUM parsing logic
- `internal/driver/stmt.go` - Added VACUUM execution
- Added pager import to driver/stmt.go

## Compatibility

This implementation follows SQLite's VACUUM behavior:
- Same syntax and semantics
- Same error conditions
- Same transaction safety guarantees
- Compatible VACUUM INTO functionality

## Conclusion

The VACUUM implementation provides a complete and safe database maintenance operation for the Anthony SQLite clone. It successfully removes free pages, defragments the database, and supports the VACUUM INTO variant for creating compacted backups.

All major SQLite VACUUM features are supported, with comprehensive testing to ensure data integrity and proper error handling.
