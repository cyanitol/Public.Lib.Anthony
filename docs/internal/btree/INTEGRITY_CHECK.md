# B-tree Integrity Checking

This document describes the B-tree integrity checking implementation in `integrity.go`.

## Overview

The integrity checker performs comprehensive validation of B-tree structures, similar to SQLite's `PRAGMA integrity_check` command. It's designed to detect corruption, structural errors, and logical inconsistencies in the B-tree.

## API

### Main Functions

#### `CheckIntegrity(bt *Btree, rootPage uint32) *IntegrityResult`

Performs a full recursive integrity check on a B-tree starting from the root page.

**Parameters:**
- `bt`: The B-tree to check
- `rootPage`: The root page number of the tree to check

**Returns:**
- `IntegrityResult`: Contains all errors found, page count, and row count

**Example:**
```go
bt := btree.NewBtree(4096)
rootPage, _ := bt.CreateTable()

result := btree.CheckIntegrity(bt, rootPage)
if result.OK() {
    fmt.Println("Tree is valid!")
} else {
    for _, err := range result.Errors {
        fmt.Printf("Error: %v\n", err)
    }
}
```

#### `CheckPageIntegrity(bt *Btree, pageNum uint32) *IntegrityResult`

Checks the integrity of a single page (non-recursive). Useful for debugging specific pages.

**Example:**
```go
result := btree.CheckPageIntegrity(bt, 1)
if !result.OK() {
    fmt.Printf("Page 1 has %d errors\n", len(result.Errors))
}
```

#### `ValidateFreeBlockList(bt *Btree, pageNum uint32) *IntegrityResult`

Validates the free block list on a page, checking for cycles and invalid sizes.

## Types

### `IntegrityResult`

Contains the results of an integrity check:

```go
type IntegrityResult struct {
    Errors    []*IntegrityError // All errors found
    PageCount uint32            // Number of pages checked
    RowCount  int64             // Total rows found
}
```

**Methods:**
- `OK() bool`: Returns true if no errors were found

### `IntegrityError`

Describes a specific integrity error:

```go
type IntegrityError struct {
    PageNum     uint32 // Page number where error occurred (0 if tree-wide)
    ErrorType   string // Type of error
    Description string // Detailed description
}
```

## Error Types

The checker can detect the following error types:

### Tree-level Errors
- `null_btree`: The B-tree pointer is nil
- `invalid_root`: Root page number is 0 or invalid
- `orphan_page`: Page exists but is not referenced in the tree

### Page-level Errors
- `page_not_found`: Page cannot be retrieved
- `invalid_header`: Page header is malformed
- `invalid_page_size`: Page size doesn't match expected size
- `content_overlap`: Cell content overlaps with header/cell pointers
- `content_out_of_bounds`: Cell content area exceeds page bounds
- `too_many_cells`: Cell count exceeds reasonable maximum

### Cell-level Errors
- `unsorted_cell_pointers`: Cell pointers are not in descending order
- `cell_out_of_bounds`: Cell offset exceeds page size
- `invalid_cell`: Cell cannot be parsed
- `overlapping_cells`: Two or more cells overlap in memory

### Key-level Errors
- `keys_not_sorted`: Keys are not in ascending order
- `key_out_of_range`: Key violates bounds set by parent page

### Interior Page Errors
- `invalid_child_pointer`: Child page pointer is 0
- `self_reference`: Page points to itself as a child
- `cycle_detected`: Circular reference in tree structure
- `invalid_right_child`: Right-most child pointer is invalid
- `depth_exceeded`: Tree depth exceeds maximum allowed

### Free Block Errors
- `freeblock_cycle`: Free block list contains a cycle
- `freeblock_out_of_bounds`: Free block offset exceeds page bounds
- `freeblock_exceeds_page`: Free block size exceeds page size
- `invalid_freeblock_size`: Free block size is less than minimum (4 bytes)
- `freeblock_list_too_long`: Free block list exceeds iteration limit

## Validation Details

### 1. Page Format Validation

Checks that:
- Page size matches the B-tree's configured page size
- Cell content area doesn't overlap with page header or cell pointer array
- Cell content start is within page bounds
- Number of cells is reasonable for the page size
- For interior pages, right child pointer is valid and doesn't point to itself

### 2. Cell Pointer Array Validation

Verifies that:
- Cell pointers are stored in descending order (cells grow backwards from end)
- All cell pointers are within valid page bounds
- Cell pointer array doesn't overflow into cell content area

### 3. Overlapping Cells Detection

Ensures that:
- No two cells occupy the same memory region
- All cells fit completely within the page
- Cells don't overlap with page header or cell pointer array

### 4. Key Ordering Verification

Validates that:
- Keys within a page are in strictly ascending order
- No duplicate keys exist
- Keys respect minimum and maximum bounds set by parent interior pages
- For interior pages, child pages contain keys within the correct range

### 5. Interior Page Validation

Checks that:
- All child page pointers are non-zero
- No page points to itself as a child
- No circular references exist in the tree
- Tree depth doesn't exceed `MaxBtreeDepth` (20 levels)
- All child pages exist and are accessible

### 6. Free Block List Validation

Verifies that:
- Free block chain has no cycles
- Each free block has a valid size (minimum 4 bytes)
- Free blocks are within page bounds
- Free block list doesn't exceed iteration limit

### 7. Orphan Page Detection

Identifies:
- Pages that exist in the B-tree but are not referenced from the root
- Pages that are allocated but not part of any tree structure

### 8. Row Count Verification

Counts and reports:
- Total number of leaf cells (rows) in the tree
- Number of pages visited during traversal

## Test Coverage

The implementation includes comprehensive tests in `integrity_test.go`:

- **Valid tree tests**: Verify that valid trees pass all checks
- **Error detection tests**: Ensure all error types are correctly detected
- **Edge case tests**: Test boundary conditions and unusual structures
- **Multi-level tree tests**: Verify correct handling of interior pages
- **Corruption tests**: Test detection of various forms of corruption

### Test Categories

1. **Basic validation**: nil B-tree, invalid root, missing pages
2. **Header validation**: invalid page types, corrupted headers
3. **Cell pointer validation**: unsorted pointers, out-of-bounds pointers
4. **Key ordering**: unsorted keys, duplicate keys, out-of-range keys
5. **Interior page validation**: invalid children, self-references, cycles
6. **Orphan detection**: unreferenced pages
7. **Free block validation**: cycles, invalid sizes, out-of-bounds
8. **Multi-error detection**: multiple errors in a single check

## Performance Considerations

- The checker visits each page exactly once (O(n) where n = number of pages)
- Cycle detection uses a visited map to prevent infinite loops
- Memory usage is proportional to the number of pages in the tree
- The checker is designed to continue after finding errors, so it may take longer on corrupt databases

## Usage in SQLite Compatibility

This implementation supports the SQLite `PRAGMA integrity_check` command:

```sql
PRAGMA integrity_check;
```

The checker returns a list of error messages, or "ok" if no errors are found.

## Future Enhancements

Potential improvements:
- Overflow page chain validation
- Freelist validation (database-level)
- Index key uniqueness checking
- Referential integrity between tables and indexes
- Performance metrics (page utilization, fragmentation)
- Repair suggestions for detected errors
