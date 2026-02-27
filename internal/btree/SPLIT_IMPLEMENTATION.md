# B-Tree Page Split Implementation

This document describes the B-tree page split algorithm implemented in `split.go`.

## Overview

The split implementation handles the case when a B-tree page becomes full and needs to be divided into two pages. This is a critical operation for maintaining B-tree balance and allowing unlimited growth.

## Files

- `split.go` - Main implementation of split algorithms
- `split_test.go` - Comprehensive test suite for split operations

## Algorithm Details

### Leaf Page Split (`splitLeafPage`)

When a leaf page is full and cannot accommodate a new cell, it is split into two pages:

1. **Collect Cells**: Gather all existing cells plus the new cell to be inserted, maintaining sorted order
2. **Find Median**: Calculate the median index (middle cell) to divide the cells
3. **Allocate Sibling**: Create a new page to hold half the cells
4. **Distribute Cells**:
   - Left page (original): cells [0, median)
   - Right page (new): cells [median, end)
5. **Update Parent**: Insert a divider key in the parent pointing to both pages
6. **Handle Root Split**: If splitting the root, create a new interior root page

### Interior Page Split (`splitInteriorPage`)

Interior pages are split when they become full during parent updates after leaf splits:

1. **Collect Cells**: Gather all interior cells plus the new divider key
2. **Find Median**: Calculate median, which becomes the new divider for the parent
3. **Allocate Sibling**: Create new interior page
4. **Distribute Cells**:
   - Left page: cells [0, median)
   - Median key: promoted to parent
   - Right page: cells [median+1, end)
5. **Update Children**: Fix child page pointers including rightmost pointers
6. **Recursive Update**: May trigger parent split if parent is also full

### Helper Functions

#### `collectLeafCellsForSplit`
Collects all cells from a leaf page plus the new cell, returning them in sorted order by key. This ensures the split maintains B-tree ordering invariants.

#### `collectInteriorCellsForSplit`
Similar to leaf collection but handles interior cells with child page pointers.

#### `updateParentAfterSplit`
Updates the parent page after a split:
- If current page is root: calls `createNewRoot`
- Otherwise: inserts divider into parent (may recursively split parent)

#### `createNewRoot`
Creates a new root page when the old root splits:
- Allocates new interior page
- Inserts single divider key pointing to old root and new sibling
- Updates cursor's root page reference

#### `initializeLeafPage` / `initializeInteriorPage`
Initialize page headers for newly allocated pages.

#### `clearPageCells`
Removes all cells from a page, resetting it to empty state before rebuilding.

#### `getHeaderOffset`
Returns the correct header offset (100 for page 1, 0 for others).

## Key Design Decisions

### Median Selection
Uses simple `len(cells) / 2` to find median. This provides balanced splits in most cases. SQLite uses more sophisticated algorithms considering cell sizes, but this simpler approach works well for equal-sized cells.

### Cell Distribution
Cells are distributed evenly between left and right pages. The median cell's key becomes the divider:
- Leaf splits: First key of right page is the divider
- Interior splits: Median key is promoted, not stored in either child

### Parent Updates
The implementation handles cascading splits - if inserting a divider into the parent causes the parent to overflow, the parent is split recursively.

### Root Splits
When the root splits, a new root is created with:
- Single divider key
- Left child = old root
- Right child = new sibling

This increases the tree height by 1.

## Testing Strategy

### Test Coverage

1. **Basic Split** (`TestSplitLeafPageBasic`)
   - Insert enough cells to force a split
   - Verify data integrity after split

2. **Ordering** (`TestSplitLeafPageOrder`)
   - Insert in random order
   - Verify all cells remain sorted after split

3. **Root Split** (`TestSplitCreatesNewRoot`)
   - Verify root becomes interior page after split
   - Check new root structure

4. **Cell Collection** (`TestCollectLeafCellsForSplit`)
   - Test helper function in isolation
   - Verify correct insertion point for new cell

5. **Duplicate Keys** (`TestSplitWithDuplicateKey`)
   - Verify duplicate key rejection

6. **Edge Cases**:
   - Empty payloads (`TestSplitEmptyPayload`)
   - Large payloads (`TestSplitLargePayloads`)
   - Multiple split levels (`TestSplitMultipleLevels`)

7. **Helper Functions**:
   - Page initialization
   - Header offset calculation
   - Cell clearing

### Test Methodology

Tests use different page sizes to control when splits occur:
- Small pages (512-1024 bytes): Force splits with fewer inserts
- Normal pages (4096 bytes): Realistic testing
- Varied payloads: Test split behavior with different cell sizes

## Integration with Existing Code

### Cursor Integration
The `BtCursor.Insert` method calls `splitPage` when a page is full:
```go
if len(cellData) > btreePage.FreeSpace() {
    return c.splitPage(key, payload)
}
```

### Page Management
Splits use existing B-tree infrastructure:
- `Btree.AllocatePage()` - Allocate new pages
- `BtreePage.InsertCell()` - Insert cells
- `BtreePage.Defragment()` - Compact pages after split
- `PageProvider.MarkDirty()` - Track modified pages for transactions

### Compatibility
The implementation maintains SQLite compatibility:
- Cell formats match SQLite exactly
- Page layouts follow SQLite specification
- Split behavior mirrors SQLite's algorithm (simplified)

## Limitations and Future Work

### Current Limitations

1. **Index B-Trees**: Only table B-trees (integer keys) supported
2. **Overflow Pages**: Large payloads that require overflow pages not fully tested
3. **Parent Pointer Map**: No efficient parent lookup (uses cursor stack)
4. **Rebalancing**: No merging of underfull pages after deletion
5. **Cell Size Optimization**: Doesn't consider cell sizes when choosing split point

### Future Enhancements

1. **Index Support**: Extend to index B-trees with blob keys
2. **Better Split Points**: Use SQLite's algorithm that considers cell sizes
3. **Page Merging**: Implement page merge for underfull pages
4. **Overflow Handling**: Full support for overflow page chains
5. **Performance**: Add benchmarks for split operations
6. **Verification**: Add B-tree invariant checking (all leaves at same depth, etc.)

## References

### SQLite Source Code
- `btree.c:balance_nonroot()` - Main split logic
- `btree.c:balance()` - Top-level balancing
- `btree.c:insertCell()` - Cell insertion
- `btree.c:allocateBtreePage()` - Page allocation

### Algorithm
Based on classic B-tree splitting algorithm:
1. Bayer & McCreight (1972) - Original B-tree paper
2. Knuth, TAOCP Vol 3 - Sorting and Searching
3. SQLite File Format Documentation

## Usage Example

```go
// Create a B-tree
bt := NewBtree(4096)
rootPage, _ := bt.CreateTable()
cursor := NewCursor(bt, rootPage)

// Insert many rows - splits happen automatically
for i := 1; i <= 1000; i++ {
    key := int64(i)
    payload := []byte{...}

    // Split occurs transparently when page is full
    err := cursor.Insert(key, payload)
    if err != nil {
        // Handle error
    }
}

// Traverse the split tree
cursor.MoveToFirst()
for cursor.IsValid() {
    key := cursor.GetKey()
    data := cursor.GetPayload()
    // Process data...
    cursor.Next()
}
```

## Error Handling

Split operations can fail for several reasons:

1. **Allocation Failure**: Can't allocate new page
2. **I/O Errors**: Can't read/write pages (if using pager)
3. **Corruption**: Invalid page structures
4. **Memory**: Out of memory for large operations

All errors are propagated up with context using `fmt.Errorf(...: %w, err)` pattern.

## Performance Characteristics

### Time Complexity
- Single split: O(n) where n = cells per page (typically < 100)
- Cascading splits: O(d * n) where d = tree depth (typically < 10)
- Amortized: O(1) per insert over many inserts

### Space Complexity
- Temporary storage: O(n) for cell collection
- New pages: Allocated as needed
- No significant memory overhead

### Optimizations
- Cell data copied directly (no parsing/re-encoding)
- Single defragmentation pass per page
- Minimal heap allocations

## Maintenance Notes

### Code Organization
Functions are ordered by abstraction level:
1. Public API (`splitLeafPage`, `splitInteriorPage`)
2. Collection helpers
3. Parent update logic
4. Initialization helpers
5. Utility functions

### Testing
Run split tests with:
```bash
go test -v ./internal/btree -run Split
```

### Debugging
Enable verbose logging to see split operations:
```go
t.Logf("Splitting page %d at key %d", pageNum, dividerKey)
```

## License

This implementation is based on SQLite source code, which is in the public domain.
The Go implementation follows the same public domain dedication.
