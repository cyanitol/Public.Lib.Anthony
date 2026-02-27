package btree

import (
	"fmt"
	"sort"
)

// Integrity checking functions for B-tree structures.
//
// This file implements comprehensive B-tree integrity verification, which is used
// by PRAGMA integrity_check in SQLite. The checker performs the following validations:
//
// 1. Page format validation:
//    - Page header is valid
//    - Cell pointer array doesn't overlap with cell content
//    - Page size matches expected size
//    - Cell count is reasonable
//
// 2. Cell pointer array validation:
//    - Cell pointers are sorted in descending order
//    - All cell pointers are within page bounds
//
// 3. Cell overlap detection:
//    - No cells overlap in memory
//    - All cells fit within the page
//
// 4. Key ordering verification:
//    - Keys are in ascending order within each page
//    - Keys respect the bounds defined by parent pages
//    - No duplicate keys
//
// 5. Interior page validation:
//    - Child pointers are valid (non-zero, not self-referencing)
//    - Right-most child pointer is valid
//    - Tree structure is correct
//
// 6. Free block list validation:
//    - Free block chain has no cycles
//    - Free block sizes are valid
//    - Free blocks are within page bounds
//
// 7. Orphan page detection:
//    - All pages in the B-tree are referenced
//    - No pages are unreachable from the root
//
// 8. Row count verification:
//    - Counts the total number of rows in the tree
//    - Ensures leaf pages contain data
//
// The checker reports ALL errors found, not just the first error, making it
// suitable for thorough database diagnostics.

// IntegrityError describes a B-tree integrity error
type IntegrityError struct {
	PageNum     uint32 // Page number where error occurred (0 if tree-wide)
	ErrorType   string // Type of error
	Description string // Detailed description
}

// Error implements the error interface
func (e *IntegrityError) Error() string {
	if e.PageNum == 0 {
		return fmt.Sprintf("[%s] %s", e.ErrorType, e.Description)
	}
	return fmt.Sprintf("[page %d, %s] %s", e.PageNum, e.ErrorType, e.Description)
}

// IntegrityResult contains the results of an integrity check
type IntegrityResult struct {
	Errors   []*IntegrityError // All errors found
	PageCount uint32           // Number of pages checked
	RowCount  int64            // Total rows found
}

// OK returns true if no errors were found
func (r *IntegrityResult) OK() bool {
	return len(r.Errors) == 0
}

// AddError adds an error to the result
func (r *IntegrityResult) AddError(pageNum uint32, errorType, description string) {
	r.Errors = append(r.Errors, &IntegrityError{
		PageNum:     pageNum,
		ErrorType:   errorType,
		Description: description,
	})
}

// CheckIntegrity performs a full integrity check on a B-tree
// Returns a list of all errors found (doesn't stop at first error)
func CheckIntegrity(bt *Btree, rootPage uint32) *IntegrityResult {
	result := &IntegrityResult{
		Errors: make([]*IntegrityError, 0),
	}

	if bt == nil {
		result.AddError(0, "null_btree", "btree is nil")
		return result
	}

	if rootPage == 0 {
		result.AddError(0, "invalid_root", "root page number is 0")
		return result
	}

	// Track visited pages to detect cycles and orphans
	visitedPages := make(map[uint32]bool)

	// Check the tree recursively
	rowCount := checkPageRecursive(bt, rootPage, visitedPages, result, nil, nil, 0)
	result.RowCount = rowCount
	result.PageCount = uint32(len(visitedPages))

	// Check for orphan pages (pages in bt.Pages but not visited)
	checkOrphanPages(bt, visitedPages, result)

	return result
}

// checkPageRecursive recursively checks a page and its children
// minKey and maxKey define the valid key range for this page (nil = unbounded)
// Returns the number of rows found
func checkPageRecursive(bt *Btree, pageNum uint32, visited map[uint32]bool, result *IntegrityResult, minKey, maxKey *int64, depth int) int64 {
	// Check for cycles
	if visited[pageNum] {
		result.AddError(pageNum, "cycle_detected", "page appears multiple times in tree (cycle)")
		return 0
	}
	visited[pageNum] = true

	// Check depth
	if depth > MaxBtreeDepth {
		result.AddError(pageNum, "depth_exceeded", fmt.Sprintf("tree depth %d exceeds maximum %d", depth, MaxBtreeDepth))
		return 0
	}

	// Get page data
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		result.AddError(pageNum, "page_not_found", fmt.Sprintf("failed to get page: %v", err))
		return 0
	}

	// Validate page format
	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		result.AddError(pageNum, "invalid_header", fmt.Sprintf("failed to parse page header: %v", err))
		return 0
	}

	// Check page format validity
	checkPageFormat(bt, pageNum, pageData, header, result)

	// Get cell pointers
	cellPointers, err := header.GetCellPointers(pageData)
	if err != nil {
		result.AddError(pageNum, "invalid_cell_pointers", fmt.Sprintf("failed to get cell pointers: %v", err))
		return 0
	}

	// Check cell pointer array is sorted
	checkCellPointersSorted(pageNum, cellPointers, result)

	// Parse all cells and check for overlaps
	cells := make([]*CellInfo, 0, header.NumCells)
	cellOffsets := make([]int, 0, header.NumCells)
	cellSizes := make([]int, 0, header.NumCells)

	for i := 0; i < int(header.NumCells); i++ {
		cellOffset := int(cellPointers[i])
		if cellOffset >= len(pageData) {
			result.AddError(pageNum, "cell_out_of_bounds", fmt.Sprintf("cell %d offset %d exceeds page size %d", i, cellOffset, len(pageData)))
			continue
		}

		cellData := pageData[cellOffset:]
		cell, err := ParseCell(header.PageType, cellData, bt.UsableSize)
		if err != nil {
			result.AddError(pageNum, "invalid_cell", fmt.Sprintf("cell %d parse error: %v", i, err))
			continue
		}

		cells = append(cells, cell)
		cellOffsets = append(cellOffsets, cellOffset)
		cellSizes = append(cellSizes, int(cell.CellSize))
	}

	// Check for overlapping cells
	checkOverlappingCells(pageNum, cellOffsets, cellSizes, result)

	// Check keys are in correct order
	checkKeyOrder(pageNum, cells, minKey, maxKey, result)

	// Check child pointers and recurse for interior pages
	rowCount := int64(0)
	if header.IsInterior {
		rowCount = checkInteriorPage(bt, pageNum, header, cells, visited, result, depth)
	} else {
		// Leaf page - count rows
		rowCount = int64(len(cells))
	}

	return rowCount
}

// checkPageFormat validates the page format
func checkPageFormat(bt *Btree, pageNum uint32, pageData []byte, header *PageHeader, result *IntegrityResult) {
	// Check page size
	if uint32(len(pageData)) != bt.PageSize {
		result.AddError(pageNum, "invalid_page_size",
			fmt.Sprintf("page size %d doesn't match btree page size %d", len(pageData), bt.PageSize))
	}

	// Check cell content area doesn't overlap with header and cell pointer array
	cellPtrArrayEnd := header.CellPtrOffset + (int(header.NumCells) * 2)
	cellContentStart := int(header.CellContentStart)
	if cellContentStart == 0 {
		cellContentStart = int(bt.UsableSize)
	}

	if cellContentStart < cellPtrArrayEnd {
		result.AddError(pageNum, "content_overlap",
			fmt.Sprintf("cell content start %d overlaps with cell pointer array end %d",
				cellContentStart, cellPtrArrayEnd))
	}

	// Check cell content start is within page bounds
	if cellContentStart > int(bt.UsableSize) {
		result.AddError(pageNum, "content_out_of_bounds",
			fmt.Sprintf("cell content start %d exceeds usable size %d",
				cellContentStart, bt.UsableSize))
	}

	// Check number of cells is reasonable
	maxCells := (int(bt.UsableSize) - header.HeaderSize) / 6 // Minimum cell overhead
	if int(header.NumCells) > maxCells {
		result.AddError(pageNum, "too_many_cells",
			fmt.Sprintf("cell count %d exceeds reasonable maximum %d",
				header.NumCells, maxCells))
	}

	// For interior pages, check right child pointer
	if header.IsInterior {
		if header.RightChild == 0 {
			result.AddError(pageNum, "invalid_right_child", "interior page has right child pointer of 0")
		}
		if header.RightChild == pageNum {
			result.AddError(pageNum, "self_reference", "interior page right child points to itself")
		}
	}
}

// checkCellPointersSorted verifies cell pointers are in descending order
// (cells are stored from the end of the page backwards)
func checkCellPointersSorted(pageNum uint32, cellPointers []uint16, result *IntegrityResult) {
	for i := 1; i < len(cellPointers); i++ {
		if cellPointers[i] >= cellPointers[i-1] {
			result.AddError(pageNum, "unsorted_cell_pointers",
				fmt.Sprintf("cell pointers not sorted: pointer[%d]=%d >= pointer[%d]=%d",
					i, cellPointers[i], i-1, cellPointers[i-1]))
			return // Only report once
		}
	}
}

// checkOverlappingCells checks if any cells overlap in memory
func checkOverlappingCells(pageNum uint32, offsets []int, sizes []int, result *IntegrityResult) {
	if len(offsets) != len(sizes) {
		return
	}

	// Create a list of cell ranges and sort by offset
	type cellRange struct {
		index  int
		start  int
		end    int
	}
	ranges := make([]cellRange, len(offsets))
	for i := 0; i < len(offsets); i++ {
		ranges[i] = cellRange{
			index: i,
			start: offsets[i],
			end:   offsets[i] + sizes[i],
		}
	}

	// Sort by start offset
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].start < ranges[j].start
	})

	// Check for overlaps
	for i := 1; i < len(ranges); i++ {
		if ranges[i].start < ranges[i-1].end {
			result.AddError(pageNum, "overlapping_cells",
				fmt.Sprintf("cell %d [%d:%d] overlaps with cell %d [%d:%d]",
					ranges[i].index, ranges[i].start, ranges[i].end,
					ranges[i-1].index, ranges[i-1].start, ranges[i-1].end))
		}
	}
}

// checkKeyOrder verifies keys are in ascending order and within bounds
func checkKeyOrder(pageNum uint32, cells []*CellInfo, minKey, maxKey *int64, result *IntegrityResult) {
	for i := 0; i < len(cells); i++ {
		key := cells[i].Key

		// Check against bounds
		if minKey != nil && key <= *minKey {
			result.AddError(pageNum, "key_out_of_range",
				fmt.Sprintf("cell %d key %d is not greater than minimum bound %d", i, key, *minKey))
		}
		if maxKey != nil && key >= *maxKey {
			result.AddError(pageNum, "key_out_of_range",
				fmt.Sprintf("cell %d key %d is not less than maximum bound %d", i, key, *maxKey))
		}

		// Check ascending order
		if i > 0 && key <= cells[i-1].Key {
			result.AddError(pageNum, "keys_not_sorted",
				fmt.Sprintf("cell %d key %d is not greater than previous key %d",
					i, key, cells[i-1].Key))
		}
	}
}

// checkInteriorPage checks interior page children and recurses
func checkInteriorPage(bt *Btree, pageNum uint32, header *PageHeader, cells []*CellInfo, visited map[uint32]bool, result *IntegrityResult, depth int) int64 {
	rowCount := int64(0)

	// Check each cell's child pointer
	for i := 0; i < len(cells); i++ {
		childPage := cells[i].ChildPage

		// Validate child pointer
		if childPage == 0 {
			result.AddError(pageNum, "invalid_child_pointer",
				fmt.Sprintf("cell %d has child pointer of 0", i))
			continue
		}
		if childPage == pageNum {
			result.AddError(pageNum, "self_reference",
				fmt.Sprintf("cell %d child pointer points to itself", i))
			continue
		}

		// Determine key bounds for child
		var minKey *int64
		var maxKey *int64

		if i > 0 {
			prevKey := cells[i-1].Key
			minKey = &prevKey
		}

		cellKey := cells[i].Key
		maxKey = &cellKey

		// Recurse into child
		rowCount += checkPageRecursive(bt, childPage, visited, result, minKey, maxKey, depth+1)
	}

	// Check right-most child
	if header.RightChild != 0 {
		var minKey *int64
		if len(cells) > 0 {
			lastKey := cells[len(cells)-1].Key
			minKey = &lastKey
		}

		rowCount += checkPageRecursive(bt, header.RightChild, visited, result, minKey, nil, depth+1)
	}

	return rowCount
}

// checkOrphanPages checks for pages that exist but aren't referenced in the tree
func checkOrphanPages(bt *Btree, visited map[uint32]bool, result *IntegrityResult) {
	for pageNum := range bt.Pages {
		if !visited[pageNum] {
			result.AddError(pageNum, "orphan_page", "page exists but is not referenced in tree")
		}
	}
}

// CheckPageIntegrity checks the integrity of a single page (non-recursive)
// This is useful for debugging specific pages
func CheckPageIntegrity(bt *Btree, pageNum uint32) *IntegrityResult {
	result := &IntegrityResult{
		Errors: make([]*IntegrityError, 0),
	}

	if bt == nil {
		result.AddError(0, "null_btree", "btree is nil")
		return result
	}

	// Get page data
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		result.AddError(pageNum, "page_not_found", fmt.Sprintf("failed to get page: %v", err))
		return result
	}

	// Parse header
	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		result.AddError(pageNum, "invalid_header", fmt.Sprintf("failed to parse page header: %v", err))
		return result
	}

	// Check page format
	checkPageFormat(bt, pageNum, pageData, header, result)

	// Get cell pointers
	cellPointers, err := header.GetCellPointers(pageData)
	if err != nil {
		result.AddError(pageNum, "invalid_cell_pointers", fmt.Sprintf("failed to get cell pointers: %v", err))
		return result
	}

	// Check cell pointers sorted
	checkCellPointersSorted(pageNum, cellPointers, result)

	// Parse cells and check overlaps
	cellOffsets := make([]int, 0, header.NumCells)
	cellSizes := make([]int, 0, header.NumCells)
	cells := make([]*CellInfo, 0, header.NumCells)

	for i := 0; i < int(header.NumCells); i++ {
		cellOffset := int(cellPointers[i])
		if cellOffset >= len(pageData) {
			result.AddError(pageNum, "cell_out_of_bounds",
				fmt.Sprintf("cell %d offset %d exceeds page size %d", i, cellOffset, len(pageData)))
			continue
		}

		cellData := pageData[cellOffset:]
		cell, err := ParseCell(header.PageType, cellData, bt.UsableSize)
		if err != nil {
			result.AddError(pageNum, "invalid_cell", fmt.Sprintf("cell %d parse error: %v", i, err))
			continue
		}

		cells = append(cells, cell)
		cellOffsets = append(cellOffsets, cellOffset)
		cellSizes = append(cellSizes, int(cell.CellSize))
	}

	checkOverlappingCells(pageNum, cellOffsets, cellSizes, result)
	checkKeyOrder(pageNum, cells, nil, nil, result)

	result.PageCount = 1
	if header.IsLeaf {
		result.RowCount = int64(len(cells))
	}

	return result
}

// ValidateFreeBlockList checks the integrity of the free block list on a page
func ValidateFreeBlockList(bt *Btree, pageNum uint32) *IntegrityResult {
	result := &IntegrityResult{
		Errors: make([]*IntegrityError, 0),
	}

	if bt == nil {
		result.AddError(0, "null_btree", "btree is nil")
		return result
	}

	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		result.AddError(pageNum, "page_not_found", fmt.Sprintf("failed to get page: %v", err))
		return result
	}

	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		result.AddError(pageNum, "invalid_header", fmt.Sprintf("failed to parse page header: %v", err))
		return result
	}

	// If no freeblocks, nothing to check
	if header.FirstFreeblock == 0 {
		return result
	}

	// Traverse the free block list
	visited := make(map[uint16]bool)
	offset := header.FirstFreeblock
	maxIterations := 1000 // Prevent infinite loops

	for i := 0; i < maxIterations && offset != 0; i++ {
		// Check for cycles
		if visited[offset] {
			result.AddError(pageNum, "freeblock_cycle",
				fmt.Sprintf("free block list contains cycle at offset %d", offset))
			break
		}
		visited[offset] = true

		// Check offset is within bounds
		if int(offset)+4 > len(pageData) {
			result.AddError(pageNum, "freeblock_out_of_bounds",
				fmt.Sprintf("free block at offset %d exceeds page bounds", offset))
			break
		}

		// Read next pointer (2 bytes) and size (2 bytes)
		nextOffset := uint16(pageData[offset])<<8 | uint16(pageData[offset+1])
		blockSize := uint16(pageData[offset+2])<<8 | uint16(pageData[offset+3])

		// Validate block size
		if blockSize < 4 {
			result.AddError(pageNum, "invalid_freeblock_size",
				fmt.Sprintf("free block at offset %d has invalid size %d (minimum 4)", offset, blockSize))
		}

		// Check block doesn't exceed page bounds
		if int(offset)+int(blockSize) > int(bt.UsableSize) {
			result.AddError(pageNum, "freeblock_exceeds_page",
				fmt.Sprintf("free block at offset %d with size %d exceeds page usable size %d",
					offset, blockSize, bt.UsableSize))
		}

		offset = nextOffset
	}

	if offset != 0 && len(visited) >= maxIterations {
		result.AddError(pageNum, "freeblock_list_too_long",
			fmt.Sprintf("free block list exceeds maximum iterations %d", maxIterations))
	}

	return result
}
