// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
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
	Errors    []*IntegrityError // All errors found
	PageCount uint32            // Number of pages checked
	RowCount  int64             // Total rows found
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
	if !validatePageAccess(pageNum, visited, depth, result) {
		return 0
	}
	visited[pageNum] = true

	header, cellPointers := loadAndValidatePage(bt, pageNum, result)
	if header == nil {
		return 0
	}

	cells := parseCellsFromPage(bt, pageNum, header, cellPointers, result)
	checkOverlappingCells(pageNum, extractOffsets(cells), extractSizes(cells), result)
	checkKeyOrder(pageNum, extractCells(cells), minKey, maxKey, result)

	return countPageRows(bt, pageNum, header, extractCells(cells), visited, result, depth)
}

// validatePageAccess checks for cycles and depth limits
func validatePageAccess(pageNum uint32, visited map[uint32]bool, depth int, result *IntegrityResult) bool {
	if visited[pageNum] {
		result.AddError(pageNum, "cycle_detected", "page appears multiple times in tree (cycle)")
		return false
	}

	if depth > MaxBtreeDepth {
		result.AddError(pageNum, "depth_exceeded", fmt.Sprintf("tree depth %d exceeds maximum %d", depth, MaxBtreeDepth))
		return false
	}

	return true
}

// loadAndValidatePage loads page data and validates its format
func loadAndValidatePage(bt *Btree, pageNum uint32, result *IntegrityResult) (*PageHeader, []uint16) {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		result.AddError(pageNum, "page_not_found", fmt.Sprintf("failed to get page: %v", err))
		return nil, nil
	}

	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		result.AddError(pageNum, "invalid_header", fmt.Sprintf("failed to parse page header: %v", err))
		return nil, nil
	}

	checkPageFormat(bt, pageNum, pageData, header, result)

	cellPointers, err := header.GetCellPointers(pageData)
	if err != nil {
		result.AddError(pageNum, "invalid_cell_pointers", fmt.Sprintf("failed to get cell pointers: %v", err))
		return nil, nil
	}

	checkCellPointersSorted(pageNum, cellPointers, result)
	return header, cellPointers
}

// cellParseResult holds parsed cell data
type cellParseResult struct {
	cell   *CellInfo
	offset int
	size   int
}

// parseCellsFromPage parses all cells from a page
func parseCellsFromPage(bt *Btree, pageNum uint32, header *PageHeader, cellPointers []uint16, result *IntegrityResult) []cellParseResult {
	pageData, _ := bt.GetPage(pageNum)
	cells := make([]cellParseResult, 0, header.NumCells)

	for i := 0; i < int(header.NumCells); i++ {
		cellOffset := int(cellPointers[i])
		if cellOffset >= len(pageData) {
			result.AddError(pageNum, "cell_out_of_bounds", fmt.Sprintf("cell %d offset %d exceeds page size %d", i, cellOffset, len(pageData)))
			continue
		}

		cell, err := ParseCell(header.PageType, pageData[cellOffset:], bt.UsableSize)
		if err != nil {
			result.AddError(pageNum, "invalid_cell", fmt.Sprintf("cell %d parse error: %v", i, err))
			continue
		}

		cells = append(cells, cellParseResult{cell: cell, offset: cellOffset, size: int(cell.CellSize)})
	}

	return cells
}

// extractCells extracts CellInfo from parse results
func extractCells(results []cellParseResult) []*CellInfo {
	cells := make([]*CellInfo, len(results))
	for i, r := range results {
		cells[i] = r.cell
	}
	return cells
}

// extractOffsets extracts offsets from parse results
func extractOffsets(results []cellParseResult) []int {
	offsets := make([]int, len(results))
	for i, r := range results {
		offsets[i] = r.offset
	}
	return offsets
}

// extractSizes extracts sizes from parse results
func extractSizes(results []cellParseResult) []int {
	sizes := make([]int, len(results))
	for i, r := range results {
		sizes[i] = r.size
	}
	return sizes
}

// countPageRows counts rows based on page type
func countPageRows(bt *Btree, pageNum uint32, header *PageHeader, cells []*CellInfo, visited map[uint32]bool, result *IntegrityResult, depth int) int64 {
	if header.IsInterior {
		return checkInteriorPage(bt, pageNum, header, cells, visited, result, depth)
	}
	return int64(len(cells))
}

// checkPageFormat validates the page format
func checkPageFormat(bt *Btree, pageNum uint32, pageData []byte, header *PageHeader, result *IntegrityResult) {
	checkPageSize(bt, pageNum, pageData, result)
	checkCellContentArea(bt, pageNum, header, result)
	checkInteriorPageRightChild(pageNum, header, result)
}

// checkPageSize validates the page size matches btree page size
func checkPageSize(bt *Btree, pageNum uint32, pageData []byte, result *IntegrityResult) {
	if uint32(len(pageData)) != bt.PageSize {
		result.AddError(pageNum, "invalid_page_size",
			fmt.Sprintf("page size %d doesn't match btree page size %d", len(pageData), bt.PageSize))
	}
}

// checkCellContentArea validates cell content area boundaries
func checkCellContentArea(bt *Btree, pageNum uint32, header *PageHeader, result *IntegrityResult) {
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

	if cellContentStart > int(bt.UsableSize) {
		result.AddError(pageNum, "content_out_of_bounds",
			fmt.Sprintf("cell content start %d exceeds usable size %d",
				cellContentStart, bt.UsableSize))
	}

	maxCells := (int(bt.UsableSize) - header.HeaderSize) / 6
	if int(header.NumCells) > maxCells {
		result.AddError(pageNum, "too_many_cells",
			fmt.Sprintf("cell count %d exceeds reasonable maximum %d",
				header.NumCells, maxCells))
	}
}

// checkInteriorPageRightChild validates interior page right child pointer
func checkInteriorPageRightChild(pageNum uint32, header *PageHeader, result *IntegrityResult) {
	if !header.IsInterior {
		return
	}
	if header.RightChild == 0 {
		result.AddError(pageNum, "invalid_right_child", "interior page has right child pointer of 0")
	}
	if header.RightChild == pageNum {
		result.AddError(pageNum, "self_reference", "interior page right child points to itself")
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
		index int
		start int
		end   int
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
		checkKeyBounds(pageNum, i, cells[i].Key, minKey, maxKey, result)
		checkKeySequence(pageNum, i, cells, result)
	}
}

// checkKeyBounds validates a key is within the specified bounds.
func checkKeyBounds(pageNum uint32, idx int, key int64, minKey, maxKey *int64, result *IntegrityResult) {
	if minKey != nil && key <= *minKey {
		result.AddError(pageNum, "key_out_of_range",
			fmt.Sprintf("cell %d key %d is not greater than minimum bound %d", idx, key, *minKey))
	}
	if maxKey != nil && key > *maxKey {
		result.AddError(pageNum, "key_out_of_range",
			fmt.Sprintf("cell %d key %d is greater than maximum bound %d", idx, key, *maxKey))
	}
}

// checkKeySequence validates keys are in ascending order.
func checkKeySequence(pageNum uint32, idx int, cells []*CellInfo, result *IntegrityResult) {
	if idx > 0 && cells[idx].Key <= cells[idx-1].Key {
		result.AddError(pageNum, "keys_not_sorted",
			fmt.Sprintf("cell %d key %d is not greater than previous key %d",
				idx, cells[idx].Key, cells[idx-1].Key))
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

	pageData, header := loadPageAndHeader(bt, pageNum, result)
	if pageData == nil || header == nil {
		return result
	}

	checkPageFormat(bt, pageNum, pageData, header, result)

	cellPointers := validateCellPointers(bt, pageNum, pageData, header, result)
	if cellPointers == nil {
		return result
	}

	cells, cellOffsets, cellSizes := parseAndCollectCells(bt, pageNum, pageData, header, cellPointers, result)

	checkOverlappingCells(pageNum, cellOffsets, cellSizes, result)
	checkKeyOrder(pageNum, cells, nil, nil, result)

	finalizeResult(result, header, cells)
	return result
}

// loadPageAndHeader loads page data and parses the header.
func loadPageAndHeader(bt *Btree, pageNum uint32, result *IntegrityResult) ([]byte, *PageHeader) {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		result.AddError(pageNum, "page_not_found", fmt.Sprintf("failed to get page: %v", err))
		return nil, nil
	}

	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		result.AddError(pageNum, "invalid_header", fmt.Sprintf("failed to parse page header: %v", err))
		return nil, nil
	}

	return pageData, header
}

// validateCellPointers gets and validates cell pointers.
func validateCellPointers(bt *Btree, pageNum uint32, pageData []byte, header *PageHeader, result *IntegrityResult) []uint16 {
	cellPointers, err := header.GetCellPointers(pageData)
	if err != nil {
		result.AddError(pageNum, "invalid_cell_pointers", fmt.Sprintf("failed to get cell pointers: %v", err))
		return nil
	}

	checkCellPointersSorted(pageNum, cellPointers, result)
	return cellPointers
}

// parseAndCollectCells parses all cells and collects their information.
func parseAndCollectCells(bt *Btree, pageNum uint32, pageData []byte, header *PageHeader, cellPointers []uint16, result *IntegrityResult) ([]*CellInfo, []int, []int) {
	cellOffsets := make([]int, 0, header.NumCells)
	cellSizes := make([]int, 0, header.NumCells)
	cells := make([]*CellInfo, 0, header.NumCells)

	for i := 0; i < int(header.NumCells); i++ {
		cell, offset, size := parseSingleCell(bt, pageNum, pageData, header, cellPointers, i, result)
		if cell != nil {
			cells = append(cells, cell)
			cellOffsets = append(cellOffsets, offset)
			cellSizes = append(cellSizes, size)
		}
	}

	return cells, cellOffsets, cellSizes
}

// parseSingleCell parses a single cell at the given index.
func parseSingleCell(bt *Btree, pageNum uint32, pageData []byte, header *PageHeader, cellPointers []uint16, i int, result *IntegrityResult) (*CellInfo, int, int) {
	cellOffset := int(cellPointers[i])
	if cellOffset >= len(pageData) {
		result.AddError(pageNum, "cell_out_of_bounds",
			fmt.Sprintf("cell %d offset %d exceeds page size %d", i, cellOffset, len(pageData)))
		return nil, 0, 0
	}

	cellData := pageData[cellOffset:]
	cell, err := ParseCell(header.PageType, cellData, bt.UsableSize)
	if err != nil {
		result.AddError(pageNum, "invalid_cell", fmt.Sprintf("cell %d parse error: %v", i, err))
		return nil, 0, 0
	}

	return cell, cellOffset, int(cell.CellSize)
}

// finalizeResult sets the final counts in the result.
func finalizeResult(result *IntegrityResult, header *PageHeader, cells []*CellInfo) {
	result.PageCount = 1
	if header.IsLeaf {
		result.RowCount = int64(len(cells))
	}
}

// ValidateFreeBlockList checks the integrity of the free block list on a page
func ValidateFreeBlockList(bt *Btree, pageNum uint32) *IntegrityResult {
	result := &IntegrityResult{
		Errors: make([]*IntegrityError, 0),
	}

	pageData, header := validateFreeBlockPrerequisites(bt, pageNum, result)
	if pageData == nil || header == nil {
		return result
	}

	// If no freeblocks, nothing to check
	if header.FirstFreeblock == 0 {
		return result
	}

	// Traverse the free block list
	traverseFreeBlockList(bt, pageNum, pageData, header.FirstFreeblock, result)

	return result
}

// validateFreeBlockPrerequisites checks btree, page, and header validity
// Returns nil values if validation fails
func validateFreeBlockPrerequisites(bt *Btree, pageNum uint32, result *IntegrityResult) ([]byte, *PageHeader) {
	if bt == nil {
		result.AddError(0, "null_btree", "btree is nil")
		return nil, nil
	}

	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		result.AddError(pageNum, "page_not_found", fmt.Sprintf("failed to get page: %v", err))
		return nil, nil
	}

	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		result.AddError(pageNum, "invalid_header", fmt.Sprintf("failed to parse page header: %v", err))
		return nil, nil
	}

	return pageData, header
}

// traverseFreeBlockList walks through the free block chain and validates each block
func traverseFreeBlockList(bt *Btree, pageNum uint32, pageData []byte, startOffset uint16, result *IntegrityResult) {
	visited := make(map[uint16]bool)
	offset := startOffset
	maxIterations := 1000 // Prevent infinite loops

	for i := 0; i < maxIterations && offset != 0; i++ {
		if shouldStopTraversal(pageNum, offset, visited, pageData, result) {
			break
		}
		visited[offset] = true

		nextOffset, blockSize := parseFreeBlock(pageData, offset)
		validateFreeBlock(bt, pageNum, offset, blockSize, result)

		offset = nextOffset
	}

	checkMaxIterationsExceeded(pageNum, offset, visited, maxIterations, result)
}

// shouldStopTraversal checks if we should stop traversing (cycle detected or out of bounds)
func shouldStopTraversal(pageNum uint32, offset uint16, visited map[uint16]bool, pageData []byte, result *IntegrityResult) bool {
	// Check for cycles
	if visited[offset] {
		result.AddError(pageNum, "freeblock_cycle",
			fmt.Sprintf("free block list contains cycle at offset %d", offset))
		return true
	}

	// Check offset is within bounds
	if int(offset)+4 > len(pageData) {
		result.AddError(pageNum, "freeblock_out_of_bounds",
			fmt.Sprintf("free block at offset %d exceeds page bounds", offset))
		return true
	}

	return false
}

// parseFreeBlock reads the next pointer and size from a free block
func parseFreeBlock(pageData []byte, offset uint16) (nextOffset uint16, blockSize uint16) {
	nextOffset = uint16(pageData[offset])<<8 | uint16(pageData[offset+1])
	blockSize = uint16(pageData[offset+2])<<8 | uint16(pageData[offset+3])
	return nextOffset, blockSize
}

// validateFreeBlock validates a single free block's size and bounds
func validateFreeBlock(bt *Btree, pageNum uint32, offset uint16, blockSize uint16, result *IntegrityResult) {
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
}

// checkMaxIterationsExceeded checks if the free block list is too long
func checkMaxIterationsExceeded(pageNum uint32, offset uint16, visited map[uint16]bool, maxIterations int, result *IntegrityResult) {
	if offset != 0 && len(visited) >= maxIterations {
		result.AddError(pageNum, "freeblock_list_too_long",
			fmt.Sprintf("free block list exceeds maximum iterations %d", maxIterations))
	}
}
