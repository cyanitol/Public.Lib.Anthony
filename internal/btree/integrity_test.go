package btree

import (
	"encoding/binary"
	"testing"
)

func TestCheckIntegrity_ValidTree(t *testing.T) {
	bt := NewBtree(4096)

	// Create a valid leaf page
	// Use page 2 instead of page 1 because page 1 has a 100-byte file header
	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{2, []byte("row2")},
		{3, []byte("row3")},
	})

	bt.Pages[2] = pageData

	result := CheckIntegrity(bt, 2)

	if !result.OK() {
		t.Errorf("Expected no errors, got %d errors:", len(result.Errors))
		for _, err := range result.Errors {
			t.Errorf("  - %v", err)
		}
	}

	if result.RowCount != 3 {
		t.Errorf("Expected row count 3, got %d", result.RowCount)
	}

	if result.PageCount != 1 {
		t.Errorf("Expected page count 1, got %d", result.PageCount)
	}
}

func TestCheckIntegrity_NilBtree(t *testing.T) {
	result := CheckIntegrity(nil, 1)

	if result.OK() {
		t.Error("Expected errors for nil btree")
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}

	if result.Errors[0].ErrorType != "null_btree" {
		t.Errorf("Expected error type 'null_btree', got '%s'", result.Errors[0].ErrorType)
	}
}

func TestCheckIntegrity_InvalidRootPage(t *testing.T) {
	bt := NewBtree(4096)
	result := CheckIntegrity(bt, 0)

	if result.OK() {
		t.Error("Expected errors for invalid root page")
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}

	if result.Errors[0].ErrorType != "invalid_root" {
		t.Errorf("Expected error type 'invalid_root', got '%s'", result.Errors[0].ErrorType)
	}
}

func TestCheckIntegrity_PageNotFound(t *testing.T) {
	bt := NewBtree(4096)

	// Don't add page 1
	result := CheckIntegrity(bt, 1)

	if result.OK() {
		t.Error("Expected errors for missing page")
	}

	hasPageNotFound := false
	for _, err := range result.Errors {
		if err.ErrorType == "page_not_found" {
			hasPageNotFound = true
			break
		}
	}

	if !hasPageNotFound {
		t.Error("Expected 'page_not_found' error")
	}
}

func TestCheckIntegrity_InvalidPageHeader(t *testing.T) {
	bt := NewBtree(4096)

	// Create page with invalid page type
	pageData := make([]byte, 4096)
	pageData[0] = 0xFF // Invalid page type

	bt.Pages[1] = pageData

	result := CheckIntegrity(bt, 1)

	if result.OK() {
		t.Error("Expected errors for invalid page header")
	}

	hasInvalidHeader := false
	for _, err := range result.Errors {
		if err.ErrorType == "invalid_header" {
			hasInvalidHeader = true
			break
		}
	}

	if !hasInvalidHeader {
		t.Error("Expected 'invalid_header' error")
	}
}

func TestCheckIntegrity_UnsortedCellPointers(t *testing.T) {
	bt := NewBtree(4096)
	pageData := make([]byte, 4096)

	// Create a leaf table page (use page 2 to avoid file header issues)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[3:], 2) // NumCells = 2

	// Cell pointers (should be descending, but we make them wrong)
	binary.BigEndian.PutUint16(pageData[8:], 100)  // First cell at offset 100
	binary.BigEndian.PutUint16(pageData[10:], 200) // Second cell at offset 200 (wrong - should be < 100)

	// Cell content start
	binary.BigEndian.PutUint16(pageData[5:], 100)

	// Add minimal cell data
	binary.BigEndian.PutUint16(pageData[100:], 0) // Payload size
	binary.BigEndian.PutUint16(pageData[200:], 0) // Payload size

	bt.Pages[2] = pageData

	result := CheckIntegrity(bt, 2)

	if result.OK() {
		t.Error("Expected errors for unsorted cell pointers")
	}

	hasUnsortedPointers := false
	for _, err := range result.Errors {
		if err.ErrorType == "unsorted_cell_pointers" {
			hasUnsortedPointers = true
			break
		}
	}

	if !hasUnsortedPointers {
		t.Error("Expected 'unsorted_cell_pointers' error")
	}
}

func TestCheckIntegrity_KeysNotSorted(t *testing.T) {
	bt := NewBtree(4096)

	// Create a leaf page with unsorted keys (use page 2)
	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{3, []byte("row3")},
		{2, []byte("row2")}, // Out of order!
	})

	bt.Pages[2] = pageData

	result := CheckIntegrity(bt, 2)

	if result.OK() {
		t.Error("Expected errors for unsorted keys")
	}

	hasUnsortedKeys := false
	for _, err := range result.Errors {
		if err.ErrorType == "keys_not_sorted" {
			hasUnsortedKeys = true
			break
		}
	}

	if !hasUnsortedKeys {
		t.Error("Expected 'keys_not_sorted' error")
	}
}

func TestCheckIntegrity_DuplicateKeys(t *testing.T) {
	bt := NewBtree(4096)

	// Create a leaf page with duplicate keys (use page 2)
	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{2, []byte("row2")},
		{2, []byte("row2_duplicate")}, // Duplicate key
	})

	bt.Pages[2] = pageData

	result := CheckIntegrity(bt, 2)

	if result.OK() {
		t.Error("Expected errors for duplicate keys")
	}

	hasUnsortedKeys := false
	for _, err := range result.Errors {
		if err.ErrorType == "keys_not_sorted" {
			hasUnsortedKeys = true
			break
		}
	}

	if !hasUnsortedKeys {
		t.Error("Expected 'keys_not_sorted' error for duplicate keys")
	}
}

func TestCheckIntegrity_InteriorPage(t *testing.T) {
	bt := NewBtree(4096)

	// Create a valid two-level tree
	// Root (interior) -> Two leaf pages
	// Use page 4 as root to avoid file header issues with page 1

	// Leaf page 2: rows 1-5
	leaf1 := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{2, []byte("row2")},
		{3, []byte("row3")},
		{4, []byte("row4")},
		{5, []byte("row5")},
	})
	bt.Pages[2] = leaf1

	// Leaf page 3: rows 6-10
	leaf2 := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{6, []byte("row6")},
		{7, []byte("row7")},
		{8, []byte("row8")},
		{9, []byte("row9")},
		{10, []byte("row10")},
	})
	bt.Pages[3] = leaf2

	// Interior root page (page 4 to avoid file header issues)
	root := make([]byte, 4096)
	root[0] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(root[3:], 1)    // NumCells = 1
	binary.BigEndian.PutUint32(root[8:], 3)    // RightChild = page 3
	binary.BigEndian.PutUint16(root[5:], 4084) // CellContentStart

	// Cell 0: child page 2, key 5
	cellOffset := 4084
	binary.BigEndian.PutUint16(root[12:], uint16(cellOffset)) // Cell pointer

	binary.BigEndian.PutUint32(root[cellOffset:], 2) // Child page 2
	PutVarint(root[cellOffset+4:], 5)                // Key 5

	bt.Pages[4] = root

	result := CheckIntegrity(bt, 4)

	if !result.OK() {
		t.Errorf("Expected no errors, got %d errors:", len(result.Errors))
		for _, err := range result.Errors {
			t.Errorf("  - %v", err)
		}
	}

	if result.RowCount != 10 {
		t.Errorf("Expected row count 10, got %d", result.RowCount)
	}

	if result.PageCount != 3 {
		t.Errorf("Expected page count 3, got %d", result.PageCount)
	}
}

func TestCheckIntegrity_InvalidChildPointer(t *testing.T) {
	bt := NewBtree(4096)

	// Create interior page with invalid child pointer (0)
	// Use page 4 as root to avoid file header issues
	root := make([]byte, 4096)
	root[0] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(root[3:], 1)    // NumCells = 1
	binary.BigEndian.PutUint32(root[8:], 2)    // RightChild = page 2
	binary.BigEndian.PutUint16(root[5:], 4084) // CellContentStart

	// Cell 0: child page 0 (invalid!), key 5
	cellOffset := 4084
	binary.BigEndian.PutUint16(root[12:], uint16(cellOffset))

	binary.BigEndian.PutUint32(root[cellOffset:], 0) // Child page 0 - INVALID
	PutVarint(root[cellOffset+4:], 5)                // Key 5

	// Create leaf page 2
	leaf := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{6, []byte("row6")},
	})
	bt.Pages[2] = leaf

	bt.Pages[4] = root

	result := CheckIntegrity(bt, 4)

	if result.OK() {
		t.Error("Expected errors for invalid child pointer")
	}

	hasInvalidChild := false
	for _, err := range result.Errors {
		if err.ErrorType == "invalid_child_pointer" {
			hasInvalidChild = true
			break
		}
	}

	if !hasInvalidChild {
		t.Error("Expected 'invalid_child_pointer' error")
	}
}

func TestCheckIntegrity_SelfReference(t *testing.T) {
	bt := NewBtree(4096)

	// Create interior page that points to itself (use page 4)
	root := make([]byte, 4096)
	root[0] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(root[3:], 1)    // NumCells = 1
	binary.BigEndian.PutUint32(root[8:], 4)    // RightChild = page 4 (SELF!)
	binary.BigEndian.PutUint16(root[5:], 4084) // CellContentStart

	cellOffset := 4084
	binary.BigEndian.PutUint16(root[12:], uint16(cellOffset))

	binary.BigEndian.PutUint32(root[cellOffset:], 4) // Child page 4 (SELF!)
	PutVarint(root[cellOffset+4:], 5)                // Key 5

	bt.Pages[4] = root

	result := CheckIntegrity(bt, 4)

	if result.OK() {
		t.Error("Expected errors for self-reference")
	}

	hasSelfRef := false
	for _, err := range result.Errors {
		if err.ErrorType == "self_reference" {
			hasSelfRef = true
			break
		}
	}

	if !hasSelfRef {
		t.Error("Expected 'self_reference' error")
	}
}

func TestCheckIntegrity_CycleDetection(t *testing.T) {
	bt := NewBtree(4096)

	// Create a cycle: page 4 -> page 2 -> page 4 (use page 4 as root)
	root := make([]byte, 4096)
	root[0] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(root[3:], 0)    // NumCells = 0
	binary.BigEndian.PutUint32(root[8:], 2)    // RightChild = page 2
	binary.BigEndian.PutUint16(root[5:], 4096) // CellContentStart

	page2 := make([]byte, 4096)
	page2[0] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(page2[3:], 0)    // NumCells = 0
	binary.BigEndian.PutUint32(page2[8:], 4)    // RightChild = page 4 (CYCLE!)
	binary.BigEndian.PutUint16(page2[5:], 4096) // CellContentStart

	bt.Pages[4] = root
	bt.Pages[2] = page2

	result := CheckIntegrity(bt, 4)

	if result.OK() {
		t.Error("Expected errors for cycle")
	}

	hasCycle := false
	for _, err := range result.Errors {
		if err.ErrorType == "cycle_detected" {
			hasCycle = true
			break
		}
	}

	if !hasCycle {
		t.Error("Expected 'cycle_detected' error")
	}
}

func TestCheckIntegrity_OrphanPage(t *testing.T) {
	bt := NewBtree(4096)

	// Create a valid leaf page as root (use page 2)
	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
	})
	bt.Pages[2] = pageData

	// Create an orphan page (not referenced)
	orphan := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{2, []byte("row2")},
	})
	bt.Pages[999] = orphan

	result := CheckIntegrity(bt, 2)

	if result.OK() {
		t.Error("Expected errors for orphan page")
	}

	hasOrphan := false
	for _, err := range result.Errors {
		if err.ErrorType == "orphan_page" && err.PageNum == 999 {
			hasOrphan = true
			break
		}
	}

	if !hasOrphan {
		t.Error("Expected 'orphan_page' error for page 999")
	}
}

func TestCheckIntegrity_KeyOutOfRange(t *testing.T) {
	bt := NewBtree(4096)

	// Create interior page with bounds (use page 4 as root)
	root := make([]byte, 4096)
	root[0] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(root[3:], 1)    // NumCells = 1
	binary.BigEndian.PutUint32(root[8:], 3)    // RightChild = page 3
	binary.BigEndian.PutUint16(root[5:], 4084) // CellContentStart

	cellOffset := 4084
	binary.BigEndian.PutUint16(root[12:], uint16(cellOffset))
	binary.BigEndian.PutUint32(root[cellOffset:], 2) // Child page 2
	PutVarint(root[cellOffset+4:], 5)                // Key 5 (boundary)

	// Leaf page 2 should have keys < 5, but we put key 6 (out of range!)
	leaf1 := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{6, []byte("row6")}, // Out of range! Should be < 5
	})

	// Leaf page 3 should have keys >= 5
	leaf2 := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{7, []byte("row7")},
	})

	bt.Pages[4] = root
	bt.Pages[2] = leaf1
	bt.Pages[3] = leaf2

	result := CheckIntegrity(bt, 4)

	if result.OK() {
		t.Error("Expected errors for key out of range")
	}

	hasOutOfRange := false
	for _, err := range result.Errors {
		if err.ErrorType == "key_out_of_range" {
			hasOutOfRange = true
			break
		}
	}

	if !hasOutOfRange {
		t.Error("Expected 'key_out_of_range' error")
	}
}

func TestCheckPageIntegrity(t *testing.T) {
	bt := NewBtree(4096)

	// Create a valid leaf page (use page 2)
	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{2, []byte("row2")},
	})

	bt.Pages[2] = pageData

	result := CheckPageIntegrity(bt, 2)

	if !result.OK() {
		t.Errorf("Expected no errors, got %d errors:", len(result.Errors))
		for _, err := range result.Errors {
			t.Errorf("  - %v", err)
		}
	}

	if result.RowCount != 2 {
		t.Errorf("Expected row count 2, got %d", result.RowCount)
	}

	if result.PageCount != 1 {
		t.Errorf("Expected page count 1, got %d", result.PageCount)
	}
}

func TestValidateFreeBlockList_NoFreeBlocks(t *testing.T) {
	bt := NewBtree(4096)

	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
	})

	bt.Pages[2] = pageData

	result := ValidateFreeBlockList(bt, 2)

	if !result.OK() {
		t.Errorf("Expected no errors for page with no free blocks, got %d errors", len(result.Errors))
	}
}

func TestValidateFreeBlockList_ValidList(t *testing.T) {
	bt := NewBtree(4096)

	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[1:], 100) // FirstFreeblock at offset 100
	binary.BigEndian.PutUint16(pageData[3:], 0)   // NumCells = 0
	binary.BigEndian.PutUint16(pageData[5:], 0)   // CellContentStart = 0 (end of page)

	// Free block at offset 100
	binary.BigEndian.PutUint16(pageData[100:], 200) // Next block at 200
	binary.BigEndian.PutUint16(pageData[102:], 50)  // Block size 50

	// Free block at offset 200
	binary.BigEndian.PutUint16(pageData[200:], 0)  // No next block
	binary.BigEndian.PutUint16(pageData[202:], 40) // Block size 40

	bt.Pages[2] = pageData

	result := ValidateFreeBlockList(bt, 2)

	if !result.OK() {
		t.Errorf("Expected no errors for valid free block list, got %d errors:", len(result.Errors))
		for _, err := range result.Errors {
			t.Errorf("  - %v", err)
		}
	}
}

func TestValidateFreeBlockList_Cycle(t *testing.T) {
	bt := NewBtree(4096)

	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[1:], 100) // FirstFreeblock at offset 100
	binary.BigEndian.PutUint16(pageData[3:], 0)   // NumCells = 0
	binary.BigEndian.PutUint16(pageData[5:], 0)   // CellContentStart = 0

	// Free block at offset 100
	binary.BigEndian.PutUint16(pageData[100:], 200) // Next block at 200
	binary.BigEndian.PutUint16(pageData[102:], 50)  // Block size 50

	// Free block at offset 200 - points back to 100 (cycle!)
	binary.BigEndian.PutUint16(pageData[200:], 100) // Next block at 100 (CYCLE!)
	binary.BigEndian.PutUint16(pageData[202:], 40)  // Block size 40

	bt.Pages[2] = pageData

	result := ValidateFreeBlockList(bt, 2)

	if result.OK() {
		t.Error("Expected errors for free block list cycle")
	}

	hasCycle := false
	for _, err := range result.Errors {
		if err.ErrorType == "freeblock_cycle" {
			hasCycle = true
			break
		}
	}

	if !hasCycle {
		t.Error("Expected 'freeblock_cycle' error")
	}
}

func TestValidateFreeBlockList_InvalidSize(t *testing.T) {
	bt := NewBtree(4096)

	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[1:], 100) // FirstFreeblock at offset 100
	binary.BigEndian.PutUint16(pageData[3:], 0)   // NumCells = 0
	binary.BigEndian.PutUint16(pageData[5:], 0)   // CellContentStart = 0

	// Free block with invalid size (< 4)
	binary.BigEndian.PutUint16(pageData[100:], 0) // No next block
	binary.BigEndian.PutUint16(pageData[102:], 2) // Block size 2 (INVALID - must be >= 4)

	bt.Pages[2] = pageData

	result := ValidateFreeBlockList(bt, 2)

	if result.OK() {
		t.Error("Expected errors for invalid free block size")
	}

	hasInvalidSize := false
	for _, err := range result.Errors {
		if err.ErrorType == "invalid_freeblock_size" {
			hasInvalidSize = true
			break
		}
	}

	if !hasInvalidSize {
		t.Error("Expected 'invalid_freeblock_size' error")
	}
}

func TestValidateFreeBlockList_OutOfBounds(t *testing.T) {
	bt := NewBtree(4096)

	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[1:], 5000) // FirstFreeblock at offset 5000 (OUT OF BOUNDS!)
	binary.BigEndian.PutUint16(pageData[3:], 0)    // NumCells = 0
	binary.BigEndian.PutUint16(pageData[5:], 0)    // CellContentStart = 0

	bt.Pages[2] = pageData

	result := ValidateFreeBlockList(bt, 2)

	if result.OK() {
		t.Error("Expected errors for free block out of bounds")
	}

	hasOutOfBounds := false
	for _, err := range result.Errors {
		if err.ErrorType == "freeblock_out_of_bounds" {
			hasOutOfBounds = true
			break
		}
	}

	if !hasOutOfBounds {
		t.Error("Expected 'freeblock_out_of_bounds' error")
	}
}

func TestIntegrityError_Error(t *testing.T) {
	err1 := &IntegrityError{
		PageNum:     5,
		ErrorType:   "test_error",
		Description: "test description",
	}

	expected1 := "[page 5, test_error] test description"
	if err1.Error() != expected1 {
		t.Errorf("Expected error message '%s', got '%s'", expected1, err1.Error())
	}

	err2 := &IntegrityError{
		PageNum:     0,
		ErrorType:   "tree_error",
		Description: "tree-wide error",
	}

	expected2 := "[tree_error] tree-wide error"
	if err2.Error() != expected2 {
		t.Errorf("Expected error message '%s', got '%s'", expected2, err2.Error())
	}
}

func TestCheckIntegrity_ContentOverlap(t *testing.T) {
	bt := NewBtree(4096)

	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[3:], 2)  // NumCells = 2
	binary.BigEndian.PutUint16(pageData[5:], 10) // CellContentStart = 10 (overlaps with header!)

	// Add cell pointers
	binary.BigEndian.PutUint16(pageData[8:], 100)
	binary.BigEndian.PutUint16(pageData[10:], 90)

	bt.Pages[2] = pageData

	result := CheckIntegrity(bt, 2)

	if result.OK() {
		t.Error("Expected errors for content overlap")
	}

	hasOverlap := false
	for _, err := range result.Errors {
		if err.ErrorType == "content_overlap" {
			hasOverlap = true
			break
		}
	}

	if !hasOverlap {
		t.Error("Expected 'content_overlap' error")
	}
}

func TestCheckIntegrity_DepthExceeded(t *testing.T) {
	bt := NewBtree(4096)

	// Create a very deep tree (deeper than MaxBtreeDepth)
	// Start from page 100 to avoid file header issues
	startPage := 100
	for i := 0; i <= MaxBtreeDepth+1; i++ {
		page := make([]byte, 4096)
		page[0] = PageTypeInteriorTable
		binary.BigEndian.PutUint16(page[3:], 0)                    // NumCells = 0
		binary.BigEndian.PutUint32(page[8:], uint32(startPage+i+1)) // RightChild = next page
		binary.BigEndian.PutUint16(page[5:], 4096)                 // CellContentStart

		bt.Pages[uint32(startPage+i)] = page
	}

	// Last page is a leaf
	lastPage := make([]byte, 4096)
	lastPage[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(lastPage[3:], 0)    // NumCells = 0
	binary.BigEndian.PutUint16(lastPage[5:], 4096) // CellContentStart

	bt.Pages[uint32(startPage+MaxBtreeDepth+2)] = lastPage

	result := CheckIntegrity(bt, uint32(startPage))

	if result.OK() {
		t.Error("Expected errors for excessive depth")
	}

	hasDepthError := false
	for _, err := range result.Errors {
		if err.ErrorType == "depth_exceeded" {
			hasDepthError = true
			break
		}
	}

	if !hasDepthError {
		t.Error("Expected 'depth_exceeded' error")
	}
}

func TestCheckIntegrity_MultipleErrors(t *testing.T) {
	bt := NewBtree(4096)

	// Create a page with multiple errors (use page 2)
	pageData := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{3, []byte("row3")},
		{2, []byte("row2")}, // Out of order
	})

	// Also add an orphan page
	orphan := createTestLeafPage(4096, []struct {
		rowid   int64
		payload []byte
	}{
		{100, []byte("orphan")},
	})

	bt.Pages[2] = pageData
	bt.Pages[999] = orphan

	result := CheckIntegrity(bt, 2)

	if result.OK() {
		t.Error("Expected multiple errors")
	}

	// Should have at least 2 errors: unsorted keys and orphan page
	if len(result.Errors) < 2 {
		t.Errorf("Expected at least 2 errors, got %d", len(result.Errors))
	}

	hasUnsortedKeys := false
	hasOrphan := false

	for _, err := range result.Errors {
		if err.ErrorType == "keys_not_sorted" {
			hasUnsortedKeys = true
		}
		if err.ErrorType == "orphan_page" {
			hasOrphan = true
		}
	}

	if !hasUnsortedKeys {
		t.Error("Expected 'keys_not_sorted' error")
	}
	if !hasOrphan {
		t.Error("Expected 'orphan_page' error")
	}
}
