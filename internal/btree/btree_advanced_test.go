package btree

import (
	"fmt"
	"testing"
)

// TestDropTable_InteriorPages tests dropping a table with interior pages
func TestDropTable_InteriorPages(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages to force interior structure
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many entries to create interior pages
	for i := int64(1); i <= 50; i++ {
		err := cursor.Insert(i, []byte("data"))
		if err != nil {
			// Page might be full, that's OK
			break
		}
	}

	// Count pages before drop
	pagesBefore := len(bt.Pages)

	// Drop the table
	err = bt.DropTable(rootPage)
	if err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	// Pages should be freed
	pagesAfter := len(bt.Pages)
	if pagesAfter >= pagesBefore {
		t.Errorf("Pages not freed: before=%d, after=%d", pagesBefore, pagesAfter)
	}

	// Root page should be gone
	if _, exists := bt.Pages[rootPage]; exists {
		t.Error("Root page should be freed")
	}
}

// TestDropTable_InvalidRootPage tests dropping with invalid root
func TestDropTable_InvalidRootPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	err := bt.DropTable(0)
	if err == nil {
		t.Error("DropTable(0) should fail")
	}
}

// TestDropTable_NonExistentPage tests dropping non-existent page
func TestDropTable_NonExistentPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	err := bt.DropTable(999)
	if err == nil {
		t.Error("DropTable() should fail for non-existent page")
	}
}

// TestDropTable_CorruptedHeader tests dropping table with corrupted header
func TestDropTable_CorruptedHeader(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create corrupted page
	pageNum := uint32(5)
	pageData := make([]byte, 4096)
	pageData[0] = 0xFF // Invalid page type
	bt.SetPage(pageNum, pageData)

	err := bt.DropTable(pageNum)
	if err == nil {
		t.Error("DropTable() should fail with corrupted header")
	}
}

// TestNewRowid_EmptyTable tests NewRowid on empty table
func TestNewRowid_EmptyTable(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	rowid, err := bt.NewRowid(rootPage)
	if err != nil {
		t.Fatalf("NewRowid() error = %v", err)
	}

	if rowid != 1 {
		t.Errorf("NewRowid() = %d, want 1 for empty table", rowid)
	}
}

// TestNewRowid_WithExistingRows tests NewRowid with existing data
func TestNewRowid_WithExistingRows(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert some rows
	maxRowid := int64(10)
	for i := int64(1); i <= maxRowid; i++ {
		cursor.Insert(i, []byte("data"))
	}

	// Get new rowid
	rowid, err := bt.NewRowid(rootPage)
	if err != nil {
		t.Fatalf("NewRowid() error = %v", err)
	}

	if rowid != maxRowid+1 {
		t.Errorf("NewRowid() = %d, want %d", rowid, maxRowid+1)
	}
}

// TestNewRowid_InvalidRootPage tests NewRowid with invalid root
func TestNewRowid_InvalidRootPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	_, err := bt.NewRowid(0)
	if err == nil {
		t.Error("NewRowid(0) should fail")
	}
}

// TestAllocatePage_Overflow tests page allocation overflow
func TestAllocatePage_Overflow(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Allocate pages until we approach the limit
	// We won't actually overflow uint32, but we can test the logic
	for i := 0; i < 100; i++ {
		_, err := bt.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error at iteration %d: %v", i, err)
		}
	}

	// Verify pages were allocated
	if len(bt.Pages) != 100 {
		t.Errorf("Expected 100 pages, got %d", len(bt.Pages))
	}
}

// TestAllocatePage_WithProvider tests allocation with provider
func TestAllocatePage_WithProvider(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	mockProvider := &MockPageProvider{
		pages:      make(map[uint32][]byte),
		dirtyPages: make(map[uint32]bool),
	}
	bt.Provider = mockProvider

	pageNum, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	// Verify page was allocated through provider
	if pageNum == 0 {
		t.Error("AllocatePage() should return non-zero page number")
	}

	// Verify page exists in cache
	if _, exists := bt.Pages[pageNum]; !exists {
		t.Error("Allocated page should be in cache")
	}
}

// TestSetPage_WrongSize tests SetPage with wrong page size
func TestSetPage_WrongSize(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Try to set page with wrong size
	wrongSizeData := make([]byte, 2048)
	err := bt.SetPage(1, wrongSizeData)
	if err == nil {
		t.Error("SetPage() should fail with wrong size")
	}
}

// TestSetPage_WithProvider tests SetPage marks page dirty
func TestSetPage_WithProvider(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	mockProvider := &MockPageProvider{
		pages:      make(map[uint32][]byte),
		dirtyPages: make(map[uint32]bool),
	}
	bt.Provider = mockProvider

	pageData := make([]byte, 4096)
	err := bt.SetPage(1, pageData)
	if err != nil {
		t.Fatalf("SetPage() error = %v", err)
	}

	// Verify page was marked dirty
	if !mockProvider.dirtyPages[1] {
		t.Error("Page should be marked dirty")
	}
}

// TestGetPage_WithProvider tests GetPage with provider
func TestGetPage_WithProvider(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	mockProvider := &MockPageProvider{
		pages:      make(map[uint32][]byte),
		dirtyPages: make(map[uint32]bool),
	}
	bt.Provider = mockProvider

	// Add page to provider
	pageNum := uint32(10)
	expectedData := make([]byte, 4096)
	expectedData[0] = PageTypeLeafTable // Use valid page type
	expectedData[100] = 42              // Use different byte for testing data retrieval
	mockProvider.pages[pageNum] = expectedData

	// Get page (should fetch from provider and cache it)
	data, err := bt.GetPage(pageNum)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	if data[100] != 42 {
		t.Error("GetPage() should return data from provider")
	}

	// Verify page is now cached
	if _, exists := bt.Pages[pageNum]; !exists {
		t.Error("Page should be cached after GetPage")
	}

	// Second call should use cache
	data2, err := bt.GetPage(pageNum)
	if err != nil {
		t.Fatalf("Second GetPage() error = %v", err)
	}

	if data2[100] != 42 {
		t.Error("Second GetPage() should return cached data")
	}
}

// TestGetPage_NotFound tests GetPage with missing page
func TestGetPage_NotFound(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	_, err := bt.GetPage(999)
	if err == nil {
		t.Error("GetPage() should fail for non-existent page")
	}
}

// TestGetPage_ProviderError tests GetPage when provider fails
func TestGetPage_ProviderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	bt.Provider = &FailingGetProvider{}

	_, err := bt.GetPage(1)
	if err == nil {
		t.Error("GetPage() should fail when provider fails")
	}
}

// TestClearCacheAdvanced tests cache clearing with provider
func TestClearCacheAdvanced(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Add some pages
	for i := uint32(1); i <= 10; i++ {
		pageData := make([]byte, 4096)
		bt.SetPage(i, pageData)
	}

	if len(bt.Pages) != 10 {
		t.Fatalf("Expected 10 pages, got %d", len(bt.Pages))
	}

	// Clear cache
	bt.ClearCache()

	if len(bt.Pages) != 0 {
		t.Errorf("Cache should be empty, got %d pages", len(bt.Pages))
	}
}

// TestParsePageWithProvider tests ParsePage with provider
func TestParsePageWithProvider(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	mockProvider := &MockPageProvider{
		pages:      make(map[uint32][]byte),
		dirtyPages: make(map[uint32]bool),
	}
	bt.Provider = mockProvider

	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("test"))

	// Put page in provider
	pageData, _ := bt.GetPage(rootPage)
	mockProvider.pages[rootPage] = pageData

	// Clear local cache
	bt.ClearCache()

	// Parse should fetch from provider
	header, cells, err := bt.ParsePage(rootPage)
	if err != nil {
		t.Fatalf("ParsePage() error = %v", err)
	}

	if header == nil {
		t.Error("ParsePage() should return header")
	}

	if len(cells) != 1 {
		t.Errorf("ParsePage() returned %d cells, want 1", len(cells))
	}
}

// TestIteratePage tests IteratePage functionality
func TestIteratePage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	cursor := NewCursor(bt, rootPage)

	// Insert multiple rows
	numRows := 5
	for i := 1; i <= numRows; i++ {
		cursor.Insert(int64(i), []byte(fmt.Sprintf("data%d", i)))
	}

	// Iterate and count
	count := 0
	err := bt.IteratePage(rootPage, func(cellIndex int, cell *CellInfo) error {
		count++
		if cell.Key < 1 || cell.Key > int64(numRows) {
			t.Errorf("Invalid key %d at index %d", cell.Key, cellIndex)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("IteratePage() error = %v", err)
	}

	if count != numRows {
		t.Errorf("Iterated %d cells, want %d", count, numRows)
	}
}

// TestIteratePage_VisitorError tests IteratePage with visitor returning error
func TestIteratePage_VisitorError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("test"))

	expectedErr := fmt.Errorf("visitor error")
	err := bt.IteratePage(rootPage, func(cellIndex int, cell *CellInfo) error {
		return expectedErr
	})

	if err != expectedErr {
		t.Errorf("IteratePage() error = %v, want %v", err, expectedErr)
	}
}

// TestIteratePage_InvalidPage tests IteratePage with invalid page
func TestIteratePage_InvalidPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	err := bt.IteratePage(999, func(cellIndex int, cell *CellInfo) error {
		return nil
	})

	if err == nil {
		t.Error("IteratePage() should fail for invalid page")
	}
}

// TestString tests Btree String method
func TestString(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	str := bt.String()
	if str == "" {
		t.Error("String() should return non-empty string")
	}

	t.Logf("Btree: %s", str)

	// Add some pages
	bt.CreateTable()
	bt.CreateTable()

	str2 := bt.String()
	if str2 == "" {
		t.Error("String() should return non-empty string after adding pages")
	}

	t.Logf("Btree with pages: %s", str2)
}

// TestCreateTable_Page1 tests creating table at page 1 (with file header)
func TestCreateTable_Page1(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// First table should be at page 1
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if rootPage != 1 {
		t.Errorf("First table root = %d, want 1", rootPage)
	}

	// Verify page 1 has correct header offset
	pageData, _ := bt.GetPage(1)
	header, err := ParsePageHeader(pageData, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader() error = %v", err)
	}

	if !header.IsLeaf {
		t.Error("New table should be a leaf page")
	}

	if header.NumCells != 0 {
		t.Errorf("New table should have 0 cells, got %d", header.NumCells)
	}
}

// TestCreateTable_Multiple tests creating multiple tables
func TestCreateTable_Multiple(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	tables := make([]uint32, 5)
	for i := range tables {
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable() %d error = %v", i, err)
		}
		tables[i] = rootPage

		// Verify each table is unique
		for j := 0; j < i; j++ {
			if tables[j] == rootPage {
				t.Errorf("Duplicate table root page: %d", rootPage)
			}
		}
	}
}

// TestNewBtree_DefaultPageSize tests NewBtree with zero page size
func TestNewBtree_DefaultPageSize(t *testing.T) {
	t.Parallel()
	bt := NewBtree(0)

	if bt.PageSize != 4096 {
		t.Errorf("Default page size = %d, want 4096", bt.PageSize)
	}
}

// TestNewBtree_CustomPageSize tests NewBtree with custom page size
func TestNewBtree_CustomPageSize(t *testing.T) {
	t.Parallel()
	customSize := uint32(8192)
	bt := NewBtree(customSize)

	if bt.PageSize != customSize {
		t.Errorf("Page size = %d, want %d", bt.PageSize, customSize)
	}

	if bt.UsableSize != customSize {
		t.Errorf("Usable size = %d, want %d", bt.UsableSize, customSize)
	}

	if bt.ReservedSize != 0 {
		t.Errorf("Reserved size = %d, want 0", bt.ReservedSize)
	}
}

// FailingGetProvider is a mock that fails GetPageData
type FailingGetProvider struct{}

func (f *FailingGetProvider) GetPageData(pgno uint32) ([]byte, error) {
	return nil, fmt.Errorf("mock: get page failed")
}

func (f *FailingGetProvider) AllocatePageData() (uint32, []byte, error) {
	return 0, nil, fmt.Errorf("mock: allocation failed")
}

func (f *FailingGetProvider) MarkDirty(pgno uint32) error {
	return nil
}

// TestDropInteriorChildrenAdvanced tests dropInteriorChildren helper
func TestDropInteriorChildrenAdvanced(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create leaf children
	page2Data := createTestPage(2, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("a")}})
	bt.SetPage(2, page2Data)

	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{2, []byte("b")}})
	bt.SetPage(3, page3Data)

	// Create interior page with children
	interiorCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 1}}
	interiorData := createInteriorPage(1, pageSize, interiorCells, 3)
	bt.SetPage(1, interiorData)

	header, _ := ParsePageHeader(interiorData, 1)

	// Drop children
	bt.dropInteriorChildren(interiorData, header)

	// Children should be dropped
	if _, exists := bt.Pages[2]; exists {
		t.Error("Page 2 should be dropped")
	}
	if _, exists := bt.Pages[3]; exists {
		t.Error("Page 3 should be dropped")
	}
}
