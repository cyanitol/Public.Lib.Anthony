package btree

import (
	"encoding/binary"
	"testing"
)

func createTestLeafPage(pageSize uint32, cells []struct {
	rowid   int64
	payload []byte
}) []byte {
	data := make([]byte, pageSize)

	// Page header
	data[0] = PageTypeLeafTable // Leaf table page
	// Freeblock offset = 0
	numCells := uint16(len(cells))
	binary.BigEndian.PutUint16(data[3:], numCells)

	// Cell content area starts from the end
	cellContentOffset := pageSize
	cellPtrOffset := PageHeaderSizeLeaf

	// First pass: calculate all cell offsets (writing from end backwards)
	cellOffsets := make([]uint32, len(cells))
	for i := 0; i < len(cells); i++ {
		cell := cells[i]

		// Calculate cell size
		var cellBuf [1024]byte
		offset := 0

		// Write payload size
		n := PutVarint(cellBuf[offset:], uint64(len(cell.payload)))
		offset += n

		// Write rowid
		n = PutVarint(cellBuf[offset:], uint64(cell.rowid))
		offset += n

		// Write payload
		copy(cellBuf[offset:], cell.payload)
		offset += len(cell.payload)

		// Write cell to page (from end backwards)
		cellContentOffset -= uint32(offset)
		copy(data[cellContentOffset:], cellBuf[:offset])
		cellOffsets[i] = cellContentOffset
	}

	// Second pass: write cell pointers in order
	for i := 0; i < len(cells); i++ {
		binary.BigEndian.PutUint16(data[cellPtrOffset:], uint16(cellOffsets[i]))
		cellPtrOffset += 2
	}

	// Update cell content start
	binary.BigEndian.PutUint16(data[5:], uint16(cellContentOffset))

	return data
}

func TestParsePageHeader(t *testing.T) {
	tests := []struct {
		name     string
		pageNum  uint32
		data     []byte
		wantType byte
		wantLeaf bool
		wantErr  bool
	}{
		{
			name:     "leaf table page",
			pageNum:  2,
			data:     []byte{0x0d, 0, 0, 0, 1, 0, 100, 0},
			wantType: PageTypeLeafTable,
			wantLeaf: true,
			wantErr:  false,
		},
		{
			name:     "interior table page",
			pageNum:  2,
			data:     []byte{0x05, 0, 0, 0, 2, 0, 200, 0, 0, 0, 0, 5},
			wantType: PageTypeInteriorTable,
			wantLeaf: false,
			wantErr:  false,
		},
		{
			name:     "leaf index page",
			pageNum:  3,
			data:     []byte{0x0a, 0, 0, 0, 3, 0, 150, 0},
			wantType: PageTypeLeafIndex,
			wantLeaf: true,
			wantErr:  false,
		},
		{
			name:    "invalid page type",
			pageNum: 2,
			data:    []byte{0xff, 0, 0, 0, 0, 0, 0, 0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, err := ParsePageHeader(tt.data, tt.pageNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePageHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if header.PageType != tt.wantType {
				t.Errorf("PageType = 0x%02x, want 0x%02x", header.PageType, tt.wantType)
			}
			if header.IsLeaf != tt.wantLeaf {
				t.Errorf("IsLeaf = %v, want %v", header.IsLeaf, tt.wantLeaf)
			}
		})
	}
}

func TestBtreeIteratePage(t *testing.T) {
	bt := NewBtree(4096)

	// Create a test page with 3 cells
	cells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("hello")},
		{2, []byte("world")},
		{3, []byte("btree")},
	}

	// Use page 2 to avoid the 100-byte file header offset that page 1 requires
	pageData := createTestLeafPage(4096, cells)
	bt.SetPage(2, pageData)

	// Iterate and verify
	visitCount := 0
	err := bt.IteratePage(2, func(cellIndex int, cell *CellInfo) error {
		if cellIndex >= len(cells) {
			t.Errorf("Unexpected cell index: %d", cellIndex)
			return nil
		}

		expectedRowid := cells[cellIndex].rowid
		if cell.Key != expectedRowid {
			t.Errorf("Cell %d: rowid = %d, want %d", cellIndex, cell.Key, expectedRowid)
		}

		expectedPayload := cells[cellIndex].payload
		if string(cell.Payload) != string(expectedPayload) {
			t.Errorf("Cell %d: payload = %q, want %q", cellIndex, cell.Payload, expectedPayload)
		}

		visitCount++
		return nil
	})

	if err != nil {
		t.Errorf("IteratePage() error = %v", err)
	}

	if visitCount != len(cells) {
		t.Errorf("Visited %d cells, want %d", visitCount, len(cells))
	}
}

func TestNewBtree(t *testing.T) {
	tests := []struct {
		name     string
		pageSize uint32
		want     uint32
	}{
		{"default", 0, 4096},
		{"custom 1024", 1024, 1024},
		{"custom 8192", 8192, 8192},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := NewBtree(tt.pageSize)
			if bt.PageSize != tt.want {
				t.Errorf("PageSize = %d, want %d", bt.PageSize, tt.want)
			}
			if bt.Pages == nil {
				t.Error("Pages map is nil")
			}
		})
	}
}

func TestBtreeGetSetPage(t *testing.T) {
	bt := NewBtree(4096)

	// Create test page data
	pageData := make([]byte, 4096)
	pageData[0] = 0x0d // Leaf table page

	// Set page
	err := bt.SetPage(1, pageData)
	if err != nil {
		t.Fatalf("SetPage() error = %v", err)
	}

	// Get page
	retrieved, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	if len(retrieved) != len(pageData) {
		t.Errorf("Retrieved page length = %d, want %d", len(retrieved), len(pageData))
	}

	if retrieved[0] != pageData[0] {
		t.Errorf("Retrieved page type = 0x%02x, want 0x%02x", retrieved[0], pageData[0])
	}

	// Try to get non-existent page
	_, err = bt.GetPage(999)
	if err == nil {
		t.Error("GetPage(999) expected error, got nil")
	}

	// Try to set page with wrong size
	wrongSize := make([]byte, 2048)
	err = bt.SetPage(2, wrongSize)
	if err == nil {
		t.Error("SetPage() with wrong size expected error, got nil")
	}
}

// TestBtreeString tests the String method
func TestBtreeString(t *testing.T) {
	bt := NewBtree(4096)

	// Add a page
	pageData := make([]byte, 4096)
	bt.SetPage(1, pageData)

	str := bt.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	// Should contain key information
	if !contains(str, "Btree") {
		t.Errorf("String() = %q, should contain 'Btree'", str)
	}
}

// TestClearCache tests the ClearCache method
func TestClearCache(t *testing.T) {
	bt := NewBtree(4096)

	// Add some pages
	pageData1 := make([]byte, 4096)
	pageData2 := make([]byte, 4096)
	bt.SetPage(1, pageData1)
	bt.SetPage(2, pageData2)

	// Verify pages exist
	_, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage(1) before clear failed: %v", err)
	}

	// Clear cache
	bt.ClearCache()

	// Pages should be gone
	_, err = bt.GetPage(1)
	if err == nil {
		t.Error("GetPage(1) after clear should fail, got nil error")
	}
}

// TestAllocatePageWithProvider tests page allocation with a provider
func TestAllocatePageWithProvider(t *testing.T) {
	bt := NewBtree(4096)

	// Set up a mock provider
	provider := &mockPageProvider{
		pages: make(map[uint32][]byte),
		dirty: make(map[uint32]bool),
	}
	bt.Provider = provider

	// Allocate a page
	pageNum, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	if pageNum == 0 {
		t.Error("AllocatePage() returned page number 0")
	}

	// Verify page exists in provider
	if _, ok := provider.pages[pageNum]; !ok {
		t.Errorf("Page %d not found in provider", pageNum)
	}
}

// TestAllocatePageWithoutProvider tests page allocation without a provider
func TestAllocatePageWithoutProvider(t *testing.T) {
	bt := NewBtree(4096)

	// Allocate pages
	page1, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	page2, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	// Pages should be different
	if page1 == page2 {
		t.Errorf("AllocatePage() returned same page number twice: %d", page1)
	}

	// Both pages should exist
	_, err = bt.GetPage(page1)
	if err != nil {
		t.Errorf("GetPage(%d) failed: %v", page1, err)
	}

	_, err = bt.GetPage(page2)
	if err != nil {
		t.Errorf("GetPage(%d) failed: %v", page2, err)
	}
}

// TestGetPageWithProvider tests GetPage with a provider
func TestGetPageWithProvider(t *testing.T) {
	bt := NewBtree(4096)

	// Set up a mock provider
	provider := &mockPageProvider{
		pages: make(map[uint32][]byte),
		dirty: make(map[uint32]bool),
	}

	// Add a page to the provider
	pageData := make([]byte, 4096)
	pageData[0] = 0x0d
	provider.pages[5] = pageData

	bt.Provider = provider

	// Get page from provider
	retrieved, err := bt.GetPage(5)
	if err != nil {
		t.Fatalf("GetPage(5) error = %v", err)
	}

	if retrieved[0] != 0x0d {
		t.Errorf("Retrieved page type = 0x%02x, want 0x0d", retrieved[0])
	}

	// Page should now be cached
	_, err = bt.GetPage(5)
	if err != nil {
		t.Errorf("GetPage(5) from cache error = %v", err)
	}
}

// Helper function for string contains check
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
