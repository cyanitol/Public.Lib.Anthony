package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
)

// SafetyTestProvider is a thread-safe mock page provider for safety tests
type SafetyTestProvider struct {
	mu         sync.RWMutex
	pages      map[uint32][]byte
	dirty      map[uint32]bool
	callCount  int
	getCalls   func(uint32) ([]byte, error)
}

func NewSafetyTestProvider() *SafetyTestProvider {
	return &SafetyTestProvider{
		pages: make(map[uint32][]byte),
		dirty: make(map[uint32]bool),
	}
}

func (m *SafetyTestProvider) GetPageData(pgno uint32) ([]byte, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()

	if m.getCalls != nil {
		return m.getCalls(pgno)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if data, ok := m.pages[pgno]; ok {
		// Return a copy to simulate disk read
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	}
	return nil, fmt.Errorf("page %d not found", pgno)
}

func (m *SafetyTestProvider) AllocatePageData() (uint32, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pgno := uint32(len(m.pages) + 1)
	data := make([]byte, 4096)
	m.pages[pgno] = data
	return pgno, data, nil
}

func (m *SafetyTestProvider) MarkDirty(pgno uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dirty[pgno] = true
	return nil
}

func (m *SafetyTestProvider) AddPage(pgno uint32, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pages[pgno] = data
}

func (m *SafetyTestProvider) GetCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.callCount
}

// TestGetPageConcurrentAccess tests concurrent access to GetPage with double-check pattern
func TestGetPageConcurrentAccess(t *testing.T) {
	bt := NewBtree(4096)
	provider := NewSafetyTestProvider()

	// Create a valid test page (use page 2, not 1, to avoid file header offset)
	validPage := createValidLeafPage(4096)
	provider.AddPage(2, validPage)
	bt.Provider = provider

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines trying to get the same page
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			page, err := bt.GetPage(2)
			if err != nil {
				t.Errorf("GetPage failed: %v", err)
				return
			}

			if len(page) != 4096 {
				t.Errorf("Expected page size 4096, got %d", len(page))
			}
		}()
	}

	wg.Wait()

	// Verify the page was only loaded once (should be in cache now)
	bt.mu.RLock()
	if _, ok := bt.Pages[2]; !ok {
		t.Error("Page should be cached after concurrent access")
	}
	bt.mu.RUnlock()
}

// TestGetPageInvalidPageNumber tests that page 0 is rejected
func TestGetPageInvalidPageNumber(t *testing.T) {
	bt := NewBtree(4096)

	_, err := bt.GetPage(0)
	if err != ErrInvalidPageNumber {
		t.Errorf("Expected ErrInvalidPageNumber, got %v", err)
	}
}

// TestGetPageDoubleCheckPattern verifies the double-check pattern works correctly
func TestGetPageDoubleCheckPattern(t *testing.T) {
	bt := NewBtree(4096)
	provider := NewSafetyTestProvider()

	// Use page 2, not 1, to avoid file header offset issues
	validPage := createValidLeafPage(4096)
	provider.AddPage(2, validPage)
	bt.Provider = provider

	// First call should load from provider
	_, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	if provider.GetCallCount() != 1 {
		t.Errorf("Expected 1 provider call, got %d", provider.GetCallCount())
	}

	// Second call should use cache
	_, err = bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	if provider.GetCallCount() != 1 {
		t.Errorf("Expected 1 provider call total, got %d", provider.GetCallCount())
	}
}

// TestPageValidation tests page validation
func TestPageValidation(t *testing.T) {
	tests := []struct {
		name      string
		setupPage func() []byte
		wantErr   bool
		errType   error
	}{
		{
			name: "valid leaf table page",
			setupPage: func() []byte {
				return createValidLeafPage(4096)
			},
			wantErr: false,
		},
		{
			name: "valid interior table page",
			setupPage: func() []byte {
				return createValidInteriorPage(4096)
			},
			wantErr: false,
		},
		{
			name: "invalid page type",
			setupPage: func() []byte {
				page := make([]byte, 4096)
				page[0] = 0xFF // Invalid page type
				return page
			},
			wantErr: true,
			errType: ErrInvalidPageType,
		},
		{
			name: "page too small",
			setupPage: func() []byte {
				return make([]byte, 100) // Too small
			},
			wantErr: true,
			errType: ErrCorruptedPage,
		},
		{
			name: "cell count exceeds page bounds",
			setupPage: func() []byte {
				page := make([]byte, 4096)
				page[0] = PageTypeLeafTable
				// Set NumCells to a huge value
				binary.BigEndian.PutUint16(page[3:5], 65535)
				return page
			},
			wantErr: true,
			errType: ErrCorruptedPage,
		},
		{
			name: "invalid cell content start",
			setupPage: func() []byte {
				page := make([]byte, 4096)
				page[0] = PageTypeLeafTable
				binary.BigEndian.PutUint16(page[3:5], 0) // NumCells = 0
				binary.BigEndian.PutUint16(page[5:7], 65535) // Invalid cell start
				return page
			},
			wantErr: true,
			errType: ErrCorruptedPage,
		},
		{
			name: "cell pointers overlap with content",
			setupPage: func() []byte {
				page := make([]byte, 4096)
				page[0] = PageTypeLeafTable
				binary.BigEndian.PutUint16(page[3:5], 100) // NumCells = 100
				binary.BigEndian.PutUint16(page[5:7], 20) // Cell content starts too early
				return page
			},
			wantErr: true,
			errType: ErrCorruptedPage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := NewBtree(4096)
			page := tt.setupPage()

			// Use page 2, not 1, to avoid file header offset issues
			err := bt.validatePage(page, 2)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestPageValidationWithProvider tests that validation is called when loading from provider
func TestPageValidationWithProvider(t *testing.T) {
	bt := NewBtree(4096)
	provider := NewSafetyTestProvider()

	// Add a corrupted page with invalid type (use page 2, not 1)
	corruptedPage := make([]byte, 4096)
	corruptedPage[0] = 0xFF // Invalid page type
	provider.AddPage(2, corruptedPage)

	bt.Provider = provider

	_, err := bt.GetPage(2)
	if err == nil {
		t.Error("Expected error for corrupted page")
	}

	// Verify the corrupted page was NOT cached
	bt.mu.RLock()
	if _, ok := bt.Pages[2]; ok {
		t.Error("Corrupted page should not be cached")
	}
	bt.mu.RUnlock()
}

// TestCursorStateValidation tests cursor state validation
func TestCursorStateValidation(t *testing.T) {
	t.Run("nil btree", func(t *testing.T) {
		cursor := &BtCursor{
			Btree:    nil,
			RootPage: 1,
			State:    CursorInvalid,
			Depth:    -1,
		}

		err := cursor.MoveToFirst()
		if err == nil {
			t.Error("Expected error for nil btree")
		}
	})

	t.Run("invalid root page", func(t *testing.T) {
		bt := NewBtree(4096)
		cursor := &BtCursor{
			Btree:    bt,
			RootPage: 0,
			State:    CursorInvalid,
			Depth:    -1,
		}

		err := cursor.MoveToFirst()
		if err == nil {
			t.Error("Expected error for invalid root page")
		}
	})

	t.Run("depth exceeded", func(t *testing.T) {
		bt := NewBtree(4096)
		cursor := &BtCursor{
			Btree:    bt,
			RootPage: 1,
			State:    CursorValid,
			Depth:    MaxBtreeDepth + 1,
		}

		err := cursor.validateCursorState()
		if err == nil {
			t.Error("Expected error for exceeded depth")
		}
	})

	t.Run("valid cursor state", func(t *testing.T) {
		bt := NewBtree(4096)
		cursor := NewCursor(bt, 1)

		err := cursor.validateCursorState()
		if err != nil {
			t.Errorf("Unexpected error for valid cursor: %v", err)
		}
	})
}

// TestCursorGetKeyWithNilBtree tests that GetKey handles nil btree safely
func TestCursorGetKeyWithNilBtree(t *testing.T) {
	cursor := &BtCursor{
		Btree: nil,
		State: CursorValid,
		CurrentCell: &CellInfo{
			Key: 123,
		},
	}

	key := cursor.GetKey()
	if key != 0 {
		t.Errorf("Expected 0 for nil btree, got %d", key)
	}
}

// TestCursorGetPayloadWithNilBtree tests that GetPayload handles nil btree safely
func TestCursorGetPayloadWithNilBtree(t *testing.T) {
	cursor := &BtCursor{
		Btree: nil,
		State: CursorValid,
		CurrentCell: &CellInfo{
			Payload: []byte("test"),
		},
	}

	payload := cursor.GetPayload()
	if payload != nil {
		t.Errorf("Expected nil for nil btree, got %v", payload)
	}
}

// TestConcurrentPageAccess tests concurrent access to different pages
func TestConcurrentPageAccess(t *testing.T) {
	bt := NewBtree(4096)
	provider := NewSafetyTestProvider()

	// Create multiple valid pages (use pages 2-11, not 1, to avoid file header offset)
	for i := uint32(2); i <= 11; i++ {
		validPage := createValidLeafPage(4096)
		provider.AddPage(i, validPage)
	}
	bt.Provider = provider

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			// Access random pages (2-11)
			pageNum := uint32((id % 10) + 2)
			_, err := bt.GetPage(pageNum)
			if err != nil {
				t.Errorf("GetPage(%d) failed: %v", pageNum, err)
			}
		}(i)
	}

	wg.Wait()

	// All pages should be cached
	bt.mu.RLock()
	if len(bt.Pages) != 10 {
		t.Errorf("Expected 10 cached pages, got %d", len(bt.Pages))
	}
	bt.mu.RUnlock()
}

// Helper functions

func createValidLeafPage(size uint32) []byte {
	page := make([]byte, size)
	page[0] = PageTypeLeafTable
	// FirstFreeblock = 0
	binary.BigEndian.PutUint16(page[1:3], 0)
	// NumCells = 0
	binary.BigEndian.PutUint16(page[3:5], 0)
	// CellContentStart = 0 (means end of page)
	binary.BigEndian.PutUint16(page[5:7], 0)
	// FragmentedBytes = 0
	page[7] = 0
	return page
}

func createValidInteriorPage(size uint32) []byte {
	page := make([]byte, size)
	page[0] = PageTypeInteriorTable
	// FirstFreeblock = 0
	binary.BigEndian.PutUint16(page[1:3], 0)
	// NumCells = 0
	binary.BigEndian.PutUint16(page[3:5], 0)
	// CellContentStart = 0
	binary.BigEndian.PutUint16(page[5:7], 0)
	// FragmentedBytes = 0
	page[7] = 0
	// RightChild = 2 (valid page number)
	binary.BigEndian.PutUint32(page[8:12], 2)
	return page
}

// TestRaceConditionInGetPage specifically tests for race conditions
func TestRaceConditionInGetPage(t *testing.T) {
	bt := NewBtree(4096)
	provider := NewSafetyTestProvider()

	// Use page 2, not 1, to avoid file header offset issues
	validPage := createValidLeafPage(4096)
	provider.AddPage(2, validPage)
	bt.Provider = provider

	// Multiple goroutines racing to load the same page
	const numGoroutines = 1000
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make([][]byte, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			page, err := bt.GetPage(2)
			if err != nil {
				t.Errorf("GetPage failed: %v", err)
				return
			}
			results[idx] = page
		}(i)
	}

	wg.Wait()

	// All results should be identical (same cached page)
	for i := 1; i < len(results); i++ {
		if !bytes.Equal(results[0], results[i]) {
			t.Error("Race condition detected: different pages returned")
		}
	}

	// Should only be one copy in cache
	bt.mu.RLock()
	cachedPage := bt.Pages[2]
	bt.mu.RUnlock()

	for i := 0; i < len(results); i++ {
		if !bytes.Equal(cachedPage, results[i]) {
			t.Error("Cached page doesn't match returned page")
		}
	}
}
