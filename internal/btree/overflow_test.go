package btree

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestCalculateLocalPayload tests the local payload calculation
func TestCalculateLocalPayload(t *testing.T) {
	tests := []struct {
		name       string
		totalSize  uint32
		pageSize   uint32
		isTable    bool
		wantLocal  uint16
		wantReason string
	}{
		{
			name:       "small payload fits entirely",
			totalSize:  100,
			pageSize:   4096,
			isTable:    true,
			wantLocal:  100,
			wantReason: "payload smaller than maxLocal",
		},
		{
			name:       "payload exactly at maxLocal",
			totalSize:  4061, // 4096 - 35
			pageSize:   4096,
			isTable:    true,
			wantLocal:  4061,
			wantReason: "payload equals maxLocal",
		},
		{
			name:       "large payload requires overflow",
			totalSize:  10000,
			pageSize:   4096,
			isTable:    true,
			wantLocal:  1816, // Calculated based on SQLite's algorithm
			wantReason: "large payload uses surplus calculation",
		},
		{
			name:       "index page with large payload",
			totalSize:  5000,
			pageSize:   4096,
			isTable:    false,
			wantLocal:  908, // Calculated based on SQLite's algorithm
			wantReason: "index with overflow",
		},
		{
			name:       "minimal payload",
			totalSize:  1,
			pageSize:   4096,
			isTable:    true,
			wantLocal:  1,
			wantReason: "minimal payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateLocalPayload(tt.totalSize, tt.pageSize, tt.isTable)
			if got != tt.wantLocal {
				t.Errorf("CalculateLocalPayload() = %d, want %d (%s)", got, tt.wantLocal, tt.wantReason)
			}
		})
	}
}

// TestWriteOverflowChain tests writing data to overflow pages
func TestWriteOverflowChain(t *testing.T) {
	bt := NewBtree(4096)

	tests := []struct {
		name     string
		data     []byte
		wantErr  bool
		numPages int
	}{
		{
			name:     "empty data",
			data:     []byte{},
			wantErr:  false,
			numPages: 0,
		},
		{
			name:     "small data - single page",
			data:     make([]byte, 1000),
			wantErr:  false,
			numPages: 1,
		},
		{
			name:     "data fitting exactly one page",
			data:     make([]byte, 4096-OverflowHeaderSize),
			wantErr:  false,
			numPages: 1,
		},
		{
			name:     "data requiring two pages",
			data:     make([]byte, 5000),
			wantErr:  false,
			numPages: 2,
		},
		{
			name:     "large data requiring multiple pages",
			data:     make([]byte, 15000),
			wantErr:  false,
			numPages: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill test data with pattern
			for i := range tt.data {
				tt.data[i] = byte(i % 256)
			}

			firstPage, err := writeOverflowChain(bt, tt.data, bt.UsableSize)

			if (err != nil) != tt.wantErr {
				t.Errorf("writeOverflowChain() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if len(tt.data) == 0 {
				if firstPage != 0 {
					t.Errorf("writeOverflowChain() returned page %d for empty data, want 0", firstPage)
				}
				return
			}

			// Verify the chain was created correctly
			if firstPage == 0 {
				t.Errorf("writeOverflowChain() returned page 0 for non-empty data")
				return
			}

			// Count pages in chain
			pageCount := countOverflowPages(bt, firstPage)
			if pageCount != tt.numPages {
				t.Errorf("overflow chain has %d pages, want %d", pageCount, tt.numPages)
			}
		})
	}
}

// TestReadOverflowChain tests reading data from overflow pages
func TestReadOverflowChain(t *testing.T) {
	bt := NewBtree(4096)

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "small data",
			data: []byte("Hello, World!"),
		},
		{
			name: "medium data",
			data: make([]byte, 5000),
		},
		{
			name: "large data",
			data: make([]byte, 20000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill test data with pattern
			for i := range tt.data {
				tt.data[i] = byte(i % 256)
			}

			// Write the overflow chain
			firstPage, err := writeOverflowChain(bt, tt.data, bt.UsableSize)
			if err != nil {
				t.Fatalf("writeOverflowChain() error = %v", err)
			}

			// Read it back
			readData, err := readOverflowChain(bt, firstPage, len(tt.data), bt.UsableSize)
			if err != nil {
				t.Fatalf("readOverflowChain() error = %v", err)
			}

			// Verify data matches
			if !bytes.Equal(readData, tt.data) {
				t.Errorf("readOverflowChain() returned different data")
				t.Logf("Expected %d bytes, got %d bytes", len(tt.data), len(readData))
				// Show first difference
				for i := 0; i < len(tt.data) && i < len(readData); i++ {
					if tt.data[i] != readData[i] {
						t.Logf("First difference at byte %d: expected %d, got %d", i, tt.data[i], readData[i])
						break
					}
				}
			}
		})
	}
}

// TestReadWriteOverflowRoundtrip tests complete round-trip with cursor
func TestReadWriteOverflowRoundtrip(t *testing.T) {
	bt := NewBtree(4096)

	// Create root page for cursor
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	tests := []struct {
		name        string
		payload     []byte
		localSize   uint16
		wantOverflow bool
	}{
		{
			name:        "no overflow needed",
			payload:     []byte("Short payload"),
			localSize:   13,
			wantOverflow: false,
		},
		{
			name:        "overflow needed",
			payload:     make([]byte, 5000),
			localSize:   496,
			wantOverflow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill payload with pattern
			for i := range tt.payload {
				tt.payload[i] = byte(i % 256)
			}

			// Write overflow
			firstPage, err := cursor.WriteOverflow(tt.payload, tt.localSize, bt.UsableSize)
			if err != nil {
				t.Fatalf("WriteOverflow() error = %v", err)
			}

			if tt.wantOverflow && firstPage == 0 {
				t.Error("WriteOverflow() returned 0 page number when overflow expected")
			}
			if !tt.wantOverflow && firstPage != 0 {
				t.Errorf("WriteOverflow() returned page %d when no overflow expected", firstPage)
			}

			if !tt.wantOverflow {
				return // No overflow to read
			}

			// Read back using cursor
			localPayload := tt.payload[:tt.localSize]
			completePayload, err := cursor.ReadOverflow(
				localPayload,
				firstPage,
				uint32(len(tt.payload)),
				bt.UsableSize,
			)
			if err != nil {
				t.Fatalf("ReadOverflow() error = %v", err)
			}

			// Verify data
			if !bytes.Equal(completePayload, tt.payload) {
				t.Errorf("ReadOverflow() returned different data")
				t.Logf("Expected %d bytes, got %d bytes", len(tt.payload), len(completePayload))
			}
		})
	}
}

// TestFreeOverflowChain tests freeing overflow pages
func TestFreeOverflowChain(t *testing.T) {
	bt := NewBtree(4096)

	// Create test data
	testData := make([]byte, 10000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write overflow chain
	firstPage, err := writeOverflowChain(bt, testData, bt.UsableSize)
	if err != nil {
		t.Fatalf("writeOverflowChain() error = %v", err)
	}

	// Count pages before freeing
	pagesBefore := countOverflowPages(bt, firstPage)
	if pagesBefore == 0 {
		t.Fatal("No pages in overflow chain")
	}

	// Count total pages in btree before freeing
	totalPagesBefore := len(bt.Pages)

	// Free the chain
	err = freeOverflowChain(bt, firstPage, bt.UsableSize)
	if err != nil {
		t.Fatalf("freeOverflowChain() error = %v", err)
	}

	// Verify pages were freed
	totalPagesAfter := len(bt.Pages)
	if totalPagesAfter != totalPagesBefore-pagesBefore {
		t.Errorf("Expected %d pages after freeing, got %d", totalPagesBefore-pagesBefore, totalPagesAfter)
	}

	// Verify pages are no longer accessible
	_, err = bt.GetPage(firstPage)
	if err == nil {
		t.Error("GetPage() should fail for freed page, but succeeded")
	}
}

// TestOverflowChainCorruptionDetection tests detection of corrupt chains
func TestOverflowChainCorruptionDetection(t *testing.T) {
	bt := NewBtree(4096)

	t.Run("excessive chain traversal", func(t *testing.T) {
		// Test that we handle valid long chains correctly
		// The maxPages calculation allows enough pages for the data requested
		// This test ensures we DON'T error for valid (though long) chains

		firstPage, _ := bt.AllocatePage()
		currentPage := firstPage

		// Create a chain of 10 pages (would hold ~40KB of data)
		for i := 0; i < 10; i++ {
			var nextPage uint32
			if i < 9 {
				nextPage, _ = bt.AllocatePage()
			} // Last page points to 0
			data, _ := bt.GetPage(currentPage)
			binary.BigEndian.PutUint32(data[0:4], nextPage)
			currentPage = nextPage
		}

		// Try to read 40KB of data - should need all 10 pages
		// maxPages = (40000 / 4092) + 2 = 11, so this should succeed
		data, err := readOverflowChain(bt, firstPage, 40000, bt.UsableSize)
		if err != nil {
			t.Errorf("readOverflowChain() failed for valid chain: %v", err)
		}
		if len(data) != 40000 {
			t.Errorf("readOverflowChain() returned %d bytes, want 40000", len(data))
		}
	})

	t.Run("chain too long", func(t *testing.T) {
		// Create a very long chain that claims more pages than reasonable
		firstPage, _ := bt.AllocatePage()
		currentPage := firstPage

		// Create a chain of 100 pages
		for i := 0; i < 100; i++ {
			nextPage, _ := bt.AllocatePage()
			data, _ := bt.GetPage(currentPage)
			binary.BigEndian.PutUint32(data[0:4], nextPage)
			currentPage = nextPage
		}

		// Try to read huge amount of data - should detect excessive chain
		_, err := readOverflowChain(bt, firstPage, 1000000, bt.UsableSize)
		if err == nil {
			t.Error("readOverflowChain() should detect excessively long chain")
		}
	})

	t.Run("premature chain end", func(t *testing.T) {
		// Create a short chain
		firstPage, _ := bt.AllocatePage()
		data, _ := bt.GetPage(firstPage)
		binary.BigEndian.PutUint32(data[0:4], 0) // No next page

		// Try to read more data than the chain contains
		_, err := readOverflowChain(bt, firstPage, 10000, bt.UsableSize)
		if err == nil {
			t.Error("readOverflowChain() should detect premature chain end")
		}
	})
}

// TestOverflowWithDifferentPageSizes tests overflow with various page sizes
func TestOverflowWithDifferentPageSizes(t *testing.T) {
	pageSizes := []uint32{512, 1024, 2048, 4096, 8192, 16384, 32768}

	for _, pageSize := range pageSizes {
		t.Run(string(rune(pageSize)), func(t *testing.T) {
			bt := NewBtree(pageSize)

			// Create test data larger than one page
			testData := make([]byte, int(pageSize)*3)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			// Write and read back
			firstPage, err := writeOverflowChain(bt, testData, bt.UsableSize)
			if err != nil {
				t.Fatalf("writeOverflowChain() error = %v", err)
			}

			readData, err := readOverflowChain(bt, firstPage, len(testData), bt.UsableSize)
			if err != nil {
				t.Fatalf("readOverflowChain() error = %v", err)
			}

			if !bytes.Equal(readData, testData) {
				t.Errorf("Data mismatch for page size %d", pageSize)
			}
		})
	}
}

// TestGetCompletePayload tests the convenience function
func TestGetCompletePayload(t *testing.T) {
	bt := NewBtree(4096)

	// Create a table with some test data
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Test data
	smallPayload := []byte("Small payload without overflow")
	largePayload := make([]byte, 10000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	t.Run("small payload without overflow", func(t *testing.T) {
		cursor := NewCursor(bt, rootPage)

		// Simulate a cell with no overflow
		cursor.CurrentCell = &CellInfo{
			Payload:      smallPayload,
			PayloadSize:  uint32(len(smallPayload)),
			OverflowPage: 0,
		}
		cursor.State = CursorValid

		payload, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload() error = %v", err)
		}

		if !bytes.Equal(payload, smallPayload) {
			t.Error("GetCompletePayload() returned incorrect data")
		}
	})

	t.Run("large payload with overflow", func(t *testing.T) {
		cursor := NewCursor(bt, rootPage)

		// Write overflow data
		localSize := CalculateLocalPayload(uint32(len(largePayload)), bt.PageSize, true)
		firstOverflowPage, err := writeOverflowChain(bt, largePayload[localSize:], bt.UsableSize)
		if err != nil {
			t.Fatalf("writeOverflowChain() error = %v", err)
		}

		// Simulate a cell with overflow
		cursor.CurrentCell = &CellInfo{
			Payload:      largePayload[:localSize],
			PayloadSize:  uint32(len(largePayload)),
			OverflowPage: firstOverflowPage,
		}
		cursor.State = CursorValid

		payload, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload() error = %v", err)
		}

		if !bytes.Equal(payload, largePayload) {
			t.Error("GetCompletePayload() returned incorrect data")
		}
	})

	t.Run("invalid cursor state", func(t *testing.T) {
		cursor := NewCursor(bt, rootPage)
		cursor.State = CursorInvalid

		_, err := cursor.GetCompletePayload()
		if err == nil {
			t.Error("GetCompletePayload() should fail for invalid cursor")
		}
	})
}

// Helper function to count pages in an overflow chain
func countOverflowPages(bt *Btree, firstPage uint32) int {
	if firstPage == 0 {
		return 0
	}

	count := 0
	currentPage := firstPage

	for currentPage != 0 && count < 1000 {
		count++
		pageData, err := bt.GetPage(currentPage)
		if err != nil {
			break
		}
		currentPage = binary.BigEndian.Uint32(pageData[0:4])
	}

	return count
}

// Benchmark tests
func BenchmarkWriteOverflow(b *testing.B) {
	bt := NewBtree(4096)
	data := make([]byte, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeOverflowChain(bt, data, bt.UsableSize)
	}
}

func BenchmarkReadOverflow(b *testing.B) {
	bt := NewBtree(4096)
	data := make([]byte, 10000)
	firstPage, _ := writeOverflowChain(bt, data, bt.UsableSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readOverflowChain(bt, firstPage, len(data), bt.UsableSize)
	}
}

func BenchmarkCalculateLocalPayload(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculateLocalPayload(10000, 4096, true)
	}
}
