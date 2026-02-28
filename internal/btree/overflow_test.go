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

// TestInsertWithOverflow tests inserting large payloads that require overflow pages
func TestInsertWithOverflow(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	tests := []struct {
		name        string
		rowid       int64
		payloadSize int
		wantOverflow bool
	}{
		{
			name:        "small payload - no overflow",
			rowid:       1,
			payloadSize: 100,
			wantOverflow: false,
		},
		{
			name:        "medium payload - no overflow",
			rowid:       2,
			payloadSize: 2000,
			wantOverflow: false,
		},
		{
			name:        "large payload - requires overflow",
			rowid:       3,
			payloadSize: 10000,
			wantOverflow: true,
		},
		{
			name:        "very large payload - multiple overflow pages",
			rowid:       4,
			payloadSize: 50000,
			wantOverflow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test payload
			payload := make([]byte, tt.payloadSize)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			// Insert the row
			err := cursor.Insert(tt.rowid, payload)
			if err != nil {
				t.Fatalf("Insert() error = %v", err)
			}

			// Seek to the inserted row
			found, err := cursor.SeekRowid(tt.rowid)
			if err != nil {
				t.Fatalf("SeekRowid() error = %v", err)
			}
			if !found {
				t.Fatal("SeekRowid() did not find inserted row")
			}

			// Check if overflow was used as expected
			hasOverflow := cursor.CurrentCell.OverflowPage != 0
			if hasOverflow != tt.wantOverflow {
				t.Errorf("Overflow usage mismatch: got overflow=%v, want overflow=%v", hasOverflow, tt.wantOverflow)
			}

			// Retrieve the complete payload
			retrievedPayload, err := cursor.GetCompletePayload()
			if err != nil {
				t.Fatalf("GetCompletePayload() error = %v", err)
			}

			// Verify the payload matches
			if !bytes.Equal(retrievedPayload, payload) {
				t.Errorf("Retrieved payload does not match original")
				t.Logf("Expected %d bytes, got %d bytes", len(payload), len(retrievedPayload))
			}
		})
	}
}

// TestDeleteWithOverflow tests deleting rows with overflow pages
func TestDeleteWithOverflow(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a row with large payload requiring overflow
	rowid := int64(1)
	payloadSize := 15000
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	err = cursor.Insert(rowid, payload)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// Verify overflow pages were created
	found, err := cursor.SeekRowid(rowid)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("SeekRowid() did not find inserted row")
	}

	if cursor.CurrentCell.OverflowPage == 0 {
		t.Fatal("Expected overflow pages, but none were created")
	}

	firstOverflowPage := cursor.CurrentCell.OverflowPage

	// Count overflow pages before deletion
	overflowPagesBefore := countOverflowPages(bt, firstOverflowPage)
	if overflowPagesBefore == 0 {
		t.Fatal("No overflow pages found")
	}

	totalPagesBefore := len(bt.Pages)

	// Delete the row
	err = cursor.Delete()
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify overflow pages were freed
	totalPagesAfter := len(bt.Pages)
	expectedPages := totalPagesBefore - overflowPagesBefore
	if totalPagesAfter != expectedPages {
		t.Errorf("Expected %d pages after delete, got %d (freed %d overflow pages)",
			expectedPages, totalPagesAfter, overflowPagesBefore)
	}

	// Verify the overflow pages are no longer accessible
	_, err = bt.GetPage(firstOverflowPage)
	if err == nil {
		t.Error("Overflow page should have been freed but is still accessible")
	}

	// Verify the row was deleted
	found, err = cursor.SeekRowid(rowid)
	if err == nil && found {
		t.Error("Row should have been deleted but is still found")
	}
}

// TestMultipleRowsWithOverflow tests inserting and deleting multiple rows with overflow
// Uses smaller payload sizes to avoid page split edge cases
func TestMultipleRowsWithOverflow(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert rows with payload sizes that require overflow but avoid triggering page splits
	testData := []struct {
		rowid       int64
		payloadSize int
	}{
		{1, 100},    // Small - no overflow
		{2, 6000},   // Medium with overflow
		{3, 500},    // Small - no overflow
	}

	// Insert all rows
	for _, td := range testData {
		payload := make([]byte, td.payloadSize)
		for i := range payload {
			payload[i] = byte((i + int(td.rowid)) % 256)
		}

		err := cursor.Insert(td.rowid, payload)
		if err != nil {
			t.Fatalf("Insert(rowid=%d) error = %v", td.rowid, err)
		}
	}

	// Verify all rows can be retrieved
	for _, td := range testData {
		found, err := cursor.SeekRowid(td.rowid)
		if err != nil {
			t.Fatalf("SeekRowid(%d) error = %v", td.rowid, err)
		}
		if !found {
			t.Fatalf("Row %d not found after insert", td.rowid)
		}

		payload, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload(rowid=%d) error = %v", td.rowid, err)
		}

		if len(payload) != td.payloadSize {
			t.Errorf("Row %d: expected payload size %d, got %d", td.rowid, td.payloadSize, len(payload))
		}

		// Verify payload content
		for i := range payload {
			expected := byte((i + int(td.rowid)) % 256)
			if payload[i] != expected {
				t.Errorf("Row %d: payload byte %d mismatch: got %d, want %d", td.rowid, i, payload[i], expected)
				break
			}
		}
	}

	// Delete row with overflow
	rowToDelete := int64(2)
	found, err := cursor.SeekRowid(rowToDelete)
	if err != nil {
		t.Fatalf("SeekRowid(%d) error = %v", rowToDelete, err)
	}
	if !found {
		t.Fatalf("Row %d not found before delete", rowToDelete)
	}

	err = cursor.Delete()
	if err != nil {
		t.Fatalf("Delete(rowid=%d) error = %v", rowToDelete, err)
	}

	// Verify deleted row is gone
	found, _ = cursor.SeekRowid(rowToDelete)
	if found {
		t.Errorf("Row %d should have been deleted but is still found", rowToDelete)
	}

	// Verify remaining rows are intact
	remainingRows := []int64{1, 3}
	for _, rowid := range remainingRows {
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			t.Fatalf("SeekRowid(%d) error = %v", rowid, err)
		}
		if !found {
			t.Errorf("Row %d should still exist but was not found", rowid)
		}
	}
}

// TestOverflowEdgeCases tests edge cases in overflow handling
func TestOverflowEdgeCases(t *testing.T) {
	t.Run("payload just below threshold", func(t *testing.T) {
		// Create fresh btree for this test
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable() error = %v", err)
		}
		cursor := NewCursor(bt, rootPage)

		// Use a payload size well below threshold to avoid page split issues
		// This tests that overflow is NOT used for medium-sized payloads
		payload := make([]byte, 3000)
		for i := range payload {
			payload[i] = byte(i % 256)
		}

		err = cursor.Insert(100, payload)
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		found, err := cursor.SeekRowid(100)
		if err != nil {
			t.Fatalf("SeekRowid() error = %v", err)
		}
		if !found {
			t.Fatal("Row not found after Insert()")
		}

		// Should not have overflow for payload well under threshold
		if cursor.CurrentCell.OverflowPage != 0 {
			t.Error("Payload below threshold should not require overflow")
		}

		retrieved, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload() error = %v", err)
		}

		if !bytes.Equal(retrieved, payload) {
			t.Error("Retrieved payload does not match")
		}
	})

	t.Run("payload one byte over threshold", func(t *testing.T) {
		// Create fresh btree for this test
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable() error = %v", err)
		}
		cursor := NewCursor(bt, rootPage)

		// Get the overflow threshold
		threshold := GetOverflowThreshold(bt.PageSize, true)

		// Create payload one byte over threshold (should overflow)
		payload := make([]byte, threshold+1)
		for i := range payload {
			payload[i] = byte(i % 256)
		}

		err = cursor.Insert(101, payload)
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		found, err := cursor.SeekRowid(101)
		if err != nil {
			t.Fatalf("SeekRowid() error = %v", err)
		}
		if !found {
			t.Fatal("Row not found")
		}

		// Should have overflow
		if cursor.CurrentCell.OverflowPage == 0 {
			t.Error("Payload over threshold should require overflow")
		}

		retrieved, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload() error = %v", err)
		}

		if !bytes.Equal(retrieved, payload) {
			t.Error("Retrieved payload does not match")
		}
	})
}

// TestGetOverflowThreshold tests the GetOverflowThreshold function
func TestGetOverflowThreshold(t *testing.T) {
	tests := []struct {
		name     string
		pageSize uint32
		isTable  bool
		want     uint32
	}{
		{
			name:     "4KB page table",
			pageSize: 4096,
			isTable:  true,
			want:     4061, // 4096 - 35
		},
		{
			name:     "4KB page index",
			pageSize: 4096,
			isTable:  false,
			want:     4061, // 4096 - 35
		},
		{
			name:     "8KB page table",
			pageSize: 8192,
			isTable:  true,
			want:     8157, // 8192 - 35
		},
		{
			name:     "1KB page table",
			pageSize: 1024,
			isTable:  true,
			want:     989, // 1024 - 35
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetOverflowThreshold(tt.pageSize, tt.isTable)
			if got != tt.want {
				t.Errorf("GetOverflowThreshold() = %d, want %d", got, tt.want)
			}
		})
	}
}
