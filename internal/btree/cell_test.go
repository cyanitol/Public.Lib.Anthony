package btree

import (
	"testing"
)

// TestCellInfoString tests the String method
func TestCellInfoString(t *testing.T) {
	t.Parallel()
	cell := &CellInfo{
		Key:          123,
		PayloadSize:  100,
		LocalPayload: 50,
		CellSize:     55,
		OverflowPage: 0,
		ChildPage:    0,
	}

	str := cell.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	// Should contain key information (case may vary - check for "key" or "Key")
	if (!contains(str, "Key") && !contains(str, "key")) || !contains(str, "123") {
		t.Errorf("String() = %q, should contain key information and '123'", str)
	}
}

// TestEncodeIndexInteriorCell tests encoding index interior cells
func TestEncodeIndexInteriorCell(t *testing.T) {
	t.Parallel()
	// Create a test payload
	payload := []byte("test index key")

	// Encode the cell
	cell := EncodeIndexInteriorCell(123, payload)

	if len(cell) == 0 {
		t.Error("EncodeIndexInteriorCell() returned empty cell")
	}

	// Parse it back
	cellInfo, err := ParseCell(PageTypeInteriorIndex, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell() error = %v", err)
	}

	if cellInfo.ChildPage != 123 {
		t.Errorf("ChildPage = %d, want 123", cellInfo.ChildPage)
	}

	if cellInfo.PayloadSize != uint32(len(payload)) {
		t.Errorf("PayloadSize = %d, want %d", cellInfo.PayloadSize, len(payload))
	}

	if string(cellInfo.Payload) != string(payload) {
		t.Errorf("Payload = %q, want %q", cellInfo.Payload, payload)
	}
}

// TestParseIndexInteriorCell tests parsing index interior cells
func TestParseIndexInteriorCell(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		childPage   uint32
		payload     []byte
		usableSize  uint32
		wantErr     bool
	}{
		{
			name:       "small payload",
			childPage:  42,
			payload:    []byte("small"),
			usableSize: 4096,
			wantErr:    false,
		},
		{
			name:       "empty payload",
			childPage:  1,
			payload:    []byte{},
			usableSize: 4096,
			wantErr:    false,
		},
		{
			name:       "large payload",
			childPage:  999,
			payload:    make([]byte, 100),
			usableSize: 4096,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Encode the cell
			cell := EncodeIndexInteriorCell(tt.childPage, tt.payload)

			// Parse it
			cellInfo, err := ParseCell(PageTypeInteriorIndex, cell, tt.usableSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCell() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			if cellInfo.ChildPage != tt.childPage {
				t.Errorf("ChildPage = %d, want %d", cellInfo.ChildPage, tt.childPage)
			}

			if cellInfo.PayloadSize != uint32(len(tt.payload)) {
				t.Errorf("PayloadSize = %d, want %d", cellInfo.PayloadSize, len(tt.payload))
			}
		})
	}
}

// TestParseTableInteriorCellError tests error handling
func TestParseTableInteriorCellError(t *testing.T) {
	t.Parallel()
	// Test with too small data
	tooSmall := []byte{0x01, 0x02}
	_, err := ParseCell(PageTypeInteriorTable, tooSmall, 4096)
	if err == nil {
		t.Error("ParseCell() with too small data should fail, got nil")
	}
}

// TestParseInvalidPageType tests parsing with invalid page type
func TestParseInvalidPageType(t *testing.T) {
	t.Parallel()
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	_, err := ParseCell(0xFF, data, 4096)
	if err == nil {
		t.Error("ParseCell() with invalid page type should fail, got nil")
	}
}

// TestCalculateLocalPayloadValues tests local payload calculation
func TestCalculateLocalPayloadValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		payloadSize uint32
		usableSize  uint32
		isLeaf      bool
	}{
		{
			name:        "small payload leaf",
			payloadSize: 100,
			usableSize:  4096,
			isLeaf:      true,
		},
		{
			name:        "large payload leaf",
			payloadSize: 5000,
			usableSize:  4096,
			isLeaf:      true,
		},
		{
			name:        "small payload interior",
			payloadSize: 100,
			usableSize:  4096,
			isLeaf:      false,
		},
		{
			name:        "large payload interior",
			payloadSize: 5000,
			usableSize:  1024,
			isLeaf:      false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			local := CalculateLocalPayload(tt.payloadSize, tt.usableSize, tt.isLeaf)

			// Local should never exceed usable size
			if uint32(local) > tt.usableSize {
				t.Errorf("CalculateLocalPayload() = %d, exceeds usable size %d", local, tt.usableSize)
			}

			// Local should never exceed payload size
			if uint32(local) > tt.payloadSize {
				t.Errorf("CalculateLocalPayload() = %d, exceeds payload size %d", local, tt.payloadSize)
			}

			// Local should be positive for non-zero payload
			if tt.payloadSize > 0 && local <= 0 {
				t.Errorf("CalculateLocalPayload() = %d, should be positive", local)
			}
		})
	}
}

// TestEncodeTableLeafCellWithOverflow tests encoding cells that need overflow
func TestEncodeTableLeafCellWithOverflow(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Create large enough payload to require overflow
	payload := make([]byte, 5000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Calculate how much fits locally
	localSize := CalculateLocalPayload(uint32(len(payload)), 4096, true)
	overflowPage := uint32(42)
	totalSize := uint32(len(payload))

	// Use the actual local payload
	cell := cursor.encodeTableLeafCellWithOverflow(100, payload[:localSize], overflowPage, totalSize)

	if len(cell) == 0 {
		t.Error("encodeTableLeafCellWithOverflow() returned empty cell")
	}

	// Parse it
	cellInfo, err := ParseCell(PageTypeLeafTable, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell() error = %v", err)
	}

	if cellInfo.Key != 100 {
		t.Errorf("Key = %d, want 100", cellInfo.Key)
	}

	if cellInfo.PayloadSize != totalSize {
		t.Errorf("PayloadSize = %d, want %d", cellInfo.PayloadSize, totalSize)
	}

	if cellInfo.OverflowPage != overflowPage {
		t.Errorf("OverflowPage = %d, want %d", cellInfo.OverflowPage, overflowPage)
	}
}

// TestParseTableLeafCellWithOverflow tests parsing cells with overflow
func TestParseTableLeafCellWithOverflow(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Insert large payload that requires overflow
	largePayload := make([]byte, 6000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	err := cursor.Insert(200, largePayload)
	if err != nil {
		t.Fatalf("Insert large payload failed: %v", err)
	}

	// Seek to it
	found, err := cursor.SeekRowid(200)
	if err != nil {
		t.Fatalf("SeekRowid failed: %v", err)
	}

	if !found {
		t.Fatal("SeekRowid did not find key")
	}

	// Current cell should have overflow
	if cursor.CurrentCell == nil {
		t.Fatal("CurrentCell is nil")
	}

	if cursor.CurrentCell.PayloadSize != uint32(len(largePayload)) {
		t.Errorf("PayloadSize = %d, want %d", cursor.CurrentCell.PayloadSize, len(largePayload))
	}

	if cursor.CurrentCell.OverflowPage == 0 {
		t.Error("OverflowPage should not be 0 for large payload")
	}
}

// TestEncodeIndexLeafCellLarge tests encoding large index leaf cells
func TestEncodeIndexLeafCellLarge(t *testing.T) {
	t.Parallel()
	// Create large payload
	payload := make([]byte, 8000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	cell := EncodeIndexLeafCell(payload)

	if len(cell) == 0 {
		t.Error("EncodeIndexLeafCell() returned empty cell")
	}

	// Parse it
	cellInfo, err := ParseCell(PageTypeLeafIndex, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell() error = %v", err)
	}

	if cellInfo.PayloadSize != uint32(len(payload)) {
		t.Errorf("PayloadSize = %d, want %d", cellInfo.PayloadSize, len(payload))
	}

	// For large payloads, should have overflow
	if cellInfo.PayloadSize > 4000 && cellInfo.LocalPayload == uint16(cellInfo.PayloadSize) {
		t.Error("Large payload should not fit entirely locally")
	}
}

// TestCalculateMaxLocal tests max local calculation
func TestCalculateMaxLocal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		usableSize uint32
		isLeaf     bool
	}{
		{4096, true},
		{4096, false},
		{1024, true},
		{1024, false},
		{8192, true},
		{8192, false},
	}

	for _, tt := range tests {
		tt := tt
		maxLocal := calculateMaxLocal(tt.usableSize, tt.isLeaf)

		// Should be positive
		if maxLocal <= 0 {
			t.Errorf("calculateMaxLocal(%d, %v) = %d, should be positive", tt.usableSize, tt.isLeaf, maxLocal)
		}

		// Should not exceed usable size
		if maxLocal > tt.usableSize {
			t.Errorf("calculateMaxLocal(%d, %v) = %d, exceeds usable size", tt.usableSize, tt.isLeaf, maxLocal)
		}
	}
}

// TestCalculateMinLocal tests min local calculation
func TestCalculateMinLocal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		usableSize uint32
		isLeaf     bool
	}{
		{4096, true},
		{4096, false},
		{1024, true},
		{1024, false},
	}

	for _, tt := range tests {
		tt := tt
		minLocal := calculateMinLocal(tt.usableSize, tt.isLeaf)

		// Should be positive
		if minLocal <= 0 {
			t.Errorf("calculateMinLocal(%d, %v) = %d, should be positive", tt.usableSize, tt.isLeaf, minLocal)
		}

		// Min should be less than or equal to max
		maxLocal := calculateMaxLocal(tt.usableSize, tt.isLeaf)
		if minLocal > maxLocal {
			t.Errorf("calculateMinLocal(%d, %v) = %d, exceeds max local %d", tt.usableSize, tt.isLeaf, minLocal, maxLocal)
		}
	}
}
