// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"fmt"
	"testing"
)

// mockProvider is a minimal PageProvider used for writeSingleOverflowPage coverage.
type mockProvider struct {
	markDirtyErr error
	allocCount   uint32
}

func (m *mockProvider) GetPageData(pgno uint32) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockProvider) AllocatePageData() (uint32, []byte, error) {
	m.allocCount++
	return m.allocCount, make([]byte, 4096), nil
}

func (m *mockProvider) MarkDirty(pgno uint32) error {
	return m.markDirtyErr
}

// TestSafePayloadSize_OverflowCases exercises the uncovered branches of safePayloadSize.
// The function is called from CalculateLocalPayload; we drive it directly with large values
// by picking inputs that force each branch in the internal logic.
func TestSafePayloadSize_OverflowCases(t *testing.T) {
	t.Parallel()

	// Branch: size overflows uint16, fallback is 0 → returns 0.
	got := safePayloadSize(70000, 0)
	if got != 0 {
		t.Errorf("safePayloadSize(70000, 0) = %d, want 0", got)
	}

	// Branch: size overflows uint16, fallback is valid → returns fallback value.
	got = safePayloadSize(70000, 1000)
	if got != 1000 {
		t.Errorf("safePayloadSize(70000, 1000) = %d, want 1000", got)
	}

	// Branch: size overflows uint16, fallback also overflows → returns 0.
	got = safePayloadSize(70000, 70001)
	if got != 0 {
		t.Errorf("safePayloadSize(70000, 70001) = %d, want 0", got)
	}

	// Happy path: size fits → returns size.
	got = safePayloadSize(500, 0)
	if got != 500 {
		t.Errorf("safePayloadSize(500, 0) = %d, want 500", got)
	}
}

// TestSafePayloadSizeWithFallback_OverflowCases exercises the uncovered branches
// of safePayloadSizeWithFallback.
func TestSafePayloadSizeWithFallback_OverflowCases(t *testing.T) {
	t.Parallel()

	// Branch: primary overflows uint16, fallback is valid → returns fallback.
	got := safePayloadSizeWithFallback(70000, 800)
	if got != 800 {
		t.Errorf("safePayloadSizeWithFallback(70000, 800) = %d, want 800", got)
	}

	// Branch: primary overflows uint16, fallback also overflows → returns 0.
	got = safePayloadSizeWithFallback(70000, 70001)
	if got != 0 {
		t.Errorf("safePayloadSizeWithFallback(70000, 70001) = %d, want 0", got)
	}

	// Happy path: primary fits → returns primary.
	got = safePayloadSizeWithFallback(300, 800)
	if got != 300 {
		t.Errorf("safePayloadSizeWithFallback(300, 800) = %d, want 300", got)
	}
}

// TestWriteSingleOverflowPage_WithProvider exercises the Provider != nil branch in
// writeSingleOverflowPage, covering both the MarkDirty-success and MarkDirty-error paths.
func TestWriteSingleOverflowPage_WithProvider(t *testing.T) {
	t.Parallel()

	t.Run("provider marks dirty successfully", func(t *testing.T) {
		bt := NewBtree(4096)
		bt.Provider = &mockProvider{}

		// Allocate a page so GetPage can find it in the cache.
		pageNum, err := bt.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}

		data := make([]byte, 100)
		err = writeSingleOverflowPage(bt, pageNum, data, 0, 100, 0)
		if err != nil {
			t.Errorf("writeSingleOverflowPage() unexpected error: %v", err)
		}
	})

	t.Run("provider MarkDirty returns error", func(t *testing.T) {
		bt := NewBtree(4096)
		bt.Provider = &mockProvider{markDirtyErr: fmt.Errorf("disk full")}

		pageNum, err := bt.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}

		data := make([]byte, 50)
		err = writeSingleOverflowPage(bt, pageNum, data, 0, 50, 0)
		if err == nil {
			t.Error("writeSingleOverflowPage() expected error from MarkDirty, got nil")
		}
	})
}

// TestWriteSingleOverflowPage_BoundsError exercises the bounds-check error branch
// where OverflowHeaderSize+toWrite exceeds the page data length.
func TestWriteSingleOverflowPage_BoundsError(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	pageNum, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	// Request more data than the page can hold.
	data := make([]byte, 5000)
	toWrite := 5000 // OverflowHeaderSize(4) + 5000 > 4096
	err = writeSingleOverflowPage(bt, pageNum, data, 0, toWrite, 0)
	if err == nil {
		t.Error("writeSingleOverflowPage() expected bounds error, got nil")
	}
}

// TestFreeOverflowChain_NilBtree exercises the nil-Btree guard in FreeOverflowChain.
func TestFreeOverflowChain_NilBtree(t *testing.T) {
	t.Parallel()

	cursor := &BtCursor{Btree: nil}
	err := cursor.FreeOverflowChain(1)
	if err == nil {
		t.Error("FreeOverflowChain() expected error for nil Btree, got nil")
	}
}

// TestFreeOverflowChain_ZeroPage exercises the zero-page fast-return path.
func TestFreeOverflowChain_ZeroPage(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	err = cursor.FreeOverflowChain(0)
	if err != nil {
		t.Errorf("FreeOverflowChain(0) expected nil error, got %v", err)
	}
}

// TestReadSingleOverflowPage_TooSmall exercises the "page too small" error branch
// where the raw page data has fewer than OverflowHeaderSize bytes.
func TestReadSingleOverflowPage_TooSmall(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	// Inject a tiny page manually via the Pages map (bypass size validation).
	bt.mu.Lock()
	bt.Pages[99] = []byte{0x00, 0x01} // only 2 bytes, < OverflowHeaderSize(4)
	bt.mu.Unlock()

	_, _, err := readSingleOverflowPage(bt, 99, make([]byte, 10), 0, 10, 4092)
	if err == nil {
		t.Error("readSingleOverflowPage() expected 'too small' error, got nil")
	}
}

// TestReadSingleOverflowPage_BoundsError exercises the bounds-check error branch
// where OverflowHeaderSize+toRead exceeds page length.
func TestReadSingleOverflowPage_BoundsError(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	// A page with exactly OverflowHeaderSize bytes (4), so any toRead > 0 overflows.
	bt.mu.Lock()
	bt.Pages[98] = []byte{0x00, 0x00, 0x00, 0x00} // 4 bytes, just enough for header
	bt.mu.Unlock()

	result := make([]byte, 100)
	// pageCapacity = 4092 (would allow 4092 bytes), but the actual page is only 4 bytes.
	// toRead = min(100, 4092) = 100; OverflowHeaderSize(4)+100 = 104 > 4 → error.
	_, _, err := readSingleOverflowPage(bt, 98, result, 0, 100, 4092)
	if err == nil {
		t.Error("readSingleOverflowPage() expected bounds error, got nil")
	}
}

// TestWriteAndReadOverflow_SmallPageSize uses a 512-byte page (smallest valid size)
// so overflow pages are triggered sooner, exercising write/read paths with small pages.
func TestWriteAndReadOverflow_SmallPageSize(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// 3000 bytes far exceeds the 512-byte page threshold.
	payload := make([]byte, 3000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	if err := cursor.Insert(1, payload); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	found, err := cursor.SeekRowid(1)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("Row not found after insert")
	}

	if cursor.CurrentCell.OverflowPage == 0 {
		t.Fatal("Expected overflow pages for 3000-byte payload on 512-byte pages")
	}

	retrieved, err := cursor.GetCompletePayload()
	if err != nil {
		t.Fatalf("GetCompletePayload() error = %v", err)
	}
	if len(retrieved) != len(payload) {
		t.Fatalf("Payload size mismatch: got %d, want %d", len(retrieved), len(payload))
	}
	for i := range payload {
		if retrieved[i] != payload[i] {
			t.Fatalf("Payload byte %d mismatch: got %d, want %d", i, retrieved[i], payload[i])
		}
	}

	// Delete to exercise FreeOverflowChain.
	if err := cursor.Delete(); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	found, _ = cursor.SeekRowid(1)
	if found {
		t.Error("Row should be gone after delete")
	}
}
