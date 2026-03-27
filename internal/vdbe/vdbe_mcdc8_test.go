// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"os"
	"testing"
)

// newMCDC8Sorter creates a SorterWithSpill with the given config for internal tests.
func newMCDC8Sorter(cfg *SorterConfig) *SorterWithSpill {
	return NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, cfg)
}

// TestMCDC8SerializeRows_RoundTrip exercises serializeRows and deserializeRows
// directly to bring them from 0% to covered.  These methods are called by the
// WASM backend; on native platforms they are reachable only from internal tests.
func TestMCDC8SerializeRows_RoundTrip(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	rows := [][]*Mem{
		{NewMemInt(42), NewMemStr("hello")},
		{NewMemNull(), NewMemReal(3.14)},
		{NewMemInt(-1), NewMemBlob([]byte{0xDE, 0xAD, 0xBE, 0xEF})},
	}

	data, err := s.serializeRows(rows)
	if err != nil {
		t.Fatalf("serializeRows: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("serializeRows returned empty data")
	}

	got, err := s.deserializeRows(data)
	if err != nil {
		t.Fatalf("deserializeRows: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("row count: want %d, got %d", len(rows), len(got))
	}
}

// TestMCDC8SerializeRows_Empty exercises serializeRows with zero rows.
func TestMCDC8SerializeRows_Empty(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	data, err := s.serializeRows([][]*Mem{})
	if err != nil {
		t.Fatalf("serializeRows empty: %v", err)
	}

	got, err := s.deserializeRows(data)
	if err != nil {
		t.Fatalf("deserializeRows empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 rows, got %d", len(got))
	}
}

// TestMCDC8DeserializeRows_TooShort exercises the error path in deserializeRows
// when the input is shorter than the 8-byte header.
func TestMCDC8DeserializeRows_TooShort(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	_, err := s.deserializeRows([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for truncated header, got nil")
	}
}

// TestMCDC8DeserializeRows_TruncatedAtRowLength exercises the error path
// when data is cut off before a row's length field.
func TestMCDC8DeserializeRows_TruncatedAtRowLength(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	// Serialize one row, then shorten the buffer just past the 8-byte header
	// so the row-length read at offset 8 fails.
	rows := [][]*Mem{{NewMemInt(1), NewMemInt(2)}}
	data, err := s.serializeRows(rows)
	if err != nil {
		t.Fatalf("serializeRows: %v", err)
	}
	// Truncate: keep the 8-byte count header but remove the row content.
	truncated := data[:9] // only 1 byte of the 4-byte row-length word
	_, err = s.deserializeRows(truncated)
	if err == nil {
		t.Fatal("expected error for truncated row length, got nil")
	}
}

// TestMCDC8DeserializeRows_TruncatedAtRowData exercises the error path
// when data ends inside a row's payload bytes.
func TestMCDC8DeserializeRows_TruncatedAtRowData(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	rows := [][]*Mem{{NewMemStr("somevalue"), NewMemInt(99)}}
	data, err := s.serializeRows(rows)
	if err != nil {
		t.Fatalf("serializeRows: %v", err)
	}
	// Trim the last few bytes so row data is incomplete.
	truncated := data[:len(data)-3]
	_, err = s.deserializeRows(truncated)
	if err == nil {
		t.Fatal("expected error for truncated row data, got nil")
	}
}

// TestMCDC8CloseReaders_FileBackend directly calls closeReaders on a native
// SorterWithSpill, covering the no-op in spill_file.go.
func TestMCDC8CloseReaders_FileBackend(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	// closeReaders is a no-op on the file backend; calling it must not panic.
	readers := []*runReader{
		{rows: [][]*Mem{{NewMemInt(1)}}, current: 0},
		{rows: [][]*Mem{{NewMemStr("x")}}, current: 1},
	}
	s.closeReaders(readers)
}

// TestMCDC8DoSpillCurrentRun_BadDir exercises the doSpillCurrentRun error path
// that is hit when TempDir is set to an unwritable/nonexistent path.  This
// covers the remaining 25% branch in spill_file.go:doSpillCurrentRun.
func TestMCDC8DoSpillCurrentRun_BadDir(t *testing.T) {
	t.Parallel()

	cfg := &SorterConfig{
		MaxMemoryBytes: 1, // force spill on every insert
		TempDir:        "/nonexistent_dir_mcdc8_test",
		EnableSpill:    true,
	}
	s := newMCDC8Sorter(cfg)
	defer s.Close()

	// Manually stage a row so spillCurrentRun has something to work with.
	s.Sorter.Insert([]*Mem{NewMemInt(1), NewMemStr("a")})
	s.currentMemBytes = cfg.MaxMemoryBytes + 1

	err := s.doSpillCurrentRun(1)
	// On non-WASM platforms this calls writeAndRecordSpill which calls
	// os.Create on a bad path; expect an error.
	if err == nil {
		// If somehow the dir exists (unlikely), skip gracefully.
		if _, statErr := os.Stat("/nonexistent_dir_mcdc8_test"); os.IsNotExist(statErr) {
			t.Fatal("expected error for nonexistent TempDir, got nil")
		}
		t.Skip("TempDir unexpectedly exists; skipping")
	}
}

// TestMCDC8SerializeRows_AllMemTypes exercises serializeRows with every Mem
// type so that all branches in serializeMem are reachable through this path.
func TestMCDC8SerializeRows_AllMemTypes(t *testing.T) {
	t.Parallel()

	s := newMCDC8Sorter(DefaultSorterConfig())
	defer s.Close()

	rows := [][]*Mem{
		{NewMemNull(), NewMemInt(0)},
		{NewMemInt(100), NewMemReal(2.71)},
		{NewMemReal(-0.5), NewMemStr("blob_next")},
		{NewMemStr("text"), NewMemBlob([]byte{0x01, 0x02})},
		{NewMemBlob([]byte{0xCA, 0xFE}), NewMem()},
	}

	data, err := s.serializeRows(rows)
	if err != nil {
		t.Fatalf("serializeRows all types: %v", err)
	}

	got, err := s.deserializeRows(data)
	if err != nil {
		t.Fatalf("deserializeRows all types: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("row count: want %d, got %d", len(rows), len(got))
	}
}
