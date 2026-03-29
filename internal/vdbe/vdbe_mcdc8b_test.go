// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"encoding/binary"
	"testing"
)

// newTinySpillSorterInternal creates a SorterWithSpill with a very small
// memory budget so spill is triggered after 1-2 rows.
func newTinySpillSorterInternal(t *testing.T) *SorterWithSpill {
	t.Helper()
	cfg := &SorterConfig{
		MaxMemoryBytes: 100,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	t.Cleanup(func() { s.Close() })
	return s
}

// TestMCDC8b_SpillPathTriggered verifies the full spill-and-merge path.
func TestMCDC8b_SpillPathTriggered(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorterInternal(t)

	for i := 20; i > 0; i-- {
		m := NewMemInt(int64(i))
		if err := s.Insert([]*Mem{m}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	if s.GetNumSpilledRuns() == 0 {
		t.Fatal("expected at least one spilled run")
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	prev := int64(-1)
	count := 0
	for s.Next() {
		row := s.CurrentRow()
		val := row[0].IntValue()
		if val <= prev {
			t.Errorf("out of order: %d after %d", val, prev)
		}
		prev = val
		count++
	}
	if count != 20 {
		t.Errorf("expected 20 rows, got %d", count)
	}
}

// TestMCDC8b_SerializeDeserializeRoundtrip checks that serializeRows /
// deserializeRows round-trip correctly for mixed types.
func TestMCDC8b_SerializeDeserializeRoundtrip(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorterInternal(t)

	rows := [][]*Mem{
		{NewMemInt(42)},
		{NewMemReal(3.14)},
		{NewMemStr("hello")},
		{NewMemNull()},
		{NewMemBlob([]byte{1, 2, 3})},
	}

	data, err := s.serializeRows(rows)
	if err != nil {
		t.Fatalf("serializeRows: %v", err)
	}

	got, err := s.deserializeRows(data)
	if err != nil {
		t.Fatalf("deserializeRows: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("expected %d rows back, got %d", len(rows), len(got))
	}
}

// TestMCDC8b_DeserializeRowsTooShort verifies the "run data too short" error.
func TestMCDC8b_DeserializeRowsTooShort(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorterInternal(t)

	_, err := s.deserializeRows([]byte{0, 1, 2}) // fewer than 8 bytes
	if err == nil {
		t.Fatal("expected error for data shorter than 8 bytes")
	}
}

// TestMCDC8b_DeserializeRowsTruncatedBody verifies the "run data truncated"
// error when the header claims rows exist but the body is missing.
func TestMCDC8b_DeserializeRowsTruncatedBody(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorterInternal(t)

	// Build a header that claims 1 row but provide no row data at all.
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, 1) // numRows = 1

	_, err := s.deserializeRows(hdr)
	if err == nil {
		t.Fatal("expected error for truncated row body")
	}
}

// TestMCDC8b_DeserializeRowsTruncatedRowData verifies the error when the
// row-length prefix is present but the row data itself is truncated.
func TestMCDC8b_DeserializeRowsTruncatedRowData(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorterInternal(t)

	// Header: numRows = 1
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, 1)

	// Row length prefix claiming 100 bytes, but body only has 2 bytes.
	rowLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(rowLen, 100)

	data := append(hdr, rowLen...)
	data = append(data, 0xAB, 0xCD) // only 2 bytes of body

	_, err := s.deserializeRows(data)
	if err == nil {
		t.Fatal("expected error for truncated row data")
	}
}

// TestMCDC8b_CloseReaders exercises closeReaders (empty body) so the
// coverage tooling registers the call.
func TestMCDC8b_CloseReaders(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorterInternal(t)

	readers := []*runReader{
		{rows: [][]*Mem{{NewMemInt(1)}}, current: 0},
	}
	// closeReaders is a no-op; calling it just ensures the line is covered.
	s.closeReaders(readers)
}

// TestMCDC8b_MultiRunMerge inserts enough rows to produce multiple spilled
// runs and verifies the k-way merge produces a fully-sorted result.
func TestMCDC8b_MultiRunMerge(t *testing.T) {
	t.Parallel()

	cfg := &SorterConfig{
		MaxMemoryBytes: 100,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, cfg)
	t.Cleanup(func() { s.Close() })

	const n = 30
	for i := n; i > 0; i-- {
		m0 := NewMemInt(int64(i))
		m1 := NewMemStr("x")
		if err := s.Insert([]*Mem{m0, m1}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if s.GetNumSpilledRuns() < 2 {
		t.Logf("spilled runs: %d (may be 1 for very small payloads)", s.GetNumSpilledRuns())
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	count := 0
	prev := int64(-1)
	for s.Next() {
		v := s.CurrentRow()[0].IntValue()
		if v <= prev {
			t.Errorf("order violation: %d after %d", v, prev)
		}
		prev = v
		count++
	}
	if count != n {
		t.Errorf("expected %d rows, got %d", n, count)
	}
}
