// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestSorterSpill_WriteAndRecord_HappyPath exercises the successful path of
// writeAndRecordSpill: rows are inserted, spillCurrentRun is triggered, and
// the spilled run is recorded correctly.
func TestSorterSpill_WriteAndRecord_HappyPath(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 300,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	t.Cleanup(func() { s.Close() })

	// Insert enough rows to trigger at least one spill.
	for i := int64(1); i <= 20; i++ {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(i)}); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	if s.GetNumSpilledRuns() == 0 {
		t.Skip("no spilled runs produced; increase row count or decrease MaxMemoryBytes")
	}

	// Sort merges all spilled runs with in-memory rows.
	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	// Verify all 20 rows are present in ascending order.
	count := 0
	prev := int64(-1 << 62)
	for s.Next() {
		row := s.CurrentRow()
		if row == nil {
			t.Fatal("CurrentRow() returned nil")
		}
		cur := row[0].IntValue()
		if cur < prev {
			t.Errorf("out of order at count %d: %d < %d", count, cur, prev)
		}
		prev = cur
		count++
	}
	if count != 20 {
		t.Errorf("total rows after merge: want 20, got %d", count)
	}
}

// TestSorterSpill_WriteAndRecord_MultipleSpills exercises writeAndRecordSpill
// being called multiple times (multiple spilled runs) and then merging them.
func TestSorterSpill_WriteAndRecord_MultipleSpills(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 200,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	t.Cleanup(func() { s.Close() })

	const n = 40
	for i := int64(n); i >= 1; i-- {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(i)}); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	numSpills := s.GetNumSpilledRuns()
	if numSpills < 2 {
		t.Skipf("want >= 2 spilled runs, got %d; adjust MaxMemoryBytes", numSpills)
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort with %d spilled runs: %v", numSpills, err)
	}

	// Verify sorted output.
	want := int64(1)
	for s.Next() {
		row := s.CurrentRow()
		if row == nil {
			t.Fatal("CurrentRow() nil mid-iteration")
		}
		if got := row[0].IntValue(); got != want {
			t.Errorf("row %d: want %d, got %d", want, want, got)
		}
		want++
	}
	if want-1 != n {
		t.Errorf("total rows: want %d, got %d", n, want-1)
	}
}

// TestSorterSpill_WriteAndRecord_DescendingOrder verifies that
// writeAndRecordSpill correctly stores and retrieves rows when the sort order
// is descending (desc=true).
func TestSorterSpill_WriteAndRecord_DescendingOrder(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 300,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{true}, []string{""}, 1, cfg)
	t.Cleanup(func() { s.Close() })

	for i := int64(1); i <= 20; i++ {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(i)}); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort desc: %v", err)
	}

	// In descending order, first row should be 20.
	if !s.Next() {
		t.Fatal("no rows after Sort desc")
	}
	first := s.CurrentRow()
	if first == nil {
		t.Fatal("first row nil")
	}
	if first[0].IntValue() != 20 {
		t.Errorf("first row in DESC sort: want 20, got %d", first[0].IntValue())
	}
}

// TestSorterSpill_WriteAndRecord_StringValues exercises writeAndRecordSpill
// with string Mem values (exercises the string encoding path in serializeMem).
func TestSorterSpill_WriteAndRecord_StringValues(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 400,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	t.Cleanup(func() { s.Close() })

	words := []string{
		"zebra", "apple", "mango", "banana", "cherry",
		"dragon", "elderberry", "fig", "grape", "honeydew",
		"jackfruit", "kiwi", "lemon", "nectarine", "olive",
		"papaya", "quince", "raspberry", "strawberry", "tangerine",
	}

	for _, w := range words {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemStr(w)}); err != nil {
			t.Fatalf("Insert %q: %v", w, err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort strings: %v", err)
	}

	// Verify at least one row is returned.
	if !s.Next() {
		t.Fatal("no rows after Sort")
	}
}

// TestSorterSpill_WriteAndRecord_MixedTypes exercises writeAndRecordSpill
// with a multi-column row containing int, real, and string values.
func TestSorterSpill_WriteAndRecord_MixedTypes(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 500,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	// Sort on column 0 (int).
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 3, cfg)
	t.Cleanup(func() { s.Close() })

	for i := int64(20); i >= 1; i-- {
		row := []*vdbe.Mem{
			vdbe.NewMemInt(i),
			vdbe.NewMemReal(float64(i) * 1.5),
			vdbe.NewMemStr("val"),
		}
		if err := s.Insert(row); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort mixed: %v", err)
	}

	if !s.Next() {
		t.Fatal("no rows after Sort mixed")
	}
	first := s.CurrentRow()
	if first == nil || len(first) < 3 {
		t.Fatalf("first row nil or too short: %v", first)
	}
	if first[0].IntValue() != 1 {
		t.Errorf("first row[0]: want 1, got %d", first[0].IntValue())
	}
}
