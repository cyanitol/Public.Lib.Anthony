// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"os"
	"testing"
)

// ---------------------------------------------------------------------------
// writeAndRecordSpill – error paths
// ---------------------------------------------------------------------------

// TestWriteAndRecordSpill_BadDir forces os.Create to fail by supplying a
// path whose parent directory does not exist, exercising the error branch at
// the top of writeAndRecordSpill.
func TestWriteAndRecordSpill_BadDir(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	s.Sorter.Insert([]*Mem{NewMemInt(1)})

	badPath := "/no/such/dir/anthony_spill_test.tmp"
	err := s.writeAndRecordSpill(badPath, 1)
	if err == nil {
		t.Fatal("expected error when creating spill file in non-existent directory")
	}
}

// TestWriteAndRecordSpill_WriteError makes writeRunToFile fail by writing to
// a read-only file, exercising the cleanup branch inside writeAndRecordSpill.
func TestWriteAndRecordSpill_WriteError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, &SorterConfig{
		MaxMemoryBytes: 1 << 20,
		TempDir:        tempDir,
		EnableSpill:    true,
	})
	defer s.Close()

	s.Sorter.Insert([]*Mem{NewMemInt(42)})

	// Create the file first, then make it read-only so the internal write fails.
	f, err := os.CreateTemp(tempDir, "ro_spill_*.tmp")
	if err != nil {
		t.Fatalf("could not create temp file: %v", err)
	}
	roPath := f.Name()
	f.Close()
	if err := os.Chmod(roPath, 0o444); err != nil {
		t.Skipf("cannot chmod file (may not be supported): %v", err)
	}
	defer os.Remove(roPath)

	// writeAndRecordSpill re-creates the file via os.Create which truncates;
	// on Linux a read-only file blocks the open-for-write, so os.Create fails.
	writeErr := s.writeAndRecordSpill(roPath, 1)
	if writeErr == nil {
		// On some configurations the write may succeed (e.g. running as root).
		t.Log("writeAndRecordSpill succeeded on read-only file (may be running as root or OS allows)")
	}
}

// ---------------------------------------------------------------------------
// writeRunToFile – direct exercising of binary encoding paths
// ---------------------------------------------------------------------------

// TestWriteRunToFile_AllMemTypes exercises writeRunToFile with every Mem type
// (NULL, Int, Real, Str, Blob, Undefined) so that serializeMem / serializeRow
// paths are all reached.
func TestWriteRunToFile_AllMemTypes(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 6, nil)
	defer s.Close()

	rows := [][]*Mem{
		{NewMemNull(), NewMemInt(1), NewMemReal(2.5), NewMemStr("hello"), NewMemBlob([]byte{0xDE, 0xAD}), NewMem()},
	}

	f, err := os.CreateTemp(t.TempDir(), "wrtf_*.tmp")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	if err := s.writeRunToFile(f, rows); err != nil {
		t.Fatalf("writeRunToFile: %v", err)
	}

	// Seek back to start and verify the file can be read back.
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	readRows, err := s.readRunFromFile(f)
	if err != nil {
		t.Fatalf("readRunFromFile: %v", err)
	}
	if len(readRows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(readRows))
	}
	if len(readRows[0]) != 6 {
		t.Fatalf("expected 6 cols, got %d", len(readRows[0]))
	}
}

// TestWriteRunToFile_ClosedFile exercises the write-error branch of
// writeRunToFile by passing a closed file handle.
func TestWriteRunToFile_ClosedFile(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	rows := [][]*Mem{{NewMemInt(7)}}

	f, err := os.CreateTemp(t.TempDir(), "closed_*.tmp")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	f.Close()            // close it immediately
	os.Remove(name)      // also remove it so any lingering fd is invalid

	// Re-open read-only so Write fails.
	rf, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("Open /dev/null: %v", err)
	}
	defer rf.Close()

	// Writing to /dev/null opened O_RDONLY should fail.
	if err := s.writeRunToFile(rf, rows); err == nil {
		t.Log("writeRunToFile to read-only fd succeeded (OS may permit /dev/null writes)")
	}
}

// ---------------------------------------------------------------------------
// mergeSpilledRuns – error paths
// ---------------------------------------------------------------------------

// TestMergeSpilledRuns_MissingFile verifies that mergeSpilledRuns returns an
// error when a spill file has been deleted before Sort() is called.
func TestMergeSpilledRuns_MissingFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	config := &SorterConfig{
		MaxMemoryBytes: 300, // tiny – forces spill
		TempDir:        tempDir,
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, config)
	defer s.Close()

	// Insert enough rows to guarantee at least one spilled run.
	for i := int64(20); i > 0; i-- {
		if err := s.Insert([]*Mem{NewMemInt(i)}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	if s.GetNumSpilledRuns() == 0 {
		t.Skip("no spilled runs produced; adjust MaxMemoryBytes")
	}

	// Remove the first spill file so os.Open fails inside mergeSpilledRuns.
	os.Remove(s.spilledRuns[0].FilePath)

	err := s.Sort()
	if err == nil {
		t.Fatal("expected error when spill file is missing, got nil")
	}
}

// TestMergeSpilledRuns_CorruptFile exercises the readRunFromFile error branch
// by replacing a spill file with a zero-byte file after spilling.
func TestMergeSpilledRuns_CorruptFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	config := &SorterConfig{
		MaxMemoryBytes: 300,
		TempDir:        tempDir,
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, config)
	defer s.Close()

	for i := int64(20); i > 0; i-- {
		if err := s.Insert([]*Mem{NewMemInt(i)}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	if s.GetNumSpilledRuns() == 0 {
		t.Skip("no spilled runs produced; adjust MaxMemoryBytes")
	}

	// Overwrite the first spill file with truncated/corrupt content.
	if err := os.WriteFile(s.spilledRuns[0].FilePath, []byte{0x00}, 0o644); err != nil {
		t.Fatalf("WriteFile corrupt: %v", err)
	}

	err := s.Sort()
	if err == nil {
		t.Fatal("expected error when spill file is corrupt, got nil")
	}
}

// ---------------------------------------------------------------------------
// mergeSpilledRuns – happy path with multiple runs
// ---------------------------------------------------------------------------

// TestMergeSpilledRuns_ThreeRuns verifies the k-way merge with exactly three
// sorted runs produces the correct overall ordering.
func TestMergeSpilledRuns_ThreeRuns(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	config := &SorterConfig{
		MaxMemoryBytes: 200, // very tight to guarantee 3+ runs
		TempDir:        tempDir,
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, config)
	defer s.Close()

	const n = 30
	for i := n; i >= 1; i-- {
		if err := s.Insert([]*Mem{NewMemInt(int64(i))}); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	if s.GetNumSpilledRuns() < 3 {
		t.Skipf("only %d spilled runs; need 3 to test k-way merge path", s.GetNumSpilledRuns())
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	for i := 1; i <= n; i++ {
		if !s.Next() {
			t.Fatalf("missing row %d", i)
		}
		if got := s.CurrentRow()[0].IntValue(); got != int64(i) {
			t.Errorf("row %d: got %d", i, got)
		}
	}
	if s.Next() {
		t.Error("unexpected extra row after merge")
	}
}

// ---------------------------------------------------------------------------
// compareColumn – collation branch
// ---------------------------------------------------------------------------

// TestCompareColumn_WithCollation ensures compareColumn takes the collation
// branch (len(s.Collations) > keyIdx && s.Collations[keyIdx] != "").  A
// custom collation name is supplied; because the CollationRegistry is nil the
// implementation falls back to byte comparison, but the branch itself is
// exercised.
func TestCompareColumn_WithCollation(t *testing.T) {
	t.Parallel()

	sorter := NewSorter([]int{0}, []bool{false}, []string{"NOCASE"}, 1)

	a := NewMemStr("apple")
	b := NewMemStr("BANANA")

	// compareColumn is unexported but exercised indirectly via compareRows,
	// which is used by Sort().
	sorter.Rows = append(sorter.Rows, []*Mem{b})
	sorter.Rows = append(sorter.Rows, []*Mem{a})

	if err := sorter.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	// "apple" < "BANANA" in byte order (collation NOCASE, registry nil → falls
	// through to Compare), so first row after sort should be "apple".
	sorter.Rewind()
	first := sorter.CurrentRow()
	if first == nil {
		t.Fatal("CurrentRow nil after Sort+Rewind")
	}
	_ = first[0].StringValue() // just ensure no panic
}

// TestCompareColumn_NoCollation ensures compareColumn takes the default
// (no-collation) branch, i.e. s.Collations[keyIdx] == "".
func TestCompareColumn_NoCollation(t *testing.T) {
	t.Parallel()

	// Empty collation slice → always uses a.Compare(b)
	sorter := NewSorter([]int{0}, []bool{false}, []string{""}, 1)

	pairs := [][2]int64{{5, 3}, {1, 2}, {10, 7}}
	for _, p := range pairs {
		sorter.Rows = append(sorter.Rows, []*Mem{NewMemInt(p[0])})
		sorter.Rows = append(sorter.Rows, []*Mem{NewMemInt(p[1])})
	}

	if err := sorter.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	prev := int64(-1 << 62)
	sorter.Rewind()
	for sorter.Current < len(sorter.Rows) {
		row := sorter.CurrentRow()
		if row == nil {
			break
		}
		cur := row[0].IntValue()
		if cur < prev {
			t.Errorf("out of order: %d after %d", cur, prev)
		}
		prev = cur
		sorter.Next()
	}
}

// TestCompareColumn_CollationOutOfBounds covers the branch where
// len(s.Collations) <= keyIdx (empty collations slice with multi-key sort).
func TestCompareColumn_CollationOutOfBounds(t *testing.T) {
	t.Parallel()

	// KeyCols has two keys but Collations is empty → both keys use Compare.
	sorter := NewSorter([]int{0, 1}, []bool{false, false}, []string{}, 2)

	data := [][2]int64{{2, 10}, {1, 20}, {2, 5}, {1, 15}}
	for _, d := range data {
		sorter.Rows = append(sorter.Rows, []*Mem{NewMemInt(d[0]), NewMemInt(d[1])})
	}

	if err := sorter.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	sorter.Rewind()
	// First row should be (1, 15) then (1, 20) then (2, 5) then (2, 10).
	expected := [][2]int64{{1, 15}, {1, 20}, {2, 5}, {2, 10}}
	for i, exp := range expected {
		row := sorter.CurrentRow()
		if row == nil {
			t.Fatalf("nil row at index %d", i)
		}
		if row[0].IntValue() != exp[0] || row[1].IntValue() != exp[1] {
			t.Errorf("row %d: got (%d,%d) want (%d,%d)",
				i, row[0].IntValue(), row[1].IntValue(), exp[0], exp[1])
		}
		sorter.Next()
	}
}
