// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSorterConfig(t *testing.T) {
	t.Parallel()

	t.Run("DefaultConfig", func(t *testing.T) {
		t.Parallel()
		config := DefaultSorterConfig()

		if config.MaxMemoryBytes != 10*1024*1024 {
			t.Errorf("Expected MaxMemoryBytes=10MB, got %d", config.MaxMemoryBytes)
		}

		if !config.EnableSpill {
			t.Error("Expected EnableSpill=true")
		}

		if config.TempDir != "" {
			t.Errorf("Expected empty TempDir, got %s", config.TempDir)
		}
	})

	t.Run("CustomConfig", func(t *testing.T) {
		t.Parallel()
		config := &SorterConfig{
			MaxMemoryBytes: 1024,
			TempDir:        "/tmp/test",
			EnableSpill:    false,
		}

		if config.MaxMemoryBytes != 1024 {
			t.Errorf("Expected MaxMemoryBytes=1024, got %d", config.MaxMemoryBytes)
		}

		if config.EnableSpill {
			t.Error("Expected EnableSpill=false")
		}
	})
}

func TestSorterWithSpill_NoSpill(t *testing.T) {
	t.Parallel()

	// Create sorter with large memory limit
	config := &SorterConfig{
		MaxMemoryBytes: 100 * 1024 * 1024, // 100 MB
		EnableSpill:    true,
	}

	sorter := NewSorterWithSpill(
		[]int{0},      // Sort by first column
		[]bool{false}, // Ascending
		[]string{""},  // Default collation
		2,             // 2 columns
		config,
	)
	defer sorter.Close()

	// Insert some rows (should fit in memory)
	testData := []struct {
		col1 int64
		col2 string
	}{
		{3, "third"},
		{1, "first"},
		{2, "second"},
	}

	for _, row := range testData {
		mem1 := NewMemInt(row.col1)
		mem2 := NewMemStr(row.col2)
		err := sorter.Insert([]*Mem{mem1, mem2})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Should have 3 rows in memory
	if len(sorter.Rows) != 3 {
		t.Errorf("Expected 3 rows in memory, got %d", len(sorter.Rows))
	}

	// Should have no spilled runs
	if sorter.GetNumSpilledRuns() != 0 {
		t.Errorf("Expected 0 spilled runs, got %d", sorter.GetNumSpilledRuns())
	}

	// Sort
	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Check sorted order
	// Don't call Rewind() - Sort() already sets Current to -1
	expectedOrder := []int64{1, 2, 3}
	for i, expected := range expectedOrder {
		if !sorter.Next() {
			t.Fatalf("Expected row %d to exist", i)
		}

		row := sorter.CurrentRow()
		if row[0].IntValue() != expected {
			t.Errorf("Row %d: expected %d, got %d", i, expected, row[0].IntValue())
		}
	}
}

func createSpillSorter(tempDir string) *SorterWithSpill {
	config := &SorterConfig{
		MaxMemoryBytes: 500,
		TempDir:        tempDir,
		EnableSpill:    true,
	}
	return NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, config)
}

func insertSpillRows(t *testing.T, sorter *SorterWithSpill) {
	t.Helper()
	for i := int64(10); i > 0; i-- {
		err := sorter.Insert([]*Mem{NewMemInt(i), NewMemStr(fmt.Sprintf("row_%d", i))})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}
}

func verifySpillSortedOrder(t *testing.T, sorter *SorterWithSpill) {
	t.Helper()
	for i := int64(1); i <= 10; i++ {
		if !sorter.Next() {
			t.Fatalf("Expected row %d to exist", i)
		}
		if row := sorter.CurrentRow(); row[0].IntValue() != i {
			t.Errorf("Row %d: expected %d, got %d", i, i, row[0].IntValue())
		}
	}
}

func countSpillFiles(tempDir string) int {
	files, _ := os.ReadDir(tempDir)
	count := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".tmp" {
			count++
		}
	}
	return count
}

func TestSorterWithSpill_SingleSpill(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	sorter := createSpillSorter(tempDir)
	defer sorter.Close()

	insertSpillRows(t, sorter)
	if sorter.GetNumSpilledRuns() == 0 {
		t.Error("Expected at least one spilled run")
	}

	if err := sorter.Sort(); err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	verifySpillSortedOrder(t, sorter)

	if n := countSpillFiles(tempDir); n > 0 {
		t.Errorf("Expected spill files to be cleaned up, found %d", n)
	}
}

func TestSorterWithSpill_MultipleSpills(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 300, // Very small to force multiple spills
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		1,
		config,
	)
	defer sorter.Close()

	// Insert 50 rows to force multiple spills
	numRows := 50
	for i := numRows; i > 0; i-- {
		mem := NewMemInt(int64(i))
		err := sorter.Insert([]*Mem{mem})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Should have multiple spilled runs
	numSpills := sorter.GetNumSpilledRuns()
	if numSpills < 2 {
		t.Errorf("Expected at least 2 spilled runs, got %d", numSpills)
	}

	// Sort (triggers k-way merge)
	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Verify all rows are sorted correctly
	// Sort() already sets Current to -1, so don't call Rewind()
	for i := 1; i <= numRows; i++ {
		if !sorter.Next() {
			t.Fatalf("Expected row %d to exist", i)
		}

		row := sorter.CurrentRow()
		if row[0].IntValue() != int64(i) {
			t.Errorf("Row %d: expected %d, got %d", i, i, row[0].IntValue())
		}
	}

	// Should have exactly numRows
	if sorter.Next() {
		t.Error("Expected no more rows after 50")
	}
}

func TestSorterWithSpill_Descending(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 400,
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{true}, // Descending
		[]string{""},
		1,
		config,
	)
	defer sorter.Close()

	// Insert rows
	for i := int64(1); i <= 20; i++ {
		mem := NewMemInt(i)
		err := sorter.Insert([]*Mem{mem})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Check descending order
	// Sort() already sets Current to -1, so don't call Rewind()
	for i := int64(20); i >= 1; i-- {
		if !sorter.Next() {
			t.Fatalf("Expected row for value %d", i)
		}

		row := sorter.CurrentRow()
		if row[0].IntValue() != i {
			t.Errorf("Expected %d, got %d", i, row[0].IntValue())
		}
	}
}

func TestSorterWithSpill_MultipleColumns(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 500, // Small limit to force spilling
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	// Sort by col0 ASC, col1 DESC
	sorter := NewSorterWithSpill(
		[]int{0, 1},
		[]bool{false, true},
		[]string{"", ""},
		3,
		config,
	)
	defer sorter.Close()

	// Insert test data: (col0, col1, col2)
	testData := []struct {
		col0 int64
		col1 int64
		col2 string
	}{
		{1, 5, "a"},
		{2, 3, "b"},
		{1, 8, "c"},
		{2, 1, "d"},
		{1, 2, "e"},
	}

	for _, row := range testData {
		err := sorter.Insert([]*Mem{
			NewMemInt(row.col0),
			NewMemInt(row.col1),
			NewMemStr(row.col2),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Expected order: col0 ASC, col1 DESC
	// (1,8,'c'), (1,5,'a'), (1,2,'e'), (2,3,'b'), (2,1,'d')
	expected := []string{"c", "a", "e", "b", "d"}

	// Sort() already sets Current to -1, so don't call Rewind()
	for i, exp := range expected {
		if !sorter.Next() {
			t.Fatalf("Expected row %d", i)
		}

		row := sorter.CurrentRow()
		if row[2].StringValue() != exp {
			t.Errorf("Row %d: expected col2='%s', got '%s'", i, exp, row[2].StringValue())
		}
	}
}

func TestSorterWithSpill_DifferentTypes(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 600,
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		3,
		config,
	)
	defer sorter.Close()

	// Insert different types
	rows := []*Mem{
		NewMemInt(42),
		NewMemReal(3.14),
		NewMemStr("hello"),
		NewMemNull(),
		NewMemBlob([]byte{1, 2, 3}),
	}

	for i, mem := range rows {
		err := sorter.Insert([]*Mem{mem, NewMemInt(int64(i)), NewMemStr(fmt.Sprintf("row%d", i))})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Just verify we can read all rows back
	// Sort() already sets Current to -1, so don't call Rewind()
	count := 0
	for sorter.Next() {
		row := sorter.CurrentRow()
		if row == nil {
			t.Fatal("CurrentRow returned nil")
		}
		count++
	}

	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}

func TestSorterWithSpill_SpillDisabled(t *testing.T) {
	t.Parallel()

	config := &SorterConfig{
		MaxMemoryBytes: 100,   // Small limit
		EnableSpill:    false, // But spill disabled
	}

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		1,
		config,
	)
	defer sorter.Close()

	// Insert many rows (would normally trigger spill)
	for i := int64(1); i <= 100; i++ {
		mem := NewMemInt(i)
		err := sorter.Insert([]*Mem{mem})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Should have NO spilled runs (spill disabled)
	if sorter.GetNumSpilledRuns() != 0 {
		t.Errorf("Expected 0 spilled runs with spill disabled, got %d", sorter.GetNumSpilledRuns())
	}

	// All rows should be in memory
	if len(sorter.Rows) != 100 {
		t.Errorf("Expected 100 rows in memory, got %d", len(sorter.Rows))
	}

	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
}

func TestSorterWithSpill_MemoryTracking(t *testing.T) {
	t.Parallel()

	config := DefaultSorterConfig()

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		2,
		config,
	)
	defer sorter.Close()

	initialMem := sorter.GetMemoryUsage()
	if initialMem != 0 {
		t.Errorf("Expected initial memory=0, got %d", initialMem)
	}

	// Insert a row
	mem1 := NewMemInt(42)
	mem2 := NewMemStr("test string value")
	err := sorter.Insert([]*Mem{mem1, mem2})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Memory usage should increase
	afterInsert := sorter.GetMemoryUsage()
	if afterInsert <= 0 {
		t.Errorf("Expected memory usage > 0 after insert, got %d", afterInsert)
	}
}

func TestSorterWithSpill_EmptySorter(t *testing.T) {
	t.Parallel()

	config := DefaultSorterConfig()

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		1,
		config,
	)
	defer sorter.Close()

	// Sort empty sorter
	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort on empty sorter failed: %v", err)
	}

	// Should have no rows
	// Sort() already sets Current to -1, so don't call Rewind()
	if sorter.Next() {
		t.Error("Expected no rows in empty sorter")
	}
}

func TestSorterWithSpill_LargeDataset(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 8 * 1024, // 8 KB
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		2,
		config,
	)
	defer sorter.Close()

	// Insert 1000 rows in reverse order
	numRows := 1000
	for i := numRows; i > 0; i-- {
		mem1 := NewMemInt(int64(i))
		mem2 := NewMemStr(fmt.Sprintf("row_data_%04d", i))
		err := sorter.Insert([]*Mem{mem1, mem2})
		if err != nil {
			t.Fatalf("Insert failed at row %d: %v", i, err)
		}
	}

	// Should have spilled multiple times
	numSpills := sorter.GetNumSpilledRuns()
	if numSpills == 0 {
		t.Error("Expected multiple spilled runs for large dataset")
	}

	t.Logf("Large dataset test: %d rows, %d spilled runs", numRows, numSpills)

	// Sort
	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Verify all rows are present and sorted
	// Sort() already sets Current to -1, so don't call Rewind()
	for i := 1; i <= numRows; i++ {
		if !sorter.Next() {
			t.Fatalf("Missing row %d", i)
		}

		row := sorter.CurrentRow()
		if row[0].IntValue() != int64(i) {
			t.Errorf("Row %d: expected %d, got %d", i, i, row[0].IntValue())
		}
	}

	if sorter.Next() {
		t.Error("Found extra rows beyond expected count")
	}
}

func TestSorterWithSpill_WithRegistry(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 500,
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	// Create sorter with registry
	sorter := NewSorterWithSpillAndRegistry(
		[]int{0},
		[]bool{false},
		[]string{""},
		1,
		nil, // No custom registry for this test
		config,
	)
	defer sorter.Close()

	// Insert and verify basic functionality
	for i := int64(5); i > 0; i-- {
		err := sorter.Insert([]*Mem{NewMemInt(i)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	err := sorter.Sort()
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	// Sort() already sets Current to -1, so don't call Rewind()
	for i := int64(1); i <= 5; i++ {
		if !sorter.Next() {
			t.Fatalf("Missing row %d", i)
		}

		row := sorter.CurrentRow()
		if row[0].IntValue() != i {
			t.Errorf("Expected %d, got %d", i, row[0].IntValue())
		}
	}
}

func TestSorterWithSpill_CleanupOnClose(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 400,
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	sorter := NewSorterWithSpill(
		[]int{0},
		[]bool{false},
		[]string{""},
		1,
		config,
	)

	// Insert rows to trigger spill
	for i := int64(1); i <= 30; i++ {
		sorter.Insert([]*Mem{NewMemInt(i)})
	}

	// Verify spill files exist
	spillsBefore := sorter.GetNumSpilledRuns()
	if spillsBefore == 0 {
		t.Error("Expected spill files to exist")
	}

	// Close should clean up
	sorter.Close()

	// Verify spill files are removed
	files, _ := os.ReadDir(tempDir)
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".tmp" {
			t.Errorf("Found spill file after close: %s", file.Name())
		}
	}
}

// Benchmark tests
func BenchmarkSorterWithSpill_NoSpill(b *testing.B) {
	config := &SorterConfig{
		MaxMemoryBytes: 100 * 1024 * 1024, // 100 MB
		EnableSpill:    true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorter := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, config)

		for j := 100; j > 0; j-- {
			sorter.Insert([]*Mem{NewMemInt(int64(j))})
		}

		sorter.Sort()
		sorter.Close()
	}
}

func BenchmarkSorterWithSpill_WithSpill(b *testing.B) {
	tempDir := b.TempDir()

	config := &SorterConfig{
		MaxMemoryBytes: 2 * 1024, // 2 KB
		TempDir:        tempDir,
		EnableSpill:    true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorter := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, config)

		for j := 100; j > 0; j-- {
			sorter.Insert([]*Mem{NewMemInt(int64(j))})
		}

		sorter.Sort()
		sorter.Close()
	}
}
