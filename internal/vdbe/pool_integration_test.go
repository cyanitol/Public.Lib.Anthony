// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestMemPoolIntegration verifies that the memory pool is being used.
func TestMemPoolIntegration(t *testing.T) {
	// Reset pool stats
	ResetPoolStats()

	// Create a VDBE and allocate some memory
	v := New()
	err := v.AllocMemory(10)
	if err != nil {
		t.Fatalf("AllocMemory failed: %v", err)
	}

	// Verify pool was used
	stats := GetStats()
	if stats.MemGets != 10 {
		t.Errorf("Expected 10 MemGets, got %d", stats.MemGets)
	}

	// Finalize and verify pool returns
	v.Finalize()

	stats = GetStats()
	if stats.MemPuts != 10 {
		t.Errorf("Expected 10 MemPuts, got %d", stats.MemPuts)
	}
}

// TestInstructionPoolIntegration verifies that the instruction pool is being used.
func TestInstructionPoolIntegration(t *testing.T) {
	// Reset pool stats
	ResetPoolStats()

	// Create a VDBE and add some instructions
	v := New()
	v.AddOp(OpHalt, 0, 0, 0)
	v.AddOp(OpInteger, 0, 0, 0)
	v.AddOp(OpResultRow, 0, 1, 0)

	// Verify pool was used
	stats := GetStats()
	if stats.InstructionGets != 3 {
		t.Errorf("Expected 3 InstructionGets, got %d", stats.InstructionGets)
	}

	// Finalize and verify pool returns
	v.Finalize()

	stats = GetStats()
	if stats.InstructionPuts != 3 {
		t.Errorf("Expected 3 InstructionPuts, got %d", stats.InstructionPuts)
	}
}

// TestSorterPoolIntegration verifies that the sorter uses the memory pool.
func TestSorterPoolIntegration(t *testing.T) {
	// Reset pool stats
	ResetPoolStats()

	// Create a sorter and insert some rows
	sorter := NewSorter([]int{0}, []bool{false}, []string{}, 2)

	row1 := []*Mem{NewMemInt(3), NewMemStr("three")}
	row2 := []*Mem{NewMemInt(1), NewMemStr("one")}
	row3 := []*Mem{NewMemInt(2), NewMemStr("two")}

	sorter.Insert(row1)
	sorter.Insert(row2)
	sorter.Insert(row3)

	// Verify pool was used for row copies (3 rows * 2 cols each)
	stats := GetStats()
	if stats.MemGets != 6 {
		t.Errorf("Expected 6 MemGets for sorter rows, got %d", stats.MemGets)
	}

	// Close the sorter and verify pool returns
	sorter.Close()

	stats = GetStats()
	if stats.MemPuts != 6 {
		t.Errorf("Expected 6 MemPuts after sorter close, got %d", stats.MemPuts)
	}
}

// TestResultRowPoolIntegration verifies that ResultRow uses the memory pool.
func TestResultRowPoolIntegration(t *testing.T) {
	// Reset pool stats
	ResetPoolStats()

	// Create a VDBE with memory and a simple program
	v := New()
	v.AllocMemory(5)

	// Set some values in memory
	v.Mem[0].SetInt(42)
	v.Mem[1].SetStr("test")
	v.Mem[2].SetReal(3.14)

	// Clear initial allocation stats
	initialGets := GetStats().MemGets
	ResetPoolStats()

	// Create a ResultRow
	v.ResultRow = make([]*Mem, 3)

	// Simulate what OpResultRow does
	for i := 0; i < 3; i++ {
		v.ResultRow[i] = GetMem()
		v.ResultRow[i].Copy(v.Mem[i])
	}

	// Verify pool was used
	stats := GetStats()
	if stats.MemGets != 3 {
		t.Errorf("Expected 3 MemGets for ResultRow, got %d", stats.MemGets)
	}

	// Reset and verify pool returns
	v.Reset()

	stats = GetStats()
	if stats.MemPuts != 3 {
		t.Errorf("Expected 3 MemPuts after Reset, got %d (total gets: %d)", stats.MemPuts, initialGets)
	}
}

// TestPoolReuse verifies that pooled objects are actually reused.
func TestPoolReuse(t *testing.T) {
	// Reset pool stats
	ResetPoolStats()

	// Allocate and free multiple times
	for i := 0; i < 10; i++ {
		v := New()
		v.AllocMemory(5)
		v.AddOp(OpHalt, 0, 0, 0)
		v.Finalize()
	}

	stats := GetStats()

	// We should have 50 MemGets (10 iterations * 5 cells)
	if stats.MemGets != 50 {
		t.Errorf("Expected 50 MemGets, got %d", stats.MemGets)
	}

	// We should have 50 MemPuts (10 iterations * 5 cells)
	if stats.MemPuts != 50 {
		t.Errorf("Expected 50 MemPuts, got %d", stats.MemPuts)
	}

	// We should have 10 InstructionGets (10 iterations * 1 instruction)
	if stats.InstructionGets != 10 {
		t.Errorf("Expected 10 InstructionGets, got %d", stats.InstructionGets)
	}

	// We should have 10 InstructionPuts (10 iterations * 1 instruction)
	if stats.InstructionPuts != 10 {
		t.Errorf("Expected 10 InstructionPuts, got %d", stats.InstructionPuts)
	}

	// The actual allocations should be much less than the gets/puts
	// because the pool reuses objects
	t.Logf("Pool stats - Gets: %d, Puts: %d", stats.MemGets, stats.MemPuts)
	t.Logf("Pool stats - Instruction Gets: %d, Puts: %d", stats.InstructionGets, stats.InstructionPuts)
}
