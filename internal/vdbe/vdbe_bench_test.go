// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import "testing"

// BenchmarkVDBESimpleOps benchmarks simple register operations
func BenchmarkVDBESimpleOps(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Simple program: load integers and add them
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpInteger, 42, 0, 0)
	vm.AddOp(OpInteger, 58, 1, 0)
	vm.AddOp(OpAdd, 0, 1, 2)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}

// BenchmarkVDBEResultRow benchmarks emitting result rows
func BenchmarkVDBEResultRow(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)
	vm.ResultCols = []string{"col1", "col2", "col3"}

	// Program that emits a result row
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpInteger, 1, 0, 0)
	vm.AddOp(OpInteger, 2, 1, 0)
	vm.AddOp(OpInteger, 3, 2, 0)
	vm.AddOp(OpResultRow, 0, 3, 0)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_, _ = vm.Step()
	}
}

// BenchmarkVDBEJumps benchmarks conditional jumps
func BenchmarkVDBEJumps(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Program with conditional jumps
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpInteger, 1, 0, 0)
	vm.AddOp(OpInteger, 2, 1, 0)
	vm.AddOp(OpLt, 0, 6, 1) // if reg[0] < reg[1] goto 6
	vm.AddOp(OpInteger, 100, 2, 0)
	vm.AddOp(OpGoto, 0, 7, 0)
	vm.AddOp(OpInteger, 200, 2, 0)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}

// BenchmarkVDBEStringOps benchmarks string operations
func BenchmarkVDBEStringOps(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Program with string operations
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOpWithP4Str(OpString8, 0, 0, 0, "Hello")
	vm.AddOpWithP4Str(OpString8, 0, 1, 0, "World")
	vm.AddOp(OpConcat, 0, 1, 2)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}

// BenchmarkVDBEComparison benchmarks comparison operations
func BenchmarkVDBEComparison(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Program with comparisons
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpInteger, 42, 0, 0)
	vm.AddOp(OpInteger, 42, 1, 0)
	vm.AddOp(OpEq, 0, 0, 1)
	vm.AddOp(OpInteger, 100, 2, 0)
	vm.AddOp(OpInteger, 50, 3, 0)
	vm.AddOp(OpGt, 2, 0, 3)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}

// BenchmarkVDBELoop benchmarks a simple loop
func BenchmarkVDBELoop(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Loop from 0 to 9
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpInteger, 0, 0, 0)  // counter in reg[0]
	vm.AddOp(OpInteger, 10, 1, 0) // limit in reg[1]
	// Loop start at address 3
	vm.AddOp(OpLt, 0, 6, 1)   // if counter < limit goto 6 (exit)
	vm.AddOp(OpAdd, 0, 0, 0)  // dummy operation
	vm.AddOp(OpGoto, 0, 3, 0) // goto loop start
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}

// BenchmarkVDBEMemoryAlloc benchmarks memory cell allocation
func BenchmarkVDBEMemoryAlloc(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := New()
		_ = vm.AllocMemory(100)
	}
}

// BenchmarkVDBECursorAlloc benchmarks cursor allocation
func BenchmarkVDBECursorAlloc(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := New()
		_ = vm.AllocCursors(10)
	}
}

// BenchmarkVDBEMakeRecord benchmarks record creation
func BenchmarkVDBEMakeRecord(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Program that makes a record from multiple values
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpInteger, 1, 0, 0)
	vm.AddOp(OpInteger, 2, 1, 0)
	vm.AddOpWithP4Str(OpString8, 0, 2, 0, "test")
	vm.AddOp(OpMakeRecord, 0, 3, 3)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}

// BenchmarkVDBENullOps benchmarks NULL handling
func BenchmarkVDBENullOps(b *testing.B) {
	vm := New()
	vm.AllocMemory(10)

	// Program with NULL operations
	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpNull, 0, 0, 0)
	vm.AddOp(OpNull, 0, 1, 0)
	vm.AddOp(OpIsNull, 0, 5, 0)
	vm.AddOp(OpInteger, 100, 2, 0)
	vm.AddOp(OpHalt, 0, 0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm.Reset()
		_ = vm.Run()
	}
}
