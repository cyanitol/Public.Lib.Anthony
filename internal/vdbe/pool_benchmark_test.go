package vdbe

import (
	"testing"
)

// BenchmarkVDBEWithPool benchmarks VDBE execution with pooling enabled.
func BenchmarkVDBEWithPool(b *testing.B) {
	ResetPoolStats()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := New()
		v.AllocMemory(10)

		// Simulate some operations
		v.AddOp(OpInteger, 42, 0, 0)
		v.AddOp(OpString, 0, 1, 0)
		v.AddOp(OpReal, 0, 2, 0)
		v.AddOp(OpResultRow, 0, 3, 0)
		v.AddOp(OpHalt, 0, 0, 0)

		// Set some values
		v.Mem[0].SetInt(42)
		v.Mem[1].SetStr("test")
		v.Mem[2].SetReal(3.14)

		// Finalize to return to pool
		v.Finalize()
	}
	b.StopTimer()

	stats := GetStats()
	b.ReportMetric(float64(stats.MemGets)/float64(b.N), "mem_gets/op")
	b.ReportMetric(float64(stats.InstructionGets)/float64(b.N), "instr_gets/op")
}

// BenchmarkSorterWithPool benchmarks sorter operations with pooling.
func BenchmarkSorterWithPool(b *testing.B) {
	ResetPoolStats()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorter := NewSorter([]int{0}, []bool{false}, []string{}, 2)

		// Insert some rows
		for j := 0; j < 10; j++ {
			row := []*Mem{NewMemInt(int64(j)), NewMemStr("test")}
			sorter.Insert(row)
		}

		sorter.Sort()
		sorter.Close()
	}
	b.StopTimer()

	stats := GetStats()
	b.ReportMetric(float64(stats.MemGets)/float64(b.N), "mem_gets/op")
}

// BenchmarkMemPoolGetPut benchmarks direct pool operations.
func BenchmarkMemPoolGetPut(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := GetMem()
		m.SetInt(42)
		PutMem(m)
	}
}

// BenchmarkInstructionPoolGetPut benchmarks instruction pool operations.
func BenchmarkInstructionPoolGetPut(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instr := GetInstruction()
		instr.Opcode = OpHalt
		instr.P1 = 1
		PutInstruction(instr)
	}
}
