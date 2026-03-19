// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package vdbe implements memory pooling for VDBE components.
package vdbe

import (
	"sync"
)

// Global pools for VDBE memory optimization
var (
	// memPool is a sync.Pool for Mem cells
	memPool = sync.Pool{
		New: func() interface{} {
			return &Mem{
				flags: MemUndefined,
			}
		},
	}

	// instructionPool is a sync.Pool for Instructions
	instructionPool = sync.Pool{
		New: func() interface{} {
			return &Instruction{}
		},
	}

	// pageBufferPool is a sync.Pool for page buffers (4KB default SQLite page size)
	pageBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 4096)
			return &buf
		},
	}

	// largePageBufferPool is a sync.Pool for large page buffers (64KB max)
	largePageBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 65536)
			return &buf
		},
	}

	// instructionSlicePool is a sync.Pool for instruction slices
	instructionSlicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]*Instruction, 0, 64)
			return &slice
		},
	}

	// memSlicePool is a sync.Pool for Mem cell slices
	memSlicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]*Mem, 0, 16)
			return &slice
		},
	}
)

// MemoryPool provides pooled memory allocation for VDBE components.
type MemoryPool struct {
	// Statistics for monitoring pool usage
	stats PoolStats
	mu    sync.RWMutex
}

// PoolStats tracks memory pool usage statistics.
type PoolStats struct {
	MemGets              int64
	MemPuts              int64
	InstructionGets      int64
	InstructionPuts      int64
	PageBufferGets       int64
	PageBufferPuts       int64
	InstructionSliceGets int64
	InstructionSlicePuts int64
	MemSliceGets         int64
	MemSlicePuts         int64
}

// GlobalPool is the global memory pool instance.
var GlobalPool = &MemoryPool{}

// GetMem retrieves a Mem cell from the pool.
// The returned Mem is reset to an undefined state.
func GetMem() *Mem {
	GlobalPool.mu.Lock()
	GlobalPool.stats.MemGets++
	GlobalPool.mu.Unlock()

	mem := memPool.Get().(*Mem)
	// Reset the Mem to undefined state
	mem.flags = MemUndefined
	mem.i = 0
	mem.r = 0
	mem.z = nil
	mem.n = 0
	mem.nZero = 0
	mem.subtype = 0
	mem.xDel = nil
	return mem
}

// PutMem returns a Mem cell to the pool.
// The Mem is cleaned up before being returned to the pool.
func PutMem(mem *Mem) {
	if mem == nil {
		return
	}

	GlobalPool.mu.Lock()
	GlobalPool.stats.MemPuts++
	GlobalPool.mu.Unlock()

	// Clean up dynamic memory
	if mem.flags&(MemDyn|MemAgg) != 0 && mem.xDel != nil {
		mem.xDel(mem.z)
	}

	// Reset to undefined
	mem.flags = MemUndefined
	mem.i = 0
	mem.r = 0
	mem.z = nil
	mem.n = 0
	mem.nZero = 0
	mem.subtype = 0
	mem.xDel = nil

	memPool.Put(mem)
}

// GetInstruction retrieves an Instruction from the pool.
// The returned Instruction is zeroed.
func GetInstruction() *Instruction {
	GlobalPool.mu.Lock()
	GlobalPool.stats.InstructionGets++
	GlobalPool.mu.Unlock()

	instr := instructionPool.Get().(*Instruction)
	// Zero the instruction
	instr.Opcode = 0
	instr.P1 = 0
	instr.P2 = 0
	instr.P3 = 0
	instr.P4 = P4Union{}
	instr.P4Type = 0
	instr.P5 = 0
	instr.Comment = ""
	return instr
}

// PutInstruction returns an Instruction to the pool.
func PutInstruction(instr *Instruction) {
	if instr == nil {
		return
	}

	GlobalPool.mu.Lock()
	GlobalPool.stats.InstructionPuts++
	GlobalPool.mu.Unlock()

	// Clear the instruction
	instr.Opcode = 0
	instr.P1 = 0
	instr.P2 = 0
	instr.P3 = 0
	instr.P4 = P4Union{}
	instr.P4Type = 0
	instr.P5 = 0
	instr.Comment = ""

	instructionPool.Put(instr)
}

// GetPageBuffer retrieves a page buffer from the pool.
// The size parameter determines which pool to use (standard or large).
func GetPageBuffer(size int) *[]byte {
	GlobalPool.mu.Lock()
	GlobalPool.stats.PageBufferGets++
	GlobalPool.mu.Unlock()

	if size <= 4096 {
		buf := pageBufferPool.Get().(*[]byte)
		// Reslice to requested size
		*buf = (*buf)[:size]
		return buf
	}

	// Use large buffer pool for pages > 4KB
	buf := largePageBufferPool.Get().(*[]byte)
	*buf = (*buf)[:size]
	return buf
}

// PutPageBuffer returns a page buffer to the pool.
func PutPageBuffer(buf *[]byte) {
	if buf == nil {
		return
	}

	GlobalPool.mu.Lock()
	GlobalPool.stats.PageBufferPuts++
	GlobalPool.mu.Unlock()

	size := cap(*buf)
	if size <= 4096 {
		// Reset to full capacity
		*buf = (*buf)[:cap(*buf)]
		// Zero the buffer for security
		for i := range *buf {
			(*buf)[i] = 0
		}
		pageBufferPool.Put(buf)
	} else {
		*buf = (*buf)[:cap(*buf)]
		// Zero the buffer for security
		for i := range *buf {
			(*buf)[i] = 0
		}
		largePageBufferPool.Put(buf)
	}
}

// GetInstructionSlice retrieves an instruction slice from the pool.
func GetInstructionSlice() *[]*Instruction {
	GlobalPool.mu.Lock()
	GlobalPool.stats.InstructionSliceGets++
	GlobalPool.mu.Unlock()

	slice := instructionSlicePool.Get().(*[]*Instruction)
	// Clear the slice
	*slice = (*slice)[:0]
	return slice
}

// PutInstructionSlice returns an instruction slice to the pool.
func PutInstructionSlice(slice *[]*Instruction) {
	if slice == nil {
		return
	}

	GlobalPool.mu.Lock()
	GlobalPool.stats.InstructionSlicePuts++
	GlobalPool.mu.Unlock()

	// Clear references
	for i := range *slice {
		(*slice)[i] = nil
	}
	*slice = (*slice)[:0]

	instructionSlicePool.Put(slice)
}

// GetMemSlice retrieves a Mem cell slice from the pool.
func GetMemSlice() *[]*Mem {
	GlobalPool.mu.Lock()
	GlobalPool.stats.MemSliceGets++
	GlobalPool.mu.Unlock()

	slice := memSlicePool.Get().(*[]*Mem)
	*slice = (*slice)[:0]
	return slice
}

// PutMemSlice returns a Mem cell slice to the pool.
func PutMemSlice(slice *[]*Mem) {
	if slice == nil {
		return
	}

	GlobalPool.mu.Lock()
	GlobalPool.stats.MemSlicePuts++
	GlobalPool.mu.Unlock()

	// Clear references
	for i := range *slice {
		(*slice)[i] = nil
	}
	*slice = (*slice)[:0]

	memSlicePool.Put(slice)
}

// Stats returns a copy of the current pool statistics.
func (p *MemoryPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// ResetStats resets all pool statistics to zero.
func (p *MemoryPool) ResetStats() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats = PoolStats{}
}

// GetStats returns the current pool statistics without locking.
// This is useful for monitoring but may return slightly stale data.
func GetStats() PoolStats {
	return GlobalPool.Stats()
}

// ResetPoolStats resets the global pool statistics.
func ResetPoolStats() {
	GlobalPool.ResetStats()
}

// AllocateMems allocates a slice of Mem cells from the pool.
// This is more efficient than calling GetMem() in a loop.
func AllocateMems(count int) []*Mem {
	mems := make([]*Mem, count)
	for i := 0; i < count; i++ {
		mems[i] = GetMem()
	}
	return mems
}

// FreeMems returns a slice of Mem cells to the pool.
// This is more efficient than calling PutMem() in a loop.
func FreeMems(mems []*Mem) {
	for _, mem := range mems {
		PutMem(mem)
	}
}

// AllocateInstructions allocates a slice of Instructions from the pool.
func AllocateInstructions(count int) []*Instruction {
	instrs := make([]*Instruction, count)
	for i := 0; i < count; i++ {
		instrs[i] = GetInstruction()
	}
	return instrs
}

// FreeInstructions returns a slice of Instructions to the pool.
func FreeInstructions(instrs []*Instruction) {
	for _, instr := range instrs {
		PutInstruction(instr)
	}
}
