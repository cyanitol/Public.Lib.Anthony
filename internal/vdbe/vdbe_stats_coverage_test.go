// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import "testing"

// TestStatsResetStatistics verifies that ResetStatistics clears all counters.
func TestStatsResetStatistics(t *testing.T) {
	v := New()
	v.Stats.PageReads = 5
	v.Stats.CacheHits = 3
	v.Stats.NumInstructions = 10

	v.ResetStatistics()

	if v.Stats == nil {
		t.Fatal("Stats should not be nil after ResetStatistics")
	}
	if v.Stats.PageReads != 0 {
		t.Errorf("PageReads should be 0 after reset, got %d", v.Stats.PageReads)
	}
	if v.Stats.CacheHits != 0 {
		t.Errorf("CacheHits should be 0 after reset, got %d", v.Stats.CacheHits)
	}
	if v.Stats.NumInstructions != 0 {
		t.Errorf("NumInstructions should be 0 after reset, got %d", v.Stats.NumInstructions)
	}
}

// TestStatsRecordCacheMiss verifies that RecordCacheMiss increments CacheMisses.
func TestStatsRecordCacheMiss(t *testing.T) {
	s := NewQueryStatistics()
	s.RecordCacheMiss()
	s.RecordCacheMiss()
	if s.CacheMisses != 2 {
		t.Errorf("expected CacheMisses=2, got %d", s.CacheMisses)
	}
}

// TestStatsRecordCacheHit verifies that RecordCacheHit increments CacheHits.
func TestStatsRecordCacheHit(t *testing.T) {
	s := NewQueryStatistics()
	s.RecordCacheHit()
	s.RecordCacheHit()
	s.RecordCacheHit()
	if s.CacheHits != 3 {
		t.Errorf("expected CacheHits=3, got %d", s.CacheHits)
	}
}

// TestStatsRecordPageWrite verifies that RecordPageWrite increments PageWrites.
func TestStatsRecordPageWrite(t *testing.T) {
	s := NewQueryStatistics()
	s.RecordPageWrite()
	if s.PageWrites != 1 {
		t.Errorf("expected PageWrites=1, got %d", s.PageWrites)
	}
}

// TestStatsRecordPageRead verifies that RecordPageRead increments PageReads.
func TestStatsRecordPageRead(t *testing.T) {
	s := NewQueryStatistics()
	s.RecordPageRead()
	s.RecordPageRead()
	if s.PageReads != 2 {
		t.Errorf("expected PageReads=2, got %d", s.PageReads)
	}
}

// TestStatsAddOpWithP4Callback verifies AddOpWithP4Callback stores the callback in P4.P.
func TestStatsAddOpWithP4Callback(t *testing.T) {
	v := New()
	cb := func() {}
	addr := v.AddOpWithP4Callback(OpHalt, 0, 0, 0, cb)
	if addr < 0 || addr >= len(v.Program) {
		t.Fatalf("invalid address %d", addr)
	}
	instr := v.Program[addr]
	if instr.P4Type != P4Callback {
		t.Errorf("expected P4Type=P4Callback, got %v", instr.P4Type)
	}
	if instr.P4.P == nil {
		t.Error("P4.P should not be nil")
	}
}

// TestStatsAddOpWithP4Int64 verifies AddOpWithP4Int64 stores the int64 in P4.I64.
func TestStatsAddOpWithP4Int64(t *testing.T) {
	v := New()
	const val int64 = 9876543210
	addr := v.AddOpWithP4Int64(OpInteger, 1, 2, 3, val)
	if addr < 0 || addr >= len(v.Program) {
		t.Fatalf("invalid address %d", addr)
	}
	instr := v.Program[addr]
	if instr.P4Type != P4Int64 {
		t.Errorf("expected P4Type=P4Int64, got %v", instr.P4Type)
	}
	if instr.P4.I64 != val {
		t.Errorf("expected P4.I64=%d, got %d", val, instr.P4.I64)
	}
}
