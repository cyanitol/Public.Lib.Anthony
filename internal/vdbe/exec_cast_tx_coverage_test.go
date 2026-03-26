// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ---------------------------------------------------------------------------
// CAST function tests
// ---------------------------------------------------------------------------

// TestExecCastExplicit exercises execCast → execExplicitCast path (P4.Z != "").
func TestExecCastExplicit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		input        interface{} // nil = NULL
		typeName     string
		expectedType string
		expectedVal  interface{}
	}{
		// NULL source → destination becomes NULL
		{"NULL to INTEGER", nil, "INTEGER", "null", nil},
		{"NULL to TEXT", nil, "TEXT", "null", nil},
		// int source
		{"int to INTEGER", int64(7), "INTEGER", "int", int64(7)},
		{"int to INT", int64(3), "INT", "int", int64(3)},
		{"int to REAL", int64(5), "REAL", "real", 5.0},
		{"int to TEXT", int64(42), "TEXT", "string", "42"},
		{"int to BLOB", int64(9), "BLOB", "blob", []byte("9")},
		{"int to NUMERIC", int64(10), "NUMERIC", "int", int64(10)},
		// real source
		{"real to INTEGER truncates", 3.9, "INTEGER", "int", int64(3)},
		{"real to TEXT", 1.5, "TEXT", "string", "1.5"},
		// string source
		{"text numeric to INTEGER", "42", "INTEGER", "int", int64(42)},
		{"text float to INTEGER", "3.7", "INTEGER", "int", int64(3)},
		{"text non-numeric to INTEGER", "abc", "INTEGER", "int", int64(0)},
		{"text leading numeric to INTEGER", "123abc", "INTEGER", "int", int64(123)},
		{"text to REAL", "2.5", "REAL", "real", 2.5},
		// blob source
		{"blob to TEXT", []byte("hi"), "TEXT", "string", "hi"},
		// unknown type name falls through to integer
		{"unknown type acts as INTEGER", int64(8), "UNKNOWNTYPE", "int", int64(8)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			v.AllocMemory(3) // need indices 1 and 2

			setMemValue(v.Mem[1], tt.input)

			instr := &Instruction{
				Opcode: OpCast,
				P1:     1,
				P2:     2,
				P4:     P4Union{Z: tt.typeName},
				P4Type: P4Static,
			}

			if err := v.execCast(instr); err != nil {
				t.Fatalf("execCast failed: %v", err)
			}

			verifyMemValue(t, v.Mem[2], tt.expectedType, tt.expectedVal)
		})
	}
}

// TestApplyCast directly tests the applyCast package-level function.
func TestApplyCast(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		input        interface{}
		typeName     string
		expectedType string
		expectedVal  interface{}
	}{
		{"INTEGER int", int64(5), "INTEGER", "int", int64(5)},
		{"INT int", int64(5), "INT", "int", int64(5)},
		{"REAL int", int64(2), "REAL", "real", 2.0},
		{"TEXT int", int64(99), "TEXT", "string", "99"},
		{"BLOB text", "hi", "BLOB", "blob", []byte("hi")},
		{"NUMERIC real whole", 4.0, "NUMERIC", "int", int64(4)},
		{"NUMERIC real frac", 4.5, "NUMERIC", "real", 4.5},
		{"default/unknown type", int64(1), "WHATEVER", "int", int64(1)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMem()
			setMemValue(m, tt.input)
			if err := applyCast(m, tt.typeName); err != nil {
				t.Fatalf("applyCast failed: %v", err)
			}
			verifyMemValue(t, m, tt.expectedType, tt.expectedVal)
		})
	}
}

// TestExplicitCastToInteger directly tests explicitCastToInteger.
func TestExplicitCastToInteger(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"int stays", int64(7), 7},
		{"real truncated positive", 3.9, 3},
		{"real truncated negative", -2.7, -2},
		{"text integer", "10", 10},
		{"text float", "2.9", 2},
		{"text leading int", "55xyz", 55},
		{"text non-numeric", "abc", 0},
		{"empty string", "", 0},
		{"blob integer", []byte("20"), 20},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMem()
			setMemValue(m, tt.input)
			if err := explicitCastToInteger(m); err != nil {
				t.Fatalf("explicitCastToInteger failed: %v", err)
			}
			if !m.IsInt() {
				t.Errorf("expected int type, got %s", getMemType(m))
				return
			}
			if m.IntValue() != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, m.IntValue())
			}
		})
	}
}

// TestCastIntegerFromReal tests castIntegerFromReal edge cases.
func TestCastIntegerFromReal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		input        float64
		expectInt    bool
		expectedInt  int64
		expectedReal float64
	}{
		{"whole positive", 5.0, true, 5, 0},
		{"whole negative", -3.0, true, -3, 0},
		{"zero", 0.0, true, 0, 0},
		{"fractional positive keeps real", 1.5, false, 0, 1.5},
		{"fractional negative keeps real", -2.7, false, 0, -2.7},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMemReal(tt.input)
			castIntegerFromReal(m)
			if tt.expectInt {
				if !m.IsInt() {
					t.Errorf("expected int, got %s", getMemType(m))
					return
				}
				if m.IntValue() != tt.expectedInt {
					t.Errorf("expected %d, got %d", tt.expectedInt, m.IntValue())
				}
			} else {
				if !m.IsReal() {
					t.Errorf("expected real (unchanged), got %s", getMemType(m))
					return
				}
				if m.RealValue() != tt.expectedReal {
					t.Errorf("expected %f, got %f", tt.expectedReal, m.RealValue())
				}
			}
		})
	}
}

// TestSetFloatOrInt tests setFloatOrInt branching.
func TestSetFloatOrInt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		val         float64
		expectInt   bool
		expectedInt int64
	}{
		{"whole number 4.0", 4.0, true, 4},
		{"whole number -1.0", -1.0, true, -1},
		{"zero", 0.0, true, 0},
		{"fractional 1.5 stays real", 1.5, false, 0},
		{"fractional -0.1 stays real", -0.1, false, 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMem()
			setFloatOrInt(m, tt.val)
			if tt.expectInt {
				if !m.IsInt() {
					t.Errorf("expected int, got %s", getMemType(m))
					return
				}
				if m.IntValue() != tt.expectedInt {
					t.Errorf("expected %d, got %d", tt.expectedInt, m.IntValue())
				}
			} else {
				if !m.IsReal() {
					t.Errorf("expected real, got %s", getMemType(m))
				}
				if m.RealValue() != tt.val {
					t.Errorf("expected %f, got %f", tt.val, m.RealValue())
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Transaction / misc function tests
// ---------------------------------------------------------------------------

// mockBtreeCacheCleared is a test btree that records ClearCache calls.
type mockBtreeCacheCleared struct {
	cleared int
}

func (b *mockBtreeCacheCleared) ClearCache() { b.cleared++ }

// BtreeAccess stubs — only ClearCache is exercised.
func (b *mockBtreeCacheCleared) CreateTable() (uint32, error)   { return 0, nil }
func (b *mockBtreeCacheCleared) AllocatePage() (uint32, error)  { return 0, nil }
func (b *mockBtreeCacheCleared) GetPage(uint32) ([]byte, error) { return nil, nil }
func (b *mockBtreeCacheCleared) SetPage(uint32, []byte) error   { return nil }
func (b *mockBtreeCacheCleared) NewRowid(uint32) (int64, error) { return 0, nil }

// TestClearBtreeCache covers the three branches of clearBtreeCache.
func TestClearBtreeCache(t *testing.T) {
	t.Parallel()

	t.Run("nil ctx no panic", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = nil
		v.clearBtreeCache() // must not panic
	})

	t.Run("nil btree no panic", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{Btree: nil}
		v.clearBtreeCache() // must not panic
	})

	t.Run("btree without ClearCache no panic", func(t *testing.T) {
		t.Parallel()
		v := New()
		// Use a pager-only context; the btree is set to a type that does NOT have ClearCache.
		// We satisfy BtreeAccess but NOT cacheClearer by using a minimal stub.
		v.Ctx = &VDBEContext{Btree: &noCacheBtree{}}
		v.clearBtreeCache() // must not panic
	})

	t.Run("btree with ClearCache is called", func(t *testing.T) {
		t.Parallel()
		v := New()
		bt := &mockBtreeCacheCleared{}
		v.Ctx = &VDBEContext{Btree: bt}
		v.clearBtreeCache()
		if bt.cleared != 1 {
			t.Errorf("expected ClearCache to be called once, got %d", bt.cleared)
		}
	})
}

// noCacheBtree satisfies types.BtreeAccess but NOT cacheClearer.
type noCacheBtree struct{}

func (b *noCacheBtree) CreateTable() (uint32, error)   { return 0, nil }
func (b *noCacheBtree) AllocatePage() (uint32, error)  { return 0, nil }
func (b *noCacheBtree) GetPage(uint32) ([]byte, error) { return nil, nil }
func (b *noCacheBtree) SetPage(uint32, []byte) error   { return nil }
func (b *noCacheBtree) NewRowid(uint32) (int64, error) { return 0, nil }

// TestGetAutoCommitPager covers the two error paths and the happy path.
func TestGetAutoCommitPager(t *testing.T) {
	t.Parallel()

	t.Run("nil ctx returns error", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = nil
		_, err := v.getAutoCommitPager()
		if err == nil {
			t.Error("expected error for nil ctx")
		}
	})

	t.Run("nil pager returns error", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{Pager: nil}
		_, err := v.getAutoCommitPager()
		if err == nil {
			t.Error("expected error for nil pager")
		}
	})

	t.Run("pager not PagerWriter returns error", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{Pager: "notapager"}
		_, err := v.getAutoCommitPager()
		if err == nil {
			t.Error("expected error when pager is not PagerWriter")
		}
	})

	t.Run("valid pager returns writer", func(t *testing.T) {
		t.Parallel()
		v := New()
		p := NewMockPager()
		v.Ctx = &VDBEContext{Pager: p}
		pw, err := v.getAutoCommitPager()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pw == nil {
			t.Error("expected non-nil PagerWriter")
		}
	})
}

// TestExecBeginTransaction covers the two branches of execBeginTransaction.
func TestExecBeginTransaction(t *testing.T) {
	t.Parallel()

	t.Run("begins write when not in transaction", func(t *testing.T) {
		t.Parallel()
		p := NewMockPager()
		v := New()
		v.Ctx = &VDBEContext{Pager: p}
		if err := v.execBeginTransaction(p); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !p.InWriteTransaction() {
			t.Error("expected write transaction to be active")
		}
	})

	t.Run("no-op when already in transaction", func(t *testing.T) {
		t.Parallel()
		p := NewMockPager()
		p.BeginWrite()
		v := New()
		v.Ctx = &VDBEContext{Pager: p}
		// already in transaction; should return nil without error
		if err := v.execBeginTransaction(p); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !p.InWriteTransaction() {
			t.Error("write transaction should still be active")
		}
	})
}

// TestExecShiftRightNullPropagation covers the NULL branch (68.8% → missing NULL).
func TestExecShiftRightNullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		shiftIsNull bool
		valueIsNull bool
	}{
		{"null shift", true, false},
		{"null value", false, true},
		{"both null", true, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			if tt.shiftIsNull {
				v.Mem[1].SetNull()
			} else {
				v.Mem[1].SetInt(2)
			}
			if tt.valueIsNull {
				v.Mem[2].SetNull()
			} else {
				v.Mem[2].SetInt(8)
			}

			instr := &Instruction{
				Opcode: OpShiftRight,
				P1:     1,
				P2:     2,
				P3:     3,
			}
			if err := v.execShiftRight(instr); err != nil {
				t.Fatalf("execShiftRight failed: %v", err)
			}
			if !v.Mem[3].IsNull() {
				t.Errorf("expected NULL result, got %s", getMemType(v.Mem[3]))
			}
		})
	}
}

// TestExecOnce covers execOnce: first execution falls through, second jumps.
func TestExecOnce(t *testing.T) {
	t.Parallel()

	t.Run("error when no OpInit at position 0", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Program = []*Instruction{
			{Opcode: OpNoop},
		}
		instr := &Instruction{Opcode: OpOnce, P1: 0, P2: 5}
		if err := v.execOnce(instr); err == nil {
			t.Error("expected error when position 0 is not OpInit")
		}
	})

	t.Run("error when program is empty", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Program = []*Instruction{}
		instr := &Instruction{Opcode: OpOnce, P1: 0, P2: 5}
		if err := v.execOnce(instr); err == nil {
			t.Error("expected error for empty program")
		}
	})

	t.Run("first execution falls through and sets P1", func(t *testing.T) {
		t.Parallel()
		v := New()
		initInstr := &Instruction{Opcode: OpInit, P1: 42}
		onceInstr := &Instruction{Opcode: OpOnce, P1: 0, P2: 99}
		v.Program = []*Instruction{initInstr, onceInstr}
		v.PC = 1

		pcBefore := v.PC
		if err := v.execOnce(onceInstr); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// P1 should now match initInstr.P1
		if onceInstr.P1 != 42 {
			t.Errorf("expected P1=42 after first execution, got %d", onceInstr.P1)
		}
		// PC should not have jumped
		if v.PC != pcBefore {
			t.Errorf("PC should not change on first execution: was %d, got %d", pcBefore, v.PC)
		}
	})

	t.Run("subsequent execution jumps to P2", func(t *testing.T) {
		t.Parallel()
		v := New()
		// P1 of OP_Init and OP_Once already match → jump
		initInstr := &Instruction{Opcode: OpInit, P1: 42}
		onceInstr := &Instruction{Opcode: OpOnce, P1: 42, P2: 7}
		v.Program = []*Instruction{initInstr, onceInstr}
		v.PC = 1

		if err := v.execOnce(onceInstr); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v.PC != 7 {
			t.Errorf("expected PC=7 after jump, got %d", v.PC)
		}
	})
}
