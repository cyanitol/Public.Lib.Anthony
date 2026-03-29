// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

// MC/DC 14b — internal VDBE unit tests for low-coverage paths.
//
// Targets:
//   spill_file.go:178  closeReaders            — no-op function (0% coverage)
//   record.go:174      decodeFloat64           — truncated error path (75%)
//   record.go:115      decodeFixedInt          — truncated error path
//   record.go:128      decodeIntValue          — int24/int48/int8 branches
//   exec.go:527        execInt64               — P4Type mismatch error
//   exec.go:540        execReal                — P4Type mismatch error
//   exec.go:553        execString              — P4Type mismatch error
//   exec.go:434        execIf                  — real (non-int) true/false paths
//   mem.go:286         Value                   — blob path
//   mem.go:335         Integerify              — error paths (undefined value)
//   mem.go:377         Realify                 — error paths

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// closeReaders — no-op function
// ---------------------------------------------------------------------------

// TestMCDC14b_CloseReaders_NoOp calls closeReaders to exercise the 0% function.
func TestMCDC14b_CloseReaders_NoOp(t *testing.T) {
	t.Parallel()
	s := &SorterWithSpill{}
	// closeReaders is a no-op; just verify it doesn't panic.
	s.closeReaders(nil)
	s.closeReaders([]*runReader{})
}

// ---------------------------------------------------------------------------
// decodeFloat64 — truncated error path
// ---------------------------------------------------------------------------

// TestMCDC14b_DecodeFloat64_Truncated exercises the offset+8 > len(data) error.
func TestMCDC14b_DecodeFloat64_Truncated(t *testing.T) {
	t.Parallel()

	// data is only 4 bytes; offset 0 + 8 > 4 → truncated error.
	_, _, err := decodeFloat64([]byte{0x01, 0x02, 0x03, 0x04}, 0)
	if err == nil {
		t.Error("expected truncated float64 error, got nil")
	}
}

// TestMCDC14b_DecodeFloat64_Valid exercises successful float64 decode.
func TestMCDC14b_DecodeFloat64_Valid(t *testing.T) {
	t.Parallel()

	// Encode pi as IEEE 754.
	bits := math.Float64bits(math.Pi)
	data := []byte{
		byte(bits >> 56), byte(bits >> 48), byte(bits >> 40), byte(bits >> 32),
		byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits),
	}

	val, width, err := decodeFloat64(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if width != 8 {
		t.Errorf("expected width 8, got %d", width)
	}
	f, ok := val.(float64)
	if !ok || math.Abs(f-math.Pi) > 1e-15 {
		t.Errorf("expected Pi, got %v", val)
	}
}

// ---------------------------------------------------------------------------
// decodeFixedInt — truncated error path
// ---------------------------------------------------------------------------

// TestMCDC14b_DecodeFixedInt_Truncated exercises the width overflow check.
func TestMCDC14b_DecodeFixedInt_Truncated(t *testing.T) {
	t.Parallel()

	// Serial type 6 = 8-byte int; data has only 2 bytes → truncated.
	_, _, err := decodeFixedInt([]byte{0x00, 0x01}, 0, 6)
	if err == nil {
		t.Error("expected truncated error, got nil")
	}
}

// TestMCDC14b_DecodeIntValue_Int24 exercises the int24 branch (serial type 3).
func TestMCDC14b_DecodeIntValue_Int24(t *testing.T) {
	t.Parallel()

	// Positive value: 0x010203 = 66051.
	v, err := decodeIntValue([]byte{0x01, 0x02, 0x03}, 0, 3)
	if err != nil {
		t.Fatalf("decodeIntValue int24: %v", err)
	}
	if v != 66051 {
		t.Errorf("expected 66051, got %d", v)
	}
}

// TestMCDC14b_DecodeIntValue_Int48 exercises the int48 branch (serial type 5).
func TestMCDC14b_DecodeIntValue_Int48(t *testing.T) {
	t.Parallel()

	// Value 1 encoded in 6 bytes big-endian.
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	v, err := decodeIntValue(data, 0, 5)
	if err != nil {
		t.Fatalf("decodeIntValue int48: %v", err)
	}
	if v != 1 {
		t.Errorf("expected 1, got %d", v)
	}
}

// TestMCDC14b_DecodeIntValue_Default exercises the default (int64, serial type 6+).
func TestMCDC14b_DecodeIntValue_Default(t *testing.T) {
	t.Parallel()

	// Serial type 6 = 8-byte int; value = 255.
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF}
	v, err := decodeIntValue(data, 0, 6)
	if err != nil {
		t.Fatalf("decodeIntValue default: %v", err)
	}
	if v != 255 {
		t.Errorf("expected 255, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// execInt64 / execReal / execString — P4Type mismatch error paths
// ---------------------------------------------------------------------------

// TestMCDC14b_ExecInt64_WrongP4Type exercises the P4Type != P4Int64 error path.
func TestMCDC14b_ExecInt64_WrongP4Type(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.AllocMemory(2)
	instr := &Instruction{
		Opcode: OpInt64,
		P2:     1,
		P4Type: P4Static, // Wrong type
	}
	err := vm.execInt64(instr)
	if err == nil {
		t.Error("expected error for wrong P4Type in execInt64, got nil")
	}
}

// TestMCDC14b_ExecReal_WrongP4Type exercises the P4Type != P4Real error path.
func TestMCDC14b_ExecReal_WrongP4Type(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.AllocMemory(2)
	instr := &Instruction{
		Opcode: OpReal,
		P2:     1,
		P4Type: P4Static, // Wrong type
	}
	err := vm.execReal(instr)
	if err == nil {
		t.Error("expected error for wrong P4Type in execReal, got nil")
	}
}

// TestMCDC14b_ExecString_WrongP4Type exercises the type check in execString.
func TestMCDC14b_ExecString_WrongP4Type(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.AllocMemory(2)
	instr := &Instruction{
		Opcode: OpString,
		P2:     1,
		P4Type: P4Int64, // Wrong type — not P4Static or P4Dynamic
	}
	err := vm.execString(instr)
	if err == nil {
		t.Error("expected error for wrong P4Type in execString, got nil")
	}
}

// ---------------------------------------------------------------------------
// execIf — real (non-int) path
// ---------------------------------------------------------------------------

// TestMCDC14b_ExecIf_RealTrue exercises the real-value true branch in execIf.
func TestMCDC14b_ExecIf_RealTrue(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.AllocMemory(2)

	mem, err := vm.GetMem(1)
	if err != nil {
		t.Fatalf("GetMem: %v", err)
	}
	mem.SetReal(3.14) // Non-int, non-zero → isTrue = true

	instr := &Instruction{Opcode: OpIf, P1: 1, P2: 99}
	if err := vm.execIf(instr); err != nil {
		t.Fatalf("execIf (real true): %v", err)
	}
	if vm.PC != 99 {
		t.Errorf("expected PC=99 (jumped), got %d", vm.PC)
	}
}

// TestMCDC14b_ExecIf_RealFalse exercises the real-value false branch in execIf.
func TestMCDC14b_ExecIf_RealFalse(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.AllocMemory(2)

	mem, err := vm.GetMem(1)
	if err != nil {
		t.Fatalf("GetMem: %v", err)
	}
	mem.SetReal(0.0) // Non-int, zero → isTrue = false

	vm.PC = 5
	instr := &Instruction{Opcode: OpIf, P1: 1, P2: 99}
	if err := vm.execIf(instr); err != nil {
		t.Fatalf("execIf (real false): %v", err)
	}
	if vm.PC == 99 {
		t.Error("expected PC not to jump for zero real")
	}
}

// ---------------------------------------------------------------------------
// Mem.Value — blob path
// ---------------------------------------------------------------------------

// TestMCDC14b_MemValue_Blob exercises the MemBlob branch in Value().
func TestMCDC14b_MemValue_Blob(t *testing.T) {
	t.Parallel()

	m := NewMem()
	blob := []byte{0x01, 0x02, 0x03}
	m.SetBlob(blob)

	v := m.Value()
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", v)
	}
	if len(b) != 3 {
		t.Errorf("expected len 3, got %d", len(b))
	}
}

// TestMCDC14b_MemValue_Undefined exercises the final nil return in Value()
// when no flags are set.
func TestMCDC14b_MemValue_Undefined(t *testing.T) {
	t.Parallel()

	m := &Mem{} // Zero value — no flags set
	v := m.Value()
	if v != nil {
		t.Errorf("expected nil for undefined Mem, got %v", v)
	}
}

// ---------------------------------------------------------------------------
// Integerify — error paths
// ---------------------------------------------------------------------------

// TestMCDC14b_Integerify_Undefined exercises the final error return in Integerify.
func TestMCDC14b_Integerify_Undefined(t *testing.T) {
	t.Parallel()

	m := &Mem{} // No flags set
	err := m.Integerify()
	if err == nil {
		t.Error("expected error for undefined Mem, got nil")
	}
}

// TestMCDC14b_Integerify_BadString exercises the string parse failure in Integerify.
func TestMCDC14b_Integerify_BadString(t *testing.T) {
	t.Parallel()

	m := NewMem()
	m.SetStr("not-a-number-at-all")

	err := m.Integerify()
	if err == nil {
		t.Error("expected parse error for non-numeric string, got nil")
	}
}

// TestMCDC14b_Integerify_FloatString exercises the float→int conversion in Integerify.
func TestMCDC14b_Integerify_FloatString(t *testing.T) {
	t.Parallel()

	m := NewMem()
	m.SetStr("3.7")

	if err := m.Integerify(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if m.IntValue() != 3 {
		t.Errorf("expected 3 (truncated), got %d", m.IntValue())
	}
}

// ---------------------------------------------------------------------------
// Realify — error paths
// ---------------------------------------------------------------------------

// TestMCDC14b_Realify_BadString exercises the string parse failure in Realify.
func TestMCDC14b_Realify_BadString(t *testing.T) {
	t.Parallel()

	m := NewMem()
	m.SetStr("not-a-float")

	err := m.Realify()
	if err == nil {
		t.Error("expected parse error for non-numeric string, got nil")
	}
}
