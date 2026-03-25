// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
)

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – valueToMem default branch
// ─────────────────────────────────────────────────────────────────────────────

// customNonNullValue implements functions.Value but reports IsNull() == false
// and has a type that is not handled by the valueToMem switch (TypeNull while
// IsNull() returns false is not possible via SimpleValue, so we use TypeBlob
// to take the normal blob branch — the real gap is the default branch hit when
// a future/unknown ValueType is returned).
//
// We exercise the default branch by returning a ValueType not in the switch.
type unknownTypeValue struct{}

func (u *unknownTypeValue) Type() functions.ValueType { return functions.ValueType(99) }
func (u *unknownTypeValue) AsInt64() int64            { return 0 }
func (u *unknownTypeValue) AsFloat64() float64        { return 0 }
func (u *unknownTypeValue) AsString() string          { return "" }
func (u *unknownTypeValue) AsBlob() []byte            { return nil }
func (u *unknownTypeValue) IsNull() bool              { return false }
func (u *unknownTypeValue) Bytes() int                { return 0 }

func TestFunctionsMemPoolValueToMemDefaultBranch(t *testing.T) {
	t.Parallel()
	// unknownTypeValue.IsNull() == false and Type() is not in the switch,
	// so valueToMem falls through to the default case and returns NewMemNull().
	result := valueToMem(&unknownTypeValue{})
	if !result.IsNull() {
		t.Errorf("valueToMem with unknown type: expected NULL Mem, got flags=%v", result.flags)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – valueToMem blob branch
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolValueToMemBlob(t *testing.T) {
	t.Parallel()
	// Verify the blob branch in valueToMem produces a MemBlob.
	blobData := []byte{0xCA, 0xFE, 0xBA, 0xBE}
	v := functions.NewBlobValue(blobData)
	result := valueToMem(v)
	if !result.IsBlob() {
		t.Fatalf("valueToMem(blob): expected MemBlob, got flags=%v", result.flags)
	}
	got := result.BlobValue()
	if len(got) != len(blobData) {
		t.Errorf("BlobValue len = %d, want %d", len(got), len(blobData))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – storeResult error path (out-of-bounds register)
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolStoreResultOutOfBounds(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(3) // registers 0..2 only

	// Attempt to store into register 100 which does not exist.
	err := v.storeResult(100, NewMemInt(42))
	if err == nil {
		t.Error("storeResult to out-of-bounds register: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – collectFunctionArgs error path
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolCollectFunctionArgsOutOfBounds(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(2) // registers 0..1

	// Ask for 3 args starting at register 1 → register 3 is out of bounds.
	_, err := v.collectFunctionArgs(1, 3)
	if err == nil {
		t.Error("collectFunctionArgs with OOB register: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – collectArgValues error path
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolCollectArgValuesOutOfBounds(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(2) // registers 0..1

	// Ask for 5 args starting at register 0 → registers 2..4 are out of bounds.
	_, err := v.collectArgValues(0, 5)
	if err == nil {
		t.Error("collectArgValues with OOB register: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – opAggStep: collectArgValues error path
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolOpAggStepCollectArgsError(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(2) // registers 0..1

	// Valid instruction pointing to "count" aggregate.
	instr := &Instruction{
		Opcode: OpAggStep,
		P1:     0,
		P2:     0,  // first arg register
		P3:     0,  // func index
		P4:     P4Union{Z: "count"},
		P4Type: P4Static,
		P5:     5, // want 5 args → registers 0..4, but only 0..1 exist
	}
	v.Program = append(v.Program, instr)
	v.PC = len(v.Program)

	err := v.opAggStep(0, 0, 0, 0, 5)
	if err == nil {
		t.Error("opAggStep with OOB arg register: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – ensureAggFuncSlot: "not an aggregate function" error path
// ─────────────────────────────────────────────────────────────────────────────

// scalarOnlyFunc is a Function that is NOT an AggregateFunction.
// It can be injected into a registry builtin to trigger the
// "%s is not an aggregate function" error branch in ensureAggFuncSlot.
type scalarOnlyFunc struct{ name string }

func (s *scalarOnlyFunc) Name() string                           { return s.name }
func (s *scalarOnlyFunc) NumArgs() int                           { return 0 }
func (s *scalarOnlyFunc) Call(_ []functions.Value) (functions.Value, error) {
	return functions.NewNullValue(), nil
}

func TestFunctionsMemPoolEnsureAggFuncSlotNotAggregate(t *testing.T) {
	t.Parallel()

	// Build a registry that has a scalar (non-aggregate) builtin named "notanagg".
	reg := functions.NewRegistry()
	reg.Register(&scalarOnlyFunc{name: "notanagg"})

	fc := NewFunctionContextWithRegistry(reg)
	v := New()
	v.AllocMemory(5)
	v.funcCtx = fc

	instr := &Instruction{
		Opcode: OpAggStep,
		P1:     0,
		P2:     0,
		P3:     0,
		P4:     P4Union{Z: "notanagg"},
		P4Type: P4Static,
		P5:     0,
	}
	v.Program = append(v.Program, instr)
	v.PC = len(v.Program)

	err := v.opAggStep(0, 0, 0, 0, 0)
	if err == nil {
		t.Error("opAggStep with scalar-only function: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – opAggStep: Step() returns error
// ─────────────────────────────────────────────────────────────────────────────

// errorStepAgg always returns an error from Step().
type errorStepAgg struct{ name string }

func (e *errorStepAgg) Name() string                               { return e.name }
func (e *errorStepAgg) NumArgs() int                               { return 1 }
func (e *errorStepAgg) Call(_ []functions.Value) (functions.Value, error) {
	return functions.NewNullValue(), nil
}
func (e *errorStepAgg) Step(_ []functions.Value) error {
	return fmt.Errorf("step deliberately failed")
}
func (e *errorStepAgg) Final() (functions.Value, error) { return functions.NewNullValue(), nil }
func (e *errorStepAgg) Reset()                          {}

func TestFunctionsMemPoolOpAggStepStepError(t *testing.T) {
	t.Parallel()

	reg := functions.NewRegistry()
	reg.Register(&errorStepAgg{name: "errstagg"})

	fc := NewFunctionContextWithRegistry(reg)
	v := New()
	v.AllocMemory(5)
	v.funcCtx = fc
	v.Mem[0].SetInt(1)

	instr := &Instruction{
		Opcode: OpAggStep,
		P1:     0,
		P2:     0,
		P3:     0,
		P4:     P4Union{Z: "errstagg"},
		P4Type: P4Static,
		P5:     1,
	}
	v.Program = append(v.Program, instr)
	v.PC = len(v.Program)

	err := v.opAggStep(0, 0, 0, 0, 1)
	if err == nil {
		t.Error("opAggStep when Step() errors: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – opAggFinal: GetMem error path (out-of-bounds output register)
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolOpAggFinalGetMemError(t *testing.T) {
	t.Parallel()

	v := New()
	v.AllocMemory(3) // registers 0..2

	// Set up a valid aggregate so finalization can proceed past the function-check.
	instr := &Instruction{
		Opcode: OpAggStep,
		P1:     0,
		P2:     0,
		P3:     0,
		P4:     P4Union{Z: "count"},
		P4Type: P4Static,
		P5:     1,
	}
	v.Program = append(v.Program, instr)
	v.PC = len(v.Program)
	v.Mem[0].SetInt(1)
	if err := v.opAggStep(0, 0, 0, 0, 1); err != nil {
		t.Fatalf("opAggStep setup: %v", err)
	}

	// Ask opAggFinal to store into register 999 → OOB → GetMem error.
	err := v.opAggFinal(0, 999, 0)
	if err == nil {
		t.Error("opAggFinal with OOB output register: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – opAggFinal: Final() returns error
// ─────────────────────────────────────────────────────────────────────────────

// errorFinalAgg always returns an error from Final().
type errorFinalAgg struct{ name string }

func (e *errorFinalAgg) Name() string                               { return e.name }
func (e *errorFinalAgg) NumArgs() int                               { return 1 }
func (e *errorFinalAgg) Call(_ []functions.Value) (functions.Value, error) {
	return functions.NewNullValue(), nil
}
func (e *errorFinalAgg) Step(_ []functions.Value) error { return nil }
func (e *errorFinalAgg) Final() (functions.Value, error) {
	return nil, fmt.Errorf("final deliberately failed")
}
func (e *errorFinalAgg) Reset() {}

func TestFunctionsMemPoolOpAggFinalError(t *testing.T) {
	t.Parallel()

	reg := functions.NewRegistry()
	reg.Register(&errorFinalAgg{name: "errfinalagg"})

	fc := NewFunctionContextWithRegistry(reg)
	v := New()
	v.AllocMemory(5)
	v.funcCtx = fc
	v.Mem[0].SetInt(1)

	instr := &Instruction{
		Opcode: OpAggStep,
		P1:     0,
		P2:     0,
		P3:     0,
		P4:     P4Union{Z: "errfinalagg"},
		P4Type: P4Static,
		P5:     1,
	}
	v.Program = append(v.Program, instr)
	v.PC = len(v.Program)

	if err := v.opAggStep(0, 0, 0, 0, 1); err != nil {
		t.Fatalf("opAggStep setup: %v", err)
	}

	err := v.opAggFinal(0, 4, 0)
	if err == nil {
		t.Error("opAggFinal when Final() errors: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – createAggregateInstance: fresh state is returned
// ─────────────────────────────────────────────────────────────────────────────

// minimalAgg is a minimal AggregateFunction for testing createAggregateInstance.
type minimalAgg struct {
	count int
}

func (m *minimalAgg) Name() string                               { return "minimalagg" }
func (m *minimalAgg) NumArgs() int                               { return 1 }
func (m *minimalAgg) Call(_ []functions.Value) (functions.Value, error) {
	return functions.NewNullValue(), nil
}
func (m *minimalAgg) Step(_ []functions.Value) error {
	m.count++
	return nil
}
func (m *minimalAgg) Final() (functions.Value, error) {
	return functions.NewIntValue(int64(m.count)), nil
}
func (m *minimalAgg) Reset() { m.count = 0 }

func TestFunctionsMemPoolCreateAggregateInstanceFreshState(t *testing.T) {
	t.Parallel()

	// Step the original instance a few times.
	original := &minimalAgg{}
	_ = original.Step(nil)
	_ = original.Step(nil)
	// original.count == 2

	// createAggregateInstance must return a fresh instance with count == 0.
	fresh := createAggregateInstance(original)
	result, err := fresh.Final()
	if err != nil {
		t.Fatalf("Final() on fresh instance: %v", err)
	}
	if result.AsInt64() != 0 {
		t.Errorf("fresh instance count = %d, want 0 (original was 2)", result.AsInt64())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// mem.go – RealValue: leading-numeric-prefix fallback for string
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolRealValueLeadingNumericStr(t *testing.T) {
	t.Parallel()

	// "3.14abc" — ParseFloat fails, extractLeadingNumeric returns "3.14".
	m := NewMemStr("3.14abc")
	got := m.RealValue()
	const want = 3.14
	if got < 3.13 || got > 3.15 {
		t.Errorf("RealValue('3.14abc') = %v, want ~3.14", got)
	}
	_ = want
}

// TestFunctionsMemPoolRealValueLeadingNumericBlob exercises the same path for a blob.
func TestFunctionsMemPoolRealValueLeadingNumericBlob(t *testing.T) {
	t.Parallel()

	// blob containing "2.71abc" — ParseFloat fails, leading numeric gives 2.71.
	m := NewMemBlob([]byte("2.71abc"))
	got := m.RealValue()
	if got < 2.70 || got > 2.72 {
		t.Errorf("RealValue(blob '2.71abc') = %v, want ~2.71", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// mem.go – BlobValue: integer and null return nil
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolBlobValueIntReturnsNil(t *testing.T) {
	t.Parallel()
	m := NewMemInt(42)
	if got := m.BlobValue(); got != nil {
		t.Errorf("BlobValue() for MemInt = %v, want nil", got)
	}
}

func TestFunctionsMemPoolBlobValueNullReturnsNil(t *testing.T) {
	t.Parallel()
	m := NewMemNull()
	if got := m.BlobValue(); got != nil {
		t.Errorf("BlobValue() for MemNull = %v, want nil", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// mem.go – Stringify: undefined returns error
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolStringifyUndefinedError(t *testing.T) {
	t.Parallel()
	m := NewMem() // MemUndefined flags
	err := m.Stringify()
	if err == nil {
		t.Error("Stringify on undefined Mem: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// pool.go – PutMem: dynamic memory destructor path (MemDyn flag + xDel set)
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolPutMemWithDestructor(t *testing.T) {
	t.Parallel()

	called := false
	destructor := func(_ interface{}) {
		called = true
	}

	m := &Mem{
		flags: MemDyn | MemStr,
		z:     []byte("dynamic string"),
		n:     14,
		xDel:  destructor,
	}

	// PutMem should call the destructor because MemDyn is set and xDel != nil.
	PutMem(m)

	if !called {
		t.Error("PutMem with MemDyn and xDel: destructor was not called")
	}
}

// TestFunctionsMemPoolPutMemWithAggFlagDestructor exercises the MemAgg path.
func TestFunctionsMemPoolPutMemWithAggFlagDestructor(t *testing.T) {
	t.Parallel()

	called := false
	destructor := func(_ interface{}) {
		called = true
	}

	m := &Mem{
		flags: MemAgg,
		z:     []byte("agg data"),
		n:     8,
		xDel:  destructor,
	}

	PutMem(m)

	if !called {
		t.Error("PutMem with MemAgg and xDel: destructor was not called")
	}
}

// TestFunctionsMemPoolPutMemNilDestructor verifies PutMem does not panic when
// MemDyn is set but xDel is nil (guard condition: both must be true).
func TestFunctionsMemPoolPutMemNilDestructor(t *testing.T) {
	t.Parallel()

	m := &Mem{
		flags: MemDyn | MemStr,
		z:     []byte("nodestruct"),
		n:     10,
		xDel:  nil, // no destructor
	}

	// Should not panic.
	PutMem(m)
}

// ─────────────────────────────────────────────────────────────────────────────
// pool.go – PutMem nil guard
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolPutMemNil(t *testing.T) {
	t.Parallel()
	// Must not panic.
	PutMem(nil)
}

// ─────────────────────────────────────────────────────────────────────────────
// pool.go – PutInstruction nil guard (already tested elsewhere, confirm no dup)
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolPutInstructionNil(t *testing.T) {
	t.Parallel()
	// Must not panic.
	PutInstruction(nil)
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – opFunction: collectFunctionArgs error via OOB register
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolOpFunctionArgsOOB(t *testing.T) {
	t.Parallel()

	v := New()
	v.AllocMemory(2) // only registers 0..1

	// upper() with 1 arg starting at register 1, then register 2 OOB when
	// we ask for 3 args. Use P5=3 args starting at register 0 → reg 2 OOB.
	instr := &Instruction{
		Opcode: OpFunction,
		P2:     0,
		P3:     0,
		P4:     P4Union{Z: "upper"},
		P4Type: P4Static,
		P5:     3,
	}
	v.Program = append(v.Program, instr)
	v.PC = len(v.Program)

	err := v.opFunction(0, 0, 0, 0, 3)
	if err == nil {
		t.Error("opFunction with OOB arg register: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – memToValue: all branches
// ─────────────────────────────────────────────────────────────────────────────

func TestFunctionsMemPoolMemToValueAllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mem      *Mem
		wantType functions.ValueType
	}{
		{"null", NewMemNull(), functions.TypeNull},
		{"int", NewMemInt(42), functions.TypeInteger},
		{"real", NewMemReal(3.14), functions.TypeFloat},
		{"str", NewMemStr("hello"), functions.TypeText},
		{"blob", NewMemBlob([]byte{1, 2}), functions.TypeBlob},
		{"undefined fallback", NewMem(), functions.TypeNull},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := memToValue(tt.mem)
			if v.Type() != tt.wantType {
				t.Errorf("memToValue(%s): type = %v, want %v", tt.name, v.Type(), tt.wantType)
			}
		})
	}
}
