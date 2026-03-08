// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"testing"
)

// Helper to setup aggregate test
func setupAggTest(t *testing.T, funcName string) *VDBE {
	t.Helper()
	v := NewTestVDBE(10)
	v.funcCtx = NewFunctionContext()
	v.Program = []*Instruction{
		{
			Opcode: OpAggStep,
			P1:     0,
			P2:     1,
			P3:     0,
			P4:     P4Union{Z: funcName},
			P4Type: P4Static,
			P5:     1,
		},
	}
	return v
}

// Helper to run aggregate step
func runAggStep(t *testing.T, v *VDBE) {
	t.Helper()
	v.PC = 1
	err := v.execAggStep(v.Program[0])
	if err != nil {
		t.Fatalf("execAggStep failed: %v", err)
	}
}

// Helper to finalize aggregate and check result
func finalizeAndCheckInt(t *testing.T, v *VDBE, expected int64) {
	t.Helper()
	finalInstr := &Instruction{Opcode: OpAggFinal, P1: 0, P2: 5, P3: 0}
	err := v.execAggFinal(finalInstr)
	if err != nil {
		t.Fatalf("execAggFinal failed: %v", err)
	}
	if !v.Mem[5].IsInt() || v.Mem[5].IntValue() != expected {
		t.Errorf("Expected result=%d, got %v", expected, v.Mem[5].IntValue())
	}
}

// Helper to finalize aggregate and check real result
func finalizeAndCheckReal(t *testing.T, v *VDBE, expected float64) {
	t.Helper()
	finalInstr := &Instruction{Opcode: OpAggFinal, P1: 0, P2: 5, P3: 0}
	err := v.execAggFinal(finalInstr)
	if err != nil {
		t.Fatalf("execAggFinal failed: %v", err)
	}
	result := v.Mem[5].RealValue()
	if result != expected {
		t.Errorf("Expected result=%.1f, got %.1f", expected, result)
	}
}

// Helper to finalize aggregate and check string result
func finalizeAndCheckString(t *testing.T, v *VDBE, expected string) {
	t.Helper()
	finalInstr := &Instruction{Opcode: OpAggFinal, P1: 0, P2: 5, P3: 0}
	err := v.execAggFinal(finalInstr)
	if err != nil {
		t.Fatalf("execAggFinal failed: %v", err)
	}
	result := v.Mem[5].StringValue()
	if result != expected {
		t.Errorf("Expected result='%s', got '%s'", expected, result)
	}
}

func TestAggregateOpcodesCount(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "count")
	for i := 0; i < 3; i++ {
		v.Mem[1].SetInt(int64(i))
		runAggStep(t, v)
	}
	finalizeAndCheckInt(t, v, 3)
}

func TestAggregateOpcodesSum(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "sum")
	for i := 1; i <= 4; i++ {
		v.Mem[1].SetInt(int64(i))
		runAggStep(t, v)
	}
	finalizeAndCheckInt(t, v, 10)
}

func TestAggregateOpcodesAvg(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "avg")
	for _, val := range []int64{2, 4, 6} {
		v.Mem[1].SetInt(val)
		runAggStep(t, v)
	}
	finalizeAndCheckReal(t, v, 4.0)
}

func TestAggregateOpcodesMax(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "max")
	for _, val := range []int64{3, 7, 2, 9, 1} {
		v.Mem[1].SetInt(val)
		runAggStep(t, v)
	}
	finalizeAndCheckInt(t, v, 9)
}

func TestAggregateOpcodesMin(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "min")
	for _, val := range []int64{3, 7, 2, 9, 1} {
		v.Mem[1].SetInt(val)
		runAggStep(t, v)
	}
	finalizeAndCheckInt(t, v, 1)
}

func TestAggregateOpcodesMultipleGroups(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	v.funcCtx = NewFunctionContext()

	v.Program = []*Instruction{
		{Opcode: OpAggStep, P1: 0, P2: 1, P3: 0, P4: P4Union{Z: "sum"}, P4Type: P4Static, P5: 1},
		{Opcode: OpAggStep, P1: 1, P2: 1, P3: 0, P4: P4Union{Z: "sum"}, P4Type: P4Static, P5: 1},
	}

	for _, val := range []int64{1, 2, 3} {
		v.Mem[1].SetInt(val)
		v.PC = 1
		v.execAggStep(v.Program[0])
	}

	for _, val := range []int64{10, 20, 30} {
		v.Mem[1].SetInt(val)
		v.PC = 2
		v.execAggStep(v.Program[1])
	}

	v.execAggFinal(&Instruction{Opcode: OpAggFinal, P1: 0, P2: 5, P3: 0})
	if v.Mem[5].IntValue() != 6 {
		t.Errorf("Expected group 0 SUM=6, got %d", v.Mem[5].IntValue())
	}

	v.execAggFinal(&Instruction{Opcode: OpAggFinal, P1: 1, P2: 6, P3: 0})
	if v.Mem[6].IntValue() != 60 {
		t.Errorf("Expected group 1 SUM=60, got %d", v.Mem[6].IntValue())
	}
}

func TestAggregateOpcodesWithNullValues(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "count")

	v.Mem[1].SetInt(1)
	runAggStep(t, v)

	v.Mem[1].SetNull()
	runAggStep(t, v)

	v.Mem[1].SetInt(2)
	runAggStep(t, v)

	finalInstr := &Instruction{Opcode: OpAggFinal, P1: 0, P2: 5, P3: 0}
	v.execAggFinal(finalInstr)

	result := v.Mem[5].IntValue()
	if result != 2 && result != 3 {
		t.Errorf("Expected COUNT=2 or 3, got %d", result)
	}
}

func TestAggregateOpcodesFinalWithoutContext(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	err := v.execAggFinal(&Instruction{Opcode: OpAggFinal, P1: 0, P2: 5, P3: 0})
	if err == nil {
		t.Error("Expected error when funcCtx is nil")
	}
}

func TestAggregateOpcodesRealValues(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "sum")
	for _, val := range []float64{1.5, 2.5, 3.5} {
		v.Mem[1].SetReal(val)
		runAggStep(t, v)
	}
	finalizeAndCheckReal(t, v, 7.5)
}

func TestAggregateOpcodesStringAggregate(t *testing.T) {
	t.Parallel()
	v := setupAggTest(t, "max")
	for _, val := range []string{"apple", "zebra", "banana"} {
		v.Mem[1].SetStr(val)
		runAggStep(t, v)
	}
	finalizeAndCheckString(t, v, "zebra")
}
