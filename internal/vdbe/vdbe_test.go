// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

func testMemNull(t *testing.T) {
	t.Parallel()
	mem := NewMemNull()
	if !mem.IsNull() {
		t.Error("Expected NULL flag to be set")
	}
	if mem.IntValue() != 0 {
		t.Error("NULL should convert to 0 integer")
	}
}

func testMemInteger(t *testing.T) {
	t.Parallel()
	mem := NewMemInt(42)
	if !mem.IsInt() {
		t.Error("Expected INT flag to be set")
	}
	if mem.IntValue() != 42 {
		t.Errorf("Expected 42, got %d", mem.IntValue())
	}
	if mem.StrValue() != "42" {
		t.Errorf("Expected '42', got '%s'", mem.StrValue())
	}
}

func testMemReal(t *testing.T) {
	t.Parallel()
	mem := NewMemReal(3.14159)
	if !mem.IsReal() {
		t.Error("Expected REAL flag to be set")
	}
	if mem.RealValue() != 3.14159 {
		t.Errorf("Expected 3.14159, got %f", mem.RealValue())
	}
}

func testMemString(t *testing.T) {
	t.Parallel()
	mem := NewMemStr("hello")
	if !mem.IsStr() {
		t.Error("Expected STR flag to be set")
	}
	if mem.StrValue() != "hello" {
		t.Errorf("Expected 'hello', got '%s'", mem.StrValue())
	}
}

func testMemBlob(t *testing.T) {
	t.Parallel()
	data := []byte{1, 2, 3, 4, 5}
	mem := NewMemBlob(data)
	if !mem.IsBlob() {
		t.Error("Expected BLOB flag to be set")
	}
	if len(mem.BlobValue()) != 5 {
		t.Errorf("Expected blob length 5, got %d", len(mem.BlobValue()))
	}
}

func TestMemBasicTypes(t *testing.T) {
	t.Parallel()
	t.Run("Null", testMemNull)
	t.Run("Integer", testMemInteger)
	t.Run("Real", testMemReal)
	t.Run("String", testMemString)
	t.Run("Blob", testMemBlob)
}

func testIntToReal(t *testing.T) {
	t.Parallel()
	mem := NewMemInt(42)
	if err := mem.Realify(); err != nil {
		t.Fatalf("Realify failed: %v", err)
	}
	if !mem.IsReal() {
		t.Error("Expected REAL flag after conversion")
	}
	if mem.RealValue() != 42.0 {
		t.Errorf("Expected 42.0, got %f", mem.RealValue())
	}
}

func testStringToInt(t *testing.T) {
	t.Parallel()
	mem := NewMemStr("123")
	if err := mem.Integerify(); err != nil {
		t.Fatalf("Integerify failed: %v", err)
	}
	if mem.IntValue() != 123 {
		t.Errorf("Expected 123, got %d", mem.IntValue())
	}
}

func testStringToReal(t *testing.T) {
	t.Parallel()
	mem := NewMemStr("3.14")
	if err := mem.Realify(); err != nil {
		t.Fatalf("Realify failed: %v", err)
	}
	if mem.RealValue() != 3.14 {
		t.Errorf("Expected 3.14, got %f", mem.RealValue())
	}
}

func testIntToString(t *testing.T) {
	t.Parallel()
	mem := NewMemInt(42)
	if err := mem.Stringify(); err != nil {
		t.Fatalf("Stringify failed: %v", err)
	}
	if mem.StrValue() != "42" {
		t.Errorf("Expected '42', got '%s'", mem.StrValue())
	}
}

func TestMemConversions(t *testing.T) {
	t.Parallel()
	t.Run("IntToReal", testIntToReal)
	t.Run("StringToInt", testStringToInt)
	t.Run("StringToReal", testStringToReal)
	t.Run("IntToString", testIntToString)
}

func testAddIntegers(t *testing.T) {
	t.Parallel()
	a := NewMemInt(10)
	if err := a.Add(NewMemInt(20)); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if a.IntValue() != 30 {
		t.Errorf("Expected 30, got %d", a.IntValue())
	}
}

func testSubtractIntegers(t *testing.T) {
	t.Parallel()
	a := NewMemInt(50)
	if err := a.Subtract(NewMemInt(20)); err != nil {
		t.Fatalf("Subtract failed: %v", err)
	}
	if a.IntValue() != 30 {
		t.Errorf("Expected 30, got %d", a.IntValue())
	}
}

func testMultiplyIntegers(t *testing.T) {
	t.Parallel()
	a := NewMemInt(6)
	if err := a.Multiply(NewMemInt(7)); err != nil {
		t.Fatalf("Multiply failed: %v", err)
	}
	if a.IntValue() != 42 {
		t.Errorf("Expected 42, got %d", a.IntValue())
	}
}

func testDivideReals(t *testing.T) {
	t.Parallel()
	a := NewMemReal(10.0)
	if err := a.Divide(NewMemReal(4.0)); err != nil {
		t.Fatalf("Divide failed: %v", err)
	}
	if a.RealValue() != 2.5 {
		t.Errorf("Expected 2.5, got %f", a.RealValue())
	}
}

func testDivideByZero(t *testing.T) {
	t.Parallel()
	a := NewMemInt(10)
	if err := a.Divide(NewMemInt(0)); err != nil {
		t.Fatalf("Divide failed: %v", err)
	}
	if !a.IsNull() {
		t.Error("Expected NULL after division by zero")
	}
}

func testRemainder(t *testing.T) {
	t.Parallel()
	a := NewMemInt(17)
	if err := a.Remainder(NewMemInt(5)); err != nil {
		t.Fatalf("Remainder failed: %v", err)
	}
	if a.IntValue() != 2 {
		t.Errorf("Expected 2, got %d", a.IntValue())
	}
}

func TestMemArithmetic(t *testing.T) {
	t.Parallel()
	t.Run("AddIntegers", testAddIntegers)
	t.Run("SubtractIntegers", testSubtractIntegers)
	t.Run("MultiplyIntegers", testMultiplyIntegers)
	t.Run("DivideReals", testDivideReals)
	t.Run("DivideByZero", testDivideByZero)
	t.Run("Remainder", testRemainder)
}

func testMemComparisonInteger(t *testing.T) {
	t.Parallel()
	a := NewMemInt(10)
	b := NewMemInt(20)
	c := NewMemInt(10)

	if a.Compare(b) != -1 {
		t.Error("10 should be less than 20")
	}
	if b.Compare(a) != 1 {
		t.Error("20 should be greater than 10")
	}
	if a.Compare(c) != 0 {
		t.Error("10 should equal 10")
	}
}

func testMemComparisonString(t *testing.T) {
	t.Parallel()
	a := NewMemStr("apple")
	b := NewMemStr("banana")
	c := NewMemStr("apple")

	if a.Compare(b) != -1 {
		t.Error("'apple' should be less than 'banana'")
	}
	if b.Compare(a) != 1 {
		t.Error("'banana' should be greater than 'apple'")
	}
	if a.Compare(c) != 0 {
		t.Error("'apple' should equal 'apple'")
	}
}

func testMemComparisonNull(t *testing.T) {
	t.Parallel()
	a := NewMemNull()
	b := NewMemNull()
	c := NewMemInt(42)

	if a.Compare(b) != 0 {
		t.Error("NULL should equal NULL")
	}
	if a.Compare(c) != -1 {
		t.Error("NULL should be less than any value")
	}
}

func TestMemComparison(t *testing.T) {
	t.Parallel()
	t.Run("IntegerComparison", testMemComparisonInteger)
	t.Run("StringComparison", testMemComparisonString)
	t.Run("NullComparison", testMemComparisonNull)
}

func TestMemCopyMove(t *testing.T) {
	t.Parallel()
	t.Run("DeepCopy", func(t *testing.T) {
		t.Parallel()
		src := NewMemStr("hello")
		dst := NewMem()
		if err := dst.Copy(src); err != nil {
			t.Fatalf("Copy failed: %v", err)
		}

		if dst.StrValue() != "hello" {
			t.Error("Copy didn't preserve string value")
		}

		// Modify source shouldn't affect destination
		src.SetStr("world")
		if dst.StrValue() != "hello" {
			t.Error("Deep copy was affected by source modification")
		}
	})

	t.Run("Move", func(t *testing.T) {
		t.Parallel()
		src := NewMemInt(42)
		dst := NewMem()
		dst.Move(src)

		if dst.IntValue() != 42 {
			t.Error("Move didn't transfer value")
		}
		if src.flags != MemUndefined {
			t.Errorf("Source should be undefined after move, got flags=%v", src.flags)
		}
	})

	t.Run("ShallowCopy", func(t *testing.T) {
		t.Parallel()
		src := NewMemStr("test")
		dst := NewMem()
		dst.ShallowCopy(src)

		if dst.StrValue() != "test" {
			t.Error("Shallow copy didn't preserve value")
		}
	})
}

func testVdbeSimpleProgram(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)
	v.AddOp(OpInteger, 42, 1, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	if err := v.Run(); err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	mem, _ := v.GetMem(1)
	if mem.IntValue() != 42 {
		t.Errorf("Expected 42, got %d", mem.IntValue())
	}
}

func testVdbeArithmeticProgram(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)
	v.AddOp(OpInteger, 10, 1, 0)
	v.AddOp(OpInteger, 20, 2, 0)
	v.AddOp(OpAdd, 1, 2, 3)
	v.AddOp(OpHalt, 0, 0, 0)
	if err := v.Run(); err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	mem, _ := v.GetMem(3)
	if mem.IntValue() != 30 {
		t.Errorf("Expected 30, got %d", mem.IntValue())
	}
}

func testVdbeConditionalJump(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)
	v.AddOp(OpInteger, 1, 1, 0)
	v.AddOp(OpIf, 1, 4, 0)
	v.AddOp(OpInteger, 99, 2, 0)
	v.AddOp(OpInteger, -1, 2, 0)
	v.AddOp(OpInteger, 42, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	if err := v.Run(); err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	mem, _ := v.GetMem(2)
	if mem.IntValue() != 42 {
		t.Errorf("Expected 42 (jump taken), got %d", mem.IntValue())
	}
}

func testVdbeLoop(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)
	v.AddOp(OpInteger, 0, 1, 0)
	v.AddOp(OpInteger, 10, 2, 0)
	v.AddOp(OpInteger, 1, 3, 0)
	v.AddOp(OpAdd, 1, 3, 1)
	v.AddOp(OpLt, 1, 2, 4)
	v.AddOp(OpIf, 4, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	if err := v.Run(); err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	mem, _ := v.GetMem(1)
	if mem.IntValue() != 10 {
		t.Errorf("Expected 10, got %d", mem.IntValue())
	}
}

func TestVdbeBasicExecution(t *testing.T) {
	t.Parallel()
	t.Run("SimpleProgram", testVdbeSimpleProgram)
	t.Run("ArithmeticProgram", testVdbeArithmeticProgram)
	t.Run("ConditionalJump", testVdbeConditionalJump)
	t.Run("Loop", testVdbeLoop)
}

func TestVdbeComparison(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		opcode Opcode
		a, b   int64
		result int64 // Expected result (1=true, 0=false)
	}{
		{"EqTrue", OpEq, 5, 5, 1},
		{"EqFalse", OpEq, 5, 10, 0},
		{"NeTrue", OpNe, 5, 10, 1},
		{"NeFalse", OpNe, 5, 5, 0},
		{"LtTrue", OpLt, 5, 10, 1},
		{"LtFalse", OpLt, 10, 5, 0},
		{"LeTrue", OpLe, 5, 10, 1},
		{"LeEqual", OpLe, 5, 5, 1},
		{"LeFalse", OpLe, 10, 5, 0},
		{"GtTrue", OpGt, 10, 5, 1},
		{"GtFalse", OpGt, 5, 10, 0},
		{"GeTrue", OpGe, 10, 5, 1},
		{"GeEqual", OpGe, 5, 5, 1},
		{"GeFalse", OpGe, 5, 10, 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			v.AllocMemory(10)

			// Program: r[1] = a; r[2] = b; compare(r[1], r[2]) -> r[3]; Halt
			// P1 = left operand register
			// P2 = right operand register
			// P3 = result register (1=true, 0=false)
			v.AddOp(OpInteger, int(tt.a), 1, 0)
			v.AddOp(OpInteger, int(tt.b), 2, 0)
			v.AddOp(tt.opcode, 1, 2, 3)
			v.AddOp(OpHalt, 0, 0, 0)

			if err := v.Run(); err != nil {
				t.Fatalf("Execution failed: %v", err)
			}

			mem, _ := v.GetMem(3)
			if mem.IntValue() != tt.result {
				t.Errorf("Expected result=%d, got result=%d", tt.result, mem.IntValue())
			}
		})
	}
}

func TestVdbeExplain(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)

	v.AddOp(OpInteger, 42, 1, 0)
	v.SetComment(0, "Load constant 42")
	v.AddOp(OpResultRow, 1, 1, 0)
	v.SetComment(1, "Output result")
	v.AddOp(OpHalt, 0, 0, 0)

	explain := v.ExplainProgram()
	if explain == "" {
		t.Error("ExplainProgram should return non-empty string")
	}

	// Check that it contains expected opcode names
	if !contains(explain, "Integer") {
		t.Error("ExplainProgram should contain 'Integer' opcode")
	}
	if !contains(explain, "ResultRow") {
		t.Error("ExplainProgram should contain 'ResultRow' opcode")
	}
	if !contains(explain, "Halt") {
		t.Error("ExplainProgram should contain 'Halt' opcode")
	}
}

func TestVdbeCursorOperations(t *testing.T) {
	t.Parallel()
	t.Run("OpenAndClose", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocCursors(5)

		err := v.OpenCursor(0, CursorBTree, 1, true)
		if err != nil {
			t.Fatalf("OpenCursor failed: %v", err)
		}

		cursor, err := v.GetCursor(0)
		if err != nil {
			t.Fatalf("GetCursor failed: %v", err)
		}

		if cursor.CurType != CursorBTree {
			t.Error("Cursor type mismatch")
		}

		err = v.CloseCursor(0)
		if err != nil {
			t.Fatalf("CloseCursor failed: %v", err)
		}

		_, err = v.GetCursor(0)
		if err == nil {
			t.Error("Expected error when getting closed cursor")
		}
	})
}

func TestVdbeReset(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)

	v.AddOp(OpInteger, 42, 1, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	// Run once
	if err := v.Run(); err != nil {
		t.Fatalf("First run failed: %v", err)
	}

	mem, _ := v.GetMem(1)
	if mem.IntValue() != 42 {
		t.Error("First run didn't set value")
	}

	// Reset
	if err := v.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Run again
	if err := v.Run(); err != nil {
		t.Fatalf("Second run failed: %v", err)
	}

	mem, _ = v.GetMem(1)
	if mem.IntValue() != 42 {
		t.Error("Second run didn't set value")
	}
}

func verifyResultRowColumn(t *testing.T, row []*Mem, i int, expected interface{}) {
	t.Helper()
	switch exp := expected.(type) {
	case int64:
		if row[i].IntValue() != exp {
			t.Errorf("Column %d: expected %d, got %d", i, exp, row[i].IntValue())
		}
	case string:
		if row[i].StrValue() != exp {
			t.Errorf("Column %d: expected '%s', got '%s'", i, exp, row[i].StrValue())
		}
	}
}

// Helper to verify result row state and content
func verifyResultRow(t *testing.T, v *VDBE, expectedCols int, expectedValues []interface{}) {
	t.Helper()
	if v.State != StateRowReady {
		t.Errorf("Expected StateRowReady, got %v", v.State)
	}
	if v.ResultRow == nil {
		t.Fatal("ResultRow should be populated")
	}
	if len(v.ResultRow) != expectedCols {
		t.Fatalf("Expected %d columns, got %d", expectedCols, len(v.ResultRow))
	}
	for i, expected := range expectedValues {
		verifyResultRowColumn(t, v.ResultRow, i, expected)
	}
}

// Helper to step and verify row
func stepAndVerifyRow(t *testing.T, v *VDBE, expectedValue int64) {
	t.Helper()
	hasMore, err := v.Step()
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}
	if !hasMore || v.State != StateRowReady {
		t.Error("Expected StateRowReady after ResultRow")
	}
	if v.ResultRow[0].IntValue() != expectedValue {
		t.Errorf("Expected row value %d, got %d", expectedValue, v.ResultRow[0].IntValue())
	}
}

func testResultRowSingle(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)

	v.AddOp(OpInteger, 42, 1, 0)
	v.AddOpWithP4Str(OpString8, 0, 2, 0, "hello")
	v.AddOp(OpResultRow, 1, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	hasMore, err := v.Step()
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}
	if !hasMore {
		t.Error("Expected more steps after ResultRow")
	}

	verifyResultRow(t, v, 2, []interface{}{int64(42), "hello"})

	hasMore, err = v.Step()
	if err != nil {
		t.Fatalf("Second step failed: %v", err)
	}
	if hasMore {
		t.Error("Expected no more steps after Halt")
	}
	if v.State != StateHalt {
		t.Errorf("Expected StateHalt, got %v", v.State)
	}
}

func testResultRowMultiple(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)

	v.AddOp(OpInteger, 1, 1, 0)
	v.AddOp(OpResultRow, 1, 1, 0)
	v.AddOp(OpInteger, 2, 1, 0)
	v.AddOp(OpResultRow, 1, 1, 0)
	v.AddOp(OpInteger, 3, 1, 0)
	v.AddOp(OpResultRow, 1, 1, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	stepAndVerifyRow(t, v, 1)
	stepAndVerifyRow(t, v, 2)
	stepAndVerifyRow(t, v, 3)

	hasMore, err := v.Step()
	if err != nil {
		t.Fatalf("Step 4 failed: %v", err)
	}
	if hasMore || v.State != StateHalt {
		t.Error("Expected StateHalt after processing all rows")
	}
}

func TestResultRowHandling(t *testing.T) {
	t.Parallel()
	t.Run("SingleRowResult", testResultRowSingle)
	t.Run("MultipleRowResults", testResultRowMultiple)
}

// contains is defined in exec_transaction_test.go
