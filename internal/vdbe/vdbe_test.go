package vdbe

import (
	"testing"
)

func TestMemBasicTypes(t *testing.T) {
	t.Parallel()
	t.Run("Null", func(t *testing.T) {
		t.Parallel()
		mem := NewMemNull()
		if !mem.IsNull() {
			t.Error("Expected NULL flag to be set")
		}
		if mem.IntValue() != 0 {
			t.Error("NULL should convert to 0 integer")
		}
	})

	t.Run("Integer", func(t *testing.T) {
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
	})

	t.Run("Real", func(t *testing.T) {
		t.Parallel()
		mem := NewMemReal(3.14159)
		if !mem.IsReal() {
			t.Error("Expected REAL flag to be set")
		}
		if mem.RealValue() != 3.14159 {
			t.Errorf("Expected 3.14159, got %f", mem.RealValue())
		}
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("hello")
		if !mem.IsStr() {
			t.Error("Expected STR flag to be set")
		}
		if mem.StrValue() != "hello" {
			t.Errorf("Expected 'hello', got '%s'", mem.StrValue())
		}
	})

	t.Run("Blob", func(t *testing.T) {
		t.Parallel()
		data := []byte{1, 2, 3, 4, 5}
		mem := NewMemBlob(data)
		if !mem.IsBlob() {
			t.Error("Expected BLOB flag to be set")
		}
		blob := mem.BlobValue()
		if len(blob) != 5 {
			t.Errorf("Expected blob length 5, got %d", len(blob))
		}
	})
}

func TestMemConversions(t *testing.T) {
	t.Parallel()
	t.Run("IntToReal", func(t *testing.T) {
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
	})

	t.Run("StringToInt", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("123")
		if err := mem.Integerify(); err != nil {
			t.Fatalf("Integerify failed: %v", err)
		}
		if mem.IntValue() != 123 {
			t.Errorf("Expected 123, got %d", mem.IntValue())
		}
	})

	t.Run("StringToReal", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("3.14")
		if err := mem.Realify(); err != nil {
			t.Fatalf("Realify failed: %v", err)
		}
		if mem.RealValue() != 3.14 {
			t.Errorf("Expected 3.14, got %f", mem.RealValue())
		}
	})

	t.Run("IntToString", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(42)
		if err := mem.Stringify(); err != nil {
			t.Fatalf("Stringify failed: %v", err)
		}
		if mem.StrValue() != "42" {
			t.Errorf("Expected '42', got '%s'", mem.StrValue())
		}
	})
}

func TestMemArithmetic(t *testing.T) {
	t.Parallel()
	t.Run("AddIntegers", func(t *testing.T) {
		t.Parallel()
		a := NewMemInt(10)
		b := NewMemInt(20)
		if err := a.Add(b); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if a.IntValue() != 30 {
			t.Errorf("Expected 30, got %d", a.IntValue())
		}
	})

	t.Run("SubtractIntegers", func(t *testing.T) {
		t.Parallel()
		a := NewMemInt(50)
		b := NewMemInt(20)
		if err := a.Subtract(b); err != nil {
			t.Fatalf("Subtract failed: %v", err)
		}
		if a.IntValue() != 30 {
			t.Errorf("Expected 30, got %d", a.IntValue())
		}
	})

	t.Run("MultiplyIntegers", func(t *testing.T) {
		t.Parallel()
		a := NewMemInt(6)
		b := NewMemInt(7)
		if err := a.Multiply(b); err != nil {
			t.Fatalf("Multiply failed: %v", err)
		}
		if a.IntValue() != 42 {
			t.Errorf("Expected 42, got %d", a.IntValue())
		}
	})

	t.Run("DivideReals", func(t *testing.T) {
		t.Parallel()
		a := NewMemReal(10.0)
		b := NewMemReal(4.0)
		if err := a.Divide(b); err != nil {
			t.Fatalf("Divide failed: %v", err)
		}
		if a.RealValue() != 2.5 {
			t.Errorf("Expected 2.5, got %f", a.RealValue())
		}
	})

	t.Run("DivideByZero", func(t *testing.T) {
		t.Parallel()
		a := NewMemInt(10)
		b := NewMemInt(0)
		if err := a.Divide(b); err != nil {
			t.Fatalf("Divide failed: %v", err)
		}
		if !a.IsNull() {
			t.Error("Expected NULL after division by zero")
		}
	})

	t.Run("Remainder", func(t *testing.T) {
		t.Parallel()
		a := NewMemInt(17)
		b := NewMemInt(5)
		if err := a.Remainder(b); err != nil {
			t.Fatalf("Remainder failed: %v", err)
		}
		if a.IntValue() != 2 {
			t.Errorf("Expected 2, got %d", a.IntValue())
		}
	})
}

func TestMemComparison(t *testing.T) {
	t.Parallel()
	t.Run("IntegerComparison", func(t *testing.T) {
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
	})

	t.Run("StringComparison", func(t *testing.T) {
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
	})

	t.Run("NullComparison", func(t *testing.T) {
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
	})
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

func TestVdbeBasicExecution(t *testing.T) {
	t.Parallel()
	t.Run("SimpleProgram", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)

		// Program: r[1] = 42; Halt
		v.AddOp(OpInteger, 42, 1, 0)
		v.AddOp(OpHalt, 0, 0, 0)

		if err := v.Run(); err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		mem, _ := v.GetMem(1)
		if mem.IntValue() != 42 {
			t.Errorf("Expected 42, got %d", mem.IntValue())
		}
	})

	t.Run("ArithmeticProgram", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)

		// Program: r[1] = 10; r[2] = 20; r[3] = r[1] + r[2]; Halt
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
	})

	t.Run("ConditionalJump", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)

		// Program: r[1] = 1; if r[1] goto 4; r[2] = 99; r[2] = 42; Halt
		v.AddOp(OpInteger, 1, 1, 0)  // 0: r[1] = 1
		v.AddOp(OpIf, 1, 4, 0)       // 1: if r[1] goto 4
		v.AddOp(OpInteger, 99, 2, 0) // 2: r[2] = 99 (should skip)
		v.AddOp(OpInteger, -1, 2, 0) // 3: r[2] = -1 (should skip)
		v.AddOp(OpInteger, 42, 2, 0) // 4: r[2] = 42
		v.AddOp(OpHalt, 0, 0, 0)     // 5: Halt

		if err := v.Run(); err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		mem, _ := v.GetMem(2)
		if mem.IntValue() != 42 {
			t.Errorf("Expected 42 (jump taken), got %d", mem.IntValue())
		}
	})

	t.Run("Loop", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)

		// Program: r[1] = 0; r[2] = 10; r[1]++; r[4] = (r[1] < r[2]); if r[4] goto 2; Halt
		v.AddOp(OpInteger, 0, 1, 0)  // 0: r[1] = 0 (counter)
		v.AddOp(OpInteger, 10, 2, 0) // 1: r[2] = 10 (limit)
		v.AddOp(OpInteger, 1, 3, 0)  // 2: r[3] = 1
		v.AddOp(OpAdd, 1, 3, 1)      // 3: r[1] = r[1] + r[3] (increment)
		v.AddOp(OpLt, 1, 2, 4)       // 4: r[4] = (r[1] < r[2])
		v.AddOp(OpIf, 4, 2, 0)       // 5: if r[4] is true, goto 2
		v.AddOp(OpHalt, 0, 0, 0)     // 6: Halt

		if err := v.Run(); err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		mem, _ := v.GetMem(1)
		if mem.IntValue() != 10 {
			t.Errorf("Expected 10, got %d", mem.IntValue())
		}
	})
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

func TestResultRowHandling(t *testing.T) {
	t.Parallel()
	t.Run("SingleRowResult", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)

		// Program: r[1] = 42; r[2] = "hello"; ResultRow(r[1], r[2]); Halt
		v.AddOp(OpInteger, 42, 1, 0)
		v.AddOpWithP4Str(OpString8, 0, 2, 0, "hello") // P2=2 is the destination register
		v.AddOp(OpResultRow, 1, 2, 0)                 // Output registers 1-2
		v.AddOp(OpHalt, 0, 0, 0)

		// First step should execute until ResultRow
		hasMore, err := v.Step()
		if err != nil {
			t.Fatalf("Step failed: %v", err)
		}

		if !hasMore {
			t.Error("Expected more steps after ResultRow")
		}

		if v.State != StateRowReady {
			t.Errorf("Expected StateRowReady, got %v", v.State)
		}

		// Check that ResultRow is populated
		if v.ResultRow == nil {
			t.Fatal("ResultRow should be populated")
		}

		if len(v.ResultRow) != 2 {
			t.Fatalf("Expected 2 columns, got %d", len(v.ResultRow))
		}

		if v.ResultRow[0].IntValue() != 42 {
			t.Errorf("Expected first column to be 42, got %d", v.ResultRow[0].IntValue())
		}

		if v.ResultRow[1].StrValue() != "hello" {
			t.Errorf("Expected second column to be 'hello', got '%s'", v.ResultRow[1].StrValue())
		}

		// Next step should clear ResultRow and halt
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
	})

	t.Run("MultipleRowResults", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)

		// Program that outputs 3 rows:
		// r[1] = 1; ResultRow(r[1]); r[1] = 2; ResultRow(r[1]); r[1] = 3; ResultRow(r[1]); Halt
		v.AddOp(OpInteger, 1, 1, 0)
		v.AddOp(OpResultRow, 1, 1, 0)
		v.AddOp(OpInteger, 2, 1, 0)
		v.AddOp(OpResultRow, 1, 1, 0)
		v.AddOp(OpInteger, 3, 1, 0)
		v.AddOp(OpResultRow, 1, 1, 0)
		v.AddOp(OpHalt, 0, 0, 0)

		// First row
		hasMore, err := v.Step()
		if err != nil {
			t.Fatalf("Step 1 failed: %v", err)
		}
		if !hasMore || v.State != StateRowReady {
			t.Error("Expected StateRowReady after first ResultRow")
		}
		if v.ResultRow[0].IntValue() != 1 {
			t.Errorf("Expected first row value 1, got %d", v.ResultRow[0].IntValue())
		}

		// Second row
		hasMore, err = v.Step()
		if err != nil {
			t.Fatalf("Step 2 failed: %v", err)
		}
		if !hasMore || v.State != StateRowReady {
			t.Error("Expected StateRowReady after second ResultRow")
		}
		if v.ResultRow[0].IntValue() != 2 {
			t.Errorf("Expected second row value 2, got %d", v.ResultRow[0].IntValue())
		}

		// Third row
		hasMore, err = v.Step()
		if err != nil {
			t.Fatalf("Step 3 failed: %v", err)
		}
		if !hasMore || v.State != StateRowReady {
			t.Error("Expected StateRowReady after third ResultRow")
		}
		if v.ResultRow[0].IntValue() != 3 {
			t.Errorf("Expected third row value 3, got %d", v.ResultRow[0].IntValue())
		}

		// Halt
		hasMore, err = v.Step()
		if err != nil {
			t.Fatalf("Step 4 failed: %v", err)
		}
		if hasMore || v.State != StateHalt {
			t.Error("Expected StateHalt after processing all rows")
		}
	})
}

// contains is defined in exec_transaction_test.go
