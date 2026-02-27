package vdbe

import (
	"testing"
)

// TestOpCast tests the Cast opcode with various affinity types.
func TestOpCast(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    interface{}
		affinity      int // 0=NONE, 1=BLOB, 2=TEXT, 3=INTEGER, 4=REAL, 5=NUMERIC
		expectedType  string
		expectedValue interface{}
	}{
		// NONE/BLOB affinity (0) - keep as-is
		{"NONE affinity on int", int64(42), 0, "int", int64(42)},
		{"NONE affinity on real", 3.14, 0, "real", 3.14},
		{"NONE affinity on text", "hello", 0, "string", "hello"},

		// BLOB affinity (1)
		{"BLOB affinity on text", "hello", 1, "blob", []byte("hello")},
		{"BLOB affinity on int", int64(42), 1, "blob", []byte("42")},
		{"BLOB affinity on real", 3.14, 1, "blob", []byte("3.14")},

		// TEXT affinity (2)
		{"TEXT affinity on int", int64(42), 2, "string", "42"},
		{"TEXT affinity on real", 3.14, 2, "string", "3.14"},
		{"TEXT affinity on text", "hello", 2, "string", "hello"},
		{"TEXT affinity on blob", []byte("world"), 2, "string", "world"},

		// INTEGER affinity (3)
		{"INTEGER affinity on int", int64(42), 3, "int", int64(42)},
		{"INTEGER affinity on real", 3.14, 3, "int", int64(3)},
		{"INTEGER affinity on text valid", "123", 3, "int", int64(123)},
		{"INTEGER affinity on text invalid", "hello", 3, "null", nil},
		{"INTEGER affinity on text float", "3.14", 3, "int", int64(3)},

		// REAL affinity (4)
		{"REAL affinity on int", int64(42), 4, "real", 42.0},
		{"REAL affinity on real", 3.14, 4, "real", 3.14},
		{"REAL affinity on text", "3.14", 4, "real", 3.14},

		// NUMERIC affinity (5)
		{"NUMERIC affinity on int", int64(42), 5, "int", int64(42)},
		{"NUMERIC affinity on real", 3.14, 5, "real", 3.14},
		{"NUMERIC affinity on text int", "123", 5, "int", int64(123)},
		{"NUMERIC affinity on text real", "3.14", 5, "real", 3.14},
		{"NUMERIC affinity on text invalid", "hello", 5, "string", "hello"},

		// NULL handling
		{"NULL with any affinity", nil, 2, "null", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := New()
			v.AllocMemory(2)

			// Set up input value
			mem := v.Mem[1]
			setMemValue(mem, tt.inputValue)

			// Execute Cast opcode
			instr := &Instruction{
				Opcode: OpCast,
				P1:     1,
				P2:     tt.affinity,
			}

			err := v.execCast(instr)
			if err != nil {
				t.Fatalf("execCast failed: %v", err)
			}

			// Verify result type and value
			verifyMemValue(t, mem, tt.expectedType, tt.expectedValue)
		})
	}
}

// TestOpToText tests the ToText opcode.
func TestOpToText(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    interface{}
		expectedValue string
	}{
		{"int to text", int64(42), "42"},
		{"real to text", 3.14, "3.14"},
		{"text to text", "hello", "hello"},
		{"blob to text", []byte("world"), "world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := New()
			v.AllocMemory(2)

			mem := v.Mem[1]
			setMemValue(mem, tt.inputValue)

			instr := &Instruction{
				Opcode: OpToText,
				P1:     1,
			}

			err := v.execToText(instr)
			if err != nil {
				t.Fatalf("execToText failed: %v", err)
			}

			if !mem.IsString() {
				t.Errorf("expected string type, got %s", getMemType(mem))
			}
			if mem.StrValue() != tt.expectedValue {
				t.Errorf("expected %q, got %q", tt.expectedValue, mem.StrValue())
			}
		})
	}
}

// TestOpToTextNull tests ToText with NULL value.
func TestOpToTextNull(t *testing.T) {
	v := New()
	v.AllocMemory(2)

	mem := v.Mem[1]
	mem.SetNull()

	instr := &Instruction{
		Opcode: OpToText,
		P1:     1,
	}

	err := v.execToText(instr)
	if err != nil {
		t.Fatalf("execToText failed: %v", err)
	}

	if !mem.IsNull() {
		t.Errorf("expected NULL to remain NULL")
	}
}

// TestOpToBlob tests the ToBlob opcode.
func TestOpToBlob(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    interface{}
		expectedValue []byte
	}{
		{"text to blob", "hello", []byte("hello")},
		{"int to blob", int64(42), []byte("42")},
		{"real to blob", 3.14, []byte("3.14")},
		{"blob to blob", []byte("world"), []byte("world")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := New()
			v.AllocMemory(2)

			mem := v.Mem[1]
			setMemValue(mem, tt.inputValue)

			instr := &Instruction{
				Opcode: OpToBlob,
				P1:     1,
			}

			err := v.execToBlob(instr)
			if err != nil {
				t.Fatalf("execToBlob failed: %v", err)
			}

			if !mem.IsBlob() {
				t.Errorf("expected blob type, got %s", getMemType(mem))
			}

			result := mem.BlobValue()
			if string(result) != string(tt.expectedValue) {
				t.Errorf("expected %v, got %v", tt.expectedValue, result)
			}
		})
	}
}

// TestOpToBlobNull tests ToBlob with NULL value.
func TestOpToBlobNull(t *testing.T) {
	v := New()
	v.AllocMemory(2)

	mem := v.Mem[1]
	mem.SetNull()

	instr := &Instruction{
		Opcode: OpToBlob,
		P1:     1,
	}

	err := v.execToBlob(instr)
	if err != nil {
		t.Fatalf("execToBlob failed: %v", err)
	}

	if !mem.IsNull() {
		t.Errorf("expected NULL to remain NULL")
	}
}

// TestOpToNumeric tests the ToNumeric opcode.
func TestOpToNumeric(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    interface{}
		expectedType  string
		expectedValue interface{}
	}{
		{"int stays int", int64(42), "int", int64(42)},
		{"real stays real", 3.14, "real", 3.14},
		{"text int becomes int", "123", "int", int64(123)},
		{"text real becomes real", "3.14", "real", 3.14},
		{"text invalid stays text", "hello", "string", "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := New()
			v.AllocMemory(2)

			mem := v.Mem[1]
			setMemValue(mem, tt.inputValue)

			instr := &Instruction{
				Opcode: OpToNumeric,
				P1:     1,
			}

			err := v.execToNumeric(instr)
			if err != nil {
				t.Fatalf("execToNumeric failed: %v", err)
			}

			verifyMemValue(t, mem, tt.expectedType, tt.expectedValue)
		})
	}
}

// TestOpToNumericNull tests ToNumeric with NULL value.
func TestOpToNumericNull(t *testing.T) {
	v := New()
	v.AllocMemory(2)

	mem := v.Mem[1]
	mem.SetNull()

	instr := &Instruction{
		Opcode: OpToNumeric,
		P1:     1,
	}

	err := v.execToNumeric(instr)
	if err != nil {
		t.Fatalf("execToNumeric failed: %v", err)
	}

	if !mem.IsNull() {
		t.Errorf("expected NULL to remain NULL")
	}
}

// TestOpToInt tests the ToInt opcode.
func TestOpToInt(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    interface{}
		expectedValue int64
	}{
		{"int stays int", int64(42), 42},
		{"real truncates", 3.14, 3},
		{"real truncates negative", -3.14, -3},
		{"text int becomes int", "123", 123},
		{"text real truncates", "3.14", 3},
		{"text invalid becomes 0", "hello", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := New()
			v.AllocMemory(2)

			mem := v.Mem[1]
			setMemValue(mem, tt.inputValue)

			instr := &Instruction{
				Opcode: OpToInt,
				P1:     1,
			}

			err := v.execToInt(instr)
			if err != nil {
				t.Fatalf("execToInt failed: %v", err)
			}

			if !mem.IsInt() {
				t.Errorf("expected int type, got %s", getMemType(mem))
			}
			if mem.IntValue() != tt.expectedValue {
				t.Errorf("expected %d, got %d", tt.expectedValue, mem.IntValue())
			}
		})
	}
}

// TestOpToIntNull tests ToInt with NULL value.
func TestOpToIntNull(t *testing.T) {
	v := New()
	v.AllocMemory(2)

	mem := v.Mem[1]
	mem.SetNull()

	instr := &Instruction{
		Opcode: OpToInt,
		P1:     1,
	}

	err := v.execToInt(instr)
	if err != nil {
		t.Fatalf("execToInt failed: %v", err)
	}

	if !mem.IsNull() {
		t.Errorf("expected NULL to remain NULL")
	}
}

// TestOpToReal tests the ToReal opcode.
func TestOpToReal(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    interface{}
		expectedValue float64
	}{
		{"int to real", int64(42), 42.0},
		{"real stays real", 3.14, 3.14},
		{"text to real", "3.14", 3.14},
		{"text int to real", "42", 42.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := New()
			v.AllocMemory(2)

			mem := v.Mem[1]
			setMemValue(mem, tt.inputValue)

			instr := &Instruction{
				Opcode: OpToReal,
				P1:     1,
			}

			err := v.execToReal(instr)
			if err != nil {
				t.Fatalf("execToReal failed: %v", err)
			}

			if !mem.IsReal() {
				t.Errorf("expected real type, got %s", getMemType(mem))
			}
			if mem.RealValue() != tt.expectedValue {
				t.Errorf("expected %f, got %f", tt.expectedValue, mem.RealValue())
			}
		})
	}
}

// TestOpToRealNull tests ToReal with NULL value.
func TestOpToRealNull(t *testing.T) {
	v := New()
	v.AllocMemory(2)

	mem := v.Mem[1]
	mem.SetNull()

	instr := &Instruction{
		Opcode: OpToReal,
		P1:     1,
	}

	err := v.execToReal(instr)
	if err != nil {
		t.Fatalf("execToReal failed: %v", err)
	}

	if !mem.IsNull() {
		t.Errorf("expected NULL to remain NULL")
	}
}

// TestTypeConversionIntegration tests type conversions in a VDBE program.
func TestTypeConversionIntegration(t *testing.T) {
	v := New()
	v.AllocMemory(10)

	// Test program that performs various type conversions
	v.Program = []*Instruction{
		// Load test values
		{Opcode: OpInteger, P1: 42, P2: 1},       // r1 = 42
		{Opcode: OpReal, P2: 2, P4Type: P4Real, P4: P4Union{R: 3.14}}, // r2 = 3.14
		{Opcode: OpString, P2: 3, P4Type: P4Static, P4: P4Union{Z: "123"}}, // r3 = "123"
		{Opcode: OpString, P2: 4, P4Type: P4Static, P4: P4Union{Z: "hello"}}, // r4 = "hello"

		// Test conversions
		{Opcode: OpToText, P1: 1},    // r1 = "42"
		{Opcode: OpToInt, P1: 2},     // r2 = 3 (truncated)
		{Opcode: OpToNumeric, P1: 3}, // r3 = 123 (int)
		{Opcode: OpToNumeric, P1: 4}, // r4 = "hello" (stays text)

		{Opcode: OpHalt, P1: 0},
	}

	// Run the program
	err := v.Run()
	if err != nil {
		t.Fatalf("VDBE execution failed: %v", err)
	}

	// Verify results
	if !v.Mem[1].IsString() || v.Mem[1].StrValue() != "42" {
		t.Errorf("r1: expected string '42', got %v", v.Mem[1])
	}
	if !v.Mem[2].IsInt() || v.Mem[2].IntValue() != 3 {
		t.Errorf("r2: expected int 3, got %v", v.Mem[2])
	}
	if !v.Mem[3].IsInt() || v.Mem[3].IntValue() != 123 {
		t.Errorf("r3: expected int 123, got %v", v.Mem[3])
	}
	if !v.Mem[4].IsString() || v.Mem[4].StrValue() != "hello" {
		t.Errorf("r4: expected string 'hello', got %v", v.Mem[4])
	}
}

// TestCastAffinityTypes tests all affinity types with Cast opcode.
func TestCastAffinityTypes(t *testing.T) {
	v := New()
	v.AllocMemory(10)

	// Test NUMERIC affinity with various inputs
	v.Program = []*Instruction{
		{Opcode: OpString, P2: 1, P4Type: P4Static, P4: P4Union{Z: "42"}},
		{Opcode: OpCast, P1: 1, P2: 5}, // NUMERIC affinity
		{Opcode: OpString, P2: 2, P4Type: P4Static, P4: P4Union{Z: "3.14"}},
		{Opcode: OpCast, P1: 2, P2: 5}, // NUMERIC affinity
		{Opcode: OpString, P2: 3, P4Type: P4Static, P4: P4Union{Z: "abc"}},
		{Opcode: OpCast, P1: 3, P2: 5}, // NUMERIC affinity
		{Opcode: OpHalt, P1: 0},
	}

	err := v.Run()
	if err != nil {
		t.Fatalf("VDBE execution failed: %v", err)
	}

	// "42" should become int 42
	if !v.Mem[1].IsInt() || v.Mem[1].IntValue() != 42 {
		t.Errorf("r1: expected int 42, got %v", v.Mem[1])
	}

	// "3.14" should become real 3.14
	if !v.Mem[2].IsReal() || v.Mem[2].RealValue() != 3.14 {
		t.Errorf("r2: expected real 3.14, got %v", v.Mem[2])
	}

	// "abc" should stay as string
	if !v.Mem[3].IsString() || v.Mem[3].StrValue() != "abc" {
		t.Errorf("r3: expected string 'abc', got %v", v.Mem[3])
	}
}

// Helper functions

func setMemValue(mem *Mem, value interface{}) {
	if value == nil {
		mem.SetNull()
		return
	}

	switch v := value.(type) {
	case int64:
		mem.SetInt(v)
	case int:
		mem.SetInt(int64(v))
	case float64:
		mem.SetReal(v)
	case string:
		mem.SetStr(v)
	case []byte:
		mem.SetBlob(v)
	default:
		panic("unsupported value type")
	}
}

func verifyMemValue(t *testing.T, mem *Mem, expectedType string, expectedValue interface{}) {
	switch expectedType {
	case "null":
		if !mem.IsNull() {
			t.Errorf("expected NULL, got %s: %v", getMemType(mem), mem.Value())
		}
	case "int":
		if !mem.IsInt() {
			t.Errorf("expected int type, got %s", getMemType(mem))
		} else if mem.IntValue() != expectedValue.(int64) {
			t.Errorf("expected %d, got %d", expectedValue.(int64), mem.IntValue())
		}
	case "real":
		if !mem.IsReal() {
			t.Errorf("expected real type, got %s", getMemType(mem))
		} else if mem.RealValue() != expectedValue.(float64) {
			t.Errorf("expected %f, got %f", expectedValue.(float64), mem.RealValue())
		}
	case "string":
		if !mem.IsString() {
			t.Errorf("expected string type, got %s", getMemType(mem))
		} else if mem.StrValue() != expectedValue.(string) {
			t.Errorf("expected %q, got %q", expectedValue.(string), mem.StrValue())
		}
	case "blob":
		if !mem.IsBlob() {
			t.Errorf("expected blob type, got %s", getMemType(mem))
		} else if string(mem.BlobValue()) != string(expectedValue.([]byte)) {
			t.Errorf("expected %v, got %v", expectedValue.([]byte), mem.BlobValue())
		}
	default:
		t.Errorf("unknown expected type: %s", expectedType)
	}
}

func getMemType(mem *Mem) string {
	if mem.IsNull() {
		return "NULL"
	}
	if mem.IsInt() {
		return "INT"
	}
	if mem.IsReal() {
		return "REAL"
	}
	if mem.IsString() {
		return "STRING"
	}
	if mem.IsBlob() {
		return "BLOB"
	}
	return "UNKNOWN"
}
