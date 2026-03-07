// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
)

// TestScalarFunctions tests scalar function execution
func TestScalarFunctions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		funcName string
		args     []*Mem
		want     *Mem
		wantErr  bool
	}{
		{
			name:     "upper",
			funcName: "upper",
			args:     []*Mem{NewMemStr("hello")},
			want:     NewMemStr("HELLO"),
		},
		{
			name:     "lower",
			funcName: "lower",
			args:     []*Mem{NewMemStr("WORLD")},
			want:     NewMemStr("world"),
		},
		{
			name:     "length string",
			funcName: "length",
			args:     []*Mem{NewMemStr("hello")},
			want:     NewMemInt(5),
		},
		{
			name:     "length null",
			funcName: "length",
			args:     []*Mem{NewMemNull()},
			want:     NewMemNull(),
		},
		{
			name:     "substr 2 args",
			funcName: "substr",
			args:     []*Mem{NewMemStr("hello"), NewMemInt(2)},
			want:     NewMemStr("ello"),
		},
		{
			name:     "substr 3 args",
			funcName: "substr",
			args:     []*Mem{NewMemStr("hello"), NewMemInt(2), NewMemInt(3)},
			want:     NewMemStr("ell"),
		},
		{
			name:     "replace",
			funcName: "replace",
			args:     []*Mem{NewMemStr("hello world"), NewMemStr("world"), NewMemStr("Go")},
			want:     NewMemStr("hello Go"),
		},
		{
			name:     "trim default",
			funcName: "trim",
			args:     []*Mem{NewMemStr("  hello  ")},
			want:     NewMemStr("hello"),
		},
		{
			name:     "trim custom",
			funcName: "trim",
			args:     []*Mem{NewMemStr("xxxhelloxxx"), NewMemStr("x")},
			want:     NewMemStr("hello"),
		},
		{
			name:     "coalesce first non-null",
			funcName: "coalesce",
			args:     []*Mem{NewMemNull(), NewMemInt(42), NewMemStr("test")},
			want:     NewMemInt(42),
		},
		{
			name:     "coalesce all null",
			funcName: "coalesce",
			args:     []*Mem{NewMemNull(), NewMemNull()},
			want:     NewMemNull(),
		},
		{
			name:     "ifnull first",
			funcName: "ifnull",
			args:     []*Mem{NewMemInt(10), NewMemInt(20)},
			want:     NewMemInt(10),
		},
		{
			name:     "ifnull second",
			funcName: "ifnull",
			args:     []*Mem{NewMemNull(), NewMemInt(20)},
			want:     NewMemInt(20),
		},
		{
			name:     "typeof int",
			funcName: "typeof",
			args:     []*Mem{NewMemInt(42)},
			want:     NewMemStr("integer"),
		},
		{
			name:     "typeof text",
			funcName: "typeof",
			args:     []*Mem{NewMemStr("hello")},
			want:     NewMemStr("text"),
		},
		{
			name:     "typeof null",
			funcName: "typeof",
			args:     []*Mem{NewMemNull()},
			want:     NewMemStr("null"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := NewFunctionContext()
			got, err := fc.ExecuteFunction(tt.funcName, tt.args)

			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteFunction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			if !memEqual(got, tt.want) {
				t.Errorf("ExecuteFunction() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAggregateFunctions tests aggregate function execution
func TestAggregateFunctions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		funcName string
		rows     [][]*Mem // Each row is an array of arguments
		want     *Mem
		wantErr  bool
	}{
		{
			name:     "count values",
			funcName: "count",
			rows: [][]*Mem{
				{NewMemInt(1)},
				{NewMemInt(2)},
				{NewMemInt(3)},
			},
			want: NewMemInt(3),
		},
		{
			name:     "count with nulls",
			funcName: "count",
			rows: [][]*Mem{
				{NewMemInt(1)},
				{NewMemNull()},
				{NewMemInt(3)},
			},
			want: NewMemInt(2),
		},
		{
			name:     "sum integers",
			funcName: "sum",
			rows: [][]*Mem{
				{NewMemInt(10)},
				{NewMemInt(20)},
				{NewMemInt(30)},
			},
			want: NewMemInt(60),
		},
		{
			name:     "sum with nulls",
			funcName: "sum",
			rows: [][]*Mem{
				{NewMemInt(10)},
				{NewMemNull()},
				{NewMemInt(20)},
			},
			want: NewMemInt(30),
		},
		{
			name:     "sum empty",
			funcName: "sum",
			rows:     [][]*Mem{},
			want:     NewMemNull(),
		},
		{
			name:     "avg",
			funcName: "avg",
			rows: [][]*Mem{
				{NewMemInt(10)},
				{NewMemInt(20)},
				{NewMemInt(30)},
			},
			want: NewMemReal(20.0),
		},
		{
			name:     "min",
			funcName: "min",
			rows: [][]*Mem{
				{NewMemInt(30)},
				{NewMemInt(10)},
				{NewMemInt(20)},
			},
			want: NewMemInt(10),
		},
		{
			name:     "max",
			funcName: "max",
			rows: [][]*Mem{
				{NewMemInt(10)},
				{NewMemInt(30)},
				{NewMemInt(20)},
			},
			want: NewMemInt(30),
		},
		{
			name:     "group_concat default separator",
			funcName: "group_concat",
			rows: [][]*Mem{
				{NewMemStr("a")},
				{NewMemStr("b")},
				{NewMemStr("c")},
			},
			want: NewMemStr("a,b,c"),
		},
		{
			name:     "group_concat custom separator",
			funcName: "group_concat",
			rows: [][]*Mem{
				{NewMemStr("a"), NewMemStr("|")},
				{NewMemStr("b"), NewMemStr("|")},
				{NewMemStr("c"), NewMemStr("|")},
			},
			want: NewMemStr("a|b|c"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := NewFunctionContext()

			// Look up the aggregate function
			// Use LookupBuiltin for min/max since scalar versions have priority in Lookup
			var fn functions.Function
			var ok bool
			if tt.funcName == "min" || tt.funcName == "max" {
				fn, ok = fc.registry.LookupBuiltin(tt.funcName)
			} else {
				fn, ok = fc.registry.Lookup(tt.funcName)
			}
			if !ok {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			aggFn, ok := fn.(functions.AggregateFunction)
			if !ok {
				t.Fatalf("Function %s is not an aggregate function", tt.funcName)
			}

			// Create a fresh instance
			aggFn = createAggregateInstance(aggFn)

			// Execute step for each row
			for _, row := range tt.rows {
				values := make([]functions.Value, len(row))
				for i, mem := range row {
					values[i] = memToValue(mem)
				}

				err := aggFn.Step(values)
				if err != nil {
					if !tt.wantErr {
						t.Fatalf("Step() error = %v", err)
					}
					return
				}
			}

			// Finalize
			result, err := aggFn.Final()
			if (err != nil) != tt.wantErr {
				t.Errorf("Final() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			got := valueToMem(result)
			if !memEqual(got, tt.want) {
				t.Errorf("Final() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestOPFunction tests the OP_Function opcode
func TestOPFunction(t *testing.T) {
	t.Parallel()
	v := New()

	// Allocate memory for registers
	v.AllocMemory(10)

	// Set up arguments in registers 1, 2
	v.Mem[1].SetStr("hello")
	v.Mem[2].SetInt(2)
	v.Mem[3].SetInt(3)

	// Create a SUBSTR function instruction
	// substr("hello", 2, 3) = "ell"
	instr := &Instruction{
		Opcode: OpFunction,
		P1:     0, // constant mask
		P2:     1, // first argument register
		P3:     5, // output register
		P4: P4Union{
			Z: "substr",
		},
		P4Type: P4Static,
		P5:     3, // number of arguments
	}

	// Add instruction to program
	v.Program = append(v.Program, instr)
	v.PC = 1 // Set PC to after the instruction

	// Execute the function
	err := v.opFunction(instr.P1, instr.P2, instr.P3, instr.P1, int(instr.P5))
	if err != nil {
		t.Fatalf("opFunction() error = %v", err)
	}

	// Check result
	result := v.Mem[5]
	if !result.IsStr() || result.StrValue() != "ell" {
		t.Errorf("opFunction() result = %v, want 'ell'", result.StrValue())
	}
}

// TestOPAggStep tests the OP_AggStep opcode
func TestOPAggStep(t *testing.T) {
	t.Parallel()
	v := New()

	// Allocate memory and cursors
	v.AllocMemory(10)
	v.AllocCursors(1)

	cursor := 0

	// Create COUNT aggregate
	// Add values to registers
	testValues := []int64{10, 20, 30}

	for i, val := range testValues {
		v.Mem[i].SetInt(val)

		instr := &Instruction{
			Opcode: OpAggStep,
			P1:     cursor, // cursor
			P2:     i,      // first argument register
			P3:     0,      // function index
			P4: P4Union{
				Z: "count",
			},
			P4Type: P4Static,
			P5:     1, // number of arguments
		}

		v.Program = append(v.Program, instr)
		v.PC = len(v.Program)

		err := v.opAggStep(instr.P1, instr.P2, instr.P3, instr.P1, int(instr.P5))
		if err != nil {
			t.Fatalf("opAggStep() error = %v", err)
		}
	}

	// Finalize the aggregate
	err := v.opAggFinal(cursor, 5, 0)
	if err != nil {
		t.Fatalf("opAggFinal() error = %v", err)
	}

	// Check result
	result := v.Mem[5]
	if !result.IsInt() || result.IntValue() != 3 {
		t.Errorf("opAggFinal() result = %v, want 3", result.IntValue())
	}
}

// TestNestedFunctionCalls tests nested function calls
func TestNestedFunctionCalls(t *testing.T) {
	t.Parallel()
	fc := NewFunctionContext()

	// Test UPPER(LOWER("HELLO"))
	// First call LOWER("HELLO")
	inner, err := fc.ExecuteFunction("lower", []*Mem{NewMemStr("HELLO")})
	if err != nil {
		t.Fatalf("ExecuteFunction(lower) error = %v", err)
	}

	// Then call UPPER on the result
	result, err := fc.ExecuteFunction("upper", []*Mem{inner})
	if err != nil {
		t.Fatalf("ExecuteFunction(upper) error = %v", err)
	}

	if !result.IsStr() || result.StrValue() != "HELLO" {
		t.Errorf("Nested function result = %v, want 'HELLO'", result.StrValue())
	}
}

// TestNullHandling tests NULL value handling in functions
func TestNullHandling(t *testing.T) {
	t.Parallel()
	fc := NewFunctionContext()

	tests := []struct {
		name     string
		funcName string
		args     []*Mem
		wantNull bool
	}{
		{
			name:     "upper null",
			funcName: "upper",
			args:     []*Mem{NewMemNull()},
			wantNull: true,
		},
		{
			name:     "length null",
			funcName: "length",
			args:     []*Mem{NewMemNull()},
			wantNull: true,
		},
		{
			name:     "substr null string",
			funcName: "substr",
			args:     []*Mem{NewMemNull(), NewMemInt(1)},
			wantNull: true,
		},
		{
			name:     "replace null pattern",
			funcName: "replace",
			args:     []*Mem{NewMemStr("hello"), NewMemNull(), NewMemStr("x")},
			wantNull: true,
		},
		{
			name:     "coalesce with null",
			funcName: "coalesce",
			args:     []*Mem{NewMemNull(), NewMemNull(), NewMemStr("value")},
			wantNull: false, // Should return "value"
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := fc.ExecuteFunction(tt.funcName, tt.args)
			if err != nil {
				t.Fatalf("ExecuteFunction() error = %v", err)
			}

			if result.IsNull() != tt.wantNull {
				t.Errorf("ExecuteFunction() IsNull = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// Helper function to compare two Mem values
func memEqual(a, b *Mem) bool {
	if a.IsNull() && b.IsNull() {
		return true
	}
	if a.IsNull() || b.IsNull() {
		return false
	}

	if a.IsInt() && b.IsInt() {
		return a.IntValue() == b.IntValue()
	}
	if a.IsReal() && b.IsReal() {
		return a.RealValue() == b.RealValue()
	}
	if a.IsStr() && b.IsStr() {
		return a.StrValue() == b.StrValue()
	}
	if a.IsBlob() && b.IsBlob() {
		aBlob := a.BlobValue()
		bBlob := b.BlobValue()
		if len(aBlob) != len(bBlob) {
			return false
		}
		for i := range aBlob {
			if aBlob[i] != bBlob[i] {
				return false
			}
		}
		return true
	}

	return false
}
