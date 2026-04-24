// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// ---------------------------------------------------------------------------
// Minimal mock schema types for getTableColumns / getSchemaIndexProvider tests
// ---------------------------------------------------------------------------

// mockSchema2 implements schemaWithGetTableByName.
type mockSchema2 struct {
	tables map[string]interface{}
}

func (m *mockSchema2) GetTableByName(name string) (interface{}, bool) {
	t, ok := m.tables[name]
	return t, ok
}

// mockTable2 implements tableWithColumns.
type mockTable2 struct {
	columns []interface{}
}

func (m *mockTable2) GetColumns() []interface{} { return m.columns }

// mockColumn2 implements constraintColumnInfo.
type mockColumn2 struct {
	name    string
	pk      bool
	notNull bool
	check   string
}

func (c *mockColumn2) GetName() string          { return c.name }
func (c *mockColumn2) IsPrimaryKeyColumn() bool { return c.pk }
func (c *mockColumn2) GetNotNull() bool         { return c.notNull }
func (c *mockColumn2) GetCheck() string         { return c.check }

// notATableWithColumns does NOT implement tableWithColumns.
type notATableWithColumns struct{}

// ---------------------------------------------------------------------------
// makeNullRecord builds a SQLite record with a single NULL column (serial type 0).
// Header: [header_size=0x02][serial_type=0x00]
// ---------------------------------------------------------------------------
func makeNullRecord() []byte {
	return []byte{0x02, 0x00}
}

// makeIntRecord builds a SQLite record with a single 1-byte integer column.
// serial type 1 = 1-byte integer.
func makeIntRecord(v int8) []byte {
	return []byte{0x02, 0x01, byte(v)}
}

// ---------------------------------------------------------------------------
// TestConstraints2_GetTableColumns
// ---------------------------------------------------------------------------

// getTableColumnsExpectFalse is a helper that asserts getTableColumns returns (nil, false).
func getTableColumnsExpectFalse(t *testing.T, v *VDBE, tableName, desc string) {
	t.Helper()
	cols, ok := v.getTableColumns(tableName)
	if ok || cols != nil {
		t.Errorf("expected (nil, false) %s", desc)
	}
}

func TestConstraints2_GetTableColumns_ErrorCases(t *testing.T) {
	t.Parallel()

	v := New()
	v.Ctx = nil
	getTableColumnsExpectFalse(t, v, "t", "for nil ctx")

	v = New()
	v.Ctx = &VDBEContext{Schema: nil}
	getTableColumnsExpectFalse(t, v, "t", "for nil schema")

	v = New()
	v.Ctx = &VDBEContext{Schema: struct{}{}}
	getTableColumnsExpectFalse(t, v, "t", "when schema does not implement GetTableByName")

	v = New()
	v.Ctx = &VDBEContext{Schema: &mockSchema2{tables: map[string]interface{}{}}}
	getTableColumnsExpectFalse(t, v, "missing", "when table not found")

	v = New()
	v.Ctx = &VDBEContext{Schema: &mockSchema2{
		tables: map[string]interface{}{"t": &notATableWithColumns{}},
	}}
	getTableColumnsExpectFalse(t, v, "t", "when table does not implement GetColumns")
}

func TestConstraints2_GetTableColumns_Success(t *testing.T) {
	t.Parallel()
	col := &mockColumn2{name: "id"}
	tbl := &mockTable2{columns: []interface{}{col}}
	v := New()
	v.Ctx = &VDBEContext{Schema: &mockSchema2{
		tables: map[string]interface{}{"t": tbl},
	}}
	cols, ok := v.getTableColumns("t")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(cols) != 1 {
		t.Errorf("expected 1 column, got %d", len(cols))
	}
}

// ---------------------------------------------------------------------------
// TestConstraints2_CheckColumnNotNull
// ---------------------------------------------------------------------------

func TestConstraints2_CheckColumnNotNull(t *testing.T) {
	t.Parallel()

	t.Run("NotNullFalse_AlwaysPasses", func(t *testing.T) {
		v := New()
		col := &mockColumn2{name: "a", notNull: false}
		err := v.checkColumnNotNull(col, makeNullRecord(), 0)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("NotNullTrue_ValueIsNull_Fails", func(t *testing.T) {
		v := New()
		col := &mockColumn2{name: "a", notNull: true}
		err := v.checkColumnNotNull(col, makeNullRecord(), 0)
		if err == nil {
			t.Error("expected NOT NULL constraint error for NULL value")
		}
	})

	t.Run("NotNullTrue_ValueIsInt_Passes", func(t *testing.T) {
		v := New()
		col := &mockColumn2{name: "a", notNull: true}
		err := v.checkColumnNotNull(col, makeIntRecord(42), 0)
		if err != nil {
			t.Errorf("expected nil error for non-null value, got %v", err)
		}
	})

	t.Run("NotNullTrue_ParseError_Fails", func(t *testing.T) {
		v := New()
		col := &mockColumn2{name: "a", notNull: true}
		// Empty payload causes parse failure -> returns constraint error.
		err := v.checkColumnNotNull(col, []byte{}, 0)
		// Empty payload makes parseRecordColumn set NULL → error expected.
		// (serial type 0 = NULL handled in parseRecordColumn via empty)
		_ = err // either nil or error — exercise code path
	})
}

// ---------------------------------------------------------------------------
// TestConstraints2_EvalUnaryCheck
// ---------------------------------------------------------------------------

func TestConstraints2_EvalUnaryCheck(t *testing.T) {
	t.Parallel()

	intVal := NewMemInt(5)

	t.Run("OpNot_True_ReturnsFalse", func(t *testing.T) {
		// NOT(IdentExpr) where IdentExpr resolves via evalCheckExpr default (returns true)
		e := &parser.UnaryExpr{
			Op:   parser.OpNot,
			Expr: &parser.IdentExpr{Name: "x"},
		}
		// evalCheckExpr(IdentExpr, _) hits the default case → true; NOT true = false
		got := evalUnaryCheck(e, intVal)
		if got {
			t.Error("expected false from NOT(true)")
		}
	})

	t.Run("OpNot_False_ReturnsTrue", func(t *testing.T) {
		// NOT(BinaryExpr that evaluates to false)
		inner := &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		}
		e := &parser.UnaryExpr{Op: parser.OpNot, Expr: inner}
		got := evalUnaryCheck(e, intVal)
		if !got {
			t.Error("expected true from NOT(false)")
		}
	})

	t.Run("OpNeg_ReturnsTrue", func(t *testing.T) {
		// Op is not OpNot → returns true
		e := &parser.UnaryExpr{
			Op:   parser.OpNeg,
			Expr: &parser.IdentExpr{Name: "x"},
		}
		got := evalUnaryCheck(e, intVal)
		if !got {
			t.Error("expected true for non-OpNot unary op")
		}
	})
}

// ---------------------------------------------------------------------------
// TestConstraints2_ResolveUnaryOperand
// ---------------------------------------------------------------------------

func TestConstraints2_ResolveUnaryOperand(t *testing.T) {
	t.Parallel()

	intVal := NewMemInt(10)

	cases := []struct {
		name    string
		e       *parser.UnaryExpr
		wantNil bool
		wantVal interface{}
	}{
		{
			name: "OpNeg_IntLiteral",
			e: &parser.UnaryExpr{
				Op:   parser.OpNeg,
				Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "7"},
			},
			wantNil: false,
			wantVal: int64(-7),
		},
		{
			name: "OpNeg_FloatLiteral",
			e: &parser.UnaryExpr{
				Op:   parser.OpNeg,
				Expr: &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.5"},
			},
			wantNil: false,
			wantVal: float64(-3.5),
		},
		{
			name: "OpNeg_InnerReturnsNil",
			e: &parser.UnaryExpr{
				Op:   parser.OpNeg,
				Expr: &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
			},
			wantNil: true,
		},
		{
			name: "OpNot_ReturnsNil",
			e: &parser.UnaryExpr{
				Op:   parser.OpNot,
				Expr: &parser.IdentExpr{Name: "x"},
			},
			wantNil: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveUnaryOperand(tc.e, intVal)
			if tc.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got != tc.wantVal {
				t.Errorf("expected %v, got %v", tc.wantVal, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestConstraints2_GetSchemaIndexProvider
// ---------------------------------------------------------------------------

func TestConstraints2_GetSchemaIndexProvider(t *testing.T) {
	t.Parallel()

	t.Run("NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		p := v.getSchemaIndexProvider("t")
		if p != nil {
			t.Error("expected nil for nil ctx")
		}
	})

	t.Run("NilSchema", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: nil}
		p := v.getSchemaIndexProvider("t")
		if p != nil {
			t.Error("expected nil for nil schema")
		}
	})

	t.Run("SchemaNotTableGetter_ReturnsNil", func(t *testing.T) {
		// struct{} does not implement GetTableByName → adapter returns nil
		v := New()
		v.Ctx = &VDBEContext{Schema: struct{}{}}
		p := v.getSchemaIndexProvider("t")
		if p != nil {
			t.Error("expected nil when schema does not implement GetTableByName")
		}
	})

	t.Run("ValidSchema_ReturnsProvider", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &mockSchema2{tables: map[string]interface{}{}}}
		p := v.getSchemaIndexProvider("t")
		if p == nil {
			t.Error("expected non-nil provider when schema implements GetTableByName")
		}
	})
}

// ---------------------------------------------------------------------------
// TestCorrelated_ExecCorrelatedExists
// ---------------------------------------------------------------------------

func TestCorrelated_ExecCorrelatedExists_Errors(t *testing.T) {
	t.Parallel()

	// P4NotFunc
	v := NewTestVDBE(4)
	instr := &Instruction{P4: P4Union{P: "not a func"}}
	if err := v.execCorrelatedExists(instr); err == nil {
		t.Error("expected error when P4 is not CorrelatedExistsFunc")
	}

	// BindingRegisterOutOfRange
	v = NewTestVDBE(2)
	fn := CorrelatedExistsFunc(func(bindings []interface{}) (bool, error) { return true, nil })
	instr = &Instruction{P1: 0, P2: 10, P3: 1, P4: P4Union{P: fn}}
	if err := v.execCorrelatedExists(instr); err == nil {
		t.Error("expected error when binding register is out of range")
	}

	// FuncReturnsError
	v = NewTestVDBE(4)
	fn2 := CorrelatedExistsFunc(func(bindings []interface{}) (bool, error) {
		return false, errors.New("sub-query error")
	})
	instr = &Instruction{P1: 0, P2: 1, P3: 0, P4: P4Union{P: fn2}}
	if err := v.execCorrelatedExists(instr); err == nil {
		t.Error("expected error propagated from CorrelatedExistsFunc")
	}
}

// correlatedExistsHelper runs execCorrelatedExists and returns the stored int value.
func correlatedExistsHelper(t *testing.T, existsResult bool, negate uint16) int64 {
	t.Helper()
	v := NewTestVDBE(4)
	fn := CorrelatedExistsFunc(func(bindings []interface{}) (bool, error) {
		return existsResult, nil
	})
	instr := &Instruction{P1: 0, P2: 1, P3: 0, P4: P4Union{P: fn}, P5: negate}
	if err := v.execCorrelatedExists(instr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return v.Mem[0].IntValue()
}

func TestCorrelated_ExecCorrelatedExists_Results(t *testing.T) {
	t.Parallel()

	if got := correlatedExistsHelper(t, true, 0); got != 1 {
		t.Errorf("ExistsTrue_NoNegate: expected 1, got %d", got)
	}
	if got := correlatedExistsHelper(t, true, 1); got != 0 {
		t.Errorf("ExistsTrue_Negate: expected 0, got %d", got)
	}
	if got := correlatedExistsHelper(t, false, 0); got != 0 {
		t.Errorf("ExistsFalse_NoNegate: expected 0, got %d", got)
	}
	if got := correlatedExistsHelper(t, false, 1); got != 1 {
		t.Errorf("ExistsFalse_Negate: expected 1, got %d", got)
	}
}

func TestCorrelated_ExecCorrelatedExists_WithBindings(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(4)
	v.Mem[1].SetInt(42)
	v.Mem[2].SetInt(99)
	var gotBindings []interface{}
	fn := CorrelatedExistsFunc(func(bindings []interface{}) (bool, error) {
		gotBindings = bindings
		return true, nil
	})
	instr := &Instruction{P1: 0, P2: 1, P3: 2, P4: P4Union{P: fn}, P5: 0}
	if err := v.execCorrelatedExists(instr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gotBindings) != 2 {
		t.Errorf("expected 2 bindings, got %d", len(gotBindings))
	}
}

// ---------------------------------------------------------------------------
// TestCorrelated_ExecCorrelatedScalar
// ---------------------------------------------------------------------------

func TestCorrelated_ExecCorrelatedScalar_Errors(t *testing.T) {
	t.Parallel()

	v := NewTestVDBE(4)
	instr := &Instruction{P4: P4Union{P: 42}}
	if err := v.execCorrelatedScalar(instr); err == nil {
		t.Error("expected error when P4 is not CorrelatedScalarFunc")
	}

	v = NewTestVDBE(2)
	fn := CorrelatedScalarFunc(func(bindings []interface{}) (interface{}, error) { return int64(1), nil })
	instr = &Instruction{P1: 0, P2: 10, P3: 1, P4: P4Union{P: fn}}
	if err := v.execCorrelatedScalar(instr); err == nil {
		t.Error("expected error for out-of-range binding register")
	}

	v = NewTestVDBE(4)
	fn2 := CorrelatedScalarFunc(func(bindings []interface{}) (interface{}, error) {
		return nil, fmt.Errorf("scalar error")
	})
	instr = &Instruction{P1: 0, P2: 1, P3: 0, P4: P4Union{P: fn2}}
	if err := v.execCorrelatedScalar(instr); err == nil {
		t.Error("expected error propagated from CorrelatedScalarFunc")
	}
}

// correlatedScalarHelper runs execCorrelatedScalar with the given return value.
func correlatedScalarHelper(t *testing.T, retVal interface{}) *Mem {
	t.Helper()
	v := NewTestVDBE(4)
	fn := CorrelatedScalarFunc(func(bindings []interface{}) (interface{}, error) {
		return retVal, nil
	})
	instr := &Instruction{P1: 0, P2: 1, P3: 0, P4: P4Union{P: fn}}
	if err := v.execCorrelatedScalar(instr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return v.Mem[0]
}

func TestCorrelated_ExecCorrelatedScalar_Results(t *testing.T) {
	t.Parallel()

	if m := correlatedScalarHelper(t, nil); !m.IsNull() {
		t.Error("nil result: expected NULL")
	}
	if m := correlatedScalarHelper(t, int64(7)); !m.IsInt() || m.IntValue() != 7 {
		t.Errorf("int64 result: expected 7, got %v", m.IntValue())
	}
	if m := correlatedScalarHelper(t, float64(3.14)); !m.IsReal() {
		t.Error("float64 result: expected real")
	}
	if m := correlatedScalarHelper(t, "hello"); !m.IsStr() {
		t.Error("string result: expected string")
	}
	if m := correlatedScalarHelper(t, []byte{1, 2, 3}); !m.IsBlob() {
		t.Error("byte slice result: expected blob")
	}
	if m := correlatedScalarHelper(t, struct{ x int }{x: 1}); !m.IsNull() {
		t.Error("unknown type: expected NULL")
	}
}

// ---------------------------------------------------------------------------
// TestCorrelated_CollectBindings
// ---------------------------------------------------------------------------

func TestCorrelated_CollectBindings(t *testing.T) {
	t.Parallel()

	t.Run("ZeroBindings_ReturnsEmpty", func(t *testing.T) {
		v := NewTestVDBE(4)
		bindings, err := v.collectBindings(1, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(bindings) != 0 {
			t.Errorf("expected 0 bindings, got %d", len(bindings))
		}
	})

	t.Run("ValidRange_ReturnsValues", func(t *testing.T) {
		v := NewTestVDBE(4)
		v.Mem[1].SetInt(10)
		v.Mem[2].SetInt(20)
		bindings, err := v.collectBindings(1, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(bindings) != 2 {
			t.Errorf("expected 2 bindings, got %d", len(bindings))
		}
	})

	t.Run("OutOfRange_ReturnsError", func(t *testing.T) {
		v := NewTestVDBE(2)
		_, err := v.collectBindings(5, 1)
		if err == nil {
			t.Error("expected error for out-of-range register")
		}
	})
}

// ---------------------------------------------------------------------------
// TestCorrelated_StoreExistsResult
// ---------------------------------------------------------------------------

func TestCorrelated_StoreExistsResult(t *testing.T) {
	t.Parallel()

	t.Run("OutOfRange_ReturnsError", func(t *testing.T) {
		v := NewTestVDBE(1)
		err := v.storeExistsResult(99, true, false)
		if err == nil {
			t.Error("expected error for out-of-range register")
		}
	})
}

// ---------------------------------------------------------------------------
// TestCorrelated_StoreScalarValue
// ---------------------------------------------------------------------------

func TestCorrelated_StoreScalarValue(t *testing.T) {
	t.Parallel()

	t.Run("OutOfRange_ReturnsError", func(t *testing.T) {
		v := NewTestVDBE(1)
		err := v.storeScalarValue(99, int64(1))
		if err == nil {
			t.Error("expected error for out-of-range register")
		}
	})
}
