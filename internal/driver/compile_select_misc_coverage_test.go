// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// newMiscCovStmt returns a fresh *Stmt backed by an in-memory connection.
func newMiscCovStmt(t *testing.T) *Stmt {
	t.Helper()
	conn := openMemConn(t)
	return stmtFor(conn)
}

// ---------------------------------------------------------------------------
// fromTableAlias
// ---------------------------------------------------------------------------

func TestFromTableAlias(t *testing.T) {
	t.Run("NilFrom", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		stmt := &parser.SelectStmt{From: nil}
		got := s.fromTableAlias(stmt)
		if got != "" {
			t.Errorf("nil From: want \"\", got %q", got)
		}
	})

	t.Run("EmptyTables", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{Tables: []parser.TableOrSubquery{}},
		}
		got := s.fromTableAlias(stmt)
		if got != "" {
			t.Errorf("empty Tables: want \"\", got %q", got)
		}
	})

	t.Run("WithAlias", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "employees", Alias: "e"},
				},
			},
		}
		got := s.fromTableAlias(stmt)
		if got != "e" {
			t.Errorf("aliased table: want \"e\", got %q", got)
		}
	})

	t.Run("NoAlias", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "employees"},
				},
			},
		}
		got := s.fromTableAlias(stmt)
		if got != "" {
			t.Errorf("no-alias table: want \"\", got %q", got)
		}
	})
}

// ---------------------------------------------------------------------------
// emitGeneratedExpr
// ---------------------------------------------------------------------------

func TestEmitGeneratedExprSameReg(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(5)
	gen := expr.NewCodeGenerator(vm)

	e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	before := len(vm.Program)
	s.emitGeneratedExpr(vm, gen, e, 0)
	if len(vm.Program) <= before {
		t.Error("emitGeneratedExpr/SameReg: expected instructions to be emitted")
	}
}

func TestEmitGeneratedExprDifferentReg(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "7"}
	s.emitGeneratedExpr(vm, gen, e, 5)

	found := false
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpCopy && instr.P2 == 5 {
			found = true
			break
		}
	}
	if !found {
		t.Error("emitGeneratedExpr/DifferentReg: expected OpCopy to reg 5")
	}
}

func TestEmitGeneratedExprErrorPath(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	e := &parser.IdentExpr{Name: "no_such_column_xyz"}
	colIdx := 3
	s.emitGeneratedExpr(vm, gen, e, colIdx)

	found := false
	for _, instr := range vm.Program {
		if instr.P2 == colIdx {
			found = true
			break
		}
	}
	if !found {
		t.Error("emitGeneratedExpr/ErrorPath: expected an instruction targeting colIdx 3")
	}
}

// ---------------------------------------------------------------------------
// extractOrderByExpression
// ---------------------------------------------------------------------------

func TestExtractOrderByExpressionMisc(t *testing.T) {
	t.Run("IdentExpr", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		ident := &parser.IdentExpr{Name: "age"}
		term := parser.OrderingTerm{Expr: ident, Asc: true}
		info := &orderByColumnInfo{}
		baseExpr, collation := s.extractOrderByExpression(term, 0, info)
		if baseExpr != ident {
			t.Errorf("IdentExpr: got %T, want *parser.IdentExpr", baseExpr)
		}
		if collation != "" {
			t.Errorf("IdentExpr: want empty collation, got %q", collation)
		}
	})

	t.Run("CollateExpr", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		inner := &parser.IdentExpr{Name: "name"}
		collateExpr := &parser.CollateExpr{Expr: inner, Collation: "NOCASE"}
		term := parser.OrderingTerm{Expr: collateExpr, Asc: false}
		info := &orderByColumnInfo{}
		baseExpr, collation := s.extractOrderByExpression(term, 0, info)
		if baseExpr != inner {
			t.Errorf("CollateExpr: expected inner IdentExpr, got %T", baseExpr)
		}
		if collation != "NOCASE" {
			t.Errorf("CollateExpr: want collation \"NOCASE\", got %q", collation)
		}
	})

	t.Run("BinaryExpr", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		binExpr := &parser.BinaryExpr{
			Op:    parser.OpPlus,
			Left:  &parser.IdentExpr{Name: "x"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}
		term := parser.OrderingTerm{Expr: binExpr, Asc: true, Collation: "BINARY"}
		info := &orderByColumnInfo{}
		baseExpr, collation := s.extractOrderByExpression(term, 0, info)
		if baseExpr != binExpr {
			t.Errorf("BinaryExpr: expected binExpr back, got %T", baseExpr)
		}
		if collation != "BINARY" {
			t.Errorf("BinaryExpr: want collation \"BINARY\", got %q", collation)
		}
	})

	t.Run("Literal", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}
		term := parser.OrderingTerm{Expr: lit, Asc: true}
		info := &orderByColumnInfo{}
		baseExpr, _ := s.extractOrderByExpression(term, 0, info)
		if baseExpr != lit {
			t.Errorf("Literal: expected lit back, got %T", baseExpr)
		}
	})
}

// ---------------------------------------------------------------------------
// emitFuncValue
// ---------------------------------------------------------------------------

func TestEmitFuncValueNil(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	emitFuncValue(vm, nil, 0)
	if len(vm.Program) == 0 {
		t.Fatal("emitFuncValue/Nil: expected at least one instruction")
	}
	if vm.Program[0].Opcode != vdbe.OpNull {
		t.Errorf("emitFuncValue/Nil: want OpNull, got opcode %d", vm.Program[0].Opcode)
	}
}

func TestEmitFuncValueNull(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	emitFuncValue(vm, functions.NewNullValue(), 1)
	if len(vm.Program) == 0 {
		t.Fatal("emitFuncValue/NullValue: expected instruction")
	}
	if vm.Program[0].Opcode != vdbe.OpNull {
		t.Errorf("emitFuncValue/NullValue: want OpNull, got opcode %d", vm.Program[0].Opcode)
	}
}

func TestEmitFuncValueInteger(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	emitFuncValue(vm, functions.NewIntValue(42), 2)
	if len(vm.Program) == 0 {
		t.Fatal("emitFuncValue/Integer: expected instruction")
	}
	op := vm.Program[0].Opcode
	if op != vdbe.OpInteger && op != vdbe.OpInt64 {
		t.Errorf("emitFuncValue/Integer: want OpInteger or OpInt64, got %d", op)
	}
}

func TestEmitFuncValueFloat(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	emitFuncValue(vm, functions.NewFloatValue(3.14), 0)
	if len(vm.Program) == 0 {
		t.Fatal("emitFuncValue/Float: expected instruction")
	}
	if vm.Program[0].Opcode != vdbe.OpReal {
		t.Errorf("emitFuncValue/Float: want OpReal, got opcode %d", vm.Program[0].Opcode)
	}
}

func TestEmitFuncValueText(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	emitFuncValue(vm, functions.NewTextValue("hello"), 0)
	if len(vm.Program) == 0 {
		t.Fatal("emitFuncValue/Text: expected instruction")
	}
	if vm.Program[0].Opcode != vdbe.OpString8 {
		t.Errorf("emitFuncValue/Text: want OpString8, got opcode %d", vm.Program[0].Opcode)
	}
}

func TestEmitFuncValueBlob(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	emitFuncValue(vm, functions.NewBlobValue([]byte{0x01, 0x02}), 0)
	if len(vm.Program) == 0 {
		t.Fatal("emitFuncValue/Blob: expected instruction")
	}
	if vm.Program[0].Opcode != vdbe.OpNull {
		t.Errorf("emitFuncValue/Blob: want OpNull (default case), got opcode %d", vm.Program[0].Opcode)
	}
}

// ---------------------------------------------------------------------------
// emitIntValue
// ---------------------------------------------------------------------------

func TestEmitIntValueSmall(t *testing.T) {
	t.Run("Positive", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, 100, 0)
		if len(vm.Program) == 0 {
			t.Fatal("expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("Negative", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, -1000, 1)
		if len(vm.Program) == 0 {
			t.Fatal("expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})
}

func TestEmitIntValueLarge(t *testing.T) {
	t.Run("Positive", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, 3_000_000_000, 0)
		if len(vm.Program) == 0 {
			t.Fatal("expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInt64 {
			t.Errorf("want OpInt64, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("Negative", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, -3_000_000_000, 0)
		if len(vm.Program) == 0 {
			t.Fatal("expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInt64 {
			t.Errorf("want OpInt64, got %d", vm.Program[0].Opcode)
		}
	})
}

func TestEmitIntValueBoundary(t *testing.T) {
	t.Run("MaxInt32", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, 2147483647, 0)
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("MinInt32", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, -2147483648, 0)
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})
}

// ---------------------------------------------------------------------------
// hasColumnRefArg
// ---------------------------------------------------------------------------

func TestHasColumnRefArg(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		if s.hasColumnRefArg(nil) {
			t.Error("hasColumnRefArg(nil): want false, got true")
		}
		if s.hasColumnRefArg([]parser.Expression{}) {
			t.Error("hasColumnRefArg([]): want false, got true")
		}
	})

	t.Run("OnlyLiteral", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		args := []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		}
		if s.hasColumnRefArg(args) {
			t.Error("hasColumnRefArg with literal: want false, got true")
		}
	})

	t.Run("WithIdent", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		args := []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			&parser.IdentExpr{Name: "col1"},
		}
		if !s.hasColumnRefArg(args) {
			t.Error("hasColumnRefArg with ident: want true, got false")
		}
	})

	t.Run("OnlyIdent", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		args := []parser.Expression{
			&parser.IdentExpr{Name: "some_col"},
		}
		if !s.hasColumnRefArg(args) {
			t.Error("hasColumnRefArg single ident: want true, got false")
		}
	})
}

// ---------------------------------------------------------------------------
// compareFuncValues
// ---------------------------------------------------------------------------

func TestCompareFuncValuesNilNull(t *testing.T) {
	t.Run("BothNil", func(t *testing.T) {
		t.Parallel()
		if r := compareFuncValues(nil, nil); r != 0 {
			t.Errorf("both nil: want 0, got %d", r)
		}
	})

	t.Run("BothNull", func(t *testing.T) {
		t.Parallel()
		a := functions.NewNullValue()
		b := functions.NewNullValue()
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("both null: want 0, got %d", r)
		}
	})

	t.Run("LeftNil", func(t *testing.T) {
		t.Parallel()
		b := functions.NewIntValue(1)
		if r := compareFuncValues(nil, b); r >= 0 {
			t.Errorf("nil < int: want <0, got %d", r)
		}
	})

	t.Run("RightNil", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(1)
		if r := compareFuncValues(a, nil); r <= 0 {
			t.Errorf("int > nil: want >0, got %d", r)
		}
	})
}

func TestCompareFuncValuesInt(t *testing.T) {
	t.Run("Equal", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(42)
		b := functions.NewIntValue(42)
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("equal ints: want 0, got %d", r)
		}
	})

	t.Run("Less", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(1)
		b := functions.NewIntValue(2)
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("1 < 2: want <0, got %d", r)
		}
	})

	t.Run("Greater", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(10)
		b := functions.NewIntValue(5)
		if r := compareFuncValues(a, b); r <= 0 {
			t.Errorf("10 > 5: want >0, got %d", r)
		}
	})
}

func TestCompareFuncValuesTextAndFloat(t *testing.T) {
	t.Run("TextEqual", func(t *testing.T) {
		t.Parallel()
		a := functions.NewTextValue("hello")
		b := functions.NewTextValue("hello")
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("equal text: want 0, got %d", r)
		}
	})

	t.Run("TextLess", func(t *testing.T) {
		t.Parallel()
		a := functions.NewTextValue("apple")
		b := functions.NewTextValue("zebra")
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("apple < zebra: want <0, got %d", r)
		}
	})

	t.Run("DifferentTypes_IntVsText", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(999)
		b := functions.NewTextValue("a")
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("int vs text: want <0 (int has lower type), got %d", r)
		}
	})

	t.Run("FloatEqual", func(t *testing.T) {
		t.Parallel()
		a := functions.NewFloatValue(1.5)
		b := functions.NewFloatValue(1.5)
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("equal floats: want 0, got %d", r)
		}
	})

	t.Run("FloatLess", func(t *testing.T) {
		t.Parallel()
		a := functions.NewFloatValue(0.1)
		b := functions.NewFloatValue(9.9)
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("0.1 < 9.9: want <0, got %d", r)
		}
	})
}

// ---------------------------------------------------------------------------
// pragmaRowMatchesWhere
// ---------------------------------------------------------------------------

func TestPragmaRowMatchesWhere(t *testing.T) {
	t.Run("NilWhere", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{"id", "INTEGER", 0, nil, 0}
		cols := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
		if !pragmaRowMatchesWhere(nil, row, cols) {
			t.Error("nil where: want true, got false")
		}
	})

	t.Run("EqualityMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(0), "alpha", "TEXT"}
		cols := []string{"cid", "name", "type"}
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "alpha"},
		}
		if !pragmaRowMatchesWhere(where, row, cols) {
			t.Error("equality match: want true, got false")
		}
	})

	t.Run("EqualityNoMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(0), "alpha", "TEXT"}
		cols := []string{"cid", "name", "type"}
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "beta"},
		}
		if pragmaRowMatchesWhere(where, row, cols) {
			t.Error("equality no-match: want false, got true")
		}
	})
}

// ---------------------------------------------------------------------------
// evalPragmaWhere
// ---------------------------------------------------------------------------

func TestEvalPragmaWhereBasic(t *testing.T) {
	t.Run("UnknownExpr", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(1), "col"}
		cols := []string{"cid", "name"}
		lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
		if !evalPragmaWhere(lit, row, cols) {
			t.Error("unknown expr: want true (pass), got false")
		}
	})

	t.Run("BinaryEq", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(2), "beta"}
		cols := []string{"cid", "name"}
		expr := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "beta"},
		}
		if !evalPragmaWhere(expr, row, cols) {
			t.Error("binary eq match: want true, got false")
		}
	})

	t.Run("DefaultOp_ReturnsTrue", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(1)}
		cols := []string{"cid"}
		e := &parser.BinaryExpr{
			Op:    parser.OpLt,
			Left:  &parser.IdentExpr{Name: "cid"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "999"},
		}
		if !evalPragmaWhere(e, row, cols) {
			t.Error("unsupported op: want true (default pass), got false")
		}
	})
}

func TestEvalPragmaWhereComparison(t *testing.T) {
	t.Run("BinaryGe_Match", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(2), "gamma"}
		cols := []string{"cid", "name"}
		e := &parser.BinaryExpr{
			Op:    parser.OpGe,
			Left:  &parser.IdentExpr{Name: "cid"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}
		if !evalPragmaWhere(e, row, cols) {
			t.Error("OpGe match: want true, got false")
		}
	})

	t.Run("BinaryGe_NoMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(2), "gamma"}
		cols := []string{"cid", "name"}
		e := &parser.BinaryExpr{
			Op:    parser.OpGe,
			Left:  &parser.IdentExpr{Name: "cid"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		}
		if evalPragmaWhere(e, row, cols) {
			t.Error("OpGe no-match: want false, got true")
		}
	})

	t.Run("BinaryGt_Match", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(1), "delta"}
		cols := []string{"cid", "name"}
		e := &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Name: "cid"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		}
		if !evalPragmaWhere(e, row, cols) {
			t.Error("OpGt match: want true, got false")
		}
	})

	t.Run("BinaryGt_NoMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(1), "delta"}
		cols := []string{"cid", "name"}
		e := &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Name: "cid"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}
		if evalPragmaWhere(e, row, cols) {
			t.Error("OpGt no-match: want false, got true")
		}
	})
}

func TestEvalPragmaWhereLogical(t *testing.T) {
	t.Run("AndBothMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(3), "epsilon"}
		cols := []string{"cid", "name"}
		e := &parser.BinaryExpr{
			Op: parser.OpAnd,
			Left: &parser.BinaryExpr{
				Op:    parser.OpGe,
				Left:  &parser.IdentExpr{Name: "cid"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			Right: &parser.BinaryExpr{
				Op:    parser.OpGe,
				Left:  &parser.IdentExpr{Name: "cid"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			},
		}
		if !evalPragmaWhere(e, row, cols) {
			t.Error("AND both match: want true, got false")
		}
	})

	t.Run("OrOneMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(0), "zeta"}
		cols := []string{"cid", "name"}
		e := &parser.BinaryExpr{
			Op: parser.OpOr,
			Left: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "name"},
				Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "zeta"},
			},
			Right: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "name"},
				Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "no_match"},
			},
		}
		if !evalPragmaWhere(e, row, cols) {
			t.Error("OR one match: want true, got false")
		}
	})
}
