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

// TestCompileSelectMiscCoverage is the umbrella test for all functions listed
// in the task description.
func TestCompileSelectMiscCoverage(t *testing.T) {

	// -----------------------------------------------------------------------
	// fromTableAlias (compile_select_agg.go)
	// -----------------------------------------------------------------------

	t.Run("fromTableAlias/NilFrom", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		stmt := &parser.SelectStmt{From: nil}
		got := s.fromTableAlias(stmt)
		if got != "" {
			t.Errorf("nil From: want \"\", got %q", got)
		}
	})

	t.Run("fromTableAlias/EmptyTables", func(t *testing.T) {
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

	t.Run("fromTableAlias/WithAlias", func(t *testing.T) {
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

	t.Run("fromTableAlias/NoAlias", func(t *testing.T) {
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

	// -----------------------------------------------------------------------
	// emitGeneratedExpr (compile_select_agg.go)
	// -----------------------------------------------------------------------

	t.Run("emitGeneratedExpr/SameReg", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		vm := vdbe.New()
		vm.AllocMemory(5)
		gen := expr.NewCodeGenerator(vm)

		// A literal whose generated register happens to equal colIdx=0.
		// GenerateExpr for an integer literal emits OpInteger into reg 0
		// (the first allocated register). When reg == colIdx no OpCopy is needed.
		e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
		before := len(vm.Program)
		s.emitGeneratedExpr(vm, gen, e, 0)
		// Some instructions should have been added (at minimum the literal load).
		if len(vm.Program) <= before {
			t.Error("emitGeneratedExpr/SameReg: expected instructions to be emitted")
		}
	})

	t.Run("emitGeneratedExpr/DifferentReg", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		vm := vdbe.New()
		vm.AllocMemory(10)
		gen := expr.NewCodeGenerator(vm)

		// Generate the expression into register 0, but ask to copy it to reg 5.
		e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "7"}
		s.emitGeneratedExpr(vm, gen, e, 5)

		// Verify that an OpCopy targeting register 5 was emitted.
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
	})

	t.Run("emitGeneratedExpr/ErrorPath", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		vm := vdbe.New()
		vm.AllocMemory(10)
		gen := expr.NewCodeGenerator(vm)

		// An IdentExpr referencing an unknown column causes GenerateExpr to
		// return an error. The function should fall back to OpNull for colIdx.
		e := &parser.IdentExpr{Name: "no_such_column_xyz"}
		colIdx := 3
		s.emitGeneratedExpr(vm, gen, e, colIdx)

		// We just need the call to complete without panic. Verify at least one
		// instruction targets colIdx (either OpNull or something else).
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
	})

	// -----------------------------------------------------------------------
	// extractOrderByExpression (compile_select.go)
	// -----------------------------------------------------------------------

	t.Run("extractOrderByExpression/IdentExpr", func(t *testing.T) {
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

	t.Run("extractOrderByExpression/CollateExpr", func(t *testing.T) {
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

	t.Run("extractOrderByExpression/BinaryExpr", func(t *testing.T) {
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

	t.Run("extractOrderByExpression/Literal", func(t *testing.T) {
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

	// -----------------------------------------------------------------------
	// emitFuncValue (compile_tvf.go)
	// -----------------------------------------------------------------------

	t.Run("emitFuncValue/Nil", func(t *testing.T) {
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
	})

	t.Run("emitFuncValue/NullValue", func(t *testing.T) {
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
	})

	t.Run("emitFuncValue/Integer", func(t *testing.T) {
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
	})

	t.Run("emitFuncValue/Float", func(t *testing.T) {
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
	})

	t.Run("emitFuncValue/Text", func(t *testing.T) {
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
	})

	t.Run("emitFuncValue/Blob", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		// Blob is the "default" branch in the SimpleValue switch → OpNull.
		emitFuncValue(vm, functions.NewBlobValue([]byte{0x01, 0x02}), 0)
		if len(vm.Program) == 0 {
			t.Fatal("emitFuncValue/Blob: expected instruction")
		}
		// Blob falls through the default case → OpNull
		if vm.Program[0].Opcode != vdbe.OpNull {
			t.Errorf("emitFuncValue/Blob: want OpNull (default case), got opcode %d", vm.Program[0].Opcode)
		}
	})

	// -----------------------------------------------------------------------
	// emitIntValue (compile_tvf.go)
	// -----------------------------------------------------------------------

	t.Run("emitIntValue/SmallPositive", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, 100, 0)
		if len(vm.Program) == 0 {
			t.Fatal("emitIntValue/SmallPositive: expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("emitIntValue/SmallPositive: want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("emitIntValue/SmallNegative", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, -1000, 1)
		if len(vm.Program) == 0 {
			t.Fatal("emitIntValue/SmallNegative: expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("emitIntValue/SmallNegative: want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("emitIntValue/LargePositive", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		// Value outside int32 range → OpInt64
		emitIntValue(vm, 3_000_000_000, 0)
		if len(vm.Program) == 0 {
			t.Fatal("emitIntValue/LargePositive: expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInt64 {
			t.Errorf("emitIntValue/LargePositive: want OpInt64, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("emitIntValue/LargeNegative", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, -3_000_000_000, 0)
		if len(vm.Program) == 0 {
			t.Fatal("emitIntValue/LargeNegative: expected instruction")
		}
		if vm.Program[0].Opcode != vdbe.OpInt64 {
			t.Errorf("emitIntValue/LargeNegative: want OpInt64, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("emitIntValue/Boundary_MaxInt32", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, 2147483647, 0) // INT32_MAX
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("emitIntValue/Boundary_MaxInt32: want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})

	t.Run("emitIntValue/Boundary_MinInt32", func(t *testing.T) {
		t.Parallel()
		vm := vdbe.New()
		vm.AllocMemory(5)
		emitIntValue(vm, -2147483648, 0) // INT32_MIN
		if vm.Program[0].Opcode != vdbe.OpInteger {
			t.Errorf("emitIntValue/Boundary_MinInt32: want OpInteger, got %d", vm.Program[0].Opcode)
		}
	})

	// -----------------------------------------------------------------------
	// hasColumnRefArg (compile_tvf.go)
	// -----------------------------------------------------------------------

	t.Run("hasColumnRefArg/Empty", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		if s.hasColumnRefArg(nil) {
			t.Error("hasColumnRefArg(nil): want false, got true")
		}
		if s.hasColumnRefArg([]parser.Expression{}) {
			t.Error("hasColumnRefArg([]): want false, got true")
		}
	})

	t.Run("hasColumnRefArg/OnlyLiteral", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		args := []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		}
		if s.hasColumnRefArg(args) {
			t.Error("hasColumnRefArg with literal: want false, got true")
		}
	})

	t.Run("hasColumnRefArg/WithIdent", func(t *testing.T) {
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

	t.Run("hasColumnRefArg/OnlyIdent", func(t *testing.T) {
		t.Parallel()
		s := newMiscCovStmt(t)
		args := []parser.Expression{
			&parser.IdentExpr{Name: "some_col"},
		}
		if !s.hasColumnRefArg(args) {
			t.Error("hasColumnRefArg single ident: want true, got false")
		}
	})

	// -----------------------------------------------------------------------
	// compareFuncValues (compile_tvf.go)
	// -----------------------------------------------------------------------

	t.Run("compareFuncValues/BothNil", func(t *testing.T) {
		t.Parallel()
		if r := compareFuncValues(nil, nil); r != 0 {
			t.Errorf("both nil: want 0, got %d", r)
		}
	})

	t.Run("compareFuncValues/BothNull", func(t *testing.T) {
		t.Parallel()
		a := functions.NewNullValue()
		b := functions.NewNullValue()
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("both null: want 0, got %d", r)
		}
	})

	t.Run("compareFuncValues/LeftNil", func(t *testing.T) {
		t.Parallel()
		b := functions.NewIntValue(1)
		if r := compareFuncValues(nil, b); r >= 0 {
			t.Errorf("nil < int: want <0, got %d", r)
		}
	})

	t.Run("compareFuncValues/RightNil", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(1)
		if r := compareFuncValues(a, nil); r <= 0 {
			t.Errorf("int > nil: want >0, got %d", r)
		}
	})

	t.Run("compareFuncValues/SameIntEqual", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(42)
		b := functions.NewIntValue(42)
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("equal ints: want 0, got %d", r)
		}
	})

	t.Run("compareFuncValues/IntLess", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(1)
		b := functions.NewIntValue(2)
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("1 < 2: want <0, got %d", r)
		}
	})

	t.Run("compareFuncValues/IntGreater", func(t *testing.T) {
		t.Parallel()
		a := functions.NewIntValue(10)
		b := functions.NewIntValue(5)
		if r := compareFuncValues(a, b); r <= 0 {
			t.Errorf("10 > 5: want >0, got %d", r)
		}
	})

	t.Run("compareFuncValues/TextEqual", func(t *testing.T) {
		t.Parallel()
		a := functions.NewTextValue("hello")
		b := functions.NewTextValue("hello")
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("equal text: want 0, got %d", r)
		}
	})

	t.Run("compareFuncValues/TextLess", func(t *testing.T) {
		t.Parallel()
		a := functions.NewTextValue("apple")
		b := functions.NewTextValue("zebra")
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("apple < zebra: want <0, got %d", r)
		}
	})

	t.Run("compareFuncValues/DifferentTypes_IntVsText", func(t *testing.T) {
		t.Parallel()
		// TypeInteger < TypeText in the affinity ordering
		a := functions.NewIntValue(999)
		b := functions.NewTextValue("a")
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("int vs text: want <0 (int has lower type), got %d", r)
		}
	})

	t.Run("compareFuncValues/FloatEqual", func(t *testing.T) {
		t.Parallel()
		a := functions.NewFloatValue(1.5)
		b := functions.NewFloatValue(1.5)
		if r := compareFuncValues(a, b); r != 0 {
			t.Errorf("equal floats: want 0, got %d", r)
		}
	})

	t.Run("compareFuncValues/FloatLess", func(t *testing.T) {
		t.Parallel()
		a := functions.NewFloatValue(0.1)
		b := functions.NewFloatValue(9.9)
		if r := compareFuncValues(a, b); r >= 0 {
			t.Errorf("0.1 < 9.9: want <0, got %d", r)
		}
	})

	// -----------------------------------------------------------------------
	// pragmaRowMatchesWhere / evalPragmaWhere (compile_pragma_tvf.go)
	// -----------------------------------------------------------------------

	t.Run("pragmaRowMatchesWhere/NilWhere", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{"id", "INTEGER", 0, nil, 0}
		cols := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
		if !pragmaRowMatchesWhere(nil, row, cols) {
			t.Error("nil where: want true, got false")
		}
	})

	t.Run("pragmaRowMatchesWhere/EqualityMatch", func(t *testing.T) {
		t.Parallel()
		// row: cid=0, name="alpha", type="TEXT"
		row := []interface{}{int64(0), "alpha", "TEXT"}
		cols := []string{"cid", "name", "type"}
		// WHERE name = 'alpha'
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "alpha"},
		}
		if !pragmaRowMatchesWhere(where, row, cols) {
			t.Error("equality match: want true, got false")
		}
	})

	t.Run("pragmaRowMatchesWhere/EqualityNoMatch", func(t *testing.T) {
		t.Parallel()
		row := []interface{}{int64(0), "alpha", "TEXT"}
		cols := []string{"cid", "name", "type"}
		// WHERE name = 'beta'
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "beta"},
		}
		if pragmaRowMatchesWhere(where, row, cols) {
			t.Error("equality no-match: want false, got true")
		}
	})

	t.Run("evalPragmaWhere/UnknownExpr", func(t *testing.T) {
		t.Parallel()
		// Any non-BinaryExpr expression should return true (unknown = pass).
		row := []interface{}{int64(1), "col"}
		cols := []string{"cid", "name"}
		lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
		if !evalPragmaWhere(lit, row, cols) {
			t.Error("unknown expr: want true (pass), got false")
		}
	})

	t.Run("evalPragmaWhere/BinaryEq", func(t *testing.T) {
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

	t.Run("evalPragmaWhere/BinaryGe_Match", func(t *testing.T) {
		t.Parallel()
		// cid >= 1 for a row with cid=2 → should match
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

	t.Run("evalPragmaWhere/BinaryGe_NoMatch", func(t *testing.T) {
		t.Parallel()
		// cid >= 5 for a row with cid=2 → should not match
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

	t.Run("evalPragmaWhere/BinaryGt_Match", func(t *testing.T) {
		t.Parallel()
		// cid > 0 for cid=1 → should match
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

	t.Run("evalPragmaWhere/BinaryGt_NoMatch", func(t *testing.T) {
		t.Parallel()
		// cid > 1 for cid=1 → should not match
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

	t.Run("evalPragmaWhere/AndBothMatch", func(t *testing.T) {
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

	t.Run("evalPragmaWhere/OrOneMatch", func(t *testing.T) {
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

	t.Run("evalPragmaWhere/DefaultOp_ReturnsTrue", func(t *testing.T) {
		t.Parallel()
		// An unsupported binary operator (e.g. OpLt) should return true (unknown=pass).
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
