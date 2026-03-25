// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// --- adjustInstructionJumps (42.9%) ---

// TestAdjustInstructionJumps_NoRule verifies that opcodes with no jump rule are skipped.
func TestAdjustInstructionJumps_NoRule(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{
		Opcode: vdbe.OpNoop,
		P2:     5,
	}
	gen.adjustInstructionJumps(instr, map[int]int{5: 10})
	// P2 unchanged because OpNoop has no rule
	if instr.P2 != 5 {
		t.Errorf("expected P2=5, got %d", instr.P2)
	}
}

// TestAdjustInstructionJumps_AdjustP2 verifies that P2 is remapped when a rule exists.
func TestAdjustInstructionJumps_AdjustP2(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{
		Opcode: vdbe.OpGoto,
		P2:     3,
	}
	gen.adjustInstructionJumps(instr, map[int]int{3: 7})
	if instr.P2 != 7 {
		t.Errorf("expected P2=7, got %d", instr.P2)
	}
}

// TestAdjustInstructionJumps_AdjustP2AndP3 covers the InitCoroutine dual-adjust path.
func TestAdjustInstructionJumps_AdjustP2AndP3(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{
		Opcode: vdbe.OpInitCoroutine,
		P2:     2,
		P3:     4,
	}
	gen.adjustInstructionJumps(instr, map[int]int{2: 20, 4: 40})
	if instr.P2 != 20 {
		t.Errorf("expected P2=20, got %d", instr.P2)
	}
	if instr.P3 != 40 {
		t.Errorf("expected P3=40, got %d", instr.P3)
	}
}

// TestAdjustInstructionJumps_ZeroNotMapped ensures targets of 0 are not adjusted.
func TestAdjustInstructionJumps_ZeroNotMapped(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{
		Opcode: vdbe.OpGoto,
		P2:     0,
	}
	gen.adjustInstructionJumps(instr, map[int]int{0: 99})
	if instr.P2 != 0 {
		t.Errorf("expected P2=0 (zero not remapped), got %d", instr.P2)
	}
}

// --- adjustInstructionRegisters (57.1%) ---

// TestAdjustInstructionRegisters_NoRule covers the no-rule early return.
func TestAdjustInstructionRegisters_NoRule(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{
		Opcode: vdbe.OpNoop,
		P1:     1,
		P3:     2,
	}
	gen.adjustInstructionRegisters(instr, 10)
	if instr.P1 != 1 || instr.P3 != 2 {
		t.Error("expected no change for opcode with no register rule")
	}
}

// TestAdjustInstructionRegisters_AdjustBoth covers P1+P3 adjustment.
func TestAdjustInstructionRegisters_AdjustBoth(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{
		Opcode: vdbe.OpAdd,
		P1:     2,
		P3:     5,
	}
	gen.adjustInstructionRegisters(instr, 10)
	if instr.P1 != 12 {
		t.Errorf("expected P1=12, got %d", instr.P1)
	}
	if instr.P3 != 15 {
		t.Errorf("expected P3=15, got %d", instr.P3)
	}
}

// --- generateCase (72.7%) ---

// TestGenerateCase_SearchedWithElse tests searched CASE (no CASE expr) with ELSE.
func TestGenerateCase_SearchedWithElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.CaseExpr{
		Expr: nil, // Searched CASE
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "100"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateCase (searched+else) failed: %v", err)
	}
}

// TestGenerateCase_SearchedNoElse tests searched CASE without ELSE (emits NULL).
func TestGenerateCase_SearchedNoElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "no"},
			},
		},
		ElseClause: nil,
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateCase (searched, no else) failed: %v", err)
	}
}

// TestGenerateCase_SimpleWithElse tests simple CASE (has CASE expr).
func TestGenerateCase_SimpleWithElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "two"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "other"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateCase (simple+else) failed: %v", err)
	}
}

// --- generateCollate (80.0%) ---

// TestGenerateCollate_WithInnerExpr tests collate wrapping an inner expression.
func TestGenerateCollate_WithInnerExpr(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Collation: "NOCASE",
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateCollate failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
	// The collation should be tracked on the register
	if coll := gen.collations[reg]; coll != "NOCASE" {
		t.Errorf("expected collation NOCASE on register %d, got %q", reg, coll)
	}
}

// --- generateInValueComparison / lookupColumnCollation (85.7%) ---

// TestGenerateIn_NotInValueList tests NOT IN with value list (negation path).
func TestGenerateIn_NotInValueList(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Not:  true,
		Values: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			&parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"},
		},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateIn NOT IN failed: %v", err)
	}
}

// TestLookupColumnCollation_TableNotFound verifies empty string when table missing.
func TestLookupColumnCollation_TableNotFound(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	coll := gen.lookupColumnCollation("nonexistent", "col")
	if coll != "" {
		t.Errorf("expected empty string, got %q", coll)
	}
}

// TestLookupColumnCollation_ColumnNotFound verifies empty string when column missing.
func TestLookupColumnCollation_ColumnNotFound(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.tableInfo["t"] = TableInfo{
		Name:    "t",
		Columns: []ColumnInfo{{Name: "id", Index: 0}},
	}
	coll := gen.lookupColumnCollation("t", "missing_col")
	if coll != "" {
		t.Errorf("expected empty string for missing column, got %q", coll)
	}
}

// TestLookupColumnCollation_Found verifies collation is returned for known column.
func TestLookupColumnCollation_Found(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.tableInfo["t"] = TableInfo{
		Name:    "t",
		Columns: []ColumnInfo{{Name: "name", Index: 0, Collation: "RTRIM"}},
	}
	coll := gen.lookupColumnCollation("t", "name")
	if coll != "RTRIM" {
		t.Errorf("expected RTRIM, got %q", coll)
	}
}

// --- generateBinaryOperands (85.7%) ---

// TestGenerateBinaryOperands_ShiftOps covers shift operator register swap path.
func TestGenerateBinaryOperands_ShiftOps(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "8"},
		Op:    parser.OpLShift,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("LShift failed: %v", err)
	}
}

// TestGenerateBinaryOperands_RShift covers right shift path.
func TestGenerateBinaryOperands_RShift(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "16"},
		Op:    parser.OpRShift,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("RShift failed: %v", err)
	}
}

// --- GenerateCondition (83.3%) ---

// TestGenerateCondition covers the basic conditional jump emission.
func TestGenerateCondition(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	jumpAddr, err := gen.GenerateCondition(expr, 99)
	if err != nil {
		t.Fatalf("GenerateCondition failed: %v", err)
	}
	if jumpAddr < 0 {
		t.Errorf("expected non-negative jump address, got %d", jumpAddr)
	}
	// The last emitted instruction should be OpIfNot
	lastInstr := v.Program[v.NumOps()-1]
	if lastInstr.Opcode != vdbe.OpIfNot {
		t.Errorf("expected OpIfNot, got %s", lastInstr.Opcode.String())
	}
	if lastInstr.P2 != 99 {
		t.Errorf("expected jump target 99, got %d", lastInstr.P2)
	}
}

// --- GenerateExpr: precomputed path ---

// TestGenerateExpr_Precomputed2 verifies that precomputed register is returned.
func TestGenerateExpr_Precomputed2(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "7"}
	gen.SetPrecomputed(lit, 42)
	reg, err := gen.GenerateExpr(lit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reg != 42 {
		t.Errorf("expected precomputed register 42, got %d", reg)
	}
}

// --- generateIntegerLiteral: large int and float-parse fallback (95.0%) ---

// TestGenerateIntegerLiteral_Large covers the Int64 path (value > 2^31-1).
func TestGenerateIntegerLiteral_Large(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "9999999999"}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("large integer literal failed: %v", err)
	}
}

// TestGenerateIntegerLiteral_HexValue covers hex prefix path.
func TestGenerateIntegerLiteral_HexValue(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0xFF"}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("hex literal failed: %v", err)
	}
}

// TestGenerateIntegerLiteral_Underscore covers underscore-stripped path.
func TestGenerateIntegerLiteral_Underscore(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1_000"}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("underscore integer literal failed: %v", err)
	}
}

// --- exprChildren coverage: CollateExpr and CastExpr paths (66.7%) ---

// TestExprChildren_CollateExpr covers the CollateExpr single-child path.
func TestExprChildren_CollateExpr(t *testing.T) {
	e := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "x"},
		Collation: "BINARY",
	}
	children := exprChildren(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child, got %d", len(children))
	}
}

// TestExprChildren_CastExpr covers the CastExpr single-child path.
func TestExprChildren_CastExpr(t *testing.T) {
	e := &parser.CastExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Type: "TEXT",
	}
	children := exprChildren(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child, got %d", len(children))
	}
}

// TestExprChildren_BetweenExpr covers the BetweenExpr path.
func TestExprChildren_BetweenExpr(t *testing.T) {
	e := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
	}
	children := exprChildren(e)
	if len(children) != 3 {
		t.Errorf("expected 3 children, got %d", len(children))
	}
}

// TestExprChildren_CaseExpr2 covers the caseExprChildren path (multi-when).
func TestExprChildren_CaseExpr2(t *testing.T) {
	e := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "yes"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "no"},
	}
	children := exprChildren(e)
	// Expr + 2 per WhenClause + ElseClause = 4
	if len(children) < 3 {
		t.Errorf("expected at least 3 children, got %d", len(children))
	}
}

// TestExprChildren_InExpr2 covers the InExpr children path.
func TestExprChildren_InExpr2(t *testing.T) {
	e := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Values: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		},
	}
	children := exprChildren(e)
	if len(children) != 3 {
		t.Errorf("expected 3 children (expr + 2 values), got %d", len(children))
	}
}

// TestExprChildren_UnaryExpr covers the UnaryExpr path.
func TestExprChildren_UnaryExpr(t *testing.T) {
	e := &parser.UnaryExpr{
		Op:   parser.OpNot,
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	children := exprChildren(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child, got %d", len(children))
	}
}

// TestExprChildren_FunctionExpr covers the FunctionExpr path.
func TestExprChildren_FunctionExpr(t *testing.T) {
	e := &parser.FunctionExpr{
		Name: "coalesce",
		Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
		},
	}
	children := exprChildren(e)
	if len(children) != 2 {
		t.Errorf("expected 2 children, got %d", len(children))
	}
}

// TestExprChildren_Unknown covers the default (returns nil) path.
func TestExprChildren_Unknown(t *testing.T) {
	e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	children := exprChildren(e)
	if children != nil {
		t.Errorf("expected nil children for LiteralExpr, got %v", children)
	}
}

// --- generateLogical OR path (90.9%) ---

// TestGenerateLogical_OR covers OR short-circuit path.
func TestGenerateLogical_OR(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Op:    parser.OpOr,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("OR logical failed: %v", err)
	}
}

// --- generateBetween NOT path (85.0%) ---

// TestGenerateBetween_Not covers NOT BETWEEN path.
func TestGenerateBetween_Not(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "20"},
		Not:   true,
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("NOT BETWEEN failed: %v", err)
	}
}

// --- generateWhenClauses (80.0%) multiple WHENs ---

// TestGenerateCase_MultipleWhen covers multiple WHEN clauses path.
func TestGenerateCase_MultipleWhen(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "a"},
			},
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "b"},
			},
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "c"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "d"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("multi-WHEN case failed: %v", err)
	}
}

// --- generateElseClause nil and non-nil (85.7%) ---

// TestGenerateCase_NoWhenClauses covers CASE with no WHENs (unusual but valid).
func TestGenerateCase_NoWhenClauses(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.CaseExpr{
		Expr:        nil,
		WhenClauses: []parser.WhenClause{},
		ElseClause:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("CASE no-when failed: %v", err)
	}
}

// --- generateInSubqueryMaterialised (80.0%) ---

// TestGenerateIn_SubqueryMaterialised covers the materialized IN subquery path.
func TestGenerateIn_SubqueryMaterialised(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Provide an executor that returns rows
	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, nil
	}

	selectStmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	}
	expr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
		Not:    false,
		Select: selectStmt,
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("IN subquery (materialised) failed: %v", err)
	}
}

// TestGenerateIn_SubqueryMaterialised_Empty covers the empty rows path.
func TestGenerateIn_SubqueryMaterialised_Empty(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{}, nil
	}
	gen.subqueryCompiler = func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		return vdbe.New(), nil
	}

	selectStmt := &parser.SelectStmt{}
	expr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Not:    true,
		Select: selectStmt,
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("IN subquery empty (NOT) failed: %v", err)
	}
}

// --- generateExistsMaterialised NOT path (88.9%) ---

// TestGenerateExists_Materialised_NotTrue covers NOT EXISTS with rows present.
func TestGenerateExists_Materialised_NotTrue(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{1}}, nil
	}

	selectStmt := &parser.SelectStmt{}
	expr := &parser.ExistsExpr{
		Select: selectStmt,
		Not:    true,
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("NOT EXISTS materialised failed: %v", err)
	}
}

// TestGenerateExists_Materialised_NotFalse covers NOT EXISTS with no rows.
func TestGenerateExists_Materialised_NotFalse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{}, nil
	}

	selectStmt := &parser.SelectStmt{}
	expr := &parser.ExistsExpr{
		Select: selectStmt,
		Not:    true,
	}
	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("NOT EXISTS materialised (no rows) failed: %v", err)
	}
}

// --- affinity: GetComparisonAffinity (87.5%) ---

// TestGetComparisonAffinity_NilExpr verifies AFF_NONE for nil.
func TestGetComparisonAffinity_NilExpr(t *testing.T) {
	aff := GetComparisonAffinity(nil)
	if aff != AFF_NONE {
		t.Errorf("expected AFF_NONE for nil, got %v", aff)
	}
}

// TestGetComparisonAffinity_NonComparison verifies AFF_NONE for non-comparison op.
func TestGetComparisonAffinity_NonComparison(t *testing.T) {
	e := &Expr{Op: OpPlus}
	aff := GetComparisonAffinity(e)
	if aff != AFF_NONE {
		t.Errorf("expected AFF_NONE for non-comparison op, got %v", aff)
	}
}

// TestGetComparisonAffinity_NilLeft verifies AFF_BLOB when Left is nil.
func TestGetComparisonAffinity_NilLeft(t *testing.T) {
	e := &Expr{Op: OpEq, Left: nil}
	aff := GetComparisonAffinity(e)
	if aff != AFF_BLOB {
		t.Errorf("expected AFF_BLOB for nil Left, got %v", aff)
	}
}

// TestGetComparisonAffinity_WithRight verifies affinity from right side.
func TestGetComparisonAffinity_WithRight(t *testing.T) {
	e := &Expr{
		Op:    OpEq,
		Left:  &Expr{Op: OpColumn, Affinity: AFF_INTEGER},
		Right: &Expr{Op: OpString, Affinity: AFF_TEXT},
	}
	aff := GetComparisonAffinity(e)
	// Should compute compare affinity from left and right
	if aff == AFF_NONE {
		t.Errorf("expected non-AFF_NONE affinity, got AFF_NONE")
	}
}

// --- affinity: applyIntegerAffinity (88.9%) ---

// TestApplyIntegerAffinity_Float covers float→int64 conversion.
func TestApplyIntegerAffinity_Float(t *testing.T) {
	result := applyIntegerAffinity(float64(3.7))
	if result != int64(3) {
		t.Errorf("expected int64(3), got %v", result)
	}
}

// TestApplyIntegerAffinity_StringInt covers string-as-int path.
func TestApplyIntegerAffinity_StringInt(t *testing.T) {
	result := applyIntegerAffinity("42")
	if result != int64(42) {
		t.Errorf("expected int64(42), got %v", result)
	}
}

// TestApplyIntegerAffinity_StringFloat covers string-as-float→int path.
func TestApplyIntegerAffinity_StringFloat(t *testing.T) {
	result := applyIntegerAffinity("3.9")
	if result != int64(3) {
		t.Errorf("expected int64(3), got %v", result)
	}
}

// TestApplyIntegerAffinity_StringInvalid covers non-numeric string passthrough.
func TestApplyIntegerAffinity_StringInvalid(t *testing.T) {
	result := applyIntegerAffinity("hello")
	if result != "hello" {
		t.Errorf("expected passthrough 'hello', got %v", result)
	}
}

// --- affinity: applyNumericAffinity (90.9%) ---

// TestApplyNumericAffinity_FloatWholeNumber covers float-to-int conversion.
func TestApplyNumericAffinity_FloatWholeNumber(t *testing.T) {
	result := applyNumericAffinity(float64(4.0))
	if result != int64(4) {
		t.Errorf("expected int64(4), got %v (%T)", result, result)
	}
}

// TestApplyNumericAffinity_FloatFractional covers float passthrough.
func TestApplyNumericAffinity_FloatFractional(t *testing.T) {
	result := applyNumericAffinity(float64(3.14))
	if result != float64(3.14) {
		t.Errorf("expected float64(3.14), got %v", result)
	}
}

// TestApplyNumericAffinity_StringInt covers string→int path.
func TestApplyNumericAffinity_StringInt(t *testing.T) {
	result := applyNumericAffinity("99")
	if result != int64(99) {
		t.Errorf("expected int64(99), got %v", result)
	}
}

// TestApplyNumericAffinity_StringFloat covers string→float path.
func TestApplyNumericAffinity_StringFloat(t *testing.T) {
	result := applyNumericAffinity("1.5")
	if result != float64(1.5) {
		t.Errorf("expected float64(1.5), got %v", result)
	}
}

// --- castToNumeric (85.7%) ---

// TestCastToNumeric_String covers the string path in castToNumeric.
func TestCastToNumeric_String(t *testing.T) {
	result := castToNumeric("123")
	if result != int64(123) {
		t.Errorf("expected int64(123), got %v", result)
	}
}

// TestCastToNumeric_StringFloat covers string→float in castToNumeric.
// "2.71828" converts to integer 2 via CoerceToInteger (truncation).
func TestCastToNumeric_StringFloat(t *testing.T) {
	result := castToNumeric("2.71828")
	// CoerceToInteger parses the float and truncates to int
	if result != int64(2) {
		t.Errorf("expected int64(2), got %v (%T)", result, result)
	}
}

// TestCastToNumeric_TrueFloat covers a non-integer float passthrough.
func TestCastToNumeric_TrueFloat(t *testing.T) {
	// A string that CoerceToInteger won't handle (no leading digits integer form)
	// but ParseFloat will — use an exponent form.
	result := castToNumeric("1e300")
	// CoerceToInteger truncates very large float to max int; if it fails, ParseFloat handles it
	// Just verify no panic and a numeric result is returned
	if result == nil {
		t.Error("expected non-nil result")
	}
}

// TestCastToNumeric_NonNumericString covers non-numeric string passthrough.
func TestCastToNumeric_NonNumericString(t *testing.T) {
	result := castToNumeric("xyz")
	if result != "xyz" {
		t.Errorf("expected passthrough 'xyz', got %v", result)
	}
}

// --- EvaluateCast (87.5%) ---

// TestEvaluateCast_Nil verifies nil passes through.
func TestEvaluateCast_Nil(t *testing.T) {
	result := EvaluateCast(nil, "INTEGER")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// TestEvaluateCast_Text verifies TEXT affinity conversion.
func TestEvaluateCast_Text(t *testing.T) {
	result := EvaluateCast(int64(42), "TEXT")
	if result != "42" {
		t.Errorf("expected '42', got %v", result)
	}
}

// TestEvaluateCast_Integer verifies INTEGER affinity conversion.
func TestEvaluateCast_Integer(t *testing.T) {
	result := EvaluateCast("123", "INTEGER")
	if result != int64(123) {
		t.Errorf("expected int64(123), got %v", result)
	}
}

// TestEvaluateCast_Real verifies REAL affinity conversion.
func TestEvaluateCast_Real(t *testing.T) {
	result := EvaluateCast(int64(5), "REAL")
	if result != float64(5) {
		t.Errorf("expected float64(5), got %v", result)
	}
}

// TestEvaluateCast_Numeric verifies NUMERIC affinity conversion.
func TestEvaluateCast_Numeric(t *testing.T) {
	result := EvaluateCast("77", "NUMERIC")
	if result != int64(77) {
		t.Errorf("expected int64(77), got %v", result)
	}
}

// TestEvaluateCast_Blob verifies BLOB affinity conversion.
func TestEvaluateCast_Blob(t *testing.T) {
	result := EvaluateCast("hello", "BLOB")
	b, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(b) != "hello" {
		t.Errorf("expected 'hello', got %s", string(b))
	}
}

// TestEvaluateCast_UnknownType verifies passthrough for unrecognized type.
func TestEvaluateCast_UnknownType(t *testing.T) {
	result := EvaluateCast(int64(10), "UNKNOWN_TYPE")
	if result != int64(10) {
		t.Errorf("expected passthrough int64(10), got %v", result)
	}
}

// --- generateSubqueryBytecodeEmbedding (91.7%) ---

// TestGenerateSubquery_NoCompilerNoExecutor covers the error when neither is set.
func TestGenerateSubquery_NoCompilerNoExecutor(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Neither executor nor compiler set
	expr := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{},
	}
	_, err := gen.GenerateExpr(expr)
	if err == nil {
		t.Fatal("expected error when no subquery compiler or executor set")
	}
}

// --- rewriteBinary: no-change path (87.5%) ---

// TestRewriteBinary_NoChange covers the path where children don't change.
func TestRewriteBinary_NoChange(t *testing.T) {
	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Op:    parser.OpPlus,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	// Empty refMap - no rewrites happen
	result := rewriteBinary(expr, map[string]interface{}{})
	if result != expr {
		t.Error("expected same pointer when no refs match")
	}
}
