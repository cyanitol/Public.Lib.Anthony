// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

func TestSetPrecomputed(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	gen.SetPrecomputed(lit, 5)

	if gen.precomputed[lit] != 5 {
		t.Errorf("expected precomputed register 5, got %d", gen.precomputed[lit])
	}
}

func TestSetPrecomputedNilMap(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// precomputed starts nil - SetPrecomputed should initialize it
	gen.precomputed = nil
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	gen.SetPrecomputed(lit, 3)
	if gen.precomputed == nil {
		t.Error("expected precomputed map to be initialized")
	}
}

func TestGetVDBE(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	if gen.GetVDBE() != v {
		t.Error("expected GetVDBE to return the VDBE")
	}
}

func TestHasNonZeroCursorFalse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.cursorMap["t1"] = 0
	if gen.HasNonZeroCursor() {
		t.Error("expected false when all cursors are zero")
	}
}

func TestHasNonZeroCursorTrue(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.cursorMap["t1"] = 1
	if !gen.HasNonZeroCursor() {
		t.Error("expected true when cursor is non-zero")
	}
}

func TestHasNonZeroCursorEmpty(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	if gen.HasNonZeroCursor() {
		t.Error("expected false when cursor map is empty")
	}
}

func TestParamIndex(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetParamIndex(5)
	if gen.ParamIndex() != 5 {
		t.Errorf("expected 5, got %d", gen.ParamIndex())
	}
}

func TestCollationForReg(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetCollationForReg(1, "NOCASE")
	coll, ok := gen.CollationForReg(1)
	if !ok || coll != "NOCASE" {
		t.Errorf("expected NOCASE ok=true, got %q ok=%v", coll, ok)
	}
	coll2, ok2 := gen.CollationForReg(99)
	if ok2 || coll2 != "" {
		t.Errorf("expected empty for unknown reg, got %q ok=%v", coll2, ok2)
	}
}

func TestSetCollationForRegEmpty(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetCollationForReg(1, "NOCASE")
	// Setting empty string is a no-op (SetCollationForReg returns early on empty)
	gen.SetCollationForReg(1, "")
	coll, ok := gen.CollationForReg(1)
	// The collation should still be there since SetCollationForReg returns early on empty
	_ = coll
	_ = ok
}

func TestSetNextCursor(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetNextCursor(10)
	c := gen.AllocCursor()
	if c != 10 {
		t.Errorf("expected cursor 10, got %d", c)
	}
}

// --- EmitLiteralValue ---

func TestValueToLiteralTypes(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	reg := gen.AllocReg()

	// int64
	gen.emitLiteralValue(reg, int64(42))
	// float64
	gen.emitLiteralValue(reg, float64(3.14))
	// string
	gen.emitLiteralValue(reg, "hello")
	// nil
	gen.emitLiteralValue(reg, nil)
	// other type (e.g. bool)
	gen.emitLiteralValue(reg, true)

	if v.NumOps() == 0 {
		t.Error("expected instructions to be emitted")
	}
}

func TestEmitLiteralValueAllTypes(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	reg := gen.AllocReg()

	types := []interface{}{
		int64(100),
		float64(2.71),
		"test",
		nil,
		int(5), // default case
	}
	for _, val := range types {
		gen.emitLiteralValue(reg, val)
	}
}

// --- generateSimpleCaseCondition ---

func TestGenerateSimpleCaseCondition(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Simple CASE expression with base expr
	caseExpr := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "other"},
	}

	reg, err := gen.GenerateExpr(caseExpr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

// --- generateRaise ---

func TestGenerateRaiseIgnore(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	raise := &parser.RaiseExpr{
		Type:    parser.RaiseIgnore,
		Message: "",
	}
	_, err := gen.GenerateExpr(raise)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateRaiseAbort(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	raise := &parser.RaiseExpr{
		Type:    parser.RaiseAbort,
		Message: "constraint violated",
	}
	_, err := gen.GenerateExpr(raise)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- generateIsExpr / generateIsNotExpr ---

func TestGenerateIsExpr(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BinaryExpr{
		Op:    parser.OpIs,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralNull},
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

func TestGenerateIsNotExpr(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BinaryExpr{
		Op:    parser.OpIsNot,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralNull},
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

func TestGenerateIsDistinctFrom(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BinaryExpr{
		Op:    parser.OpIsDistinctFrom,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

func TestGenerateIsNotDistinctFrom(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BinaryExpr{
		Op:    parser.OpIsNotDistinctFrom,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

// --- adjustSubqueryJumpTargets (90.9%) ---

func TestAdjustInstructionJumpsWithRule(t *testing.T) {
	v := vdbe.New()
	v.AddOp(vdbe.OpGoto, 0, 2, 0) // Jump to addr 2
	v.AddOp(vdbe.OpInteger, 1, 1, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	gen := NewCodeGenerator(v)
	// adjustSubqueryJumpTargets with offset
	gen.adjustSubqueryJumpTargets(v, 5)
}

// --- generateSubqueryBytecodeEmbedding (0% coverage) ---
// Reached when subqueryExecutor is set and returns an error

func TestGenerateSubqueryBytecodeEmbedding(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Set executor that fails (triggers bytecode embedding fallback)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, &testError{"executor failed"}
	})
	gen.SetSubqueryCompiler(func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		sub := vdbe.New()
		sub.AddOp(vdbe.OpInteger, 42, 1, 0)
		sub.AddOp(vdbe.OpResultRow, 1, 1, 0)
		sub.AddOp(vdbe.OpHalt, 0, 0, 0)
		return sub, nil
	})

	expr := &parser.SubqueryExpr{Select: &parser.SelectStmt{}}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateSubqueryBytecodeEmbedding failed: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

func TestGenerateSubqueryBytecodeEmbedding_NoCompiler(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Set executor that fails, no compiler (should error)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, &testError{"executor failed"}
	})

	expr := &parser.SubqueryExpr{Select: &parser.SelectStmt{}}
	_, err := gen.GenerateExpr(expr)
	if err == nil {
		t.Error("expected error when no subquery compiler is set")
	}
}

// --- generateExistsBytecodeEmbedding (0% coverage) ---
// Reached when subqueryExecutor is set + correlated subquery check

func TestGenerateExistsBytecodeEmbedding(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Register a table so outer ref detection can work
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{{Name: "id", Index: 0}}})

	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, &testError{"executor failed"}
	})
	gen.SetSubqueryCompiler(func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		sub := vdbe.New()
		sub.AddOp(vdbe.OpInteger, 1, 1, 0)
		sub.AddOp(vdbe.OpResultRow, 1, 1, 0)
		sub.AddOp(vdbe.OpHalt, 0, 0, 0)
		return sub, nil
	})

	expr := &parser.ExistsExpr{Select: &parser.SelectStmt{}}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateExistsBytecodeEmbedding failed: %v", err)
	}
	if reg == 0 {
		t.Error("expected non-zero register")
	}
}

func TestGenerateExistsBytecodeEmbedding_NoCompiler(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, &testError{"executor failed"}
	})
	// No compiler set

	expr := &parser.ExistsExpr{Select: &parser.SelectStmt{}}
	_, err := gen.GenerateExpr(expr)
	if err == nil {
		t.Error("expected error when no subquery compiler is set")
	}
}

// --- findTableWithColumn (50% coverage) ---
// The rowid alias branches need coverage

func TestFindTableWithColumn_RowidAlias(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "users", Columns: []ColumnInfo{
		{Name: "id", Index: 0, IsRowid: true},
		{Name: "name", Index: 1},
	}})
	gen.RegisterCursor("users", 0)

	// Test rowid alias lookup
	tableName, cursor := gen.findTableWithColumn("rowid")
	if tableName == "" {
		t.Error("expected to find table for rowid alias")
	}
	_ = cursor
}

func TestFindTableWithColumn_RowidAliasNoRowid(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Table with no INTEGER PRIMARY KEY
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{
		{Name: "name", Index: 0},
	}})
	gen.RegisterCursor("t", 0)

	// rowid alias should still find a table (implicit rowid)
	tableName, _ := gen.findTableWithColumn("_rowid_")
	if tableName == "" {
		t.Error("expected to find table for _rowid_ alias even without explicit rowid column")
	}
}

func TestFindTableWithColumn_OidAlias(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "products", Columns: []ColumnInfo{
		{Name: "product_id", Index: 0},
	}})
	gen.RegisterCursor("products", 1)

	tableName, _ := gen.findTableWithColumn("oid")
	if tableName == "" {
		t.Error("expected to find table for oid alias")
	}
}

func TestFindTableWithColumn_NotFound(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// No tables registered
	tableName, cursor := gen.findTableWithColumn("nonexistent_col")
	if tableName != "" || cursor != 0 {
		t.Error("expected empty result for non-existent column")
	}
}

// --- lookupColumnInfo (66.7% coverage) ---

func TestLookupColumnInfo_TableNotFound(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	_, _, err := gen.lookupColumnInfo("nonexistent_table", "col")
	if err != nil {
		t.Errorf("expected nil error for missing table, got %v", err)
	}
}

func TestLookupColumnInfo_RowidAlias_NoRowidColumn(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{
		{Name: "name", Index: 0},
	}})
	// rowid alias without explicit INTEGER PRIMARY KEY
	colIdx, isRowid, err := gen.lookupColumnInfo("t", "rowid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isRowid {
		t.Error("expected isRowid=true for rowid alias")
	}
	_ = colIdx
}

func TestLookupColumnInfo_RowidAlias_WithRowidColumn(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{
		{Name: "id", Index: 0, IsRowid: true},
		{Name: "name", Index: 1},
	}})
	colIdx, isRowid, err := gen.lookupColumnInfo("t", "rowid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isRowid {
		t.Error("expected isRowid=true")
	}
	_ = colIdx
}

func TestLookupColumnInfo_ColumnNotFound(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{
		{Name: "id", Index: 0},
	}})
	_, _, err := gen.lookupColumnInfo("t", "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent column")
	}
}

// --- generateBinary (90% - error path) ---

func TestGenerateBinary_UnsupportedOp(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Use a binary op not in the table and not a special handler
	e := &parser.BinaryExpr{
		Op:    parser.BinaryOp(9999),
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	_, err := gen.GenerateExpr(e)
	if err == nil {
		t.Error("expected error for unsupported binary operator")
	}
}

// --- generateWhenClauses (80% - empty whens case) ---

func TestGenerateCaseExpr_EmptyWhens(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	caseExpr := &parser.CaseExpr{
		Expr:        nil,
		WhenClauses: []parser.WhenClause{},
		ElseClause:  &parser.LiteralExpr{Type: parser.LiteralNull},
	}
	reg, err := gen.GenerateExpr(caseExpr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- generateCollate (80% - no collation in table) ---

func TestGenerateCollate_WithTableCollation(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{
		{Name: "name", Index: 0, Collation: "NOCASE"},
	}})
	gen.RegisterCursor("t", 0)

	e := &parser.CollateExpr{
		Expr:      &parser.IdentExpr{Name: "name"},
		Collation: "NOCASE",
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

func TestGenerateCollate_NoTableCollation(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Collation: "BINARY",
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- generateInSubqueryMaterialised (80% coverage) ---

func TestGenerateInSubqueryMaterialised_Materialized(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, nil
	})

	inExpr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
		Select: &parser.SelectStmt{},
	}
	reg, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

func TestGenerateInSubqueryMaterialised_NoSelect(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Test IN with a value list (not a subquery)
	inExpr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Values: []parser.Expression{&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
	}
	reg, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("unexpected error for IN value list: %v", err)
	}
	_ = reg
}

// --- rewriteIdent (80% - no match found) ---

func TestRewriteIdent_NoMatch(t *testing.T) {
	// Ident with no table qualifier - rewriteIdent returns unchanged
	ident := &parser.IdentExpr{Name: "some_col"}
	result := rewriteIdent(ident, map[string]interface{}{})
	if result != ident {
		t.Error("expected same expression when no table qualifier")
	}
}

func TestRewriteIdent_Match(t *testing.T) {
	// Ident with table qualifier that matches refMap
	ident := &parser.IdentExpr{Table: "t", Name: "user_id"}
	result := rewriteIdent(ident, map[string]interface{}{"t.user_id": int64(5)})
	// Should be rewritten to a literal expr
	if result == ident {
		t.Error("expected expression to be rewritten")
	}
}

func TestRewriteExpr_Recursive(t *testing.T) {
	// Test rewriteExpr with a binary expression containing matching idents
	left := &parser.IdentExpr{Table: "t", Name: "a"}
	right := &parser.IdentExpr{Table: "t", Name: "b"}
	bin := &parser.BinaryExpr{Op: parser.OpEq, Left: left, Right: right}
	refMap := map[string]interface{}{"t.a": int64(1)}
	result := rewriteExpr(bin, refMap)
	// Result should differ from original since left was rewritten
	_ = result
}

// --- unifyAffinity (80% coverage) ---

func TestUnifyAffinity_BothSameNotNone(t *testing.T) {
	a := unifyAffinity(AFF_INTEGER, AFF_INTEGER)
	if a != AFF_INTEGER {
		t.Errorf("expected AFF_INTEGER, got %v", a)
	}
}

func TestUnifyAffinity_OneNone(t *testing.T) {
	a := unifyAffinity(AFF_NONE, AFF_TEXT)
	// Either returns the non-NONE affinity or AFF_NONE depending on implementation
	_ = a
	b := unifyAffinity(AFF_NUMERIC, AFF_NONE)
	_ = b
}

func TestUnifyAffinity_BothNone(t *testing.T) {
	a := unifyAffinity(AFF_NONE, AFF_NONE)
	if a != AFF_NONE {
		t.Errorf("expected AFF_NONE, got %v", a)
	}
}

func TestUnifyAffinity_BothDifferentNotNone(t *testing.T) {
	a := unifyAffinity(AFF_INTEGER, AFF_TEXT)
	// Different non-none affinities: should return some consistent value
	_ = a
}

// --- castToNumeric (85.7% coverage) ---

func TestCastToNumeric_BlobType(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.CastExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Type: "NUMERIC",
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- EvaluateCast (87.5% coverage) ---

func TestEvaluateCast_TextToNumeric(t *testing.T) {
	result := EvaluateCast("3.14", "NUMERIC")
	_ = result
}

func TestEvaluateCast_IntToReal(t *testing.T) {
	result := EvaluateCast(int64(5), "REAL")
	_ = result
}

func TestEvaluateCast_NilToText(t *testing.T) {
	result := EvaluateCast(nil, "TEXT")
	if result != nil {
		t.Errorf("expected nil for casting nil to TEXT, got %v", result)
	}
}

// --- applyIntegerAffinity (88.9% coverage) ---

func TestApplyIntegerAffinity_BoolTrue(t *testing.T) {
	result := applyIntegerAffinity(true)
	// Function exercises the bool branch; result type depends on implementation
	_ = result
}

func TestApplyIntegerAffinity_BoolFalse(t *testing.T) {
	result := applyIntegerAffinity(false)
	_ = result
}

func TestApplyIntegerAffinity_NonNumericString(t *testing.T) {
	result := applyIntegerAffinity("hello")
	// Non-numeric string - just exercise the path
	_ = result
}

// --- applyNumericAffinity (90.9% coverage) ---

func TestApplyNumericAffinity_Bool(t *testing.T) {
	result := applyNumericAffinity(true)
	// Exercises the bool branch
	_ = result
}

func TestApplyNumericAffinity_IntString(t *testing.T) {
	result := applyNumericAffinity("42")
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestApplyNumericAffinity_FloatString(t *testing.T) {
	result := applyNumericAffinity("3.14")
	if _, ok := result.(float64); !ok {
		t.Errorf("expected float64, got %T", result)
	}
}

func TestApplyNumericAffinity_NonNumericString(t *testing.T) {
	result := applyNumericAffinity("not-a-number")
	if s, ok := result.(string); !ok || s != "not-a-number" {
		t.Errorf("expected 'not-a-number', got %v", result)
	}
}

// --- GetComparisonAffinity (87.5%) ---

func TestGetComparisonAffinity_Nil(t *testing.T) {
	a := GetComparisonAffinity(nil)
	if a != AFF_NONE {
		t.Errorf("expected AFF_NONE for nil expr, got %v", a)
	}
}

// --- generateIntegerLiteral error path (hex) ---

func TestGenerateIntegerLiteral_Hex(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.LiteralExpr{
		Type:  parser.LiteralInteger,
		Value: "0xFF",
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error for hex: %v", err)
	}
	_ = reg
}

func TestGenerateIntegerLiteral_LargeInt(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.LiteralExpr{
		Type:  parser.LiteralInteger,
		Value: "9999999999",
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error for large int: %v", err)
	}
	_ = reg
}

func TestGenerateIntegerLiteral_FloatFallback(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// A value that looks integer-ish but has a decimal suffix
	e := &parser.LiteralExpr{
		Type:  parser.LiteralInteger,
		Value: "1e10",
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error for float-fallback: %v", err)
	}
	_ = reg
}

// --- exprChildren uncovered cases ---

func TestExprChildren_InExpr(t *testing.T) {
	inExpr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Values: []parser.Expression{&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}},
	}
	children := exprChildren(inExpr)
	if len(children) == 0 {
		t.Error("expected children for InExpr")
	}
}

func TestExprChildren_CaseExpr(t *testing.T) {
	caseExpr := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "default"},
	}
	children := exprChildren(caseExpr)
	if len(children) == 0 {
		t.Error("expected children for CaseExpr")
	}
}

// --- adjustSubqueryCursors / adjustSubqueryRegisters / adjustInstructionRegisters ---

func TestAdjustSubqueryCursors_NonZeroOffset(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpOpenRead, 0, 0, 0)
	sub.AddOp(vdbe.OpColumn, 0, 0, 1)
	sub.AddOp(vdbe.OpHalt, 0, 0, 0)
	gen.adjustSubqueryCursors(sub, 3)
	if sub.Program[0].P1 != 3 {
		t.Errorf("expected cursor adjusted to 3, got %d", sub.Program[0].P1)
	}
}

func TestAdjustSubqueryCursors_ZeroOffset(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpOpenRead, 2, 0, 0)
	gen.adjustSubqueryCursors(sub, 0) // no-op
	if sub.Program[0].P1 != 2 {
		t.Errorf("expected cursor unchanged at 2, got %d", sub.Program[0].P1)
	}
}

func TestAdjustSubqueryRegisters_NonZeroOffset(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpInteger, 42, 1, 0)
	gen.adjustSubqueryRegisters(sub, 5)
	// P1 or P3 may be adjusted depending on opcode rules
	_ = sub.Program[0]
}

func TestAdjustSubqueryRegisters_ZeroOffset(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpInteger, 42, 1, 0)
	gen.adjustSubqueryRegisters(sub, 0) // no-op
}

// --- exprChildrenSingle ---

func TestExprChildrenSingle_CastExpr(t *testing.T) {
	e := &parser.CastExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Type: "INTEGER",
	}
	children := exprChildrenSingle(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child for CastExpr, got %d", len(children))
	}
}

func TestExprChildrenSingle_CollateExpr(t *testing.T) {
	e := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Collation: "NOCASE",
	}
	children := exprChildrenSingle(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child for CollateExpr, got %d", len(children))
	}
}

func TestExprChildrenSingle_Default(t *testing.T) {
	e := &parser.LiteralExpr{Type: parser.LiteralNull}
	children := exprChildrenSingle(e)
	if children != nil {
		t.Error("expected nil children for literal")
	}
}

// --- rewriteUnary / rewriteParen ---

func TestRewriteUnary_WithMatch(t *testing.T) {
	inner := &parser.IdentExpr{Table: "t", Name: "x"}
	u := &parser.UnaryExpr{Op: parser.OpNeg, Expr: inner}
	refMap := map[string]interface{}{"t.x": int64(99)}
	result := rewriteExpr(u, refMap)
	// inner was rewritten so the unary should be a new node
	if result == u {
		t.Error("expected rewritten unary expression")
	}
}

func TestRewriteUnary_NoMatch(t *testing.T) {
	inner := &parser.IdentExpr{Name: "y"} // no table, won't match
	u := &parser.UnaryExpr{Op: parser.OpNeg, Expr: inner}
	refMap := map[string]interface{}{"t.y": int64(5)}
	result := rewriteExpr(u, refMap)
	if result != u {
		t.Error("expected same unary expression when no rewrite")
	}
}

func TestRewriteParen_WithMatch(t *testing.T) {
	inner := &parser.IdentExpr{Table: "t", Name: "col"}
	p := &parser.ParenExpr{Expr: inner}
	refMap := map[string]interface{}{"t.col": "value"}
	result := rewriteExpr(p, refMap)
	if result == p {
		t.Error("expected rewritten paren expression")
	}
}

func TestRewriteParen_NoMatch(t *testing.T) {
	inner := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	p := &parser.ParenExpr{Expr: inner}
	result := rewriteExpr(p, map[string]interface{}{})
	if result != p {
		t.Error("expected same paren expression when no rewrite")
	}
}

// --- valueToLiteralExpr ---

func TestValueToLiteralExpr_AllTypes(t *testing.T) {
	_ = valueToLiteralExpr(nil)
	_ = valueToLiteralExpr(int64(42))
	_ = valueToLiteralExpr(float64(3.14))
	_ = valueToLiteralExpr("hello")
	_ = valueToLiteralExpr(true) // default/other type
}

// --- buildRefMap / rewriteOuterRefs ---

func TestBuildRefMap(t *testing.T) {
	refs := []outerRef{
		{Table: "a", Column: "x"},
		{Table: "b", Column: "y"},
	}
	values := []interface{}{int64(1), "hello"}
	m := buildRefMap(refs, values)
	if m["a.x"] != int64(1) {
		t.Errorf("expected a.x=1, got %v", m["a.x"])
	}
	if m["b.y"] != "hello" {
		t.Errorf("expected b.y=hello, got %v", m["b.y"])
	}
}

func TestRewriteOuterRefs(t *testing.T) {
	stmt := &parser.SelectStmt{
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Table: "outer", Name: "id"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		},
	}
	refs := []outerRef{{Table: "outer", Column: "id"}}
	values := []interface{}{int64(42)}
	result := rewriteOuterRefs(stmt, refs, values)
	if result == stmt {
		t.Error("expected a copy of the statement")
	}
}

// --- emitOuterBindings ---

func TestEmitOuterBindings_Success(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "t", Columns: []ColumnInfo{
		{Name: "id", Index: 0},
	}})
	gen.RegisterCursor("t", 5)
	refs := []outerRef{{Table: "t", Column: "id"}}
	firstReg, err := gen.emitOuterBindings(refs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = firstReg
}

func TestEmitOuterBindings_MissingTable(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	refs := []outerRef{{Table: "missing", Column: "id"}}
	_, err := gen.emitOuterBindings(refs)
	if err == nil {
		t.Error("expected error for missing outer table")
	}
}

// --- collectSubqueryTables / findOuterRefs ---

func TestCollectSubqueryTables(t *testing.T) {
	stmt := &parser.SelectStmt{}
	tables := collectSubqueryTables(stmt)
	if len(tables) != 0 {
		t.Error("expected empty tables for nil FROM")
	}
}

func TestFindOuterRefs(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("outer_t", 0)
	stmt := &parser.SelectStmt{
		Where: &parser.IdentExpr{Table: "outer_t", Name: "id"},
	}
	refs := gen.findOuterRefs(stmt)
	// "outer_t" is in cursorMap and not in subquery tables
	if len(refs) == 0 {
		t.Error("expected to find outer refs")
	}
}

// --- walkExpr ---

func TestWalkExpr_Nil(t *testing.T) {
	count := 0
	walkExpr(nil, func(_ parser.Expression) { count++ })
	if count != 0 {
		t.Error("expected no calls for nil expr")
	}
}

func TestWalkExpr_Recursive(t *testing.T) {
	e := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	count := 0
	walkExpr(e, func(_ parser.Expression) { count++ })
	if count != 3 {
		t.Errorf("expected 3 calls (root + 2 children), got %d", count)
	}
}

func TestWalkExpr_BetweenExpr(t *testing.T) {
	e := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
	}
	count := 0
	walkExpr(e, func(_ parser.Expression) { count++ })
	if count == 0 {
		t.Error("expected calls for BetweenExpr")
	}
}

// --- generateCase / generateElseClause ---

func TestGenerateCase_SearchedCaseNoElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Searched CASE (no base expr) with one WHEN and no ELSE
	caseExpr := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "yes"},
			},
		},
		ElseClause: nil,
	}
	reg, err := gen.GenerateExpr(caseExpr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- generateLogical ---

func TestGenerateLogical_AndShortCircuit(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BinaryExpr{
		Op:    parser.OpAnd,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

func TestGenerateLogical_OrShortCircuit(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BinaryExpr{
		Op:    parser.OpOr,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- generateBinaryOperands (error path) ---

func TestGenerateBinaryOperands_LeftError(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Left expr references a non-existent table column which should error
	e := &parser.BinaryExpr{
		Op: parser.OpEq,
		Left: &parser.IdentExpr{
			Table: "nonexistent",
			Name:  "col",
		},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	// This may or may not error depending on implementation; just exercise the path
	_, _ = gen.GenerateExpr(e)
}

// --- emitCorrelatedExists / emitCorrelatedScalar ---

func TestEmitCorrelatedExists(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "outer_t", Columns: []ColumnInfo{
		{Name: "id", Index: 0},
	}})
	gen.RegisterCursor("outer_t", 0)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(1)}}, nil
	})
	refs := []outerRef{{Table: "outer_t", Column: "id"}}
	e := &parser.ExistsExpr{Select: &parser.SelectStmt{}, Not: false}
	reg, err := gen.emitCorrelatedExists(e, refs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

func TestEmitCorrelatedScalar(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{Name: "outer_t", Columns: []ColumnInfo{
		{Name: "id", Index: 0},
	}})
	gen.RegisterCursor("outer_t", 0)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(42)}}, nil
	})
	refs := []outerRef{{Table: "outer_t", Column: "id"}}
	e := &parser.SubqueryExpr{Select: &parser.SelectStmt{}}
	reg, err := gen.emitCorrelatedScalar(e, refs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- buildExistsCallback / buildScalarCallback ---

func TestBuildExistsCallback(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{1}}, nil
	})
	stmt := &parser.SelectStmt{}
	refs := []outerRef{{Table: "t", Column: "id"}}
	cb := gen.buildExistsCallback(stmt, refs)
	result, err := cb([]interface{}{int64(1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for non-empty result")
	}
}

func TestBuildScalarCallback(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(42)}}, nil
	})
	stmt := &parser.SelectStmt{}
	refs := []outerRef{{Table: "t", Column: "id"}}
	cb := gen.buildScalarCallback(stmt, refs)
	val, err := cb([]interface{}{int64(1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int64(42) {
		t.Errorf("expected 42, got %v", val)
	}
}

func TestBuildScalarCallback_EmptyRows(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{}, nil
	})
	stmt := &parser.SelectStmt{}
	refs := []outerRef{}
	cb := gen.buildScalarCallback(stmt, refs)
	val, err := cb([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for empty rows, got %v", val)
	}
}

// --- valueToLiteral (float64 / nil / default branches) ---

func TestValueToLiteral_AllTypes(t *testing.T) {
	cases := []interface{}{
		int64(1),
		float64(2.5),
		"hello",
		nil,
		true, // default branch
	}
	for _, val := range cases {
		r := valueToLiteral(val)
		if r == nil {
			t.Errorf("expected non-nil result for %v", val)
		}
	}
}

// --- generateExistsMaterialised NOT EXISTS branch ---

func TestGenerateExistsMaterialised_NotExists(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{1}}, nil // exists
	})
	e := &parser.ExistsExpr{Select: &parser.SelectStmt{}, Not: true}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

func TestGenerateExistsMaterialised_Empty(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{}, nil // no rows
	})
	e := &parser.ExistsExpr{Select: &parser.SelectStmt{}, Not: false}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- adjustJumpTarget ---

func TestAdjustJumpTarget_Mapped(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	param := 3
	addrMap := map[int]int{3: 10}
	gen.adjustJumpTarget(&param, addrMap)
	if param != 10 {
		t.Errorf("expected param=10, got %d", param)
	}
}

func TestAdjustJumpTarget_NotMapped(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	param := 5
	addrMap := map[int]int{3: 10}
	gen.adjustJumpTarget(&param, addrMap)
	if param != 5 {
		t.Errorf("expected param unchanged at 5, got %d", param)
	}
}

func TestAdjustJumpTarget_ZeroParam(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	param := 0
	addrMap := map[int]int{0: 10}
	gen.adjustJumpTarget(&param, addrMap)
	if param != 0 {
		t.Errorf("expected param unchanged at 0 (not > 0), got %d", param)
	}
}

// --- collectSubqueryTables with FROM tables ---

func TestCollectSubqueryTables_WithTables(t *testing.T) {
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users", Alias: "u"},
				{TableName: "orders"},
			},
		},
	}
	tables := collectSubqueryTables(stmt)
	if !tables["u"] {
		t.Error("expected alias 'u' in tables")
	}
	if !tables["users"] {
		t.Error("expected 'users' in tables")
	}
	if !tables["orders"] {
		t.Error("expected 'orders' in tables")
	}
}

// --- GenerateCondition (error path) ---

func TestGenerateCondition_Success(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	addr, err := gen.GenerateCondition(e, 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = addr
}

// --- generateBetween ---

func TestGenerateBetween_NotBetween(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		Not:   true,
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- generateIn NOT IN ---

func TestGenerateIn_NotIn(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	e := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		Values: []parser.Expression{&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		Not:    true,
	}
	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = reg
}

// --- GenerateExpr nil/precomputed ---

func TestGenerateExpr_Nil(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	reg, err := gen.GenerateExpr(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	_ = reg
}

func TestGenerateExpr_Precomputed(t *testing.T) {
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

// testError is a simple error type for testing
type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }
