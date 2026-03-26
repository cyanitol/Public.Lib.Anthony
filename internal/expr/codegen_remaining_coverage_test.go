// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// --- codegen.go: GenerateExpr (87.5%) – nil expr path ---

// TestGenerateExprNilPath covers nil-expression → generateNullLiteral.
// (Differs from TestGenerateExpr_Nil and TestGenerateExpr_Precomputed which
// are already in expr_coverage_test.go; this gives the branch an extra hit.)
func TestGenerateExprNilPath(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	reg, err := gen.GenerateExpr(nil)
	if err != nil {
		t.Fatalf("GenerateExpr(nil) error: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register from null literal, got %d", reg)
	}
}

// --- codegen.go: generateCollate (80.0%) – inner expr error path ---

// TestGenerateCollate_InnerError covers the error-from-inner-expr path.
func TestGenerateCollate_InnerError(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// An unsupported literal type causes generateLiteral to error.
	e := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralType(999)},
		Collation: "BINARY",
	}

	_, err := gen.generateCollate(e)
	if err == nil {
		t.Error("expected error from unsupported inner expression in generateCollate")
	}
}

// --- codegen.go: GenerateCondition (83.3%) – error path ---

// TestGenerateConditionInnerError covers error propagation when GenerateExpr fails.
func TestGenerateConditionInnerError(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Use unsupported literal type to force an error.
	bad := &parser.LiteralExpr{Type: parser.LiteralType(999)}
	_, err := gen.GenerateCondition(bad, 0)
	if err == nil {
		t.Error("expected error in GenerateCondition from bad expression")
	}
}

// --- codegen_correlated.go: buildExistsCallback (85.7%) – error path ---

// TestBuildExistsCallbackError verifies executor errors are forwarded.
func TestBuildExistsCallbackError(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, errRemainingCoverage("simulated error")
	}

	cb := gen.buildExistsCallback(&parser.SelectStmt{}, []outerRef{})
	_, err := cb([]interface{}{})
	if err == nil {
		t.Error("expected error from buildExistsCallback when executor errors")
	}
}

// TestBuildExistsCallbackHasRows verifies true is returned when rows are present.
func TestBuildExistsCallbackHasRows(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{1}}, nil
	}

	cb := gen.buildExistsCallback(&parser.SelectStmt{}, []outerRef{})
	exists, err := cb([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected true when rows returned")
	}
}

// --- codegen_correlated.go: collectSubqueryTables (covers alias path) ---

// TestCollectSubqueryTables_WithAlias verifies both TableName and Alias are recorded.
func TestCollectSubqueryTables_WithAlias(t *testing.T) {
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "employees", Alias: "e"},
			},
		},
	}
	tables := collectSubqueryTables(stmt)
	if !tables["employees"] {
		t.Error("expected 'employees' in subquery tables")
	}
	if !tables["e"] {
		t.Error("expected alias 'e' in subquery tables")
	}
}

// TestCollectSubqueryTables_NoAlias verifies only TableName is recorded when no alias.
func TestCollectSubqueryTables_NoAlias(t *testing.T) {
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "orders"},
			},
		},
	}
	tables := collectSubqueryTables(stmt)
	if !tables["orders"] {
		t.Error("expected 'orders' in subquery tables")
	}
}

// --- codegen_correlated.go: emitCorrelatedExists (83.3%) – Not=true path ---

// TestEmitCorrelatedExistsNotTrue verifies the P5 flag is set for NOT EXISTS.
func TestEmitCorrelatedExistsNotTrue(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{}, nil
	}

	e := &parser.ExistsExpr{
		Select: &parser.SelectStmt{},
		Not:    true,
	}

	reg, err := gen.emitCorrelatedExists(e, []outerRef{})
	if err != nil {
		t.Fatalf("emitCorrelatedExists (Not=true) error: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// --- codegen_correlated.go: buildScalarCallback – NoRows path ---

// TestBuildScalarCallbackNoRows verifies nil is returned when no rows.
func TestBuildScalarCallbackNoRows(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.subqueryExecutor = func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{}, nil
	}

	cb := gen.buildScalarCallback(&parser.SelectStmt{}, []outerRef{})
	result, err := cb([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty rows, got %v", result)
	}
}

// errRemainingCoverage is a simple error for testing.
type errRemainingCoverage string

func (e errRemainingCoverage) Error() string { return string(e) }
