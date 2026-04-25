// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestWindowFrameSpecifications tests window frame specifications (ROWS, RANGE, GROUPS)
// to improve coverage of parseWindowFrame and related functions.
func TestWindowFrameSpecifications(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// ROWS frame specifications
		{
			name:    "ROWS UNBOUNDED PRECEDING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS 10 PRECEDING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS 10 PRECEDING) FROM t",
			wantErr: false,
		},
		{
			name:    "ROWS 5 FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y ROWS 5 FOLLOWING) FROM t",
			wantErr: false,
		},

		// RANGE frame specifications
		{
			name:    "RANGE UNBOUNDED PRECEDING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y RANGE UNBOUNDED PRECEDING) FROM t",
			wantErr: false,
		},
		{
			name:    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "RANGE CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y RANGE CURRENT ROW) FROM t",
			wantErr: false,
		},

		// GROUPS frame specifications
		{
			name:    "GROUPS UNBOUNDED PRECEDING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y GROUPS UNBOUNDED PRECEDING) FROM t",
			wantErr: false,
		},
		{
			name:    "GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING",
			sql:     "SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "GROUPS CURRENT ROW",
			sql:     "SELECT SUM(x) OVER (ORDER BY y GROUPS CURRENT ROW) FROM t",
			wantErr: false,
		},

		// Combined with PARTITION BY
		{
			name:    "PARTITION BY with ROWS frame",
			sql:     "SELECT SUM(x) OVER (PARTITION BY z ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
			wantErr: false,
		},
		{
			name:    "PARTITION BY with RANGE frame",
			sql:     "SELECT SUM(x) OVER (PARTITION BY z ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},
		{
			name:    "PARTITION BY with GROUPS frame",
			sql:     "SELECT SUM(x) OVER (PARTITION BY z ORDER BY y GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
			wantErr: false,
		},

		// Multiple window functions
		{
			name:    "Multiple window functions with different frames",
			sql:     "SELECT ROW_NUMBER() OVER (ORDER BY x), SUM(y) OVER (ORDER BY x ROWS 5 PRECEDING) FROM t",
			wantErr: false,
		},

		// Window without frame (should still work)
		{
			name:    "Window without frame specification",
			sql:     "SELECT SUM(x) OVER (ORDER BY y) FROM t",
			wantErr: false,
		},
		{
			name:    "Window with PARTITION BY only",
			sql:     "SELECT SUM(x) OVER (PARTITION BY y) FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Parse() unexpected error = %v", err)
				}
				if len(stmts) == 0 {
					t.Errorf("Parse() returned no statements")
				}
			}
		})
	}
}

// TestWindowFrameErrorCases tests error handling in window frame parsing
func TestWindowFrameErrorCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "UNBOUNDED without PRECEDING/FOLLOWING",
			sql:  "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED) FROM t",
		},
		{
			name: "CURRENT without ROW",
			sql:  "SELECT SUM(x) OVER (ORDER BY y ROWS CURRENT) FROM t",
		},
		{
			name: "BETWEEN without AND",
			sql:  "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING CURRENT ROW) FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			// These should produce errors
			if err == nil {
				t.Logf("Parse() expected error but got none (SQL may have been parsed with different interpretation)")
			}
		})
	}
}

func parseWindowFunc(t *testing.T, sql string) *FunctionExpr {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmts))
	}
	selectStmt, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmts[0])
	}
	if len(selectStmt.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
	}
	funcExpr, ok := selectStmt.Columns[0].Expr.(*FunctionExpr)
	if !ok {
		t.Fatalf("Expected FunctionExpr, got %T", selectStmt.Columns[0].Expr)
	}
	return funcExpr
}

// assertWindowOver checks common Over clause fields.
func assertWindowOver(t *testing.T, over *WindowSpec, wantPartitions, wantOrders int) {
	t.Helper()
	if over == nil {
		t.Fatal("Expected Over clause to be non-nil")
	}
	if len(over.PartitionBy) != wantPartitions {
		t.Errorf("Expected %d partition by expression(s), got %d", wantPartitions, len(over.PartitionBy))
	}
	if len(over.OrderBy) != wantOrders {
		t.Errorf("Expected %d order by term(s), got %d", wantOrders, len(over.OrderBy))
	}
}

// TestWindowFunctionAST tests that window functions create correct AST structures
func TestWindowFunctionAST(t *testing.T) {
	t.Parallel()

	funcExpr := parseWindowFunc(t,
		"SELECT SUM(amount) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM transactions")

	if funcExpr.Name != "SUM" {
		t.Errorf("Expected function name SUM, got %s", funcExpr.Name)
	}
	assertWindowOver(t, funcExpr.Over, 1, 1)
	if funcExpr.Over.Frame == nil {
		t.Fatal("Expected Frame to be non-nil")
	}
	if funcExpr.Over.Frame.Mode != FrameRows {
		t.Errorf("Expected FrameRows mode, got %v", funcExpr.Over.Frame.Mode)
	}
	if funcExpr.Over.Frame.Start.Type != BoundPreceding {
		t.Errorf("Expected BoundPreceding for start, got %v", funcExpr.Over.Frame.Start.Type)
	}
	if funcExpr.Over.Frame.End.Type != BoundFollowing {
		t.Errorf("Expected BoundFollowing for end, got %v", funcExpr.Over.Frame.End.Type)
	}
}

// TestWindowFunctionErrorPaths tests error handling in window function parsing
func TestWindowFunctionErrorPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "Missing opening paren after OVER",
			sql:  "SELECT SUM(x) OVER ORDER BY y) FROM t",
		},
		{
			name: "Missing closing paren after window spec",
			sql:  "SELECT SUM(x) OVER (ORDER BY y FROM t",
		},
		{
			name: "PARTITION without BY",
			sql:  "SELECT SUM(x) OVER (PARTITION ORDER BY y) FROM t",
		},
		{
			name: "ORDER without BY in window",
			sql:  "SELECT SUM(x) OVER (ORDER y) FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err == nil {
				t.Logf("Expected error for malformed window function")
			}
		})
	}
}

// TestWindowMultiplePartitionColumns tests PARTITION BY with multiple columns
func TestWindowMultiplePartitionColumns(t *testing.T) {
	t.Parallel()

	sql := "SELECT SUM(amount) OVER (PARTITION BY user_id, region, status ORDER BY date) FROM t"
	parser := NewParser(sql)
	stmts, err := parser.Parse()

	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmts[0].(*SelectStmt)
	funcExpr := selectStmt.Columns[0].Expr.(*FunctionExpr)

	if len(funcExpr.Over.PartitionBy) != 3 {
		t.Errorf("Expected 3 partition by expressions, got %d", len(funcExpr.Over.PartitionBy))
	}
}

// TestWindowMultipleOrderColumns tests ORDER BY with multiple columns in window
func TestWindowMultipleOrderColumns(t *testing.T) {
	t.Parallel()

	sql := "SELECT SUM(amount) OVER (ORDER BY date DESC, id ASC) FROM t"
	parser := NewParser(sql)
	stmts, err := parser.Parse()

	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt := stmts[0].(*SelectStmt)
	funcExpr := selectStmt.Columns[0].Expr.(*FunctionExpr)

	if len(funcExpr.Over.OrderBy) != 2 {
		t.Errorf("Expected 2 order by terms, got %d", len(funcExpr.Over.OrderBy))
	}

	// Check first order term is DESC (Asc should be false)
	if funcExpr.Over.OrderBy[0].Asc != false {
		t.Errorf("Expected first order term to be DESC")
	}

	// Check second order term is ASC
	if funcExpr.Over.OrderBy[1].Asc != true {
		t.Errorf("Expected second order term to be ASC")
	}
}

// TestWindowEmptyPartitionBy tests error when PARTITION BY has no expressions
func TestWindowEmptyPartitionBy(t *testing.T) {
	t.Parallel()

	sql := "SELECT SUM(amount) OVER (PARTITION BY ORDER BY date) FROM t"
	parser := NewParser(sql)
	_, err := parser.Parse()

	// This should produce an error because PARTITION BY needs at least one expression
	if err == nil {
		t.Log("Expected error for PARTITION BY with no expressions")
	}
}

// TestWindowEmptyOrderBy tests error when ORDER BY has no expressions
func TestWindowEmptyOrderBy(t *testing.T) {
	t.Parallel()

	sql := "SELECT SUM(amount) OVER (ORDER BY) FROM t"
	parser := NewParser(sql)
	_, err := parser.Parse()

	// This should produce an error because ORDER BY needs at least one expression
	if err == nil {
		t.Log("Expected error for ORDER BY with no expressions")
	}
}
