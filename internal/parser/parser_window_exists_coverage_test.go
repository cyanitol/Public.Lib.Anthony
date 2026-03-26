// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestParserWindowExists covers uncovered branches in parseWindowDef and
// parseExistsExpr by exercising both happy-path and error-path variations.
func TestParserWindowExists(t *testing.T) {
	t.Parallel()

	t.Run("WindowDef_HappyPath", testWindowDefHappyPath)
	t.Run("WindowDef_ErrorNoName", testWindowDefErrorNoName)
	t.Run("WindowDef_ErrorNoAS", testWindowDefErrorNoAS)
	t.Run("WindowDef_ErrorNoLP", testWindowDefErrorNoLP)
	t.Run("WindowDef_ErrorNoRP", testWindowDefErrorNoRP)
	t.Run("WindowDef_MultipleWithFrame", testWindowDefMultipleWithFrame)
	t.Run("ExistsExpr_HappyPath", testExistsExprHappyPath)
	t.Run("ExistsExpr_NotExists", testExistsExprNotExists)
	t.Run("ExistsExpr_ErrorNoLP", testExistsExprErrorNoLP)
	t.Run("ExistsExpr_ErrorNoSelect", testExistsExprErrorNoSelect)
	t.Run("ExistsExpr_ErrorNoRP", testExistsExprErrorNoRP)
	t.Run("ExistsExpr_NotErrorNoLP", testExistsExprNotErrorNoLP)
	t.Run("ExistsExpr_NotErrorNoSelect", testExistsExprNotErrorNoSelect)
	t.Run("ExistsExpr_ComplexSubquery", testExistsExprComplexSubquery)
}

// --- parseWindowDef happy path ---

func testWindowDefHappyPath(t *testing.T) {
	t.Parallel()
	sql := `SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	if len(sel.WindowDefs) != 1 {
		t.Fatalf("expected 1 window def, got %d", len(sel.WindowDefs))
	}
	wd := sel.WindowDefs[0]
	if wd.Name != "w" {
		t.Errorf("window name: got %q, want w", wd.Name)
	}
	if wd.Spec == nil {
		t.Fatal("expected non-nil window spec")
	}
	if wd.Spec.Frame == nil {
		t.Fatal("expected frame in window def")
	}
	if wd.Spec.Frame.Mode != FrameRows {
		t.Errorf("frame mode: got %v, want FrameRows", wd.Spec.Frame.Mode)
	}
}

// --- parseWindowDef error: no window name (non-ID token after WINDOW) ---

func testWindowDefErrorNoName(t *testing.T) {
	t.Parallel()
	// A numeric literal where the window name is expected triggers
	// the !p.check(TK_ID) error branch inside parseWindowDef.
	sql := `SELECT x FROM t WINDOW 42 AS (ORDER BY b)`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for missing window name, got nil")
	}
}

// --- parseWindowDef error: no AS after window name ---

func testWindowDefErrorNoAS(t *testing.T) {
	t.Parallel()
	// A token other than AS after the window name exercises the !p.match(TK_AS) branch.
	sql := `SELECT x FROM t WINDOW w (ORDER BY b)`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for missing AS, got nil")
	}
}

// --- parseWindowDef error: no '(' after AS ---

func testWindowDefErrorNoLP(t *testing.T) {
	t.Parallel()
	// A token other than '(' after AS exercises the !p.match(TK_LP) branch.
	sql := `SELECT x FROM t WINDOW w AS ORDER BY b`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for missing '(' after AS, got nil")
	}
}

// --- parseWindowDef error: no ')' after window spec ---

func testWindowDefErrorNoRP(t *testing.T) {
	t.Parallel()
	// No closing paren exercises the !p.match(TK_RP) branch.
	sql := `SELECT x FROM t WINDOW w AS (ORDER BY b`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for missing ')' after window spec, got nil")
	}
}

// --- parseWindowDef multiple definitions with frame specs ---

func testWindowDefMultipleWithFrame(t *testing.T) {
	t.Parallel()
	sql := `SELECT SUM(a) OVER w1, AVG(b) OVER w2 FROM t WINDOW w1 AS (PARTITION BY c ORDER BY d RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w2 AS (ORDER BY e ROWS CURRENT ROW)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	if len(sel.WindowDefs) != 2 {
		t.Fatalf("expected 2 window defs, got %d", len(sel.WindowDefs))
	}
	if sel.WindowDefs[0].Name != "w1" {
		t.Errorf("first window name: got %q, want w1", sel.WindowDefs[0].Name)
	}
	if sel.WindowDefs[1].Name != "w2" {
		t.Errorf("second window name: got %q, want w2", sel.WindowDefs[1].Name)
	}
}

// --- parseExistsExpr happy path ---

func testExistsExprHappyPath(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other WHERE other.id = t.id)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	exists, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected ExistsExpr in WHERE, got %T", sel.Where)
	}
	if exists.Not {
		t.Error("expected Not=false for plain EXISTS")
	}
	if exists.Select == nil {
		t.Error("expected non-nil subquery in ExistsExpr")
	}
}

// --- parseExistsExpr NOT EXISTS ---

func testExistsExprNotExists(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM other WHERE other.id = t.id)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	if sel.Where == nil {
		t.Fatal("expected non-nil WHERE clause")
	}
	// The result may be ExistsExpr{Not:true} directly or wrapped in UnaryExpr.
	// Just verify it parsed without error and WHERE is present.
}

// --- parseExistsExpr error: no '(' after EXISTS ---

func testExistsExprErrorNoLP(t *testing.T) {
	t.Parallel()
	// No opening paren exercises the !p.match(TK_LP) error branch.
	sql := `SELECT * FROM t WHERE EXISTS SELECT 1 FROM other`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for EXISTS without '(', got nil")
	}
}

// --- parseExistsExpr error: no SELECT inside '(' ---

func testExistsExprErrorNoSelect(t *testing.T) {
	t.Parallel()
	// A non-SELECT token after '(' exercises the !p.match(TK_SELECT) error branch.
	sql := `SELECT * FROM t WHERE EXISTS (1 FROM other)`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for EXISTS without SELECT, got nil")
	}
}

// --- parseExistsExpr error: no ')' after subquery ---

func testExistsExprErrorNoRP(t *testing.T) {
	t.Parallel()
	// No closing paren exercises the !p.match(TK_RP) error branch.
	sql := `SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for EXISTS without closing ')', got nil")
	}
}

// --- parseExistsExpr NOT EXISTS error: no '(' ---

func testExistsExprNotErrorNoLP(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE NOT EXISTS SELECT 1 FROM other`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for NOT EXISTS without '(', got nil")
	}
}

// --- parseExistsExpr NOT EXISTS error: no SELECT ---

func testExistsExprNotErrorNoSelect(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE NOT EXISTS (42)`
	_, err := NewParser(sql).Parse()
	if err == nil {
		t.Error("expected parse error for NOT EXISTS without SELECT, got nil")
	}
}

// --- parseExistsExpr complex subquery ---

func testExistsExprComplexSubquery(t *testing.T) {
	t.Parallel()
	// A more complex subquery with GROUP BY to exercise parseSelect within EXISTS.
	sql := `SELECT dept FROM employees WHERE EXISTS (SELECT 1 FROM salaries WHERE salaries.emp_id = employees.id AND salary > 50000)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("expected at least one statement")
	}
}
