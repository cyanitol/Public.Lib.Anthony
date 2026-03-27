// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// MC/DC tests for internal/planner – third file
//
// Sources covered: view.go, explain.go, cte.go, subquery.go
//
// Each function documents source file:line, compound condition, sub-condition
// labels, and the required N+1 MC/DC test cases.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Source: view.go flattenViewsInSelect
//   Condition: stmt == nil || stmt.From == nil
//   Sub-conditions:
//     A = stmt == nil
//     B = stmt.From == nil
//   Coverage pairs:
//     A=T, *   -> returns (stmt, nil) immediately (A dominates OR)
//     A=F, B=T -> returns (stmt, nil) immediately (B flips outcome)
//     A=F, B=F -> proceeds to process tables
// ---------------------------------------------------------------------------

func TestMCDC_FlattenViewsInSelect_NilGuard(t *testing.T) {
	t.Parallel()

	s := schema.NewSchema()

	cases := []struct {
		name      string
		stmt      *parser.SelectStmt
		wantNilOK bool // expect no error and non-nil result
	}{
		// A=T: nil stmt
		{"MCDC A=T: nil stmt", nil, true},
		// A=F, B=T: non-nil stmt but nil From (B flips)
		{"MCDC A=F B=T: nil From", &parser.SelectStmt{From: nil}, true},
		// A=F, B=F: non-nil stmt and non-nil From
		{"MCDC A=F B=F: non-nil From", &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "users"}},
			},
		}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := flattenViewsInSelect(tc.stmt, s, 0)
			if err != nil {
				t.Errorf("flattenViewsInSelect: unexpected error: %v", err)
			}
			if tc.stmt == nil && result != nil {
				t.Errorf("expected nil result for nil stmt, got non-nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go flattenViewsInSelect (and expandViewsInSelectWithDepth)
//   Condition: depth > 100
//   Sub-condition:
//     A = depth > 100
//   Coverage pairs:
//     A=T -> returns error "depth limit exceeded"
//     A=F -> proceeds normally
// ---------------------------------------------------------------------------

func TestMCDC_FlattenViewsInSelect_DepthLimit(t *testing.T) {
	t.Parallel()

	s := schema.NewSchema()
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "t"}},
		},
	}

	cases := []struct {
		name      string
		depth     int
		wantError bool
	}{
		// A=F: depth=0 -> no error
		{"MCDC A=F: depth=0", 0, false},
		// A=F: depth=100 -> no error (boundary: 100 is NOT > 100)
		{"MCDC A=F: depth=100", 100, false},
		// A=T: depth=101 -> error (depth > 100)
		{"MCDC A=T: depth=101", 101, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := flattenViewsInSelect(stmt, s, tc.depth)
			gotError := err != nil
			if gotError != tc.wantError {
				t.Errorf("flattenViewsInSelect(depth=%d): gotError=%v, wantError=%v, err=%v",
					tc.depth, gotError, tc.wantError, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go hasValidFromClauseForFlattening
//   Condition 1: sel.From == nil || len(sel.From.Tables) != 1
//   Sub-conditions:
//     A = sel.From == nil
//     B = len(sel.From.Tables) != 1
//   Coverage pairs:
//     A=T, *   -> false (A dominates OR)
//     A=F, B=T -> false (B flips outcome)
//     A=F, B=F -> continues to subquery/join checks -> true
//
//   Condition 2: len(sel.From.Joins) > 0
//   Sub-condition:
//     C = len(sel.From.Joins) > 0
//   Coverage pairs:
//     C=T -> false (has JOINs, cannot flatten)
//     C=F -> true (no JOINs, can flatten)
// ---------------------------------------------------------------------------

func TestMCDC_HasValidFromClauseForFlattening_NilAndCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sel  *parser.SelectStmt
		want bool
	}{
		// A=T: nil From
		{"MCDC A=T: nil From", &parser.SelectStmt{From: nil}, false},
		// A=F, B=T: From has 0 tables (B flips: len != 1)
		{"MCDC A=F B=T: zero tables", &parser.SelectStmt{
			From: &parser.FromClause{Tables: []parser.TableOrSubquery{}},
		}, false},
		// A=F, B=T: From has 2 tables
		{"MCDC A=F B=T: two tables", &parser.SelectStmt{
			From: &parser.FromClause{Tables: []parser.TableOrSubquery{
				{TableName: "a"}, {TableName: "b"},
			}},
		}, false},
		// A=F, B=F: exactly one table, no subquery, no joins -> true
		{"MCDC A=F B=F: one simple table", &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "users"}},
			},
		}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := hasValidFromClauseForFlattening(tc.sel)
			if got != tc.want {
				t.Errorf("hasValidFromClauseForFlattening: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMCDC_HasValidFromClauseForFlattening_JoinGuard(t *testing.T) {
	t.Parallel()

	oneTable := []parser.TableOrSubquery{{TableName: "users"}}
	joinClause := parser.JoinClause{Type: parser.JoinInner, Table: parser.TableOrSubquery{TableName: "orders"}}

	cases := []struct {
		name string
		sel  *parser.SelectStmt
		want bool
	}{
		// C=F: no joins -> true
		{"MCDC C=F: no joins", &parser.SelectStmt{
			From: &parser.FromClause{Tables: oneTable, Joins: nil},
		}, true},
		// C=T: has one join -> false
		{"MCDC C=T: has join", &parser.SelectStmt{
			From: &parser.FromClause{Tables: oneTable, Joins: []parser.JoinClause{joinClause}},
		}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := hasValidFromClauseForFlattening(tc.sel)
			if got != tc.want {
				t.Errorf("hasValidFromClauseForFlattening (JoinGuard): got %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go isSelectStar
//   Condition: len(stmt.Columns) == 1  (guard)
//     and then: col.Star && col.Table == ""
//   Sub-conditions for inner compound (col.Star && col.Table == ""):
//     A = col.Star
//     B = col.Table == ""
//   Coverage pairs:
//     A=T, B=T -> true  (bare star)
//     A=F, B=T -> false (A flips: non-star column)
//     A=T, B=F -> false (B flips: qualified star like "t.*")
// ---------------------------------------------------------------------------

func TestMCDC_IsSelectStar_StarAndNoTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		columns []parser.ResultColumn
		want    bool
	}{
		// A=T, B=T: bare SELECT *
		{"MCDC A=T B=T: bare star", []parser.ResultColumn{{Star: true, Table: ""}}, true},
		// A=F, B=T: single non-star column (A flips)
		{"MCDC A=F B=T: non-star col", []parser.ResultColumn{
			{Star: false, Table: "", Expr: &parser.IdentExpr{Name: "id"}},
		}, false},
		// A=T, B=F: qualified star like t.* (B flips)
		{"MCDC A=T B=F: qualified star t.*", []parser.ResultColumn{{Star: true, Table: "t"}}, false},
		// Outer guard: len != 1 -> false
		{"MCDC outer: zero cols", []parser.ResultColumn{}, false},
		{"MCDC outer: two cols", []parser.ResultColumn{
			{Star: true, Table: ""}, {Star: true, Table: ""},
		}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmt := &parser.SelectStmt{Columns: tc.columns}
			got := isSelectStar(stmt)
			if got != tc.want {
				t.Errorf("isSelectStar: got %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go mergeWhereClauses
//   Condition: outer.Where != nil
//   Sub-condition:
//     A = outer.Where != nil
//   Coverage pairs:
//     A=T -> both WHEREs combined with AND
//     A=F -> outer.Where set directly to viewSelect.Where
//
//   (The early return `if viewSelect.Where == nil` is a single condition;
//    tested here as setup but not the focus.)
// ---------------------------------------------------------------------------

func TestMCDC_MergeWhereClauses_OuterWhereNil(t *testing.T) {
	t.Parallel()

	viewWhere := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}

	cases := []struct {
		name       string
		outerWhere parser.Expression
		wantAnd    bool // true if result should be BinaryExpr(AND, ...)
	}{
		// A=T: outer has WHERE -> merged with AND
		{"MCDC A=T: outer has Where", &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}, true},
		// A=F: outer has no WHERE -> set directly
		{"MCDC A=F: outer nil Where", nil, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			outer := &parser.SelectStmt{Where: tc.outerWhere}
			viewSelect := &parser.SelectStmt{Where: viewWhere}
			mergeWhereClauses(outer, viewSelect)

			if tc.wantAnd {
				binExpr, ok := outer.Where.(*parser.BinaryExpr)
				if !ok {
					t.Errorf("mergeWhereClauses: expected BinaryExpr (AND), got %T", outer.Where)
				} else if binExpr.Op != parser.OpAnd {
					t.Errorf("mergeWhereClauses: expected AND op, got %v", binExpr.Op)
				}
			} else {
				if outer.Where != viewWhere {
					t.Errorf("mergeWhereClauses: expected outer.Where == viewWhere, got %v", outer.Where)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go preserveColumnAlias
//   Condition: origName != "" && col.Alias == ""
//   Sub-conditions:
//     A = origName != ""
//     B = col.Alias == ""
//   Coverage pairs:
//     A=T, B=T -> col.Alias set to origName
//     A=F, B=T -> col.Alias stays "" (A flips)
//     A=T, B=F -> col.Alias stays unchanged (B flips: already has alias)
// ---------------------------------------------------------------------------

func TestMCDC_PreserveColumnAlias_OrigNameAndEmptyAlias(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		origName   string
		existAlias string
		wantAlias  string
	}{
		// A=T, B=T: origName set, no existing alias -> alias gets set
		{"MCDC A=T B=T: set alias", "myCol", "", "myCol"},
		// A=F, B=T: origName empty -> alias stays "" (A flips)
		{"MCDC A=F B=T: empty origName", "", "", ""},
		// A=T, B=F: origName set but alias already exists -> alias unchanged (B flips)
		{"MCDC A=T B=F: alias already set", "myCol", "existingAlias", "existingAlias"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			col := &parser.ResultColumn{Alias: tc.existAlias}
			preserveColumnAlias(col, tc.origName)
			if col.Alias != tc.wantAlias {
				t.Errorf("preserveColumnAlias: got alias=%q, want %q", col.Alias, tc.wantAlias)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go buildSelectAliasMap
//   Condition: col.Alias == ""  (inside loop: skip if true)
//   Sub-condition:
//     A = col.Alias == ""
//   Coverage pairs:
//     A=T -> column skipped (not added to map)
//     A=F -> column added to map
// ---------------------------------------------------------------------------

func TestMCDC_BuildSelectAliasMap_EmptyAliasSkipped(t *testing.T) {
	t.Parallel()

	expr1 := &parser.IdentExpr{Name: "a"}
	expr2 := &parser.IdentExpr{Name: "b"}

	cases := []struct {
		name    string
		columns []parser.ResultColumn
		wantLen int
	}{
		// A=T: all columns have empty alias -> map is empty
		{"MCDC A=T: all empty alias", []parser.ResultColumn{
			{Alias: "", Expr: expr1},
			{Alias: "", Expr: expr2},
		}, 0},
		// A=F: all columns have alias -> all in map
		{"MCDC A=F: all have alias", []parser.ResultColumn{
			{Alias: "x", Expr: expr1},
			{Alias: "y", Expr: expr2},
		}, 2},
		// Mixed: one with alias, one without -> map has 1 entry
		{"MCDC mixed: one alias one not", []parser.ResultColumn{
			{Alias: "x", Expr: expr1},
			{Alias: "", Expr: expr2},
		}, 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			viewSelect := &parser.SelectStmt{Columns: tc.columns}
			got := buildSelectAliasMap(viewSelect)
			if len(got) != tc.wantLen {
				t.Errorf("buildSelectAliasMap: len(map)=%d, want %d", len(got), tc.wantLen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go explainSingleSelect
//   Condition: len(stmt.GroupBy) > 0 || detectAggregates(stmt)
//   Sub-conditions:
//     A = len(stmt.GroupBy) > 0
//     B = detectAggregates(stmt)
//   Coverage pairs:
//     A=T, *   -> "USE TEMP B-TREE FOR GROUP BY" emitted (A dominates OR)
//     A=F, B=T -> "USE TEMP B-TREE FOR GROUP BY" emitted (B flips outcome)
//     A=F, B=F -> node NOT emitted
// ---------------------------------------------------------------------------

func TestMCDC_ExplainSingleSelect_GroupByOrAggregate(t *testing.T) {
	t.Parallel()

	tableFrom := &parser.FromClause{
		Tables: []parser.TableOrSubquery{{TableName: "products"}},
	}
	groupByExpr := &parser.IdentExpr{Name: "category"}
	aggCol := parser.ResultColumn{
		Expr: &parser.FunctionExpr{Name: "COUNT", Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}},
	}
	plainCol := parser.ResultColumn{Expr: &parser.IdentExpr{Name: "id"}}

	cases := []struct {
		name        string
		stmt        *parser.SelectStmt
		wantGroupBy bool
	}{
		// A=T: has GROUP BY (A dominates OR) -> emit node
		{"MCDC A=T: explicit GroupBy", &parser.SelectStmt{
			Columns: []parser.ResultColumn{plainCol},
			From:    tableFrom,
			GroupBy: []parser.Expression{groupByExpr},
		}, true},
		// A=F, B=T: no GROUP BY but aggregate function (B flips) -> emit node
		{"MCDC A=F B=T: aggregate col", &parser.SelectStmt{
			Columns: []parser.ResultColumn{aggCol},
			From:    tableFrom,
		}, true},
		// A=F, B=F: no GROUP BY, no aggregate -> no node
		{"MCDC A=F B=F: plain select", &parser.SelectStmt{
			Columns: []parser.ResultColumn{plainCol},
			From:    tableFrom,
		}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan, err := GenerateExplain(tc.stmt)
			if err != nil {
				t.Fatalf("GenerateExplain: unexpected error: %v", err)
			}
			text := plan.FormatAsText()
			hasGroupByNode := strings.Contains(text, "GROUP BY")
			if hasGroupByNode != tc.wantGroupBy {
				t.Errorf("explainSingleSelect GroupBy node: got present=%v, want present=%v\nplan:\n%s",
					hasGroupByNode, tc.wantGroupBy, text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go extractMainTableName
//   Condition 1: stmt.From != nil && len(stmt.From.Tables) > 0
//   Sub-conditions:
//     A = stmt.From != nil
//     B = len(stmt.From.Tables) > 0
//   Coverage pairs:
//     A=T, B=T -> returns table name or "subquery"
//     A=F, B=T -> returns "" (A flips)
//     A=T, B=F -> returns "" (B flips: empty table list)
//
//   Condition 2: tableName == "" && stmt.From.Tables[0].Subquery != nil
//   Sub-conditions:
//     C = tableName == ""
//     D = stmt.From.Tables[0].Subquery != nil
//   Coverage pairs:
//     C=T, D=T -> returns "subquery"
//     C=F, D=T -> returns tableName (C flips: named table, not subquery path)
//     C=T, D=F -> returns "" (D flips: anon table without subquery)
// ---------------------------------------------------------------------------

func TestMCDC_ExtractMainTableName_FromNilAndEmpty(t *testing.T) {
	t.Parallel()

	subSelect := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
	}

	cases := []struct {
		name string
		stmt *parser.SelectStmt
		want string
	}{
		// A=F: nil From -> ""
		{"MCDC A=F: nil From", &parser.SelectStmt{From: nil}, ""},
		// A=T, B=F: From non-nil but empty tables -> ""
		{"MCDC A=T B=F: empty tables", &parser.SelectStmt{
			From: &parser.FromClause{Tables: []parser.TableOrSubquery{}},
		}, ""},
		// A=T, B=T (C=F): named table -> returns table name
		{"MCDC A=T B=T C=F: named table", &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "orders"}},
			},
		}, "orders"},
		// A=T, B=T, C=T, D=T: unnamed table with subquery -> "subquery"
		{"MCDC C=T D=T: subquery in FROM", &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "", Subquery: subSelect}},
			},
		}, "subquery"},
		// A=T, B=T, C=T, D=F: empty TableName with no subquery -> ""
		{"MCDC C=T D=F: empty name no subquery", &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: ""}},
			},
		}, ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractMainTableName(tc.stmt)
			if got != tc.want {
				t.Errorf("extractMainTableName: got %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go formatIndexScan
//   Condition 1: candidate.IsUnique && candidate.HasEquality
//   Sub-conditions:
//     A = candidate.IsUnique
//     B = candidate.HasEquality
//   Coverage pairs:
//     A=T, B=T -> "SEARCH TABLE ... USING INDEX ... (col=?)"
//     A=F, B=T -> falls through to IsCovering check or SEARCH/SCAN (A flips)
//     A=T, B=F -> falls through (B flips)
//
//   Condition 2: candidate.IsCovering
//   Sub-condition:
//     C = candidate.IsCovering
//   Coverage pairs:
//     C=T -> "SCAN TABLE ... USING COVERING INDEX ..."
//     C=F -> "SEARCH TABLE ..." or "SCAN TABLE ..."
// ---------------------------------------------------------------------------

func TestMCDC_FormatIndexScan_UniqueAndEquality(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		isUnique    bool
		hasEquality bool
		isCovering  bool
		wantContain string
	}{
		// A=T, B=T -> unique equality search
		{"MCDC A=T B=T: unique equality", true, true, false, "=?"},
		// A=F, B=T -> not unique equality (A flips)
		{"MCDC A=F B=T: not unique", false, true, false, "SEARCH"},
		// A=T, B=F -> unique but no equality: falls to bottom SCAN/SEARCH path
		// HasEquality=false so op="SCAN", result contains "SCAN"
		{"MCDC A=T B=F: unique no equality", true, false, false, "SCAN"},
		// C=T: covering index scan
		{"MCDC C=T: covering index", false, false, true, "COVERING INDEX"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cand := &IndexCandidate{
				IndexName:   "idx_test",
				TableName:   "users",
				Columns:     []string{"email"},
				IsUnique:    tc.isUnique,
				HasEquality: tc.hasEquality,
				IsCovering:  tc.isCovering,
			}
			got := formatIndexScan(cand)
			if !strings.Contains(got, tc.wantContain) {
				t.Errorf("formatIndexScan: got %q, want to contain %q", got, tc.wantContain)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go checkWhereSubqueries
//   Condition: where == nil || parent == nil
//   Sub-conditions:
//     A = where == nil
//     B = parent == nil
//   Coverage pairs:
//     A=T, *   -> returns immediately (A dominates OR)
//     A=F, B=T -> returns immediately (B flips outcome)
//     A=F, B=F -> walks expression for subqueries
// ---------------------------------------------------------------------------

func TestMCDC_CheckWhereSubqueries_NilGuard(t *testing.T) {
	t.Parallel()

	someWhere := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	plan := NewExplainPlan()
	parentNode := plan.AddNode(nil, "SCAN TABLE t")

	ctx := &explainCtx{plan: plan, schema: nil}

	cases := []struct {
		name          string
		parent        *ExplainNode
		where         parser.Expression
		wantExtraNode bool // extra child node added under parent?
	}{
		// A=T: nil where -> returns immediately, no extra node
		{"MCDC A=T: nil where", parentNode, nil, false},
		// A=F, B=T: non-nil where but nil parent -> returns immediately
		{"MCDC A=F B=T: nil parent", nil, someWhere, false},
		// A=F, B=F: both non-nil, non-subquery literal -> no subquery node emitted
		{"MCDC A=F B=F: non-subquery where", parentNode, someWhere, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Not parallel: shared plan state
			before := len(parentNode.Children)
			ctx.checkWhereSubqueries(tc.parent, tc.where)
			after := len(parentNode.Children)
			extraAdded := after > before
			if extraAdded != tc.wantExtraNode {
				t.Errorf("checkWhereSubqueries: extraNode=%v, want %v", extraAdded, tc.wantExtraNode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go isRowidLookup (schema branch)
//   Condition: colType == "INTEGER" || colType == "INT"
//   Sub-conditions:
//     A = colType == "INTEGER"
//     B = colType == "INT"
//   Coverage pairs:
//     A=T, *   -> true (A dominates OR)
//     A=F, B=T -> true (B flips outcome)
//     A=F, B=F -> false (not an integer primary key alias)
// ---------------------------------------------------------------------------

func TestMCDC_IsRowidLookup_IntegerOrInt(t *testing.T) {
	t.Parallel()

	makeSchema := func(colType string) *schema.Schema {
		s := schema.NewSchema()
		s.AddTableDirect(&schema.Table{
			Name: "items",
			Columns: []*schema.Column{
				{Name: "id", Type: colType, PrimaryKey: true},
			},
		})
		return s
	}

	makeWhere := func() parser.Expression {
		return &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "id"},
			Op:    parser.OpEq,
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
		}
	}

	cases := []struct {
		name      string
		colType   string
		wantRowid bool
	}{
		// A=T: colType == "INTEGER" -> rowid alias
		{"MCDC A=T: INTEGER type", "INTEGER", true},
		// A=F, B=T: colType == "INT" -> rowid alias (B flips)
		{"MCDC A=F B=T: INT type", "INT", true},
		// A=F, B=F: colType == "TEXT" -> not rowid alias
		{"MCDC A=F B=F: TEXT type", "TEXT", false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sch := makeSchema(tc.colType)
			ctx := &explainCtx{plan: NewExplainPlan(), schema: sch}
			_, gotRowid := ctx.isRowidLookup("items", makeWhere())
			if gotRowid != tc.wantRowid {
				t.Errorf("isRowidLookup(colType=%q): got %v, want %v", tc.colType, gotRowid, tc.wantRowid)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go isRowidAlias
//   Condition: lower == "rowid" || lower == "oid" || lower == "_rowid_"
//   Sub-conditions:
//     A = lower == "rowid"
//     B = lower == "oid"
//     C = lower == "_rowid_"
//   Coverage pairs (OR chain, 4 cases for N+1=4):
//     A=T, *        -> true
//     A=F, B=T, *  -> true (B flips)
//     A=F, B=F, C=T -> true (C flips)
//     A=F, B=F, C=F -> false (none match)
// ---------------------------------------------------------------------------

func TestMCDC_IsRowidAlias_AllVariants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		col  string
		want bool
	}{
		// A=T: "rowid"
		{"MCDC A=T: rowid", "rowid", true},
		// A=T: uppercase "ROWID"
		{"MCDC A=T: ROWID upper", "ROWID", true},
		// A=F, B=T: "oid" (B flips)
		{"MCDC A=F B=T: oid", "oid", true},
		// A=F, B=F, C=T: "_rowid_" (C flips)
		{"MCDC A=F B=F C=T: _rowid_", "_rowid_", true},
		// A=F, B=F, C=F: normal column name
		{"MCDC A=F B=F C=F: normal col", "user_id", false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isRowidAlias(tc.col)
			if got != tc.want {
				t.Errorf("isRowidAlias(%q): got %v, want %v", tc.col, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go emitJoinScanNode
//   Condition: join.Table.Subquery != nil
//   Sub-condition:
//     A = join.Table.Subquery != nil
//   Coverage pairs:
//     A=T -> emits SUBQUERY node
//     A=F -> emits scan node for named table
// ---------------------------------------------------------------------------

func TestMCDC_EmitJoinScanNode_SubqueryVsTable(t *testing.T) {
	t.Parallel()

	subSelect := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "inner_t"}},
		},
	}

	cases := []struct {
		name         string
		join         parser.JoinClause
		wantSubqNode bool
	}{
		// A=T: join table is a subquery -> SUBQUERY node emitted
		{"MCDC A=T: subquery join", parser.JoinClause{
			Type:  parser.JoinInner,
			Table: parser.TableOrSubquery{TableName: "", Subquery: subSelect},
		}, true},
		// A=F: join table is a named table -> scan node emitted
		{"MCDC A=F: named table join", parser.JoinClause{
			Type:  parser.JoinInner,
			Table: parser.TableOrSubquery{TableName: "orders"},
		}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan := NewExplainPlan()
			ctx := &explainCtx{plan: plan, schema: nil}
			parentNode := plan.AddNode(nil, "SCAN TABLE t")
			ctx.emitJoinScanNode(parentNode, tc.join)
			text := plan.FormatAsText()
			gotSubq := strings.Contains(text, "SUBQUERY")
			if gotSubq != tc.wantSubqNode {
				t.Errorf("emitJoinScanNode: gotSubq=%v, want %v\nplan:\n%s",
					gotSubq, tc.wantSubqNode, text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go isIndexableOp
//   Condition: op == parser.OpEq || op == parser.OpLt || op == parser.OpGt ||
//              op == parser.OpLe || op == parser.OpGe
//   This is a 5-way OR. MC/DC requires 6 cases:
//     A = op == OpEq,  B = op == OpLt,  C = op == OpGt,
//     D = op == OpLe,  E = op == OpGe
//   Coverage pairs:
//     A=T              -> true
//     A=F, B=T         -> true (B flips)
//     A=F, B=F, C=T   -> true (C flips)
//     A=F, B=F, C=F, D=T -> true (D flips)
//     A=F, B=F, C=F, D=F, E=T -> true (E flips)
//     all false        -> false
// ---------------------------------------------------------------------------

func TestMCDC_IsIndexableOp_AllOperators(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		op   parser.BinaryOp
		want bool
	}{
		{"MCDC A=T: OpEq", parser.OpEq, true},
		{"MCDC B=T: OpLt", parser.OpLt, true},
		{"MCDC C=T: OpGt", parser.OpGt, true},
		{"MCDC D=T: OpLe", parser.OpLe, true},
		{"MCDC E=T: OpGe", parser.OpGe, true},
		// A non-indexable op: OpMul
		{"MCDC all-F: OpMul", parser.OpMul, false},
		// Another non-indexable: OpOr
		{"MCDC all-F: OpOr", parser.OpOr, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isIndexableOp(tc.op)
			if got != tc.want {
				t.Errorf("isIndexableOp(%v): got %v, want %v", tc.op, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go EstimateIndexScan
//   Condition 1: isUnique && hasEquality
//   Sub-conditions:
//     A = isUnique
//     B = hasEquality
//   Coverage pairs:
//     A=T, B=T -> (1, 10.0) unique equality lookup
//     A=F, B=T -> equality range scan (A flips)
//     A=T, B=F -> unique but range (B flips)
//
//   Condition 2 (in else branch): hasEquality
//   Sub-condition:
//     B = hasEquality (also C here for clarity)
//   Coverage pairs:
//     B=T -> equality range path
//     B=F -> range scan path
//
//   Condition 3: isCovering  (cost *= 0.8)
//   Sub-condition:
//     D = isCovering
//   Coverage pairs:
//     D=T -> cost is reduced
//     D=F -> cost unchanged
// ---------------------------------------------------------------------------

func TestMCDC_EstimateIndexScan_UniqueEqualityAndCovering(t *testing.T) {
	t.Parallel()

	ce := NewCostEstimator()

	cases := []struct {
		name        string
		isUnique    bool
		isCovering  bool
		hasEquality bool
		wantRows    int64
		wantCost    float64
	}{
		// A=T, B=T: unique equality -> (1, 10.0)
		{"MCDC A=T B=T: unique equality", true, false, true, 1, 10.0},
		// A=F, B=T: non-unique equality (A flips)
		{"MCDC A=F B=T: non-unique equality", false, false, true, 1000, 500.0},
		// A=T, B=F: unique range (B flips)
		{"MCDC A=T B=F: unique range", true, false, false, 10000, 7000.0},
		// D=T: covering index reduces cost
		{"MCDC D=T: covering non-unique eq", false, true, true, 1000, 400.0},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rows, cost := ce.EstimateIndexScan("idx", tc.isUnique, tc.isCovering, tc.hasEquality)
			if rows != tc.wantRows {
				t.Errorf("EstimateIndexScan rows: got %d, want %d", rows, tc.wantRows)
			}
			if cost != tc.wantCost {
				t.Errorf("EstimateIndexScan cost: got %v, want %v", cost, tc.wantCost)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go checkIfRecursive
//   Condition: !ctx.IsRecursive  (early return)
//   Sub-condition:
//     A = ctx.IsRecursive  (negated: A=F means !IsRecursive=T)
//   Coverage pairs:
//     A=F (!IsRecursive=T) -> returns false immediately
//     A=T (!IsRecursive=F) -> proceeds to check self-reference
// ---------------------------------------------------------------------------

func TestMCDC_CTECheckIfRecursive_IsRecursiveGuard(t *testing.T) {
	t.Parallel()

	cteSQL := "WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r"
	nonRecSQL := "WITH r AS (SELECT 1) SELECT * FROM r"

	cases := []struct {
		name          string
		sql           string
		cteName       string
		wantRecursive bool
	}{
		// A=T: IsRecursive=true WITH RECURSIVE + self-reference -> CTE is recursive
		{"MCDC A=T: recursive self-ref", cteSQL, "r", true},
		// A=F: IsRecursive=false -> no CTE can be recursive
		{"MCDC A=F: non-recursive ctx", nonRecSQL, "r", false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := parser.NewParser(tc.sql)
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			sel := stmts[0].(*parser.SelectStmt)
			ctx, err := NewCTEContext(sel.With)
			if err != nil {
				t.Fatalf("NewCTEContext: %v", err)
			}
			def, ok := ctx.CTEs[tc.cteName]
			if !ok {
				t.Fatalf("CTE %q not found", tc.cteName)
			}
			if def.IsRecursive != tc.wantRecursive {
				t.Errorf("CTE.IsRecursive: got %v, want %v", def.IsRecursive, tc.wantRecursive)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go calculateLevel
//   Condition: def.Level > 0  (early return: already calculated)
//   Sub-condition:
//     A = def.Level > 0
//   Coverage pairs:
//     A=T -> returns nil immediately (level already set)
//     A=F -> proceeds to compute level
// ---------------------------------------------------------------------------

func TestMCDC_CTECalculateLevel_AlreadyCalculated(t *testing.T) {
	t.Parallel()

	// Two CTEs: a (no deps) and b (depends on a).
	// After buildDependencyOrder, both have Level > 0.
	sql := "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b"

	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*parser.SelectStmt)
	ctx, err := NewCTEContext(sel.With)
	if err != nil {
		t.Fatalf("NewCTEContext: %v", err)
	}

	// After construction, levels should be set (> 0 or == 1 for the base).
	// Re-calling calculateLevel should be a no-op (A=T path).
	for name := range ctx.CTEs {
		levelBefore := ctx.CTEs[name].Level
		err2 := ctx.calculateLevel(name, make(map[string]bool))
		if err2 != nil {
			t.Errorf("calculateLevel(%q) unexpected error: %v", name, err2)
		}
		levelAfter := ctx.CTEs[name].Level
		if levelAfter != levelBefore {
			t.Errorf("calculateLevel(%q): level changed from %d to %d (should be no-op)",
				name, levelBefore, levelAfter)
		}
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go hasUnionStructure
//   Condition: compound.Op == parser.CompoundUnion || compound.Op == parser.CompoundUnionAll
//   Sub-conditions:
//     A = compound.Op == CompoundUnion
//     B = compound.Op == CompoundUnionAll
//   Coverage pairs:
//     A=T, *  -> true (A dominates OR)
//     A=F, B=T -> true (B flips outcome)
//     A=F, B=F -> false (neither UNION nor UNION ALL)
// ---------------------------------------------------------------------------

func TestMCDC_HasUnionStructure_UnionOrUnionAll(t *testing.T) {
	t.Parallel()

	left := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}}},
	}
	right := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}}},
	}

	makeSelect := func(op parser.CompoundOp) *parser.SelectStmt {
		return &parser.SelectStmt{
			Columns:  left.Columns,
			Compound: &parser.CompoundSelect{Op: op, Left: left, Right: right},
		}
	}

	ctx := &CTEContext{}

	cases := []struct {
		name string
		sel  *parser.SelectStmt
		want bool
	}{
		// A=T: UNION
		{"MCDC A=T: UNION", makeSelect(parser.CompoundUnion), true},
		// A=F, B=T: UNION ALL (B flips)
		{"MCDC A=F B=T: UNION ALL", makeSelect(parser.CompoundUnionAll), true},
		// A=F, B=F: EXCEPT -> false
		{"MCDC A=F B=F: EXCEPT", makeSelect(parser.CompoundExcept), false},
		// A=F, B=F: INTERSECT -> false
		{"MCDC A=F B=F: INTERSECT", makeSelect(parser.CompoundIntersect), false},
		// Nil compound -> false
		{"MCDC nil compound", &parser.SelectStmt{}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ctx.hasUnionStructure(tc.sel)
			if got != tc.want {
				t.Errorf("hasUnionStructure: got %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go validateSelfReference
//   Condition: isRecursive (negated guard: if isRecursive -> ok)
//   Sub-condition:
//     A = isRecursive
//   Coverage pairs:
//     A=T -> no error (self-reference is fine in recursive CTE)
//     A=F -> returns error
// ---------------------------------------------------------------------------

func TestMCDC_CTEValidateSelfReference_IsRecursive(t *testing.T) {
	t.Parallel()

	ctx := &CTEContext{}

	cases := []struct {
		name        string
		isRecursive bool
		wantError   bool
	}{
		// A=T: recursive -> no error
		{"MCDC A=T: recursive ok", true, false},
		// A=F: non-recursive -> error
		{"MCDC A=F: non-recursive error", false, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ctx.validateSelfReference("my_cte", tc.isRecursive)
			gotError := err != nil
			if gotError != tc.wantError {
				t.Errorf("validateSelfReference(isRecursive=%v): gotError=%v, wantError=%v err=%v",
					tc.isRecursive, gotError, tc.wantError, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go calculateMaxDependencyLevel
//   Condition: dep == name && !def.IsRecursive  (inside dependency loop)
//   Sub-conditions:
//     A = dep == name
//     B = def.IsRecursive  (B=F means non-recursive; B=T means recursive)
//   Coverage pairs:
//     A=T, B=F -> error: "non-recursive CTE cannot reference itself"
//     A=T, B=T -> continue (self-ref allowed in recursive CTE)
//     A=F, *   -> normal dependency lookup
//
//   We exercise this via checkCircularDependency (called by ValidateCTEs).
//   At NewCTEContext time the dependency is only detected when the CTE name
//   appears as a known CTE at parse time (which happens for RECURSIVE CTEs).
// ---------------------------------------------------------------------------

func TestMCDC_CTECalculateMaxDependencyLevel_SelfRefGuard(t *testing.T) {
	t.Parallel()

	// Recursive self-referencing CTE: the recursive member references itself,
	// the dep == name path is exercised with IsRecursive=true -> continue.
	recSelfRef := "WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT n+1 FROM a WHERE n < 3) SELECT * FROM a"

	// Non-recursive CTE with one level of dependency (dep != name path):
	normalDep := "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b"

	cases := []struct {
		name      string
		sql       string
		wantError bool
	}{
		// A=T, B=T (IsRecursive=true): recursive self-ref -> no error (continue path)
		{"MCDC A=T B=T: recursive self-ref ok", recSelfRef, false},
		// A=F: normal inter-CTE dependency -> no error
		{"MCDC A=F: normal dependency", normalDep, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := parser.NewParser(tc.sql)
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			sel := stmts[0].(*parser.SelectStmt)
			_, ctxErr := NewCTEContext(sel.With)
			gotError := ctxErr != nil
			if gotError != tc.wantError {
				t.Errorf("NewCTEContext (self-ref): gotError=%v, wantError=%v, err=%v",
					gotError, tc.wantError, ctxErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go handleInExpr
//   Condition: e.Select != nil
//   Sub-condition:
//     A = e.Select != nil
//   Coverage pairs:
//     A=T -> collectCTEReferences called on e.Select
//     A=F -> collectCTEReferences NOT called (only e.Expr and e.Values processed)
// ---------------------------------------------------------------------------

func TestMCDC_CTEHandleInExpr_SelectNilGuard(t *testing.T) {
	t.Parallel()

	// CTE "names" referenced in an IN subquery
	subSelect := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Expr: &parser.IdentExpr{Name: "name"}}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "names"}},
		},
	}

	inExprWithSelect := &parser.InExpr{
		Expr:   &parser.IdentExpr{Name: "x"},
		Select: subSelect,
		Values: nil,
	}
	inExprWithoutSelect := &parser.InExpr{
		Expr:   &parser.IdentExpr{Name: "x"},
		Select: nil,
		Values: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}

	// Set up a CTEContext that knows about "names" as a CTE
	namesSelect := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Expr: &parser.IdentExpr{Name: "n"}}},
		From:    &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "raw"}}},
	}
	ctx := &CTEContext{
		CTEs: map[string]*CTEDefinition{
			"names": {
				Name:   "names",
				Select: namesSelect,
			},
		},
		IsRecursive:      false,
		MaterializedCTEs: make(map[string]*MaterializedCTE),
	}

	cases := []struct {
		name      string
		inExpr    *parser.InExpr
		wantDepOn string // expect "names" in deps if A=T
		wantFound bool
	}{
		// A=T: IN with SELECT referencing CTE "names" -> dependency detected
		{"MCDC A=T: IN subquery references CTE", inExprWithSelect, "names", true},
		// A=F: IN with values list only -> "names" NOT in deps
		{"MCDC A=F: IN values list no select", inExprWithoutSelect, "names", false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := make(map[string]bool)
			ctx.handleInExpr(tc.inExpr, deps)
			found := deps[tc.wantDepOn]
			if found != tc.wantFound {
				t.Errorf("handleInExpr: dep[%q]=%v, want %v", tc.wantDepOn, found, tc.wantFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cte.go inferColumnName
//   Condition: col.Alias != ""  (first guard)
//   and: col.Star
//   Sub-conditions:
//     A = col.Alias != ""
//     B = col.Star
//   Coverage pairs for combined logic:
//     A=T       -> returns alias immediately
//     A=F, B=T  -> returns "*"
//     A=F, B=F  -> tries to extract from IdentExpr or returns "column_N"
// ---------------------------------------------------------------------------

func TestMCDC_CTEInferColumnName_AliasAndStar(t *testing.T) {
	t.Parallel()

	ctx := &CTEContext{}

	cases := []struct {
		name  string
		col   parser.ResultColumn
		index int
		want  string
	}{
		// A=T: alias set -> returns alias
		{"MCDC A=T: has alias", parser.ResultColumn{Alias: "myAlias", Star: false}, 0, "myAlias"},
		// A=F, B=T: no alias, star -> returns "*"
		{"MCDC A=F B=T: star col", parser.ResultColumn{Alias: "", Star: true}, 0, "*"},
		// A=F, B=F: no alias, no star, IdentExpr -> returns ident name
		{"MCDC A=F B=F: ident expr", parser.ResultColumn{
			Alias: "", Star: false, Expr: &parser.IdentExpr{Name: "price"},
		}, 0, "price"},
		// A=F, B=F: no alias, no star, non-ident -> returns "column_N"
		{"MCDC A=F B=F: literal expr", parser.ResultColumn{
			Alias: "", Star: false,
			Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
		}, 3, "column_3"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ctx.inferColumnName(tc.col, tc.index)
			if got != tc.want {
				t.Errorf("inferColumnName: got %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: subquery.go shouldMaterializeSubquery
//   Condition: !info.IsCorrelated  (early return false)
//   Sub-condition:
//     A = info.IsCorrelated
//   Coverage pairs:
//     A=F (!IsCorrelated=T) -> returns false immediately
//     A=T (!IsCorrelated=F) -> proceeds to cost comparison
// ---------------------------------------------------------------------------

func TestMCDC_ShouldMaterializeSubquery_CorrelatedGuard3(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()
	opt := NewSubqueryOptimizer(cm)

	cases := []struct {
		name         string
		isCorrelated bool
		wantMat      bool
	}{
		// A=F: uncorrelated -> false immediately
		{"MCDC A=F: uncorrelated no materialize", false, false},
		// A=T: correlated -> cost comparison (small estimated rows -> materialize)
		{"MCDC A=T: correlated small cost", true, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			info := &SubqueryInfo{
				Type:           SubqueryScalar,
				IsCorrelated:   tc.isCorrelated,
				EstimatedRows:  NewLogEst(10),
				ExecutionCount: NewLogEst(1000), // high execution count -> materialize
			}
			got := opt.shouldMaterializeSubquery(info)
			if got != tc.wantMat {
				t.Errorf("shouldMaterializeSubquery(isCorrelated=%v): got %v, want %v",
					tc.isCorrelated, got, tc.wantMat)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: subquery.go SubqueryExpr.UsedTables
//   Condition: e.OuterColumn != nil  (checked separately from e.Query)
//   Sub-condition:
//     A = e.OuterColumn != nil
//   Coverage pairs:
//     A=T -> OuterColumn.UsedTables() ORed into mask
//     A=F -> only Query.UsedTables() contributes
// ---------------------------------------------------------------------------

func TestMCDC_SubqueryExprUsedTables_OuterColumnNil(t *testing.T) {
	t.Parallel()

	// A ColumnExpr that contributes table bit 1
	colExpr := &ColumnExpr{Cursor: 1, Column: "id"}

	// A query expression that contributes table bit 0
	var queryBit Bitmask
	queryBit.Set(0)
	queryExpr := &ColumnExpr{Cursor: 0, Column: "name"}

	cases := []struct {
		name        string
		query       Expr
		outerColumn *ColumnExpr
		wantBit1    bool // bit 1 set (outerColumn contributes)
	}{
		// A=F: no outer column -> only query bit
		{"MCDC A=F: no outerColumn", queryExpr, nil, false},
		// A=T: outer column with bit 1 -> bit 1 set
		{"MCDC A=T: with outerColumn", queryExpr, colExpr, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			se := &SubqueryExpr{
				Query:       tc.query,
				Type:        SubqueryScalar,
				OuterColumn: tc.outerColumn,
			}
			mask := se.UsedTables()
			// Check if bit 1 is set (Cursor=1 from colExpr)
			var bit1mask Bitmask
			bit1mask.Set(1)
			gotBit1 := mask.Overlaps(bit1mask)
			if gotBit1 != tc.wantBit1 {
				t.Errorf("SubqueryExpr.UsedTables: bit1=%v, want %v (mask=%v)",
					gotBit1, tc.wantBit1, mask)
			}
		})
	}
}
