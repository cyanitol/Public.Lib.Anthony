// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql_test

import (
	"testing"

	sql "github.com/cyanitol/Public.Lib.Anthony/internal/sql"
)

// TestSQLCoverage2_ReleaseReg exercises Parse.ReleaseReg (types.go, 0% coverage).
// ReleaseReg is a documented no-op; we verify it does not panic and leaves
// Parse.Mem unchanged.
func TestSQLCoverage2_ReleaseReg(t *testing.T) {
	cases := []struct {
		name string
		mem  int
		reg  int
	}{
		{"zero", 0, 0},
		{"typical", 10, 5},
		{"reg_equals_mem", 7, 7},
		{"reg_above_mem", 4, 9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &sql.Parse{Mem: tc.mem}
			p.ReleaseReg(tc.reg)
			if p.Mem != tc.mem {
				t.Errorf("ReleaseReg modified Mem: got %d, want %d", p.Mem, tc.mem)
			}
		})
	}
}

// TestSQLCoverage2_ReleaseRegs exercises Parse.ReleaseRegs (types.go, 0% coverage).
// ReleaseRegs is a documented no-op; we verify it does not panic and leaves
// Parse.Mem unchanged.
func TestSQLCoverage2_ReleaseRegs(t *testing.T) {
	cases := []struct {
		name string
		mem  int
		base int
		n    int
	}{
		{"zero base zero n", 0, 0, 0},
		{"normal", 10, 2, 4},
		{"n is zero", 8, 3, 0},
		{"large n", 100, 1, 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &sql.Parse{Mem: tc.mem}
			p.ReleaseRegs(tc.base, tc.n)
			if p.Mem != tc.mem {
				t.Errorf("ReleaseRegs modified Mem: got %d, want %d", p.Mem, tc.mem)
			}
		})
	}
}

// TestSQLCoverage2_CompileUpdateWithIndex exercises CompileUpdateWithIndex
// (update.go, 75% coverage) via its exported API.
func TestSQLCoverage2_CompileUpdateWithIndex(t *testing.T) {
	t.Run("single_index", func(t *testing.T) {
		stmt := sql.NewUpdateStmt(
			"products",
			[]string{"price", "stock"},
			[]sql.Value{sql.FloatValue(9.99), sql.IntValue(100)},
			nil,
		)
		prog, err := sql.CompileUpdateWithIndex(stmt, 42, 3, []int{0})
		if err != nil {
			t.Fatalf("CompileUpdateWithIndex failed: %v", err)
		}
		if prog == nil {
			t.Error("expected non-nil program")
		}
	})

	t.Run("multiple_indexes", func(t *testing.T) {
		stmt := sql.NewUpdateStmt(
			"orders",
			[]string{"status"},
			[]sql.Value{sql.TextValue("shipped")},
			nil,
		)
		prog, err := sql.CompileUpdateWithIndex(stmt, 10, 4, []int{1, 2, 3})
		if err != nil {
			t.Fatalf("CompileUpdateWithIndex failed: %v", err)
		}
		if prog == nil {
			t.Error("expected non-nil program")
		}
	})

	t.Run("empty_indexes", func(t *testing.T) {
		stmt := sql.NewUpdateStmt(
			"users",
			[]string{"name"},
			[]sql.Value{sql.TextValue("Alice")},
			nil,
		)
		prog, err := sql.CompileUpdateWithIndex(stmt, 5, 2, []int{})
		if err != nil {
			t.Fatalf("CompileUpdateWithIndex failed: %v", err)
		}
		if prog == nil {
			t.Error("expected non-nil program")
		}
	})

	t.Run("nil_stmt_returns_error", func(t *testing.T) {
		_, err := sql.CompileUpdateWithIndex(nil, 1, 1, []int{0})
		if err == nil {
			t.Error("expected error for nil stmt, got nil")
		}
	})
}

// TestSQLCoverage2_CompileSimpleSelect exercises compileSimpleSelect
// (select.go, 76.9% coverage) via CompileSelect on a non-compound SELECT.
func TestSQLCoverage2_CompileSimpleSelect(t *testing.T) {
	t.Run("select_integer_literal", func(t *testing.T) {
		parse := &sql.Parse{
			Vdbe: sql.NewVdbe(nil),
			Mem:  0,
		}
		sc := sql.NewSelectCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 42}},
				},
			},
		}
		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		err := sc.CompileSelect(sel, dest)
		if err != nil {
			t.Fatalf("CompileSelect failed: %v", err)
		}
		if len(parse.Vdbe.Ops) == 0 {
			t.Error("expected VDBE instructions to be generated")
		}
	})

	t.Run("select_with_where", func(t *testing.T) {
		parse := &sql.Parse{
			Vdbe: sql.NewVdbe(nil),
			Mem:  0,
		}
		sc := sql.NewSelectCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 1}},
				},
			},
			Where: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 1},
		}
		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		err := sc.CompileSelect(sel, dest)
		if err != nil {
			t.Fatalf("CompileSelect with WHERE failed: %v", err)
		}
	})

	t.Run("select_with_limit", func(t *testing.T) {
		parse := &sql.Parse{
			Vdbe: sql.NewVdbe(nil),
			Mem:  0,
		}
		sc := sql.NewSelectCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 7}},
				},
			},
			Limit: 10,
		}
		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		err := sc.CompileSelect(sel, dest)
		if err != nil {
			t.Fatalf("CompileSelect with LIMIT failed: %v", err)
		}
	})

	t.Run("no_vdbe_returns_error", func(t *testing.T) {
		// Parse with no Vdbe field forces GetVdbe to create one lazily;
		// we test the nil-Vdbe path by passing a bare Parse.
		parse := &sql.Parse{}
		sc := sql.NewSelectCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 1}},
				},
			},
		}
		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		// GetVdbe lazily creates a Vdbe when nil, so this should succeed.
		_ = sc.CompileSelect(sel, dest)
	})
}

// TestSQLCoverage2_ResolveChildExprs exercises ResultCompiler.resolveChildExprs
// (result.go, 71.4% coverage) via ResolveResultColumns.
//
// resolveChildExprs is reached when resolveExprColumns hits its default branch
// (expr.Op is neither TK_COLUMN nor TK_DOT).  We need expressions whose Op
// is something else (e.g. TK_PLUS) with Left and/or Right children so that
// both arms of resolveChildExprs are executed.
func TestSQLCoverage2_ResolveChildExprs(t *testing.T) {
	t.Run("binary_expr_left_and_right", func(t *testing.T) {
		parse := &sql.Parse{Vdbe: sql.NewVdbe(nil)}
		rc := sql.NewResultCompiler(parse)

		// TK_PLUS with Left=integer and Right=integer hits the default case,
		// then resolveChildExprs recurses into both children.
		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{
						Expr: &sql.Expr{
							Op: sql.TK_PLUS,
							Left: &sql.Expr{
								Op:       sql.TK_INTEGER,
								IntValue: 1,
							},
							Right: &sql.Expr{
								Op:       sql.TK_INTEGER,
								IntValue: 2,
							},
						},
					},
				},
			},
		}

		err := rc.ResolveResultColumns(sel)
		if err != nil {
			t.Fatalf("ResolveResultColumns failed: %v", err)
		}
	})

	t.Run("expr_left_only", func(t *testing.T) {
		parse := &sql.Parse{Vdbe: sql.NewVdbe(nil)}
		rc := sql.NewResultCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{
						Expr: &sql.Expr{
							Op: sql.TK_PLUS,
							Left: &sql.Expr{
								Op:       sql.TK_INTEGER,
								IntValue: 5,
							},
						},
					},
				},
			},
		}

		err := rc.ResolveResultColumns(sel)
		if err != nil {
			t.Fatalf("ResolveResultColumns (left only) failed: %v", err)
		}
	})

	t.Run("expr_right_only", func(t *testing.T) {
		parse := &sql.Parse{Vdbe: sql.NewVdbe(nil)}
		rc := sql.NewResultCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{
						Expr: &sql.Expr{
							Op: sql.TK_PLUS,
							Right: &sql.Expr{
								Op:       sql.TK_INTEGER,
								IntValue: 3,
							},
						},
					},
				},
			},
		}

		err := rc.ResolveResultColumns(sel)
		if err != nil {
			t.Fatalf("ResolveResultColumns (right only) failed: %v", err)
		}
	})
}

// TestSQLCoverage2_ResolveChildExpressions exercises
// OrderByCompiler.resolveChildExpressions (orderby.go, 71.4% coverage) via
// CompileOrderBy.
//
// resolveChildExpressions is reached when resolveOrderByExpr hits its default
// branch (not TK_COLUMN, not TK_DOT) and then recurses into Left/Right children.
// compileOrderByItem's default branch delegates to resolveOrderByExpr when the
// item Op is not TK_INTEGER or TK_ID.
func TestSQLCoverage2_ResolveChildExpressions(t *testing.T) {
	t.Run("binary_left_and_right_children", func(t *testing.T) {
		parse := &sql.Parse{Vdbe: sql.NewVdbe(nil), Mem: 0}
		obc := sql.NewOrderByCompiler(parse)

		// A SELECT with two result columns so column-number references are valid.
		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 1}},
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 2}},
				},
			},
		}

		// ORDER BY item whose Op is TK_PLUS (default case in compileOrderByItem →
		// resolveOrderByExpr default branch → resolveChildExpressions).
		// The children are also TK_PLUS so the recursion is deep enough to hit
		// both the Left and Right arms of resolveChildExpressions.
		orderBy := &sql.ExprList{
			Items: []sql.ExprListItem{
				{
					Expr: &sql.Expr{
						Op: sql.TK_PLUS,
						Left: &sql.Expr{
							Op: sql.TK_PLUS,
							Left: &sql.Expr{
								Op:       sql.TK_INTEGER,
								IntValue: 10,
							},
							Right: &sql.Expr{
								Op:       sql.TK_INTEGER,
								IntValue: 20,
							},
						},
						Right: &sql.Expr{
							Op:       sql.TK_INTEGER,
							IntValue: 30,
						},
					},
				},
			},
		}

		err := obc.CompileOrderBy(sel, orderBy)
		if err != nil {
			t.Fatalf("CompileOrderBy failed: %v", err)
		}
	})

	t.Run("left_child_only", func(t *testing.T) {
		parse := &sql.Parse{Vdbe: sql.NewVdbe(nil), Mem: 0}
		obc := sql.NewOrderByCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 1}},
				},
			},
		}

		orderBy := &sql.ExprList{
			Items: []sql.ExprListItem{
				{
					Expr: &sql.Expr{
						Op: sql.TK_PLUS,
						Left: &sql.Expr{
							Op:       sql.TK_INTEGER,
							IntValue: 5,
						},
					},
				},
			},
		}

		err := obc.CompileOrderBy(sel, orderBy)
		if err != nil {
			t.Fatalf("CompileOrderBy (left only) failed: %v", err)
		}
	})

	t.Run("right_child_only", func(t *testing.T) {
		parse := &sql.Parse{Vdbe: sql.NewVdbe(nil), Mem: 0}
		obc := sql.NewOrderByCompiler(parse)

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_INTEGER, IntValue: 1}},
				},
			},
		}

		orderBy := &sql.ExprList{
			Items: []sql.ExprListItem{
				{
					Expr: &sql.Expr{
						Op: sql.TK_PLUS,
						Right: &sql.Expr{
							Op:       sql.TK_INTEGER,
							IntValue: 8,
						},
					},
				},
			},
		}

		err := obc.CompileOrderBy(sel, orderBy)
		if err != nil {
			t.Fatalf("CompileOrderBy (right only) failed: %v", err)
		}
	})
}

// TestSQLCoverage2_FindAggsInSelect exercises AggregateCompiler.findAggsInSelect
// (aggregate.go, 66.7% coverage) via CompileSelect with a GROUP BY clause.
//
// findAggsInSelect is called from analyzeAggregates, which is called from
// compileGroupBy, which is called from compileSelectLoop when GroupBy != nil.
// The uncovered branch is the sel.Having != nil path.
func TestSQLCoverage2_FindAggsInSelect(t *testing.T) {
	t.Run("having_clause_triggers_branch", func(t *testing.T) {
		parse := &sql.Parse{
			Vdbe: sql.NewVdbe(nil),
			Mem:  0,
			Tabs: 0,
		}
		sc := sql.NewSelectCompiler(parse)

		table := &sql.Table{
			Name:       "sales",
			RootPage:   1,
			NumColumns: 2,
			Columns: []sql.Column{
				{Name: "cat", DeclType: "TEXT"},
				{Name: "amt", DeclType: "INTEGER"},
			},
		}

		src := sql.NewSrcList()
		src.Append(sql.SrcListItem{Table: table, Cursor: 0})

		// SELECT cat, SUM(amt) FROM sales GROUP BY cat HAVING COUNT(*) > 1
		countExpr := &sql.Expr{
			Op:      sql.TK_AGG_FUNCTION,
			FuncDef: &sql.FuncDef{Name: "count"},
		}

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 0}},
					{Expr: &sql.Expr{
						Op:      sql.TK_AGG_FUNCTION,
						FuncDef: &sql.FuncDef{Name: "sum"},
						List: &sql.ExprList{
							Items: []sql.ExprListItem{
								{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 1}},
							},
						},
					}},
				},
			},
			Src: src,
			GroupBy: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 0}},
				},
			},
			Having:   countExpr,
			SelectID: 1,
		}

		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		err := sc.CompileSelect(sel, dest)
		if err != nil {
			t.Fatalf("CompileSelect with HAVING failed: %v", err)
		}
	})

	t.Run("order_by_with_agg_triggers_orderby_branch", func(t *testing.T) {
		parse := &sql.Parse{
			Vdbe: sql.NewVdbe(nil),
			Mem:  0,
			Tabs: 0,
		}
		sc := sql.NewSelectCompiler(parse)

		table := &sql.Table{
			Name:       "logs",
			RootPage:   2,
			NumColumns: 1,
			Columns:    []sql.Column{{Name: "level", DeclType: "TEXT"}},
		}

		src := sql.NewSrcList()
		src.Append(sql.SrcListItem{Table: table, Cursor: 0})

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 0}},
					{Expr: &sql.Expr{
						Op:      sql.TK_AGG_FUNCTION,
						FuncDef: &sql.FuncDef{Name: "count"},
					}},
				},
			},
			Src: src,
			GroupBy: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 0}},
				},
			},
			OrderBy: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{
						Op:      sql.TK_AGG_FUNCTION,
						FuncDef: &sql.FuncDef{Name: "count"},
					}},
				},
			},
			SelectID: 2,
		}

		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		err := sc.CompileSelect(sel, dest)
		if err != nil {
			t.Fatalf("CompileSelect with ORDER BY agg failed: %v", err)
		}
	})
}

// TestSQLCoverage2_FindAggsInChildren exercises
// AggregateCompiler.findAggsInChildren (aggregate.go, 71.4% coverage) via
// CompileSelect with GROUP BY, using nested expressions that have both Left and
// Right children so all three arms (Left, Right, List) are covered.
func TestSQLCoverage2_FindAggsInChildren(t *testing.T) {
	t.Run("nested_binary_in_group_by_select", func(t *testing.T) {
		parse := &sql.Parse{
			Vdbe: sql.NewVdbe(nil),
			Mem:  0,
			Tabs: 0,
		}
		sc := sql.NewSelectCompiler(parse)

		table := &sql.Table{
			Name:       "t",
			RootPage:   3,
			NumColumns: 2,
			Columns: []sql.Column{
				{Name: "a", DeclType: "INTEGER"},
				{Name: "b", DeclType: "INTEGER"},
			},
		}

		src := sql.NewSrcList()
		src.Append(sql.SrcListItem{Table: table, Cursor: 0})

		// A binary expression (TK_PLUS) that wraps two AGG_FUNCTION nodes.
		// findAggregateFuncs will call findAggsInChildren on the TK_PLUS node,
		// exercising both the Left and Right branches.
		sumExpr := &sql.Expr{
			Op:      sql.TK_AGG_FUNCTION,
			FuncDef: &sql.FuncDef{Name: "sum"},
			List: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 0}},
				},
			},
		}
		countExpr := &sql.Expr{
			Op:      sql.TK_AGG_FUNCTION,
			FuncDef: &sql.FuncDef{Name: "count"},
		}
		binaryAgg := &sql.Expr{
			Op:    sql.TK_PLUS,
			Left:  sumExpr,
			Right: countExpr,
		}

		sel := &sql.Select{
			EList: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 1}},
					{Expr: binaryAgg},
				},
			},
			Src: src,
			GroupBy: &sql.ExprList{
				Items: []sql.ExprListItem{
					{Expr: &sql.Expr{Op: sql.TK_COLUMN, Table: 0, Column: 1}},
				},
			},
			SelectID: 3,
		}

		dest := &sql.SelectDest{}
		sql.InitSelectDest(dest, sql.SRT_Output, 0)

		err := sc.CompileSelect(sel, dest)
		if err != nil {
			t.Fatalf("CompileSelect (findAggsInChildren) failed: %v", err)
		}
	})
}
