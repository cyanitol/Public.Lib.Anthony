// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// --- aggregate.go: findAggsInSelect with OrderBy (66.7%) ---

// TestFindAggsInSelect3_WithOrderBy covers the ORDER BY aggregate search path.
func TestFindAggsInSelect3_WithOrderBy(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: &FuncDef{Name: "max"},
					},
				},
			},
		},
	}

	err := ac.findAggsInSelect(sel, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInSelect with OrderBy failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func from OrderBy, got %d", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInExprList3_NilList verifies nil list is handled gracefully.
func TestFindAggsInExprList3_NilList(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	err := ac.findAggsInExprList(nil, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInExprList nil list failed: %v", err)
	}
}

// TestFindAggsInChildren3_OnlyList exercises ExprList-only child search.
func TestFindAggsInChildren3_OnlyList(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	// Expr with only a List (no Left/Right)
	expr := &Expr{
		Op: TK_FUNCTION,
		List: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "avg"},
				}},
			},
		},
	}

	err := ac.findAggsInChildren(expr, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInChildren ExprList failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func, got %d", len(aggInfo.AggFuncs))
	}
}

// --- select.go: compileSimpleSelect (76.5%) ---

// TestCompileSimpleSelect3_WithGroupBy exercises GROUP BY path.
func TestCompileSimpleSelect3_WithGroupBy(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0, Tabs: 0}
	sc := NewSelectCompiler(parse)

	table := &Table{
		Name:       "orders",
		NumColumns: 2,
		RootPage:   1,
		Columns: []Column{
			{Name: "status", DeclType: "TEXT"},
			{Name: "amount", DeclType: "REAL"},
		},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
				{Expr: &Expr{Op: TK_AGG_FUNCTION, FuncDef: &FuncDef{Name: "count"}}},
			},
		},
		Src: srcList,
		GroupBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
	}

	dest := &SelectDest{Dest: SRT_Output}
	err := sc.CompileSelect(sel, dest)
	if err != nil {
		t.Fatalf("compileSimpleSelect with GROUP BY failed: %v", err)
	}
}

// TestCompileSimpleSelect3_WithWhere exercises WHERE clause path.
func TestCompileSimpleSelect3_WithWhere(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0, Tabs: 0}
	sc := NewSelectCompiler(parse)

	table := &Table{
		Name:       "items",
		NumColumns: 1,
		RootPage:   2,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
		Src: srcList,
		Where: &Expr{
			Op:    TK_EQ,
			Left:  &Expr{Op: TK_COLUMN, Table: 0, Column: 0},
			Right: &Expr{Op: TK_INTEGER, IntValue: 1},
		},
	}

	dest := &SelectDest{Dest: SRT_Output}
	err := sc.CompileSelect(sel, dest)
	if err != nil {
		t.Fatalf("compileSimpleSelect with WHERE failed: %v", err)
	}
}

// --- select.go: compileExcept (80%) ---

// TestCompileExcept3_NonOutput exercises the non-SRT_Output destination in compileExcept.
func TestCompileExcept3_NonOutput(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0, Tabs: 0}
	sc := NewSelectCompiler(parse)

	left := makeSimpleSelect(parse, 1)
	right := makeSimpleSelect(parse, 2)

	right.Op = TK_EXCEPT
	right.Prior = left

	dest := &SelectDest{Dest: SRT_Union, SDParm: parse.AllocCursor()}
	err := sc.compileExcept(right, dest)
	if err != nil {
		t.Fatalf("compileExcept non-output dest failed: %v", err)
	}
}

// --- select.go: setupDistinct (87.5%) ---

// TestSetupDistinct3_WithDistinctFlag verifies distinct cursor is allocated.
func TestSetupDistinct3_WithDistinctFlag(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0, Tabs: 0}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		SelFlags: SF_Distinct,
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
	}
	var distinct DistinctCtx
	sc.setupDistinct(sel, &distinct)

	if distinct.TabTnct == 0 {
		t.Error("expected distinct cursor to be allocated")
	}
}

// --- orderby.go: resolveChildExpressions (71.4%) ---

// TestResolveChildExpressions3_LeftAndRight covers left and right child resolution.
func TestResolveChildExpressions3_LeftAndRight(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 2,
		Columns: []Column{
			{Name: "x", DeclType: "INTEGER"},
			{Name: "y", DeclType: "INTEGER"},
		},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, StringValue: "x"}},
				{Expr: &Expr{Op: TK_COLUMN, StringValue: "y"}},
			},
		},
		Src: srcList,
	}

	// Expression with left and right children (both columns to resolve)
	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:          TK_COLUMN,
			StringValue: "x",
		},
		Right: &Expr{
			Op:          TK_COLUMN,
			StringValue: "y",
		},
	}

	err := obc.resolveChildExpressions(sel, expr)
	if err != nil {
		t.Fatalf("resolveChildExpressions failed: %v", err)
	}
}

// --- orderby.go: resolveQualifiedColumnInOrderBy (75%) ---

// TestResolveQualifiedColumnInOrderBy3_Success verifies qualified column resolves.
func TestResolveQualifiedColumnInOrderBy3_Success(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0, Name: "users"})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		Src: srcList,
	}

	// TK_DOT expression: users.name
	expr := &Expr{
		Op:    TK_DOT,
		Left:  &Expr{Op: TK_ID, StringValue: "users"},
		Right: &Expr{Op: TK_ID, StringValue: "name"},
	}

	err := obc.resolveQualifiedColumnInOrderBy(sel, expr)
	if err != nil {
		t.Fatalf("resolveQualifiedColumnInOrderBy failed: %v", err)
	}
	if expr.Column != 1 {
		t.Errorf("expected column index 1 for 'name', got %d", expr.Column)
	}
}

// TestResolveQualifiedColumnInOrderBy3_NoSrc returns error for nil Src.
func TestResolveQualifiedColumnInOrderBy3_NoSrc(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{}},
		Src:   nil,
	}

	expr := &Expr{
		Op:    TK_DOT,
		Left:  &Expr{Op: TK_ID, StringValue: "missing_table"},
		Right: &Expr{Op: TK_ID, StringValue: "col"},
	}

	err := obc.resolveQualifiedColumnInOrderBy(sel, expr)
	if err == nil {
		t.Error("expected error for nil Src")
	}
}

// TestResolveQualifiedColumnInOrderBy3_TableNotFound returns error when table not found.
func TestResolveQualifiedColumnInOrderBy3_TableNotFound(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	table := &Table{Name: "t", NumColumns: 1, Columns: []Column{{Name: "id"}}}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{}},
		Src:   srcList,
	}

	expr := &Expr{
		Op:    TK_DOT,
		Left:  &Expr{Op: TK_ID, StringValue: "nonexistent"},
		Right: &Expr{Op: TK_ID, StringValue: "id"},
	}

	err := obc.resolveQualifiedColumnInOrderBy(sel, expr)
	if err == nil {
		t.Error("expected error for table not found")
	}
}

// --- orderby.go: compileExpr (85.7%) ---

// TestCompileExpr3_Column verifies TK_COLUMN emits OP_Column.
func TestCompileExpr3_Column(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0}
	obc := NewOrderByCompiler(parse)
	target := parse.AllocReg()

	expr := &Expr{Op: TK_COLUMN, Table: 0, Column: 2}
	obc.compileExpr(expr, target)
	if len(parse.GetVdbe().Ops) == 0 {
		t.Error("expected OP_Column to be emitted")
	}
}

// TestCompileExpr3_String verifies TK_STRING emits OP_String8.
func TestCompileExpr3_String(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0}
	obc := NewOrderByCompiler(parse)
	target := parse.AllocReg()

	expr := &Expr{Op: TK_STRING, StringValue: "hello"}
	obc.compileExpr(expr, target)
	if len(parse.GetVdbe().Ops) == 0 {
		t.Error("expected OP_String8 to be emitted")
	}
}

// TestCompileExpr3_Null verifies TK_NULL emits OP_Null.
func TestCompileExpr3_Null(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0}
	obc := NewOrderByCompiler(parse)
	target := parse.AllocReg()

	expr := &Expr{Op: TK_NULL}
	obc.compileExpr(expr, target)
	if len(parse.GetVdbe().Ops) == 0 {
		t.Error("expected OP_Null to be emitted")
	}
}

// TestCompileExpr3_Default verifies unknown op emits OP_Null.
func TestCompileExpr3_Default(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0}
	obc := NewOrderByCompiler(parse)
	target := parse.AllocReg()

	expr := &Expr{Op: TK_PLUS}
	obc.compileExpr(expr, target)
	if len(parse.GetVdbe().Ops) == 0 {
		t.Error("expected OP_Null to be emitted for default case")
	}
}

// --- result.go: tryGetImplicitName (85.7%) ---

// TestTryGetImplicitName3_Column verifies TK_COLUMN+ColumnRef returns column name.
func TestTryGetImplicitName3_Column(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	col := &Column{Name: "price", DeclType: "REAL"}
	item := &ExprListItem{
		Expr: &Expr{
			Op:        TK_COLUMN,
			ColumnRef: col,
		},
	}
	name := rc.tryGetImplicitName(item)
	if name != "price" {
		t.Errorf("expected 'price', got %q", name)
	}
}

// TestTryGetImplicitName3_ID verifies TK_ID returns StringValue.
func TestTryGetImplicitName3_ID(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	item := &ExprListItem{
		Expr: &Expr{Op: TK_ID, StringValue: "myAlias"},
	}
	name := rc.tryGetImplicitName(item)
	if name != "myAlias" {
		t.Errorf("expected 'myAlias', got %q", name)
	}
}

// TestTryGetImplicitName3_NilExpr returns empty for nil expr.
func TestTryGetImplicitName3_NilExpr(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	item := &ExprListItem{Expr: nil}
	name := rc.tryGetImplicitName(item)
	if name != "" {
		t.Errorf("expected empty for nil expr, got %q", name)
	}
}

// TestTryGetImplicitName3_Other returns empty for other ops.
func TestTryGetImplicitName3_Other(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	item := &ExprListItem{
		Expr: &Expr{Op: TK_INTEGER, IntValue: 42},
	}
	name := rc.tryGetImplicitName(item)
	if name != "" {
		t.Errorf("expected empty for TK_INTEGER, got %q", name)
	}
}

// --- result.go: resolveChildExprs (71.4%) - left-only path ---

// TestResolveChildExprs3_LeftOnly exercises left-only child resolution.
func TestResolveChildExprs3_LeftOnly(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "val", DeclType: "INTEGER"}},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op: TK_NOT,
					Left: &Expr{
						Op:          TK_COLUMN,
						StringValue: "val",
					},
					Right: nil,
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns with left-only child failed: %v", err)
	}
}

// --- result.go: resolveColumnRef (75%) - EmptyStringValue (already resolved) ---

// TestResolveColumnRef3_EmptyStringValue verifies empty StringValue returns nil (already resolved).
func TestResolveColumnRef3_EmptyStringValue(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	sel := &Select{Src: NewSrcList()}

	expr := &Expr{Op: TK_COLUMN, StringValue: ""}
	err := rc.resolveColumnRef(sel, expr)
	if err != nil {
		t.Fatalf("resolveColumnRef with empty StringValue should not fail, got: %v", err)
	}
}

// TestResolveColumnRef3_NoSrc returns error when Src is nil and column not empty.
func TestResolveColumnRef3_NoSrc(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	sel := &Select{Src: nil}
	expr := &Expr{Op: TK_COLUMN, StringValue: "missing"}
	err := rc.resolveColumnRef(sel, expr)
	if err == nil {
		t.Error("expected error for nil Src with non-empty column name")
	}
}

// --- result.go: searchColumnInSrc (85.7%) ---

// TestSearchColumnInSrc3_TableNilSkipped verifies nil Table is skipped.
func TestSearchColumnInSrc3_TableNilSkipped(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: nil}) // nil table should be skipped

	expr := &Expr{Op: TK_COLUMN, StringValue: "x"}
	err := rc.searchColumnInSrc(srcList, "x", expr)
	if err == nil {
		t.Error("expected error when only nil-table entries exist")
	}
}

// --- result.go: resolveQualifiedColumn (88.9%) ---

// TestResolveQualifiedColumn3_DotExpr verifies qualified column resolves correctly.
func TestResolveQualifiedColumn3_DotExpr(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "products",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0, Name: "products"})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:    TK_DOT,
					Left:  &Expr{Op: TK_ID, StringValue: "products"},
					Right: &Expr{Op: TK_ID, StringValue: "name"},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns with dot expr failed: %v", err)
	}
}

// --- orderby.go: generateSortNext (75%) ---

// TestGenerateSortNext3_WithSorter verifies OP_SorterNext when sorter flag set.
func TestGenerateSortNext3_WithSorter(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0, Tabs: 0}
	obc := NewOrderByCompiler(parse)

	orderBy := &ExprList{
		Items: []ExprListItem{{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}}},
	}
	sort := &SortCtx{
		ECursor:   1,
		SortFlags: SORTFLAG_UseSorter,
		OrderBy:   orderBy,
		LabelDone: parse.GetVdbe().MakeLabel(),
	}
	ctx := sortLoopContext{addr: 0, iSortTab: 1, bSeq: false}
	obc.generateSortNext(sort, 1, ctx)

	ops := parse.GetVdbe().Ops
	found := false
	for _, op := range ops {
		if op.Opcode == OP_SorterNext {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected OP_SorterNext to be emitted")
	}
}

// TestGenerateSortNext3_WithoutSorter verifies OP_Next when no sorter flag.
func TestGenerateSortNext3_WithoutSorter(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 0, Tabs: 0}
	obc := NewOrderByCompiler(parse)

	orderBy := &ExprList{
		Items: []ExprListItem{{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}}},
	}
	sort := &SortCtx{
		ECursor:   2,
		SortFlags: 0, // no sorter
		OrderBy:   orderBy,
		LabelDone: parse.GetVdbe().MakeLabel(),
	}
	ctx := sortLoopContext{addr: 0, iSortTab: 2, bSeq: false}
	obc.generateSortNext(sort, 2, ctx)

	ops := parse.GetVdbe().Ops
	found := false
	for _, op := range ops {
		if op.Opcode == OP_Next {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected OP_Next to be emitted")
	}
}

// --- helper for making a simple select ---

func makeSimpleSelect(parse *Parse, rootPage int) *Select {
	table := &Table{
		Name:       "t",
		NumColumns: 1,
		RootPage:   rootPage,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: parse.AllocCursor()})

	return &Select{
		Op: TK_SELECT,
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
		Src: srcList,
	}
}
