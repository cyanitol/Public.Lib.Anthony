// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// TestMCDC5_ReleaseReg covers Parse.ReleaseReg (0% coverage).
// The function is a documented no-op; calling it exercises the body and
// brings the function to 100%.
func TestMCDC5_ReleaseReg(t *testing.T) {
	t.Parallel()
	p := &Parse{Mem: 10}
	// Must not panic and must leave Mem unchanged.
	p.ReleaseReg(5)
	if p.Mem != 10 {
		t.Errorf("ReleaseReg must not modify Mem: got %d, want 10", p.Mem)
	}
}

// TestMCDC5_ReleaseRegs covers Parse.ReleaseRegs (0% coverage).
// Same reasoning as ReleaseReg.
func TestMCDC5_ReleaseRegs(t *testing.T) {
	t.Parallel()
	p := &Parse{Mem: 10}
	p.ReleaseRegs(3, 4)
	if p.Mem != 10 {
		t.Errorf("ReleaseRegs must not modify Mem: got %d, want 10", p.Mem)
	}
}

// TestMCDC5_FindAggsInSelect_WithHavingAndOrderBy covers the Having != nil
// branch and the OrderBy path together in findAggsInSelect (66.7% → 100%).
func TestMCDC5_FindAggsInSelect_WithHavingAndOrderBy(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	havingExpr := &Expr{
		Op:      TK_AGG_FUNCTION,
		FuncDef: &FuncDef{Name: "count"},
	}
	orderByAgg := &Expr{
		Op:      TK_AGG_FUNCTION,
		FuncDef: &FuncDef{Name: "sum"},
	}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
		Having: havingExpr,
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: orderByAgg},
			},
		},
		SelectID: 42,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect: unexpected error: %v", err)
	}
	// count (Having) + sum (OrderBy) = 2
	if len(aggInfo.AggFuncs) != 2 {
		t.Errorf("AggFuncs = %d, want 2", len(aggInfo.AggFuncs))
	}
}

// TestMCDC5_FindAggsInSelect_NoHaving covers the Having == nil branch so
// that branch is explicitly exercised in isolation (for MC/DC completeness).
func TestMCDC5_FindAggsInSelect_NoHaving(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "max"},
				}},
			},
		},
		Having:   nil,
		OrderBy:  nil,
		SelectID: 1,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect no-Having: unexpected error: %v", err)
	}
	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestMCDC5_FindAggsInChildren_NilLeft covers findAggsInChildren when Left
// is nil but Right is non-nil (71.4% branch for Left == nil).
func TestMCDC5_FindAggsInChildren_NilLeft(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	expr := &Expr{
		Op:   TK_MINUS,
		Left: nil,
		Right: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "min"},
		},
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInChildren(expr, aggInfo); err != nil {
		t.Fatalf("findAggsInChildren nil-Left: unexpected error: %v", err)
	}
	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestMCDC5_FindAggsInChildren_NilRight covers the Right == nil branch.
func TestMCDC5_FindAggsInChildren_NilRight(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "sum"},
		},
		Right: nil,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInChildren(expr, aggInfo); err != nil {
		t.Fatalf("findAggsInChildren nil-Right: unexpected error: %v", err)
	}
	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestMCDC5_FindAggsInChildren_NilLeftNilRight covers the case where both
// Left and Right are nil, only List is non-nil.
func TestMCDC5_FindAggsInChildren_NilLeftNilRight(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	expr := &Expr{
		Op:    TK_FUNCTION,
		Left:  nil,
		Right: nil,
		List: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "avg"},
				}},
			},
		},
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInChildren(expr, aggInfo); err != nil {
		t.Fatalf("findAggsInChildren nil-both: unexpected error: %v", err)
	}
	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestMCDC5_ResolveChildExpressions_BothChildren covers resolveChildExpressions
// (71.4%) with an expression that has both Left and Right, each containing a
// resolvable column.
func TestMCDC5_ResolveChildExpressions_BothChildren(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 2,
		Columns: []Column{
			{Name: "a"},
			{Name: "b"},
		},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{}},
		Src:   src,
	}

	// An expression like: a + b  (TK_PLUS with two TK_COLUMN children)
	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:          TK_COLUMN,
			StringValue: "a",
		},
		Right: &Expr{
			Op:          TK_COLUMN,
			StringValue: "b",
		},
	}

	if err := obc.resolveChildExpressions(sel, expr); err != nil {
		t.Fatalf("resolveChildExpressions: unexpected error: %v", err)
	}
}

// TestMCDC5_ResolveChildExpressions_NilLeft covers the Left == nil branch in
// resolveChildExpressions.
func TestMCDC5_ResolveChildExpressions_NilLeft(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "x"}},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{}},
		Src:   src,
	}

	expr := &Expr{
		Op:   TK_NOT,
		Left: nil,
		Right: &Expr{
			Op:          TK_COLUMN,
			StringValue: "x",
		},
	}

	if err := obc.resolveChildExpressions(sel, expr); err != nil {
		t.Fatalf("resolveChildExpressions nil-Left: unexpected error: %v", err)
	}
}

// TestMCDC5_ResolveChildExpressions_NilRight covers Right == nil branch.
func TestMCDC5_ResolveChildExpressions_NilRight(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "y"}},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{}},
		Src:   src,
	}

	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:          TK_COLUMN,
			StringValue: "y",
		},
		Right: nil,
	}

	if err := obc.resolveChildExpressions(sel, expr); err != nil {
		t.Fatalf("resolveChildExpressions nil-Right: unexpected error: %v", err)
	}
}

// TestMCDC5_ResolveChildExprs_BothChildren covers result.go resolveChildExprs
// (71.4%) with both Left and Right set.
func TestMCDC5_ResolveChildExprs_BothChildren(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "emp",
		NumColumns: 2,
		Columns: []Column{
			{Name: "salary"},
			{Name: "bonus"},
		},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{Src: src}

	// salary + bonus — both children are unresolved TK_COLUMN
	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:          TK_COLUMN,
			StringValue: "salary",
		},
		Right: &Expr{
			Op:          TK_COLUMN,
			StringValue: "bonus",
		},
	}

	if err := rc.resolveChildExprs(sel, expr); err != nil {
		t.Fatalf("resolveChildExprs both-children: unexpected error: %v", err)
	}
	// Both should be resolved (StringValue cleared, ColumnRef set).
	if expr.Left.ColumnRef == nil {
		t.Error("Left.ColumnRef should be resolved")
	}
	if expr.Right.ColumnRef == nil {
		t.Error("Right.ColumnRef should be resolved")
	}
}

// TestMCDC5_ResolveChildExprs_NilLeft covers the Left == nil branch in
// resolveChildExprs.
func TestMCDC5_ResolveChildExprs_NilLeft(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "emp",
		NumColumns: 1,
		Columns:    []Column{{Name: "salary"}},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{Src: src}

	expr := &Expr{
		Op:   TK_NOT,
		Left: nil,
		Right: &Expr{
			Op:          TK_COLUMN,
			StringValue: "salary",
		},
	}

	if err := rc.resolveChildExprs(sel, expr); err != nil {
		t.Fatalf("resolveChildExprs nil-Left: unexpected error: %v", err)
	}
	if expr.Right.ColumnRef == nil {
		t.Error("Right.ColumnRef should be resolved when Left is nil")
	}
}

// TestMCDC5_ResolveChildExprs_NilRight covers Right == nil branch.
func TestMCDC5_ResolveChildExprs_NilRight(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "emp",
		NumColumns: 1,
		Columns:    []Column{{Name: "bonus"}},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{Src: src}

	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:          TK_COLUMN,
			StringValue: "bonus",
		},
		Right: nil,
	}

	if err := rc.resolveChildExprs(sel, expr); err != nil {
		t.Fatalf("resolveChildExprs nil-Right: unexpected error: %v", err)
	}
	if expr.Left.ColumnRef == nil {
		t.Error("Left.ColumnRef should be resolved when Right is nil")
	}
}

// TestMCDC5_ProcessFromClause_NegativeCursor covers the cursor < 0 branch in
// processFromClause (select.go, 93.8%) where the cursor is negative and must
// be allocated.
func TestMCDC5_ProcessFromClause_NegativeCursor(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil), Tabs: 0}
	sc := NewSelectCompiler(parse)

	table := &Table{
		Name:     "orders",
		RootPage: 7,
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: -1})

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{
			{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
		}},
		Src: src,
	}

	if err := sc.processFromClause(sel); err != nil {
		t.Fatalf("processFromClause negative cursor: unexpected error: %v", err)
	}
	// Cursor must have been allocated (non-negative).
	if src.Get(0).Cursor < 0 {
		t.Error("cursor should be non-negative after processFromClause")
	}
}

// TestMCDC5_ProcessFromClause_NilSrc covers the nil/empty Src early-return
// branch in processFromClause.
func TestMCDC5_ProcessFromClause_NilSrc(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{
			{Expr: &Expr{Op: TK_INTEGER, IntValue: 42}},
		}},
		Src: nil,
	}

	if err := sc.processFromClause(sel); err != nil {
		t.Fatalf("processFromClause nil-Src: unexpected error: %v", err)
	}
}

// TestMCDC5_ProcessFromClause_NilTable covers the nil table continue branch.
func TestMCDC5_ProcessFromClause_NilTable(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	sc := NewSelectCompiler(parse)

	src := NewSrcList()
	src.Append(SrcListItem{Table: nil, Cursor: 0})

	sel := &Select{
		EList: &ExprList{Items: []ExprListItem{
			{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
		}},
		Src: src,
	}

	if err := sc.processFromClause(sel); err != nil {
		t.Fatalf("processFromClause nil-table: unexpected error: %v", err)
	}
}

// TestMCDC5_CompileDeleteWithIndex_NoIndexes covers the len(indexes)==0 early
// return in CompileDeleteWithIndex (93.8%).
func TestMCDC5_CompileDeleteWithIndex_NoIndexes(t *testing.T) {
	t.Parallel()
	stmt := &DeleteStmt{Table: "items"}
	prog, err := CompileDeleteWithIndex(stmt, 1, nil)
	if err != nil {
		t.Fatalf("CompileDeleteWithIndex no indexes: unexpected error: %v", err)
	}
	if prog == nil {
		t.Error("expected non-nil program")
	}
}

// TestMCDC5_CompileDeleteWithIndex_WithIndexes covers the indexes path in
// CompileDeleteWithIndex — exercises the loop that inserts cursor open/close.
func TestMCDC5_CompileDeleteWithIndex_WithIndexes(t *testing.T) {
	t.Parallel()
	stmt := &DeleteStmt{Table: "items"}
	indexes := []IndexInfo{
		{Name: "idx_a", Root: 10, Columns: []string{"a"}},
	}
	prog, err := CompileDeleteWithIndex(stmt, 1, indexes)
	if err != nil {
		t.Fatalf("CompileDeleteWithIndex with indexes: unexpected error: %v", err)
	}
	if prog == nil {
		t.Error("expected non-nil program")
	}
	// Should have at least one extra OpenWrite for the index cursor.
	openWriteCount := 0
	for _, inst := range prog.Instructions {
		if inst.OpCode == OpOpenWrite {
			openWriteCount++
		}
	}
	if openWriteCount < 2 {
		t.Errorf("expected ≥2 OpenWrite instructions (table + index), got %d", openWriteCount)
	}
}

// TestMCDC5_CompileDelete_WithWhere covers the WHERE branch of CompileDelete
// (94.4%).
func TestMCDC5_CompileDelete_WithWhere(t *testing.T) {
	t.Parallel()
	where := &WhereClause{
		Expr: &Expression{
			Type:  ExprLiteral,
			Value: Value{Type: TypeInteger, Int: 1},
		},
	}
	stmt := &DeleteStmt{Table: "products", Where: where}
	prog, err := CompileDelete(stmt, 2)
	if err != nil {
		t.Fatalf("CompileDelete with WHERE: unexpected error: %v", err)
	}
	if prog == nil {
		t.Error("expected non-nil program")
	}
	hasDelete := false
	for _, inst := range prog.Instructions {
		if inst.OpCode == OpDelete {
			hasDelete = true
			break
		}
	}
	if !hasDelete {
		t.Error("expected OpDelete instruction")
	}
}

// TestMCDC5_CompileDelete_NoWhere covers the no-WHERE (clear-table) branch of
// CompileDelete.
func TestMCDC5_CompileDelete_NoWhere(t *testing.T) {
	t.Parallel()
	stmt := &DeleteStmt{Table: "products"}
	prog, err := CompileDelete(stmt, 2)
	if err != nil {
		t.Fatalf("CompileDelete no WHERE: unexpected error: %v", err)
	}
	if prog == nil {
		t.Error("expected non-nil program")
	}
}

// TestMCDC5_CompileUpdate_WithWhere covers the WHERE branch inside
// updateCompiler.compile → compileWhere (update.go compile at 95.8%).
func TestMCDC5_CompileUpdate_WithWhere(t *testing.T) {
	t.Parallel()
	where := &WhereClause{
		Expr: &Expression{
			Type:  ExprLiteral,
			Value: Value{Type: TypeInteger, Int: 1},
		},
	}
	stmt := &UpdateStmt{
		Table:   "employees",
		Columns: []string{"name"},
		Values:  []Value{{Type: TypeText, Text: "Alice"}},
		Where:   where,
	}
	prog, err := CompileUpdate(stmt, 1, 3)
	if err != nil {
		t.Fatalf("CompileUpdate with WHERE: unexpected error: %v", err)
	}
	if prog == nil {
		t.Error("expected non-nil program")
	}
	hasIfNot := false
	for _, inst := range prog.Instructions {
		if inst.OpCode == OpIfNot {
			hasIfNot = true
			break
		}
	}
	if !hasIfNot {
		t.Error("expected OpIfNot instruction for WHERE clause")
	}
}

// TestMCDC5_EstimateDeleteCost_WithLimitLessThanRows covers the limit branch
// in EstimateDeleteCost (delete.go).
func TestMCDC5_EstimateDeleteCost_WithLimitLessThanRows(t *testing.T) {
	t.Parallel()
	lim := 5
	stmt := &DeleteStmt{
		Table: "t",
		Where: &WhereClause{Expr: &Expression{Type: ExprLiteral}},
		Limit: &lim,
	}
	cost := EstimateDeleteCost(stmt, 100)
	if cost != 5 {
		t.Errorf("EstimateDeleteCost = %d, want 5", cost)
	}
}

// TestMCDC5_EstimateDeleteCost_NoWhere covers the no-WHERE truncate path.
func TestMCDC5_EstimateDeleteCost_NoWhere(t *testing.T) {
	t.Parallel()
	stmt := &DeleteStmt{Table: "t"}
	cost := EstimateDeleteCost(stmt, 1000)
	if cost != 1 {
		t.Errorf("EstimateDeleteCost no-WHERE = %d, want 1", cost)
	}
}

// TestMCDC5_EstimateDeleteCost_LimitGreaterThanRows covers the branch where
// Limit >= tableRows (cost stays at tableRows).
func TestMCDC5_EstimateDeleteCost_LimitGreaterThanRows(t *testing.T) {
	t.Parallel()
	lim := 500
	stmt := &DeleteStmt{
		Table: "t",
		Where: &WhereClause{Expr: &Expression{Type: ExprLiteral}},
		Limit: &lim,
	}
	cost := EstimateDeleteCost(stmt, 100)
	if cost != 100 {
		t.Errorf("EstimateDeleteCost limit>rows = %d, want 100", cost)
	}
}

// TestMCDC5_SplitLimitOffset_ZeroOffset covers the offset==0 early-return in
// SplitLimitOffset (limit.go).
func TestMCDC5_SplitLimitOffset_ZeroOffset(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	got := lc.SplitLimitOffset(10, 0)
	if got != 10 {
		t.Errorf("SplitLimitOffset(10,0) = %d, want 10", got)
	}
}

// TestMCDC5_SplitLimitOffset_ZeroEffective covers the effective==0 path.
func TestMCDC5_SplitLimitOffset_ZeroEffective(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	got := lc.SplitLimitOffset(0, 5)
	if got != 0 {
		t.Errorf("SplitLimitOffset(0,5) = %d, want 0", got)
	}
}

// TestMCDC5_SplitLimitOffset_EffectiveLessThanOffset covers the
// effective <= offset path (returns 0).
func TestMCDC5_SplitLimitOffset_EffectiveLessThanOffset(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	got := lc.SplitLimitOffset(3, 5)
	if got != 0 {
		t.Errorf("SplitLimitOffset(3,5) = %d, want 0", got)
	}
}

// TestMCDC5_GenerateLimitOffsetPlan_OrderBy covers the ApplyAfterSort branch.
func TestMCDC5_GenerateLimitOffsetPlan_OrderBy(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)

	sel := &Select{
		Limit:  10,
		Offset: 2,
		OrderBy: &ExprList{Items: []ExprListItem{
			{Expr: &Expr{Op: TK_COLUMN}},
		}},
	}

	plan, err := lc.GenerateLimitOffsetPlan(sel)
	if err != nil {
		t.Fatalf("GenerateLimitOffsetPlan: unexpected error: %v", err)
	}
	if !plan.ApplyAfterSort {
		t.Error("plan.ApplyAfterSort should be true when OrderBy is set")
	}
}

// TestMCDC5_GenerateLimitOffsetPlan_GroupBy covers the ApplyAfterGroup branch.
func TestMCDC5_GenerateLimitOffsetPlan_GroupBy(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)

	sel := &Select{
		Limit:  5,
		Offset: 0,
		GroupBy: &ExprList{Items: []ExprListItem{
			{Expr: &Expr{Op: TK_COLUMN}},
		}},
	}

	plan, err := lc.GenerateLimitOffsetPlan(sel)
	if err != nil {
		t.Fatalf("GenerateLimitOffsetPlan GroupBy: unexpected error: %v", err)
	}
	if !plan.ApplyAfterGroup {
		t.Error("plan.ApplyAfterGroup should be true when GroupBy is set")
	}
}

// TestMCDC5_GenerateLimitOffsetPlan_DuringScan covers the ApplyDuringScan
// branch (no OrderBy, no GroupBy).
func TestMCDC5_GenerateLimitOffsetPlan_DuringScan(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)

	sel := &Select{
		Limit:  3,
		Offset: 1,
	}

	plan, err := lc.GenerateLimitOffsetPlan(sel)
	if err != nil {
		t.Fatalf("GenerateLimitOffsetPlan DuringScan: unexpected error: %v", err)
	}
	if !plan.ApplyDuringScan {
		t.Error("plan.ApplyDuringScan should be true for plain query")
	}
}

// TestMCDC5_ValidateLimitOffset_NegativeLimit covers the negative LIMIT error.
func TestMCDC5_ValidateLimitOffset_NegativeLimit(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	if err := lc.ValidateLimitOffset(-1, 0); err == nil {
		t.Error("expected error for negative LIMIT")
	}
}

// TestMCDC5_ValidateLimitOffset_NegativeOffset covers the negative OFFSET error.
func TestMCDC5_ValidateLimitOffset_NegativeOffset(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	if err := lc.ValidateLimitOffset(0, -1); err == nil {
		t.Error("expected error for negative OFFSET")
	}
}

// TestMCDC5_ValidateLimitOffset_Overflow covers the overflow check branch.
func TestMCDC5_ValidateLimitOffset_Overflow(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	maxInt := int(^uint(0) >> 1)
	if err := lc.ValidateLimitOffset(maxInt, 1); err == nil {
		t.Error("expected overflow error for large LIMIT+OFFSET")
	}
}

// TestMCDC5_ComputeLimitOffset_NegativeLimit covers the limit < 0 error path.
func TestMCDC5_ComputeLimitOffset_NegativeLimit(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	limitExpr := &Expr{Op: TK_INTEGER, IntValue: -5}
	_, _, err := lc.ComputeLimitOffset(limitExpr, nil)
	if err == nil {
		t.Error("expected error for negative LIMIT expression")
	}
}

// TestMCDC5_ComputeLimitOffset_NegativeOffset covers the offset < 0 error path.
func TestMCDC5_ComputeLimitOffset_NegativeOffset(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	offsetExpr := &Expr{Op: TK_INTEGER, IntValue: -3}
	_, _, err := lc.ComputeLimitOffset(nil, offsetExpr)
	if err == nil {
		t.Error("expected error for negative OFFSET expression")
	}
}

// TestMCDC5_ComputeLimitOffset_NonConstantLimit covers the non-INTEGER LIMIT error.
func TestMCDC5_ComputeLimitOffset_NonConstantLimit(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	limitExpr := &Expr{Op: TK_COLUMN} // not TK_INTEGER
	_, _, err := lc.ComputeLimitOffset(limitExpr, nil)
	if err == nil {
		t.Error("expected error for non-constant LIMIT expression")
	}
}

// TestMCDC5_ComputeLimitOffset_NonConstantOffset covers the non-INTEGER OFFSET error.
func TestMCDC5_ComputeLimitOffset_NonConstantOffset(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	offsetExpr := &Expr{Op: TK_COLUMN}
	_, _, err := lc.ComputeLimitOffset(nil, offsetExpr)
	if err == nil {
		t.Error("expected error for non-constant OFFSET expression")
	}
}

// TestMCDC5_OptimizeLimitWithIndex_ZeroLimit covers the info.Limit==0 path.
func TestMCDC5_OptimizeLimitWithIndex_ZeroLimit(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	sel := &Select{}
	info := &LimitInfo{Limit: 0}
	if lc.OptimizeLimitWithIndex(sel, info) {
		t.Error("expected false when Limit==0")
	}
}

// TestMCDC5_OptimizeLimitWithIndex_NoOrderBy covers the no-OrderBy false path.
func TestMCDC5_OptimizeLimitWithIndex_NoOrderBy(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	sel := &Select{
		GroupBy: nil, // no groupby → passes that check
		OrderBy: nil, // no orderby → returns false
	}
	info := &LimitInfo{Limit: 10}
	if lc.OptimizeLimitWithIndex(sel, info) {
		t.Error("expected false when no OrderBy")
	}
}

// TestMCDC5_OptimizeLimitWithIndex_WithGroupBy covers the GroupBy path.
func TestMCDC5_OptimizeLimitWithIndex_WithGroupBy(t *testing.T) {
	t.Parallel()
	parse := &Parse{Vdbe: NewVdbe(nil)}
	lc := NewLimitCompiler(parse)
	sel := &Select{
		GroupBy: &ExprList{Items: []ExprListItem{
			{Expr: &Expr{Op: TK_COLUMN}},
		}},
	}
	info := &LimitInfo{Limit: 10}
	if lc.OptimizeLimitWithIndex(sel, info) {
		t.Error("expected false when GroupBy is non-empty")
	}
}
