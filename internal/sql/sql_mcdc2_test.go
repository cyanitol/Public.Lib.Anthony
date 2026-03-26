// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// TestMCDC_TableGetColumn exercises the compound condition in Table.GetColumn.
//
// Condition: t == nil || idx < 0 || idx >= len(t.Columns)
//
// MC/DC cases (three sub-conditions with short-circuit OR):
//
//	A: t=nil                      → first sub true (flips outcome)
//	B: t!=nil, idx=-1             → first false, second true (flips outcome)
//	C: t!=nil, idx>=len(Columns)  → first+second false, third true (flips outcome)
//	D: t!=nil, idx in range       → all false → returns column
func TestMCDC_TableGetColumn(t *testing.T) {
	t.Parallel()

	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", Affinity: SQLITE_AFF_INTEGER},
			{Name: "name", Affinity: SQLITE_AFF_TEXT},
		},
	}

	tests := []struct {
		name    string
		tbl     *Table
		idx     int
		wantNil bool
	}{
		// A: nil receiver
		{name: "MCDC_nil_table_returns_nil", tbl: nil, idx: 0, wantNil: true},
		// B: negative index
		{name: "MCDC_negative_index_returns_nil", tbl: table, idx: -1, wantNil: true},
		// C: index == len (boundary)
		{name: "MCDC_index_equals_len_returns_nil", tbl: table, idx: 2, wantNil: true},
		// C: index > len
		{name: "MCDC_index_beyond_len_returns_nil", tbl: table, idx: 99, wantNil: true},
		// D: valid index 0 (all sub-conditions false)
		{name: "MCDC_valid_index_zero_returns_column", tbl: table, idx: 0, wantNil: false},
		// D: valid index 1
		{name: "MCDC_valid_index_one_returns_column", tbl: table, idx: 1, wantNil: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.tbl.GetColumn(tt.idx)
			if tt.wantNil && got != nil {
				t.Errorf("GetColumn(%d) = %+v, want nil", tt.idx, got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("GetColumn(%d) = nil, want non-nil", tt.idx)
			}
		})
	}
}

// TestMCDC_AggregateCheckNewGroup exercises the compound condition in checkNewGroup.
//
// Condition: sel.GroupBy == nil || sel.GroupBy.Len() == 0
//
// MC/DC cases:
//
//	A: GroupBy=nil          → first sub true → early return (flips outcome)
//	B: GroupBy non-nil, Len=0 → first false, second true → early return (flips outcome)
//	C: GroupBy non-nil, Len>0 → both false → emits comparison code
func TestMCDC_AggregateCheckNewGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		groupBy       *ExprList
		wantNoOpsEmit bool // true = early return expected (no GROUP BY ops emitted)
	}{
		// A: nil GroupBy → early return
		{
			name:          "MCDC_nil_groupby_returns_early",
			groupBy:       nil,
			wantNoOpsEmit: true,
		},
		// B: non-nil but empty GroupBy → early return
		{
			name:          "MCDC_empty_groupby_returns_early",
			groupBy:       NewExprList(),
			wantNoOpsEmit: true,
		},
		// C: GroupBy with items → emits comparison code
		{
			name: "MCDC_nonempty_groupby_emits_ops",
			groupBy: &ExprList{Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			}},
			wantNoOpsEmit: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			ac := NewAggregateCompiler(p)
			vdbe := p.GetVdbe()
			initialOps := len(vdbe.Ops)

			sel := &Select{
				EList:   &ExprList{Items: []ExprListItem{{Expr: &Expr{Op: TK_INTEGER, IntValue: 42}}}},
				GroupBy: tt.groupBy,
			}
			aggInfo := &AggInfo{}

			// continueAddr is an arbitrary label
			ac.checkNewGroup(sel, aggInfo, 0)

			opsAdded := len(vdbe.Ops) - initialOps
			if tt.wantNoOpsEmit && opsAdded != 0 {
				t.Errorf("expected no ops emitted (early return), but got %d ops", opsAdded)
			}
			if !tt.wantNoOpsEmit && opsAdded == 0 {
				t.Errorf("expected ops to be emitted, but none were")
			}
		})
	}
}

// TestMCDC_EvalArgReg exercises the compound condition in evalArgReg.
//
// Condition: aggFunc.Expr.List == nil || aggFunc.Expr.List.Len() == 0
//
// MC/DC cases:
//
//	A: List=nil          → first sub true → returns 0 (flips outcome)
//	B: List non-nil, Len=0 → first false, second true → returns 0 (flips outcome)
//	C: List non-nil, Len>0 → both false → allocates register, returns non-zero
func TestMCDC_EvalArgReg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		exprList *ExprList
		wantZero bool // true = returns 0 (no argument allocated)
	}{
		// A: nil List → returns 0
		{
			name:     "MCDC_nil_list_returns_zero",
			exprList: nil,
			wantZero: true,
		},
		// B: non-nil empty List → returns 0
		{
			name:     "MCDC_empty_list_returns_zero",
			exprList: NewExprList(),
			wantZero: true,
		},
		// C: non-nil non-empty List → returns non-zero register
		{
			name: "MCDC_nonempty_list_returns_nonzero",
			exprList: &ExprList{Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 5}},
			}},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			ac := NewAggregateCompiler(p)
			// Ensure VDBE is initialized
			_ = p.GetVdbe()

			aggFunc := &AggFunc{
				Expr: &Expr{
					Op:   TK_AGG_FUNCTION,
					List: tt.exprList,
				},
				Func:   &FuncDef{Name: "count"},
				RegAcc: 1,
			}

			got := ac.evalArgReg(aggFunc)
			if tt.wantZero && got != 0 {
				t.Errorf("evalArgReg() = %d, want 0", got)
			}
			if !tt.wantZero && got == 0 {
				t.Errorf("evalArgReg() = 0, want non-zero register")
			}
		})
	}
}

// TestMCDC_EmitNextRow exercises the compound condition in emitNextRow.
//
// Condition: sel.Src == nil || sel.Src.Len() == 0
//
// MC/DC cases:
//
//	A: Src=nil           → first sub true → early return, no OP_Next emitted
//	B: Src non-nil, Len=0 → first false, second true → early return, no OP_Next
//	C: Src non-nil, Len>0 → both false → emits OP_Next instruction
func TestMCDC_EmitNextRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		src        *SrcList
		wantNextOp bool // true = expect OP_Next to be emitted
	}{
		// A: nil Src → no OP_Next
		{
			name:       "MCDC_nil_src_no_op_next",
			src:        nil,
			wantNextOp: false,
		},
		// B: non-nil empty Src → no OP_Next
		{
			name:       "MCDC_empty_src_no_op_next",
			src:        NewSrcList(),
			wantNextOp: false,
		},
		// C: Src with one item → emits OP_Next
		{
			name: "MCDC_nonempty_src_emits_op_next",
			src: &SrcList{Items: []SrcListItem{
				{Name: "t1", Cursor: 0},
			}},
			wantNextOp: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			ac := NewAggregateCompiler(p)
			vdbe := p.GetVdbe()

			sel := &Select{
				EList: &ExprList{Items: []ExprListItem{{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}}}},
				Src:   tt.src,
			}

			initialOps := len(vdbe.Ops)
			ac.emitNextRow(sel, 0)
			opsAdded := len(vdbe.Ops) - initialOps

			hasNext := false
			for i := initialOps; i < len(vdbe.Ops); i++ {
				if vdbe.Ops[i].Opcode == OP_Next {
					hasNext = true
					break
				}
			}

			if tt.wantNextOp && !hasNext {
				t.Errorf("expected OP_Next to be emitted (%d ops added), but it was not", opsAdded)
			}
			if !tt.wantNextOp && hasNext {
				t.Errorf("expected no OP_Next, but one was emitted")
			}
		})
	}
}

// TestMCDC_ComputeNumCols exercises the two compound conditions in computeNumCols.
//
// Condition 1: len(stmt.Columns) > 0
//
//	A: Columns non-empty → true → return len(Columns) (flips outcome)
//	B: Columns empty     → false → check Values
//
// Condition 2 (only when A false): len(stmt.Values) > 0
//
//	C: Values non-empty → true → return len(Values[0]) (flips outcome)
//	D: Values empty     → false → return 0
func TestMCDC_ComputeNumCols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stmt *InsertStmt
		want int
	}{
		// A: Columns non-empty → uses Columns length
		{
			name: "MCDC_columns_present_uses_columns_len",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: []string{"a", "b", "c"},
				Values:  [][]Value{{{Type: TypeInteger, Int: 1}}},
			},
			want: 3,
		},
		// B+C: Columns empty, Values non-empty → uses Values[0] length
		{
			name: "MCDC_no_columns_values_present_uses_values_len",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: []string{},
				Values: [][]Value{
					{{Type: TypeInteger, Int: 1}, {Type: TypeText, Text: "hi"}},
				},
			},
			want: 2,
		},
		// B+D: Columns empty, Values empty → returns 0
		{
			name: "MCDC_no_columns_no_values_returns_zero",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: []string{},
				Values:  [][]Value{},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := computeNumCols(tt.stmt)
			if got != tt.want {
				t.Errorf("computeNumCols() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestMCDC_ValidateInsertRowCounts exercises the compound condition in validateInsertRowCounts.
//
// Condition: numCols == 0 && len(stmt.Values) > 0
//
// This condition is used to derive numCols from Values[0] when no explicit columns given.
//
//	A: numCols=0 (no columns), len(Values)>0 → both true → numCols = len(Values[0])
//	B: numCols>0 (columns given)             → first sub false → numCols already set
//	C: numCols=0, len(Values)=0             → second sub false → numCols stays 0
//
// Secondary condition: len(row) != numCols (per-row check)
//
//	D: len(row) == numCols → false → no error
//	E: len(row) != numCols → true → error
func TestMCDC_ValidateInsertRowCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stmt    *InsertStmt
		wantErr bool
	}{
		// A: no columns, values present → numCols derived from Values[0], consistent rows ok
		{
			name: "MCDC_no_cols_values_present_consistent_ok",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: nil,
				Values: [][]Value{
					{{Type: TypeInteger, Int: 1}, {Type: TypeText, Text: "a"}},
					{{Type: TypeInteger, Int: 2}, {Type: TypeText, Text: "b"}},
				},
			},
			wantErr: false,
		},
		// B: columns given → numCols from len(Columns), consistent rows ok
		{
			name: "MCDC_cols_given_consistent_rows_ok",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: []string{"x", "y"},
				Values: [][]Value{
					{{Type: TypeInteger, Int: 1}, {Type: TypeText, Text: "a"}},
				},
			},
			wantErr: false,
		},
		// E: row count mismatch → error (len(row) != numCols is true)
		{
			name: "MCDC_row_length_mismatch_errors",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: []string{"x", "y"},
				Values: [][]Value{
					{{Type: TypeInteger, Int: 1}}, // only 1 value for 2 columns
				},
			},
			wantErr: true,
		},
		// A + E: no cols, second row mismatched
		{
			name: "MCDC_no_cols_second_row_mismatch_errors",
			stmt: &InsertStmt{
				Table:   "t",
				Columns: nil,
				Values: [][]Value{
					{{Type: TypeInteger, Int: 1}, {Type: TypeText, Text: "a"}},
					{{Type: TypeInteger, Int: 2}}, // only 1 value, expect 2
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateInsertRowCounts(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateInsertRowCounts() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_EstimateDeleteCost exercises the compound condition in EstimateDeleteCost.
//
// Condition: stmt.Limit != nil && *stmt.Limit < tableRows
//
// MC/DC cases:
//
//	A: Limit=nil              → first sub false → cost = tableRows (flips outcome)
//	B: Limit non-nil, *Limit >= tableRows → second sub false → cost = tableRows
//	C: Limit non-nil, *Limit < tableRows  → both true → cost = *Limit (flips outcome)
//
// Note: stmt.Where == nil triggers the early return of 1, so for conditions A/B/C
// we must have stmt.Where != nil.
func TestMCDC_EstimateDeleteCost(t *testing.T) {
	t.Parallel()

	whereClause := &WhereClause{Expr: &Expression{Type: ExprColumn, Column: "id"}}

	limitOf := func(v int) *int { return &v }

	tests := []struct {
		name      string
		stmt      *DeleteStmt
		tableRows int
		want      int
	}{
		// Where=nil → early return 1 (no compound condition reached)
		{
			name:      "MCDC_no_where_returns_one",
			stmt:      &DeleteStmt{Table: "t", Where: nil},
			tableRows: 1000,
			want:      1,
		},
		// A: Where present, Limit=nil → cost = tableRows
		{
			name:      "MCDC_where_nil_limit_cost_equals_table_rows",
			stmt:      &DeleteStmt{Table: "t", Where: whereClause, Limit: nil},
			tableRows: 500,
			want:      500,
		},
		// B: Where present, Limit non-nil but >= tableRows → cost = tableRows
		{
			name:      "MCDC_where_limit_gte_table_rows_cost_equals_table_rows",
			stmt:      &DeleteStmt{Table: "t", Where: whereClause, Limit: limitOf(500)},
			tableRows: 500,
			want:      500,
		},
		{
			name:      "MCDC_where_limit_gt_table_rows_cost_equals_table_rows",
			stmt:      &DeleteStmt{Table: "t", Where: whereClause, Limit: limitOf(1000)},
			tableRows: 500,
			want:      500,
		},
		// C: Where present, Limit non-nil and < tableRows → cost = *Limit
		{
			name:      "MCDC_where_limit_lt_table_rows_cost_equals_limit",
			stmt:      &DeleteStmt{Table: "t", Where: whereClause, Limit: limitOf(10)},
			tableRows: 500,
			want:      10,
		},
		{
			name:      "MCDC_where_limit_one_lt_table_rows_cost_equals_one",
			stmt:      &DeleteStmt{Table: "t", Where: whereClause, Limit: limitOf(1)},
			tableRows: 100,
			want:      1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EstimateDeleteCost(tt.stmt, tt.tableRows)
			if got != tt.want {
				t.Errorf("EstimateDeleteCost() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestMCDC_ApplyOffsetFilter exercises the compound condition in applyOffsetFilter.
//
// Condition: sort != nil || sel.Offset <= 0
//
// MC/DC cases:
//
//	A: sort!=nil              → first sub true → early return (flips outcome)
//	B: sort=nil, Offset<=0    → first false, second true → early return (flips outcome)
//	C: sort=nil, Offset>0     → both false → emits offset code
func TestMCDC_ApplyOffsetFilter(t *testing.T) {
	t.Parallel()

	nonNilSort := &SortCtx{}

	tests := []struct {
		name        string
		sort        *SortCtx
		offset      int
		wantOpsEmit bool // true = expects ops to be emitted
	}{
		// A: sort != nil → no offset code emitted (early return)
		{
			name:        "MCDC_sort_nonnull_no_offset_code",
			sort:        nonNilSort,
			offset:      5,
			wantOpsEmit: false,
		},
		// B: sort=nil, Offset=0 → no offset code emitted
		{
			name:        "MCDC_sort_nil_offset_zero_no_offset_code",
			sort:        nil,
			offset:      0,
			wantOpsEmit: false,
		},
		// B: sort=nil, Offset<0 → no offset code emitted
		{
			name:        "MCDC_sort_nil_offset_negative_no_offset_code",
			sort:        nil,
			offset:      -1,
			wantOpsEmit: false,
		},
		// C: sort=nil, Offset>0 → emits offset checking ops
		{
			name:        "MCDC_sort_nil_offset_positive_emits_code",
			sort:        nil,
			offset:      3,
			wantOpsEmit: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			sc := NewSelectCompiler(p)
			vdbe := p.GetVdbe()

			sel := &Select{
				EList:  &ExprList{Items: []ExprListItem{{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}}}},
				Offset: tt.offset,
			}

			initialOps := len(vdbe.Ops)
			sc.applyOffsetFilter(tt.sort, sel, 0)
			opsAdded := len(vdbe.Ops) - initialOps

			if tt.wantOpsEmit && opsAdded == 0 {
				t.Errorf("expected ops to be emitted for offset filter, but none were")
			}
			if !tt.wantOpsEmit && opsAdded != 0 {
				t.Errorf("expected no ops, but got %d ops emitted", opsAdded)
			}
		})
	}
}

// TestMCDC_ApplyLimitFilter exercises the compound condition in applyLimitFilter.
//
// Condition: sort != nil || sel.Limit <= 0
//
// MC/DC cases:
//
//	A: sort!=nil              → first sub true → early return (flips outcome)
//	B: sort=nil, Limit<=0     → first false, second true → early return (flips outcome)
//	C: sort=nil, Limit>0      → both false → emits limit check code
func TestMCDC_ApplyLimitFilter(t *testing.T) {
	t.Parallel()

	nonNilSort := &SortCtx{}

	tests := []struct {
		name        string
		sort        *SortCtx
		limit       int
		wantOpsEmit bool // true = expects limit-check ops to be emitted
	}{
		// A: sort != nil → no limit code emitted
		{
			name:        "MCDC_sort_nonnull_no_limit_code",
			sort:        nonNilSort,
			limit:       10,
			wantOpsEmit: false,
		},
		// B: sort=nil, Limit=0 → no limit code emitted
		{
			name:        "MCDC_sort_nil_limit_zero_no_limit_code",
			sort:        nil,
			limit:       0,
			wantOpsEmit: false,
		},
		// B: sort=nil, Limit<0 → no limit code emitted
		{
			name:        "MCDC_sort_nil_limit_negative_no_limit_code",
			sort:        nil,
			limit:       -1,
			wantOpsEmit: false,
		},
		// C: sort=nil, Limit>0 → emits limit check ops
		{
			name:        "MCDC_sort_nil_limit_positive_emits_code",
			sort:        nil,
			limit:       5,
			wantOpsEmit: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			sc := NewSelectCompiler(p)
			vdbe := p.GetVdbe()

			sel := &Select{
				EList: &ExprList{Items: []ExprListItem{{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}}}},
				Limit: tt.limit,
			}

			initialOps := len(vdbe.Ops)
			sc.applyLimitFilter(tt.sort, sel, 0)
			opsAdded := len(vdbe.Ops) - initialOps

			if tt.wantOpsEmit && opsAdded == 0 {
				t.Errorf("expected limit-check ops to be emitted, but none were")
			}
			if !tt.wantOpsEmit && opsAdded != 0 {
				t.Errorf("expected no ops, but got %d ops emitted", opsAdded)
			}
		})
	}
}

// TestMCDC_CanUseOrderedDistinct exercises the compound condition in canUseOrderedDistinct.
//
// Condition: sel.OrderBy == nil || sel.EList.Len() != sel.OrderBy.Len()
//
// MC/DC cases (function returns false when condition is true, true when both false):
//
//	A: OrderBy=nil                   → first sub true → returns false (flips outcome)
//	B: OrderBy!=nil, EList.Len() != OrderBy.Len() → second sub true → returns false (flips)
//	C: OrderBy!=nil, EList.Len() == OrderBy.Len() → both false → returns true
func TestMCDC_CanUseOrderedDistinct(t *testing.T) {
	t.Parallel()

	twoItems := &ExprList{Items: []ExprListItem{
		{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
		{Expr: &Expr{Op: TK_INTEGER, IntValue: 2}},
	}}

	tests := []struct {
		name    string
		elist   *ExprList
		orderBy *ExprList
		want    bool
	}{
		// A: OrderBy=nil → cannot use ordered distinct
		{
			name:    "MCDC_nil_orderby_returns_false",
			elist:   twoItems,
			orderBy: nil,
			want:    false,
		},
		// B: OrderBy length differs from EList length → cannot use ordered distinct
		{
			name:  "MCDC_orderby_len_differs_returns_false",
			elist: twoItems,
			orderBy: &ExprList{Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			}},
			want: false,
		},
		// C: OrderBy length equals EList length → can use ordered distinct
		{
			name:  "MCDC_orderby_len_equals_elist_len_returns_true",
			elist: twoItems,
			orderBy: &ExprList{Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 2}},
			}},
			want: true,
		},
		// C: both have zero items (edge case: 0 == 0)
		{
			name:    "MCDC_both_empty_returns_true",
			elist:   NewExprList(),
			orderBy: NewExprList(),
			want:    true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			sc := NewSelectCompiler(p)

			sel := &Select{
				EList:   tt.elist,
				OrderBy: tt.orderBy,
			}

			got := sc.canUseOrderedDistinct(sel)
			if got != tt.want {
				t.Errorf("canUseOrderedDistinct() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMCDC_TryGetExplicitName exercises the compound condition in tryGetExplicitName.
//
// Condition: item.Name != "" && !item.BNoExpand
//
// MC/DC cases:
//
//	A: Name=""           → first sub false → returns "" (flips outcome)
//	B: Name!="", BNoExpand=true  → first true, second false → returns "" (flips outcome)
//	C: Name!="", BNoExpand=false → both true → returns Name
func TestMCDC_TryGetExplicitName(t *testing.T) {
	t.Parallel()

	p := &Parse{DB: &Database{Name: "test"}}
	rc := NewResultCompiler(p)

	tests := []struct {
		name      string
		item      ExprListItem
		wantEmpty bool // true = returns ""
		wantName  string
	}{
		// A: Name="" → returns ""
		{
			name:      "MCDC_empty_name_returns_empty",
			item:      ExprListItem{Name: "", BNoExpand: false},
			wantEmpty: true,
		},
		// B: Name set but BNoExpand=true → returns ""
		{
			name:      "MCDC_name_set_but_noexpand_returns_empty",
			item:      ExprListItem{Name: "alias", BNoExpand: true},
			wantEmpty: true,
		},
		// C: Name set, BNoExpand=false → returns Name
		{
			name:     "MCDC_name_set_expand_allowed_returns_name",
			item:     ExprListItem{Name: "alias", BNoExpand: false},
			wantName: "alias",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := rc.tryGetExplicitName(&tt.item)
			if tt.wantEmpty && got != "" {
				t.Errorf("tryGetExplicitName() = %q, want empty string", got)
			}
			if !tt.wantEmpty && got != tt.wantName {
				t.Errorf("tryGetExplicitName() = %q, want %q", got, tt.wantName)
			}
		})
	}
}

// TestMCDC_TryGetImplicitName_ColumnRef exercises one compound condition in tryGetImplicitName.
//
// Condition: item.Expr.Op == TK_COLUMN && item.Expr.ColumnRef != nil
//
// MC/DC cases:
//
//	A: Op != TK_COLUMN                   → first sub false → does not use ColumnRef path
//	B: Op == TK_COLUMN, ColumnRef=nil    → first true, second false → falls through
//	C: Op == TK_COLUMN, ColumnRef non-nil → both true → returns ColumnRef.Name
func TestMCDC_TryGetImplicitName_ColumnRef(t *testing.T) {
	t.Parallel()

	p := &Parse{DB: &Database{Name: "test"}}
	rc := NewResultCompiler(p)

	col := &Column{Name: "user_id"}

	tests := []struct {
		name     string
		item     ExprListItem
		wantName string // "" means not from ColumnRef path (may still come from TK_ID)
	}{
		// A: Op is TK_INTEGER → neither ColumnRef nor TK_ID path
		{
			name:     "MCDC_non_column_op_no_column_ref_name",
			item:     ExprListItem{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			wantName: "",
		},
		// B: Op is TK_COLUMN but ColumnRef=nil → falls through to "" (no TK_ID either)
		{
			name:     "MCDC_column_op_nil_columnref_returns_empty",
			item:     ExprListItem{Expr: &Expr{Op: TK_COLUMN, ColumnRef: nil}},
			wantName: "",
		},
		// C: Op is TK_COLUMN with ColumnRef → returns ColumnRef.Name
		{
			name:     "MCDC_column_op_with_columnref_returns_name",
			item:     ExprListItem{Expr: &Expr{Op: TK_COLUMN, ColumnRef: col}},
			wantName: "user_id",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := rc.tryGetImplicitName(&tt.item)
			if got != tt.wantName {
				t.Errorf("tryGetImplicitName() = %q, want %q", got, tt.wantName)
			}
		})
	}
}

// TestMCDC_ResolveDotExpr exercises the compound condition in resolveDotExpr.
//
// Condition: expr.Left != nil && expr.Right != nil
//
// MC/DC cases:
//
//	A: Left=nil               → first sub false → no further resolution attempted
//	B: Left!=nil, Right=nil   → first true, second false → no further resolution
//	C: Left!=nil, Right!=nil  → both true → calls resolveQualifiedColumn
func TestMCDC_ResolveDotExpr(t *testing.T) {
	t.Parallel()

	// Build a minimal table+src so resolveQualifiedColumn can succeed
	tbl := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}
	src := &SrcList{Items: []SrcListItem{{Name: "users", Table: tbl, Cursor: 0}}}

	mkDotExpr := func(left, right *Expr) *Expr {
		return &Expr{Op: TK_DOT, Left: left, Right: right}
	}
	leftID := &Expr{Op: TK_ID, StringValue: "users"}
	rightID := &Expr{Op: TK_ID, StringValue: "id"}

	tests := []struct {
		name    string
		expr    *Expr
		sel     *Select
		wantErr bool // A/B: nil → no error since nothing is done; C: also no error if resolved OK
	}{
		// A: Left=nil → returns nil immediately (no-op, no error)
		{
			name:    "MCDC_nil_left_no_resolution",
			expr:    mkDotExpr(nil, rightID),
			sel:     &Select{Src: src},
			wantErr: false,
		},
		// B: Right=nil → returns nil immediately (no-op, no error)
		{
			name:    "MCDC_nil_right_no_resolution",
			expr:    mkDotExpr(leftID, nil),
			sel:     &Select{Src: src},
			wantErr: false,
		},
		// C: Both non-nil → attempts qualified column resolution (should succeed)
		{
			name:    "MCDC_both_nonnull_resolves_qualified_column",
			expr:    mkDotExpr(leftID, rightID),
			sel:     &Select{Src: src},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Parse{DB: &Database{Name: "test"}}
			rc := NewResultCompiler(p)

			err := rc.resolveDotExpr(tt.sel, tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveDotExpr() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_IsTableStar exercises the compound condition in isTableStar.
//
// Condition: expr.Op == TK_DOT && expr.Right != nil && expr.Right.Op == TK_ASTERISK
//
// MC/DC cases (short-circuit AND):
//
//	A: Op != TK_DOT                          → first sub false → returns false
//	B: Op == TK_DOT, Right=nil               → second sub false → returns false
//	C: Op == TK_DOT, Right!=nil, Right.Op != TK_ASTERISK → third sub false → returns false
//	D: Op == TK_DOT, Right!=nil, Right.Op == TK_ASTERISK → all true → returns true
func TestMCDC_IsTableStar(t *testing.T) {
	t.Parallel()

	p := &Parse{DB: &Database{Name: "test"}}
	rc := NewResultCompiler(p)

	tests := []struct {
		name string
		expr *Expr
		want bool
	}{
		// A: wrong op
		{
			name: "MCDC_non_dot_op_returns_false",
			expr: &Expr{Op: TK_ID, Right: &Expr{Op: TK_ASTERISK}},
			want: false,
		},
		// B: TK_DOT but Right=nil
		{
			name: "MCDC_dot_nil_right_returns_false",
			expr: &Expr{Op: TK_DOT, Right: nil},
			want: false,
		},
		// C: TK_DOT, Right non-nil but not TK_ASTERISK
		{
			name: "MCDC_dot_right_not_asterisk_returns_false",
			expr: &Expr{Op: TK_DOT, Right: &Expr{Op: TK_ID}},
			want: false,
		},
		// D: TK_DOT, Right non-nil, Right.Op == TK_ASTERISK
		{
			name: "MCDC_dot_right_asterisk_returns_true",
			expr: &Expr{Op: TK_DOT, Right: &Expr{Op: TK_ASTERISK}},
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := rc.isTableStar(tt.expr)
			if got != tt.want {
				t.Errorf("isTableStar() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMCDC_AppendValue_ZeroWidthCondition exercises the compound condition in appendValue.
//
// Condition: st == SerialTypeNull || st == SerialTypeZero || st == SerialTypeOne
//
// MC/DC cases:
//
//	A: st == SerialTypeNull  → first sub true → returns buf unchanged (no data appended)
//	B: st == SerialTypeZero  → second sub true → returns buf unchanged
//	C: st == SerialTypeOne   → third sub true → returns buf unchanged
//	D: st == SerialTypeInt8  → all false → data is appended to buf
func TestMCDC_AppendValue_ZeroWidthCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		st         SerialType
		val        Value
		wantAppend bool // true = expect bytes appended to buf
	}{
		// A: SerialTypeNull → no bytes appended
		{
			name:       "MCDC_null_type_no_bytes_appended",
			st:         SerialTypeNull,
			val:        NullValue(),
			wantAppend: false,
		},
		// B: SerialTypeZero → no bytes appended
		{
			name:       "MCDC_zero_type_no_bytes_appended",
			st:         SerialTypeZero,
			val:        IntValue(0),
			wantAppend: false,
		},
		// C: SerialTypeOne → no bytes appended
		{
			name:       "MCDC_one_type_no_bytes_appended",
			st:         SerialTypeOne,
			val:        IntValue(1),
			wantAppend: false,
		},
		// D: SerialTypeInt8 → 1 byte appended (all sub-conditions false)
		{
			name:       "MCDC_int8_type_one_byte_appended",
			st:         SerialTypeInt8,
			val:        IntValue(42),
			wantAppend: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			initial := []byte{0xFF} // sentinel prefix
			result := appendValue(initial, tt.val, tt.st)
			grew := len(result) > len(initial)
			if tt.wantAppend && !grew {
				t.Errorf("appendValue(st=%d) did not append bytes, want append", tt.st)
			}
			if !tt.wantAppend && grew {
				t.Errorf("appendValue(st=%d) appended %d bytes, want none", tt.st, len(result)-len(initial))
			}
		})
	}
}

// TestMCDC_ParseBlobOrText_EvenOddCondition exercises the condition in parseBlobOrText.
//
// Condition: st%2 == 0  (true = BLOB, false = TEXT)
//
// MC/DC cases (single binary condition — two cases required):
//
//	A: st%2 == 0 (even serial type, e.g. 12) → true → returns TypeBlob
//	B: st%2 != 0 (odd serial type,  e.g. 13) → false → returns TypeText
func TestMCDC_ParseBlobOrText_EvenOddCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		st       SerialType
		data     string // content to encode
		wantType ValueType
	}{
		// A: even type (12 = BLOB of 0 bytes, but we need data so use 12+2*n)
		// For 4-byte blob: SerialType = 12 + 2*4 = 20 (even)
		{
			name:     "MCDC_even_serial_type_returns_blob",
			st:       SerialType(20), // (20-12)/2 = 4 bytes, BLOB
			data:     "abcd",
			wantType: TypeBlob,
		},
		// B: odd type (13 = TEXT of 0 bytes; for 4-byte text: 13+2*4 = 21, odd)
		{
			name:     "MCDC_odd_serial_type_returns_text",
			st:       SerialType(21), // (21-13)/2 = 4 bytes, TEXT
			data:     "abcd",
			wantType: TypeText,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			buf := []byte(tt.data)
			val, n, err := parseBlobOrText(buf, 0, tt.st)
			if err != nil {
				t.Fatalf("parseBlobOrText() error: %v", err)
			}
			if n != len(tt.data) {
				t.Errorf("parseBlobOrText() read %d bytes, want %d", n, len(tt.data))
			}
			if val.Type != tt.wantType {
				t.Errorf("parseBlobOrText() type = %v, want %v", val.Type, tt.wantType)
			}
		})
	}
}

// TestMCDC_FindTableInSrc exercises the compound condition in findTableInSrc.
//
// Condition (per-item match): item.Table.Name == tableName || item.Alias == tableName
//
// MC/DC cases:
//
//	A: Table.Name matches         → first sub true → found (flips outcome)
//	B: Table.Name no match, Alias matches  → first false, second true → found (flips)
//	C: Neither Table.Name nor Alias match  → both false → not found
func TestMCDC_FindTableInSrc(t *testing.T) {
	t.Parallel()

	tbl := &Table{Name: "users"}
	srcWithAlias := &SrcList{Items: []SrcListItem{
		{Name: "users", Alias: "u", Table: tbl},
	}}

	tests := []struct {
		name      string
		src       *SrcList
		tableName string
		wantNil   bool
	}{
		// A: match by table name
		{
			name:      "MCDC_match_by_table_name_returns_item",
			src:       srcWithAlias,
			tableName: "users",
			wantNil:   false,
		},
		// B: no table-name match, but alias matches
		{
			name:      "MCDC_match_by_alias_returns_item",
			src:       srcWithAlias,
			tableName: "u",
			wantNil:   false,
		},
		// C: no match at all → returns nil
		{
			name:      "MCDC_no_match_returns_nil",
			src:       srcWithAlias,
			tableName: "orders",
			wantNil:   true,
		},
		// Edge: nil src → returns nil
		{
			name:      "MCDC_nil_src_returns_nil",
			src:       nil,
			tableName: "users",
			wantNil:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := findTableInSrc(tt.src, tt.tableName)
			if tt.wantNil && got != nil {
				t.Errorf("findTableInSrc(%q) = %+v, want nil", tt.tableName, got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("findTableInSrc(%q) = nil, want non-nil", tt.tableName)
			}
		})
	}
}
