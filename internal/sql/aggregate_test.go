package sql

import (
	"testing"
)

// Test NewAggregateCompiler
func TestNewAggregateCompiler(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	if ac == nil {
		t.Fatal("NewAggregateCompiler returned nil")
	}
	if ac.parse != parse {
		t.Error("AggregateCompiler.parse not set correctly")
	}
}

// Test compileGroupBy
func TestCompileGroupBy(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	ac := NewAggregateCompiler(parse)

	table := &Table{
		Name:       "sales",
		RootPage:   1,
		NumColumns: 3,
		Columns: []Column{
			{Name: "product", DeclType: "TEXT"},
			{Name: "quantity", DeclType: "INTEGER"},
			{Name: "price", DeclType: "REAL"},
		},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	// SELECT product, SUM(quantity) FROM sales GROUP BY product
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
				{Expr: &Expr{
					Op: TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "sum"},
					List: &ExprList{
						Items: []ExprListItem{
							{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}},
						},
					},
				}},
			},
		},
		Src: srcList,
		GroupBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
		SelectID: 1,
	}

	dest := &SelectDest{
		Dest: SRT_Output,
	}

	err := ac.compileGroupBy(sel, dest)
	if err != nil {
		t.Fatalf("compileGroupBy failed: %v", err)
	}

	// Verify instructions were generated
	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}

// Test analyzeAggregates
func TestAnalyzeAggregates(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	tests := []struct {
		name            string
		sel             *Select
		wantNumFuncs    int
		wantNumGroupBy  int
	}{
		{
			name: "count",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{
							Op:      TK_AGG_FUNCTION,
							FuncDef: &FuncDef{Name: "count"},
						}},
					},
				},
				SelectID: 1,
			},
			wantNumFuncs:   1,
			wantNumGroupBy: 0,
		},
		{
			name: "sum_with_group_by",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_COLUMN}},
						{Expr: &Expr{
							Op:      TK_AGG_FUNCTION,
							FuncDef: &FuncDef{Name: "sum"},
							List: &ExprList{
								Items: []ExprListItem{
									{Expr: &Expr{Op: TK_COLUMN}},
								},
							},
						}},
					},
				},
				GroupBy: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_COLUMN}},
					},
				},
				SelectID: 1,
			},
			wantNumFuncs:   1,
			wantNumGroupBy: 1,
		},
		{
			name: "multiple_aggregates",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{
							Op:      TK_AGG_FUNCTION,
							FuncDef: &FuncDef{Name: "count"},
						}},
						{Expr: &Expr{
							Op:      TK_AGG_FUNCTION,
							FuncDef: &FuncDef{Name: "sum"},
						}},
						{Expr: &Expr{
							Op:      TK_AGG_FUNCTION,
							FuncDef: &FuncDef{Name: "avg"},
						}},
					},
				},
				SelectID: 1,
			},
			wantNumFuncs:   3,
			wantNumGroupBy: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggInfo, err := ac.analyzeAggregates(tt.sel)
			if err != nil {
				t.Fatalf("analyzeAggregates failed: %v", err)
			}

			if aggInfo.NumAggFuncs != tt.wantNumFuncs {
				t.Errorf("NumAggFuncs = %d, want %d", aggInfo.NumAggFuncs, tt.wantNumFuncs)
			}

			if aggInfo.NumGroupBy != tt.wantNumGroupBy {
				t.Errorf("NumGroupBy = %d, want %d", aggInfo.NumGroupBy, tt.wantNumGroupBy)
			}
		})
	}
}

// Test findAggregateFuncs
func TestFindAggregateFuncs(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	tests := []struct {
		name      string
		expr      *Expr
		wantCount int
	}{
		{
			name: "simple_aggregate",
			expr: &Expr{
				Op:      TK_AGG_FUNCTION,
				FuncDef: &FuncDef{Name: "count"},
			},
			wantCount: 1,
		},
		{
			name: "nested_aggregate",
			expr: &Expr{
				Op: TK_PLUS,
				Left: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "sum"},
				},
				Right: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "count"},
				},
			},
			wantCount: 2,
		},
		{
			name: "aggregate_in_list",
			expr: &Expr{
				Op: TK_FUNCTION,
				List: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{
							Op:      TK_AGG_FUNCTION,
							FuncDef: &FuncDef{Name: "max"},
						}},
					},
				},
			},
			wantCount: 1,
		},
		{
			name:      "no_aggregate",
			expr:      &Expr{Op: TK_COLUMN},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggInfo := &AggInfo{}
			err := ac.findAggregateFuncs(tt.expr, aggInfo)
			if err != nil {
				t.Fatalf("findAggregateFuncs failed: %v", err)
			}

			if len(aggInfo.AggFuncs) != tt.wantCount {
				t.Errorf("Found %d aggregates, want %d", len(aggInfo.AggFuncs), tt.wantCount)
			}
		})
	}
}

// Test initializeAccumulators
func TestInitializeAccumulators(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		expected Opcode
	}{
		{"count", "count", OP_Integer},
		{"count_star", "count(*)", OP_Integer},
		{"sum", "sum", OP_Null},
		{"total", "total", OP_Null},
		{"avg", "avg", OP_Null},
		{"min", "min", OP_Null},
		{"max", "max", OP_Null},
		{"group_concat", "group_concat", OP_String8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			ac := NewAggregateCompiler(parse)

			aggInfo := &AggInfo{
				AggFuncs: []AggFunc{
					{
						Expr:    &Expr{Op: TK_AGG_FUNCTION},
						Func:    &FuncDef{Name: tt.funcName},
						RegAcc:  0,
					},
				},
			}

			ac.initializeAccumulators(aggInfo)

			// Verify register was allocated
			if aggInfo.AggFuncs[0].RegAcc == 0 {
				t.Error("RegAcc should be allocated")
			}

			// Verify appropriate initialization opcode
			if len(parse.Vdbe.Ops) == 0 {
				t.Error("No VDBE instructions generated")
			}

			firstOp := parse.Vdbe.Ops[0]
			if firstOp.Opcode != tt.expected {
				t.Errorf("Expected opcode %v, got %v", tt.expected, firstOp.Opcode)
			}
		})
	}
}

// Test updateCount
func TestUpdateCount(t *testing.T) {
	tests := []struct {
		name   string
		argReg int
	}{
		{"count_star", 0},
		{"count_expr", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			ac := NewAggregateCompiler(parse)

			aggFunc := &AggFunc{
				RegAcc: 10,
			}

			updateCount(ac, aggFunc, tt.argReg)

			// COUNT(*) should have OP_AddImm
			// COUNT(expr) should have OP_IsNull check
			if len(parse.Vdbe.Ops) == 0 {
				t.Error("No VDBE instructions generated")
			}

			hasAddImm := false
			for _, op := range parse.Vdbe.Ops {
				if op.Opcode == OP_AddImm {
					hasAddImm = true
					break
				}
			}

			if !hasAddImm {
				t.Error("Expected OP_AddImm for COUNT")
			}
		})
	}
}

// Test updateSum
func TestUpdateSum(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggFunc := &AggFunc{
		RegAcc: 10,
	}

	updateSum(ac, aggFunc, 5)

	// Should have NULL check and Add operation
	hasIsNull := false
	hasAdd := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_IsNull {
			hasIsNull = true
		}
		if op.Opcode == OP_Add {
			hasAdd = true
		}
	}

	if !hasIsNull {
		t.Error("Expected OP_IsNull for SUM")
	}
	if !hasAdd {
		t.Error("Expected OP_Add for SUM")
	}
}

// Test updateAvg
func TestUpdateAvg(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggFunc := &AggFunc{
		RegAcc: 10,
	}

	updateAvg(ac, aggFunc, 5)

	// Should have NULL check and Add operation
	hasAdd := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Add {
			hasAdd = true
			break
		}
	}

	if !hasAdd {
		t.Error("Expected OP_Add for AVG")
	}
}

// Test updateMin
func TestUpdateMin(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggFunc := &AggFunc{
		RegAcc: 10,
	}

	updateMin(ac, aggFunc, 5)

	// Should have comparison and copy
	hasLt := false
	hasCopy := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Lt {
			hasLt = true
		}
		if op.Opcode == OP_Copy {
			hasCopy = true
		}
	}

	if !hasLt {
		t.Error("Expected OP_Lt for MIN")
	}
	if !hasCopy {
		t.Error("Expected OP_Copy for MIN")
	}
}

// Test updateMax
func TestUpdateMax(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggFunc := &AggFunc{
		RegAcc: 10,
	}

	updateMax(ac, aggFunc, 5)

	// Should have comparison and copy
	hasGt := false
	hasCopy := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Gt {
			hasGt = true
		}
		if op.Opcode == OP_Copy {
			hasCopy = true
		}
	}

	if !hasGt {
		t.Error("Expected OP_Gt for MAX")
	}
	if !hasCopy {
		t.Error("Expected OP_Copy for MAX")
	}
}

// Test updateGroupConcat
func TestUpdateGroupConcat(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggFunc := &AggFunc{
		RegAcc: 10,
	}

	updateGroupConcat(ac, aggFunc, 5)

	// Should have NULL check and Concat operation
	hasConcat := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Concat {
			hasConcat = true
			break
		}
	}

	if !hasConcat {
		t.Error("Expected OP_Concat for GROUP_CONCAT")
	}
}

// Test evalArgReg
func TestEvalArgReg(t *testing.T) {
	tests := []struct {
		name        string
		aggFunc     *AggFunc
		wantReg     bool
	}{
		{
			name: "count_star",
			aggFunc: &AggFunc{
				Expr: &Expr{
					Op:   TK_AGG_FUNCTION,
					List: nil, // COUNT(*)
				},
			},
			wantReg: false,
		},
		{
			name: "count_expr",
			aggFunc: &AggFunc{
				Expr: &Expr{
					Op: TK_AGG_FUNCTION,
					List: &ExprList{
						Items: []ExprListItem{
							{Expr: &Expr{Op: TK_COLUMN}},
						},
					},
				},
			},
			wantReg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			ac := NewAggregateCompiler(parse)

			reg := ac.evalArgReg(tt.aggFunc)

			if tt.wantReg && reg == 0 {
				t.Error("Expected register to be allocated")
			}
			if !tt.wantReg && reg != 0 {
				t.Error("Expected register to be 0")
			}
		})
	}
}

// Test updateAccumulators
func TestUpdateAccumulators(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	sel := &Select{}

	aggInfo := &AggInfo{
		AggFuncs: []AggFunc{
			{
				Expr: &Expr{
					Op: TK_AGG_FUNCTION,
					List: &ExprList{
						Items: []ExprListItem{
							{Expr: &Expr{Op: TK_COLUMN}},
						},
					},
				},
				Func: &FuncDef{Name: "sum"},
			},
		},
	}

	ac.updateAccumulators(sel, aggInfo)

	// Verify instructions were generated
	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}

// Test finalizeAggregates
func TestFinalizeAggregates(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{
		AggFuncs: []AggFunc{
			{
				Expr:   &Expr{Op: TK_AGG_FUNCTION},
				Func:   &FuncDef{Name: "count"},
				RegAcc: 5,
			},
		},
	}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: aggInfo.AggFuncs[0].Expr},
			},
		},
	}

	dest := &SelectDest{
		Dest: SRT_Output,
	}

	ac.finalizeAggregates(sel, aggInfo, dest)

	// Verify ResultRow was generated
	hasResultRow := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_ResultRow {
			hasResultRow = true
			break
		}
	}

	if !hasResultRow {
		t.Error("Expected OP_ResultRow")
	}
}

// Test finalizeAggregates with HAVING
func TestFinalizeAggregatesWithHaving(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{
		AggFuncs: []AggFunc{
			{
				Expr:   &Expr{Op: TK_AGG_FUNCTION},
				Func:   &FuncDef{Name: "count"},
				RegAcc: 5,
			},
		},
	}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: aggInfo.AggFuncs[0].Expr},
			},
		},
		Having: &Expr{Op: TK_INTEGER, IntValue: 1},
	}

	dest := &SelectDest{
		Dest: SRT_Output,
	}

	ac.finalizeAggregates(sel, aggInfo, dest)

	// Verify HAVING check was generated
	hasIfNot := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_IfNot {
			hasIfNot = true
			break
		}
	}

	if !hasIfNot {
		t.Error("Expected OP_IfNot for HAVING clause")
	}
}

// Test outputAggregateRow with different destinations
func TestOutputAggregateRow(t *testing.T) {
	tests := []struct {
		name     string
		destType SelectDestType
		parm     int
	}{
		{"SRT_Output", SRT_Output, 0},
		{"SRT_Table", SRT_Table, 1},
		{"SRT_EphemTab", SRT_EphemTab, 2},
		{"SRT_Mem", SRT_Mem, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			ac := NewAggregateCompiler(parse)

			dest := &SelectDest{
				Dest:   tt.destType,
				SDParm: tt.parm,
			}

			ac.outputAggregateRow(10, 3, dest)

			if len(parse.Vdbe.Ops) == 0 {
				t.Error("No VDBE instructions generated")
			}
		})
	}
}

// Test setupGroupBySorter
func TestSetupGroupBySorter(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		GroupBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
	}

	aggInfo := &AggInfo{}

	addrBreak := ac.setupGroupBySorter(sel, aggInfo)

	if aggInfo.GroupBySort == 0 {
		t.Error("GroupBySort should be allocated")
	}

	if addrBreak == 0 {
		t.Error("addrBreak should be set")
	}

	// Check for OP_OpenEphemeral
	hasOpenEphemeral := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_OpenEphemeral {
			hasOpenEphemeral = true
			break
		}
	}

	if !hasOpenEphemeral {
		t.Error("Expected OP_OpenEphemeral")
	}
}

// Test openSourceTables
func TestOpenSourceTables(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	ac := NewAggregateCompiler(parse)

	table := &Table{
		Name:     "users",
		RootPage: 10,
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{
		Table:  table,
		Cursor: 0,
	})

	sel := &Select{
		Src: srcList,
	}

	addrBreak := parse.Vdbe.MakeLabel()
	ac.openSourceTables(sel, addrBreak)

	// Check for OP_OpenRead and OP_Rewind
	hasOpenRead := false
	hasRewind := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_OpenRead {
			hasOpenRead = true
		}
		if op.Opcode == OP_Rewind {
			hasRewind = true
		}
	}

	if !hasOpenRead {
		t.Error("Expected OP_OpenRead")
	}
	if !hasRewind {
		t.Error("Expected OP_Rewind")
	}
}

// Test compileWhereClause
func TestAggregateCompileWhereClause(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		Where: &Expr{Op: TK_INTEGER, IntValue: 1},
	}

	addrBreak := parse.Vdbe.MakeLabel()
	ac.compileWhereClause(sel, addrBreak)

	// Check for OP_IfNot
	hasIfNot := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_IfNot {
			hasIfNot = true
			break
		}
	}

	if !hasIfNot {
		t.Error("Expected OP_IfNot for WHERE clause")
	}
}

// Test emitNextRow
func TestEmitNextRow(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	ac := NewAggregateCompiler(parse)

	table := &Table{
		Name:     "users",
		RootPage: 1,
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{
		Table:  table,
		Cursor: 5,
	})

	sel := &Select{
		Src: srcList,
	}

	addrLoop := parse.Vdbe.CurrentAddr()
	ac.emitNextRow(sel, addrLoop)

	// Check for OP_Next
	hasNext := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Next {
			hasNext = true
			break
		}
	}

	if !hasNext {
		t.Error("Expected OP_Next")
	}
}

// Test checkNewGroup
func TestCheckNewGroup(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem: 1,
	}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		GroupBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}},
			},
		},
	}

	aggInfo := &AggInfo{}
	continueAddr := parse.Vdbe.MakeLabel()

	ac.checkNewGroup(sel, aggInfo, continueAddr)

	// Check for OP_Ne (comparison)
	hasNe := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Ne {
			hasNe = true
			break
		}
	}

	if !hasNe {
		t.Error("Expected OP_Ne for GROUP BY comparison")
	}
}

// Test finalizeResultExpr
func TestFinalizeResultExpr(t *testing.T) {
	tests := []struct {
		name string
		expr *Expr
	}{
		{
			name: "aggregate_function",
			expr: &Expr{Op: TK_AGG_FUNCTION},
		},
		{
			name: "regular_column",
			expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0},
		},
		{
			name: "integer",
			expr: &Expr{Op: TK_INTEGER, IntValue: 42},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			ac := NewAggregateCompiler(parse)

			aggInfo := &AggInfo{
				AggFuncs: []AggFunc{
					{
						Expr:   &Expr{Op: TK_AGG_FUNCTION},
						Func:   &FuncDef{Name: "count"},
						RegAcc: 10,
					},
				},
			}

			target := 5
			ac.finalizeResultExpr(tt.expr, aggInfo, target)

			// Verify instructions were generated
			if len(parse.Vdbe.Ops) == 0 {
				t.Error("No VDBE instructions generated")
			}
		})
	}
}

// Test compileExpr
func TestAggregateCompileExpr(t *testing.T) {
	tests := []struct {
		name     string
		expr     *Expr
		expected Opcode
	}{
		{"column", &Expr{Op: TK_COLUMN, Table: 0, Column: 1}, OP_Column},
		{"integer", &Expr{Op: TK_INTEGER, IntValue: 42}, OP_Integer},
		{"string", &Expr{Op: TK_STRING, StringValue: "test"}, OP_String8},
		{"null", &Expr{Op: TK_NULL}, OP_Null},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			ac := NewAggregateCompiler(parse)

			target := 5
			ac.compileExpr(tt.expr, target)

			if len(parse.Vdbe.Ops) == 0 {
				t.Fatal("No VDBE instructions generated")
			}

			lastOp := parse.Vdbe.Ops[len(parse.Vdbe.Ops)-1]
			if lastOp.Opcode != tt.expected {
				t.Errorf("Expected opcode %v, got %v", tt.expected, lastOp.Opcode)
			}
		})
	}
}
