package sql

import (
	"testing"
)

// Test NewSelectCompiler
func TestNewSelectCompiler(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
	}
	sc := NewSelectCompiler(parse)
	if sc == nil {
		t.Fatal("NewSelectCompiler returned nil")
	}
	if sc.parse != parse {
		t.Error("SelectCompiler.parse not set correctly")
	}
}

// Test InitSelectDest
func TestInitSelectDest(t *testing.T) {
	tests := []struct {
		name     string
		destType SelectDestType
		parm     int
	}{
		{"output", SRT_Output, 0},
		{"mem", SRT_Mem, 5},
		{"set", SRT_Set, 10},
		{"table", SRT_Table, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest SelectDest
			InitSelectDest(&dest, tt.destType, tt.parm)

			if dest.Dest != tt.destType {
				t.Errorf("Dest = %v, want %v", dest.Dest, tt.destType)
			}
			if dest.SDParm != tt.parm {
				t.Errorf("SDParm = %d, want %d", dest.SDParm, tt.parm)
			}
			if dest.SDParm2 != 0 {
				t.Errorf("SDParm2 = %d, want 0", dest.SDParm2)
			}
			if dest.AffSdst != "" {
				t.Errorf("AffSdst = %q, want empty", dest.AffSdst)
			}
			if dest.Sdst != 0 {
				t.Errorf("Sdst = %d, want 0", dest.Sdst)
			}
			if dest.NSdst != 0 {
				t.Errorf("NSdst = %d, want 0", dest.NSdst)
			}
		})
	}
}

// Test CompileSelect with nil SELECT
func TestCompileSelectNil(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	sc := NewSelectCompiler(parse)
	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(nil, &dest)
	if err == nil {
		t.Error("CompileSelect(nil) should return error")
	}
}

// Test CompileSelect simple SELECT
func TestCompileSelectSimple(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	// Create simple SELECT with column
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 42}},
			},
		},
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect failed: %v", err)
	}

	// Verify VDBE has instructions
	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}

// Test CompileSelect with FROM clause
func TestCompileSelectWithFrom(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	// Create table
	table := &Table{
		Name:       "users",
		RootPage:   1,
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER},
			{Name: "name", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
		},
	}

	// Create SELECT FROM users
	srcList := NewSrcList()
	srcList.Append(SrcListItem{
		Table:  table,
		Cursor: -1,
	})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
		Src:      srcList,
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with FROM failed: %v", err)
	}

	// Check that OpenRead was emitted
	hasOpenRead := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_OpenRead {
			hasOpenRead = true
			break
		}
	}
	if !hasOpenRead {
		t.Error("Expected OP_OpenRead instruction")
	}
}

// Test CompileSelect with WHERE clause
func TestCompileSelectWithWhere(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	table := &Table{
		Name:       "users",
		RootPage:   1,
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER}},
	}

	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
		Src:      srcList,
		Where:    &Expr{Op: TK_INTEGER, IntValue: 1},
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with WHERE failed: %v", err)
	}

	// Check that OP_IfNot was emitted for WHERE clause
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

// Test CompileSelect with DISTINCT
func TestCompileSelectWithDistinct(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		SelFlags: SF_Distinct,
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with DISTINCT failed: %v", err)
	}
}

// Test CompileSelect with ORDER BY
func TestCompileSelectWithOrderBy(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}, SortOrder: SQLITE_SO_ASC},
			},
		},
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with ORDER BY failed: %v", err)
	}

	// Check for sorter operations
	hasSorterOpen := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_SorterOpen {
			hasSorterOpen = true
			break
		}
	}
	if !hasSorterOpen {
		t.Error("Expected OP_SorterOpen for ORDER BY")
	}
}

// Test UNION compound select
func TestCompileSelectUnion(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	// Left side
	left := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		SelectID: 1,
	}

	// Right side with UNION
	right := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 2}},
			},
		},
		Prior:    left,
		Op:       TK_UNION,
		SelectID: 2,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(right, &dest)
	if err != nil {
		t.Fatalf("CompileSelect UNION failed: %v", err)
	}

	// Check for ephemeral table for UNION
	hasOpenEphemeral := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_OpenEphemeral {
			hasOpenEphemeral = true
			break
		}
	}
	if !hasOpenEphemeral {
		t.Error("Expected OP_OpenEphemeral for UNION")
	}
}

// Test INTERSECT compound select
func TestCompileSelectIntersect(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	left := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		SelectID: 1,
	}

	right := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		Prior:    left,
		Op:       TK_INTERSECT,
		SelectID: 2,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(right, &dest)
	if err != nil {
		t.Fatalf("CompileSelect INTERSECT failed: %v", err)
	}

	// Check for ephemeral tables
	ephemeralCount := 0
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_OpenEphemeral {
			ephemeralCount++
		}
	}
	if ephemeralCount < 2 {
		t.Errorf("Expected at least 2 OP_OpenEphemeral for INTERSECT, got %d", ephemeralCount)
	}
}

// Test EXCEPT compound select
func TestCompileSelectExcept(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	left := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		SelectID: 1,
	}

	right := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 2}},
			},
		},
		Prior:    left,
		Op:       TK_EXCEPT,
		SelectID: 2,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(right, &dest)
	if err != nil {
		t.Fatalf("CompileSelect EXCEPT failed: %v", err)
	}
}

// Test disposeResult with different destinations
func TestDisposeResultDestinations(t *testing.T) {
	tests := []struct {
		name     string
		destType SelectDestType
		parm     int
	}{
		{"SRT_Output", SRT_Output, 0},
		{"SRT_Mem", SRT_Mem, 5},
		{"SRT_Set", SRT_Set, 1},
		{"SRT_Union", SRT_Union, 2},
		{"SRT_Except", SRT_Except, 3},
		{"SRT_Table", SRT_Table, 4},
		{"SRT_EphemTab", SRT_EphemTab, 5},
		{"SRT_Exists", SRT_Exists, 6},
		{"SRT_Coroutine", SRT_Coroutine, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem:    1,
				Tabs: 0,
			}
			sc := NewSelectCompiler(parse)

			dest := &SelectDest{
				Dest:   tt.destType,
				SDParm: tt.parm,
				Sdst:   10,
				NSdst:  2,
			}

			err := sc.disposeResult(dest, 2, 10)
			if err != nil {
				t.Errorf("disposeResult failed: %v", err)
			}

			// Verify appropriate opcode was generated
			if len(parse.Vdbe.Ops) == 0 {
				t.Error("No VDBE instructions generated")
			}
		})
	}
}

// Test disposeResult with unsupported destination
func TestDisposeResultUnsupported(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	dest := &SelectDest{
		Dest:  SelectDestType(999), // Invalid
		Sdst:  10,
		NSdst: 2,
	}

	err := sc.disposeResult(dest, 2, 10)
	if err == nil {
		t.Error("disposeResult should fail with unsupported destination")
	}
}

// Test compileExpr with different expression types
func TestCompileExpr(t *testing.T) {
	tests := []struct {
		name     string
		expr     *Expr
		expected Opcode
	}{
		{
			name:     "column",
			expr:     &Expr{Op: TK_COLUMN, Table: 0, Column: 1},
			expected: OP_Column,
		},
		{
			name:     "integer",
			expr:     &Expr{Op: TK_INTEGER, IntValue: 42},
			expected: OP_Integer,
		},
		{
			name:     "string",
			expr:     &Expr{Op: TK_STRING, StringValue: "hello"},
			expected: OP_String8,
		},
		{
			name:     "null",
			expr:     &Expr{Op: TK_NULL},
			expected: OP_Null,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem: 1,
			}
			sc := NewSelectCompiler(parse)

			targetReg := 1
			sc.compileExpr(tt.expr, targetReg)

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

// Test codeDistinct
func TestCodeDistinct(t *testing.T) {
	tests := []struct {
		name       string
		distinctType uint8
	}{
		{"ordered", WHERE_DISTINCT_ORDERED},
		{"unordered", WHERE_DISTINCT_UNORDERED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem:    1,
				Tabs: 0,
			}
			sc := NewSelectCompiler(parse)

			distinct := &DistinctCtx{
				IsTnct:    1,
				ETnctType: tt.distinctType,
				TabTnct:   parse.AllocCursor(),
			}

			sc.codeDistinct(distinct, 2, 10, 100)

			if len(parse.Vdbe.Ops) == 0 {
				t.Error("No VDBE instructions generated for DISTINCT")
			}
		})
	}
}

// Test canUseOrderedDistinct
func TestCanUseOrderedDistinct(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	sc := NewSelectCompiler(parse)

	tests := []struct {
		name     string
		sel      *Select
		expected bool
	}{
		{
			name: "matching_columns",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_INTEGER}},
						{Expr: &Expr{Op: TK_INTEGER}},
					},
				},
				OrderBy: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_INTEGER}},
						{Expr: &Expr{Op: TK_INTEGER}},
					},
				},
			},
			expected: true,
		},
		{
			name: "no_order_by",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_INTEGER}},
					},
				},
				OrderBy: nil,
			},
			expected: false,
		},
		{
			name: "mismatched_columns",
			sel: &Select{
				EList: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_INTEGER}},
					},
				},
				OrderBy: &ExprList{
					Items: []ExprListItem{
						{Expr: &Expr{Op: TK_INTEGER}},
						{Expr: &Expr{Op: TK_INTEGER}},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sc.canUseOrderedDistinct(tt.sel)
			if result != tt.expected {
				t.Errorf("canUseOrderedDistinct = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test SELECT with LIMIT
func TestCompileSelectWithLimit(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		Limit:    10,
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with LIMIT failed: %v", err)
	}
}

// Test SELECT with OFFSET
func TestCompileSelectWithOffset(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		Offset:   5,
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with OFFSET failed: %v", err)
	}
}

// Test SELECT with both LIMIT and OFFSET
func TestCompileSelectWithLimitAndOffset(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:    1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		Limit:    10,
		Offset:   5,
		SelectID: 1,
	}

	var dest SelectDest
	InitSelectDest(&dest, SRT_Output, 0)

	err := sc.CompileSelect(sel, &dest)
	if err != nil {
		t.Fatalf("CompileSelect with LIMIT and OFFSET failed: %v", err)
	}
}

// TestSelectCodeOffset tests generating OFFSET code in SelectCompiler
func TestSelectCodeOffset(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  0,
	}
	sc := NewSelectCompiler(parse)

	tests := []struct {
		name         string
		offset       int
		jumpTo       int
		wantOpsAdded int
	}{
		{
			name:         "offset 5",
			offset:       5,
			jumpTo:       100,
			wantOpsAdded: 2,
		},
		{
			name:         "offset 0",
			offset:       0,
			jumpTo:       100,
			wantOpsAdded: 2,
		},
		{
			name:         "offset 100",
			offset:       100,
			jumpTo:       50,
			wantOpsAdded: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse.Vdbe = NewVdbe(nil)
			parse.Mem = 0
			opsBefore := len(parse.Vdbe.Ops)
			sc.codeOffset(tt.offset, tt.jumpTo)
			opsAfter := len(parse.Vdbe.Ops)
			opsAdded := opsAfter - opsBefore

			if opsAdded != tt.wantOpsAdded {
				t.Errorf("codeOffset() added %d ops, want %d", opsAdded, tt.wantOpsAdded)
			}
		})
	}
}

// TestApplyLimitCheck tests generating LIMIT check code
func TestApplyLimitCheck(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  0,
	}
	sc := NewSelectCompiler(parse)

	tests := []struct {
		name         string
		limit        int
		jumpTo       int
		wantOpsAdded int
	}{
		{
			name:         "limit 10",
			limit:        10,
			jumpTo:       100,
			wantOpsAdded: 2,
		},
		{
			name:         "limit 0",
			limit:        0,
			jumpTo:       100,
			wantOpsAdded: 2,
		},
		{
			name:         "limit 1000",
			limit:        1000,
			jumpTo:       50,
			wantOpsAdded: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse.Vdbe = NewVdbe(nil)
			parse.Mem = 0
			opsBefore := len(parse.Vdbe.Ops)
			sc.applyLimitCheck(tt.limit, tt.jumpTo)
			opsAfter := len(parse.Vdbe.Ops)
			opsAdded := opsAfter - opsBefore

			if opsAdded != tt.wantOpsAdded {
				t.Errorf("applyLimitCheck() added %d ops, want %d", opsAdded, tt.wantOpsAdded)
			}
		})
	}
}

// TestSelectCompilerCompileGroupBy tests the wrapper function
func TestSelectCompilerCompileGroupBy(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	sc := NewSelectCompiler(parse)

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

	err := sc.compileGroupBy(sel, dest)
	if err != nil {
		t.Fatalf("compileGroupBy wrapper failed: %v", err)
	}

	// Verify instructions were generated
	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}
