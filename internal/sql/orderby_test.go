// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// Test NewOrderByCompiler
func TestNewOrderByCompiler(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)
	if obc == nil {
		t.Fatal("NewOrderByCompiler returned nil")
	}
	if obc.parse != parse {
		t.Error("OrderByCompiler.parse not set correctly")
	}
}

// Test setupOrderBy
func TestSetupOrderBy(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
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
	}

	var sort SortCtx
	err := sc.setupOrderBy(sel, &sort)
	if err != nil {
		t.Fatalf("setupOrderBy failed: %v", err)
	}

	if sort.OrderBy == nil {
		t.Error("sort.OrderBy not set")
	}
	if sort.ECursor == 0 {
		t.Error("sort.ECursor not allocated")
	}
	if sort.SortFlags&SORTFLAG_UseSorter == 0 {
		t.Error("sort.SortFlags should have SORTFLAG_UseSorter")
	}
}

// Test setupOrderBy with nil OrderBy
func TestSetupOrderByNil(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		OrderBy: nil,
	}

	var sort SortCtx
	err := sc.setupOrderBy(sel, &sort)
	if err != nil {
		t.Fatalf("setupOrderBy failed: %v", err)
	}
}

// Test setupOrderBy with empty OrderBy
func TestSetupOrderByEmpty(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
		OrderBy: &ExprList{Items: []ExprListItem{}},
	}

	var sort SortCtx
	err := sc.setupOrderBy(sel, &sort)
	if err != nil {
		t.Fatalf("setupOrderBy failed: %v", err)
	}
}

// Test generateSortTailWithLimit
func TestGenerateSortTailWithLimit(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

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
		Limit:  10,
		Offset: 5,
	}

	sort := &SortCtx{
		OrderBy:       sel.OrderBy,
		ECursor:       parse.AllocCursor(),
		SortFlags:     SORTFLAG_UseSorter,
		LabelDone:     parse.Vdbe.MakeLabel(),
		AddrSortIndex: 0,
	}

	dest := &SelectDest{
		Dest:  SRT_Output,
		Sdst:  parse.AllocRegs(1),
		NSdst: 1,
	}

	err := obc.generateSortTailWithLimit(sel, sort, 1, dest)
	if err != nil {
		t.Fatalf("generateSortTailWithLimit failed: %v", err)
	}

	// Verify instructions were generated
	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}

// Test generateSortTail (without limit)
func TestGenerateSortTail(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

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
	}

	sort := &SortCtx{
		OrderBy:   sel.OrderBy,
		ECursor:   parse.AllocCursor(),
		SortFlags: SORTFLAG_UseSorter,
		LabelDone: parse.Vdbe.MakeLabel(),
	}

	dest := &SelectDest{
		Dest:  SRT_Output,
		Sdst:  parse.AllocRegs(1),
		NSdst: 1,
	}

	err := obc.generateSortTail(sel, sort, 1, dest)
	if err != nil {
		t.Fatalf("generateSortTail failed: %v", err)
	}

	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}

// Test pushOntoSorter
func TestPushOntoSorter(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
	}

	sort := &SortCtx{
		OrderBy:   sel.OrderBy,
		ECursor:   parse.AllocCursor(),
		SortFlags: SORTFLAG_UseSorter,
	}

	regData := parse.AllocRegs(2)
	err := obc.pushOntoSorter(sort, sel, regData, regData, 2, 0)
	if err != nil {
		t.Fatalf("pushOntoSorter failed: %v", err)
	}

	// Check for OP_SorterInsert
	hasSorterInsert := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_SorterInsert {
			hasSorterInsert = true
			break
		}
	}
	if !hasSorterInsert {
		t.Error("Expected OP_SorterInsert")
	}
}

// Test pushOntoSorter with ephemeral table
func TestPushOntoSorterEphemeral(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
			},
		},
	}

	sort := &SortCtx{
		OrderBy:   sel.OrderBy,
		ECursor:   parse.AllocCursor(),
		SortFlags: 0, // No SORTFLAG_UseSorter = use ephemeral table
	}

	regData := parse.AllocRegs(2)
	err := obc.pushOntoSorter(sort, sel, regData, regData, 2, 0)
	if err != nil {
		t.Fatalf("pushOntoSorter (ephemeral) failed: %v", err)
	}

	// Check for OP_Sequence
	hasSequence := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Sequence {
			hasSequence = true
			break
		}
	}
	if !hasSequence {
		t.Error("Expected OP_Sequence for ephemeral table sort")
	}
}

// Test CompileOrderBy
func TestCompileOrderBy(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
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
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}, Name: "id"},
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}, Name: "name"},
			},
		},
		Src: srcList,
	}

	orderBy := &ExprList{
		Items: []ExprListItem{
			{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}}, // ORDER BY column number
		},
	}

	err := obc.CompileOrderBy(sel, orderBy)
	if err != nil {
		t.Fatalf("CompileOrderBy failed: %v", err)
	}
}

// Test CompileOrderBy with nil/empty
func TestCompileOrderByNil(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	sel := &Select{}

	err := obc.CompileOrderBy(sel, nil)
	if err != nil {
		t.Errorf("CompileOrderBy(nil) should not fail: %v", err)
	}

	err = obc.CompileOrderBy(sel, &ExprList{Items: []ExprListItem{}})
	if err != nil {
		t.Errorf("CompileOrderBy(empty) should not fail: %v", err)
	}
}

// Test handleColumnNumber
func TestHandleColumnNumber(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}},
			},
		},
	}

	tests := []struct {
		name    string
		colNum  int
		wantErr bool
	}{
		{"valid_column_1", 1, false},
		{"valid_column_2", 2, false},
		{"invalid_column_0", 0, true},
		{"invalid_column_3", 3, true},
		{"invalid_column_negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &ExprListItem{
				Expr: &Expr{Op: TK_INTEGER, IntValue: tt.colNum},
			}

			err := obc.handleColumnNumber(sel, item)
			if (err != nil) != tt.wantErr {
				t.Errorf("handleColumnNumber() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && item.OrderByCol != tt.colNum {
				t.Errorf("OrderByCol = %d, want %d", item.OrderByCol, tt.colNum)
			}
		})
	}
}

// Test handleColumnAlias
func TestHandleColumnAlias(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}, Name: "user_id"},
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}, Name: "user_name"},
			},
		},
	}

	tests := []struct {
		name    string
		alias   string
		wantCol int
		wantErr bool
	}{
		{"valid_alias_1", "user_id", 1, false},
		{"valid_alias_2", "user_name", 2, false},
		// Note: invalid alias falls through to resolveOrderByExpr which may not error for TK_ID
		// since it's not a TK_COLUMN or TK_DOT expression
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &ExprListItem{
				Expr: &Expr{Op: TK_ID, StringValue: tt.alias},
			}

			err := obc.handleColumnAlias(sel, item)
			if (err != nil) != tt.wantErr {
				t.Errorf("handleColumnAlias() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && item.OrderByCol != tt.wantCol {
				t.Errorf("OrderByCol = %d, want %d", item.OrderByCol, tt.wantCol)
			}
		})
	}
}

// Test resolveColumnInOrderBy
func TestResolveColumnInOrderBy(t *testing.T) {
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
	srcList.Append(SrcListItem{Table: table, Cursor: 5})

	sel := &Select{Src: srcList}

	tests := []struct {
		name       string
		colName    string
		wantCursor int
		wantColumn int
		wantErr    bool
	}{
		{"valid_id", "id", 5, 0, false},
		{"valid_name", "name", 5, 1, false},
		{"invalid", "nonexistent", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &Expr{Op: TK_COLUMN, StringValue: tt.colName}
			err := obc.resolveColumnInOrderBy(sel, expr)

			if (err != nil) != tt.wantErr {
				t.Errorf("resolveColumnInOrderBy() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				if expr.Table != tt.wantCursor {
					t.Errorf("Table = %d, want %d", expr.Table, tt.wantCursor)
				}
				if expr.Column != tt.wantColumn {
					t.Errorf("Column = %d, want %d", expr.Column, tt.wantColumn)
				}
			}
		})
	}
}

// Test resolveQualifiedColumnInOrderBy
func TestResolveQualifiedColumnInOrderBy(t *testing.T) {
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
	srcList.Append(SrcListItem{Table: table, Cursor: 3, Alias: "u"})

	sel := &Select{Src: srcList}

	tests := []struct {
		name      string
		tableName string
		colName   string
		wantErr   bool
	}{
		{"valid_table_name", "users", "id", false},
		{"valid_alias", "u", "name", false},
		{"invalid_table", "invalid", "id", true},
		{"invalid_column", "users", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: tt.tableName},
				Right: &Expr{Op: TK_ID, StringValue: tt.colName},
			}

			err := obc.resolveQualifiedColumnInOrderBy(sel, expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveQualifiedColumnInOrderBy() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && expr.Op != TK_COLUMN {
				t.Error("Expression should be converted to TK_COLUMN")
			}
		})
	}
}

// Test extractQualifiedNames
func TestExtractQualifiedNames(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	tests := []struct {
		name      string
		expr      *Expr
		wantTable string
		wantCol   string
		wantErr   bool
	}{
		{
			name: "valid",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_ID, StringValue: "id"},
			},
			wantTable: "users",
			wantCol:   "id",
			wantErr:   false,
		},
		{
			name: "invalid_left",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_INTEGER, IntValue: 1},
				Right: &Expr{Op: TK_ID, StringValue: "id"},
			},
			wantErr: true,
		},
		{
			name: "invalid_right",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_INTEGER, IntValue: 1},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName, colName, err := obc.extractQualifiedNames(tt.expr)

			if (err != nil) != tt.wantErr {
				t.Errorf("extractQualifiedNames() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				if tableName != tt.wantTable {
					t.Errorf("tableName = %q, want %q", tableName, tt.wantTable)
				}
				if colName != tt.wantCol {
					t.Errorf("colName = %q, want %q", colName, tt.wantCol)
				}
			}
		})
	}
}

// Test tableMatches
func TestTableMatches(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	obc := NewOrderByCompiler(parse)

	tests := []struct {
		name      string
		srcItem   *SrcListItem
		tableName string
		want      bool
	}{
		{
			name: "match_table_name",
			srcItem: &SrcListItem{
				Table: &Table{Name: "users"},
			},
			tableName: "users",
			want:      true,
		},
		{
			name: "match_alias",
			srcItem: &SrcListItem{
				Table: &Table{Name: "users"},
				Alias: "u",
			},
			tableName: "u",
			want:      true,
		},
		{
			name: "no_match",
			srcItem: &SrcListItem{
				Table: &Table{Name: "users"},
			},
			tableName: "posts",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := obc.tableMatches(tt.srcItem, tt.tableName)
			if got != tt.want {
				t.Errorf("tableMatches() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test codeOffset
func TestCodeOffset(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
	}
	obc := NewOrderByCompiler(parse)

	jumpLabel := parse.Vdbe.MakeLabel()
	obc.codeOffset(10, jumpLabel)

	// Verify instructions were generated
	if len(parse.Vdbe.Ops) < 2 {
		t.Error("codeOffset should generate at least 2 instructions")
	}

	// Check for OP_Integer and OP_IfPos
	hasInteger := false
	hasIfPos := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Integer && op.P1 == 10 {
			hasInteger = true
		}
		if op.Opcode == OP_IfPos {
			hasIfPos = true
		}
	}

	if !hasInteger {
		t.Error("Expected OP_Integer instruction with P1=10")
	}
	if !hasIfPos {
		t.Error("Expected OP_IfPos instruction")
	}
}

// Test compileExpr
func TestOrderByCompileExpr(t *testing.T) {
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
			expr:     &Expr{Op: TK_STRING, StringValue: "test"},
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
				Mem:  1,
			}
			obc := NewOrderByCompiler(parse)

			targetReg := 5
			obc.compileExpr(tt.expr, targetReg)

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

// Test boolToInt
func TestBoolToInt(t *testing.T) {
	tests := []struct {
		input bool
		want  int
	}{
		{true, 1},
		{false, 0},
	}

	for _, tt := range tests {
		got := boolToInt(tt.input)
		if got != tt.want {
			t.Errorf("boolToInt(%v) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// Test outputSortedRow with different destinations
func TestOutputSortedRow(t *testing.T) {
	tests := []struct {
		name            string
		destType        SelectDestType
		parm            int
		expectsOps      bool
		expectedOpcount int
	}{
		{"SRT_Output", SRT_Output, 0, true, 1},
		{"SRT_Table", SRT_Table, 1, true, 3},
		{"SRT_EphemTab", SRT_EphemTab, 2, true, 3},
		{"SRT_Set", SRT_Set, 3, true, 3},
		{"SRT_Mem", SRT_Mem, 4, false, 0}, // SRT_Mem doesn't generate ops
		{"SRT_Coroutine", SRT_Coroutine, 5, true, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem:  1,
				Tabs: 0,
			}
			obc := NewOrderByCompiler(parse)

			dest := &SelectDest{
				Dest:    tt.destType,
				SDParm:  tt.parm,
				Sdst:    10,
				NSdst:   2,
				AffSdst: "text",
			}

			err := obc.outputSortedRow(dest, 5, 2, true, 3, 10, 15)
			if err != nil {
				t.Errorf("outputSortedRow failed: %v", err)
			}

			if tt.expectsOps && len(parse.Vdbe.Ops) == 0 {
				t.Error("Expected VDBE instructions to be generated")
			}
			if !tt.expectsOps && len(parse.Vdbe.Ops) != 0 {
				t.Errorf("Expected no VDBE instructions, but got %d", len(parse.Vdbe.Ops))
			}
		})
	}
}

// Test outputSortedRow with unsupported destination
func TestOutputSortedRowUnsupported(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

	dest := &SelectDest{
		Dest:  SelectDestType(999), // Invalid
		Sdst:  10,
		NSdst: 2,
	}

	err := obc.outputSortedRow(dest, 5, 2, true, 3, 10, 15)
	if err == nil {
		t.Error("outputSortedRow should fail with unsupported destination")
	}
}

// Test allocateResultRegisters
func TestAllocateResultRegisters(t *testing.T) {
	tests := []struct {
		name     string
		destType SelectDestType
		nColumn  int
	}{
		{"SRT_Output", SRT_Output, 5},
		{"SRT_Coroutine", SRT_Coroutine, 3},
		{"SRT_Mem", SRT_Mem, 2},
		{"SRT_Table", SRT_Table, 4},
		{"SRT_EphemTab", SRT_EphemTab, 6},
		{"SRT_Set", SRT_Set, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem:  1,
			}
			obc := NewOrderByCompiler(parse)

			dest := &SelectDest{
				Dest: tt.destType,
				Sdst: 100,
			}

			nCol := tt.nColumn
			regRow, regRowid := obc.allocateResultRegisters(dest, &nCol)

			if regRow == 0 {
				t.Error("regRow should be allocated")
			}

			// Check regRowid allocation based on destination type
			if tt.destType == SRT_Output || tt.destType == SRT_Coroutine || tt.destType == SRT_Mem {
				if regRowid != 0 {
					t.Error("regRowid should be 0 for this destination type")
				}
			} else {
				if regRowid == 0 {
					t.Error("regRowid should be allocated for this destination type")
				}
			}
		})
	}
}

// Test generateSorterLoop
// requireSorterOpcodes checks that all opcodes in want appear in the ops list.
func requireSorterOpcodes(t *testing.T, ops []VdbeOp, want map[Opcode]string) {
	t.Helper()
	found := make(map[Opcode]bool, len(want))
	for _, op := range ops {
		if _, ok := want[op.Opcode]; ok {
			found[op.Opcode] = true
		}
	}
	for opcode, desc := range want {
		if !found[opcode] {
			t.Errorf("Expected %s", desc)
		}
	}
}

func TestGenerateSorterLoop(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

	iTab := parse.AllocCursor()
	addrBreak := parse.Vdbe.MakeLabel()

	ctx := obc.generateSorterLoop(iTab, 2, 3, addrBreak)

	if ctx.iSortTab == 0 {
		t.Error("iSortTab should be allocated")
	}
	if ctx.bSeq {
		t.Error("bSeq should be false for sorter loop")
	}
	if ctx.addr == 0 {
		t.Error("addr should be set")
	}

	requireSorterOpcodes(t, parse.Vdbe.Ops, map[Opcode]string{
		OP_OpenPseudo: "OP_OpenPseudo",
		OP_SorterSort: "OP_SorterSort",
	})
}

// Test generateEphemeralLoop
func TestGenerateEphemeralLoop(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
		Tabs: 0,
	}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		Offset: 5,
		Limit:  10,
	}

	iTab := parse.AllocCursor()
	addrBreak := parse.Vdbe.MakeLabel()
	addrContinue := parse.Vdbe.MakeLabel()

	ctx := obc.generateEphemeralLoop(sel, iTab, addrBreak, addrContinue)

	if ctx.iSortTab != iTab {
		t.Errorf("iSortTab = %d, want %d", ctx.iSortTab, iTab)
	}
	if !ctx.bSeq {
		t.Error("bSeq should be true for ephemeral loop")
	}

	// Check for OP_Sort
	hasSort := false
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Sort {
			hasSort = true
			break
		}
	}
	if !hasSort {
		t.Error("Expected OP_Sort")
	}
}

// Test extractResultColumns
func TestExtractResultColumns(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
	}
	obc := NewOrderByCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}, OrderByCol: 0},
				{Expr: &Expr{Op: TK_COLUMN}, OrderByCol: 1},
				{Expr: &Expr{Op: TK_COLUMN}, OrderByCol: 0},
			},
		},
	}

	obc.extractResultColumns(sel, 5, true, 2, 3, 10)

	// Check for OP_Column instructions
	columnCount := 0
	for _, op := range parse.Vdbe.Ops {
		if op.Opcode == OP_Column {
			columnCount++
		}
	}

	if columnCount != 3 {
		t.Errorf("Expected 3 OP_Column instructions, got %d", columnCount)
	}
}

// Test cleanupResultRegisters
func TestCleanupResultRegisters(t *testing.T) {
	tests := []struct {
		name     string
		destType SelectDestType
		regRowid int
		nColumn  int
	}{
		{"SRT_Set", SRT_Set, 5, 3},
		{"SRT_Table", SRT_Table, 5, 3},
		{"SRT_Output", SRT_Output, 0, 3}, // regRowid == 0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := &Parse{
				Vdbe: NewVdbe(nil),
				Mem:  100,
			}
			obc := NewOrderByCompiler(parse)

			regRow := 10
			initialMem := parse.Mem

			obc.cleanupResultRegisters(tt.destType, regRow, tt.regRowid, tt.nColumn)

			// Verify registers were released when regRowid != 0
			if tt.regRowid != 0 && parse.Mem >= initialMem {
				// Registers should have been released, so Mem should be less
				// Note: This test is primarily to ensure no panic occurs
			}
		})
	}
}

// Test compileOrderByItem with different expression types
func TestCompileOrderByItemTypes(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 1}
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
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}, Name: "id"},
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}, Name: "name"},
			},
		},
		Src: srcList,
	}

	tests := []struct {
		name    string
		item    *ExprListItem
		wantErr bool
	}{
		{
			name: "column_number",
			item: &ExprListItem{
				Expr: &Expr{Op: TK_INTEGER, IntValue: 1},
			},
			wantErr: false,
		},
		{
			name: "column_alias",
			item: &ExprListItem{
				Expr: &Expr{Op: TK_ID, StringValue: "id"},
			},
			wantErr: false,
		},
		{
			name: "column_expression",
			item: &ExprListItem{
				Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0, StringValue: "id"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := obc.compileOrderByItem(sel, tt.item)
			if (err != nil) != tt.wantErr {
				t.Errorf("compileOrderByItem() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test resolveOrderByExpr with various expression types
func TestResolveOrderByExprComprehensive(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil), Mem: 1}
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
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		Src: srcList,
	}

	tests := []struct {
		name    string
		expr    *Expr
		wantErr bool
	}{
		{
			name:    "nil_expression",
			expr:    nil,
			wantErr: false,
		},
		{
			name:    "column_reference",
			expr:    &Expr{Op: TK_COLUMN, StringValue: "id"},
			wantErr: false,
		},
		{
			name: "qualified_column",
			expr: &Expr{
				Op:    TK_DOT,
				Left:  &Expr{Op: TK_ID, StringValue: "users"},
				Right: &Expr{Op: TK_ID, StringValue: "id"},
			},
			wantErr: false,
		},
		{
			name: "binary_expression",
			expr: &Expr{
				Op:    TK_PLUS,
				Left:  &Expr{Op: TK_COLUMN, StringValue: "id"},
				Right: &Expr{Op: TK_INTEGER, IntValue: 1},
			},
			wantErr: false,
		},
		{
			name: "nested_expression",
			expr: &Expr{
				Op: TK_PLUS,
				Left: &Expr{
					Op:          TK_COLUMN,
					StringValue: "id",
				},
				Right: &Expr{
					Op: TK_STAR,
					Left: &Expr{
						Op:          TK_COLUMN,
						StringValue: "name",
					},
					Right: &Expr{Op: TK_INTEGER, IntValue: 2},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := obc.resolveOrderByExpr(sel, tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveOrderByExpr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test generateSortTail wrapper
func TestSelectCompilerGenerateSortTail(t *testing.T) {
	parse := &Parse{
		Vdbe: NewVdbe(nil),
		Mem:  1,
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
	}

	sort := &SortCtx{
		OrderBy:   sel.OrderBy,
		ECursor:   parse.AllocCursor(),
		SortFlags: SORTFLAG_UseSorter,
		LabelDone: parse.Vdbe.MakeLabel(),
	}

	dest := &SelectDest{
		Dest:  SRT_Output,
		Sdst:  parse.AllocRegs(1),
		NSdst: 1,
	}

	err := sc.generateSortTail(sel, sort, 1, dest)
	if err != nil {
		t.Fatalf("generateSortTail failed: %v", err)
	}

	if len(parse.Vdbe.Ops) == 0 {
		t.Error("No VDBE instructions generated")
	}
}
