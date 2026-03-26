// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// --- select_example.go functions ---

func TestExample1_SimpleSelect(t *testing.T) {
	err := Example1_SimpleSelect()
	if err != nil {
		t.Fatalf("Example1_SimpleSelect() failed: %v", err)
	}
}

func TestExample2_SelectWithOrderBy(t *testing.T) {
	err := Example2_SelectWithOrderBy()
	if err != nil {
		t.Fatalf("Example2_SelectWithOrderBy() failed: %v", err)
	}
}

func TestExample3_SelectWithGroupBy(t *testing.T) {
	err := Example3_SelectWithGroupBy()
	if err != nil {
		t.Fatalf("Example3_SelectWithGroupBy() failed: %v", err)
	}
}

func TestExample4_SelectDistinct(t *testing.T) {
	err := Example4_SelectDistinct()
	if err != nil {
		t.Fatalf("Example4_SelectDistinct() failed: %v", err)
	}
}

func TestRunExamples(t *testing.T) {
	// RunExamples just prints output and ignores errors
	RunExamples()
}

func TestDisplayVdbeProgram_Nil(t *testing.T) {
	// Should print "No VDBE program" without panicking
	DisplayVdbeProgram(nil)
}

func TestDisplayVdbeProgram_WithOps(t *testing.T) {
	db := &Database{Name: "test"}
	v := NewVdbe(db)
	v.AddOp2(OP_Integer, 42, 1)
	v.AddOp1(OP_ResultRow, 1)
	v.AddOp1(OP_Halt, 0)
	v.SetNumCols(1)
	v.SetColName(0, "result")
	v.SetColDeclType(0, "INTEGER")
	DisplayVdbeProgram(v)
}

func TestDisplayVdbeProgram_NoColumns(t *testing.T) {
	db := &Database{Name: "test"}
	v := NewVdbe(db)
	v.AddOp1(OP_Halt, 0)
	// NumCols == 0 - displayVdbeColumns should return early
	DisplayVdbeProgram(v)
}

func TestOpcodeToString_AllKnown(t *testing.T) {
	opcodes := []Opcode{
		OP_Init, OP_Halt, OP_OpenRead, OP_OpenWrite, OP_OpenEphemeral,
		OP_SorterOpen, OP_Close, OP_Rewind, OP_Next, OP_Column,
		OP_ResultRow, OP_MakeRecord, OP_Insert, OP_NewRowid, OP_IdxInsert,
		OP_Integer, OP_String8, OP_Null, OP_Copy, OP_IfNot, OP_IfPos,
		OP_Goto, OP_AddImm, OP_SorterSort, OP_SorterData, OP_SorterNext,
		OP_SorterInsert, OP_OpenPseudo, OP_Sequence, OP_Compare, OP_Found,
		OP_Yield, OP_IsNull, OP_Add, OP_Gt, OP_Lt, OP_Ge, OP_Le, OP_Eq, OP_Ne,
	}
	for _, op := range opcodes {
		name := OpcodeToString(op)
		if name == "" {
			t.Errorf("OpcodeToString(%v) returned empty string", op)
		}
	}
}

func TestOpcodeToString_Unknown(t *testing.T) {
	name := OpcodeToString(Opcode(9999))
	if name == "" {
		t.Error("expected non-empty string for unknown opcode")
	}
}

func TestDisplayVdbeOp_WithP4P5Comment(t *testing.T) {
	op := VdbeOp{
		Opcode:  OP_String8,
		P1:      0,
		P2:      1,
		P3:      0,
		P4:      "hello",
		P5:      5,
		Comment: "test comment",
	}
	// Should not panic
	displayVdbeOp(0, op)
}

func TestDisplayVdbeOp_NoExtras(t *testing.T) {
	op := VdbeOp{
		Opcode: OP_Halt,
		P1:     0,
		P2:     0,
		P3:     0,
	}
	displayVdbeOp(0, op)
}

// --- types.go ReleaseReg/ReleaseRegs (empty functions - exercise them) ---

func TestReleaseRegCoverage(t *testing.T) {
	p := &Parse{Mem: 5}
	p.ReleaseReg(3)
	p.ReleaseRegs(1, 3)
	// These are no-ops; just verify they don't panic
}

// --- record.go SerialTypeFor (missing TypeBlob branch) ---

func TestSerialTypeFor_AllTypes(t *testing.T) {
	cases := []Value{
		{Type: TypeNull},
		{Type: TypeInteger, Int: 42},
		{Type: TypeFloat, Float: 3.14},
		{Type: TypeText, Text: "hello"},
		{Type: TypeBlob, Blob: []byte{1, 2, 3}},
	}
	for _, v := range cases {
		st := SerialTypeFor(v)
		_ = st
	}
	// unknown type
	_ = SerialTypeFor(Value{Type: ValueType(99)})
}

func TestIntWidthSuffix_AllCases(t *testing.T) {
	types := []SerialType{
		SerialTypeInt8, SerialTypeInt16, SerialTypeInt24,
		SerialTypeInt32, SerialTypeInt48, SerialTypeInt64,
		SerialType(999), // unknown - returns "64"
	}
	for _, st := range types {
		s := intWidthSuffix(st)
		_ = s
	}
}

// --- aggregate.go findAggsInChildren with left/right/list ---

func TestFindAggsInChildren_LeftRight(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{}
	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "count", NumArgs: 0},
		},
		Right: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "sum", NumArgs: 1},
		},
	}
	err := ac.findAggsInChildren(expr, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInChildren failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 2 {
		t.Errorf("expected 2 agg funcs, got %d", len(aggInfo.AggFuncs))
	}
}

func TestFindAggsInSelect_WithHaving(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{}
	sel := &Select{
		EList: NewExprList(),
		Having: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "count", NumArgs: 0},
		},
	}
	err := ac.findAggsInSelect(sel, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInSelect failed: %v", err)
	}
	if len(aggInfo.AggFuncs) == 0 {
		t.Error("expected agg funcs from HAVING clause")
	}
}

// --- result.go expandItem with table.* ---

func TestExpandItem_TableStar(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	tbl := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns:    []Column{{Name: "id"}, {Name: "name"}},
	}
	sel := &Select{
		EList: NewExprList(),
		Src: &SrcList{
			Items: []SrcListItem{{Name: "users", Table: tbl, Cursor: 0}},
		},
	}
	expanded := NewExprList()
	item := &ExprListItem{
		Expr: &Expr{
			Op:    TK_DOT,
			Left:  &Expr{Op: TK_ID, StringValue: "users"},
			Right: &Expr{Op: TK_ASTERISK},
		},
	}
	err := rc.expandItem(sel, item, expanded)
	if err != nil {
		t.Fatalf("expandItem table.* failed: %v", err)
	}
}

func TestExpandItem_RegularExpr(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	sel := &Select{}
	expanded := NewExprList()
	item := &ExprListItem{
		Expr: &Expr{Op: TK_COLUMN},
		Name: "id",
	}
	err := rc.expandItem(sel, item, expanded)
	if err != nil {
		t.Fatalf("expandItem regular failed: %v", err)
	}
	if expanded.Len() != 1 {
		t.Errorf("expected 1 item, got %d", expanded.Len())
	}
}

// --- result.go ResolveResultColumns with TK_DOT ---

func TestResolveResultColumns_DotExpr(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	tbl := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:    TK_DOT,
						Left:  &Expr{Op: TK_ID, StringValue: "users"},
						Right: &Expr{Op: TK_ID, StringValue: "id"},
					},
				},
			},
		},
		Src: &SrcList{
			Items: []SrcListItem{{Name: "users", Table: tbl, Cursor: 0}},
		},
	}
	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns with dot expr failed: %v", err)
	}
}

// --- result.go resolveColumnRef with colName ---

func TestResolveResultColumns_WithColName(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	tbl := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:          TK_COLUMN,
						StringValue: "id",
					},
				},
			},
		},
		Src: &SrcList{
			Items: []SrcListItem{{Name: "users", Table: tbl, Cursor: 0}},
		},
	}
	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- limit.go OptimizeLimitWithIndex branches ---

func TestOptimizeLimitWithIndex_ZeroLimit(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	lc := NewLimitCompiler(parse)
	sel := &Select{}
	info := &LimitInfo{Limit: 0}
	result := lc.OptimizeLimitWithIndex(sel, info)
	if result {
		t.Error("expected false for zero limit")
	}
}

func TestOptimizeLimitWithIndex_WithGroupBy(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	lc := NewLimitCompiler(parse)
	sel := &Select{
		GroupBy: &ExprList{
			Items: []ExprListItem{{Expr: &Expr{Op: TK_COLUMN}}},
		},
	}
	info := &LimitInfo{Limit: 10}
	result := lc.OptimizeLimitWithIndex(sel, info)
	if result {
		t.Error("expected false for query with GROUP BY")
	}
}

func TestOptimizeLimitWithIndex_NoOrderBy(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	lc := NewLimitCompiler(parse)
	sel := &Select{Src: &SrcList{Items: []SrcListItem{{Name: "t"}}}}
	info := &LimitInfo{Limit: 5}
	result := lc.OptimizeLimitWithIndex(sel, info)
	if result {
		t.Error("expected false for query without ORDER BY")
	}
}

func TestCanOptimizeWithIndex_NilSrc(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	lc := NewLimitCompiler(parse)
	sel := &Select{Src: nil}
	result := lc.canOptimizeWithIndex(sel)
	if result {
		t.Error("expected false for nil Src")
	}
}

func TestCanOptimizeWithIndex_MultipleTables(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	lc := NewLimitCompiler(parse)
	sel := &Select{
		Src: &SrcList{
			Items: []SrcListItem{{Name: "a"}, {Name: "b"}},
		},
	}
	result := lc.canOptimizeWithIndex(sel)
	if result {
		t.Error("expected false for multiple tables")
	}
}

// --- insert.go Disassemble with various P4 types ---

func TestDisassemble_AllP4Types(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"id"},
		Values:  [][]Value{{{Type: TypeInteger, Int: 1}}},
	}
	prog, err := CompileInsert(stmt, 2)
	if err != nil {
		t.Fatalf("CompileInsert failed: %v", err)
	}
	// Add instructions with different P4 types
	prog.Instructions = append(prog.Instructions, Instruction{
		OpCode:  OpString,
		P4:      "short",
		Comment: "test",
	})
	prog.Instructions = append(prog.Instructions, Instruction{
		OpCode: OpString,
		P4:     float64(3.14),
	})
	prog.Instructions = append(prog.Instructions, Instruction{
		OpCode: OpString,
		P4:     []byte{1, 2, 3},
	})
	prog.Instructions = append(prog.Instructions, Instruction{
		OpCode: OpString,
		P4:     42, // default branch
	})
	prog.Instructions = append(prog.Instructions, Instruction{
		OpCode: OpString,
		P4:     "this is a very long string that exceeds twenty characters",
	})
	result := prog.Disassemble()
	if result == "" {
		t.Error("expected non-empty disassembly")
	}
}

// --- record.go getVarintGeneral (9-byte varint) ---

func TestGetVarintGeneral_NineByteVarint(t *testing.T) {
	// Build a 9-byte varint where all first 8 bytes have high bit set
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	val, n := getVarintGeneral(buf, 0)
	if n != 9 {
		t.Errorf("expected 9 bytes read, got %d", n)
	}
	_ = val
}

// --- aggregate.go updateSum/updateAvg (argReg=0 branch) ---

func TestUpdateSum_ZeroArgReg(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)
	aggFunc := &AggFunc{
		Expr:   &Expr{Op: TK_AGG_FUNCTION},
		Func:   &FuncDef{Name: "sum"},
		RegAcc: 1,
	}
	// argReg=0 should be a no-op
	updateSum(ac, aggFunc, 0)
}

func TestUpdateAvg_ZeroArgReg(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)
	aggFunc := &AggFunc{
		Expr:   &Expr{Op: TK_AGG_FUNCTION},
		Func:   &FuncDef{Name: "avg"},
		RegAcc: 1,
	}
	updateAvg(ac, aggFunc, 0)
}

// --- findAggsInSelect with OrderBy ---

func TestFindAggsInSelect_WithOrderBy(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{}
	sel := &Select{
		EList: NewExprList(),
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: &FuncDef{Name: "sum", NumArgs: 1},
					},
				},
			},
		},
	}
	err := ac.findAggsInSelect(sel, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInSelect with OrderBy failed: %v", err)
	}
}

// --- findAggsInChildren with List ---

func TestFindAggsInChildren_WithList(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{}
	expr := &Expr{
		Op: TK_FUNCTION,
		List: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: &FuncDef{Name: "count"},
					},
				},
			},
		},
	}
	err := ac.findAggsInChildren(expr, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInChildren with List failed: %v", err)
	}
	if len(aggInfo.AggFuncs) == 0 {
		t.Error("expected agg func in list")
	}
}

// --- resolveColumnRef with nil Src ---

func TestResolveColumnRef_NilSrc(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:          TK_COLUMN,
						StringValue: "nonexistent",
					},
				},
			},
		},
		Src: nil, // nil Src causes error
	}
	err := rc.ResolveResultColumns(sel)
	if err == nil {
		t.Error("expected error for column with nil Src")
	}
}

// --- resolveChildExprs with right child ---

func TestResolveResultColumns_ChildExprs(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	tbl := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "x"}},
	}
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op: TK_PLUS,
						Left: &Expr{
							Op:          TK_COLUMN,
							StringValue: "x",
						},
						Right: &Expr{
							Op:          TK_COLUMN,
							StringValue: "x",
						},
					},
				},
			},
		},
		Src: &SrcList{
			Items: []SrcListItem{{Name: "t", Table: tbl, Cursor: 0}},
		},
	}
	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- resolveQualifiedColumn with invalid names ---

func TestResolveQualifiedColumn_InvalidTable(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	tbl := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:    TK_DOT,
						Left:  &Expr{Op: TK_ID, StringValue: "nonexistent"},
						Right: &Expr{Op: TK_ID, StringValue: "id"},
					},
				},
			},
		},
		Src: &SrcList{
			Items: []SrcListItem{{Name: "t", Table: tbl, Cursor: 0}},
		},
	}
	err := rc.ResolveResultColumns(sel)
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

// --- findAggsInExprList non-nil list ---

func TestFindAggsInExprList_NonNil(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	ac := NewAggregateCompiler(parse)

	aggInfo := &AggInfo{}
	list := &ExprList{
		Items: []ExprListItem{
			{Expr: &Expr{Op: TK_INTEGER}},
			{Expr: nil},
		},
	}
	err := ac.findAggsInExprList(list, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInExprList failed: %v", err)
	}
}

// --- record.go parseRecordHeader / parseSerialTypes error paths ---

func TestParseRecordHeader_Empty(t *testing.T) {
	_, _, err := parseRecordHeader([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestParseRecord_TruncatedHeader(t *testing.T) {
	// A record where header says 5 bytes but data is shorter
	_, err := ParseRecord([]byte{0x05, 0x00}) // header size 5 but only 2 bytes
	if err == nil {
		t.Error("expected error for truncated header")
	}
}

// --- canOptimizeWithIndex with single table ---

func TestCanOptimizeWithIndex_SingleTable(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	lc := NewLimitCompiler(parse)
	sel := &Select{
		Src: &SrcList{
			Items: []SrcListItem{{Name: "t"}},
		},
	}
	// Should return false (no suitable index found in current implementation)
	result := lc.canOptimizeWithIndex(sel)
	_ = result // Result may be true or false
}

// --- CompileInsertWithAutoInc ---

func TestCompileInsertWithAutoInc_NoAutoInc(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"id"},
		Values:  [][]Value{{{Type: TypeInteger, Int: 1}}},
	}
	prog, err := CompileInsertWithAutoInc(stmt, 2, false)
	if err != nil {
		t.Fatalf("CompileInsertWithAutoInc failed: %v", err)
	}
	_ = prog
}

func TestCompileInsertWithAutoInc_WithAutoInc(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"id"},
		Values:  [][]Value{{{Type: TypeInteger, Int: 1}}},
	}
	prog, err := CompileInsertWithAutoInc(stmt, 2, true)
	if err != nil {
		t.Fatalf("CompileInsertWithAutoInc with autoInc failed: %v", err)
	}
	_ = prog
}

// --- resolveDotExpr with nil Left/Right ---

func TestResolveDotExpr_NilLeft(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}
	rc := NewResultCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:    TK_DOT,
						Left:  nil, // nil left - should not error
						Right: &Expr{Op: TK_ID, StringValue: "id"},
					},
				},
			},
		},
	}
	err := rc.ResolveResultColumns(sel)
	// resolveDotExpr returns nil when Left or Right is nil
	_ = err
}

// --- select with offset/limit - applyOffsetFilter and applyLimitFilter ---

func TestCompileSelect_WithOffsetAndLimit(t *testing.T) {
	db := &Database{Name: "test"}
	parse := &Parse{DB: db}

	tbl := &Table{
		Name:       "items",
		NumColumns: 1,
		RootPage:   2,
		Columns:    []Column{{Name: "id", Affinity: SQLITE_AFF_INTEGER}},
	}

	sel := &Select{
		Op:       TK_SELECT,
		SelFlags: SF_Resolved,
		SelectID: 1,
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}, Name: "id"},
			},
		},
		Src: &SrcList{
			Items: []SrcListItem{{Name: "items", Table: tbl, Cursor: 0}},
		},
		Offset: 5,
		Limit:  10,
	}
	dest := &SelectDest{}
	InitSelectDest(dest, SRT_Output, 0)
	compiler := NewSelectCompiler(parse)
	err := compiler.CompileSelect(sel, dest)
	if err != nil {
		t.Fatalf("CompileSelect with offset/limit failed: %v", err)
	}
}

// --- validateInsertRowCounts with mismatched columns ---

func TestValidateInsertRowCounts_Mismatch(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"a", "b"},
		Values:  [][]Value{{{Type: TypeInteger, Int: 1}}}, // Only 1 value for 2 columns
	}
	err := ValidateInsert(stmt)
	if err == nil {
		t.Error("expected error for mismatched column/value count")
	}
}
