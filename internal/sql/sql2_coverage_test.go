// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// --- types.go: ReleaseReg and ReleaseRegs (0.0%) ---

// TestReleaseReg covers the no-op ReleaseReg function.
func TestReleaseReg(t *testing.T) {
	p := &Parse{Mem: 5}
	p.ReleaseReg(3) // Should not panic or change state
	if p.Mem != 5 {
		t.Errorf("expected Mem=5 after ReleaseReg, got %d", p.Mem)
	}
}

// TestReleaseRegs covers the no-op ReleaseRegs function.
func TestReleaseRegs(t *testing.T) {
	p := &Parse{Mem: 10}
	p.ReleaseRegs(3, 4) // Should not panic or change state
	if p.Mem != 10 {
		t.Errorf("expected Mem=10 after ReleaseRegs, got %d", p.Mem)
	}
}

// --- aggregate.go: findAggsInChildren (71.4%) ---

// TestFindAggsInChildren_LeftAndRight covers left+right child recursion.
func TestFindAggsInChildren_LeftAndRight(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	// Expr with left and right children
	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "sum"},
			List: &ExprList{
				Items: []ExprListItem{
					{Expr: &Expr{Op: TK_INTEGER}},
				},
			},
		},
		Right: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "count"},
		},
	}

	err := ac.findAggregateFuncs(expr, aggInfo)
	if err != nil {
		t.Fatalf("findAggregateFuncs failed: %v", err)
	}
	// Should find at least 2 aggregate functions
	if len(aggInfo.AggFuncs) < 2 {
		t.Errorf("expected >=2 agg funcs, got %d", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInChildren_WithList covers ExprList child recursion.
func TestFindAggsInChildren_WithList2(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	// Expr with a List containing an aggregate
	expr := &Expr{
		Op: TK_INTEGER,
		List: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "max"},
				}},
			},
		},
	}

	err := ac.findAggregateFuncs(expr, aggInfo)
	if err != nil {
		t.Fatalf("findAggregateFuncs with List failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func from List, got %d", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInSelect_WithHaving covers the HAVING clause path.
func TestFindAggsInSelect_WithHaving2(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER}},
			},
		},
		Having: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "count"},
		},
	}

	err := ac.findAggsInSelect(sel, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInSelect with Having failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func from Having, got %d", len(aggInfo.AggFuncs))
	}
}

// --- result.go: resolveChildExprs (71.4%) ---

// TestResolveChildExprs_BothChildren covers left and right child resolution.
func TestResolveChildExprs_BothChildren(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 2,
		Columns: []Column{
			{Name: "a", DeclType: "INTEGER"},
			{Name: "b", DeclType: "TEXT"},
		},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op: TK_PLUS,
					Left: &Expr{
						Op:          TK_COLUMN,
						StringValue: "a",
					},
					Right: &Expr{
						Op:          TK_COLUMN,
						StringValue: "b",
					},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns with binary expr failed: %v", err)
	}
}

// --- result.go: resolveColumnRef (75.0%) ---

// TestResolveColumnRef_NotFound covers column not found error.
func TestResolveColumnRef_NotFound(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, StringValue: "nonexistent"}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

// TestResolveColumnRef_NoSrc covers column ref with no FROM clause.
func TestResolveColumnRef_NoSrc(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, StringValue: "col"}},
			},
		},
		Src: nil,
	}

	err := rc.ResolveResultColumns(sel)
	if err == nil {
		t.Fatal("expected error when no FROM clause")
	}
}

// --- result.go: findTableInSrc (77.8%) ---

// TestFindTableInSrc_NilSrc verifies nil src returns nil.
func TestFindTableInSrc_NilSrc(t *testing.T) {
	result := findTableInSrc(nil, "t")
	if result != nil {
		t.Errorf("expected nil for nil src, got %v", result)
	}
}

// TestFindTableInSrc_ByAlias verifies lookup by alias.
func TestFindTableInSrc_ByAlias(t *testing.T) {
	table := &Table{Name: "users"}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table, Alias: "u"})

	result := findTableInSrc(srcList, "u")
	if result == nil {
		t.Fatal("expected to find table by alias 'u'")
	}
}

// TestFindTableInSrc_ByTableName verifies lookup by table name.
func TestFindTableInSrc_ByTableName(t *testing.T) {
	table := &Table{Name: "orders"}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table})

	result := findTableInSrc(srcList, "orders")
	if result == nil {
		t.Fatal("expected to find table by name 'orders'")
	}
}

// TestFindTableInSrc_NilTable verifies nil table items are skipped.
func TestFindTableInSrc_NilTable(t *testing.T) {
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: nil})

	result := findTableInSrc(srcList, "t")
	if result != nil {
		t.Errorf("expected nil when table is nil in src item")
	}
}

// --- result.go: ResolveResultColumns (77.8%) ---

// TestResolveResultColumns_NilEList covers nil EList early return.
func TestResolveResultColumns_NilEList(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	sel := &Select{EList: nil}
	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("expected no error for nil EList, got %v", err)
	}
}

// TestResolveResultColumns_NilExpr covers nil expression items.
func TestResolveResultColumns_NilExpr(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{{Expr: nil}},
		},
	}
	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("expected no error for nil Expr item, got %v", err)
	}
}

// --- result.go: resolveQualifiedColumn (83.3%) ---

// TestResolveQualifiedColumn_TableNotFound covers table not found error.
func TestResolveQualifiedColumn_TableNotFound(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	srcList := NewSrcList()
	srcList.Append(SrcListItem{
		Table: &Table{Name: "t"},
	})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:    TK_DOT,
					Left:  &Expr{Op: TK_ID, StringValue: "missing_table"},
					Right: &Expr{Op: TK_ID, StringValue: "col"},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err == nil {
		t.Fatal("expected error for unknown table in qualified column")
	}
}

// TestResolveQualifiedColumn_ColumnNotFound covers column not found error.
func TestResolveQualifiedColumn_ColumnNotFound(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}
	srcList := NewSrcList()
	srcList.Append(SrcListItem{Table: table})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:    TK_DOT,
					Left:  &Expr{Op: TK_ID, StringValue: "t"},
					Right: &Expr{Op: TK_ID, StringValue: "missing_col"},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err == nil {
		t.Fatal("expected error for unknown column in qualified reference")
	}
}

// TestResolveQualifiedColumn_Success covers successful qualified column resolution.
func TestResolveQualifiedColumn_Success(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)

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
				{Expr: &Expr{
					Op:    TK_DOT,
					Left:  &Expr{Op: TK_ID, StringValue: "users"},
					Right: &Expr{Op: TK_ID, StringValue: "name"},
				}},
			},
		},
		Src: srcList,
	}

	err := rc.ResolveResultColumns(sel)
	if err != nil {
		t.Fatalf("ResolveResultColumns qualified col failed: %v", err)
	}
}

// --- result.go: ComputeColumnAffinity (88.9%) ---

// TestComputeColumnAffinity_NilExpr covers nil expression.
func TestComputeColumnAffinity_NilExpr(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	aff := rc.ComputeColumnAffinity(nil)
	if aff != SQLITE_AFF_BLOB {
		t.Errorf("expected SQLITE_AFF_BLOB for nil, got %v", aff)
	}
}

// TestComputeColumnAffinity_Column covers column expression.
func TestComputeColumnAffinity_Column(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	col := &Column{Affinity: SQLITE_AFF_INTEGER}
	expr := &Expr{Op: TK_COLUMN, ColumnRef: col}
	aff := rc.ComputeColumnAffinity(expr)
	if aff != SQLITE_AFF_INTEGER {
		t.Errorf("expected SQLITE_AFF_INTEGER for INTEGER column, got %v", aff)
	}
}

// TestComputeColumnAffinity_Integer covers TK_INTEGER path.
func TestComputeColumnAffinity_Integer(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	expr := &Expr{Op: TK_INTEGER}
	aff := rc.ComputeColumnAffinity(expr)
	if aff != SQLITE_AFF_INTEGER {
		t.Errorf("expected SQLITE_AFF_INTEGER, got %v", aff)
	}
}

// TestComputeColumnAffinity_Float covers TK_FLOAT path.
func TestComputeColumnAffinity_Float(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	expr := &Expr{Op: TK_FLOAT}
	aff := rc.ComputeColumnAffinity(expr)
	if aff != SQLITE_AFF_REAL {
		t.Errorf("expected SQLITE_AFF_REAL, got %v", aff)
	}
}

// TestComputeColumnAffinity_WithLeft covers left operand fallback.
func TestComputeColumnAffinity_WithLeft(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	expr := &Expr{
		Op:   TK_PLUS,
		Left: &Expr{Op: TK_INTEGER},
	}
	aff := rc.ComputeColumnAffinity(expr)
	if aff != SQLITE_AFF_INTEGER {
		t.Errorf("expected SQLITE_AFF_INTEGER from Left child, got %v", aff)
	}
}

// TestComputeColumnAffinity_Unknown covers unknown op without left child.
func TestComputeColumnAffinity_Unknown(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	rc := NewResultCompiler(parse)
	expr := &Expr{Op: TK_ASTERISK} // Has no left child
	aff := rc.ComputeColumnAffinity(expr)
	if aff != SQLITE_AFF_BLOB {
		t.Errorf("expected SQLITE_AFF_BLOB for unknown op without left child, got %v", aff)
	}
}

// --- update.go: compileExpression (71.4%) ---

// TestCompileExpression_LiteralInteger covers integer literal compilation.
func TestCompileExpression_LiteralInteger(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"x"},
		Values:  []Value{IntValue(42)},
	}
	prog, err := CompileUpdate(stmt, 1, 1)
	if err != nil {
		t.Fatalf("CompileUpdate integer failed: %v", err)
	}
	if prog == nil {
		t.Fatal("expected non-nil program")
	}
}

// TestCompileExpression_LiteralFloat covers float literal compilation.
func TestCompileExpression_LiteralFloat(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"price"},
		Values:  []Value{FloatValue(3.14)},
	}
	prog, err := CompileUpdate(stmt, 1, 1)
	if err != nil {
		t.Fatalf("CompileUpdate float failed: %v", err)
	}
	_ = prog
}

// TestCompileExpression_LiteralBlob covers blob literal compilation.
func TestCompileExpression_LiteralBlob(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"data"},
		Values:  []Value{BlobValue([]byte{0x01, 0x02})},
	}
	prog, err := CompileUpdate(stmt, 1, 1)
	if err != nil {
		t.Fatalf("CompileUpdate blob failed: %v", err)
	}
	_ = prog
}

// TestCompileExpression_LiteralNull covers null literal compilation.
func TestCompileExpression_LiteralNull(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"col"},
		Values:  []Value{NullValue()},
	}
	prog, err := CompileUpdate(stmt, 1, 1)
	if err != nil {
		t.Fatalf("CompileUpdate null failed: %v", err)
	}
	_ = prog
}

// TestCompileExpression_Column covers column expression compilation via WHERE.
func TestCompileExpression_Column(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"a"},
		Values:  []Value{IntValue(1)},
		Where: &WhereClause{
			Expr: &Expression{
				Type:   ExprColumn,
				Column: "a",
			},
		},
	}
	prog, err := CompileUpdate(stmt, 1, 2)
	if err != nil {
		t.Fatalf("CompileUpdate with column WHERE failed: %v", err)
	}
	_ = prog
}

// TestCompileExpression_Binary covers binary expression compilation via WHERE.
func TestCompileExpression_Binary(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"val"},
		Values:  []Value{IntValue(1)},
		Where: &WhereClause{
			Expr: &Expression{
				Type:     ExprBinary,
				Operator: "=",
				Left:     &Expression{Type: ExprColumn, Column: "id"},
				Right:    &Expression{Type: ExprLiteral, Value: IntValue(42)},
			},
		},
	}
	prog, err := CompileUpdate(stmt, 1, 2)
	if err != nil {
		t.Fatalf("CompileUpdate with binary WHERE failed: %v", err)
	}
	_ = prog
}

// TestCompileExpression_Unsupported covers unsupported expression type.
func TestCompileExpression_Unsupported(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"a"},
		Values:  []Value{IntValue(1)},
		Where: &WhereClause{
			Expr: &Expression{
				Type: ExprFunction, // unsupported in compileExpression
			},
		},
	}
	_, err := CompileUpdate(stmt, 1, 1)
	if err == nil {
		t.Fatal("expected error for unsupported expression type")
	}
}

// --- update.go: compileExprBinary (72.7%) ---

// TestCompileExprBinary_UnsupportedOp covers unsupported operator in binary expr.
func TestCompileExprBinary_UnsupportedOp(t *testing.T) {
	p := &Program{Instructions: make([]Instruction, 0), NumRegisters: 10, NumCursors: 1}
	expr := &Expression{
		Type:     ExprBinary,
		Operator: "??",
		Left:     &Expression{Type: ExprLiteral, Value: IntValue(1)},
		Right:    &Expression{Type: ExprLiteral, Value: IntValue(2)},
	}
	err := p.compileExpression(expr, 0, 1, 5)
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
}

// TestCompileExprBinary_AllOps covers all supported binary operators.
func TestCompileExprBinary_AllOps(t *testing.T) {
	ops := []string{"=", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/"}
	for _, op := range ops {
		p := &Program{Instructions: make([]Instruction, 0), NumRegisters: 20, NumCursors: 1}
		expr := &Expression{
			Type:     ExprBinary,
			Operator: op,
			Left:     &Expression{Type: ExprLiteral, Value: IntValue(5)},
			Right:    &Expression{Type: ExprLiteral, Value: IntValue(2)},
		}
		err := p.compileExpression(expr, 0, 1, 5)
		if err != nil {
			t.Errorf("op %q: unexpected error: %v", op, err)
		}
	}
}

// --- update.go: CompileUpdateWithIndex (75.0%) ---

// TestCompileUpdateWithIndex_MultipleIndexes covers index update path.
func TestCompileUpdateWithIndex_MultipleIndexes(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "t",
		Columns: []string{"name", "email"},
		Values:  []Value{TextValue("Alice"), TextValue("alice@example.com")},
	}
	prog, err := CompileUpdateWithIndex(stmt, 5, 3, []int{1, 2})
	if err != nil {
		t.Fatalf("CompileUpdateWithIndex multi-index failed: %v", err)
	}
	if prog == nil {
		t.Fatal("expected non-nil program")
	}
}

// --- record.go: getVarintGeneral (90.0%) ---

// TestGetVarintGeneral_TwoByteVarint covers 2-byte varint via GetVarint.
func TestGetVarintGeneral_TwoByteVarint(t *testing.T) {
	// A 2-byte varint: 0x81, 0x00 = 128
	buf := []byte{0x81, 0x00}
	val, n := GetVarint(buf, 0)
	if n != 2 {
		t.Errorf("expected 2 bytes, got %d", n)
	}
	if val != 128 {
		t.Errorf("expected val=128, got %d", val)
	}
}

// TestGetVarintGeneral_ThreeByteVarint covers 3-byte varint (general path).
func TestGetVarintGeneral_ThreeByteVarint(t *testing.T) {
	// 3-byte varint: 0x81, 0x80, 0x00 = 16384
	buf := []byte{0x81, 0x80, 0x00}
	val, n := GetVarint(buf, 0)
	if n < 2 {
		t.Errorf("expected at least 2 bytes, got %d", n)
	}
	_ = val
}

// TestGetVarintGeneral_NineByteVarint covers the 9-byte max varint path.
func TestGetVarintGeneral_NineByteVarint2(t *testing.T) {
	// 9-byte varint: all bytes have high bit set except last
	buf := []byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x00}
	val, n := GetVarint(buf, 0)
	if n != 9 {
		t.Errorf("expected 9 bytes, got %d", n)
	}
	_ = val
}

// --- record.go: parseSerialTypes (83.3%) ---

// TestParseRecord_EmptyData covers empty data error.
func TestParseRecord_EmptyData(t *testing.T) {
	_, err := ParseRecord([]byte{})
	if err == nil {
		t.Fatal("expected error for empty record")
	}
}

// TestParseRecord_ValidRecord covers parsing a simple record.
func TestParseRecord_ValidRecord(t *testing.T) {
	// Build a minimal valid record: integer value 42
	rec, err := MakeRecord([]Value{IntValue(42)})
	if err != nil {
		t.Fatalf("MakeRecord failed: %v", err)
	}
	parsed, err := ParseRecord(rec)
	if err != nil {
		t.Fatalf("ParseRecord failed: %v", err)
	}
	if len(parsed.Values) != 1 {
		t.Errorf("expected 1 value, got %d", len(parsed.Values))
	}
	if parsed.Values[0].Int != 42 {
		t.Errorf("expected Int=42, got %d", parsed.Values[0].Int)
	}
}

// TestParseRecord_MultipleValues covers parsing multi-column record.
func TestParseRecord_MultipleValues(t *testing.T) {
	values := []Value{
		IntValue(1),
		TextValue("hello"),
		NullValue(),
		FloatValue(3.14),
	}
	rec, err := MakeRecord(values)
	if err != nil {
		t.Fatalf("MakeRecord failed: %v", err)
	}
	parsed, err := ParseRecord(rec)
	if err != nil {
		t.Fatalf("ParseRecord multi-value failed: %v", err)
	}
	if len(parsed.Values) != 4 {
		t.Errorf("expected 4 values, got %d", len(parsed.Values))
	}
}

// --- select.go: selectInnerLoop (69.2%) ---

// TestSelectInnerLoop_WithSrcTab covers the srcTab >= 0 path.
func TestSelectInnerLoop_WithSrcTab(t *testing.T) {
	db := &Database{Name: "test"}
	vdbe := NewVdbe(db)
	parse := &Parse{Vdbe: vdbe, Mem: 0, Tabs: 0}
	sc := NewSelectCompiler(parse)

	table := &Table{
		Name:       "t",
		NumColumns: 2,
		Columns:    []Column{{Name: "a"}, {Name: "b"}},
	}
	src := NewSrcList()
	src.Append(SrcListItem{Table: table, Cursor: 0})

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 0}},
				{Expr: &Expr{Op: TK_COLUMN, Table: 0, Column: 1}},
			},
		},
		Src: src,
	}
	dest := &SelectDest{
		Dest: SRT_Output,
	}

	// Call selectInnerLoop with srcTab=0 (reading from intermediate table)
	err := sc.selectInnerLoop(sel, 0, nil, nil, dest, 999, 998)
	if err != nil {
		t.Fatalf("selectInnerLoop with srcTab=0 failed: %v", err)
	}
}

// TestSelectInnerLoop_WithNegSrcTab covers the srcTab < 0 path (direct eval).
func TestSelectInnerLoop_WithNegSrcTab(t *testing.T) {
	db := &Database{Name: "test"}
	vdbe := NewVdbe(db)
	parse := &Parse{Vdbe: vdbe, Mem: 0, Tabs: 0}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
	}
	dest := &SelectDest{
		Dest: SRT_Output,
	}

	err := sc.selectInnerLoop(sel, -1, nil, nil, dest, 999, 998)
	if err != nil {
		t.Fatalf("selectInnerLoop with srcTab=-1 failed: %v", err)
	}
}

// --- select.go: applyOffsetFilter and applyLimitFilter (66.7%) ---

// TestApplyOffsetFilter_NoSort covers offset filter when no sort context.
func TestApplyOffsetFilter_NoSort(t *testing.T) {
	db := &Database{Name: "test"}
	vdbe := NewVdbe(db)
	parse := &Parse{Vdbe: vdbe, Mem: 1}
	sc := NewSelectCompiler(parse)

	label := vdbe.MakeLabel()
	sel := &Select{
		Offset: 5,
	}

	// Should emit offset check code without panicking
	sc.applyOffsetFilter(nil, sel, label)
}

// TestApplyLimitFilter_NoSort covers limit filter when no sort context.
func TestApplyLimitFilter_NoSort(t *testing.T) {
	db := &Database{Name: "test"}
	vdbeInst := NewVdbe(db)
	parse := &Parse{Vdbe: vdbeInst, Mem: 1}
	sc := NewSelectCompiler(parse)

	label := vdbeInst.MakeLabel()
	sel := &Select{
		Limit: 10,
	}

	// Should emit limit check code without panicking
	sc.applyLimitFilter(nil, sel, label)
}

// TestApplyOffsetFilter_ZeroOffset covers the early return when Offset <= 0.
func TestApplyOffsetFilter_ZeroOffset(t *testing.T) {
	db := &Database{Name: "test"}
	v := NewVdbe(db)
	parse := &Parse{Vdbe: v, Mem: 0}
	sc := NewSelectCompiler(parse)
	initialOps := v.CurrentAddr()

	sel := &Select{Offset: 0}
	sc.applyOffsetFilter(nil, sel, 99)
	if v.CurrentAddr() != initialOps {
		t.Error("expected no ops emitted when Offset=0")
	}
}

// --- select.go: extractResultColumns (62.5%) ---

// TestExtractResultColumns_FromTable covers srcTab >= 0 path.
func TestExtractResultColumns_FromTable(t *testing.T) {
	db := &Database{Name: "test"}
	v := NewVdbe(db)
	parse := &Parse{Vdbe: v, Mem: 5}
	sc := NewSelectCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
	}
	regResult := parse.AllocRegs(2)
	sc.extractResultColumns(sel, 0, 2, regResult) // srcTab=0

	// Should have emitted OP_Column instructions
	found := 0
	for _, instr := range v.Ops {
		if instr.Opcode == OP_Column {
			found++
		}
	}
	if found < 2 {
		t.Errorf("expected at least 2 OP_Column instructions, got %d", found)
	}
}

// --- aggregate.go: findAggsInExprList (83.3%) ---

// TestFindAggsInExprList_NilList covers nil ExprList early return.
func TestFindAggsInExprList_NilList(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}
	err := ac.findAggsInExprList(nil, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInExprList(nil) failed: %v", err)
	}
	if len(aggInfo.AggFuncs) != 0 {
		t.Errorf("expected 0 agg funcs from nil list, got %d", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInExprList_MultipleItems covers multiple-item list.
func TestFindAggsInExprList_MultipleItems(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	list := &ExprList{
		Items: []ExprListItem{
			{Expr: &Expr{Op: TK_INTEGER}},
			{Expr: &Expr{Op: TK_AGG_FUNCTION, FuncDef: &FuncDef{Name: "min"}}},
			{Expr: &Expr{Op: TK_INTEGER}},
		},
	}
	err := ac.findAggsInExprList(list, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInExprList multi failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func, got %d", len(aggInfo.AggFuncs))
	}
}
