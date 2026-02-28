package parser

import "testing"

// TestConcreteTypeNodeMethods tests node() methods on concrete types to ensure coverage.
// The Go coverage tool doesn't always count calls through interface values, so we need
// to explicitly call these methods on concrete types.
func TestConcreteTypeNodeMethods(t *testing.T) {
	t.Parallel()
	// Test all statement types
	var s1 SelectStmt
	s1.node()
	s1.statement()

	var i1 InsertStmt
	i1.node()
	i1.statement()

	var u1 UpdateStmt
	u1.node()
	u1.statement()

	var d1 DeleteStmt
	d1.node()
	d1.statement()

	var ct CreateTableStmt
	ct.node()
	ct.statement()

	var dt DropTableStmt
	dt.node()
	dt.statement()

	var ci CreateIndexStmt
	ci.node()
	ci.statement()

	var di DropIndexStmt
	di.node()
	di.statement()

	var cv CreateViewStmt
	cv.node()
	cv.statement()

	var dv DropViewStmt
	dv.node()
	dv.statement()

	var ctr CreateTriggerStmt
	ctr.node()
	ctr.statement()

	var dtr DropTriggerStmt
	dtr.node()
	dtr.statement()

	var b BeginStmt
	b.node()
	b.statement()

	var c CommitStmt
	c.node()
	c.statement()

	var r RollbackStmt
	r.node()
	r.statement()

	var e ExplainStmt
	e.node()
	e.statement()

	var a AttachStmt
	a.node()
	a.statement()

	var det DetachStmt
	det.node()
	det.statement()

	var p PragmaStmt
	p.node()
	p.statement()

	var alt AlterTableStmt
	alt.node()
	alt.statement()

	var v VacuumStmt
	v.node()
	v.statement()
}

// TestConcreteTypeExpressionMethods tests expression() methods on concrete types.
func TestConcreteTypeExpressionMethods(t *testing.T) {
	t.Parallel()
	var be BinaryExpr
	be.node()
	be.expression()

	var ue UnaryExpr
	ue.node()
	ue.expression()

	var le LiteralExpr
	le.node()
	le.expression()

	var ie IdentExpr
	ie.node()
	ie.expression()

	var fe FunctionExpr
	fe.node()
	fe.expression()

	var ce CaseExpr
	ce.node()
	ce.expression()

	var cae CastExpr
	cae.node()
	cae.expression()

	var cole CollateExpr
	cole.node()
	cole.expression()

	var ine InExpr
	ine.node()
	ine.expression()

	var bte BetweenExpr
	bte.node()
	bte.expression()

	var exe ExistsExpr
	exe.node()
	exe.expression()

	var pe ParenExpr
	pe.node()
	pe.expression()

	var se SubqueryExpr
	se.node()
	se.expression()

	var ve VariableExpr
	ve.node()
	ve.expression()
}

// TestConcreteTypeAlterTableActions tests alterTableAction() methods on concrete types.
func TestConcreteTypeAlterTableActions(t *testing.T) {
	t.Parallel()
	var rt RenameTableAction
	rt.node()
	rt.alterTableAction()

	var rc RenameColumnAction
	rc.node()
	rc.alterTableAction()

	var ac AddColumnAction
	ac.node()
	ac.alterTableAction()

	var dc DropColumnAction
	dc.node()
	dc.alterTableAction()
}

// TestVariableExprString tests the String() method for VariableExpr
func TestVariableExprString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *VariableExpr
		want string
	}{
		{
			name: "unnamed parameter",
			expr: &VariableExpr{Name: ""},
			want: "?",
		},
		{
			name: "named parameter",
			expr: &VariableExpr{Name: "id"},
			want: ":id",
		},
		{
			name: "named parameter with prefix",
			expr: &VariableExpr{Name: "user_name"},
			want: ":user_name",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("VariableExpr.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestExistsExprStringNotExists tests the String() method for NOT EXISTS
func TestExistsExprStringNotExists(t *testing.T) {
	t.Parallel()
	expr := &ExistsExpr{
		Select: &SelectStmt{},
		Not:    true,
	}
	got := expr.String()
	want := "NOT EXISTS (SELECT ...)"
	if got != want {
		t.Errorf("ExistsExpr.String() = %q, want %q", got, want)
	}
}

// TestLiteralExprStringAllTypes tests all literal types
func TestLiteralExprStringAllTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *LiteralExpr
		want string
	}{
		{
			name: "integer",
			expr: &LiteralExpr{Type: LiteralInteger, Value: "42"},
			want: "42",
		},
		{
			name: "float",
			expr: &LiteralExpr{Type: LiteralFloat, Value: "3.14"},
			want: "3.14",
		},
		{
			name: "string",
			expr: &LiteralExpr{Type: LiteralString, Value: "hello"},
			want: "'hello'",
		},
		{
			name: "string with quotes",
			expr: &LiteralExpr{Type: LiteralString, Value: "it's"},
			want: "'it''s'",
		},
		{
			name: "blob",
			expr: &LiteralExpr{Type: LiteralBlob, Value: "DEADBEEF"},
			want: "X'DEADBEEF'",
		},
		{
			name: "null",
			expr: &LiteralExpr{Type: LiteralNull, Value: ""},
			want: "NULL",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("LiteralExpr.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestIdentExprStringWithTable tests IdentExpr with table qualifier
func TestIdentExprStringWithTable(t *testing.T) {
	t.Parallel()
	expr := &IdentExpr{
		Table: "users",
		Name:  "id",
	}
	got := expr.String()
	want := "users.id"
	if got != want {
		t.Errorf("IdentExpr.String() = %q, want %q", got, want)
	}
}

// TestUnaryExprStringBitNot tests bitwise NOT operator
func TestUnaryExprStringBitNot(t *testing.T) {
	t.Parallel()
	expr := &UnaryExpr{
		Op:   OpBitNot,
		Expr: &LiteralExpr{Type: LiteralInteger, Value: "5"},
	}
	got := expr.String()
	want := "~5"
	if got != want {
		t.Errorf("UnaryExpr.String() = %q, want %q", got, want)
	}
}

// TestUnaryExprStringUnknownOp tests unknown unary operator
func TestUnaryExprStringUnknownOp(t *testing.T) {
	t.Parallel()
	expr := &UnaryExpr{
		Op:   UnaryOp(999),
		Expr: &LiteralExpr{Type: LiteralInteger, Value: "5"},
	}
	got := expr.String()
	want := "?5"
	if got != want {
		t.Errorf("UnaryExpr.String() = %q, want %q", got, want)
	}
}

// TestBinaryOpStringAllOps tests all binary operators
func TestBinaryOpStringAllOps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		op   BinaryOp
		want string
	}{
		{OpEq, "="},
		{OpNe, "!="},
		{OpLt, "<"},
		{OpLe, "<="},
		{OpGt, ">"},
		{OpGe, ">="},
		{OpAnd, "AND"},
		{OpOr, "OR"},
		{OpPlus, "+"},
		{OpMinus, "-"},
		{OpMul, "*"},
		{OpDiv, "/"},
		{OpRem, "%"},
		{OpConcat, "||"},
		{OpBitAnd, "&"},
		{OpBitOr, "|"},
		{OpLShift, "<<"},
		{OpRShift, ">>"},
		{OpLike, "LIKE"},
		{OpGlob, "GLOB"},
		{OpRegexp, "REGEXP"},
		{OpMatch, "MATCH"},
		{BinaryOp(999), "?"},
	}

	for _, tt := range tests {
		tt := tt
		got := tt.op.String()
		if got != tt.want {
			t.Errorf("BinaryOp(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

// TestFunctionExprStringNilArgs tests FunctionExpr with nil args
func TestFunctionExprStringNilArgs(t *testing.T) {
	t.Parallel()
	expr := &FunctionExpr{
		Name: "test",
		Args: []Expression{nil, &LiteralExpr{Type: LiteralInteger, Value: "1"}, nil},
	}
	got := expr.String()
	want := "test(1)"
	if got != want {
		t.Errorf("FunctionExpr.String() = %q, want %q", got, want)
	}
}

// TestCaseExprStringNilConditionsResults tests CaseExpr with nil conditions/results
func TestCaseExprStringNilConditionsResults(t *testing.T) {
	t.Parallel()
	expr := &CaseExpr{
		Expr: nil,
		WhenClauses: []WhenClause{
			{Condition: nil, Result: nil},
			{
				Condition: &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Result:    &LiteralExpr{Type: LiteralString, Value: "one"},
			},
		},
		ElseClause: nil,
	}
	got := expr.String()
	// Should handle nil values gracefully
	if got == "" {
		t.Errorf("CaseExpr.String() should not be empty")
	}
}

// TestCastExprStringNilExpr tests CastExpr with nil expression
func TestCastExprStringNilExpr(t *testing.T) {
	t.Parallel()
	expr := &CastExpr{
		Expr: nil,
		Type: "INTEGER",
	}
	got := expr.String()
	want := "CAST(nil AS INTEGER)"
	if got != want {
		t.Errorf("CastExpr.String() = %q, want %q", got, want)
	}
}

// TestCollateExprStringNilExpr tests CollateExpr with nil expression
func TestCollateExprStringNilExpr(t *testing.T) {
	t.Parallel()
	expr := &CollateExpr{
		Expr:      nil,
		Collation: "NOCASE",
	}
	got := expr.String()
	want := "nil COLLATE NOCASE"
	if got != want {
		t.Errorf("CollateExpr.String() = %q, want %q", got, want)
	}
}

// TestParenExprStringNilExpr tests ParenExpr with nil expression
func TestParenExprStringNilExpr(t *testing.T) {
	t.Parallel()
	expr := &ParenExpr{
		Expr: nil,
	}
	got := expr.String()
	want := "(nil)"
	if got != want {
		t.Errorf("ParenExpr.String() = %q, want %q", got, want)
	}
}

// TestInExprStringNilExpr tests InExpr with nil expression
func TestInExprStringNilExpr(t *testing.T) {
	t.Parallel()
	expr := &InExpr{
		Expr:   nil,
		Values: []Expression{&LiteralExpr{Type: LiteralInteger, Value: "1"}},
		Not:    false,
	}
	got := expr.String()
	// Should handle nil expression
	if got == "" {
		t.Errorf("InExpr.String() should not be empty")
	}
}

// TestInExprStringNilValuesInList tests InExpr with nil values in list
func TestInExprStringNilValuesInList(t *testing.T) {
	t.Parallel()
	expr := &InExpr{
		Expr:   &IdentExpr{Name: "id"},
		Values: []Expression{nil, &LiteralExpr{Type: LiteralInteger, Value: "1"}, nil},
		Not:    false,
	}
	got := expr.String()
	want := "id IN (1)"
	if got != want {
		t.Errorf("InExpr.String() = %q, want %q", got, want)
	}
}

// TestBetweenExprStringNilParts tests BetweenExpr with nil parts
func TestBetweenExprStringNilParts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *BetweenExpr
	}{
		{
			name: "nil expr",
			expr: &BetweenExpr{
				Expr:  nil,
				Lower: &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Upper: &LiteralExpr{Type: LiteralInteger, Value: "10"},
			},
		},
		{
			name: "nil lower",
			expr: &BetweenExpr{
				Expr:  &IdentExpr{Name: "x"},
				Lower: nil,
				Upper: &LiteralExpr{Type: LiteralInteger, Value: "10"},
			},
		},
		{
			name: "nil upper",
			expr: &BetweenExpr{
				Expr:  &IdentExpr{Name: "x"},
				Lower: &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Upper: nil,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			// Should handle nil values without panicking
			if got == "" {
				t.Errorf("BetweenExpr.String() should not be empty")
			}
		})
	}
}
