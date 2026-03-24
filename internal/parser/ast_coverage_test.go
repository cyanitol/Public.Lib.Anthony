// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import "testing"

// Test all AST node String() methods for full coverage

func TestStatementNodeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		stmt Statement
		want string
	}{
		{"SelectStmt", &SelectStmt{}, "SELECT"},
		{"InsertStmt", &InsertStmt{}, "INSERT"},
		{"UpdateStmt", &UpdateStmt{}, "UPDATE"},
		{"DeleteStmt", &DeleteStmt{}, "DELETE"},
		{"CreateTableStmt", &CreateTableStmt{}, "CREATE TABLE"},
		{"DropTableStmt", &DropTableStmt{}, "DROP TABLE"},
		{"CreateIndexStmt", &CreateIndexStmt{}, "CREATE INDEX"},
		{"DropIndexStmt", &DropIndexStmt{}, "DROP INDEX"},
		{"CreateViewStmt", &CreateViewStmt{}, "CREATE VIEW"},
		{"DropViewStmt", &DropViewStmt{}, "DROP VIEW"},
		{"CreateTriggerStmt", &CreateTriggerStmt{}, "CREATE TRIGGER"},
		{"DropTriggerStmt", &DropTriggerStmt{}, "DROP TRIGGER"},
		{"BeginStmt", &BeginStmt{}, "BEGIN"},
		{"CommitStmt", &CommitStmt{}, "COMMIT"},
		{"RollbackStmt", &RollbackStmt{}, "ROLLBACK"},
		{"AttachStmt", &AttachStmt{}, "ATTACH"},
		{"DetachStmt", &DetachStmt{}, "DETACH"},
		{"PragmaStmt", &PragmaStmt{}, "PRAGMA"},
		{"AlterTableStmt", &AlterTableStmt{}, "ALTER TABLE"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.stmt.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			// Also call node() and statement() methods for coverage
			tt.stmt.node()
			tt.stmt.statement()
		})
	}
}

func TestExpressionNodeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr Expression
		want string
	}{
		{
			"LiteralExpr integer",
			&LiteralExpr{Type: LiteralInteger, Value: "42"},
			"42",
		},
		{
			"LiteralExpr float",
			&LiteralExpr{Type: LiteralFloat, Value: "3.14"},
			"3.14",
		},
		{
			"LiteralExpr string",
			&LiteralExpr{Type: LiteralString, Value: "hello"},
			"'hello'",
		},
		{
			"LiteralExpr string with quotes",
			&LiteralExpr{Type: LiteralString, Value: "it's"},
			"'it''s'",
		},
		{
			"LiteralExpr blob",
			&LiteralExpr{Type: LiteralBlob, Value: "DEADBEEF"},
			"X'DEADBEEF'",
		},
		{
			"LiteralExpr null",
			&LiteralExpr{Type: LiteralNull, Value: ""},
			"NULL",
		},
		{
			"IdentExpr simple",
			&IdentExpr{Name: "column"},
			"column",
		},
		{
			"IdentExpr qualified",
			&IdentExpr{Table: "users", Name: "id"},
			"users.id",
		},
		{
			"ParenExpr",
			&ParenExpr{Expr: &LiteralExpr{Type: LiteralInteger, Value: "5"}},
			"(5)",
		},
		{
			"ParenExpr with nil",
			&ParenExpr{Expr: nil},
			"(nil)",
		},
		{
			"SubqueryExpr",
			&SubqueryExpr{Select: &SelectStmt{}},
			"(SELECT ...)",
		},
		{
			"ExistsExpr",
			&ExistsExpr{Select: &SelectStmt{}, Not: false},
			"EXISTS (SELECT ...)",
		},
		{
			"ExistsExpr NOT",
			&ExistsExpr{Select: &SelectStmt{}, Not: true},
			"NOT EXISTS (SELECT ...)",
		},
		{
			"VariableExpr unnamed",
			&VariableExpr{Name: ""},
			"?",
		},
		{
			"VariableExpr named",
			&VariableExpr{Name: "param"},
			":param",
		},
		{
			"CastExpr",
			&CastExpr{Expr: &IdentExpr{Name: "value"}, Type: "INTEGER"},
			"CAST(value AS INTEGER)",
		},
		{
			"CastExpr with nil",
			&CastExpr{Expr: nil, Type: "TEXT"},
			"CAST(nil AS TEXT)",
		},
		{
			"CollateExpr",
			&CollateExpr{Expr: &IdentExpr{Name: "name"}, Collation: "NOCASE"},
			"name COLLATE NOCASE",
		},
		{
			"CollateExpr with nil",
			&CollateExpr{Expr: nil, Collation: "BINARY"},
			"nil COLLATE BINARY",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			// Also call node() and expression() methods for coverage
			tt.expr.node()
			tt.expr.expression()
		})
	}
}

func TestBinaryOpString(t *testing.T) {
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
		{BinaryOp(9999), "?"}, // Unknown operator
	}

	for _, tt := range tests {
		tt := tt
		got := tt.op.String()
		if got != tt.want {
			t.Errorf("BinaryOp(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

func TestUnaryExprStringAllOps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *UnaryExpr
		want string
	}{
		{
			"OpNot",
			&UnaryExpr{Op: OpNot, Expr: &IdentExpr{Name: "x"}},
			"NOT x",
		},
		{
			"OpNeg",
			&UnaryExpr{Op: OpNeg, Expr: &LiteralExpr{Type: LiteralInteger, Value: "5"}},
			"-5",
		},
		{
			"OpBitNot",
			&UnaryExpr{Op: OpBitNot, Expr: &IdentExpr{Name: "flags"}},
			"~flags",
		},
		{
			"OpIsNull",
			&UnaryExpr{Op: OpIsNull, Expr: &IdentExpr{Name: "val"}},
			"val IS NULL",
		},
		{
			"OpNotNull",
			&UnaryExpr{Op: OpNotNull, Expr: &IdentExpr{Name: "val"}},
			"val IS NOT NULL",
		},
		{
			"Unknown op",
			&UnaryExpr{Op: UnaryOp(999), Expr: &IdentExpr{Name: "x"}},
			"?x",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBinaryExprStringAllOps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *BinaryExpr
		want string
	}{
		{
			"OpEq",
			&BinaryExpr{
				Left:  &IdentExpr{Name: "a"},
				Op:    OpEq,
				Right: &LiteralExpr{Type: LiteralInteger, Value: "1"},
			},
			"a = 1",
		},
		{
			"OpConcat",
			&BinaryExpr{
				Left:  &LiteralExpr{Type: LiteralString, Value: "hello"},
				Op:    OpConcat,
				Right: &LiteralExpr{Type: LiteralString, Value: "world"},
			},
			"'hello' || 'world'",
		},
		{
			"OpAnd",
			&BinaryExpr{
				Left:  &IdentExpr{Name: "x"},
				Op:    OpAnd,
				Right: &IdentExpr{Name: "y"},
			},
			"x AND y",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInExprStringNilValues(t *testing.T) {
	t.Parallel()
	expr := &InExpr{
		Expr:   &IdentExpr{Name: "id"},
		Values: []Expression{nil, &LiteralExpr{Type: LiteralInteger, Value: "1"}},
		Not:    false,
	}
	got := expr.String()
	// Should skip nil values
	if got != "id IN (1)" {
		t.Errorf("InExpr with nil value: got %q, want %q", got, "id IN (1)")
	}
}

func TestBetweenExprStringNilFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *BetweenExpr
		want string
	}{
		{
			"nil Expr",
			&BetweenExpr{
				Expr:  nil,
				Lower: &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Upper: &LiteralExpr{Type: LiteralInteger, Value: "10"},
			},
			" BETWEEN 1 AND 10",
		},
		{
			"nil Lower",
			&BetweenExpr{
				Expr:  &IdentExpr{Name: "x"},
				Lower: nil,
				Upper: &LiteralExpr{Type: LiteralInteger, Value: "10"},
			},
			"x BETWEEN  AND 10",
		},
		{
			"nil Upper",
			&BetweenExpr{
				Expr:  &IdentExpr{Name: "x"},
				Lower: &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Upper: nil,
			},
			"x BETWEEN 1 AND ",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFunctionExprStringWithNilArgs(t *testing.T) {
	t.Parallel()
	expr := &FunctionExpr{
		Name: "COALESCE",
		Args: []Expression{
			&IdentExpr{Name: "a"},
			nil,
			&IdentExpr{Name: "b"},
		},
	}
	got := expr.String()
	// Should skip nil args
	if got != "COALESCE(a, b)" {
		t.Errorf("FunctionExpr with nil arg: got %q, want %q", got, "COALESCE(a, b)")
	}
}

func TestCaseExprStringNilFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *CaseExpr
		want string
	}{
		{
			"nil condition",
			&CaseExpr{
				WhenClauses: []WhenClause{
					{Condition: nil, Result: &LiteralExpr{Type: LiteralInteger, Value: "1"}},
				},
			},
			"CASE WHEN  THEN 1 END",
		},
		{
			"nil result",
			&CaseExpr{
				WhenClauses: []WhenClause{
					{Condition: &IdentExpr{Name: "x"}, Result: nil},
				},
			},
			"CASE WHEN x THEN  END",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInExprStringWithNilExpr(t *testing.T) {
	t.Parallel()
	expr := &InExpr{
		Expr:   nil,
		Values: []Expression{&LiteralExpr{Type: LiteralInteger, Value: "1"}},
		Not:    false,
	}
	got := expr.String()
	if got != " IN (1)" {
		t.Errorf("InExpr with nil Expr: got %q, want %q", got, " IN (1)")
	}
}

// TestMissingStatementStrings covers String() for statement types not reached by parser tests.
func TestMissingStatementStrings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		stmt Statement
		want string
	}{
		{"CreateVirtualTableStmt", &CreateVirtualTableStmt{Name: "vt", Module: "fts5"}, "CREATE VIRTUAL TABLE"},
		{"SavepointStmt", &SavepointStmt{Name: "sp1"}, "SAVEPOINT"},
		{"ReleaseStmt", &ReleaseStmt{Name: "sp1"}, "RELEASE"},
		{"ReindexStmt", &ReindexStmt{Name: "idx"}, "REINDEX"},
		{"AnalyzeStmt", &AnalyzeStmt{Name: "tbl"}, "ANALYZE"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.stmt.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			tt.stmt.node()
			tt.stmt.statement()
		})
	}
}

// TestRaiseExprString covers all branches of RaiseExpr.String().
func TestRaiseExprString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr *RaiseExpr
		want string
	}{
		{"Ignore", &RaiseExpr{Type: RaiseIgnore}, "RAISE(IGNORE)"},
		{"Rollback", &RaiseExpr{Type: RaiseRollback, Message: "oops"}, "RAISE(ROLLBACK, oops)"},
		{"Abort", &RaiseExpr{Type: RaiseAbort, Message: "err"}, "RAISE(ABORT, err)"},
		{"Fail", &RaiseExpr{Type: RaiseFail, Message: "fail"}, "RAISE(FAIL, fail)"},
		{"Unknown", &RaiseExpr{Type: RaiseType(99)}, "RAISE(UNKNOWN)"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			tt.expr.node()
			tt.expr.expression()
		})
	}
}

// Test all AlterTableAction implementations
func TestAlterTableActionNode(t *testing.T) {
	t.Parallel()
	actions := []AlterTableAction{
		&RenameTableAction{NewName: "new_table"},
		&RenameColumnAction{OldName: "old_col", NewName: "new_col"},
		&AddColumnAction{Column: ColumnDef{Name: "col", Type: "TEXT"}},
		&DropColumnAction{ColumnName: "col"},
	}

	for _, action := range actions {
		// Call node() and alterTableAction() for coverage
		action.node()
		action.alterTableAction()
		// Also test String() method
		_ = action.String()
	}
}
