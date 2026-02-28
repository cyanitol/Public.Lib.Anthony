package parser

import (
	"testing"
)

func TestCompoundOpString(t *testing.T) {
	tests := []struct {
		op   CompoundOp
		want string
	}{
		{CompoundUnion, "UNION"},
		{CompoundUnionAll, "UNION ALL"},
		{CompoundExcept, "EXCEPT"},
		{CompoundIntersect, "INTERSECT"},
		{CompoundOp(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		got := tt.op.String()
		if got != tt.want {
			t.Errorf("CompoundOp(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

func TestAlterTableActionString(t *testing.T) {
	tests := []struct {
		name   string
		action AlterTableAction
		want   string
	}{
		{
			name:   "RenameTableAction",
			action: &RenameTableAction{NewName: "new_table"},
			want:   "RENAME TO",
		},
		{
			name:   "RenameColumnAction",
			action: &RenameColumnAction{OldName: "old_col", NewName: "new_col"},
			want:   "RENAME COLUMN",
		},
		{
			name: "AddColumnAction",
			action: &AddColumnAction{
				Column: ColumnDef{Name: "new_column", Type: "TEXT"},
			},
			want: "ADD COLUMN",
		},
		{
			name:   "DropColumnAction",
			action: &DropColumnAction{ColumnName: "old_column"},
			want:   "DROP COLUMN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.action.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUpsertClause(t *testing.T) {
	// UpsertClause doesn't have a String method, just test creation
	upsert := &UpsertClause{
		Target: &ConflictTarget{
			Columns: []IndexedColumn{{Column: "id"}},
		},
		Action: ConflictDoUpdate,
	}
	if upsert == nil {
		t.Error("expected UpsertClause, got nil")
	}
}

func TestBinaryExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *BinaryExpr
		want string
	}{
		{
			name: "simple binary expr",
			expr: &BinaryExpr{
				Left:  &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Op:    OpPlus,
				Right: &LiteralExpr{Type: LiteralInteger, Value: "2"},
			},
			want: "1 + 2",
		},
		{
			name: "nil left",
			expr: &BinaryExpr{
				Left:  nil,
				Op:    OpPlus,
				Right: &LiteralExpr{Type: LiteralInteger, Value: "2"},
			},
			want: "nil + 2",
		},
		{
			name: "nil right",
			expr: &BinaryExpr{
				Left:  &LiteralExpr{Type: LiteralInteger, Value: "1"},
				Op:    OpPlus,
				Right: nil,
			},
			want: "1 + nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUnaryExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *UnaryExpr
		want string
	}{
		{
			name: "negation",
			expr: &UnaryExpr{
				Op:   OpNeg,
				Expr: &LiteralExpr{Type: LiteralInteger, Value: "5"},
			},
			want: "-5",
		},
		{
			name: "not",
			expr: &UnaryExpr{
				Op:   OpNot,
				Expr: &IdentExpr{Name: "flag"},
			},
			want: "NOT flag",
		},
		{
			name: "nil expr",
			expr: &UnaryExpr{
				Op:   OpNeg,
				Expr: nil,
			},
			want: "-nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUnaryIsNullExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *UnaryExpr
		want string
	}{
		{
			name: "IS NULL",
			expr: &UnaryExpr{
				Op:   OpIsNull,
				Expr: &IdentExpr{Name: "value"},
			},
			want: "value IS NULL",
		},
		{
			name: "IS NOT NULL",
			expr: &UnaryExpr{
				Op:   OpNotNull,
				Expr: &IdentExpr{Name: "value"},
			},
			want: "value IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *InExpr
		want string
	}{
		{
			name: "IN list",
			expr: &InExpr{
				Expr: &IdentExpr{Name: "id"},
				Values: []Expression{
					&LiteralExpr{Type: LiteralInteger, Value: "1"},
					&LiteralExpr{Type: LiteralInteger, Value: "2"},
				},
				Not: false,
			},
			want: "id IN (1, 2)",
		},
		{
			name: "NOT IN list",
			expr: &InExpr{
				Expr: &IdentExpr{Name: "id"},
				Values: []Expression{
					&LiteralExpr{Type: LiteralInteger, Value: "1"},
				},
				Not: true,
			},
			want: "id NOT IN (1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBetweenExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *BetweenExpr
		want string
	}{
		{
			name: "BETWEEN",
			expr: &BetweenExpr{
				Expr:  &IdentExpr{Name: "age"},
				Lower: &LiteralExpr{Type: LiteralInteger, Value: "18"},
				Upper: &LiteralExpr{Type: LiteralInteger, Value: "65"},
				Not:   false,
			},
			want: "age BETWEEN 18 AND 65",
		},
		{
			name: "NOT BETWEEN",
			expr: &BetweenExpr{
				Expr:  &IdentExpr{Name: "age"},
				Lower: &LiteralExpr{Type: LiteralInteger, Value: "18"},
				Upper: &LiteralExpr{Type: LiteralInteger, Value: "65"},
				Not:   true,
			},
			want: "age NOT BETWEEN 18 AND 65",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFunctionExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *FunctionExpr
		want string
	}{
		{
			name: "no args",
			expr: &FunctionExpr{
				Name: "COUNT",
				Args: []Expression{},
			},
			want: "COUNT()",
		},
		{
			name: "with args",
			expr: &FunctionExpr{
				Name: "MAX",
				Args: []Expression{&IdentExpr{Name: "age"}},
			},
			want: "MAX(age)",
		},
		{
			name: "COUNT(*)",
			expr: &FunctionExpr{
				Name:     "COUNT",
				Args:     []Expression{},
				Star:     true,
				Distinct: false,
			},
			want: "COUNT(*)",
		},
		{
			name: "COUNT(DISTINCT)",
			expr: &FunctionExpr{
				Name:     "COUNT",
				Args:     []Expression{&IdentExpr{Name: "id"}},
				Distinct: true,
			},
			want: "COUNT(DISTINCT id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCaseExprString(t *testing.T) {
	tests := []struct {
		name string
		expr *CaseExpr
		want string
	}{
		{
			name: "simple CASE",
			expr: &CaseExpr{
				Expr: nil,
				WhenClauses: []WhenClause{
					{
						Condition: &BinaryExpr{
							Left:  &IdentExpr{Name: "age"},
							Op:    OpGt,
							Right: &LiteralExpr{Type: LiteralInteger, Value: "18"},
						},
						Result: &LiteralExpr{Type: LiteralString, Value: "adult"},
					},
				},
				ElseClause: &LiteralExpr{Type: LiteralString, Value: "minor"},
			},
			want: "CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END",
		},
		{
			name: "CASE with base",
			expr: &CaseExpr{
				Expr: &IdentExpr{Name: "status"},
				WhenClauses: []WhenClause{
					{
						Condition: &LiteralExpr{Type: LiteralInteger, Value: "1"},
						Result:    &LiteralExpr{Type: LiteralString, Value: "active"},
					},
				},
				ElseClause: nil,
			},
			want: "CASE status WHEN 1 THEN 'active' END",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.expr.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCastExprString(t *testing.T) {
	expr := &CastExpr{
		Expr: &IdentExpr{Name: "value"},
		Type: "INTEGER",
	}
	got := expr.String()
	want := "CAST(value AS INTEGER)"
	if got != want {
		t.Errorf("CastExpr.String() = %q, want %q", got, want)
	}
}

func TestCollateExprString(t *testing.T) {
	expr := &CollateExpr{
		Expr:      &IdentExpr{Name: "name"},
		Collation: "NOCASE",
	}
	got := expr.String()
	want := "name COLLATE NOCASE"
	if got != want {
		t.Errorf("CollateExpr.String() = %q, want %q", got, want)
	}
}

func TestExistsExprString(t *testing.T) {
	expr := &ExistsExpr{
		Select: &SelectStmt{},
	}
	got := expr.String()
	want := "EXISTS (SELECT ...)"
	if got != want {
		t.Errorf("ExistsExpr.String() = %q, want %q", got, want)
	}
}

func TestParenExprString(t *testing.T) {
	expr := &ParenExpr{
		Expr: &IdentExpr{Name: "value"},
	}
	got := expr.String()
	want := "(value)"
	if got != want {
		t.Errorf("ParenExpr.String() = %q, want %q", got, want)
	}
}

func TestSubqueryExprString(t *testing.T) {
	expr := &SubqueryExpr{
		Select: &SelectStmt{},
	}
	got := expr.String()
	want := "(SELECT ...)"
	if got != want {
		t.Errorf("SubqueryExpr.String() = %q, want %q", got, want)
	}
}

func TestVacuumStmtString(t *testing.T) {
	tests := []struct {
		name string
		stmt *VacuumStmt
		want string
	}{
		{
			name: "simple VACUUM",
			stmt: &VacuumStmt{},
			want: "VACUUM",
		},
		{
			name: "VACUUM INTO",
			stmt: &VacuumStmt{Into: "backup.db"},
			want: "VACUUM INTO",
		},
		{
			name: "VACUUM INTO with param",
			stmt: &VacuumStmt{IntoParam: true},
			want: "VACUUM INTO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.stmt.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestASTExplainStmtString(t *testing.T) {
	tests := []struct {
		name string
		stmt *ExplainStmt
		want string
	}{
		{
			name: "EXPLAIN",
			stmt: &ExplainStmt{QueryPlan: false},
			want: "EXPLAIN",
		},
		{
			name: "EXPLAIN QUERY PLAN",
			stmt: &ExplainStmt{QueryPlan: true},
			want: "EXPLAIN QUERY PLAN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.stmt.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Test node() and statement() methods to ensure they're callable
func TestNodeMethods(t *testing.T) {
	statements := []Statement{
		&SelectStmt{},
		&InsertStmt{},
		&UpdateStmt{},
		&DeleteStmt{},
		&CreateTableStmt{},
		&DropTableStmt{},
		&CreateIndexStmt{},
		&DropIndexStmt{},
		&CreateViewStmt{},
		&DropViewStmt{},
		&CreateTriggerStmt{},
		&DropTriggerStmt{},
		&BeginStmt{},
		&CommitStmt{},
		&RollbackStmt{},
		&ExplainStmt{},
		&AttachStmt{},
		&DetachStmt{},
		&PragmaStmt{},
		&AlterTableStmt{},
		&VacuumStmt{},
	}

	for _, stmt := range statements {
		stmt.node()
		stmt.statement()
	}
}

// Test expression() methods to ensure they're callable
func TestExpressionMethods(t *testing.T) {
	expressions := []Expression{
		&BinaryExpr{},
		&UnaryExpr{},
		&LiteralExpr{},
		&IdentExpr{},
		&FunctionExpr{},
		&CaseExpr{},
		&CastExpr{},
		&CollateExpr{},
		&InExpr{},
		&BetweenExpr{},
		&ExistsExpr{},
		&ParenExpr{},
		&SubqueryExpr{},
	}

	for _, expr := range expressions {
		expr.node()
		expr.expression()
	}
}

// Test alterTableAction() methods
func TestAlterTableActionMethods(t *testing.T) {
	actions := []AlterTableAction{
		&RenameTableAction{},
		&RenameColumnAction{},
		&AddColumnAction{},
		&DropColumnAction{},
	}

	for _, action := range actions {
		action.node()
		action.alterTableAction()
	}
}
