package sql

import (
	"testing"
)

func TestCompileUpdateWithIndex(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "users",
		Columns: []string{"name"},
		Values:  []Value{TextValue("John")},
	}

	prog, err := CompileUpdateWithIndex(stmt, 100, 2, []int{1})
	if err != nil {
		t.Fatalf("CompileUpdateWithIndex failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

func TestValidateUpdate(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "users",
		Columns: []string{"name"},
		Values:  []Value{TextValue("John")},
	}

	err := ValidateUpdate(stmt)
	if err != nil {
		t.Fatalf("ValidateUpdate failed: %v", err)
	}
}

func TestNewUpdateStmt(t *testing.T) {
	stmt := NewUpdateStmt("users", []string{"name"}, []Value{TextValue("John")}, nil)
	if stmt == nil {
		t.Fatal("NewUpdateStmt returned nil")
	}
	if stmt.Table != "users" {
		t.Errorf("Table = %s, want users", stmt.Table)
	}
}

func TestNewWhereClause(t *testing.T) {
	expr := &Expression{Type: ExprColumn, Column: "id"}
	where := NewWhereClause(expr)
	if where == nil {
		t.Fatal("NewWhereClause returned nil")
	}
	if where.Expr != expr {
		t.Error("Expression not set correctly")
	}
}

func TestNewBinaryExpression(t *testing.T) {
	left := &Expression{Type: ExprColumn, Column: "id"}
	right := &Expression{Type: ExprLiteral, Value: IntValue(1)}
	expr := NewBinaryExpression(left, "=", right)

	if expr == nil {
		t.Fatal("NewBinaryExpression returned nil")
	}
	if expr.Type != ExprBinary {
		t.Error("Type should be ExprBinary")
	}
	if expr.Operator != "=" {
		t.Errorf("Operator = %s, want =", expr.Operator)
	}
}

func TestNewColumnExpression(t *testing.T) {
	expr := NewColumnExpression("id")
	if expr == nil {
		t.Fatal("NewColumnExpression returned nil")
	}
	if expr.Type != ExprColumn {
		t.Error("Type should be ExprColumn")
	}
	if expr.Column != "id" {
		t.Errorf("Column = %s, want id", expr.Column)
	}
}

func TestNewLiteralExpression(t *testing.T) {
	expr := NewLiteralExpression(IntValue(42))
	if expr == nil {
		t.Fatal("NewLiteralExpression returned nil")
	}
	if expr.Type != ExprLiteral {
		t.Error("Type should be ExprLiteral")
	}
}
