package sql

import (
	"testing"
)

func TestCompileDeleteWithIndex(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &WhereClause{
			Expr: &Expression{
				Type:     ExprColumn,
				Column:   "id",
				Operator: "=",
				Value:    IntValue(1),
			},
		},
	}

	prog, err := CompileDeleteWithIndex(stmt, 100, []IndexInfo{
		{Name: "idx_id", Columns: []string{"id"}},
	})
	if err != nil {
		t.Fatalf("CompileDeleteWithIndex failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

func TestCompileDeleteWithForeignKeys(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
	}

	foreignKeys := []ForeignKeyInfo{
		{
			Name:       "fk_posts_user",
			Columns:    []string{"user_id"},
			RefTable:   "users",
			RefColumns: []string{"id"},
			OnDelete:   "CASCADE",
		},
	}

	prog, err := CompileDeleteWithForeignKeys(stmt, 100, foreignKeys)
	if err != nil {
		t.Fatalf("CompileDeleteWithForeignKeys failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

func TestValidateDelete(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
	}

	err := ValidateDelete(stmt)
	if err != nil {
		t.Fatalf("ValidateDelete failed: %v", err)
	}
}

func TestNewDeleteStmt(t *testing.T) {
	stmt := NewDeleteStmt("users", nil)
	if stmt == nil {
		t.Fatal("NewDeleteStmt returned nil")
	}
	if stmt.Table != "users" {
		t.Errorf("Table = %s, want users", stmt.Table)
	}
}

func TestEstimateDeleteCost(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &WhereClause{
			Expr: &Expression{
				Type:   ExprColumn,
				Column: "id",
			},
		},
	}

	cost := EstimateDeleteCost(stmt, 1000)
	if cost <= 0 {
		t.Error("Cost should be > 0")
	}
}
