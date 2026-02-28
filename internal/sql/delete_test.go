package sql
import "strings"

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

// Test ValidateDelete comprehensive
func TestValidateDeleteComprehensive(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *DeleteStmt
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_delete",
			stmt: &DeleteStmt{
				Table: "users",
			},
			wantErr: false,
		},
		{
			name:    "nil_stmt",
			stmt:    nil,
			wantErr: true,
			errMsg:  "nil delete statement",
		},
		{
			name: "empty_table",
			stmt: &DeleteStmt{
				Table: "",
			},
			wantErr: true,
			errMsg:  "table name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDelete(tt.stmt)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil && tt.errMsg != "" {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateDelete() error = %v, want substring %q", err, tt.errMsg)
				}
			}
		})
	}
}

// Test EstimateDeleteCost with nil WHERE (truncate case)
func TestEstimateDeleteCostNilWhere(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: nil,
	}

	cost := EstimateDeleteCost(stmt, 10000)
	if cost != 1 {
		t.Errorf("Cost for truncate = %d, want 1", cost)
	}
}

// Test EstimateDeleteCost with LIMIT
func TestEstimateDeleteCostWithLimit(t *testing.T) {
	limit := 50
	stmt := &DeleteStmt{
		Table: "users",
		Where: &WhereClause{
			Expr: &Expression{
				Type:   ExprColumn,
				Column: "id",
			},
		},
		Limit: &limit,
	}

	cost := EstimateDeleteCost(stmt, 1000)
	if cost != 50 {
		t.Errorf("Cost with LIMIT = %d, want 50", cost)
	}
}

// Test EstimateDeleteCost with LIMIT greater than table rows
func TestEstimateDeleteCostLimitGreaterThanRows(t *testing.T) {
	limit := 5000
	stmt := &DeleteStmt{
		Table: "users",
		Where: &WhereClause{
			Expr: &Expression{
				Type:   ExprColumn,
				Column: "id",
			},
		},
		Limit: &limit,
	}

	cost := EstimateDeleteCost(stmt, 1000)
	if cost != 1000 {
		t.Errorf("Cost with LIMIT > rows = %d, want 1000", cost)
	}
}

// Test CompileDeleteWithIndex with empty index list
func TestCompileDeleteWithIndexEmpty(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &WhereClause{
			Expr: &Expression{
				Type:   ExprColumn,
				Column: "id",
			},
		},
	}

	prog, err := CompileDeleteWithIndex(stmt, 100, []IndexInfo{})
	if err != nil {
		t.Fatalf("CompileDeleteWithIndex with empty indexes failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

// Test CompileDeleteWithForeignKeys with empty foreign keys
func TestCompileDeleteWithForeignKeysEmpty(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
	}

	prog, err := CompileDeleteWithForeignKeys(stmt, 100, []ForeignKeyInfo{})
	if err != nil {
		t.Fatalf("CompileDeleteWithForeignKeys with empty FKs failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

// Test CompileDeleteWithForeignKeys with RESTRICT action
func TestCompileDeleteWithForeignKeysRestrict(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
	}

	foreignKeys := []ForeignKeyInfo{
		{
			Name:       "fk_posts_user",
			Columns:    []string{"user_id"},
			RefTable:   "users",
			RefColumns: []string{"id"},
			OnDelete:   "RESTRICT",
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

// Test CompileDeleteWithForeignKeys with SET NULL action
func TestCompileDeleteWithForeignKeysSetNull(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
	}

	foreignKeys := []ForeignKeyInfo{
		{
			Name:       "fk_posts_user",
			Columns:    []string{"user_id"},
			RefTable:   "users",
			RefColumns: []string{"id"},
			OnDelete:   "SET NULL",
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

// Test CompileDeleteWithForeignKeys with NO ACTION
func TestCompileDeleteWithForeignKeysNoAction(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
	}

	foreignKeys := []ForeignKeyInfo{
		{
			Name:       "fk_posts_user",
			Columns:    []string{"user_id"},
			RefTable:   "users",
			RefColumns: []string{"id"},
			OnDelete:   "NO ACTION",
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

// Test CompileDeleteWithTruncateOptimization
func TestCompileDeleteWithTruncateOptimizationNilWhere(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: nil,
	}

	prog, err := CompileDeleteWithTruncateOptimization(stmt, 100)
	if err != nil {
		t.Fatalf("CompileDeleteWithTruncateOptimization failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

// Test CompileDeleteWithTruncateOptimization with WHERE clause
func TestCompileDeleteWithTruncateOptimizationWithWhere(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &WhereClause{
			Expr: &Expression{
				Type:   ExprColumn,
				Column: "id",
			},
		},
	}

	prog, err := CompileDeleteWithTruncateOptimization(stmt, 100)
	if err != nil {
		t.Fatalf("CompileDeleteWithTruncateOptimization failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}
