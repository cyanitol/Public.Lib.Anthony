// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

func TestAlterTableRenameTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, Statement)
	}{
		{
			name: "basic rename table",
			sql:  "ALTER TABLE users RENAME TO customers",
			check: func(t *testing.T, stmt Statement) {
				alter, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if alter.Table != "users" {
					t.Errorf("expected table 'users', got %q", alter.Table)
				}
				rename, ok := alter.Action.(*RenameTableAction)
				if !ok {
					t.Fatalf("expected *RenameTableAction, got %T", alter.Action)
				}
				if rename.NewName != "customers" {
					t.Errorf("expected new name 'customers', got %q", rename.NewName)
				}
			},
		},
		{
			name: "rename table with quoted names",
			sql:  `ALTER TABLE "old_table" RENAME TO "new_table"`,
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				if alter.Table != "old_table" {
					t.Errorf("expected table 'old_table', got %q", alter.Table)
				}
				rename := alter.Action.(*RenameTableAction)
				if rename.NewName != "new_table" {
					t.Errorf("expected new name 'new_table', got %q", rename.NewName)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			if tt.check != nil {
				tt.check(t, stmts[0])
			}
		})
	}
}

func TestAlterTableRenameColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, Statement)
	}{
		{
			name: "basic rename column",
			sql:  "ALTER TABLE users RENAME COLUMN name TO full_name",
			check: func(t *testing.T, stmt Statement) {
				alter, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if alter.Table != "users" {
					t.Errorf("expected table 'users', got %q", alter.Table)
				}
				rename, ok := alter.Action.(*RenameColumnAction)
				if !ok {
					t.Fatalf("expected *RenameColumnAction, got %T", alter.Action)
				}
				if rename.OldName != "name" {
					t.Errorf("expected old name 'name', got %q", rename.OldName)
				}
				if rename.NewName != "full_name" {
					t.Errorf("expected new name 'full_name', got %q", rename.NewName)
				}
			},
		},
		{
			name: "rename column with quoted identifiers",
			sql:  `ALTER TABLE users RENAME COLUMN "old-name" TO "new-name"`,
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				rename := alter.Action.(*RenameColumnAction)
				if rename.OldName != "old-name" {
					t.Errorf("expected old name 'old-name', got %q", rename.OldName)
				}
				if rename.NewName != "new-name" {
					t.Errorf("expected new name 'new-name', got %q", rename.NewName)
				}
			},
		},
		{
			name:    "rename column without TO",
			sql:     "ALTER TABLE users RENAME COLUMN name full_name",
			wantErr: true,
		},
		{
			name:    "rename column without new name",
			sql:     "ALTER TABLE users RENAME COLUMN name TO",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			if tt.check != nil {
				tt.check(t, stmts[0])
			}
		})
	}
}

func TestAlterTableAddColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, Statement)
	}{
		{
			name: "add column with type",
			sql:  "ALTER TABLE users ADD COLUMN email TEXT",
			check: func(t *testing.T, stmt Statement) {
				alter, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if alter.Table != "users" {
					t.Errorf("expected table 'users', got %q", alter.Table)
				}
				add, ok := alter.Action.(*AddColumnAction)
				if !ok {
					t.Fatalf("expected *AddColumnAction, got %T", alter.Action)
				}
				if add.Column.Name != "email" {
					t.Errorf("expected column name 'email', got %q", add.Column.Name)
				}
				if add.Column.Type != "TEXT" {
					t.Errorf("expected column type 'TEXT', got %q", add.Column.Type)
				}
			},
		},
		{
			name: "add column without COLUMN keyword",
			sql:  "ALTER TABLE users ADD phone TEXT",
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				add := alter.Action.(*AddColumnAction)
				if add.Column.Name != "phone" {
					t.Errorf("expected column name 'phone', got %q", add.Column.Name)
				}
			},
		},
		{
			name: "add column with constraints",
			sql:  "ALTER TABLE users ADD COLUMN age INTEGER NOT NULL DEFAULT 0",
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				add := alter.Action.(*AddColumnAction)
				if add.Column.Name != "age" {
					t.Errorf("expected column name 'age', got %q", add.Column.Name)
				}
				if add.Column.Type != "INTEGER" {
					t.Errorf("expected column type 'INTEGER', got %q", add.Column.Type)
				}
				if len(add.Column.Constraints) != 2 {
					t.Errorf("expected 2 constraints, got %d", len(add.Column.Constraints))
				}
			},
		},
		{
			name: "add column with primary key",
			sql:  "ALTER TABLE users ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT",
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				add := alter.Action.(*AddColumnAction)
				if add.Column.Name != "id" {
					t.Errorf("expected column name 'id', got %q", add.Column.Name)
				}
				if len(add.Column.Constraints) < 1 {
					t.Fatalf("expected at least 1 constraint")
				}
				if add.Column.Constraints[0].Type != ConstraintPrimaryKey {
					t.Errorf("expected PRIMARY KEY constraint")
				}
			},
		},
		{
			name: "add column with default value",
			sql:  "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'",
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				add := alter.Action.(*AddColumnAction)
				if add.Column.Name != "status" {
					t.Errorf("expected column name 'status', got %q", add.Column.Name)
				}
				if len(add.Column.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(add.Column.Constraints))
				}
				if add.Column.Constraints[0].Type != ConstraintDefault {
					t.Errorf("expected DEFAULT constraint")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			if tt.check != nil {
				tt.check(t, stmts[0])
			}
		})
	}
}

func TestAlterTableDropColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, Statement)
	}{
		{
			name: "drop column",
			sql:  "ALTER TABLE users DROP COLUMN email",
			check: func(t *testing.T, stmt Statement) {
				alter, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if alter.Table != "users" {
					t.Errorf("expected table 'users', got %q", alter.Table)
				}
				drop, ok := alter.Action.(*DropColumnAction)
				if !ok {
					t.Fatalf("expected *DropColumnAction, got %T", alter.Action)
				}
				if drop.ColumnName != "email" {
					t.Errorf("expected column name 'email', got %q", drop.ColumnName)
				}
			},
		},
		{
			name: "drop column with quoted identifier",
			sql:  `ALTER TABLE users DROP COLUMN "old-column"`,
			check: func(t *testing.T, stmt Statement) {
				alter := stmt.(*AlterTableStmt)
				drop := alter.Action.(*DropColumnAction)
				if drop.ColumnName != "old-column" {
					t.Errorf("expected column name 'old-column', got %q", drop.ColumnName)
				}
			},
		},
		{
			name:    "drop without COLUMN keyword",
			sql:     "ALTER TABLE users DROP email",
			wantErr: true,
		},
		{
			name:    "drop column without name",
			sql:     "ALTER TABLE users DROP COLUMN",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			if tt.check != nil {
				tt.check(t, stmts[0])
			}
		})
	}
}

func TestAlterTableErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "missing TABLE keyword",
			sql:  "ALTER users RENAME TO customers",
		},
		{
			name: "missing table name",
			sql:  "ALTER TABLE RENAME TO customers",
		},
		{
			name: "missing action",
			sql:  "ALTER TABLE users",
		},
		{
			name: "invalid action",
			sql:  "ALTER TABLE users MODIFY COLUMN name TEXT",
		},
		{
			name: "rename without TO or COLUMN",
			sql:  "ALTER TABLE users RENAME name",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if err == nil {
				t.Errorf("expected error for invalid SQL: %s", tt.sql)
			}
		})
	}
}

func TestAlterTableMultipleStatements(t *testing.T) {
	t.Parallel()
	sql := `
		ALTER TABLE users RENAME TO customers;
		ALTER TABLE customers ADD COLUMN email TEXT;
		ALTER TABLE customers RENAME COLUMN name TO full_name;
		ALTER TABLE customers DROP COLUMN old_field;
	`

	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString() error = %v", err)
	}

	if len(stmts) != 4 {
		t.Fatalf("expected 4 statements, got %d", len(stmts))
	}

	// Check first statement
	alter1, ok := stmts[0].(*AlterTableStmt)
	if !ok {
		t.Fatalf("statement 0: expected *AlterTableStmt, got %T", stmts[0])
	}
	if alter1.Table != "users" {
		t.Errorf("statement 0: expected table 'users', got %q", alter1.Table)
	}
	if _, ok := alter1.Action.(*RenameTableAction); !ok {
		t.Errorf("statement 0: expected RenameTableAction")
	}

	// Check second statement
	alter2, ok := stmts[1].(*AlterTableStmt)
	if !ok {
		t.Fatalf("statement 1: expected *AlterTableStmt, got %T", stmts[1])
	}
	if alter2.Table != "customers" {
		t.Errorf("statement 1: expected table 'customers', got %q", alter2.Table)
	}
	if _, ok := alter2.Action.(*AddColumnAction); !ok {
		t.Errorf("statement 1: expected AddColumnAction")
	}

	// Check third statement
	alter3, ok := stmts[2].(*AlterTableStmt)
	if !ok {
		t.Fatalf("statement 2: expected *AlterTableStmt, got %T", stmts[2])
	}
	if _, ok := alter3.Action.(*RenameColumnAction); !ok {
		t.Errorf("statement 2: expected RenameColumnAction")
	}

	// Check fourth statement
	alter4, ok := stmts[3].(*AlterTableStmt)
	if !ok {
		t.Fatalf("statement 3: expected *AlterTableStmt, got %T", stmts[3])
	}
	if _, ok := alter4.Action.(*DropColumnAction); !ok {
		t.Errorf("statement 3: expected DropColumnAction")
	}
}

func TestAlterTableComplexColumnDefinitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "add column with multiple constraints",
			sql:  "ALTER TABLE users ADD COLUMN email TEXT NOT NULL UNIQUE CHECK(length(email) > 0)",
		},
		{
			name: "add column with collation",
			sql:  "ALTER TABLE users ADD COLUMN name TEXT COLLATE NOCASE",
		},
		{
			name: "add column with foreign key",
			sql:  "ALTER TABLE orders ADD COLUMN user_id INTEGER",
		},
		{
			name: "add column with numeric type and precision",
			sql:  "ALTER TABLE products ADD COLUMN price NUMERIC(10,2)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString() error = %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			alter, ok := stmts[0].(*AlterTableStmt)
			if !ok {
				t.Fatalf("expected *AlterTableStmt, got %T", stmts[0])
			}
			add, ok := alter.Action.(*AddColumnAction)
			if !ok {
				t.Fatalf("expected *AddColumnAction, got %T", alter.Action)
			}
			if add.Column.Name == "" {
				t.Errorf("column name should not be empty")
			}
		})
	}
}
