// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

func parseAlterStmt(t *testing.T, sql string) *AlterTableStmt {
	t.Helper()
	stmts, err := ParseString(sql)
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
	return alter
}

func assertRenameTable(t *testing.T, alter *AlterTableStmt, wantTable, wantNewName string) {
	t.Helper()
	if alter.Table != wantTable {
		t.Errorf("expected table %q, got %q", wantTable, alter.Table)
	}
	rename, ok := alter.Action.(*RenameTableAction)
	if !ok {
		t.Fatalf("expected *RenameTableAction, got %T", alter.Action)
	}
	if rename.NewName != wantNewName {
		t.Errorf("expected new name %q, got %q", wantNewName, rename.NewName)
	}
}

func TestAlterTableRenameTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sql         string
		wantTable   string
		wantNewName string
	}{
		{"basic rename table", "ALTER TABLE users RENAME TO customers", "users", "customers"},
		{"rename table with quoted names", `ALTER TABLE "old_table" RENAME TO "new_table"`, "old_table", "new_table"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			alter := parseAlterStmt(t, tt.sql)
			assertRenameTable(t, alter, tt.wantTable, tt.wantNewName)
		})
	}
}

func assertRenameColumn(t *testing.T, alter *AlterTableStmt, wantTable, wantOld, wantNew string) {
	t.Helper()
	if alter.Table != wantTable {
		t.Errorf("expected table %q, got %q", wantTable, alter.Table)
	}
	rename, ok := alter.Action.(*RenameColumnAction)
	if !ok {
		t.Fatalf("expected *RenameColumnAction, got %T", alter.Action)
	}
	if rename.OldName != wantOld {
		t.Errorf("expected old name %q, got %q", wantOld, rename.OldName)
	}
	if rename.NewName != wantNew {
		t.Errorf("expected new name %q, got %q", wantNew, rename.NewName)
	}
}

func TestAlterTableRenameColumn(t *testing.T) {
	t.Parallel()

	t.Run("basic rename column", func(t *testing.T) {
		t.Parallel()
		alter := parseAlterStmt(t, "ALTER TABLE users RENAME COLUMN name TO full_name")
		assertRenameColumn(t, alter, "users", "name", "full_name")
	})

	t.Run("rename column with quoted identifiers", func(t *testing.T) {
		t.Parallel()
		alter := parseAlterStmt(t, `ALTER TABLE users RENAME COLUMN "old-name" TO "new-name"`)
		assertRenameColumn(t, alter, "users", "old-name", "new-name")
	})

	errorTests := []struct {
		name string
		sql  string
	}{
		{"rename column without TO", "ALTER TABLE users RENAME COLUMN name full_name"},
		{"rename column without new name", "ALTER TABLE users RENAME COLUMN name TO"},
	}
	for _, tt := range errorTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if err == nil {
				t.Fatal("expected error but got none")
			}
		})
	}
}

// Prefix: alter_
type alterAddColumnTestCase struct {
	name               string
	sql                string
	wantErr            bool
	wantTable          string
	wantColName        string
	wantColType        string
	wantConstraints    int
	wantConstraintType ConstraintType
}

func alter_getAddAction(t *testing.T, stmt Statement) *AddColumnAction {
	t.Helper()
	alter, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("expected *AlterTableStmt, got %T", stmt)
	}
	add, ok := alter.Action.(*AddColumnAction)
	if !ok {
		t.Fatalf("expected *AddColumnAction, got %T", alter.Action)
	}
	return add
}

func alter_checkTableAndColumn(t *testing.T, stmt Statement, tc alterAddColumnTestCase) *AddColumnAction {
	t.Helper()
	alter := stmt.(*AlterTableStmt)
	if tc.wantTable != "" && alter.Table != tc.wantTable {
		t.Errorf("expected table %q, got %q", tc.wantTable, alter.Table)
	}
	add := alter_getAddAction(t, stmt)
	if tc.wantColName != "" && add.Column.Name != tc.wantColName {
		t.Errorf("expected column name %q, got %q", tc.wantColName, add.Column.Name)
	}
	if tc.wantColType != "" && add.Column.Type != tc.wantColType {
		t.Errorf("expected column type %q, got %q", tc.wantColType, add.Column.Type)
	}
	return add
}

func alter_checkConstraints(t *testing.T, add *AddColumnAction, tc alterAddColumnTestCase) {
	t.Helper()
	if tc.wantConstraints > 0 && len(add.Column.Constraints) != tc.wantConstraints {
		t.Errorf("expected %d constraints, got %d", tc.wantConstraints, len(add.Column.Constraints))
	}
	if tc.wantConstraintType != 0 && len(add.Column.Constraints) > 0 && add.Column.Constraints[0].Type != tc.wantConstraintType {
		t.Errorf("expected constraint type %v, got %v", tc.wantConstraintType, add.Column.Constraints[0].Type)
	}
}

func alter_checkAddColumn(t *testing.T, stmt Statement, tc alterAddColumnTestCase) {
	t.Helper()
	add := alter_checkTableAndColumn(t, stmt, tc)
	alter_checkConstraints(t, add, tc)
}

func TestAlterTableAddColumn(t *testing.T) {
	t.Parallel()
	tests := []alterAddColumnTestCase{
		{
			name:      "add column with type",
			sql:       "ALTER TABLE users ADD COLUMN email TEXT",
			wantTable: "users", wantColName: "email", wantColType: "TEXT",
		},
		{
			name:        "add column without COLUMN keyword",
			sql:         "ALTER TABLE users ADD phone TEXT",
			wantColName: "phone",
		},
		{
			name:        "add column with constraints",
			sql:         "ALTER TABLE users ADD COLUMN age INTEGER NOT NULL DEFAULT 0",
			wantColName: "age", wantColType: "INTEGER", wantConstraints: 2,
		},
		{
			name:        "add column with primary key",
			sql:         "ALTER TABLE users ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT",
			wantColName: "id", wantConstraintType: ConstraintPrimaryKey,
		},
		{
			name:        "add column with default value",
			sql:         "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'",
			wantColName: "status", wantConstraints: 1, wantConstraintType: ConstraintDefault,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tc.sql)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseString() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			alter_checkAddColumn(t, stmts[0], tc)
		})
	}
}

func assertDropColumn(t *testing.T, alter *AlterTableStmt, wantTable, wantCol string) {
	t.Helper()
	if alter.Table != wantTable {
		t.Errorf("expected table %q, got %q", wantTable, alter.Table)
	}
	drop, ok := alter.Action.(*DropColumnAction)
	if !ok {
		t.Fatalf("expected *DropColumnAction, got %T", alter.Action)
	}
	if drop.ColumnName != wantCol {
		t.Errorf("expected column name %q, got %q", wantCol, drop.ColumnName)
	}
}

func TestAlterTableDropColumn(t *testing.T) {
	t.Parallel()

	t.Run("drop column", func(t *testing.T) {
		t.Parallel()
		alter := parseAlterStmt(t, "ALTER TABLE users DROP COLUMN email")
		assertDropColumn(t, alter, "users", "email")
	})

	t.Run("drop column with quoted identifier", func(t *testing.T) {
		t.Parallel()
		alter := parseAlterStmt(t, `ALTER TABLE users DROP COLUMN "old-column"`)
		assertDropColumn(t, alter, "users", "old-column")
	})

	errorTests := []struct {
		name string
		sql  string
	}{
		{"drop without COLUMN keyword", "ALTER TABLE users DROP email"},
		{"drop column without name", "ALTER TABLE users DROP COLUMN"},
	}
	for _, tt := range errorTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if err == nil {
				t.Fatal("expected error but got none")
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

func assertAlterActionType(t *testing.T, stmts []Statement, idx int, wantTable string, wantAction interface{}) {
	t.Helper()
	alter, ok := stmts[idx].(*AlterTableStmt)
	if !ok {
		t.Fatalf("statement %d: expected *AlterTableStmt, got %T", idx, stmts[idx])
	}
	if wantTable != "" && alter.Table != wantTable {
		t.Errorf("statement %d: expected table %q, got %q", idx, wantTable, alter.Table)
	}
	assertAlterAction(t, idx, alter.Action, wantAction)
}

func assertAlterAction(t *testing.T, idx int, got AlterTableAction, want interface{}) {
	t.Helper()
	// Use reflect-free comparison by getting the type name strings
	wantType := ""
	switch want.(type) {
	case *RenameTableAction:
		wantType = "RenameTableAction"
	case *AddColumnAction:
		wantType = "AddColumnAction"
	case *RenameColumnAction:
		wantType = "RenameColumnAction"
	case *DropColumnAction:
		wantType = "DropColumnAction"
	}
	gotMatch := checkAlterActionMatch(got, want)
	if !gotMatch {
		t.Errorf("statement %d: expected %s, got %T", idx, wantType, got)
	}
}

func checkAlterActionMatch(got AlterTableAction, want interface{}) bool {
	switch want.(type) {
	case *RenameTableAction:
		_, ok := got.(*RenameTableAction)
		return ok
	case *AddColumnAction:
		_, ok := got.(*AddColumnAction)
		return ok
	case *RenameColumnAction:
		_, ok := got.(*RenameColumnAction)
		return ok
	case *DropColumnAction:
		_, ok := got.(*DropColumnAction)
		return ok
	}
	return false
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

	assertAlterActionType(t, stmts, 0, "users", (*RenameTableAction)(nil))
	assertAlterActionType(t, stmts, 1, "customers", (*AddColumnAction)(nil))
	assertAlterActionType(t, stmts, 2, "", (*RenameColumnAction)(nil))
	assertAlterActionType(t, stmts, 3, "", (*DropColumnAction)(nil))
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
