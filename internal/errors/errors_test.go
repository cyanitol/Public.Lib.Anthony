// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package errors

import (
	"errors"
	"fmt"
	"testing"
)

func TestSQLiteError(t *testing.T) {
	err := NewSQLiteError(SQLITE_ERROR, "generic error")
	if err.GetCode() != SQLITE_ERROR {
		t.Errorf("expected code %d, got %d", SQLITE_ERROR, err.GetCode())
	}
	if err.Error() != "generic error (code 1)" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestSQLiteErrorWrap(t *testing.T) {
	baseErr := fmt.Errorf("underlying error")
	err := WrapSQLiteError(SQLITE_IOERR, "I/O error", baseErr)

	if err.GetCode() != SQLITE_IOERR {
		t.Errorf("expected code %d, got %d", SQLITE_IOERR, err.GetCode())
	}

	if !errors.Is(err, baseErr) {
		t.Error("error should wrap baseErr")
	}
}

func TestUniqueViolationError(t *testing.T) {
	err := NewUniqueViolationError("users", "email", "test@example.com")

	if err.Code != SQLITE_CONSTRAINT_UNIQUE {
		t.Errorf("expected code %d, got %d", SQLITE_CONSTRAINT_UNIQUE, err.Code)
	}

	if err.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", err.TableName)
	}

	if err.ColumnName != "email" {
		t.Errorf("expected column name 'email', got '%s'", err.ColumnName)
	}

	if err.ConflictingValue != "test@example.com" {
		t.Errorf("expected conflicting value 'test@example.com', got '%v'", err.ConflictingValue)
	}

	expectedMsg := "UNIQUE constraint violation on table users column email: value test@example.com already exists (code 2067)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestForeignKeyError(t *testing.T) {
	err := NewForeignKeyError("orders", "user_id", "users", "id", "INSERT")

	if err.Code != SQLITE_CONSTRAINT_FOREIGNKEY {
		t.Errorf("expected code %d, got %d", SQLITE_CONSTRAINT_FOREIGNKEY, err.Code)
	}

	if err.TableName != "orders" {
		t.Errorf("expected table name 'orders', got '%s'", err.TableName)
	}

	if err.ParentTable != "users" {
		t.Errorf("expected parent table 'users', got '%s'", err.ParentTable)
	}

	if err.Action != "INSERT" {
		t.Errorf("expected action 'INSERT', got '%s'", err.Action)
	}

	expectedMsg := "FOREIGN KEY constraint violation: orders.user_id references users.id during INSERT (code 787)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestNotNullError(t *testing.T) {
	err := NewNotNullError("users", "name")

	if err.Code != SQLITE_CONSTRAINT_NOTNULL {
		t.Errorf("expected code %d, got %d", SQLITE_CONSTRAINT_NOTNULL, err.Code)
	}

	expectedMsg := "NOT NULL constraint violation on table users column name: NOT NULL constraint failed: users.name (code 1299)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestCheckConstraintError(t *testing.T) {
	err := NewCheckConstraintError("users", "age_check", "age >= 18")

	if err.Code != SQLITE_CONSTRAINT_CHECK {
		t.Errorf("expected code %d, got %d", SQLITE_CONSTRAINT_CHECK, err.Code)
	}

	if err.CheckExpression != "age >= 18" {
		t.Errorf("expected check expression 'age >= 18', got '%s'", err.CheckExpression)
	}

	expectedMsg := "CHECK constraint violation (age_check) on table users: CHECK constraint failed: age_check (code 275)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestPrimaryKeyError(t *testing.T) {
	err := NewPrimaryKeyError("users", "id", 123)

	if err.Code != SQLITE_CONSTRAINT_PRIMARYKEY {
		t.Errorf("expected code %d, got %d", SQLITE_CONSTRAINT_PRIMARYKEY, err.Code)
	}

	if err.ConflictingValue != 123 {
		t.Errorf("expected conflicting value 123, got %v", err.ConflictingValue)
	}

	expectedMsg := "PRIMARY KEY constraint violation on table users column id: PRIMARY KEY constraint failed: users.id (code 1555)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestSyntaxError(t *testing.T) {
	err := NewSyntaxError("unexpected token", "FROM", 42)

	if err.Code != SQLITE_ERROR {
		t.Errorf("expected code %d, got %d", SQLITE_ERROR, err.Code)
	}

	if err.Token != "FROM" {
		t.Errorf("expected token 'FROM', got '%s'", err.Token)
	}

	if err.Position != 42 {
		t.Errorf("expected position 42, got %d", err.Position)
	}

	expectedMsg := "SQL syntax error near 'FROM' at position 42: unexpected token (code 1)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestLockBusyError(t *testing.T) {
	err := NewLockBusyError("test.db")

	if err.Code != SQLITE_BUSY {
		t.Errorf("expected code %d, got %d", SQLITE_BUSY, err.Code)
	}

	if !err.IsBusy {
		t.Error("expected IsBusy to be true")
	}

	if err.Database != "test.db" {
		t.Errorf("expected database 'test.db', got '%s'", err.Database)
	}
}

func TestLockTimeoutError(t *testing.T) {
	err := NewLockTimeoutError("EXCLUSIVE", "test.db")

	if err.Code != SQLITE_BUSY {
		t.Errorf("expected code %d, got %d", SQLITE_BUSY, err.Code)
	}

	if err.LockType != "EXCLUSIVE" {
		t.Errorf("expected lock type 'EXCLUSIVE', got '%s'", err.LockType)
	}

	if !err.IsTimeout {
		t.Error("expected IsTimeout to be true")
	}
}

func TestCorruptionError(t *testing.T) {
	err := NewCorruptionError("btree", 42, "invalid page type")

	if err.Code != SQLITE_CORRUPT {
		t.Errorf("expected code %d, got %d", SQLITE_CORRUPT, err.Code)
	}

	if err.Component != "btree" {
		t.Errorf("expected component 'btree', got '%s'", err.Component)
	}

	if err.PageNumber != 42 {
		t.Errorf("expected page number 42, got %d", err.PageNumber)
	}

	expectedMsg := "database corruption detected in btree at page 42: invalid page type (code 11)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestIOError(t *testing.T) {
	baseErr := fmt.Errorf("permission denied")
	err := NewIOError(SQLITE_IOERR_READ, "read", "/path/to/file.db", "failed to read", baseErr)

	if err.Code != SQLITE_IOERR_READ {
		t.Errorf("expected code %d, got %d", SQLITE_IOERR_READ, err.Code)
	}

	if err.Operation != "read" {
		t.Errorf("expected operation 'read', got '%s'", err.Operation)
	}

	if err.Path != "/path/to/file.db" {
		t.Errorf("expected path '/path/to/file.db', got '%s'", err.Path)
	}
}

func TestReadOnlyError(t *testing.T) {
	err := NewReadOnlyError("test.db", "file permissions")

	if err.Code != SQLITE_READONLY {
		t.Errorf("expected code %d, got %d", SQLITE_READONLY, err.Code)
	}

	if err.Database != "test.db" {
		t.Errorf("expected database 'test.db', got '%s'", err.Database)
	}

	if err.Reason != "file permissions" {
		t.Errorf("expected reason 'file permissions', got '%s'", err.Reason)
	}

	expectedMsg := "database is read-only: test.db (file permissions) (code 8)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestSchemaError(t *testing.T) {
	err := NewSchemaError("main", "table", "users", "schema changed")

	if err.Code != SQLITE_SCHEMA {
		t.Errorf("expected code %d, got %d", SQLITE_SCHEMA, err.Code)
	}

	if err.SchemaName != "main" {
		t.Errorf("expected schema name 'main', got '%s'", err.SchemaName)
	}

	if err.ObjectType != "table" {
		t.Errorf("expected object type 'table', got '%s'", err.ObjectType)
	}

	expectedMsg := "schema error in schema main: table users: schema changed (code 17)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestMisuseError(t *testing.T) {
	err := NewMisuseError("BeginWrite", "already in write transaction")

	if err.Code != SQLITE_MISUSE {
		t.Errorf("expected code %d, got %d", SQLITE_MISUSE, err.Code)
	}

	if err.API != "BeginWrite" {
		t.Errorf("expected API 'BeginWrite', got '%s'", err.API)
	}

	expectedMsg := "library misuse: BeginWrite: already in write transaction (code 21)"
	if err.Error() != expectedMsg {
		t.Errorf("unexpected error message:\nexpected: %s\ngot: %s", expectedMsg, err.Error())
	}
}

func TestErrorCodes(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"SQLITE_OK", SQLITE_OK},
		{"SQLITE_ERROR", SQLITE_ERROR},
		{"SQLITE_CONSTRAINT", SQLITE_CONSTRAINT},
		{"SQLITE_CONSTRAINT_UNIQUE", SQLITE_CONSTRAINT_UNIQUE},
		{"SQLITE_CONSTRAINT_FOREIGNKEY", SQLITE_CONSTRAINT_FOREIGNKEY},
		{"SQLITE_CONSTRAINT_NOTNULL", SQLITE_CONSTRAINT_NOTNULL},
		{"SQLITE_CONSTRAINT_CHECK", SQLITE_CONSTRAINT_CHECK},
		{"SQLITE_CONSTRAINT_PRIMARYKEY", SQLITE_CONSTRAINT_PRIMARYKEY},
		{"SQLITE_IOERR", SQLITE_IOERR},
		{"SQLITE_IOERR_READ", SQLITE_IOERR_READ},
		{"SQLITE_LOCKED", SQLITE_LOCKED},
		{"SQLITE_BUSY", SQLITE_BUSY},
		{"SQLITE_CORRUPT", SQLITE_CORRUPT},
		{"SQLITE_READONLY", SQLITE_READONLY},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code < 0 {
				t.Errorf("invalid error code: %d", tt.code)
			}
		})
	}
}
