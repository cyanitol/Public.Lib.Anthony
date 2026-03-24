// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package errors

import (
	"fmt"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// SQLiteError.Error() – line 135: branch where e.Err != nil is missing
// ---------------------------------------------------------------------------

func TestSQLiteError_Error_WithWrappedErr(t *testing.T) {
	inner := fmt.Errorf("disk full")
	e := WrapSQLiteError(SQLITE_FULL, "insertion failed", inner)
	got := e.Error()
	if !strings.Contains(got, "disk full") {
		t.Errorf("expected wrapped error in message, got: %s", got)
	}
	if !strings.Contains(got, "insertion failed") {
		t.Errorf("expected message in output, got: %s", got)
	}
}

func TestSQLiteError_Error_WithoutWrappedErr(t *testing.T) {
	e := NewSQLiteError(SQLITE_FULL, "insertion failed")
	got := e.Error()
	want := "insertion failed (code 13)"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// ---------------------------------------------------------------------------
// NewConstraintError + ConstraintError.Error() – line 198, 216
// ---------------------------------------------------------------------------

func TestNewConstraintError_Error(t *testing.T) {
	tests := []struct {
		name           string
		code           int
		constraintType string
		message        string
		constraintName string
		tableName      string
		columnName     string
		wantSubstr     string
	}{
		{
			// all optional fields empty → minimal path
			name:           "mcdc_all_empty",
			code:           SQLITE_CONSTRAINT,
			constraintType: "CHECK",
			message:        "",
			wantSubstr:     "CHECK constraint violation",
		},
		{
			// constraintName set
			name:           "mcdc_constraintName_set",
			code:           SQLITE_CONSTRAINT,
			constraintType: "CHECK",
			message:        "",
			constraintName: "chk_age",
			wantSubstr:     "(chk_age)",
		},
		{
			// tableName set, columnName empty → table branch only
			name:           "mcdc_tableName_only",
			code:           SQLITE_CONSTRAINT,
			constraintType: "CHECK",
			message:        "",
			tableName:      "users",
			wantSubstr:     "on table users",
		},
		{
			// columnName set
			name:           "mcdc_columnName_set",
			code:           SQLITE_CONSTRAINT,
			constraintType: "CHECK",
			message:        "",
			tableName:      "users",
			columnName:     "age",
			wantSubstr:     "column age",
		},
		{
			// message set
			name:           "mcdc_message_set",
			code:           SQLITE_CONSTRAINT,
			constraintType: "CHECK",
			message:        "value out of range",
			wantSubstr:     "value out of range",
		},
		{
			// all fields set
			name:           "mcdc_all_set",
			code:           SQLITE_CONSTRAINT_CHECK,
			constraintType: "CHECK",
			message:        "bad value",
			constraintName: "chk_x",
			tableName:      "t1",
			columnName:     "c1",
			wantSubstr:     "chk_x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewConstraintError(tt.code, tt.constraintType, tt.message)
			e.ConstraintName = tt.constraintName
			e.TableName = tt.tableName
			e.ColumnName = tt.columnName
			got := e.Error()
			if !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("expected %q in %q", tt.wantSubstr, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// UniqueViolationError.Error() – line 216 branches
// ---------------------------------------------------------------------------

func TestUniqueViolationError_Error_Branches(t *testing.T) {
	tests := []struct {
		name             string
		indexName        string
		constraintName   string
		tableName        string
		columnName       string
		conflictingValue interface{}
		wantSubstr       string
		wantAbsent       string
	}{
		{
			// IndexName set → uses index branch, not constraintName branch
			name:       "mcdc_indexName_set",
			indexName:  "idx_email",
			tableName:  "users",
			columnName: "email",
			wantSubstr: "on index idx_email",
		},
		{
			// IndexName empty, ConstraintName set → uses constraintName branch
			name:           "mcdc_constraintName_set_no_index",
			constraintName: "uq_email",
			tableName:      "users",
			columnName:     "email",
			wantSubstr:     "(uq_email)",
		},
		{
			// Both empty → neither branch taken
			name:       "mcdc_both_empty",
			tableName:  "users",
			columnName: "email",
			wantSubstr: "UNIQUE constraint violation",
			wantAbsent: "on index",
		},
		{
			// ConflictingValue nil → value branch not taken
			name:       "mcdc_conflictingValue_nil",
			tableName:  "users",
			columnName: "email",
			wantAbsent: "already exists",
		},
		{
			// ConflictingValue set
			name:             "mcdc_conflictingValue_set",
			tableName:        "users",
			columnName:       "email",
			conflictingValue: "a@b.com",
			wantSubstr:       "already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &UniqueViolationError{
				ConstraintError: ConstraintError{
					SQLiteError: SQLiteError{
						Code:    SQLITE_CONSTRAINT_UNIQUE,
						Message: "",
					},
					ConstraintType: "UNIQUE",
					ConstraintName: tt.constraintName,
					TableName:      tt.tableName,
					ColumnName:     tt.columnName,
				},
				ConflictingValue: tt.conflictingValue,
				IndexName:        tt.indexName,
			}
			got := e.Error()
			if tt.wantSubstr != "" && !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("expected %q in %q", tt.wantSubstr, got)
			}
			if tt.wantAbsent != "" && strings.Contains(got, tt.wantAbsent) {
				t.Errorf("expected %q absent from %q", tt.wantAbsent, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// addConstraintName, addTableAndColumn, addParentReference, addAction,
// addChildValue – lines 278-315: the "empty" branch was missing in each
// ---------------------------------------------------------------------------

func TestAddConstraintName(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
		wantChanged    bool
	}{
		{"mcdc_empty", "", false},
		{"mcdc_non_empty", "chk_foo", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := "FOREIGN KEY constraint violation"
			got := addConstraintName(base, tt.constraintName)
			if tt.wantChanged && got == base {
				t.Errorf("expected message to change, got: %s", got)
			}
			if !tt.wantChanged && got != base {
				t.Errorf("expected message unchanged, got: %s", got)
			}
		})
	}
}

func TestAddTableAndColumn(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		columnName  string
		wantChanged bool
	}{
		{"mcdc_both_empty", "", "", false},
		{"mcdc_table_only", "t1", "", false},
		{"mcdc_column_only", "", "c1", false},
		{"mcdc_both_set", "t1", "c1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := "FOREIGN KEY constraint violation"
			got := addTableAndColumn(base, tt.tableName, tt.columnName)
			if tt.wantChanged && got == base {
				t.Errorf("expected message to change, got: %s", got)
			}
			if !tt.wantChanged && got != base {
				t.Errorf("expected message unchanged, got: %s", got)
			}
		})
	}
}

func TestAddParentReference(t *testing.T) {
	tests := []struct {
		name         string
		parentTable  string
		parentColumn string
		wantChanged  bool
	}{
		{"mcdc_both_empty", "", "", false},
		{"mcdc_parent_table_only", "parent", "", false},
		{"mcdc_parent_column_only", "", "id", false},
		{"mcdc_both_set", "parent", "id", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := "FOREIGN KEY constraint violation"
			got := addParentReference(base, tt.parentTable, tt.parentColumn)
			if tt.wantChanged && got == base {
				t.Errorf("expected message to change, got: %s", got)
			}
			if !tt.wantChanged && got != base {
				t.Errorf("expected message unchanged, got: %s", got)
			}
		})
	}
}

func TestAddAction(t *testing.T) {
	tests := []struct {
		name        string
		action      string
		wantChanged bool
	}{
		{"mcdc_empty", "", false},
		{"mcdc_non_empty", "DELETE", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := "FOREIGN KEY constraint violation"
			got := addAction(base, tt.action)
			if tt.wantChanged && got == base {
				t.Errorf("expected message to change, got: %s", got)
			}
			if !tt.wantChanged && got != base {
				t.Errorf("expected message unchanged, got: %s", got)
			}
		})
	}
}

func TestAddChildValue(t *testing.T) {
	tests := []struct {
		name        string
		childValue  interface{}
		wantChanged bool
	}{
		{"mcdc_nil", nil, false},
		{"mcdc_non_nil", 42, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := "FOREIGN KEY constraint violation"
			got := addChildValue(base, tt.childValue)
			if tt.wantChanged && got == base {
				t.Errorf("expected message to change, got: %s", got)
			}
			if !tt.wantChanged && got != base {
				t.Errorf("expected message unchanged, got: %s", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// LockError.Error() – line 444
// ---------------------------------------------------------------------------

func TestLockError_Error_Branches(t *testing.T) {
	tests := []struct {
		name       string
		build      func() *LockError
		wantSubstr string
	}{
		{
			// IsBusy = true → "database is busy"
			name:       "mcdc_isBusy",
			build:      func() *LockError { return NewLockBusyError("test.db") },
			wantSubstr: "database is busy",
		},
		{
			// IsTimeout = true, IsBusy = false → "lock timeout"
			// NewLockTimeoutError uses SQLITE_BUSY which also sets IsBusy, so we
			// construct directly to isolate the IsTimeout-only branch.
			name: "mcdc_isTimeout",
			build: func() *LockError {
				return &LockError{
					SQLiteError: SQLiteError{Code: SQLITE_LOCKED, Message: "timeout acquiring lock"},
					LockType:    "EXCLUSIVE",
					Database:    "test.db",
					IsTimeout:   true,
					IsBusy:      false,
				}
			},
			wantSubstr: "lock timeout",
		},
		{
			// Neither busy nor timeout → "database lock error"
			name: "mcdc_generic_lock_error",
			build: func() *LockError {
				return NewLockError(SQLITE_LOCKED, "", "main.db", "")
			},
			wantSubstr: "database lock error",
		},
		{
			// LockType set → "failed to acquire X lock"
			name: "mcdc_lockType_set",
			build: func() *LockError {
				return NewLockError(SQLITE_LOCKED, "SHARED", "main.db", "")
			},
			wantSubstr: "failed to acquire SHARED lock",
		},
		{
			// LockType empty → no "failed to acquire" in output
			name: "mcdc_lockType_empty",
			build: func() *LockError {
				return NewLockError(SQLITE_LOCKED, "", "main.db", "")
			},
			wantSubstr: "main.db",
		},
		{
			// Database set → database name in output
			name: "mcdc_database_set",
			build: func() *LockError {
				return NewLockError(SQLITE_LOCKED, "", "prod.db", "")
			},
			wantSubstr: "prod.db",
		},
		{
			// Database empty → no database name in output
			name: "mcdc_database_empty",
			build: func() *LockError {
				return NewLockError(SQLITE_LOCKED, "", "", "")
			},
			wantSubstr: "database lock error",
		},
		{
			// Message set
			name: "mcdc_message_set",
			build: func() *LockError {
				return NewLockError(SQLITE_LOCKED, "", "", "custom msg")
			},
			wantSubstr: "custom msg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := tt.build()
			got := e.Error()
			if !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("expected %q in %q", tt.wantSubstr, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IOError.Error() – line 531
// ---------------------------------------------------------------------------

func TestIOError_Error_Branches(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		path       string
		message    string
		err        error
		wantSubstr string
		wantAbsent string
	}{
		{
			// all empty
			name:       "mcdc_all_empty",
			wantSubstr: "I/O error",
			wantAbsent: "during",
		},
		{
			// operation set
			name:       "mcdc_operation_set",
			operation:  "write",
			wantSubstr: "during write",
		},
		{
			// operation empty → no "during" in output
			name:       "mcdc_operation_empty",
			wantAbsent: "during",
		},
		{
			// path set
			name:       "mcdc_path_set",
			path:       "/var/db/data.db",
			wantSubstr: "/var/db/data.db",
		},
		{
			// path empty → path not in output
			name:       "mcdc_path_empty",
			wantAbsent: "/var/db/data.db",
		},
		{
			// message set
			name:       "mcdc_message_set",
			message:    "read failed",
			wantSubstr: "read failed",
		},
		{
			// wrapped error set
			name:       "mcdc_wrapped_err_set",
			err:        fmt.Errorf("underlying cause"),
			wantSubstr: "underlying cause",
		},
		{
			// wrapped error nil → no underlying cause
			name:       "mcdc_wrapped_err_nil",
			wantAbsent: "underlying cause",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewIOError(SQLITE_IOERR_READ, tt.operation, tt.path, tt.message, tt.err)
			got := e.Error()
			if tt.wantSubstr != "" && !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("expected %q in %q", tt.wantSubstr, got)
			}
			if tt.wantAbsent != "" && strings.Contains(got, tt.wantAbsent) {
				t.Errorf("expected %q absent from %q", tt.wantAbsent, got)
			}
		})
	}
}
