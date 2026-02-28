package errors

import (
	"fmt"
)

// SQLite-compatible error codes
// These match the extended result codes from SQLite3.
// Reference: https://www.sqlite.org/rescode.html
const (
	// Primary result codes (low byte)
	SQLITE_OK         = 0  // Successful result
	SQLITE_ERROR      = 1  // Generic error
	SQLITE_INTERNAL   = 2  // Internal logic error in SQLite
	SQLITE_PERM       = 3  // Access permission denied
	SQLITE_ABORT      = 4  // Callback routine requested an abort
	SQLITE_BUSY       = 5  // The database file is locked
	SQLITE_LOCKED     = 6  // A table in the database is locked
	SQLITE_NOMEM      = 7  // A malloc() failed
	SQLITE_READONLY   = 8  // Attempt to write a readonly database
	SQLITE_INTERRUPT  = 9  // Operation terminated by sqlite3_interrupt()
	SQLITE_IOERR      = 10 // Some kind of disk I/O error occurred
	SQLITE_CORRUPT    = 11 // The database disk image is malformed
	SQLITE_NOTFOUND   = 12 // Unknown opcode in sqlite3_file_control()
	SQLITE_FULL       = 13 // Insertion failed because database is full
	SQLITE_CANTOPEN   = 14 // Unable to open the database file
	SQLITE_PROTOCOL   = 15 // Database lock protocol error
	SQLITE_EMPTY      = 16 // Internal use only
	SQLITE_SCHEMA     = 17 // The database schema changed
	SQLITE_TOOBIG     = 18 // String or BLOB exceeds size limit
	SQLITE_CONSTRAINT = 19 // Abort due to constraint violation
	SQLITE_MISMATCH   = 20 // Data type mismatch
	SQLITE_MISUSE     = 21 // Library used incorrectly
	SQLITE_NOLFS      = 22 // Uses OS features not supported on host
	SQLITE_AUTH       = 23 // Authorization denied
	SQLITE_FORMAT     = 24 // Not used
	SQLITE_RANGE      = 25 // 2nd parameter to sqlite3_bind out of range
	SQLITE_NOTADB     = 26 // File opened that is not a database file
	SQLITE_NOTICE     = 27 // Notifications from sqlite3_log()
	SQLITE_WARNING    = 28 // Warnings from sqlite3_log()
	SQLITE_ROW        = 100 // sqlite3_step() has another row ready
	SQLITE_DONE       = 101 // sqlite3_step() has finished executing

	// Extended result codes for SQLITE_CONSTRAINT (19)
	SQLITE_CONSTRAINT_CHECK       = (SQLITE_CONSTRAINT | (1 << 8))  // 275
	SQLITE_CONSTRAINT_COMMITHOOK  = (SQLITE_CONSTRAINT | (2 << 8))  // 531
	SQLITE_CONSTRAINT_FOREIGNKEY  = (SQLITE_CONSTRAINT | (3 << 8))  // 787
	SQLITE_CONSTRAINT_FUNCTION    = (SQLITE_CONSTRAINT | (4 << 8))  // 1043
	SQLITE_CONSTRAINT_NOTNULL     = (SQLITE_CONSTRAINT | (5 << 8))  // 1299
	SQLITE_CONSTRAINT_PRIMARYKEY  = (SQLITE_CONSTRAINT | (6 << 8))  // 1555
	SQLITE_CONSTRAINT_TRIGGER     = (SQLITE_CONSTRAINT | (7 << 8))  // 1811
	SQLITE_CONSTRAINT_UNIQUE      = (SQLITE_CONSTRAINT | (8 << 8))  // 2067
	SQLITE_CONSTRAINT_VTAB        = (SQLITE_CONSTRAINT | (9 << 8))  // 2323
	SQLITE_CONSTRAINT_ROWID       = (SQLITE_CONSTRAINT | (10 << 8)) // 2579

	// Extended result codes for SQLITE_IOERR (10)
	SQLITE_IOERR_READ              = (SQLITE_IOERR | (1 << 8))  // 266
	SQLITE_IOERR_SHORT_READ        = (SQLITE_IOERR | (2 << 8))  // 522
	SQLITE_IOERR_WRITE             = (SQLITE_IOERR | (3 << 8))  // 778
	SQLITE_IOERR_FSYNC             = (SQLITE_IOERR | (4 << 8))  // 1034
	SQLITE_IOERR_DIR_FSYNC         = (SQLITE_IOERR | (5 << 8))  // 1290
	SQLITE_IOERR_TRUNCATE          = (SQLITE_IOERR | (6 << 8))  // 1546
	SQLITE_IOERR_FSTAT             = (SQLITE_IOERR | (7 << 8))  // 1802
	SQLITE_IOERR_UNLOCK            = (SQLITE_IOERR | (8 << 8))  // 2058
	SQLITE_IOERR_RDLOCK            = (SQLITE_IOERR | (9 << 8))  // 2314
	SQLITE_IOERR_DELETE            = (SQLITE_IOERR | (10 << 8)) // 2570
	SQLITE_IOERR_BLOCKED           = (SQLITE_IOERR | (11 << 8)) // 2826
	SQLITE_IOERR_NOMEM             = (SQLITE_IOERR | (12 << 8)) // 3082
	SQLITE_IOERR_ACCESS            = (SQLITE_IOERR | (13 << 8)) // 3338
	SQLITE_IOERR_CHECKRESERVEDLOCK = (SQLITE_IOERR | (14 << 8)) // 3594
	SQLITE_IOERR_LOCK              = (SQLITE_IOERR | (15 << 8)) // 3850
	SQLITE_IOERR_CLOSE             = (SQLITE_IOERR | (16 << 8)) // 4106
	SQLITE_IOERR_DIR_CLOSE         = (SQLITE_IOERR | (17 << 8)) // 4362
	SQLITE_IOERR_SHMOPEN           = (SQLITE_IOERR | (18 << 8)) // 4618
	SQLITE_IOERR_SHMSIZE           = (SQLITE_IOERR | (19 << 8)) // 4874
	SQLITE_IOERR_SHMLOCK           = (SQLITE_IOERR | (20 << 8)) // 5130
	SQLITE_IOERR_SHMMAP            = (SQLITE_IOERR | (21 << 8)) // 5386
	SQLITE_IOERR_SEEK              = (SQLITE_IOERR | (22 << 8)) // 5642
	SQLITE_IOERR_DELETE_NOENT      = (SQLITE_IOERR | (23 << 8)) // 5898
	SQLITE_IOERR_MMAP              = (SQLITE_IOERR | (24 << 8)) // 6154
	SQLITE_IOERR_GETTEMPPATH       = (SQLITE_IOERR | (25 << 8)) // 6410
	SQLITE_IOERR_CONVPATH          = (SQLITE_IOERR | (26 << 8)) // 6666

	// Extended result codes for SQLITE_LOCKED (6)
	SQLITE_LOCKED_SHAREDCACHE = (SQLITE_LOCKED | (1 << 8)) // 262

	// Extended result codes for SQLITE_BUSY (5)
	SQLITE_BUSY_RECOVERY = (SQLITE_BUSY | (1 << 8)) // 261
	SQLITE_BUSY_SNAPSHOT = (SQLITE_BUSY | (2 << 8)) // 517

	// Extended result codes for SQLITE_CORRUPT (11)
	SQLITE_CORRUPT_VTAB   = (SQLITE_CORRUPT | (1 << 8)) // 267
	SQLITE_CORRUPT_INDEX  = (SQLITE_CORRUPT | (2 << 8)) // 523
	SQLITE_CORRUPT_SEQUENCE = (SQLITE_CORRUPT | (3 << 8)) // 779

	// Extended result codes for SQLITE_READONLY (8)
	SQLITE_READONLY_RECOVERY     = (SQLITE_READONLY | (1 << 8)) // 264
	SQLITE_READONLY_CANTLOCK     = (SQLITE_READONLY | (2 << 8)) // 520
	SQLITE_READONLY_ROLLBACK     = (SQLITE_READONLY | (3 << 8)) // 776
	SQLITE_READONLY_DBMOVED      = (SQLITE_READONLY | (4 << 8)) // 1032
	SQLITE_READONLY_CANTINIT     = (SQLITE_READONLY | (5 << 8)) // 1288
	SQLITE_READONLY_DIRECTORY    = (SQLITE_READONLY | (6 << 8)) // 1544

	// Extended result codes for SQLITE_CANTOPEN (14)
	SQLITE_CANTOPEN_NOTEMPDIR  = (SQLITE_CANTOPEN | (1 << 8)) // 270
	SQLITE_CANTOPEN_ISDIR      = (SQLITE_CANTOPEN | (2 << 8)) // 526
	SQLITE_CANTOPEN_FULLPATH   = (SQLITE_CANTOPEN | (3 << 8)) // 782
	SQLITE_CANTOPEN_CONVPATH   = (SQLITE_CANTOPEN | (4 << 8)) // 1038
	SQLITE_CANTOPEN_DIRTYWAL   = (SQLITE_CANTOPEN | (5 << 8)) // 1294
	SQLITE_CANTOPEN_SYMLINK    = (SQLITE_CANTOPEN | (6 << 8)) // 1550

	// Extended result codes for SQLITE_ABORT (4)
	SQLITE_ABORT_ROLLBACK = (SQLITE_ABORT | (2 << 8)) // 516

	// Extended result codes for SQLITE_NOTICE (27)
	SQLITE_NOTICE_RECOVER_WAL      = (SQLITE_NOTICE | (1 << 8)) // 283
	SQLITE_NOTICE_RECOVER_ROLLBACK = (SQLITE_NOTICE | (2 << 8)) // 539

	// Extended result codes for SQLITE_WARNING (28)
	SQLITE_WARNING_AUTOINDEX = (SQLITE_WARNING | (1 << 8)) // 284

	// Extended result codes for SQLITE_AUTH (23)
	SQLITE_AUTH_USER = (SQLITE_AUTH | (1 << 8)) // 279
)

// SQLiteError represents an error with a SQLite-compatible error code.
type SQLiteError struct {
	Code    int    // SQLite error code
	Message string // Human-readable error message
	Err     error  // Wrapped underlying error, if any
}

// Error implements the error interface.
func (e *SQLiteError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s (code %d): %v", e.Message, e.Code, e.Err)
	}
	return fmt.Sprintf("%s (code %d)", e.Message, e.Code)
}

// Unwrap implements the error unwrapping interface.
func (e *SQLiteError) Unwrap() error {
	return e.Err
}

// GetCode returns the SQLite error code.
func (e *SQLiteError) GetCode() int {
	return e.Code
}

// NewSQLiteError creates a new SQLiteError with the given code and message.
func NewSQLiteError(code int, message string) *SQLiteError {
	return &SQLiteError{
		Code:    code,
		Message: message,
	}
}

// WrapSQLiteError wraps an existing error with a SQLite error code.
func WrapSQLiteError(code int, message string, err error) *SQLiteError {
	return &SQLiteError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// ConstraintError represents a constraint violation error.
// This is the base type for all constraint violations.
type ConstraintError struct {
	SQLiteError
	ConstraintType string // Type of constraint (CHECK, NOT NULL, UNIQUE, etc.)
	ConstraintName string // Name of the constraint, if named
	TableName      string // Name of the table
	ColumnName     string // Name of the column, if applicable
}

// Error implements the error interface.
func (e *ConstraintError) Error() string {
	msg := fmt.Sprintf("%s constraint violation", e.ConstraintType)
	if e.ConstraintName != "" {
		msg = fmt.Sprintf("%s (%s)", msg, e.ConstraintName)
	}
	if e.TableName != "" {
		msg = fmt.Sprintf("%s on table %s", msg, e.TableName)
	}
	if e.ColumnName != "" {
		msg = fmt.Sprintf("%s column %s", msg, e.ColumnName)
	}
	if e.Message != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Message)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewConstraintError creates a new ConstraintError.
func NewConstraintError(code int, constraintType, message string) *ConstraintError {
	return &ConstraintError{
		SQLiteError: SQLiteError{
			Code:    code,
			Message: message,
		},
		ConstraintType: constraintType,
	}
}

// UniqueViolationError represents a UNIQUE constraint violation.
type UniqueViolationError struct {
	ConstraintError
	ConflictingValue interface{} // The value that caused the conflict
	IndexName        string       // Name of the unique index, if applicable
}

// Error implements the error interface.
func (e *UniqueViolationError) Error() string {
	msg := "UNIQUE constraint violation"
	if e.IndexName != "" {
		msg = fmt.Sprintf("%s on index %s", msg, e.IndexName)
	} else if e.ConstraintName != "" {
		msg = fmt.Sprintf("%s (%s)", msg, e.ConstraintName)
	}
	if e.TableName != "" {
		msg = fmt.Sprintf("%s on table %s", msg, e.TableName)
	}
	if e.ColumnName != "" {
		msg = fmt.Sprintf("%s column %s", msg, e.ColumnName)
	}
	if e.ConflictingValue != nil {
		msg = fmt.Sprintf("%s: value %v already exists", msg, e.ConflictingValue)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewUniqueViolationError creates a new UniqueViolationError.
func NewUniqueViolationError(tableName, columnName string, value interface{}) *UniqueViolationError {
	return &UniqueViolationError{
		ConstraintError: ConstraintError{
			SQLiteError: SQLiteError{
				Code:    SQLITE_CONSTRAINT_UNIQUE,
				Message: "UNIQUE constraint failed",
			},
			ConstraintType: "UNIQUE",
			TableName:      tableName,
			ColumnName:     columnName,
		},
		ConflictingValue: value,
	}
}

// ForeignKeyError represents a FOREIGN KEY constraint violation.
type ForeignKeyError struct {
	ConstraintError
	ParentTable  string      // Referenced parent table
	ParentColumn string      // Referenced parent column
	ChildValue   interface{} // Value that doesn't have a matching parent
	Action       string      // Action that triggered the error (INSERT, UPDATE, DELETE)
}

// Error implements the error interface.
func (e *ForeignKeyError) Error() string {
	msg := "FOREIGN KEY constraint violation"
	if e.ConstraintName != "" {
		msg = fmt.Sprintf("%s (%s)", msg, e.ConstraintName)
	}
	if e.TableName != "" && e.ColumnName != "" {
		msg = fmt.Sprintf("%s: %s.%s", msg, e.TableName, e.ColumnName)
	}
	if e.ParentTable != "" && e.ParentColumn != "" {
		msg = fmt.Sprintf("%s references %s.%s", msg, e.ParentTable, e.ParentColumn)
	}
	if e.Action != "" {
		msg = fmt.Sprintf("%s during %s", msg, e.Action)
	}
	if e.ChildValue != nil {
		msg = fmt.Sprintf("%s: value %v not found in parent table", msg, e.ChildValue)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewForeignKeyError creates a new ForeignKeyError.
func NewForeignKeyError(tableName, columnName, parentTable, parentColumn string, action string) *ForeignKeyError {
	return &ForeignKeyError{
		ConstraintError: ConstraintError{
			SQLiteError: SQLiteError{
				Code:    SQLITE_CONSTRAINT_FOREIGNKEY,
				Message: "FOREIGN KEY constraint failed",
			},
			ConstraintType: "FOREIGN KEY",
			TableName:      tableName,
			ColumnName:     columnName,
		},
		ParentTable:  parentTable,
		ParentColumn: parentColumn,
		Action:       action,
	}
}

// NotNullError represents a NOT NULL constraint violation.
type NotNullError struct {
	ConstraintError
}

// NewNotNullError creates a new NotNullError.
func NewNotNullError(tableName, columnName string) *NotNullError {
	return &NotNullError{
		ConstraintError: ConstraintError{
			SQLiteError: SQLiteError{
				Code:    SQLITE_CONSTRAINT_NOTNULL,
				Message: fmt.Sprintf("NOT NULL constraint failed: %s.%s", tableName, columnName),
			},
			ConstraintType: "NOT NULL",
			TableName:      tableName,
			ColumnName:     columnName,
		},
	}
}

// CheckConstraintError represents a CHECK constraint violation.
type CheckConstraintError struct {
	ConstraintError
	CheckExpression string // The check expression that failed
}

// NewCheckConstraintError creates a new CheckConstraintError.
func NewCheckConstraintError(tableName, constraintName, checkExpr string) *CheckConstraintError {
	return &CheckConstraintError{
		ConstraintError: ConstraintError{
			SQLiteError: SQLiteError{
				Code:    SQLITE_CONSTRAINT_CHECK,
				Message: fmt.Sprintf("CHECK constraint failed: %s", constraintName),
			},
			ConstraintType: "CHECK",
			TableName:      tableName,
			ConstraintName: constraintName,
		},
		CheckExpression: checkExpr,
	}
}

// PrimaryKeyError represents a PRIMARY KEY constraint violation.
type PrimaryKeyError struct {
	ConstraintError
	ConflictingValue interface{} // The value that caused the conflict
}

// NewPrimaryKeyError creates a new PrimaryKeyError.
func NewPrimaryKeyError(tableName, columnName string, value interface{}) *PrimaryKeyError {
	return &PrimaryKeyError{
		ConstraintError: ConstraintError{
			SQLiteError: SQLiteError{
				Code:    SQLITE_CONSTRAINT_PRIMARYKEY,
				Message: fmt.Sprintf("PRIMARY KEY constraint failed: %s.%s", tableName, columnName),
			},
			ConstraintType: "PRIMARY KEY",
			TableName:      tableName,
			ColumnName:     columnName,
		},
		ConflictingValue: value,
	}
}

// SyntaxError represents a SQL syntax error.
type SyntaxError struct {
	SQLiteError
	Position int    // Position in the SQL string where the error occurred
	SQL      string // The SQL statement that caused the error
	Token    string // The token that caused the error, if known
}

// Error implements the error interface.
func (e *SyntaxError) Error() string {
	msg := "SQL syntax error"
	if e.Token != "" {
		msg = fmt.Sprintf("%s near '%s'", msg, e.Token)
	}
	if e.Position > 0 {
		msg = fmt.Sprintf("%s at position %d", msg, e.Position)
	}
	if e.Message != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Message)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewSyntaxError creates a new SyntaxError.
func NewSyntaxError(message, token string, position int) *SyntaxError {
	return &SyntaxError{
		SQLiteError: SQLiteError{
			Code:    SQLITE_ERROR,
			Message: message,
		},
		Token:    token,
		Position: position,
	}
}

// LockError represents a database locking error.
type LockError struct {
	SQLiteError
	LockType   string // Type of lock (SHARED, RESERVED, PENDING, EXCLUSIVE)
	Database   string // Name of the database file
	IsTimeout  bool   // True if the error is due to a timeout
	IsBusy     bool   // True if the database is busy
}

// Error implements the error interface.
func (e *LockError) Error() string {
	msg := "database lock error"
	if e.IsBusy {
		msg = "database is busy"
	} else if e.IsTimeout {
		msg = "lock timeout"
	}
	if e.LockType != "" {
		msg = fmt.Sprintf("%s: failed to acquire %s lock", msg, e.LockType)
	}
	if e.Database != "" {
		msg = fmt.Sprintf("%s on %s", msg, e.Database)
	}
	if e.Message != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Message)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewLockError creates a new LockError.
func NewLockError(code int, lockType, database, message string) *LockError {
	return &LockError{
		SQLiteError: SQLiteError{
			Code:    code,
			Message: message,
		},
		LockType: lockType,
		Database: database,
		IsBusy:   code == SQLITE_BUSY || code == SQLITE_BUSY_RECOVERY || code == SQLITE_BUSY_SNAPSHOT,
		IsTimeout: message != "" && (code == SQLITE_BUSY || code == SQLITE_LOCKED),
	}
}

// NewLockBusyError creates a new LockError for a busy database.
func NewLockBusyError(database string) *LockError {
	return NewLockError(SQLITE_BUSY, "", database, "database is locked")
}

// NewLockTimeoutError creates a new LockError for a lock timeout.
func NewLockTimeoutError(lockType, database string) *LockError {
	return NewLockError(SQLITE_BUSY, lockType, database, "timeout acquiring lock")
}

// CorruptionError represents a database corruption error.
type CorruptionError struct {
	SQLiteError
	PageNumber int    // Page number where corruption was detected
	Component  string // Component that is corrupted (btree, index, etc.)
	Details    string // Additional details about the corruption
}

// Error implements the error interface.
func (e *CorruptionError) Error() string {
	msg := "database corruption detected"
	if e.Component != "" {
		msg = fmt.Sprintf("%s in %s", msg, e.Component)
	}
	if e.PageNumber > 0 {
		msg = fmt.Sprintf("%s at page %d", msg, e.PageNumber)
	}
	if e.Details != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Details)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewCorruptionError creates a new CorruptionError.
func NewCorruptionError(component string, pageNum int, details string) *CorruptionError {
	return &CorruptionError{
		SQLiteError: SQLiteError{
			Code:    SQLITE_CORRUPT,
			Message: "database corruption",
		},
		Component:  component,
		PageNumber: pageNum,
		Details:    details,
	}
}

// IOError represents an I/O error.
type IOError struct {
	SQLiteError
	Operation string // Operation that failed (read, write, fsync, etc.)
	Path      string // File path involved in the operation
}

// Error implements the error interface.
func (e *IOError) Error() string {
	msg := "I/O error"
	if e.Operation != "" {
		msg = fmt.Sprintf("%s during %s", msg, e.Operation)
	}
	if e.Path != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Path)
	}
	if e.Message != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Message)
	}
	if e.Err != nil {
		msg = fmt.Sprintf("%s: %v", msg, e.Err)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewIOError creates a new IOError.
func NewIOError(code int, operation, path, message string, err error) *IOError {
	return &IOError{
		SQLiteError: SQLiteError{
			Code:    code,
			Message: message,
			Err:     err,
		},
		Operation: operation,
		Path:      path,
	}
}

// ReadOnlyError represents an attempt to write to a read-only database.
type ReadOnlyError struct {
	SQLiteError
	Database string // Name of the read-only database
	Reason   string // Reason why the database is read-only
}

// Error implements the error interface.
func (e *ReadOnlyError) Error() string {
	msg := "database is read-only"
	if e.Database != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Database)
	}
	if e.Reason != "" {
		msg = fmt.Sprintf("%s (%s)", msg, e.Reason)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewReadOnlyError creates a new ReadOnlyError.
func NewReadOnlyError(database, reason string) *ReadOnlyError {
	return &ReadOnlyError{
		SQLiteError: SQLiteError{
			Code:    SQLITE_READONLY,
			Message: "attempt to write a readonly database",
		},
		Database: database,
		Reason:   reason,
	}
}

// SchemaError represents a schema-related error.
type SchemaError struct {
	SQLiteError
	SchemaName string // Name of the schema
	ObjectType string // Type of schema object (table, index, etc.)
	ObjectName string // Name of the schema object
}

// Error implements the error interface.
func (e *SchemaError) Error() string {
	msg := "schema error"
	if e.SchemaName != "" {
		msg = fmt.Sprintf("%s in schema %s", msg, e.SchemaName)
	}
	if e.ObjectType != "" && e.ObjectName != "" {
		msg = fmt.Sprintf("%s: %s %s", msg, e.ObjectType, e.ObjectName)
	}
	if e.Message != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Message)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewSchemaError creates a new SchemaError.
func NewSchemaError(schemaName, objectType, objectName, message string) *SchemaError {
	return &SchemaError{
		SQLiteError: SQLiteError{
			Code:    SQLITE_SCHEMA,
			Message: message,
		},
		SchemaName: schemaName,
		ObjectType: objectType,
		ObjectName: objectName,
	}
}

// MisuseError represents incorrect library usage.
type MisuseError struct {
	SQLiteError
	API string // API function that was misused
}

// Error implements the error interface.
func (e *MisuseError) Error() string {
	msg := "library misuse"
	if e.API != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.API)
	}
	if e.Message != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.Message)
	}
	return fmt.Sprintf("%s (code %d)", msg, e.Code)
}

// NewMisuseError creates a new MisuseError.
func NewMisuseError(api, message string) *MisuseError {
	return &MisuseError{
		SQLiteError: SQLiteError{
			Code:    SQLITE_MISUSE,
			Message: message,
		},
		API: api,
	}
}
