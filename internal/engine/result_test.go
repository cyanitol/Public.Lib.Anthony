// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package engine

import (
	"io"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// TestResultRowCount tests the RowCount method
func TestResultRowCount(t *testing.T) {
	tests := []struct {
		name     string
		rows     [][]interface{}
		expected int
	}{
		{"empty result", [][]interface{}{}, 0},
		{"one row", [][]interface{}{{1, "test"}}, 1},
		{"multiple rows", [][]interface{}{{1}, {2}, {3}}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Result{Rows: tt.rows}
			if got := r.RowCount(); got != tt.expected {
				t.Errorf("RowCount() = %d, want %d", got, tt.expected)
			}
		})
	}
}

// TestResultColumnCount tests the ColumnCount method
func TestResultColumnCount(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		expected int
	}{
		{"no columns", []string{}, 0},
		{"one column", []string{"id"}, 1},
		{"multiple columns", []string{"id", "name", "age"}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Result{Columns: tt.columns}
			if got := r.ColumnCount(); got != tt.expected {
				t.Errorf("ColumnCount() = %d, want %d", got, tt.expected)
			}
		})
	}
}

// TestRowsColumns tests the Columns method
func TestRowsColumns(t *testing.T) {
	columns := []string{"id", "name"}
	rows := &Rows{columns: columns}

	got := rows.Columns()
	if len(got) != len(columns) {
		t.Errorf("Columns() returned %d columns, want %d", len(got), len(columns))
	}
	for i := range columns {
		if got[i] != columns[i] {
			t.Errorf("Columns()[%d] = %s, want %s", i, got[i], columns[i])
		}
	}
}

// TestRowsErr tests the Err method
func TestRowsErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"no error", nil},
		{"with error", io.EOF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := &Rows{err: tt.err}
			if got := rows.Err(); got != tt.err {
				t.Errorf("Err() = %v, want %v", got, tt.err)
			}
		})
	}
}

// TestRowsCloseWhenDone tests Close when rows are already done
func TestRowsCloseWhenDone(t *testing.T) {
	rows := &Rows{done: true}
	if err := rows.Close(); err != nil {
		t.Errorf("Close() on done rows returned error: %v", err)
	}
}

// TestRowsCloseWhenNotDone tests Close when rows are not done
func TestRowsCloseWhenNotDone(t *testing.T) {
	vm := vdbe.New()
	rows := &Rows{vdbe: vm, done: false}

	// Close should finalize the VDBE
	if err := rows.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	if !rows.done {
		t.Error("Close() should set done to true")
	}
}

// TestRowsCloseNilVdbe tests Close with nil VDBE
func TestRowsCloseNilVdbe(t *testing.T) {
	rows := &Rows{vdbe: nil, done: false}
	if err := rows.Close(); err != nil {
		t.Errorf("Close() with nil VDBE returned error: %v", err)
	}
}

// TestRowsScanNilRow tests Scan with no current row
func TestRowsScanNilRow(t *testing.T) {
	rows := &Rows{currentRow: nil}
	var val int
	err := rows.Scan(&val)
	if err == nil {
		t.Error("Scan() with nil currentRow should return error")
	}
}

// TestRowsScanWrongDestCount tests Scan with wrong number of destinations
func TestRowsScanWrongDestCount(t *testing.T) {
	mem1 := vdbe.NewMem()
	mem1.SetInt(42)
	mem2 := vdbe.NewMem()
	mem2.SetStr("test")

	rows := &Rows{currentRow: []*vdbe.Mem{mem1, mem2}}
	var val int
	err := rows.Scan(&val) // Only 1 dest, but 2 columns
	if err == nil {
		t.Error("Scan() with wrong destination count should return error")
	}
}

// TestScanTypedInt tests scanTyped with int
func TestScanTypedInt(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(42)

	var val int
	handled, err := scanTyped(mem, &val)
	if err != nil {
		t.Fatalf("scanTyped() returned error: %v", err)
	}
	if !handled {
		t.Error("scanTyped() should have handled *int")
	}
	if val != 42 {
		t.Errorf("scanTyped() set val = %d, want 42", val)
	}
}

// TestScanTypedInt64 tests scanTyped with int64
func TestScanTypedInt64(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(123)

	var val int64
	handled, err := scanTyped(mem, &val)
	if err != nil {
		t.Fatalf("scanTyped() returned error: %v", err)
	}
	if !handled {
		t.Error("scanTyped() should have handled *int64")
	}
	if val != 123 {
		t.Errorf("scanTyped() set val = %d, want 123", val)
	}
}

// TestScanTypedFloat64 tests scanTyped with float64
func TestScanTypedFloat64(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetReal(3.14)

	var val float64
	handled, err := scanTyped(mem, &val)
	if err != nil {
		t.Fatalf("scanTyped() returned error: %v", err)
	}
	if !handled {
		t.Error("scanTyped() should have handled *float64")
	}
	if val != 3.14 {
		t.Errorf("scanTyped() set val = %f, want 3.14", val)
	}
}

// TestScanTypedString tests scanTyped with string
func TestScanTypedString(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetStr("hello")

	var val string
	handled, err := scanTyped(mem, &val)
	if err != nil {
		t.Fatalf("scanTyped() returned error: %v", err)
	}
	if !handled {
		t.Error("scanTyped() should have handled *string")
	}
	if val != "hello" {
		t.Errorf("scanTyped() set val = %s, want hello", val)
	}
}

// TestScanTypedBytes tests scanTyped with []byte
func TestScanTypedBytes(t *testing.T) {
	mem := vdbe.NewMem()
	data := []byte{1, 2, 3}
	mem.SetBlob(data)

	var val []byte
	handled, err := scanTyped(mem, &val)
	if err != nil {
		t.Fatalf("scanTyped() returned error: %v", err)
	}
	if !handled {
		t.Error("scanTyped() should have handled *[]byte")
	}
	if len(val) != len(data) {
		t.Errorf("scanTyped() set val with length %d, want %d", len(val), len(data))
	}
}

// TestScanTypedBool tests scanTyped with bool
func TestScanTypedBool(t *testing.T) {
	tests := []struct {
		name     string
		intVal   int64
		expected bool
	}{
		{"true", 1, true},
		{"false", 0, false},
		{"true non-zero", 42, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := vdbe.NewMem()
			mem.SetInt(tt.intVal)

			var val bool
			handled, err := scanTyped(mem, &val)
			if err != nil {
				t.Fatalf("scanTyped() returned error: %v", err)
			}
			if !handled {
				t.Error("scanTyped() should have handled *bool")
			}
			if val != tt.expected {
				t.Errorf("scanTyped() set val = %v, want %v", val, tt.expected)
			}
		})
	}
}

// TestScanTypedUnsupported tests scanTyped with unsupported type
func TestScanTypedUnsupported(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(42)

	var val uint32 // Unsupported type
	handled, err := scanTyped(mem, &val)
	if err != nil {
		t.Fatalf("scanTyped() returned unexpected error: %v", err)
	}
	if handled {
		t.Error("scanTyped() should not have handled uint32")
	}
}

// TestScanIntoNilMem tests scanInto with nil mem
func TestScanIntoNilMem(t *testing.T) {
	var val int
	err := scanInto(nil, &val)
	if err == nil {
		t.Error("scanInto() with nil mem should return error")
	}
}

// TestScanIntoInterface tests scanInto with *interface{}
func TestScanIntoInterface(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(42)

	var val interface{}
	err := scanInto(mem, &val)
	if err != nil {
		t.Fatalf("scanInto() returned error: %v", err)
	}
	if val != int64(42) {
		t.Errorf("scanInto() set val = %v, want 42", val)
	}
}

// TestScanIntoUnsupportedType tests scanInto with unsupported type
func TestScanIntoUnsupportedType(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(42)

	var val complex128 // Unsupported type
	err := scanInto(mem, &val)
	if err == nil {
		t.Error("scanInto() with unsupported type should return error")
	}
}

// TestTxCommit tests transaction commit
func TestTxCommit(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestTxCommitAlreadyDone tests committing an already finished transaction
func TestTxCommitAlreadyDone(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestTxRollback tests transaction rollback
func TestTxRollback(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestTxRollbackAlreadyDone tests rolling back an already finished transaction
func TestTxRollbackAlreadyDone(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestTxExecuteWhenDone tests executing on finished transaction
func TestTxExecuteWhenDone(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestTxQueryWhenDone tests querying on finished transaction
func TestTxQueryWhenDone(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestTxExecWhenDone tests exec on finished transaction
func TestTxExecWhenDone(t *testing.T) {
	// Skip - requires full transaction support
	t.Skip("Requires full transaction support")
}

// TestPreparedStmtClose tests closing a prepared statement
func TestPreparedStmtClose(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	if !stmt.closed {
		t.Error("Close() should set closed to true")
	}
}

// TestPreparedStmtCloseAlreadyClosed tests closing an already closed statement
func TestPreparedStmtCloseAlreadyClosed(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	stmt.Close()
	err = stmt.Close() // Second close
	if err != nil {
		t.Error("Second Close() should not return error")
	}
}

// TestPreparedStmtExecuteWhenClosed tests executing a closed statement
func TestPreparedStmtExecuteWhenClosed(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	stmt.Close()
	_, err = stmt.Execute()
	if err == nil {
		t.Error("Execute() on closed statement should return error")
	}
}

// TestPreparedStmtQueryWhenClosed tests querying a closed statement
func TestPreparedStmtQueryWhenClosed(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	stmt.Close()
	_, err = stmt.Query()
	if err == nil {
		t.Error("Query() on closed statement should return error")
	}
}

// TestPreparedStmtSQL tests getting SQL text
func TestPreparedStmtSQL(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	sql := "SELECT 1"
	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	if got := stmt.SQL(); got != sql {
		t.Errorf("SQL() = %s, want %s", got, sql)
	}
}

// TestQueryRowScanError tests QueryRow.Scan with error
func TestQueryRowScanError(t *testing.T) {
	qr := &QueryRow{err: io.EOF}
	var val int
	err := qr.Scan(&val)
	if err != io.EOF {
		t.Errorf("Scan() with error should return that error, got %v", err)
	}
}

// TestQueryRowScanNoRows tests QueryRow.Scan when no rows
func TestQueryRowScanNoRows(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query empty table
	var id int
	err = db.QueryRow("SELECT id FROM test").Scan(&id)
	if err != io.EOF {
		t.Errorf("Scan() on empty result should return EOF, got %v", err)
	}
}

// TestMemToInterfaceNull tests memToInterface with NULL
func TestMemToInterfaceNull(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetNull()

	result := memToInterface(mem)
	if result != nil {
		t.Errorf("memToInterface(NULL) = %v, want nil", result)
	}
}

// TestMemToInterfaceInt tests memToInterface with integer
func TestMemToInterfaceInt(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(42)

	result := memToInterface(mem)
	if result != int64(42) {
		t.Errorf("memToInterface(INT) = %v, want 42", result)
	}
}

// TestMemToInterfaceReal tests memToInterface with real
func TestMemToInterfaceReal(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetReal(3.14)

	result := memToInterface(mem)
	if result != 3.14 {
		t.Errorf("memToInterface(REAL) = %v, want 3.14", result)
	}
}

// TestMemToInterfaceStr tests memToInterface with string
func TestMemToInterfaceStr(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetStr("test")

	result := memToInterface(mem)
	if result != "test" {
		t.Errorf("memToInterface(STR) = %v, want test", result)
	}
}

// TestMemToInterfaceBlob tests memToInterface with blob
func TestMemToInterfaceBlob(t *testing.T) {
	mem := vdbe.NewMem()
	data := []byte{1, 2, 3}
	mem.SetBlob(data)

	result := memToInterface(mem)
	blob, ok := result.([]byte)
	if !ok {
		t.Fatalf("memToInterface(BLOB) returned %T, want []byte", result)
	}
	if len(blob) != len(data) {
		t.Errorf("memToInterface(BLOB) returned blob with length %d, want %d", len(blob), len(data))
	}
}

// TestMemToInterfaceNil tests memToInterface with nil
func TestMemToInterfaceNil(t *testing.T) {
	result := memToInterface(nil)
	if result != nil {
		t.Errorf("memToInterface(nil) = %v, want nil", result)
	}
}
