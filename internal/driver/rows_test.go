// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"testing"
	"time"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

func TestRowsClose(t *testing.T) {
	rows := &Rows{
		vdbe:   vdbe.New(),
		closed: false,
	}

	// First close should succeed
	if err := rows.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Should be closed
	if !rows.closed {
		t.Error("rows should be closed")
	}

	// VDBE should be nil
	if rows.vdbe != nil {
		t.Error("vdbe should be nil after close")
	}

	// Second close should be safe
	if err := rows.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestRowsColumns(t *testing.T) {
	columns := []string{"id", "name", "value"}
	rows := &Rows{
		columns: columns,
	}

	result := rows.Columns()
	if len(result) != len(columns) {
		t.Errorf("Columns() length = %d, want %d", len(result), len(columns))
	}

	for i, col := range result {
		if col != columns[i] {
			t.Errorf("Columns()[%d] = %s, want %s", i, col, columns[i])
		}
	}
}

func TestRowsNextOnClosed(t *testing.T) {
	rows := &Rows{
		vdbe:   vdbe.New(),
		closed: true,
	}

	dest := make([]driver.Value, 3)
	err := rows.Next(dest)
	if err != io.EOF {
		t.Errorf("Next() on closed rows error = %v, want io.EOF", err)
	}
}

func TestRowsNextContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	rows := &Rows{
		vdbe:   vdbe.New(),
		ctx:    ctx,
		closed: false,
	}

	dest := make([]driver.Value, 3)
	err := rows.Next(dest)
	if err == nil {
		t.Error("Next() with canceled context should return error")
	}
}

func TestRowsNextContextCanceledDuringIteration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Create a VDBE that will take time to iterate
	vm := vdbe.New()

	rows := &Rows{
		vdbe:   vm,
		ctx:    ctx,
		closed: false,
	}

	// Wait for context to be canceled
	time.Sleep(2 * time.Millisecond)

	dest := make([]driver.Value, 3)
	err := rows.Next(dest)
	if err == nil {
		t.Error("Next() with canceled context should return error")
	}
}

func TestRowsCopyResultRowWithNilResultRow(t *testing.T) {
	vm := vdbe.New()
	vm.ResultRow = nil

	rows := &Rows{
		vdbe:   vm,
		ctx:    context.Background(),
		closed: false,
	}

	dest := make([]driver.Value, 3)
	err := rows.copyResultRow(dest)
	if err != io.EOF {
		t.Errorf("copyResultRow() with nil ResultRow error = %v, want io.EOF", err)
	}
}

func TestRowsCopyResultRowWithInsufficientDest(t *testing.T) {
	vm := vdbe.New()
	vm.ResultRow = []*vdbe.Mem{
		vdbe.NewMemInt(1),
		vdbe.NewMemInt(2),
		vdbe.NewMemInt(3),
	}

	rows := &Rows{
		vdbe:   vm,
		ctx:    context.Background(),
		closed: false,
	}

	// Dest too small
	dest := make([]driver.Value, 2)
	err := rows.copyResultRow(dest)
	if err != driver.ErrSkip {
		t.Errorf("copyResultRow() with small dest error = %v, want ErrSkip", err)
	}
}

func TestColumnTypeScanType(t *testing.T) {
	rows := &Rows{}

	// Should return nil for SQLite (dynamic typing)
	result := rows.ColumnTypeScanType(0)
	if result != nil {
		t.Errorf("ColumnTypeScanType() = %v, want nil", result)
	}
}

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	tests := []struct {
		name     string
		mem      *vdbe.Mem
		expected string
	}{
		{
			name:     "NULL",
			mem:      vdbe.NewMemNull(),
			expected: "NULL",
		},
		{
			name:     "INTEGER",
			mem:      vdbe.NewMemInt(42),
			expected: "INTEGER",
		},
		{
			name:     "REAL",
			mem:      vdbe.NewMemReal(3.14),
			expected: "REAL",
		},
		{
			name:     "TEXT",
			mem:      vdbe.NewMemStr("hello"),
			expected: "TEXT",
		},
		{
			name:     "BLOB",
			mem:      vdbe.NewMemBlob([]byte("data")),
			expected: "BLOB",
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			vm := vdbe.New()
			vm.ResultRow = []*vdbe.Mem{tt.mem}

			rows := &Rows{
				vdbe: vm,
			}

			result := rows.ColumnTypeDatabaseTypeName(0)
			if result != tt.expected {
				t.Errorf("ColumnTypeDatabaseTypeName() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestColumnTypeDatabaseTypeNameWithoutResultRow(t *testing.T) {
	vm := vdbe.New()
	vm.ResultRow = nil

	rows := &Rows{
		vdbe: vm,
	}

	result := rows.ColumnTypeDatabaseTypeName(0)
	if result != "" {
		t.Errorf("ColumnTypeDatabaseTypeName() without ResultRow = %s, want empty string", result)
	}
}

func TestColumnTypeDatabaseTypeNameOutOfBounds(t *testing.T) {
	vm := vdbe.New()
	vm.ResultRow = []*vdbe.Mem{
		vdbe.NewMemInt(42),
	}

	rows := &Rows{
		vdbe: vm,
	}

	// Index out of bounds
	result := rows.ColumnTypeDatabaseTypeName(5)
	if result != "" {
		t.Errorf("ColumnTypeDatabaseTypeName() out of bounds = %s, want empty string", result)
	}
}

func TestMemToValue(t *testing.T) {
	tests := []struct {
		name     string
		mem      *vdbe.Mem
		expected driver.Value
	}{
		{
			name:     "NULL",
			mem:      vdbe.NewMemNull(),
			expected: nil,
		},
		{
			name:     "INTEGER",
			mem:      vdbe.NewMemInt(42),
			expected: int64(42),
		},
		{
			name:     "REAL",
			mem:      vdbe.NewMemReal(3.14),
			expected: float64(3.14),
		},
		{
			name:     "TEXT",
			mem:      vdbe.NewMemStr("hello"),
			expected: "hello",
		},
		{
			name:     "BLOB",
			mem:      vdbe.NewMemBlob([]byte("data")),
			expected: []byte("data"),
		},
		{
			name:     "Unknown type",
			mem:      vdbe.NewMem(), // Empty mem
			expected: nil,
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result := memToValue(tt.mem)

			// Special handling for byte slices
			if expectedBytes, ok := tt.expected.([]byte); ok {
				resultBytes, ok := result.([]byte)
				if !ok {
					t.Errorf("memToValue() type = %T, want []byte", result)
					return
				}
				if string(resultBytes) != string(expectedBytes) {
					t.Errorf("memToValue() = %v, want %v", result, tt.expected)
				}
				return
			}

			if result != tt.expected {
				t.Errorf("memToValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRowsIntegrationWithRealQuery(t *testing.T) {
	dbFile := "test_rows_integration.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Prepare a simple SELECT statement
	stmt, err := c.PrepareContext(context.Background(), "SELECT 1 AS num")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute query
	rows, err := stmt.(*Stmt).QueryContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Check columns
	cols := rows.Columns()
	if len(cols) != 1 || cols[0] != "num" {
		t.Errorf("Columns() = %v, want [num]", cols)
	}
}
