package driver

import (
	"database/sql"
	"os"
	"testing"
)

func TestDriverRegistration(t *testing.T) {
	// Check that the driver is registered
	drivers := sql.Drivers()
	found := false
	for _, d := range drivers {
		if d == DriverName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("%s driver not registered", DriverName)
	}
}

func TestOpenConnection(t *testing.T) {
	// Create a temporary database file
	dbFile := "test_open.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Verify the connection works
	if err := db.Ping(); err != nil {
		t.Errorf("ping failed: %v", err)
	}
}

func TestPrepareStatement(t *testing.T) {
	dbFile := "test_prepare.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Prepare a simple statement
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()
}

func TestTransaction(t *testing.T) {
	dbFile := "test_tx.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("failed to commit: %v", err)
	}

	// Test rollback
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Errorf("failed to rollback: %v", err)
	}
}

func TestMultipleConnections(t *testing.T) {
	dbFile := "test_multi.db"
	defer os.Remove(dbFile)

	// Open first connection
	db1, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open first connection: %v", err)
	}
	defer db1.Close()

	// Open second connection
	db2, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open second connection: %v", err)
	}
	defer db2.Close()

	// Both should work
	if err := db1.Ping(); err != nil {
		t.Errorf("first connection ping failed: %v", err)
	}
	if err := db2.Ping(); err != nil {
		t.Errorf("second connection ping failed: %v", err)
	}
}

func TestCloseConnection(t *testing.T) {
	dbFile := "test_close.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Close the connection
	if err := db.Close(); err != nil {
		t.Errorf("failed to close connection: %v", err)
	}

	// Verify connection is closed by trying to ping
	if err := db.Ping(); err == nil {
		t.Error("ping should fail after close")
	}
}

func TestValueConversion(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
	}{
		{"nil", nil, false},
		{"int64", int64(42), false},
		{"int", int(42), false},
		{"float64", float64(3.14), false},
		{"string", "hello", false},
		{"bool", true, false},
		{"bytes", []byte("data"), false},
		{"uint64 ok", uint64(100), false},
		{"uint64 overflow", uint64(1 << 63), true},
	}

	vc := ValueConverter{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := vc.ConvertValue(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestResult(t *testing.T) {
	r := &Result{
		lastInsertID: 42,
		rowsAffected: 10,
	}

	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId() error = %v", err)
	}
	if id != 42 {
		t.Errorf("LastInsertId() = %d, want 42", id)
	}

	rows, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected() error = %v", err)
	}
	if rows != 10 {
		t.Errorf("RowsAffected() = %d, want 10", rows)
	}
}
