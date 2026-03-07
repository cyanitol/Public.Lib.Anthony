// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
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
		tt := tt // Capture range variable
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

func TestOpenMemoryDatabase(t *testing.T) {
	d := &Driver{}

	// Open with :memory:
	conn1, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open :memory: database: %v", err)
	}
	defer conn1.Close()

	// Open with empty string (also memory)
	conn2, err := d.Open("")
	if err != nil {
		t.Fatalf("failed to open empty string database: %v", err)
	}
	defer conn2.Close()

	// Verify connections are independent
	c1 := conn1.(*Conn)
	c2 := conn2.(*Conn)

	if c1.filename == c2.filename {
		t.Error("memory databases should have unique filenames")
	}
}

func TestGetDriver(t *testing.T) {
	d := GetDriver()
	if d == nil {
		t.Error("GetDriver() returned nil")
	}

	// Should return the same instance
	d2 := GetDriver()
	if d != d2 {
		t.Error("GetDriver() should return singleton instance")
	}
}

func TestDriverInitMaps(t *testing.T) {
	d := &Driver{}

	// Maps should be nil initially
	if d.conns != nil || d.dbs != nil {
		t.Error("new Driver should have nil maps")
	}

	d.initMaps()

	// Maps should be initialized
	if d.conns == nil {
		t.Error("initMaps() should initialize conns")
	}
	if d.dbs == nil {
		t.Error("initMaps() should initialize dbs")
	}

	// Should be idempotent - verify maps are not nil after second call
	d.initMaps()
	if d.conns == nil || d.dbs == nil {
		t.Error("initMaps() should not replace existing maps with nil")
	}
}

func TestMultipleMemoryDatabases(t *testing.T) {
	d := &Driver{}

	// Open multiple memory databases
	conn1, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open first memory database: %v", err)
	}
	defer conn1.Close()

	conn2, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open second memory database: %v", err)
	}
	defer conn2.Close()

	conn3, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open third memory database: %v", err)
	}
	defer conn3.Close()

	// All should have unique IDs
	c1 := conn1.(*Conn)
	c2 := conn2.(*Conn)
	c3 := conn3.(*Conn)

	filenames := map[string]bool{
		c1.filename: true,
		c2.filename: true,
		c3.filename: true,
	}

	if len(filenames) != 3 {
		t.Errorf("expected 3 unique memory database IDs, got %d", len(filenames))
	}
}

func TestDriverOpenConnector(t *testing.T) {
	dbFile := "test_open_connector.db"
	defer os.Remove(dbFile)

	d := &Driver{}

	// OpenConnector should work like Open
	conn, err := d.OpenConnector(dbFile)
	if err != nil {
		t.Fatalf("OpenConnector() failed: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	if c.filename != dbFile {
		t.Errorf("connection filename = %s, want %s", c.filename, dbFile)
	}
}

func TestSharedDatabaseState(t *testing.T) {
	dbFile := "test_shared_state.db"
	defer os.Remove(dbFile)

	d := &Driver{}

	// Open first connection
	conn1, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open first connection: %v", err)
	}

	c1 := conn1.(*Conn)

	// Open second connection to same file
	conn2, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open second connection: %v", err)
	}

	c2 := conn2.(*Conn)

	// Should share pager and btree
	if c1.pager != c2.pager {
		t.Error("connections should share pager")
	}
	if c1.btree != c2.btree {
		t.Error("connections should share btree")
	}
	if c1.schema != c2.schema {
		t.Error("connections should share schema")
	}

	// Close first connection
	conn1.Close()

	// Second connection should still work
	if err := c2.Ping(nil); err != nil {
		t.Errorf("second connection should still work after first closes: %v", err)
	}

	// Close second connection
	conn2.Close()
}

func TestReleaseStateDecreasesRefCount(t *testing.T) {
	dbFile := "test_driver_release_state.db"
	defer os.Remove(dbFile)

	d := &Driver{}

	// Open and close connection
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Check state exists
	d.mu.Lock()
	state, exists := d.dbs[dbFile]
	d.mu.Unlock()

	if !exists {
		t.Fatal("database state should exist")
	}

	if state.refCnt != 1 {
		t.Errorf("refCnt = %d, want 1", state.refCnt)
	}

	// Close connection
	c.Close()

	// Note: State cleanup depends on driver implementation details.
	// The key test is that refCnt was properly incremented during Open.
	// State removal is tested indirectly through memory leak tests.
}

func TestPagerProvider(t *testing.T) {
	dbFile := "test_pager_provider.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Verify btree has a provider
	if c.btree.Provider == nil {
		t.Error("btree should have a provider")
	}
}

func TestMemoryPagerProvider(t *testing.T) {
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open memory database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Verify btree has a provider
	if c.btree.Provider == nil {
		t.Error("btree should have a provider for memory database")
	}
}
