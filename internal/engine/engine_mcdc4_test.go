// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"encoding/binary"
	"os"
	"strings"
	"testing"
)

// writeMiniSQLiteDB writes a minimal valid two-page SQLite database file so
// that pager.PageCount() returns 2 when the file is opened.
func writeMiniSQLiteDB(t *testing.T, path string) {
	t.Helper()
	const pageSize = 4096
	buf := make([]byte, 2*pageSize)

	// Offset 0..15: magic header "SQLite format 3\x00"
	copy(buf[0:16], "SQLite format 3\x00")
	// Offset 16..17: page size (big-endian uint16 = 4096 = 0x1000)
	binary.BigEndian.PutUint16(buf[16:18], 4096)
	// Offset 18: file format write version = 1
	buf[18] = 1
	// Offset 19: file format read version = 1
	buf[19] = 1
	// Offset 20: reserved space per page = 0
	buf[20] = 0
	// Offset 21: max payload fraction = 64
	buf[21] = 64
	// Offset 22: min payload fraction = 32
	buf[22] = 32
	// Offset 23: leaf payload fraction = 32
	buf[23] = 32
	// Offset 24..27: file change counter = 1
	binary.BigEndian.PutUint32(buf[24:28], 1)
	// Offset 28..31: database size = 2 (pages)
	binary.BigEndian.PutUint32(buf[28:32], 2)
	// Offset 32..35: freelist trunk = 0
	binary.BigEndian.PutUint32(buf[32:36], 0)
	// Offset 36..39: freelist count = 0
	binary.BigEndian.PutUint32(buf[36:40], 0)
	// Offset 40..43: schema cookie = 1
	binary.BigEndian.PutUint32(buf[40:44], 1)
	// Offset 44..47: schema format = 4
	binary.BigEndian.PutUint32(buf[44:48], 4)
	// Offset 48..51: default cache size = 0
	binary.BigEndian.PutUint32(buf[48:52], 0)
	// Offset 52..55: largest root page = 0
	binary.BigEndian.PutUint32(buf[52:56], 0)
	// Offset 56..59: text encoding = 1 (UTF-8)
	binary.BigEndian.PutUint32(buf[56:60], 1)
	// Offset 60..63: user version = 0
	binary.BigEndian.PutUint32(buf[60:64], 0)
	// Offset 64..67: incremental vacuum = 0
	binary.BigEndian.PutUint32(buf[64:68], 0)
	// Offset 68..71: application ID = 0
	binary.BigEndian.PutUint32(buf[68:72], 0)
	// Offset 72..91: reserved (20 bytes zeros)
	// Offset 92..95: version valid for = 1
	binary.BigEndian.PutUint32(buf[92:96], 1)
	// Offset 96..99: SQLite version = 3040000
	binary.BigEndian.PutUint32(buf[96:100], 3040000)

	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatalf("writeMiniSQLiteDB: %v", err)
	}
}

// TestMCDC4_OpenWithOptions_ExistingDB covers the pg.PageCount() > 1 branch in
// OpenWithOptions (loadSchema called for existing databases).
func TestMCDC4_OpenWithOptions_ExistingDB(t *testing.T) {
	t.Parallel()
	// MC/DC for OpenWithOptions:
	//   C1: pg.PageCount() > 1 → loadSchema called
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/existing.db"

	// Write a minimal valid two-page SQLite database file directly.
	// This bypasses the engine's INSERT layer and reliably sets PageCount > 1.
	writeMiniSQLiteDB(t, dbPath)

	// Re-open with OpenWithOptions; exercises pg.PageCount() > 1 → loadSchema.
	db, err := OpenWithOptions(dbPath, false)
	if err != nil {
		t.Fatalf("OpenWithOptions on existing DB: %v", err)
	}
	defer db.Close()

	if db.schema == nil {
		t.Fatal("schema must not be nil after reopening existing DB")
	}
}

// TestMCDC4_Close_WithActiveTransaction covers the e.inTransaction=true branch
// in Close (rollback on close).
func TestMCDC4_Close_WithActiveTransaction(t *testing.T) {
	t.Parallel()
	// MC/DC for Close:
	//   C1: e.inTransaction=true → rollback before closing pager
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/txclose.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Begin a transaction without committing.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	_ = tx // intentionally not committed

	// Close should rollback the active transaction automatically.
	if err := db.Close(); err != nil {
		t.Errorf("Close with active transaction should not error, got: %v", err)
	}
}

// TestMCDC4_PreparedStmt_MultiExec covers the Reset path in PreparedStmt.Execute
// by executing the same prepared statement twice (second exec calls Reset).
func TestMCDC4_PreparedStmt_MultiExec(t *testing.T) {
	t.Parallel()
	// MC/DC for PreparedStmt.Execute:
	//   C2: ps.closed=false AND vdbe.Reset succeeds → executes normally
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/multiexec.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Execute("CREATE TABLE t4m (x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// Pre-insert a row so the SELECT below returns a result.
	if _, err := db.Execute("INSERT INTO t4m VALUES(99)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Use a read-only SELECT so Reset + re-Execute does not hit PK conflicts.
	ps, err := db.Prepare("SELECT x FROM t4m")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer ps.Close()

	// First execution.
	if _, err := ps.Execute(); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Second execution reuses the prepared stmt via Reset internally.
	if _, err := ps.Execute(); err != nil {
		t.Fatalf("second Execute (Reset path): %v", err)
	}
}

// TestMCDC4_Query_WithParam covers PreparedStmt.Query exercising the open
// (ps.closed=false) path and verifying that rows are returned.
// Note: bindParams in the engine layer is currently a stub; we use a literal
// value query so the result set is non-empty without parameter binding.
func TestMCDC4_Query_WithParam(t *testing.T) {
	t.Parallel()
	// MC/DC for PreparedStmt.Query:
	//   C2: ps.closed=false → query executes and returns Rows
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/queryparam.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Execute("CREATE TABLE t4q (x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Execute("INSERT INTO t4q VALUES(42)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	ps, err := db.Prepare("SELECT x FROM t4q WHERE x = 42")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer ps.Close()

	// Call Query (with no params) — exercises the ps.closed=false open path.
	rows, err := ps.Query()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	found := false
	for rows.Next() {
		found = true
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			t.Errorf("Scan: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Errorf("rows.Err: %v", err)
	}
	rows.Close()

	if !found {
		t.Error("expected at least one row for x=42")
	}
}

// TestMCDC4_OpenWithOptions_ReadOnly covers the readOnly=true path in
// OpenWithOptions; verifies that writes fail on a read-only database.
func TestMCDC4_OpenWithOptions_ReadOnly(t *testing.T) {
	t.Parallel()
	// MC/DC for OpenWithOptions:
	//   C3: readOnly=true → write operations should fail
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/ro.db"

	// Create the database first so it exists on disk.
	func() {
		db, err := Open(dbPath)
		if err != nil {
			t.Fatalf("create db: %v", err)
		}
		if _, err := db.Execute("CREATE TABLE t4r (id INTEGER)"); err != nil {
			t.Fatalf("CREATE TABLE: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	// Re-open read-only.
	roDB, err := OpenWithOptions(dbPath, true)
	if err != nil {
		t.Fatalf("OpenWithOptions readOnly: %v", err)
	}
	defer roDB.Close()

	// Writes must fail on a read-only database.
	_, err = roDB.Execute("INSERT INTO t4r VALUES(1)")
	if err == nil {
		t.Error("INSERT on read-only database should return an error")
	} else if !strings.Contains(strings.ToLower(err.Error()), "read") &&
		!strings.Contains(strings.ToLower(err.Error()), "readonly") &&
		!strings.Contains(strings.ToLower(err.Error()), "read only") {
		t.Logf("INSERT on read-only db returned (may be any error): %v", err)
	}
}
