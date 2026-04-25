// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// openRawConn opens a *Conn via the driver singleton for OCC testing.
// The caller is responsible for closing the returned connection.
func openRawConn(t *testing.T, dbPath string) *Conn {
	t.Helper()
	raw, err := GetDriver().Open(dbPath)
	if err != nil {
		t.Fatalf("openRawConn %s: %v", dbPath, err)
	}
	return raw.(*Conn)
}

// setupOCCDatabase creates a file-backed database with a single table and
// returns its path. The database is closed before returning.
func setupOCCDatabase(t *testing.T, filename string) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), filename)
	setupDB, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open setup: %v", err)
	}
	setupDB.SetMaxOpenConns(1)
	if _, err := setupDB.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	setupDB.Close()
	return dbPath
}

// occCommitTx begins and commits a write transaction on conn.
func occCommitTx(t *testing.T, conn *Conn, label string) {
	t.Helper()
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("begin %s: %v", label, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s: %v", label, err)
	}
}

// TestOCCWriteConflict verifies that a write transaction whose startVersion
// does not match the current dbState.writeVersion returns ErrWriteConflict
// at commit time, and that a successful commit increments the version.
func TestOCCWriteConflict(t *testing.T) {
	t.Parallel()

	dbPath := setupOCCDatabase(t, "occ_conflict.db")

	conn1 := openRawConn(t, dbPath)
	defer conn1.Close()
	conn2 := openRawConn(t, dbPath)
	defer conn2.Close()

	// conn1 commits a write transaction (version: 0 → 1).
	occCommitTx(t, conn1, "tx1")

	if conn1.writeVersion == nil {
		t.Fatal("conn1.writeVersion is nil")
	}
	if got := atomic.LoadUint64(conn1.writeVersion); got != 1 {
		t.Fatalf("writeVersion after commit: got %d, want 1", got)
	}

	// conn2 begins a write transaction (snapshots version=1).
	tx2, err := conn2.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	conn2.startVersion = 0

	err = tx2.Commit()
	if err == nil {
		t.Fatal("expected ErrWriteConflict from tx2.Commit(), got nil")
	}
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("expected ErrWriteConflict, got: %v", err)
	}
}

// TestOCCNoConflictSequential verifies that sequential write transactions
// on the same file-based database do not produce conflict errors.
func TestOCCNoConflictSequential(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "occ_seq.db")

	setupDB, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open setup: %v", err)
	}
	setupDB.SetMaxOpenConns(1)
	if _, err := setupDB.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	setupDB.Close()

	conn := openRawConn(t, dbPath)
	defer conn.Close()

	// Run multiple sequential write transactions; none should conflict.
	for i := 0; i < 3; i++ {
		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		if err != nil {
			t.Fatalf("begin tx %d: %v", i, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit tx %d (unexpected conflict): %v", i, err)
		}
	}

	// After three commits, the version should be 3.
	if conn.writeVersion == nil {
		t.Fatal("conn.writeVersion is nil")
	}
	if got := atomic.LoadUint64(conn.writeVersion); got != 3 {
		t.Fatalf("writeVersion after 3 commits: got %d, want 3", got)
	}
}

// TestOCCVersionSnapshotOnBeginTx verifies that BeginTx correctly snapshots
// the current writeVersion into conn.startVersion.
func TestOCCVersionSnapshotOnBeginTx(t *testing.T) {
	t.Parallel()

	dbPath := setupOCCDatabase(t, "occ_snapshot.db")

	conn := openRawConn(t, dbPath)
	defer conn.Close()

	// Commit two transactions to advance the version to 2.
	for i := 0; i < 2; i++ {
		occCommitTx(t, conn, fmt.Sprintf("tx%d", i))
	}

	// The next BeginTx must snapshot startVersion == current writeVersion == 2.
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("begin snapshot tx: %v", err)
	}

	if conn.startVersion != 2 {
		t.Errorf("startVersion after BeginTx: got %d, want 2", conn.startVersion)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit snapshot tx: %v", err)
	}
}
