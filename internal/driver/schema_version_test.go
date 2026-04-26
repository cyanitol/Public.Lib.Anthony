// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"sync/atomic"
	"testing"
)

func TestSchemaVersionCommittedDDL(t *testing.T) {
	t.Parallel()

	conn := openRawConn(t, setupOCCDatabase(t, "schema_version_commit.db"))
	defer conn.Close()

	assertSchemaVersion(t, conn, 0)
	execSchemaDDL(t, conn, "CREATE TABLE committed_schema(id INTEGER)")
	assertSchemaVersion(t, conn, 1)
}

func TestSchemaVersionRollbackDDL(t *testing.T) {
	t.Parallel()

	conn := openRawConn(t, setupOCCDatabase(t, "schema_version_rollback.db"))
	defer conn.Close()

	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	execSchemaDDL(t, conn, "CREATE TABLE rolled_back_schema(id INTEGER)")
	if !conn.pendingSchemaChange {
		t.Fatal("pendingSchemaChange = false, want true after transactional DDL")
	}
	assertSchemaVersion(t, conn, 0)

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if conn.pendingSchemaChange {
		t.Fatal("pendingSchemaChange = true after rollback")
	}
	assertSchemaVersion(t, conn, 0)
}

func TestSchemaVersionSnapshotOnBegin(t *testing.T) {
	t.Parallel()

	conn := openRawConn(t, setupOCCDatabase(t, "schema_version_snapshot.db"))
	defer conn.Close()

	execSchemaDDL(t, conn, "CREATE TABLE snapshot_schema(id INTEGER)")
	assertSchemaVersion(t, conn, 1)

	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()

	if conn.startSchemaVersion != 1 {
		t.Fatalf("startSchemaVersion = %d, want 1", conn.startSchemaVersion)
	}
}

func execSchemaDDL(t *testing.T, conn *Conn, sql string) {
	t.Helper()

	stmt, err := conn.PrepareContext(context.Background(), sql)
	if err != nil {
		t.Fatalf("PrepareContext(%q): %v", sql, err)
	}
	defer stmt.Close()

	if _, err := stmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("ExecContext(%q): %v", sql, err)
	}
}

func assertSchemaVersion(t *testing.T, conn *Conn, want uint64) {
	t.Helper()

	if conn.schemaVersion == nil {
		t.Fatal("schemaVersion is nil")
	}
	if got := atomic.LoadUint64(conn.schemaVersion); got != want {
		t.Fatalf("schemaVersion = %d, want %d", got, want)
	}
}
