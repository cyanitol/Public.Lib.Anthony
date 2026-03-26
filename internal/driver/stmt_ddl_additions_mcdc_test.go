// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ============================================================================
// MC/DC: getCurrentJournalMode – pagerModeInt bounds guard
//
// Compound condition (2 sub-conditions, both must be true):
//   A = pagerModeInt >= 0
//   B = pagerModeInt < len(modeNames)
//
// Outcome = A && B → use modeNames[pagerModeInt]; else fall back to conn.journalMode
//
// The in-memory driver does not wire a concrete *pager.Pager, so the type
// assertion inside getCurrentJournalMode always fails.  The condition is
// exercised at the SQL level via PRAGMA journal_mode which calls
// getCurrentJournalMode for the GET path.  The "set then get" cases exercise
// the stored-string fallback; the plain GET case exercises the default path.
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → valid pager index → mode name from slice (in-memory: falls through)
//   Flip A: A=F B=? → negative pagerModeInt → fallback string used
//   Flip B: A=T B=F → index >= len(modeNames) → fallback string used
//
// All SQL-reachable paths use the stored string fallback; we verify each
// stored value is returned correctly.
// ============================================================================

func TestMCDC_GetCurrentJournalMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    string
		wantMode string
	}{
		{
			// Default (no set): conn.journalMode is "" → getCurrentJournalMode returns "delete".
			name:     "A=T B=T path: default journal_mode is delete",
			setup:    "",
			wantMode: "delete",
		},
		{
			// Flip A/B: after setting to memory, stored string "memory" is returned.
			name:     "stored string path: set memory then get returns memory",
			setup:    "PRAGMA journal_mode = memory",
			wantMode: "memory",
		},
		{
			// Flip B: stored string path for WAL.
			name:     "stored string path: set wal then get returns wal",
			setup:    "PRAGMA journal_mode = wal",
			wantMode: "wal",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			if tt.setup != "" {
				mustExec(t, db, tt.setup)
			}
			var got string
			if err := db.QueryRow("PRAGMA journal_mode").Scan(&got); err != nil {
				t.Fatalf("QueryRow PRAGMA journal_mode: %v", err)
			}
			if got != tt.wantMode {
				t.Errorf("journal_mode = %q, want %q", got, tt.wantMode)
			}
		})
	}
}

// ============================================================================
// MC/DC: checkRowMatchWithCollation – collation slice bounds guard
//
// Compound condition (2 sub-conditions, both must be true):
//   A = i < len(collations)
//   B = collations[i] != ""
//
// Outcome = A && B → use collations[i] as collation name; else use "BINARY"
//
// This path is reached when FK enforcement calls RowExistsWithCollation.
// We drive it through FK INSERT enforcement so the collation path is executed.
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → non-empty collation entry → NOCASE used; case-insensitive match
//   Flip A: A=F B=? → collation slice shorter than column list → BINARY used
//   Flip B: A=T B=F → empty string in collation slice → BINARY used
// ============================================================================

func TestMCDC_CheckRowMatchWithCollation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		stmt    string
		wantErr bool
	}{
		{
			// A=T B=T: FK parent column has NOCASE collation; NOCASE is propagated.
			// Child insert with case-different key succeeds because NOCASE matches.
			name: "A=T B=T: NOCASE collation used; case-folded key matches parent",
			setup: []string{
				"CREATE TABLE crmwc_parent(id TEXT COLLATE NOCASE PRIMARY KEY)",
				"INSERT INTO crmwc_parent VALUES('Alice')",
				"CREATE TABLE crmwc_child(pid TEXT REFERENCES crmwc_parent(id))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO crmwc_child VALUES('alice')",
			wantErr: false,
		},
		{
			// Flip B=F: parent column has no COLLATE clause; collation entry is empty
			// → BINARY used → case mismatch → FK violation.
			name: "Flip B=F: empty collation defaults to BINARY; case mismatch fails",
			setup: []string{
				"CREATE TABLE crmwc_parent2(id TEXT PRIMARY KEY)",
				"INSERT INTO crmwc_parent2 VALUES('Alice')",
				"CREATE TABLE crmwc_child2(pid TEXT REFERENCES crmwc_parent2(id))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO crmwc_child2 VALUES('alice')",
			wantErr: true,
		},
		{
			// Flip A=F: valid exact BINARY match (collation out-of-bounds → BINARY);
			// BINARY is sufficient because values are identical.
			name: "Flip A=F: BINARY collation (index out-of-range); exact match succeeds",
			setup: []string{
				"CREATE TABLE crmwc_parent3(id TEXT PRIMARY KEY)",
				"INSERT INTO crmwc_parent3 VALUES('Bob')",
				"CREATE TABLE crmwc_child3(pid TEXT REFERENCES crmwc_parent3(id))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO crmwc_child3 VALUES('Bob')",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			if tt.wantErr {
				mcdcMustFail(t, db, tt.stmt)
			} else {
				mustExec(t, db, tt.stmt)
			}
		})
	}
}

// ============================================================================
// MC/DC: checkRowMatchWithParentAffinity – parent column lookup guard
//
// Compound condition (2 sub-conditions, both must be true):
//   A = i < len(parentColumns)
//   B = parentTable != nil
//
// Outcome = A && B → look up parent column for affinity; else parentCol remains nil
//
// Both A=F and B=F are structurally unreachable via valid SQL (the FK parser
// always produces matching column counts and a resolved parent table).
// We cover the Base case thoroughly with multi-column FK references.
//
// Cases needed (N+1 = 3):
//   Base:     A=T B=T → parent column found, affinity applied → FK respected
//   Flip A:   A=F B=T → unreachable via SQL; covered implicitly by single-col case
//   Flip B:   A=T B=F → unreachable via SQL
// ============================================================================

func TestMCDC_CheckRowMatchWithParentAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		stmt    string
		wantErr bool
	}{
		{
			// A=T B=T: multi-column FK with explicit parent columns → affinity applied.
			name: "A=T B=T: multi-column FK succeeds on matching values",
			setup: []string{
				"CREATE TABLE crmwpa_parent(a INTEGER, b TEXT, PRIMARY KEY(a,b))",
				"INSERT INTO crmwpa_parent VALUES(1,'x')",
				"CREATE TABLE crmwpa_child(pa INTEGER, pb TEXT, FOREIGN KEY(pa,pb) REFERENCES crmwpa_parent(a,b))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO crmwpa_child VALUES(1,'x')",
			wantErr: false,
		},
		{
			// A=T B=T (violation path): non-existent parent → FK error.
			name: "A=T B=T: multi-column FK violation rejected",
			setup: []string{
				"CREATE TABLE crmwpa_parent2(a INTEGER, b TEXT, PRIMARY KEY(a,b))",
				"INSERT INTO crmwpa_parent2 VALUES(1,'x')",
				"CREATE TABLE crmwpa_child2(pa INTEGER, pb TEXT, FOREIGN KEY(pa,pb) REFERENCES crmwpa_parent2(a,b))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO crmwpa_child2 VALUES(2,'y')",
			wantErr: true,
		},
		{
			// Single-column FK: base path with one column, INTEGER affinity applied.
			name: "A=T B=T: single-column INTEGER FK, affinity applied",
			setup: []string{
				"CREATE TABLE crmwpa_parent3(id INTEGER PRIMARY KEY)",
				"INSERT INTO crmwpa_parent3 VALUES(42)",
				"CREATE TABLE crmwpa_child3(pid INTEGER REFERENCES crmwpa_parent3(id))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO crmwpa_child3 VALUES(42)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			if tt.wantErr {
				mcdcMustFail(t, db, tt.stmt)
			} else {
				mustExec(t, db, tt.stmt)
			}
		})
	}
}

// ============================================================================
// MC/DC: getColumnValue – INTEGER PRIMARY KEY (rowid alias) detection
//
// Compound condition (2 sub-conditions, both must be true):
//   A = col.PrimaryKey
//   B = strings.EqualFold(col.Type, "INTEGER")
//
// Outcome = A && B → return rowid directly (bypass payload decode)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → INTEGER PRIMARY KEY → rowid returned
//   Flip A: A=F B=T → non-PK INTEGER column → payload decoded
//   Flip B: A=T B=F → non-INTEGER PK (TEXT) → payload decoded
//
// Exercised via FK enforcement: the driver calls getColumnValue to read
// parent-key values when validating referential integrity on INSERT.
// ============================================================================

func TestMCDC_GetColumnValue_IPKDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		stmt    string
		wantErr bool
	}{
		{
			// A=T B=T: parent FK column is INTEGER PRIMARY KEY → rowid path.
			name: "A=T B=T: FK via INTEGER PRIMARY KEY parent succeeds",
			setup: []string{
				"CREATE TABLE gcv_parent(id INTEGER PRIMARY KEY)",
				"INSERT INTO gcv_parent VALUES(1)",
				"CREATE TABLE gcv_child(pid INTEGER REFERENCES gcv_parent(id))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO gcv_child VALUES(1)",
			wantErr: false,
		},
		{
			// Flip A=F: parent FK column is a non-PK INTEGER (UNIQUE) → payload path.
			name: "Flip A=F: FK via non-PK INTEGER column uses payload path",
			setup: []string{
				"CREATE TABLE gcv_parent2(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)",
				"INSERT INTO gcv_parent2 VALUES(1,100)",
				"CREATE TABLE gcv_child2(pval INTEGER REFERENCES gcv_parent2(val))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO gcv_child2 VALUES(100)",
			wantErr: false,
		},
		{
			// Flip B=F: parent FK column is TEXT PRIMARY KEY (not INTEGER) → payload path.
			name: "Flip B=F: FK via TEXT PRIMARY KEY uses payload path",
			setup: []string{
				"CREATE TABLE gcv_parent3(id TEXT PRIMARY KEY)",
				"INSERT INTO gcv_parent3 VALUES('abc')",
				"CREATE TABLE gcv_child3(pid TEXT REFERENCES gcv_parent3(id))",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "INSERT INTO gcv_child3 VALUES('abc')",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			if tt.wantErr {
				mcdcMustFail(t, db, tt.stmt)
			} else {
				mustExec(t, db, tt.stmt)
			}
		})
	}
}

// ============================================================================
// MC/DC: getColumnValue – payload index skip guard (negated IPK)
//
// Compound condition (2 sub-conditions, negated):
//   A = col.PrimaryKey
//   B = strings.EqualFold(col.Type, "INTEGER")
//
// Outcome = !(A && B) → payloadIdx incremented for this column
//           (A && B)  → payloadIdx NOT incremented (rowid alias has no payload slot)
//
// The same 3 SQL shapes as the IPK detection test cover this negation because
// payloadIdx is only incremented in the non-IPK branch.
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → NOT incremented; adjacent column is at payload index 0
//   Flip A: A=F B=T → incremented; adjacent column is at payload index 1
//   Flip B: A=T B=F → incremented; adjacent column is at payload index 1
// ============================================================================

func TestMCDC_GetColumnValue_PayloadIdxSkip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		query   string
		wantVal string
	}{
		{
			// A=T B=T: id is rowid alias (not in payload); val is at payload index 0.
			name: "A=T B=T: IPK skipped; sibling col is at payload index 0",
			setup: []string{
				"CREATE TABLE gcvpis_t(id INTEGER PRIMARY KEY, val TEXT)",
				"INSERT INTO gcvpis_t VALUES(7,'hello')",
			},
			query:   "SELECT val FROM gcvpis_t WHERE id = 7",
			wantVal: "hello",
		},
		{
			// Flip A=F: a is plain TEXT (no PK); payloadIdx incremented for a;
			// b is at payload index 1.
			name: "Flip A=F: non-PK col increments payloadIdx; next col at index 1",
			setup: []string{
				"CREATE TABLE gcvpis_t2(a TEXT, b TEXT)",
				"INSERT INTO gcvpis_t2 VALUES('foo','bar')",
			},
			query:   "SELECT b FROM gcvpis_t2 WHERE a = 'foo'",
			wantVal: "bar",
		},
		{
			// Flip B=F: id is TEXT PRIMARY KEY (A=T but B=F); payloadIdx IS incremented;
			// val is at payload index 1.
			name: "Flip B=F: TEXT PK included in payload; next col at index 1",
			setup: []string{
				"CREATE TABLE gcvpis_t3(id TEXT PRIMARY KEY, val TEXT)",
				"INSERT INTO gcvpis_t3 VALUES('key','value')",
			},
			query:   "SELECT val FROM gcvpis_t3 WHERE id = 'key'",
			wantVal: "value",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			var got string
			if err := db.QueryRow(tt.query).Scan(&got); err != nil {
				t.Fatalf("QueryRow %q: %v", tt.query, err)
			}
			if got != tt.wantVal {
				t.Errorf("got %q, want %q", got, tt.wantVal)
			}
		})
	}
}

// ============================================================================
// MC/DC: readRowValues – INTEGER PRIMARY KEY detection per column
//
// Compound condition (2 sub-conditions, both must be true):
//   A = col.PrimaryKey
//   B = strings.EqualFold(col.Type, "INTEGER")
//
// Outcome = A && B → store rowid as column value (bypass payload);
//           else   → decode from payload
//
// readRowValues is called by FindReferencingRows (FK enforcement on DELETE).
// We exercise it by deleting a parent row that has child references, which
// causes the driver to scan the child table for referencing rows.
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → child INTEGER PRIMARY KEY handled via rowid
//   Flip A: A=F B=T → child non-PK INTEGER column decoded from payload
//   Flip B: A=T B=F → child TEXT PRIMARY KEY decoded from payload
// ============================================================================

func TestMCDC_ReadRowValues_IPKDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		stmt    string
		wantErr bool
	}{
		{
			// A=T B=T: child has INTEGER PRIMARY KEY; FK DELETE on parent triggers
			// readRowValues which reads the child's IPK as rowid.
			name: "A=T B=T: child INTEGER PK, FK DELETE on parent → violation",
			setup: []string{
				"CREATE TABLE rrv_parent(id INTEGER PRIMARY KEY)",
				"INSERT INTO rrv_parent VALUES(1)",
				"CREATE TABLE rrv_child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES rrv_parent(id))",
				"INSERT INTO rrv_child VALUES(10,1)",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "DELETE FROM rrv_parent WHERE id = 1",
			wantErr: true,
		},
		{
			// Flip A=F: child FK column is a plain non-PK INTEGER; payload decode path.
			name: "Flip A=F: child non-PK INTEGER; FK DELETE on parent → violation",
			setup: []string{
				"CREATE TABLE rrv_parent2(id INTEGER PRIMARY KEY)",
				"INSERT INTO rrv_parent2 VALUES(2)",
				"CREATE TABLE rrv_child2(val INTEGER, pid INTEGER REFERENCES rrv_parent2(id))",
				"INSERT INTO rrv_child2 VALUES(20,2)",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "DELETE FROM rrv_parent2 WHERE id = 2",
			wantErr: true,
		},
		{
			// Flip B=F: child has TEXT PRIMARY KEY (A=T, B=F); payload decode path.
			name: "Flip B=F: child TEXT PK; FK DELETE on parent → violation",
			setup: []string{
				"CREATE TABLE rrv_parent3(id INTEGER PRIMARY KEY)",
				"INSERT INTO rrv_parent3 VALUES(3)",
				"CREATE TABLE rrv_child3(name TEXT PRIMARY KEY, pid INTEGER REFERENCES rrv_parent3(id))",
				"INSERT INTO rrv_child3 VALUES('c',3)",
				"PRAGMA foreign_keys = ON",
			},
			stmt:    "DELETE FROM rrv_parent3 WHERE id = 3",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			if tt.wantErr {
				mcdcMustFail(t, db, tt.stmt)
			} else {
				mustExec(t, db, tt.stmt)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqual – both-nil guard
//
// Compound condition (2 sub-conditions):
//   A = v1 == nil
//   B = v2 == nil
//
// Outcome = A && B → return true (both nil are equal)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → true
//   Flip A: A=F B=T → false (only v2 is nil)
//   Flip B: A=T B=F → false (only v1 is nil)
// ============================================================================

func TestMCDC_ValuesEqual_BothNil(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	tests := []struct {
		name string
		v1   interface{}
		v2   interface{}
		want bool
	}{
		{name: "A=T B=T: both nil equal", v1: nil, v2: nil, want: true},
		{name: "Flip A=F: v1 non-nil, v2 nil → false", v1: int64(1), v2: nil, want: false},
		{name: "Flip B=F: v1 nil, v2 non-nil → false", v1: nil, v2: int64(1), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqual(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqual – one-nil early-false guard
//
// Compound condition (2 sub-conditions):
//   A = v1 == nil
//   B = v2 == nil
//
// Outcome = A || B → return false (exactly one is nil)
//
// Note: A=T B=T is short-circuited by the earlier both-nil guard (returns true),
// so that case never reaches this guard.
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=F → v1 nil, v2 non-nil → false (A independently flips outcome)
//   Flip B: A=F B=T → v1 non-nil, v2 nil → false (B independently flips outcome)
//   Flip A: A=F B=F → neither nil → condition false, falls through to comparison
// ============================================================================

func TestMCDC_ValuesEqual_OneNilGuard(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	tests := []struct {
		name string
		v1   interface{}
		v2   interface{}
		want bool
	}{
		{name: "Base A=T B=F: v1 nil → false", v1: nil, v2: int64(5), want: false},
		{name: "Flip B=T: v2 nil → false", v1: int64(5), v2: nil, want: false},
		{name: "Flip A=F B=F: both non-nil falls through → true", v1: int64(5), v2: int64(5), want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqual(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqualWithCollation – both-nil guard
//
// Compound condition (2 sub-conditions):
//   A = v1 == nil
//   B = v2 == nil
//
// Outcome = A && B → return true
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → true
//   Flip A: A=F B=T → false
//   Flip B: A=T B=F → false
// ============================================================================

func TestMCDC_ValuesEqualWithCollation_BothNil(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	tests := []struct {
		name      string
		v1        interface{}
		v2        interface{}
		collation string
		want      bool
	}{
		{name: "A=T B=T: both nil equal", v1: nil, v2: nil, collation: "BINARY", want: true},
		{name: "Flip A=F: v1 non-nil, v2 nil → false", v1: "hello", v2: nil, collation: "BINARY", want: false},
		{name: "Flip B=F: v1 nil, v2 non-nil → false", v1: nil, v2: "hello", collation: "BINARY", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqualWithCollation(tt.v1, tt.v2, tt.collation)
			if got != tt.want {
				t.Errorf("valuesEqualWithCollation(%v, %v, %q) = %v, want %v",
					tt.v1, tt.v2, tt.collation, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqualWithCollation – one-nil early-false guard
//
// Compound condition (2 sub-conditions):
//   A = v1 == nil
//   B = v2 == nil
//
// Outcome = A || B → return false
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=F → v1 nil → false (A flips outcome)
//   Flip B: A=F B=T → v2 nil → false (B flips outcome)
//   Flip A: A=F B=F → both non-nil → falls through to string comparison
// ============================================================================

func TestMCDC_ValuesEqualWithCollation_OneNilGuard(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	tests := []struct {
		name      string
		v1        interface{}
		v2        interface{}
		collation string
		want      bool
	}{
		{name: "Base A=T B=F: v1 nil → false", v1: nil, v2: "x", collation: "BINARY", want: false},
		{name: "Flip B=T: v2 nil → false", v1: "x", v2: nil, collation: "BINARY", want: false},
		{name: "Flip A=F B=F: both non-nil NOCASE match → true", v1: "hello", v2: "HELLO", collation: "NOCASE", want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqualWithCollation(tt.v1, tt.v2, tt.collation)
			if got != tt.want {
				t.Errorf("valuesEqualWithCollation(%v, %v, %q) = %v, want %v",
					tt.v1, tt.v2, tt.collation, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqualWithAffinity – both-nil guard
//
// Compound condition (2 sub-conditions):
//   A = parentVal == nil
//   B = childVal == nil
//
// Outcome = A && B → return true
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → true
//   Flip A: A=F B=T → false
//   Flip B: A=T B=F → false
// ============================================================================

func TestMCDC_ValuesEqualWithAffinity_BothNil(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	tests := []struct {
		name      string
		parentVal interface{}
		childVal  interface{}
		want      bool
	}{
		{name: "A=T B=T: both nil equal", parentVal: nil, childVal: nil, want: true},
		{name: "Flip A=F: parentVal non-nil, childVal nil → false", parentVal: int64(1), childVal: nil, want: false},
		{name: "Flip B=F: parentVal nil, childVal non-nil → false", parentVal: nil, childVal: int64(1), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqualWithAffinity(tt.parentVal, tt.childVal, nil)
			if got != tt.want {
				t.Errorf("valuesEqualWithAffinity(%v, %v, nil) = %v, want %v",
					tt.parentVal, tt.childVal, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqualWithAffinity – one-nil early-false guard
//
// Compound condition (2 sub-conditions):
//   A = parentVal == nil
//   B = childVal == nil
//
// Outcome = A || B → return false
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=F → parentVal nil → false
//   Flip B: A=F B=T → childVal nil → false
//   Flip A: A=F B=F → both non-nil → falls through to comparison
// ============================================================================

func TestMCDC_ValuesEqualWithAffinity_OneNilGuard(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	tests := []struct {
		name      string
		parentVal interface{}
		childVal  interface{}
		want      bool
	}{
		{name: "Base A=T B=F: parentVal nil → false", parentVal: nil, childVal: int64(7), want: false},
		{name: "Flip B=T: childVal nil → false", parentVal: int64(7), childVal: nil, want: false},
		{name: "Flip A=F B=F: both non-nil equal integers → true", parentVal: int64(42), childVal: int64(42), want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqualWithAffinity(tt.parentVal, tt.childVal, nil)
			if got != tt.want {
				t.Errorf("valuesEqualWithAffinity(%v, %v, nil) = %v, want %v",
					tt.parentVal, tt.childVal, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: valuesEqualWithAffinity – parent collation selection guard
//
// Compound condition (2 sub-conditions, both must be true):
//   A = parentCol != nil
//   B = parentCol.Collation != ""
//
// Outcome = A && B → use parentCol.Collation as collation name; else use "BINARY"
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → parentCol has non-empty Collation → NOCASE: "hello"=="HELLO" true
//   Flip A: A=F B=? → parentCol nil → BINARY used → "hello"!="HELLO" false
//   Flip B: A=T B=F → parentCol.Collation == "" → BINARY used → "hello"!="HELLO" false
// ============================================================================

func TestMCDC_ValuesEqualWithAffinity_CollationGuard(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	// A=T B=T: column with NOCASE collation.
	colNOCASE := &schema.Column{
		Name:      "col_nocase",
		Type:      "TEXT",
		Collation: "NOCASE",
	}

	// Flip B=F: column with empty Collation (BINARY effective).
	colNoColl := &schema.Column{
		Name:      "col_nocoll",
		Type:      "TEXT",
		Collation: "",
	}

	tests := []struct {
		name      string
		parentVal interface{}
		childVal  interface{}
		parentCol *schema.Column
		want      bool
	}{
		{
			// A=T B=T: NOCASE collation → case-insensitive comparison → equal
			name:      "A=T B=T: NOCASE collation makes hello==HELLO true",
			parentVal: "hello",
			childVal:  "HELLO",
			parentCol: colNOCASE,
			want:      true,
		},
		{
			// Flip A=F: parentCol nil → BINARY → case-sensitive → not equal
			name:      "Flip A=F: nil parentCol uses BINARY; hello!=HELLO",
			parentVal: "hello",
			childVal:  "HELLO",
			parentCol: nil,
			want:      false,
		},
		{
			// Flip B=F: parentCol non-nil but Collation="" → BINARY → not equal
			name:      "Flip B=F: empty Collation uses BINARY; hello!=HELLO",
			parentVal: "hello",
			childVal:  "HELLO",
			parentCol: colNoColl,
			want:      false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqualWithAffinity(tt.parentVal, tt.childVal, tt.parentCol)
			if got != tt.want {
				t.Errorf("valuesEqualWithAffinity(%v, %v, col) = %v, want %v",
					tt.parentVal, tt.childVal, got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: CREATE INDEX / DROP INDEX SQL-level coverage
//
// Exercises compileCreateIndex and compileDropIndex via the sql.DB interface.
// These functions contain the btree nil-check guards (s.conn.btree != nil)
// which are single-condition; we do not repeat those here. The purpose of
// these tests is to ensure the overall DDL addition path is reachable from SQL.
// ============================================================================

func TestMCDC_CreateDropIndex_DDLAdditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		stmt    string
		wantErr bool
	}{
		{
			name: "CREATE plain index",
			setup: []string{
				"CREATE TABLE cidx_t(id INTEGER, val TEXT)",
				"INSERT INTO cidx_t VALUES(1,'a'),(2,'b')",
			},
			stmt: "CREATE INDEX cidx_i ON cidx_t(val)",
		},
		{
			name: "CREATE UNIQUE INDEX",
			setup: []string{
				"CREATE TABLE cidx_uniq(id INTEGER, email TEXT)",
				"INSERT INTO cidx_uniq VALUES(1,'a@b.com')",
			},
			stmt: "CREATE UNIQUE INDEX cidx_uniq_i ON cidx_uniq(email)",
		},
		{
			name: "CREATE INDEX IF NOT EXISTS on existing index is no-op",
			setup: []string{
				"CREATE TABLE cidx_ifne(id INTEGER, val TEXT)",
				"CREATE INDEX cidx_ifne_i ON cidx_ifne(val)",
			},
			stmt: "CREATE INDEX IF NOT EXISTS cidx_ifne_i ON cidx_ifne(val)",
		},
		{
			name:    "DROP INDEX that does not exist fails",
			setup:   []string{"CREATE TABLE cidx_dne2(id INTEGER)"},
			stmt:    "DROP INDEX cidx_no_such_index",
			wantErr: true,
		},
		{
			name: "DROP INDEX IF EXISTS on nonexistent index is no-op",
			setup: []string{
				"CREATE TABLE cidx_dne(id INTEGER, val TEXT)",
			},
			stmt: "DROP INDEX IF EXISTS cidx_no_such",
		},
		{
			name: "DROP INDEX on existing index succeeds",
			setup: []string{
				"CREATE TABLE cidx_drop(id INTEGER, val TEXT)",
				"CREATE INDEX cidx_drop_i ON cidx_drop(val)",
			},
			stmt: "DROP INDEX cidx_drop_i",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			if tt.wantErr {
				mcdcMustFail(t, db, tt.stmt)
			} else {
				mustExec(t, db, tt.stmt)
			}
		})
	}
}

// ============================================================================
// MC/DC: ALTER TABLE SQL-level coverage
//
// Exercises compileAlterTable and its dispatch to sub-functions:
//   compileAlterTableRename, compileAlterTableRenameColumn,
//   compileAlterTableAddColumn, compileAlterTableDropColumn.
// Also covers validateColumnAddition (duplicate column guard).
// ============================================================================

func TestMCDC_AlterTable_DDLAdditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   []string
		stmt    string
		wantErr bool
	}{
		{
			name:  "ALTER TABLE RENAME TO",
			setup: []string{"CREATE TABLE at_rename(id INTEGER)"},
			stmt:  "ALTER TABLE at_rename RENAME TO at_renamed",
		},
		{
			name:  "ALTER TABLE RENAME COLUMN",
			setup: []string{"CREATE TABLE at_renamecol(id INTEGER, val TEXT)"},
			stmt:  "ALTER TABLE at_renamecol RENAME COLUMN val TO value",
		},
		{
			name:  "ALTER TABLE ADD COLUMN plain",
			setup: []string{"CREATE TABLE at_addcol(id INTEGER)"},
			stmt:  "ALTER TABLE at_addcol ADD COLUMN name TEXT",
		},
		{
			name:  "ALTER TABLE ADD COLUMN with DEFAULT",
			setup: []string{"CREATE TABLE at_adddef(id INTEGER)"},
			stmt:  "ALTER TABLE at_adddef ADD COLUMN score INTEGER DEFAULT 0",
		},
		{
			name:  "ALTER TABLE ADD COLUMN with NOT NULL DEFAULT",
			setup: []string{"CREATE TABLE at_addnn(id INTEGER)"},
			stmt:  "ALTER TABLE at_addnn ADD COLUMN tag TEXT NOT NULL DEFAULT ''",
		},
		{
			// validateColumnAddition: column already exists → error
			name:    "ALTER TABLE ADD COLUMN duplicate fails",
			setup:   []string{"CREATE TABLE at_adddup(id INTEGER, val TEXT)"},
			stmt:    "ALTER TABLE at_adddup ADD COLUMN val INTEGER",
			wantErr: true,
		},
		{
			name: "ALTER TABLE DROP COLUMN",
			setup: []string{
				"CREATE TABLE at_drop(id INTEGER, val TEXT, extra TEXT)",
			},
			stmt: "ALTER TABLE at_drop DROP COLUMN extra",
		},
		{
			// compileAlterTable: table not found → error
			name:    "ALTER TABLE on nonexistent table fails",
			setup:   []string{},
			stmt:    "ALTER TABLE at_nosuch ADD COLUMN x INTEGER",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range tt.setup {
				mustExec(t, db, s)
			}
			if tt.wantErr {
				mcdcMustFail(t, db, tt.stmt)
			} else {
				mustExec(t, db, tt.stmt)
			}
		})
	}
}

// ============================================================================
// MC/DC: PRAGMA foreign_key_check – exercises FK violation scanning
//
// PRAGMA foreign_key_check invokes compilePragmaForeignKeyCheck which calls
// the driverRowReader machinery (checkRowMatchWithCollation,
// checkRowMatchWithParentAffinity, valuesEqual*, getColumnValue, readRowValues).
// ============================================================================

func TestMCDC_PragmaForeignKeyCheck(t *testing.T) {
	t.Parallel()

	t.Run("no violations on consistent data", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE fkc_parent(id INTEGER PRIMARY KEY)")
		mustExec(t, db, "CREATE TABLE fkc_child(pid INTEGER REFERENCES fkc_parent(id))")
		mustExec(t, db, "INSERT INTO fkc_parent VALUES(1)")
		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, "INSERT INTO fkc_child VALUES(1)")

		rows, err := db.Query("PRAGMA foreign_key_check")
		if err != nil {
			t.Fatalf("PRAGMA foreign_key_check: %v", err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		if rows.Err() != nil {
			t.Fatalf("rows.Err: %v", rows.Err())
		}
		if n != 0 {
			t.Errorf("expected 0 violations, got %d", n)
		}
	})

	t.Run("table-scoped check on table without FK constraints", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE fkc_nofk(id INTEGER PRIMARY KEY, val TEXT)")
		mustExec(t, db, "INSERT INTO fkc_nofk VALUES(1,'hello')")

		rows, err := db.Query("PRAGMA foreign_key_check(fkc_nofk)")
		if err != nil {
			t.Fatalf("PRAGMA foreign_key_check(fkc_nofk): %v", err)
		}
		defer rows.Close()
		for rows.Next() {
		}
		if rows.Err() != nil {
			t.Fatalf("rows.Err: %v", rows.Err())
		}
	})
}

// ============================================================================
// MC/DC: PRAGMA foreign_keys – GET/SET compound condition
//
// Compound condition in compilePragmaForeignKeys:
//   A = stmt.Value != nil
//
// Outcome = A → SET path; else → GET path
//
// Cases:
//   A=F → GET path (default value returned)
//   A=T → SET path (value persisted and returned on next GET)
// ============================================================================

func TestMCDC_PragmaForeignKeys_GetSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setStmt string
		wantVal int64
	}{
		{
			// A=F: GET only, default is 0 (OFF).
			name:    "A=F: GET foreign_keys returns 0 by default",
			setStmt: "",
			wantVal: 0,
		},
		{
			// A=T: SET ON, then GET returns 1.
			name:    "A=T: SET foreign_keys = ON returns 1",
			setStmt: "PRAGMA foreign_keys = ON",
			wantVal: 1,
		},
		{
			// A=T: SET with integer literal 1.
			name:    "A=T: SET foreign_keys = 1 returns 1",
			setStmt: "PRAGMA foreign_keys = 1",
			wantVal: 1,
		},
		{
			// A=T: SET OFF, then GET returns 0.
			name:    "A=T: SET foreign_keys = OFF returns 0",
			setStmt: "PRAGMA foreign_keys = OFF",
			wantVal: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			if tt.setStmt != "" {
				mustExec(t, db, tt.setStmt)
			}
			var got int64
			if err := db.QueryRow("PRAGMA foreign_keys").Scan(&got); err != nil {
				t.Fatalf("QueryRow PRAGMA foreign_keys: %v", err)
			}
			if got != tt.wantVal {
				t.Errorf("foreign_keys = %d, want %d", got, tt.wantVal)
			}
		})
	}
}
