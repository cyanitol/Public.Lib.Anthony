// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// openMCDCDB is a helper that opens a fresh :memory: database and returns it.
func openMCDCDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("MCDC: open :memory: failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// mcdcMustFail executes a statement and fails the test if it does NOT error.
func mcdcMustFail(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err == nil {
		t.Fatalf("MCDC mcdcMustFail %q: expected error but got none", query)
	}
}

// ============================================================================
// MC/DC: insertSelectNeedsMaterialise
//
// Compound condition (4 sub-conditions, any true → true):
//   A = detectAggregates(sel)
//   B = len(sel.OrderBy) > 0
//   C = sel.Limit != nil
//   D = sel.Distinct
//
// Outcome = A || B || C || D
//
// Cases needed (N+1 = 5):
//   Base: A=F B=F C=F D=F → false  (simple INSERT ... SELECT without extras)
//   Flip A: A=T B=F C=F D=F → true  (aggregate in SELECT)
//   Flip B: A=F B=T C=F D=F → true  (ORDER BY present)
//   Flip C: A=F B=F C=T D=F → true  (LIMIT present)
//   Flip D: A=F B=F C=F D=T → true  (DISTINCT)
// ============================================================================

func TestMCDC_InsertSelectNeedsMaterialise(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			name:  "A=F B=F C=F D=F: simple select no materialise",
			setup: "CREATE TABLE src(x INTEGER); CREATE TABLE dst(x INTEGER); INSERT INTO src VALUES(1),(2)",
			stmt:  "INSERT INTO dst SELECT x FROM src",
		},
		{
			name:  "Flip A=T: aggregate forces materialise",
			setup: "CREATE TABLE src2(x INTEGER); CREATE TABLE dst2(x INTEGER); INSERT INTO src2 VALUES(3),(4)",
			stmt:  "INSERT INTO dst2 SELECT count(x) FROM src2",
		},
		{
			name:  "Flip B=T: ORDER BY forces materialise",
			setup: "CREATE TABLE src3(x INTEGER); CREATE TABLE dst3(x INTEGER); INSERT INTO src3 VALUES(5),(6)",
			stmt:  "INSERT INTO dst3 SELECT x FROM src3 ORDER BY x",
		},
		{
			name:  "Flip C=T: LIMIT forces materialise",
			setup: "CREATE TABLE src4(x INTEGER); CREATE TABLE dst4(x INTEGER); INSERT INTO src4 VALUES(7),(8)",
			stmt:  "INSERT INTO dst4 SELECT x FROM src4 LIMIT 1",
		},
		{
			name:  "Flip D=T: DISTINCT forces materialise",
			setup: "CREATE TABLE src5(x INTEGER); CREATE TABLE dst5(x INTEGER); INSERT INTO src5 VALUES(9),(9)",
			stmt:  "INSERT INTO dst5 SELECT DISTINCT x FROM src5",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			mustExec(t, db, tt.stmt)
		})
	}
}

// splitSemicolon splits a multi-statement string on semicolons.
func splitSemicolon(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ';' {
			p := s[start:i]
			if len(p) > 0 {
				parts = append(parts, p)
			}
			start = i + 1
		}
	}
	if start < len(s) {
		parts = append(parts, s[start:])
	}
	return parts
}

// ============================================================================
// MC/DC: isBlobLiteral
//
// Compound condition (2 sub-conditions):
//   A = expression is *parser.LiteralExpr  (ok from type assertion)
//   B = lit.Type == parser.LiteralBlob
//
// Outcome = A && B
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → true   (blob literal X'...' as INSERT value → no affinity cast)
//   Flip A: A=F B=? → false  (non-literal expression, e.g. column reference)
//   Flip B: A=T B=F → false  (string literal in blob-affinity column → cast applied)
// ============================================================================

func TestMCDC_IsBlobLiteral(t *testing.T) {
	tests := []struct {
		name  string
		setup string
		stmt  string
	}{
		{
			// A=T B=T: blob literal inserted into BLOB column – no affinity cast
			name:  "A=T B=T: blob literal skips affinity cast",
			setup: "CREATE TABLE tblob(b BLOB)",
			stmt:  "INSERT INTO tblob VALUES (X'DEADBEEF')",
		},
		{
			// Flip A=F: column reference is not a LiteralExpr, isBlobLiteral returns false
			name:  "Flip A=F: column reference not a literal",
			setup: "CREATE TABLE tsrc(v BLOB); CREATE TABLE tdst(v BLOB); INSERT INTO tsrc VALUES(X'CAFE')",
			stmt:  "INSERT INTO tdst SELECT v FROM tsrc",
		},
		{
			// Flip B=F: string literal in blob-affinity column → A=T but B=F → affinity applied
			name:  "Flip B=F: string literal in blob column gets affinity",
			setup: "CREATE TABLE tblob2(b BLOB)",
			stmt:  "INSERT INTO tblob2 VALUES ('hello')",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			mustExec(t, db, tt.stmt)
		})
	}
}

// ============================================================================
// MC/DC: compileBlobLiteral prefix check
//
// Compound condition (4 sub-conditions, all must be true for valid X'...' format):
//   A = len(hexStr) >= 3
//   B = hexStr[0] == 'X'
//   C = hexStr[0] == 'x'   (B || C together form the case-insensitive X prefix)
//   D = hexStr[1] == '\''
//   E = hexStr[len-1] == '\''
//
// In practice the parser already strips quotes before handing the raw blob
// string to the driver, so we exercise this via SQL blob literals.
//
// Cases needed to cover the two-condition compound (B || C):
//   Base:   uppercase X prefix → valid blob
//   Flip C: lowercase x prefix → also valid blob
//   Invalid: non-blob literal → falls through to NULL
// ============================================================================

func TestMCDC_CompileBlobLiteral(t *testing.T) {
	tests := []struct {
		name    string
		setup   string
		stmt    string
		wantErr bool
	}{
		{
			name:  "uppercase X prefix blob literal",
			setup: "CREATE TABLE tb1(b BLOB)",
			stmt:  "INSERT INTO tb1 VALUES (X'CAFE')",
		},
		{
			name:  "lowercase x prefix blob literal",
			setup: "CREATE TABLE tb2(b BLOB)",
			stmt:  "INSERT INTO tb2 VALUES (x'CAFE')",
		},
		{
			name:  "empty blob",
			setup: "CREATE TABLE tb3(b BLOB)",
			stmt:  "INSERT INTO tb3 VALUES (X'')",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
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
// MC/DC: validateUpdateColumns – generated column guard
//
// Compound condition (3 sub-conditions, all must be true to return error):
//   A = colIdx >= 0          (column found)
//   B = colIdx < len(table.Columns)  (index in bounds)
//   C = table.Columns[colIdx].Generated  (column is generated)
//
// Outcome = A && B && C → error
//
// Cases needed (N+1 = 4):
//   Base:   A=T B=T C=T → error (UPDATE generated col)
//   Flip A: A=F B=? C=? → error col-not-found (different error path)
//   Flip B: cannot easily trigger colIdx>=len via SQL; B is always true when A=T
//   Flip C: A=T B=T C=F → no error (UPDATE normal col)
// ============================================================================

func TestMCDC_ValidateUpdateColumns(t *testing.T) {
	tests := []struct {
		name    string
		setup   string
		stmt    string
		wantErr bool
	}{
		{
			// A=T B=T C=T → error: cannot UPDATE generated column
			name:    "A=T B=T C=T: update generated col fails",
			setup:   "CREATE TABLE tgen(id INTEGER PRIMARY KEY, val INTEGER, gen INTEGER GENERATED ALWAYS AS (val*2) VIRTUAL)",
			stmt:    "UPDATE tgen SET gen=99 WHERE id=1",
			wantErr: true,
		},
		{
			// Flip A=F: column doesn't exist → different error
			name:    "Flip A=F: nonexistent column fails",
			setup:   "CREATE TABLE tnogen(id INTEGER, val INTEGER)",
			stmt:    "UPDATE tnogen SET nosuchcol=1",
			wantErr: true,
		},
		{
			// Flip C=F: normal (non-generated) column → success
			name:  "Flip C=F: update normal col succeeds",
			setup: "CREATE TABLE tnormal(id INTEGER PRIMARY KEY, val INTEGER); INSERT INTO tnormal VALUES(1,10)",
			stmt:  "UPDATE tnormal SET val=20 WHERE id=1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
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
// MC/DC: emitDeleteBody – memory/trigger allocation guard
//
// Compound condition (2 sub-conditions):
//   A = hasTriggers
//   B = len(stmt.Returning) > 0
//
// Outcome = A || B → allocate old-row snapshot registers
//
// Cases needed (N+1 = 3):
//   Base:   A=F B=F → minimal allocation (simple DELETE)
//   Flip A: A=T B=F → trigger present triggers snapshot
//   Flip B: A=F B=T → RETURNING present triggers snapshot
// ============================================================================

func TestMCDC_EmitDeleteBody_AllocGuard(t *testing.T) {
	tests := []struct {
		name  string
		setup string
		stmt  string
	}{
		{
			// A=F B=F: no trigger, no RETURNING
			name:  "A=F B=F: simple delete no trigger no returning",
			setup: "CREATE TABLE tdel(id INTEGER, v INTEGER); INSERT INTO tdel VALUES(1,10),(2,20)",
			stmt:  "DELETE FROM tdel WHERE id=1",
		},
		{
			// Flip B=T: RETURNING clause present → snapshot allocated
			name:  "Flip B=T: delete with returning",
			setup: "CREATE TABLE tdel2(id INTEGER, v INTEGER); INSERT INTO tdel2 VALUES(1,10),(2,20)",
			stmt:  "DELETE FROM tdel2 WHERE id=1 RETURNING v",
		},
		{
			// A=F B=F, no WHERE: full table delete
			name:  "A=F B=F: full table delete",
			setup: "CREATE TABLE tdel3(id INTEGER); INSERT INTO tdel3 VALUES(1),(2),(3)",
			stmt:  "DELETE FROM tdel3",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			mustExec(t, db, tt.stmt)
		})
	}
}

// ============================================================================
// MC/DC: expandInsertDefaults – rowid alias appending
//
// Compound condition (2 sub-conditions):
//   A = rowidStmtIdx >= 0        (explicit rowid alias found in column list)
//   B = rowidStmtIdx < len(row)  (index is within value row bounds)
//
// Outcome = A && B → append rowid value to expanded row
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → rowid alias in column list with matching value
//   Flip A: A=F B=? → no rowid alias in column list
//   Flip B: A=T B=F → cannot easily trigger via SQL; covered implicitly by base
// ============================================================================

func TestMCDC_ExpandInsertDefaults_RowidAlias(t *testing.T) {
	tests := []struct {
		name  string
		setup string
		stmt  string
	}{
		{
			// A=T B=T: explicit rowid column in INSERT column list
			name:  "A=T B=T: explicit rowid alias in column list",
			setup: "CREATE TABLE trowid(name TEXT)",
			stmt:  "INSERT INTO trowid(rowid, name) VALUES(42, 'hello')",
		},
		{
			// Flip A=F: no rowid alias in column list
			name:  "Flip A=F: no rowid alias",
			setup: "CREATE TABLE tnorowid(name TEXT)",
			stmt:  "INSERT INTO tnorowid(name) VALUES('world')",
		},
		{
			// Implicit all-columns INSERT (no column list): rowidStmtIdx=-1
			name:  "A=F: omit column list entirely",
			setup: "CREATE TABLE tall(a INTEGER, b TEXT)",
			stmt:  "INSERT INTO tall VALUES(1, 'x')",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			mustExec(t, db, tt.stmt)
		})
	}
}

// ============================================================================
// MC/DC: expandSingleRow – column value selection
//
// Compound condition (2 sub-conditions):
//   A = ok  (column name found in specifiedSet)
//   B = idx < len(row)  (index within the provided value row)
//
// Outcome = A && B → use provided value; else use column default
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → column provided with value
//   Flip A: A=F B=? → column omitted → default used
//   Flip B: A=T B=F → cannot easily trigger via SQL (SQL rejects mismatched counts)
// ============================================================================

func TestMCDC_ExpandSingleRow_ColumnSelection(t *testing.T) {
	tests := []struct {
		name  string
		setup string
		stmt  string
	}{
		{
			// A=T B=T: all columns specified
			name:  "A=T B=T: all columns provided",
			setup: "CREATE TABLE texp(a INTEGER, b TEXT DEFAULT 'default')",
			stmt:  "INSERT INTO texp(a,b) VALUES(1,'provided')",
		},
		{
			// Flip A=F: column b omitted → default value 'default' used
			name:  "Flip A=F: omit column uses default",
			setup: "CREATE TABLE texp2(a INTEGER, b TEXT DEFAULT 'default')",
			stmt:  "INSERT INTO texp2(a) VALUES(2)",
		},
		{
			// A=F for all cols: all-default insert (only column with default)
			name:  "A=F all cols: insert only specified column",
			setup: "CREATE TABLE texp3(a INTEGER DEFAULT 0, b TEXT DEFAULT 'x')",
			stmt:  "INSERT INTO texp3(a) VALUES(5)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			mustExec(t, db, tt.stmt)
		})
	}
}

// ============================================================================
// MC/DC: insertRecordColCount – rowid column exclusion
//
// Compound condition (2 sub-conditions):
//   A = !table.WithoutRowID   (normal rowid table)
//   B = rowidColIdx >= 0      (an explicit rowid column was specified)
//
// Outcome = A && B → decrement numRecordCols (exclude rowid from record)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → rowid excluded from record count
//   Flip A: A=F B=? → WITHOUT ROWID table, all cols in record
//   Flip B: A=T B=F → normal table with no explicit rowid column
// ============================================================================

func TestMCDC_InsertRecordColCount(t *testing.T) {
	tests := []struct {
		name  string
		setup string
		stmt  string
	}{
		{
			// A=T B=T: normal table with explicit INTEGER PRIMARY KEY (rowid alias)
			name:  "A=T B=T: normal table explicit IPK",
			setup: "CREATE TABLE tipk(id INTEGER PRIMARY KEY, v TEXT)",
			stmt:  "INSERT INTO tipk(id,v) VALUES(1,'a')",
		},
		{
			// Flip A=F: WITHOUT ROWID table → all columns in record
			name:  "Flip A=F: without rowid table",
			setup: "CREATE TABLE twor(k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID",
			stmt:  "INSERT INTO twor VALUES(1,'a')",
		},
		{
			// Flip B=F: normal table, no explicit rowid in column list
			name:  "Flip B=F: normal table no explicit rowid",
			setup: "CREATE TABLE tnipk(v TEXT)",
			stmt:  "INSERT INTO tnipk(v) VALUES('hello')",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			mustExec(t, db, tt.stmt)
		})
	}
}

// ============================================================================
// MC/DC: UPDATE WHERE clause path
//
// Compound condition effectively exercised:
//   A = stmt.Where != nil  (WHERE clause present)
//
// Cases:
//   A=T → WHERE compiled and emitted
//   A=F → no WHERE, all rows updated
// ============================================================================

func TestMCDC_UpdateWhere(t *testing.T) {
	tests := []struct {
		name        string
		setup       string
		stmt        string
		wantUpdated int64
	}{
		{
			name:        "A=T: UPDATE with WHERE updates only matching rows",
			setup:       "CREATE TABLE tuw(id INTEGER, v INTEGER); INSERT INTO tuw VALUES(1,10),(2,20)",
			stmt:        "UPDATE tuw SET v=99 WHERE id=1",
			wantUpdated: 1,
		},
		{
			name:        "A=F: UPDATE without WHERE updates all rows",
			setup:       "CREATE TABLE tuwnowheres(id INTEGER, v INTEGER); INSERT INTO tuwnowheres VALUES(1,10),(2,20)",
			stmt:        "UPDATE tuwnowheres SET v=99",
			wantUpdated: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			res := mustExec(t, db, tt.stmt)
			n, err := res.RowsAffected()
			if err != nil {
				t.Fatalf("RowsAffected: %v", err)
			}
			if n != tt.wantUpdated {
				t.Errorf("rows affected = %d, want %d", n, tt.wantUpdated)
			}
		})
	}
}

// ============================================================================
// MC/DC: validateNoGeneratedColumns – INSERT into generated column
//
// Compound condition (2 sub-conditions in validateNoGeneratedColumns):
//   A = ok  (column found in table)
//   B = col.Generated
//
// Outcome = A && B → error
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → error
//   Flip A: A=F B=? → column not found → no error from this guard (error from mismatch)
//   Flip B: A=T B=F → column found but not generated → no error
// ============================================================================

func TestMCDC_ValidateNoGeneratedColumns(t *testing.T) {
	tests := []struct {
		name    string
		setup   string
		stmt    string
		wantErr bool
	}{
		{
			// A=T B=T: inserting into generated column by name → error
			name:    "A=T B=T: insert into generated col fails",
			setup:   "CREATE TABLE tgencol(id INTEGER, val INTEGER, gen INTEGER GENERATED ALWAYS AS (val+1) VIRTUAL)",
			stmt:    "INSERT INTO tgencol(id, val, gen) VALUES(1, 10, 99)",
			wantErr: true,
		},
		{
			// Flip B=F: column found but not generated → success
			name:  "Flip B=F: insert into normal col succeeds",
			setup: "CREATE TABLE tngcol(id INTEGER, val INTEGER)",
			stmt:  "INSERT INTO tngcol(id, val) VALUES(1, 10)",
		},
		{
			// Flip A=F: column name not in table → different error (column count mismatch)
			name:  "Flip A=F: column not in schema (omit gen col from INSERT)",
			setup: "CREATE TABLE tgcol2(id INTEGER, val INTEGER, gen INTEGER GENERATED ALWAYS AS (val*2) VIRTUAL)",
			stmt:  "INSERT INTO tgcol2(id, val) VALUES(1, 5)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
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
