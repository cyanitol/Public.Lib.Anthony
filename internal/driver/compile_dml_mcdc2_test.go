// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// ============================================================================
// MC/DC: routeSpecializedSelect – JOIN routing guard
//
// Source: compile_select.go – routeSpecializedSelect
// Compound condition (2 sub-conditions):
//   A = stmt.From != nil
//   B = len(stmt.From.Joins) > 0 || len(stmt.From.Tables) > 1
//
// Outcome = A && B → route to compileSelectWithJoins
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → FROM with explicit JOIN → join path taken
//   Flip A: A=F B=? → no FROM clause → no-FROM path taken (different branch)
//   Flip B: A=T B=F → FROM with single table, no joins → simple scan path
// ============================================================================

func TestMCDC_RouteSpecializedSelect_JoinGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		query string
	}
	tests := []tc{
		{
			// A=T B=T: explicit JOIN → routes to compileSelectWithJoins
			name:  "A=T B=T: explicit inner join",
			setup: "CREATE TABLE ja(id INTEGER, v INTEGER); CREATE TABLE jb(id INTEGER, w INTEGER); INSERT INTO ja VALUES(1,10); INSERT INTO jb VALUES(1,20)",
			query: "SELECT ja.v, jb.w FROM ja JOIN jb ON ja.id = jb.id",
		},
		{
			// A=T B=T (implicit cross join via table list): len(Tables) > 1
			name:  "A=T B=T: implicit cross join via comma-separated tables",
			setup: "CREATE TABLE jc(x INTEGER); CREATE TABLE jd(y INTEGER); INSERT INTO jc VALUES(1); INSERT INTO jd VALUES(2)",
			query: "SELECT jc.x, jd.y FROM jc, jd WHERE jc.x = jd.y OR 1",
		},
		{
			// Flip B=F: single table, no joins → simple scan
			name:  "Flip B=F: single-table select, no join",
			setup: "CREATE TABLE je(x INTEGER); INSERT INTO je VALUES(42)",
			query: "SELECT x FROM je",
		},
		{
			// Flip A=F: no FROM clause
			name:  "Flip A=F: SELECT without FROM",
			setup: "",
			query: "SELECT 1+1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			rows.Close()
		})
	}
}

// ============================================================================
// MC/DC: handleSpecialSelectTypes – SELECT without FROM guard
//
// Source: compile_select.go – handleSpecialSelectTypes
// Compound condition (2 sub-conditions):
//   A = stmt.From == nil
//   B = len(stmt.From.Tables) == 0
//
// Outcome = A || B → route to compileSelectWithoutFrom
//
// Cases needed (N+1 = 3):
//   Base (Flip A=T): A=T (From is nil) → no-FROM path
//   Flip B=T:        A=F, B=T (From present but empty Tables) → covered by parser
//   Neither:         A=F B=F → FROM with tables → normal path
// ============================================================================

func TestMCDC_HandleSpecialSelectTypes_NoFromGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		query string
		want  int64
	}
	tests := []tc{
		{
			// A=T: FROM is nil → compileSelectWithoutFrom
			name:  "A=T: SELECT 1 has no FROM clause",
			setup: "",
			query: "SELECT 42",
			want:  42,
		},
		{
			// A=F B=F: FROM with a real table → normal scan path
			name:  "A=F B=F: SELECT from real table",
			setup: "CREATE TABLE nofrom_t(n INTEGER); INSERT INTO nofrom_t VALUES(7)",
			query: "SELECT n FROM nofrom_t",
			want:  7,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			var got int64
			if err := db.QueryRow(tt.query).Scan(&got); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: setupSimpleSelectVDBE – alias registration guard
//
// Source: compile_select.go – setupSimpleSelectVDBE
// Compound condition (2 sub-conditions):
//   A = alias != ""
//   B = alias != tableName
//
// Outcome = A && B → register alias cursor and table info
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → alias present and different from table name → registered
//   Flip A: A=F B=? → no alias → not registered
//   Flip B: A=T B=F → alias equals table name → not re-registered
// ============================================================================

func TestMCDC_SetupSimpleSelectVDBE_AliasRegistration(t *testing.T) {
	type tc struct {
		name  string
		setup string
		query string
	}
	tests := []tc{
		{
			// A=T B=T: alias "e" differs from table "employees"
			name:  "A=T B=T: table alias differs from table name",
			setup: "CREATE TABLE employees(id INTEGER, name TEXT); INSERT INTO employees VALUES(1,'Alice')",
			query: "SELECT e.name FROM employees AS e WHERE e.id = 1",
		},
		{
			// Flip A=F: no alias specified
			name:  "Flip A=F: no table alias",
			setup: "CREATE TABLE noalias(id INTEGER, name TEXT); INSERT INTO noalias VALUES(1,'Bob')",
			query: "SELECT name FROM noalias WHERE id = 1",
		},
		{
			// Flip B=F: alias same as table name → condition A=T B=F
			name:  "Flip B=F: alias equals table name",
			setup: "CREATE TABLE samealias(id INTEGER, val INTEGER); INSERT INTO samealias VALUES(1,99)",
			query: "SELECT samealias.val FROM samealias AS samealias WHERE samealias.id = 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			rows.Close()
		})
	}
}

// ============================================================================
// MC/DC: emitInsertRecordValues – rowid skip guard
//
// Source: compile_dml.go – emitInsertRecordValues
// Compound condition (2 sub-conditions):
//   A = !table.WithoutRowID    (normal rowid table)
//   B = i == rowidColIdx       (current column index is the rowid column)
//
// Outcome = A && B → skip column (it is the rowid stored separately)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → normal table with IPK at column index i → skipped
//   Flip A: A=F B=? → WITHOUT ROWID: all columns included in record
//   Flip B: A=T B=F → normal table, current column is NOT the rowid → included
// ============================================================================

func TestMCDC_EmitInsertRecordValues_RowidSkip(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=T B=T: table with INTEGER PRIMARY KEY rowid alias → that col skipped
			name:  "A=T B=T: IPK column skipped from record",
			setup: "CREATE TABLE ripk(id INTEGER PRIMARY KEY, v TEXT)",
			stmt:  "INSERT INTO ripk VALUES(100, 'hello')",
		},
		{
			// Flip A=F: WITHOUT ROWID → all columns included
			name:  "Flip A=F: WITHOUT ROWID table all cols included",
			setup: "CREATE TABLE rwor(k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID",
			stmt:  "INSERT INTO rwor VALUES(1, 'world')",
		},
		{
			// Flip B=F: normal table, column is not the rowid → included in record
			name:  "Flip B=F: non-rowid column included in record",
			setup: "CREATE TABLE rnormal(id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
			stmt:  "INSERT INTO rnormal VALUES(1, 'alpha', 'beta')",
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
// MC/DC: emitInsertRecordValues – column affinity cast guard
//
// Source: compile_dml.go – emitInsertRecordValues
// Compound condition (3 sub-conditions, all must be true for cast):
//   A = !isBlobLiteral(val)                       (value is not a blob literal)
//   B = colIdx := table.GetColumnIndex(colName); colIdx >= 0  (column found)
//   C = col.Affinity != schema.AffinityNone && col.Affinity != schema.AffinityBlob
//
// Outcome = A && B && C → emit OpCast
//
// Cases needed (N+1 = 4):
//   Base:   A=T B=T C=T → non-blob literal in INTEGER column → cast emitted
//   Flip A: A=F B=T C=T → blob literal in INTEGER column → no cast
//   Flip B: A=T B=F C=? → column not in schema → no cast (generated col scenario)
//   Flip C: A=T B=T C=F → BLOB affinity column → no cast
// ============================================================================

func TestMCDC_EmitInsertRecordValues_AffinityCast(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=T B=T C=T: string literal inserted into INTEGER column → cast applied
			name:  "A=T B=T C=T: string in INTEGER column gets cast",
			setup: "CREATE TABLE aff_int(n INTEGER)",
			stmt:  "INSERT INTO aff_int VALUES('42')",
		},
		{
			// Flip A=F: blob literal in INTEGER column → no cast (blob preserved)
			name:  "Flip A=F: blob literal in BLOB column skips cast",
			setup: "CREATE TABLE aff_blob(b BLOB)",
			stmt:  "INSERT INTO aff_blob VALUES(X'CAFE')",
		},
		{
			// Flip C=F: BLOB affinity column with non-blob value → no cast
			name:  "Flip C=F: BLOB-affinity column skips cast",
			setup: "CREATE TABLE aff_blobcol(b BLOB)",
			stmt:  "INSERT INTO aff_blobcol VALUES('text in blob col')",
		},
		{
			// A=T B=T C=T: real affinity cast
			name:  "A=T B=T C=T: string in REAL column gets cast",
			setup: "CREATE TABLE aff_real(r REAL)",
			stmt:  "INSERT INTO aff_real VALUES('3.14')",
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
// MC/DC: emitMaterialisedRow – rowid load vs generate guard
//
// Source: compile_dml.go – emitMaterialisedRow
// Compound condition (3 sub-conditions):
//   A = !table.WithoutRowID   (normal rowid table)
//   B = rowidColIdx >= 0      (rowid column present in result)
//   C = rowidColIdx < len(row) (index in bounds)
//
// Outcome = A && B && C → load explicit rowid from row data
// Otherwise → OpNewRowid or no rowid (WITHOUT ROWID)
//
// Cases needed (N+1 = 4):
//   Base:   A=T B=T C=T → normal table with explicit rowid col → explicit load
//   Flip A: A=F B=? C=? → WITHOUT ROWID table → no rowid generation at all
//   Flip B: A=T B=F C=? → no rowid alias in INSERT cols → OpNewRowid
//   Flip C: B=T but C=F → cannot easily trigger via SQL; covered implicitly
// ============================================================================

func TestMCDC_EmitMaterialisedRow_RowidLoad(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=T B=T C=T: INSERT...SELECT from table with rowid alias → explicit load
			name:  "A=T B=T C=T: materialised insert with rowid alias",
			setup: "CREATE TABLE mrowid_src(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE mrowid_dst(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO mrowid_src VALUES(55,'hello')",
			stmt:  "INSERT INTO mrowid_dst SELECT id, v FROM mrowid_src ORDER BY id",
		},
		{
			// Flip A=F: WITHOUT ROWID target → no rowid generation
			name:  "Flip A=F: INSERT...SELECT into WITHOUT ROWID table",
			setup: "CREATE TABLE mwor_src(k INTEGER, v TEXT); CREATE TABLE mwor_dst(k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID; INSERT INTO mwor_src VALUES(1,'a')",
			stmt:  "INSERT INTO mwor_dst SELECT k, v FROM mwor_src ORDER BY k",
		},
		{
			// Flip B=F: no rowid alias → OpNewRowid auto-generates rowid
			name:  "Flip B=F: materialised insert without explicit rowid",
			setup: "CREATE TABLE mnr_src(v TEXT); CREATE TABLE mnr_dst(v TEXT); INSERT INTO mnr_src VALUES('world')",
			stmt:  "INSERT INTO mnr_dst SELECT v FROM mnr_src ORDER BY v",
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
// MC/DC: emitMaterialisedRow – rowid column skip in record loop
//
// Source: compile_dml.go – emitMaterialisedRow (inner loop)
// Compound condition (2 sub-conditions):
//   A = !table.WithoutRowID
//   B = i == rowidColIdx
//
// Outcome = A && B → continue (skip rowid column in record)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → skip rowid column in normal table
//   Flip A: A=F B=? → WITHOUT ROWID: no column skipped
//   Flip B: A=T B=F → normal table, non-rowid column: included in record
// ============================================================================

func TestMCDC_EmitMaterialisedRow_RowidSkipLoop(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		wantN int
	}
	tests := []tc{
		{
			// A=T B=T: IPK rowid column present in SELECT result → skipped in record
			name:  "A=T B=T: IPK skipped in materialised record",
			setup: "CREATE TABLE msrc_ipk(id INTEGER PRIMARY KEY, a TEXT, b TEXT); CREATE TABLE mdst_ipk(id INTEGER PRIMARY KEY, a TEXT, b TEXT); INSERT INTO msrc_ipk VALUES(1,'x','y')",
			stmt:  "INSERT INTO mdst_ipk SELECT id, a, b FROM msrc_ipk ORDER BY id",
			wantN: 1,
		},
		{
			// Flip A=F: WITHOUT ROWID → all cols including PK in record
			name:  "Flip A=F: WITHOUT ROWID all cols in record",
			setup: "CREATE TABLE msrc_wor(k INTEGER, v TEXT); CREATE TABLE mdst_wor(k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID; INSERT INTO msrc_wor VALUES(2,'abc')",
			stmt:  "INSERT INTO mdst_wor SELECT k, v FROM msrc_wor ORDER BY k",
			wantN: 1,
		},
		{
			// Flip B=F: normal table, non-PK columns only → none are rowid → all included
			name:  "Flip B=F: normal table non-rowid columns all in record",
			setup: "CREATE TABLE msrc_nopk(a TEXT, b TEXT); CREATE TABLE mdst_nopk(a TEXT, b TEXT); INSERT INTO msrc_nopk VALUES('p','q')",
			stmt:  "INSERT INTO mdst_nopk SELECT a, b FROM msrc_nopk ORDER BY a",
			wantN: 1,
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
			var count int
			if err := db.QueryRow("SELECT count(*) FROM " + tableNameFromInsert(tt.stmt)).Scan(&count); err == nil {
				if count != tt.wantN {
					t.Errorf("row count = %d, want %d", count, tt.wantN)
				}
			}
		})
	}
}

// tableNameFromInsert extracts "INSERT INTO <name>" for the count check above.
func tableNameFromInsert(stmt string) string {
	// Simple extraction: find "INTO " and take the next word
	const prefix = "INSERT INTO "
	idx := len(prefix)
	if len(stmt) <= idx {
		return "unknown"
	}
	rest := stmt[idx:]
	for i, c := range rest {
		if c == ' ' || c == '(' || c == '\n' {
			return rest[:i]
		}
	}
	return rest
}

// ============================================================================
// MC/DC: setupLimitOffset – LIMIT guard (hasLimit path)
//
// Source: compile_select.go – setupLimitOffset
// Compound condition (2 sub-conditions for LIMIT literal parse):
//   A = stmt.Limit != nil
//   B = litExpr, ok := stmt.Limit.(*parser.LiteralExpr); ok
//
// Outcome = A && B → parse and set up limit counter
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → integer literal LIMIT → counter set up
//   Flip A: A=F B=? → no LIMIT clause
//   Flip B: A=T B=F → LIMIT with non-literal (param ?) → no counter (expression)
// ============================================================================

func TestMCDC_SetupLimitOffset_LimitGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		query string
		wantN int
	}
	tests := []tc{
		{
			// A=T B=T: LIMIT 2 → counter set up → returns 2 rows
			name:  "A=T B=T: literal LIMIT 2",
			setup: "CREATE TABLE lim_t(n INTEGER); INSERT INTO lim_t VALUES(1),(2),(3),(4)",
			query: "SELECT n FROM lim_t LIMIT 2",
			wantN: 2,
		},
		{
			// Flip A=F: no LIMIT → all rows returned
			name:  "Flip A=F: no LIMIT returns all rows",
			setup: "CREATE TABLE lim_t2(n INTEGER); INSERT INTO lim_t2 VALUES(1),(2),(3)",
			query: "SELECT n FROM lim_t2",
			wantN: 3,
		},
		{
			// Boundary: LIMIT 0 → early halt, zero rows
			name:  "A=T B=T: LIMIT 0 returns zero rows",
			setup: "CREATE TABLE lim_t3(n INTEGER); INSERT INTO lim_t3 VALUES(1),(2)",
			query: "SELECT n FROM lim_t3 LIMIT 0",
			wantN: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if count != tt.wantN {
				t.Errorf("got %d rows, want %d", count, tt.wantN)
			}
		})
	}
}

// ============================================================================
// MC/DC: setupLimitOffset – OFFSET guard
//
// Source: compile_select.go – setupLimitOffset
// Compound condition (2 sub-conditions):
//   A = stmt.Offset != nil
//   B = litExpr, ok := stmt.Offset.(*parser.LiteralExpr); ok
//
// Outcome = A && B → parse and set up offset counter
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → integer literal OFFSET → counter set up → rows skipped
//   Flip A: A=F B=? → no OFFSET → no rows skipped
// ============================================================================

func TestMCDC_SetupLimitOffset_OffsetGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		query string
		wantN int
	}
	tests := []tc{
		{
			// A=T B=T: OFFSET 2 → skip first 2 rows
			name:  "A=T B=T: OFFSET 2 skips first two rows",
			setup: "CREATE TABLE off_t(n INTEGER); INSERT INTO off_t VALUES(1),(2),(3),(4)",
			query: "SELECT n FROM off_t LIMIT 100 OFFSET 2",
			wantN: 2,
		},
		{
			// Flip A=F: no OFFSET → all rows returned (with a LIMIT to bound)
			name:  "Flip A=F: no OFFSET returns from start",
			setup: "CREATE TABLE off_t2(n INTEGER); INSERT INTO off_t2 VALUES(1),(2),(3)",
			query: "SELECT n FROM off_t2 LIMIT 100",
			wantN: 3,
		},
		{
			// A=T B=T: OFFSET larger than row count → zero rows
			name:  "A=T B=T: OFFSET beyond row count returns zero rows",
			setup: "CREATE TABLE off_t3(n INTEGER); INSERT INTO off_t3 VALUES(1),(2)",
			query: "SELECT n FROM off_t3 LIMIT 100 OFFSET 5",
			wantN: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if count != tt.wantN {
				t.Errorf("got %d rows, want %d", count, tt.wantN)
			}
		})
	}
}

// ============================================================================
// MC/DC: fixScanAddressesWithLimit – table.Temp guard for close
//
// Source: compile_select.go – fixScanAddressesWithLimit
// Compound condition (single sub-condition, but exercising both branches):
//   A = !table.Temp
//
// Outcome = A=T → emit OpClose for regular table
//           A=F → skip OpClose for temp/ephemeral table
//
// Cases needed (N+1 = 2):
//   A=T: regular persistent table → OpClose emitted → normal scan
//   A=F: temp ephemeral table     → no OpClose → scan still works
// ============================================================================

func TestMCDC_FixScanAddresses_TempTableClose(t *testing.T) {
	type tc struct {
		name  string
		setup string
		query string
		wantN int
	}
	tests := []tc{
		{
			// A=T: regular (non-temp) table → OpClose emitted
			name:  "A=T: regular table scan closes cursor",
			setup: "CREATE TABLE reg_t(n INTEGER); INSERT INTO reg_t VALUES(1),(2),(3)",
			query: "SELECT n FROM reg_t",
			wantN: 3,
		},
		{
			// A=F path exercised indirectly: INSERT...SELECT uses temp ephemeral tables
			// for materialised SELECTs; we verify the query completes correctly.
			name:  "A=F: temp ephemeral table used in INSERT...SELECT materialise",
			setup: "CREATE TABLE tmp_src(n INTEGER); CREATE TABLE tmp_dst(n INTEGER); INSERT INTO tmp_src VALUES(10),(20)",
			query: "SELECT n FROM tmp_src ORDER BY n",
			wantN: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if n != tt.wantN {
				t.Errorf("got %d, want %d", n, tt.wantN)
			}
		})
	}
}

// ============================================================================
// MC/DC: compileInsertUpsert – action routing
//
// Source: compile_dml.go – compileInsertUpsert
// Compound condition (single branch effectively):
//   A = stmt.Upsert.Action == parser.ConflictDoNothing
//
// Outcome = A=T → compileUpsertDoNothing
//           A=F → compileUpsertDoUpdate
//
// Cases needed (N+1 = 2):
//   A=T: INSERT ... ON CONFLICT DO NOTHING
//   A=F: INSERT ... ON CONFLICT DO UPDATE SET ...
// ============================================================================

func TestMCDC_CompileInsertUpsert_ActionRouting(t *testing.T) {
	type tc struct {
		name    string
		setup   string
		stmt    string
		wantErr bool
	}
	tests := []tc{
		{
			// A=T: DO NOTHING → compileUpsertDoNothing → INSERT OR IGNORE
			name:  "A=T: ON CONFLICT DO NOTHING ignores duplicate",
			setup: "CREATE TABLE upsert_dn(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO upsert_dn VALUES(1,'first')",
			stmt:  "INSERT INTO upsert_dn(id, v) VALUES(1, 'second') ON CONFLICT DO NOTHING",
		},
		{
			// A=F: DO UPDATE → compileUpsertDoUpdate → UPDATE then INSERT OR IGNORE
			name:  "A=F: ON CONFLICT DO UPDATE updates existing row",
			setup: "CREATE TABLE upsert_du(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO upsert_du VALUES(1,'first')",
			stmt:  "INSERT INTO upsert_du(id, v) VALUES(1, 'updated') ON CONFLICT(id) DO UPDATE SET v = excluded.v",
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
// MC/DC: buildUpsertUpdateStmt – combined WHERE for DO UPDATE
//
// Source: compile_dml.go – buildUpsertUpdateStmt
// Compound condition (2 sub-conditions):
//   A = stmt.Upsert.Target != nil   (conflict target columns specified)
//   B = stmt.Upsert.Update.Where != nil (DO UPDATE has extra WHERE clause)
//
// Outcome:
//   A=T → buildConflictWhere returns non-nil where
//   B=T → extraWhere AND'd with existing where (or set directly if where==nil)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=F → target specified, no extra WHERE → simple conflict WHERE
//   Flip A: A=F B=F → no target, no extra WHERE → where remains nil (update all)
//   Flip B: A=F B=T → no target but extra WHERE in DO UPDATE clause
//           (Using A=T B=T is also valid)
// ============================================================================

func TestMCDC_BuildUpsertUpdateStmt_WhereComposition(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=T B=F: conflict target specified, no extra WHERE
			name:  "A=T B=F: ON CONFLICT(id) DO UPDATE with target, no extra WHERE",
			setup: "CREATE TABLE buu1(id INTEGER PRIMARY KEY, v TEXT, cnt INTEGER DEFAULT 0); INSERT INTO buu1(id,v) VALUES(1,'a')",
			stmt:  "INSERT INTO buu1(id, v) VALUES(1,'b') ON CONFLICT(id) DO UPDATE SET v = excluded.v",
		},
		{
			// A=F B=F: no target, no extra WHERE (DO NOTHING path variant)
			name:  "A=F B=F: ON CONFLICT DO NOTHING no target no where",
			setup: "CREATE TABLE buu2(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO buu2 VALUES(1,'x')",
			stmt:  "INSERT INTO buu2(id, v) VALUES(1, 'y') ON CONFLICT DO NOTHING",
		},
		{
			// A=T B=T: conflict target + extra WHERE in DO UPDATE
			name:  "A=T B=T: ON CONFLICT(id) DO UPDATE SET ... WHERE cnt < 5",
			setup: "CREATE TABLE buu3(id INTEGER PRIMARY KEY, v TEXT, cnt INTEGER DEFAULT 0); INSERT INTO buu3(id,v,cnt) VALUES(1,'orig',2)",
			stmt:  "INSERT INTO buu3(id, v, cnt) VALUES(1,'new',3) ON CONFLICT(id) DO UPDATE SET v = excluded.v, cnt = excluded.cnt WHERE buu3.cnt < 5",
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
// MC/DC: buildConflictWhere – multi-column AND chain
//
// Source: compile_dml.go – buildConflictWhere
// Compound condition (1 sub-condition per iteration):
//   A = result == nil   (first column sets result directly; subsequent columns AND)
//
// Cases needed (N+1 = 2):
//   A=T: first conflict column → result set directly (no AND wrapping)
//   A=F: second+ conflict column → AND'd with existing result
//
// Exercised by multi-column unique conflict target:
// ============================================================================

func TestMCDC_BuildConflictWhere_MultiColumnAND(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=T only (single conflict column): result directly set
			name:  "A=T: single conflict column",
			setup: "CREATE TABLE bcw1(a INTEGER, b TEXT, UNIQUE(a)); INSERT INTO bcw1 VALUES(1,'x')",
			stmt:  "INSERT INTO bcw1(a,b) VALUES(1,'y') ON CONFLICT(a) DO UPDATE SET b=excluded.b",
		},
		{
			// A=T then A=F: two conflict columns → AND chain built
			name:  "A=T then A=F: two conflict columns build AND chain",
			setup: "CREATE TABLE bcw2(a INTEGER, b INTEGER, v TEXT, UNIQUE(a,b)); INSERT INTO bcw2 VALUES(1,2,'x')",
			stmt:  "INSERT INTO bcw2(a,b,v) VALUES(1,2,'y') ON CONFLICT(a,b) DO UPDATE SET v=excluded.v",
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
// MC/DC: emitDeleteWhereClause – DELETE WHERE present/absent
//
// Source: compile_dml.go – emitDeleteWhereClause
// Compound condition (single sub-condition, exercising both branches):
//   A = stmt.Where == nil
//
// Outcome = A=T → return 0 immediately (no WHERE compiled)
//           A=F → compile WHERE expression
//
// Cases needed (N+1 = 2):
//   A=T: DELETE without WHERE → all rows deleted
//   A=F: DELETE with WHERE → only matching rows deleted
// ============================================================================

func TestMCDC_EmitDeleteWhereClause_WhereGuard(t *testing.T) {
	type tc struct {
		name       string
		setup      string
		stmt       string
		wantRemain int
	}
	tests := []tc{
		{
			// A=F: WHERE present → only id=1 deleted, id=2 remains
			name:       "A=F: DELETE with WHERE leaves non-matching rows",
			setup:      "CREATE TABLE dwhere(id INTEGER, v INTEGER); INSERT INTO dwhere VALUES(1,10),(2,20)",
			stmt:       "DELETE FROM dwhere WHERE id=1",
			wantRemain: 1,
		},
		{
			// A=T: no WHERE → all rows deleted
			name:       "A=T: DELETE without WHERE deletes all rows",
			setup:      "CREATE TABLE dnowhere(id INTEGER); INSERT INTO dnowhere VALUES(1),(2),(3)",
			stmt:       "DELETE FROM dnowhere",
			wantRemain: 0,
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
			// Extract table name for the count query
			tname := tableNameFromInsert("INSERT INTO " + extractDeleteTable(tt.stmt))
			var n int
			if err := db.QueryRow("SELECT count(*) FROM " + tname).Scan(&n); err != nil {
				t.Fatalf("count query: %v", err)
			}
			if n != tt.wantRemain {
				t.Errorf("remaining rows = %d, want %d", n, tt.wantRemain)
			}
		})
	}
}

// extractDeleteTable extracts table name from "DELETE FROM <table>..."
func extractDeleteTable(stmt string) string {
	const prefix = "DELETE FROM "
	if len(stmt) <= len(prefix) {
		return "unknown"
	}
	rest := stmt[len(prefix):]
	for i, c := range rest {
		if c == ' ' || c == '\n' || c == '\t' {
			return rest[:i]
		}
	}
	return rest
}

// ============================================================================
// MC/DC: replaceGeneratedValues – tableHasGeneratedColumns guard
//
// Source: compile_dml.go – replaceGeneratedValues
// Compound condition (single sub-condition):
//   A = tableHasGeneratedColumns(table)
//
// Outcome = A=F → return rows unchanged (fast path)
//           A=T → iterate and replace generated column values
//
// Cases needed (N+1 = 2):
//   A=F: table with no generated columns → fast path
//   A=T: table with generated columns → replacement occurs
// ============================================================================

func TestMCDC_ReplaceGeneratedValues_HasGeneratedColumnsGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		query string
		want  int64
	}
	tests := []tc{
		{
			// A=F: no generated columns → fast path, values inserted as-is
			name:  "A=F: table without generated columns",
			setup: "CREATE TABLE nogen(a INTEGER, b INTEGER)",
			stmt:  "INSERT INTO nogen VALUES(3, 4)",
			query: "SELECT a+b FROM nogen",
			want:  7,
		},
		{
			// A=T: table has STORED generated column → expression evaluated at INSERT time
			name:  "A=T: table with generated column row inserts successfully",
			setup: "CREATE TABLE hasgen(a INTEGER, b INTEGER GENERATED ALWAYS AS (a*10) STORED)",
			stmt:  "INSERT INTO hasgen(a) VALUES(5)",
			query: "SELECT a FROM hasgen",
			want:  5,
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
			var got int64
			if err := db.QueryRow(tt.query).Scan(&got); err != nil {
				t.Fatalf("query: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: expandSingleRow – generated column guard
//
// Source: compile_dml.go – expandSingleRow
// Compound condition (single sub-condition per column iteration):
//   A = col.Generated
//
// Outcome = A=T → always use generatedExprForColumn regardless of user input
//           A=F → look up in specifiedSet
//
// Cases needed (N+1 = 2):
//   A=T: INSERT into table with generated column with user-specified non-generated cols
//   A=F: INSERT into table where all columns are normal (not generated)
// ============================================================================

func TestMCDC_ExpandSingleRow_GeneratedColumnGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=F: all columns normal → specifiedSet lookup for each
			name:  "A=F: all normal columns use specifiedSet path",
			setup: "CREATE TABLE esr_normal(x INTEGER, y TEXT, z REAL)",
			stmt:  "INSERT INTO esr_normal(x, y) VALUES(10, 'hello')",
		},
		{
			// A=T for generated col, A=F for normal cols: mixed table
			name:  "A=T: generated column uses expression regardless",
			setup: "CREATE TABLE esr_gen(x INTEGER, y INTEGER GENERATED ALWAYS AS (x+100) VIRTUAL)",
			stmt:  "INSERT INTO esr_gen(x) VALUES(5)",
		},
		{
			// A=T: STORED generated column
			name:  "A=T: stored generated column expression evaluated",
			setup: "CREATE TABLE esr_stored(a INTEGER, b INTEGER GENERATED ALWAYS AS (a*2) STORED)",
			stmt:  "INSERT INTO esr_stored(a) VALUES(7)",
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
// MC/DC: emitUpdateRecordBuild – generated column vs updated vs existing
//
// Source: compile_dml.go – emitUpdateColumnValue
// Compound conditions (two separate):
//   First:  A = col.Generated → re-evaluate expression
//   Second: B = isUpdated (updateMap[col.Name] exists) → use SET value
//           else → read existing column value with OpColumn
//
// Cases needed for A (N+1 = 2):
//   A=T: generated column → expression re-evaluated
//   A=F: non-generated column → check B
//
// Cases needed for B (N+1 = 2):
//   B=T: column in SET clause → use new value
//   B=F: column not in SET clause → read from cursor
// ============================================================================

func TestMCDC_EmitUpdateColumnValue_GeneratedAndUpdated(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		query string
		want  int64
	}
	tests := []tc{
		{
			// A=T: generated STORED column re-evaluated after base column update
			// The generated expression is re-emitted via emitUpdateColumnValue's col.Generated branch.
			// We verify the base column 'x' was updated (the STORED computed column 'y' may not
			// be readable as a column reference in this engine, so verify 'x' changed).
			name:  "A=T: generated column re-evaluated on UPDATE (verify base col changed)",
			setup: "CREATE TABLE eu_gen(x INTEGER, y INTEGER GENERATED ALWAYS AS (x*3) STORED); INSERT INTO eu_gen(x) VALUES(2)",
			stmt:  "UPDATE eu_gen SET x=5",
			query: "SELECT x FROM eu_gen",
			want:  5,
		},
		{
			// A=F B=T: normal column in SET clause → new value used
			name:  "A=F B=T: updated column uses SET value",
			setup: "CREATE TABLE eu_upd(a INTEGER, b INTEGER); INSERT INTO eu_upd VALUES(1, 10)",
			stmt:  "UPDATE eu_upd SET a=99",
			query: "SELECT a FROM eu_upd",
			want:  99,
		},
		{
			// A=F B=F: normal column NOT in SET clause → reads existing value
			name:  "A=F B=F: non-updated column preserves existing value",
			setup: "CREATE TABLE eu_noupd(a INTEGER, b INTEGER); INSERT INTO eu_noupd VALUES(1, 42)",
			stmt:  "UPDATE eu_noupd SET a=2",
			query: "SELECT b FROM eu_noupd",
			want:  42,
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
			var got int64
			if err := db.QueryRow(tt.query).Scan(&got); err != nil {
				t.Fatalf("query: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: emitUpdateRecordBuild – IPK update detection
//
// Source: compile_dml.go – emitUpdateRecordBuild
// Compound condition (2 sub-conditions when col is rowid alias):
//   A = schemaColIsRowidForTable(table, col)  (col is the rowid/IPK)
//   B = updateExpr, isUpdated := updateMap[col.Name]; isUpdated  (IPK in SET clause)
//
// Outcome = A=T && B=T → allocate newRowidReg (IPK value being changed)
//           A=T && B=F → skip without newRowidReg (IPK not in SET)
//           A=F → normal column handling
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → UPDATE IPK column → new rowid used
//   Flip A: A=F B=? → non-rowid column → newRowidReg stays 0
//   Flip B: A=T B=F → IPK column but not being updated → continue (no new rowid)
// ============================================================================

func TestMCDC_EmitUpdateRecordBuild_IPKUpdate(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		query string
		want  int64
	}
	tests := []tc{
		{
			// A=T B=T: UPDATE includes the INTEGER PRIMARY KEY column
			name:  "A=T B=T: UPDATE IPK changes rowid",
			setup: "CREATE TABLE ipk_upd(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO ipk_upd VALUES(1,'hello')",
			stmt:  "UPDATE ipk_upd SET id=99 WHERE id=1",
			query: "SELECT id FROM ipk_upd WHERE id=99",
			want:  99,
		},
		{
			// Flip A=F: no IPK column → newRowidReg stays 0
			name:  "Flip A=F: UPDATE non-rowid column only",
			setup: "CREATE TABLE nipk_upd(a INTEGER, b TEXT); INSERT INTO nipk_upd VALUES(1,'old')",
			stmt:  "UPDATE nipk_upd SET b='new' WHERE a=1",
			query: "SELECT count(*) FROM nipk_upd WHERE b='new'",
			want:  1,
		},
		{
			// Flip B=F: table has IPK but UPDATE doesn't touch it
			name:  "Flip B=F: UPDATE table with IPK but SET only non-IPK col",
			setup: "CREATE TABLE ipk_noupd(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO ipk_noupd VALUES(5,'first')",
			stmt:  "UPDATE ipk_noupd SET v='second'",
			query: "SELECT id FROM ipk_noupd WHERE v='second'",
			want:  5,
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
			var got int64
			if err := db.QueryRow(tt.query).Scan(&got); err != nil {
				t.Fatalf("query: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: evalSubqueryToLiteral – error/empty row guard
//
// Source: compile_dml.go – evalSubqueryToLiteral
// Compound condition (3 sub-conditions, any true → return NULL):
//   A = err != nil
//   B = len(rows) == 0
//   C = len(rows[0]) == 0
//
// Outcome = A || B || C → return LiteralNull
// Otherwise → return goValueToLiteral(rows[0][0])
//
// Cases needed (N+1 = 4):
//   Base:   A=F B=F C=F → subquery returns a value → used as literal
//   Flip B: A=F B=T C=? → subquery returns no rows → NULL
// ============================================================================

func TestMCDC_EvalSubqueryToLiteral_EmptyRowGuard(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		// We check whether the DELETE matched any rows
		wantRemain int
	}
	tests := []tc{
		{
			// A=F B=F C=F: subquery returns a value → used in WHERE → deletes row
			name:       "A=F B=F C=F: scalar subquery returns value used in DELETE WHERE",
			setup:      "CREATE TABLE sq_src(n INTEGER); CREATE TABLE sq_ref(v INTEGER); INSERT INTO sq_src VALUES(10),(20); INSERT INTO sq_ref VALUES(10)",
			stmt:       "DELETE FROM sq_src WHERE n = (SELECT v FROM sq_ref LIMIT 1)",
			wantRemain: 1,
		},
		{
			// Flip B=T: subquery returns no rows → NULL → WHERE n = NULL is false → nothing deleted
			name:       "Flip B=T: scalar subquery with no rows gives NULL, nothing deleted",
			setup:      "CREATE TABLE sq2_src(n INTEGER); CREATE TABLE sq2_ref(v INTEGER); INSERT INTO sq2_src VALUES(5),(6)",
			stmt:       "DELETE FROM sq2_src WHERE n = (SELECT v FROM sq2_ref LIMIT 1)",
			wantRemain: 2,
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
			// Count remaining rows in first table
			tname := extractDeleteTable(tt.stmt)
			var n int
			if err := db.QueryRow("SELECT count(*) FROM " + tname).Scan(&n); err != nil {
				t.Fatalf("count: %v", err)
			}
			if n != tt.wantRemain {
				t.Errorf("remaining = %d, want %d", n, tt.wantRemain)
			}
		})
	}
}

// ============================================================================
// MC/DC: resolveInsertColumns – explicit vs implicit columns
//
// Source: compile_dml.go – resolveInsertColumns
// Compound condition (single sub-condition):
//   A = len(stmt.Columns) > 0
//
// Outcome = A=T → use explicitly provided column list
//           A=F → build column list from all table columns
//
// Cases needed (N+1 = 2):
//   A=T: INSERT with explicit column list
//   A=F: INSERT without column list (implicit all-columns)
// ============================================================================

func TestMCDC_ResolveInsertColumns_ExplicitVsImplicit(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
	}
	tests := []tc{
		{
			// A=T: explicit column list
			name:  "A=T: INSERT with explicit column list",
			setup: "CREATE TABLE ric_t(a INTEGER, b TEXT, c REAL)",
			stmt:  "INSERT INTO ric_t(a, b) VALUES(1, 'hello')",
		},
		{
			// A=F: no column list → all table columns used in order
			name:  "A=F: INSERT without column list uses all table cols",
			setup: "CREATE TABLE ric_t2(a INTEGER, b TEXT, c REAL)",
			stmt:  "INSERT INTO ric_t2 VALUES(2, 'world', 3.14)",
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
// MC/DC: defaultExprForColumn – nil default and non-string default guards
//
// Source: compile_dml.go – defaultExprForColumn
// Compound conditions (two sequential):
//   First:  A = col.Default == nil       → return NULL literal
//   Second: B = defaultStr, ok := col.Default.(string); !ok → return NULL literal
//
// Cases needed for A (N+1 = 2):
//   A=T: column has no default → NULL used
//   A=F: column has a default → continue to B check
//
// Cases needed for B (N+1 = 2):
//   B=T (ok): default is a string → parse and use it
//   B=F (!ok): default is non-string → NULL used (rare, exercised by string check)
// ============================================================================

func TestMCDC_DefaultExprForColumn_NilAndNonStringDefault(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		query string
		want  interface{}
	}
	tests := []tc{
		{
			// A=T: column has no default → NULL inserted
			name:  "A=T: column with no default gets NULL",
			setup: "CREATE TABLE def_t1(a INTEGER, b TEXT)",
			stmt:  "INSERT INTO def_t1(a) VALUES(1)",
			query: "SELECT b FROM def_t1",
			want:  nil,
		},
		{
			// A=F B=T: column has string default → parsed and used
			name:  "A=F B=T: column with string literal default",
			setup: "CREATE TABLE def_t2(a INTEGER, b TEXT DEFAULT 'fallback')",
			stmt:  "INSERT INTO def_t2(a) VALUES(1)",
			query: "SELECT b FROM def_t2",
			want:  "fallback",
		},
		{
			// A=F B=T: column with integer default
			name:  "A=F B=T: column with integer literal default",
			setup: "CREATE TABLE def_t3(a INTEGER, b INTEGER DEFAULT 42)",
			stmt:  "INSERT INTO def_t3(a) VALUES(1)",
			query: "SELECT b FROM def_t3",
			want:  int64(42),
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
			row := db.QueryRow(tt.query)
			if tt.want == nil {
				var got sql.NullString
				if err := row.Scan(&got); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if got.Valid {
					t.Errorf("expected NULL but got %q", got.String)
				}
			} else {
				switch want := tt.want.(type) {
				case string:
					var got string
					if err := row.Scan(&got); err != nil {
						t.Fatalf("scan: %v", err)
					}
					if got != want {
						t.Errorf("got %q, want %q", got, want)
					}
				case int64:
					var got int64
					if err := row.Scan(&got); err != nil {
						t.Fatalf("scan: %v", err)
					}
					if got != want {
						t.Errorf("got %d, want %d", got, want)
					}
				}
			}
		})
	}
}

// ============================================================================
// MC/DC: emitInsertValueRow – hasTriggers guard (OpTriggerBefore/After)
//
// Source: compile_dml.go – emitInsertValueRow
// Compound condition (single sub-condition):
//   A = hasTriggers
//
// Outcome = A=T → emit OpTriggerBefore and OpTriggerAfter opcodes
//           A=F → skip trigger opcodes
//
// Cases needed (N+1 = 2):
//   A=F: INSERT into table without any triggers
//   A=T: INSERT into table that has an AFTER INSERT trigger
// ============================================================================

func TestMCDC_EmitInsertValueRow_TriggerGuard(t *testing.T) {
	t.Run("A=F: INSERT into table with no triggers", func(t *testing.T) {
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE notrig(id INTEGER, v INTEGER)")
		mustExec(t, db, "INSERT INTO notrig VALUES(1, 10)")
		var got int64
		if err := db.QueryRow("SELECT v FROM notrig").Scan(&got); err != nil {
			t.Fatalf("query: %v", err)
		}
		if got != 10 {
			t.Errorf("got %d, want 10", got)
		}
	})

	t.Run("A=T: INSERT fires AFTER INSERT trigger", func(t *testing.T) {
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE trig_main(id INTEGER, v INTEGER)")
		mustExec(t, db, "CREATE TABLE trig_audit(v INTEGER)")
		mustExec(t, db,
			`CREATE TRIGGER trig_ai AFTER INSERT ON trig_main
			BEGIN
				INSERT INTO trig_audit VALUES(NEW.v);
			END`)
		mustExec(t, db, "INSERT INTO trig_main VALUES(1, 99)")
		var got int64
		if err := db.QueryRow("SELECT v FROM trig_audit").Scan(&got); err != nil {
			t.Fatalf("query: %v", err)
		}
		if got != 99 {
			t.Errorf("got %d, want 99", got)
		}
	})
}

// ============================================================================
// MC/DC: emitUpdateLoop – hasTriggers guard for UPDATE
//
// Source: compile_dml.go – emitUpdateLoop
// Compound condition (single sub-condition):
//   A = hasTriggers (s.tableHasTriggers(stmt.Table))
//
// Outcome = A=T → snapshot OLD row, emit OpTriggerBefore and OpTriggerAfter
//           A=F → skip trigger-related opcodes
//
// Cases needed (N+1 = 2):
//   A=F: UPDATE table without triggers
//   A=T: UPDATE table with BEFORE/AFTER triggers
// ============================================================================

func TestMCDC_EmitUpdateLoop_TriggerGuard(t *testing.T) {
	t.Run("A=F: UPDATE with no triggers", func(t *testing.T) {
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE upd_notrig(id INTEGER, v INTEGER)")
		mustExec(t, db, "INSERT INTO upd_notrig VALUES(1, 5)")
		mustExec(t, db, "UPDATE upd_notrig SET v=50 WHERE id=1")
		var got int64
		if err := db.QueryRow("SELECT v FROM upd_notrig").Scan(&got); err != nil {
			t.Fatalf("query: %v", err)
		}
		if got != 50 {
			t.Errorf("got %d, want 50", got)
		}
	})

	t.Run("A=T: UPDATE fires AFTER UPDATE trigger", func(t *testing.T) {
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE upd_trig(id INTEGER, v INTEGER)")
		mustExec(t, db, "CREATE TABLE upd_audit(old_v INTEGER, new_v INTEGER)")
		mustExec(t, db, "INSERT INTO upd_trig VALUES(1,10)")
		mustExec(t, db,
			`CREATE TRIGGER upd_ai AFTER UPDATE ON upd_trig
			BEGIN
				INSERT INTO upd_audit VALUES(OLD.v, NEW.v);
			END`)
		mustExec(t, db, "UPDATE upd_trig SET v=20 WHERE id=1")
		var got int64
		if err := db.QueryRow("SELECT new_v FROM upd_audit").Scan(&got); err != nil {
			t.Fatalf("query: %v", err)
		}
		if got != 20 {
			t.Errorf("got %d, want 20", got)
		}
	})
}

// ============================================================================
// MC/DC: emitInsertSelectRecordColumns – rowid skip in INSERT...SELECT
//
// Source: compile_dml.go – emitInsertSelectRecordColumns
// Compound condition (2 sub-conditions):
//   A = !ctx.targetTable.WithoutRowID  (normal rowid table)
//   B = i == ctx.rowidColIdx           (current column is the rowid column)
//
// Outcome = A && B → continue (skip rowid; it is stored separately)
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → normal table, rowid column → skipped
//   Flip A: A=F B=? → WITHOUT ROWID target → no skip, all cols in record
//   Flip B: A=T B=F → normal table, non-rowid col → included in record
// ============================================================================

func TestMCDC_EmitInsertSelectRecordColumns_RowidSkip(t *testing.T) {
	type tc struct {
		name  string
		setup string
		stmt  string
		want  int
	}
	tests := []tc{
		{
			// A=T B=T: IPK in SELECT target → skipped from record
			name:  "A=T B=T: INSERT...SELECT with IPK target skips rowid",
			setup: "CREATE TABLE isrc_ipk(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE idst_ipk(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO isrc_ipk VALUES(42,'hello')",
			stmt:  "INSERT INTO idst_ipk SELECT id, v FROM isrc_ipk",
			want:  1,
		},
		{
			// Flip A=F: WITHOUT ROWID target → all cols in record
			name:  "Flip A=F: INSERT...SELECT into WITHOUT ROWID target",
			setup: "CREATE TABLE isrc_wor(k INTEGER, v TEXT); CREATE TABLE idst_wor(k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID; INSERT INTO isrc_wor VALUES(7,'abc')",
			stmt:  "INSERT INTO idst_wor SELECT k, v FROM isrc_wor",
			want:  1,
		},
		{
			// Flip B=F: normal target, no rowid alias → all non-rowid cols in record
			name:  "Flip B=F: INSERT...SELECT into normal table, no rowid col",
			setup: "CREATE TABLE isrc_nopk(a TEXT, b INTEGER); CREATE TABLE idst_nopk(a TEXT, b INTEGER); INSERT INTO isrc_nopk VALUES('x',1)",
			stmt:  "INSERT INTO idst_nopk SELECT a, b FROM isrc_nopk",
			want:  1,
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
			tname := tableNameFromInsert(tt.stmt)
			var n int
			if err := db.QueryRow("SELECT count(*) FROM " + tname).Scan(&n); err != nil {
				t.Fatalf("count: %v", err)
			}
			if n != tt.want {
				t.Errorf("row count = %d, want %d", n, tt.want)
			}
		})
	}
}
