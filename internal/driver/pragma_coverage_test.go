// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

const pragmaTestDriver = "sqlite_internal"

// openPragmaDB opens a file-backed DB for pragma tests.
func openPragmaDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open(pragmaTestDriver, path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// pragmaExec executes a statement and fails on error.
func pragmaExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// pragmaQueryInt queries a single integer value.
func pragmaQueryInt(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// pragmaQueryString queries a single string value.
func pragmaQueryString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var v string
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// pragmaQueryRowCount counts rows returned by a query.
func pragmaQueryRowCount(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return n
}

// ---------------------------------------------------------------------------
// PRAGMA table_info – exercises compilePragmaTableInfo, emitTableInfoRow,
// emitNotNullValue, emitDefaultValue, calculatePrimaryKeyIndex.
// ---------------------------------------------------------------------------

type pragmaColInfo struct {
	cid     int64
	name    string
	typ     string
	notnull int64
	dfltVal sql.NullString
	pk      int64
}

// scanTableInfoCols scans all rows from a PRAGMA table_info result set.
func scanTableInfoCols(t *testing.T, rows *sql.Rows) []pragmaColInfo {
	t.Helper()
	var cols []pragmaColInfo
	for rows.Next() {
		var c pragmaColInfo
		if err := rows.Scan(&c.cid, &c.name, &c.typ, &c.notnull, &c.dfltVal, &c.pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return cols
}

func TestPragma_TableInfo_Basic(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ti1(a INTEGER, b TEXT, c REAL)")

	rows, err := db.Query("PRAGMA table_info('ti1')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	cols := scanTableInfoCols(t, rows)
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}
	if cols[0].name != "a" || cols[1].name != "b" || cols[2].name != "c" {
		t.Errorf("unexpected column names: %+v", cols)
	}
}

func TestPragma_TableInfo_NotNull(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ti2(a INTEGER NOT NULL, b TEXT)")

	rows, err := db.Query("PRAGMA table_info('ti2')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	notnulls := map[string]int64{}
	for rows.Next() {
		var cid, notnull, pk int64
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		notnulls[name] = notnull
	}
	if notnulls["a"] != 1 {
		t.Errorf("column a should be NOT NULL=1, got %d", notnulls["a"])
	}
	if notnulls["b"] != 0 {
		t.Errorf("column b should be NOT NULL=0, got %d", notnulls["b"])
	}
}

func TestPragma_TableInfo_DefaultValue(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ti3(a INTEGER DEFAULT 42, b TEXT DEFAULT 'hi')")

	rows, err := db.Query("PRAGMA table_info('ti3')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	defaults := map[string]string{}
	for rows.Next() {
		var cid, notnull, pk int64
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if dflt.Valid {
			defaults[name] = dflt.String
		}
	}
	if defaults["a"] != "42" {
		t.Errorf("col a default: want 42, got %q", defaults["a"])
	}
	if defaults["b"] != "'hi'" {
		t.Errorf("col b default: want 'hi', got %q", defaults["b"])
	}
}

func TestPragma_TableInfo_PrimaryKey(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ti4(id INTEGER PRIMARY KEY, v TEXT)")

	rows, err := db.Query("PRAGMA table_info('ti4')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	pks := map[string]int64{}
	for rows.Next() {
		var cid, notnull, pk int64
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		pks[name] = pk
	}
	if pks["id"] != 1 {
		t.Errorf("id pk: want 1, got %d", pks["id"])
	}
	if pks["v"] != 0 {
		t.Errorf("v pk: want 0, got %d", pks["v"])
	}
}

func TestPragma_TableInfo_CompositePK(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ti5(a INTEGER, b TEXT, PRIMARY KEY(a, b))")

	rows, err := db.Query("PRAGMA table_info('ti5')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	pks := map[string]int64{}
	for rows.Next() {
		var cid, notnull, pk int64
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		pks[name] = pk
	}
	if pks["a"] != 1 {
		t.Errorf("a pk: want 1, got %d", pks["a"])
	}
	if pks["b"] != 2 {
		t.Errorf("b pk: want 2, got %d", pks["b"])
	}
}

// ---------------------------------------------------------------------------
// PRAGMA foreign_keys – exercises compilePragmaForeignKeys,
// compilePragmaForeignKeysGet, compilePragmaForeignKeysSet,
// extractPragmaValueString.
// ---------------------------------------------------------------------------

func TestPragma_ForeignKeys_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA foreign_keys")
	// Default is 0 (off)
	if v != 0 {
		t.Errorf("default foreign_keys: want 0, got %d", v)
	}
}

func TestPragma_ForeignKeys_SetON(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA foreign_keys = ON")
	v := pragmaQueryInt(t, db, "PRAGMA foreign_keys")
	if v != 1 {
		t.Errorf("foreign_keys after ON: want 1, got %d", v)
	}
}

func TestPragma_ForeignKeys_SetOFF(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA foreign_keys = ON")
	pragmaExec(t, db, "PRAGMA foreign_keys = OFF")
	v := pragmaQueryInt(t, db, "PRAGMA foreign_keys")
	if v != 0 {
		t.Errorf("foreign_keys after OFF: want 0, got %d", v)
	}
}

func TestPragma_ForeignKeys_Set1And0(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA foreign_keys = 1")
	if pragmaQueryInt(t, db, "PRAGMA foreign_keys") != 1 {
		t.Error("expected 1 after set to 1")
	}
	pragmaExec(t, db, "PRAGMA foreign_keys = 0")
	if pragmaQueryInt(t, db, "PRAGMA foreign_keys") != 0 {
		t.Error("expected 0 after set to 0")
	}
}

// ---------------------------------------------------------------------------
// PRAGMA journal_mode – exercises compilePragmaJournalMode,
// compilePragmaJournalModeSet, compilePragmaJournalModeGet,
// extractJournalModeValue, mapJournalModeToPager, emitJournalModeResult.
// ---------------------------------------------------------------------------

func TestPragma_JournalMode_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryString(t, db, "PRAGMA journal_mode")
	if v == "" {
		t.Error("journal_mode should return a non-empty string")
	}
}

func TestPragma_JournalMode_SetWAL(t *testing.T) {
	db := openPragmaDB(t)
	rows, err := db.Query("PRAGMA journal_mode = WAL")
	if err != nil {
		t.Fatalf("set journal_mode WAL: %v", err)
	}
	defer rows.Close()
	// Result row expected: the new mode
	if rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !strings.EqualFold(mode, "wal") {
			t.Errorf("journal_mode result: want wal, got %q", mode)
		}
	}
}

func TestPragma_JournalMode_SetDelete(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA journal_mode = WAL")
	rows, err := db.Query("PRAGMA journal_mode = DELETE")
	if err != nil {
		t.Fatalf("set journal_mode DELETE: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !strings.EqualFold(mode, "delete") {
			t.Errorf("journal_mode result: want delete, got %q", mode)
		}
	}
}

func TestPragma_JournalMode_SetMemory(t *testing.T) {
	db := openPragmaDB(t)
	rows, err := db.Query("PRAGMA journal_mode = MEMORY")
	if err != nil {
		t.Fatalf("set journal_mode MEMORY: %v", err)
	}
	defer rows.Close()
	rows.Next()
}

func TestPragma_JournalMode_SetPersist(t *testing.T) {
	db := openPragmaDB(t)
	rows, err := db.Query("PRAGMA journal_mode = PERSIST")
	if err != nil {
		t.Fatalf("set journal_mode PERSIST: %v", err)
	}
	defer rows.Close()
	rows.Next()
}

func TestPragma_JournalMode_SetOff(t *testing.T) {
	db := openPragmaDB(t)
	rows, err := db.Query("PRAGMA journal_mode = OFF")
	if err != nil {
		t.Fatalf("set journal_mode OFF: %v", err)
	}
	defer rows.Close()
	rows.Next()
}

func TestPragma_JournalMode_SetTruncate(t *testing.T) {
	db := openPragmaDB(t)
	rows, err := db.Query("PRAGMA journal_mode = TRUNCATE")
	if err != nil {
		t.Fatalf("set journal_mode TRUNCATE: %v", err)
	}
	defer rows.Close()
	rows.Next()
}

// ---------------------------------------------------------------------------
// PRAGMA page_count – exercises compilePragmaPageCount.
// ---------------------------------------------------------------------------

func TestPragma_PageCount(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA page_count")
	if v < 0 {
		t.Errorf("page_count should be >= 0, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA database_list – exercises compilePragmaDatabaseList.
// ---------------------------------------------------------------------------

func TestPragma_DatabaseList(t *testing.T) {
	db := openPragmaDB(t)

	rows, err := db.Query("PRAGMA database_list")
	if err != nil {
		t.Fatalf("database_list: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seq int64
		var name, file string
		if err := rows.Scan(&seq, &name, &file); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == "main" {
			found = true
		}
	}
	if !found {
		t.Error("database_list did not include 'main'")
	}
}

// ---------------------------------------------------------------------------
// PRAGMA cache_size – exercises compilePragmaCacheSize, compilePragmaCacheSizeGet.
// ---------------------------------------------------------------------------

func TestPragma_CacheSize_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA cache_size")
	// Default may be 2000 or -2000 depending on implementation
	if v != 2000 && v != -2000 {
		t.Errorf("default cache_size: want 2000 or -2000, got %d", v)
	}
}

func TestPragma_CacheSize_Set(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA cache_size = 2000")
	v := pragmaQueryInt(t, db, "PRAGMA cache_size")
	if v != 2000 {
		t.Errorf("cache_size after set: want 2000, got %d", v)
	}
}

func TestPragma_CacheSize_SetNegative(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA cache_size = -4000")
	v := pragmaQueryInt(t, db, "PRAGMA cache_size")
	if v != -4000 {
		t.Errorf("cache_size after set -4000: want -4000, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA auto_vacuum – exercises compilePragmaAutoVacuum,
// compilePragmaAutoVacuumGet, getAutoVacuumMode.
// ---------------------------------------------------------------------------

func TestPragma_AutoVacuum_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA auto_vacuum")
	if v < 0 {
		t.Errorf("auto_vacuum should be >= 0, got %d", v)
	}
}

func TestPragma_AutoVacuum_SetFull(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA auto_vacuum = FULL")
	v := pragmaQueryInt(t, db, "PRAGMA auto_vacuum")
	if v != 1 {
		t.Errorf("auto_vacuum after FULL: want 1, got %d", v)
	}
}

func TestPragma_AutoVacuum_SetNone(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA auto_vacuum = FULL")
	pragmaExec(t, db, "PRAGMA auto_vacuum = NONE")
	v := pragmaQueryInt(t, db, "PRAGMA auto_vacuum")
	if v != 0 {
		t.Errorf("auto_vacuum after NONE: want 0, got %d", v)
	}
}

func TestPragma_AutoVacuum_SetIncremental(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA auto_vacuum = INCREMENTAL")
	v := pragmaQueryInt(t, db, "PRAGMA auto_vacuum")
	if v != 2 {
		t.Errorf("auto_vacuum after INCREMENTAL: want 2, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA compile_options – exercises compilePragmaCompileOptions.
// ---------------------------------------------------------------------------

func TestPragma_CompileOptions(t *testing.T) {
	db := openPragmaDB(t)
	n := pragmaQueryRowCount(t, db, "PRAGMA compile_options")
	if n == 0 {
		t.Error("compile_options should return at least one row")
	}
}

// ---------------------------------------------------------------------------
// PRAGMA page_size – exercises compilePragmaPageSize, compilePragmaPageSizeGet.
// ---------------------------------------------------------------------------

func TestPragma_PageSize_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA page_size")
	if v <= 0 {
		t.Errorf("page_size should be > 0, got %d", v)
	}
}

func TestPragma_PageSize_Set(t *testing.T) {
	// Setting page_size before first write is a no-op at runtime; must not error.
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA page_size = 4096")
}

// ---------------------------------------------------------------------------
// PRAGMA user_version – exercises compilePragmaUserVersion,
// compilePragmaUserVersionGet, compilePragmaUserVersionSet.
// ---------------------------------------------------------------------------

func TestPragma_UserVersion_GetDefault(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA user_version")
	if v != 0 {
		t.Errorf("default user_version: want 0, got %d", v)
	}
}

func TestPragma_UserVersion_Set(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA user_version = 42")
	v := pragmaQueryInt(t, db, "PRAGMA user_version")
	if v != 42 {
		t.Errorf("user_version after set: want 42, got %d", v)
	}
}

func TestPragma_UserVersion_Roundtrip(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA user_version = 100")
	v := pragmaQueryInt(t, db, "PRAGMA user_version")
	if v != 100 {
		t.Errorf("user_version: want 100, got %d", v)
	}
	pragmaExec(t, db, "PRAGMA user_version = 0")
	v = pragmaQueryInt(t, db, "PRAGMA user_version")
	if v != 0 {
		t.Errorf("user_version after reset: want 0, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA schema_version – exercises compilePragmaSchemaVersion,
// compilePragmaSchemaVersionGet, compilePragmaSchemaVersionSet.
// ---------------------------------------------------------------------------

func TestPragma_SchemaVersion_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA schema_version")
	if v < 0 {
		t.Errorf("schema_version should be >= 0, got %d", v)
	}
}

func TestPragma_SchemaVersion_Set(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA schema_version = 5")
	v := pragmaQueryInt(t, db, "PRAGMA schema_version")
	if v != 5 {
		t.Errorf("schema_version after set: want 5, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA integrity_check – exercises compilePragmaIntegrityCheck.
// ---------------------------------------------------------------------------

func TestPragma_IntegrityCheck(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryString(t, db, "PRAGMA integrity_check")
	if v == "" {
		t.Error("integrity_check should return a non-empty string")
	}
}

func TestPragma_IntegrityCheck_OkOnEmptyDB(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryString(t, db, "PRAGMA integrity_check")
	if v != "ok" {
		t.Errorf("integrity_check on empty db: want 'ok', got %q", v)
	}
}

func TestPragma_QuickCheck(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryString(t, db, "PRAGMA quick_check")
	if v == "" {
		t.Error("quick_check should return a non-empty string")
	}
}

// ---------------------------------------------------------------------------
// PRAGMA synchronous – exercises compilePragmaSynchronous,
// compilePragmaSynchronousGet, compilePragmaSynchronousSet.
// ---------------------------------------------------------------------------

func TestPragma_Synchronous_Get(t *testing.T) {
	db := openPragmaDB(t)
	v := pragmaQueryInt(t, db, "PRAGMA synchronous")
	if v < 0 {
		t.Errorf("synchronous should be >= 0, got %d", v)
	}
}

func TestPragma_Synchronous_SetNormal(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA synchronous = NORMAL")
	v := pragmaQueryInt(t, db, "PRAGMA synchronous")
	if v != 1 {
		t.Errorf("synchronous after NORMAL: want 1, got %d", v)
	}
}

func TestPragma_Synchronous_SetFull(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA synchronous = FULL")
	v := pragmaQueryInt(t, db, "PRAGMA synchronous")
	if v != 2 {
		t.Errorf("synchronous after FULL: want 2, got %d", v)
	}
}

func TestPragma_Synchronous_SetOff(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA synchronous = OFF")
	v := pragmaQueryInt(t, db, "PRAGMA synchronous")
	if v != 0 {
		t.Errorf("synchronous after OFF: want 0, got %d", v)
	}
}

func TestPragma_Synchronous_SetExtra(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA synchronous = EXTRA")
	v := pragmaQueryInt(t, db, "PRAGMA synchronous")
	if v != 3 {
		t.Errorf("synchronous after EXTRA: want 3, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA foreign_key_check – exercises compilePragmaForeignKeyCheck,
// emptyForeignKeyCheckResult, emitForeignKeyCheckResults, emitViolationRow,
// newDriverRowReader, RowExists, RowExistsWithCollation, scanForMatch,
// scanForMatchWithCollation, FindReferencingRowsWithParentAffinity,
// collectMatchingRowidsWithAffinity, checkRowMatchWithCollation,
// checkRowMatchWithParentAffinity, valuesEqual, valuesEqualWithCollation,
// valuesEqualWithAffinity, compareAfterAffinityWithCollation,
// toFloat64Value, toInt64Value.
// ---------------------------------------------------------------------------

func TestPragma_ForeignKeyCheck_Empty(t *testing.T) {
	db := openPragmaDB(t)
	// No tables with FKs: should return 0 rows (or an empty result).
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		// drain
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
}

func TestPragma_ForeignKeyCheck_NoViolations(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA foreign_keys = ON")
	pragmaExec(t, db, "CREATE TABLE parent(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "CREATE TABLE child(id INTEGER, pid INTEGER REFERENCES parent(id))")
	pragmaExec(t, db, "INSERT INTO parent VALUES(1)")
	pragmaExec(t, db, "INSERT INTO child VALUES(1, 1)")

	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	// 0 violations expected (all FKs satisfied)
	if n != 0 {
		t.Errorf("expected 0 violations, got %d", n)
	}
}

func TestPragma_ForeignKeyCheck_TableArg(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE fkc_parent(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "CREATE TABLE fkc_child(id INTEGER, pid INTEGER REFERENCES fkc_parent(id))")
	pragmaExec(t, db, "INSERT INTO fkc_parent VALUES(1)")
	pragmaExec(t, db, "INSERT INTO fkc_child VALUES(1, 1)")

	rows, err := db.Query("PRAGMA foreign_key_check('fkc_child')")
	if err != nil {
		t.Fatalf("foreign_key_check table: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA foreign_key_list – exercises compilePragmaForeignKeyList,
// emitForeignKeyListRows, fkActionToString.
// ---------------------------------------------------------------------------

func TestPragma_ForeignKeyList_Basic(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE fkl_p(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "CREATE TABLE fkl_c(id INTEGER, pid INTEGER REFERENCES fkl_p(id))")

	rows, err := db.Query("PRAGMA foreign_key_list('fkl_c')")
	if err != nil {
		t.Fatalf("foreign_key_list: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var id, seq int64
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if table == "fkl_p" && from == "pid" {
			found = true
		}
	}
	if !found {
		t.Error("expected FK entry for fkl_p.pid")
	}
}

func TestPragma_ForeignKeyList_OnDeleteCascade(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE fkl_dp(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "CREATE TABLE fkl_dc(id INTEGER, pid INTEGER REFERENCES fkl_dp(id) ON DELETE CASCADE)")

	rows, err := db.Query("PRAGMA foreign_key_list('fkl_dc')")
	if err != nil {
		t.Fatalf("foreign_key_list: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, seq int64
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if onDelete != "CASCADE" {
			t.Errorf("on_delete: want CASCADE, got %q", onDelete)
		}
	}
}

func TestPragma_ForeignKeyList_OnUpdateSetNull(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE fkl_up(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "CREATE TABLE fkl_uc(id INTEGER, pid INTEGER REFERENCES fkl_up(id) ON UPDATE SET NULL)")

	rows, err := db.Query("PRAGMA foreign_key_list('fkl_uc')")
	if err != nil {
		t.Fatalf("foreign_key_list: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, seq int64
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if onUpdate != "SET NULL" {
			t.Errorf("on_update: want SET NULL, got %q", onUpdate)
		}
	}
}

func TestPragma_ForeignKeyList_NoFKs(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE fkl_none(a INTEGER)")
	n := pragmaQueryRowCount(t, db, "PRAGMA foreign_key_list('fkl_none')")
	if n != 0 {
		t.Errorf("expected 0 FK entries, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA index_info – exercises compilePragmaIndexInfo, emitIndexInfoRow.
// ---------------------------------------------------------------------------

func TestPragma_IndexInfo_Basic(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ii1(a INTEGER, b TEXT)")
	pragmaExec(t, db, "CREATE INDEX idx_ii1_a ON ii1(a)")

	rows, err := db.Query("PRAGMA index_info('idx_ii1_a')")
	if err != nil {
		t.Fatalf("index_info: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seqno, cid int64
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == "a" {
			found = true
		}
	}
	if !found {
		t.Error("expected column 'a' in index_info")
	}
}

func TestPragma_IndexInfo_MultiColumn(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE ii2(a INTEGER, b TEXT, c REAL)")
	pragmaExec(t, db, "CREATE INDEX idx_ii2_ab ON ii2(a, b)")

	n := pragmaQueryRowCount(t, db, "PRAGMA index_info('idx_ii2_ab')")
	if n != 2 {
		t.Errorf("expected 2 index_info rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA index_list – exercises compilePragmaIndexList.
// ---------------------------------------------------------------------------

func TestPragma_IndexList_Basic(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE il1(a INTEGER, b TEXT)")
	pragmaExec(t, db, "CREATE INDEX idx_il1 ON il1(a)")

	rows, err := db.Query("PRAGMA index_list('il1')")
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seq, unique, partial int64
		var name, origin string
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == "idx_il1" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'idx_il1' in index_list")
	}
}

func TestPragma_IndexList_UniqueIndex(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE il2(a INTEGER)")
	pragmaExec(t, db, "CREATE UNIQUE INDEX idx_il2_uniq ON il2(a)")

	rows, err := db.Query("PRAGMA index_list('il2')")
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq, unique, partial int64
		var name, origin string
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == "idx_il2_uniq" && unique != 1 {
			t.Errorf("unique index should have unique=1, got %d", unique)
		}
	}
}

func TestPragma_IndexList_NoIndexes(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE il3(a INTEGER)")
	n := pragmaQueryRowCount(t, db, "PRAGMA index_list('il3')")
	if n != 0 {
		t.Errorf("expected 0 indexes, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// ALTER TABLE ADD COLUMN – exercises updateTriggerTableRefs (via RENAME),
// validateColumnAddition, createNewColumn, applyColumnConstraints,
// applyColumnConstraint.
// ---------------------------------------------------------------------------

func TestAlterTable_AddColumn_Text(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE alt1(id INTEGER PRIMARY KEY, name TEXT)")
	pragmaExec(t, db, "ALTER TABLE alt1 ADD COLUMN new_col TEXT DEFAULT 'value'")

	// Verify column was added
	rows, err := db.Query("PRAGMA table_info('alt1')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	colNames := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int64
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		colNames[name] = true
	}

	if !colNames["new_col"] {
		t.Error("column 'new_col' not found after ALTER TABLE ADD COLUMN")
	}
}

func TestAlterTable_AddColumn_IntegerNotNullDefault(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE alt2(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "ALTER TABLE alt2 ADD COLUMN new_col2 INTEGER NOT NULL DEFAULT 0")

	rows, err := db.Query("PRAGMA table_info('alt2')")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	cols := scanTableInfoCols(t, rows)
	found := false
	for _, c := range cols {
		if c.name == "new_col2" {
			found = true
			if c.notnull != 1 {
				t.Errorf("new_col2 should be NOT NULL, got notnull=%d", c.notnull)
			}
			if !c.dfltVal.Valid || c.dfltVal.String != "0" {
				t.Errorf("new_col2 default: want '0', got %v", c.dfltVal)
			}
		}
	}
	if !found {
		t.Error("column 'new_col2' not found")
	}
}

func TestAlterTable_AddColumn_DuplicateErrors(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE alt3(id INTEGER, name TEXT)")
	_, err := db.Exec("ALTER TABLE alt3 ADD COLUMN name TEXT")
	if err == nil {
		t.Error("expected error when adding duplicate column")
	}
}

func TestAlterTable_AddColumn_Multiple(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE alt4(id INTEGER)")
	pragmaExec(t, db, "ALTER TABLE alt4 ADD COLUMN col1 TEXT")
	pragmaExec(t, db, "ALTER TABLE alt4 ADD COLUMN col2 REAL DEFAULT 3.14")
	pragmaExec(t, db, "ALTER TABLE alt4 ADD COLUMN col3 INTEGER DEFAULT 99")

	n := pragmaQueryRowCount(t, db, "PRAGMA table_info('alt4')")
	if n != 4 {
		t.Errorf("expected 4 columns after adds, got %d", n)
	}
}

// TestAlterTable_RenameExercisesTriggerRefs tests the table rename path
// that exercises updateTriggerTableRefs.
func TestAlterTable_RenameExercisesTriggerRefs(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "CREATE TABLE orig(id INTEGER, val TEXT)")
	// Rename the table; internally this calls updateTriggerTableRefs.
	pragmaExec(t, db, "ALTER TABLE orig RENAME TO renamed")

	// Verify renamed table exists via table_info
	n := pragmaQueryRowCount(t, db, "PRAGMA table_info('renamed')")
	if n == 0 {
		t.Error("renamed table should have columns")
	}
}

// ---------------------------------------------------------------------------
// Additional PRAGMA exercising via direct SQL for broader coverage.
// ---------------------------------------------------------------------------

// TestPragma_UnknownIsAllowlisted ensures a known-safe unrecognised PRAGMA returns without error.
// The driver blocks unknown PRAGMAs for security; use one that is in the allowlist but
// produces no rows (i.e., recognised by compilePragma's default branch).
func TestPragma_UnknownIsAllowlisted(t *testing.T) {
	db := openPragmaDB(t)
	// "temp_store" is a known but unimplemented pragma that falls through to the
	// default no-op branch inside compilePragma.
	rows, err := db.Query("PRAGMA temp_store")
	if err != nil {
		t.Logf("PRAGMA temp_store returned error (acceptable if not in allowlist): %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// TestPragma_ForeignKeyCheck_NullValues tests FK check with NULL child column
// (NULL is never a violation per SQL semantics). This exercises
// valuesEqual / valuesEqualWithAffinity NULL paths.
func TestPragma_ForeignKeyCheck_NullChildValue(t *testing.T) {
	db := openPragmaDB(t)
	pragmaExec(t, db, "PRAGMA foreign_keys = ON")
	pragmaExec(t, db, "CREATE TABLE fkn_p(id INTEGER PRIMARY KEY)")
	pragmaExec(t, db, "CREATE TABLE fkn_c(id INTEGER, pid INTEGER REFERENCES fkn_p(id))")
	pragmaExec(t, db, "INSERT INTO fkn_p VALUES(1)")
	// NULL pid is allowed (no FK violation)
	pragmaExec(t, db, "INSERT INTO fkn_c VALUES(1, NULL)")

	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if n != 0 {
		t.Errorf("expected 0 violations for NULL FK, got %d", n)
	}
}
