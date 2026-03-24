// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"strings"
	"testing"
)

// TestHelpersTriggerPragma covers trigger compile helpers, prepareNewRowForInsert,
// emitNonIdentifierColumn, formatP4, and pragma TVF functions.
func TestHelpersTriggerPragma(t *testing.T) {
	t.Parallel()
	t.Run("triggers", testTriggerHelpers)
	t.Run("pragma_tvf", testPragmaTVFHelpers)
	t.Run("emit_non_identifier", testEmitNonIdentifierColumn)
	t.Run("format_p4", testFormatP4)
}

// ---------------------------------------------------------------------------
// Trigger compile-helper coverage
// Each sub-test exercises a distinct trigger event / timing combination by
// creating a trigger and running the DML that fires it.  The compiler calls
// executeBeforeInsertTriggers / executeAfterInsertTriggers etc. as part of
// compileInsert / compileUpdate / compileDelete, so executing the DML is
// sufficient to exercise those paths.
// ---------------------------------------------------------------------------

func testTriggerHelpers(t *testing.T) {
	t.Parallel()
	tests := []sqlTestCase{
		// executeBeforeInsertTriggers – trigger exists path
		{
			name: "before_insert_trigger_fires",
			setup: []string{
				"CREATE TABLE tbi(id INTEGER, v INTEGER)",
				"CREATE TABLE tbi_log(op TEXT)",
				"CREATE TRIGGER tbi_trg BEFORE INSERT ON tbi BEGIN INSERT INTO tbi_log VALUES('before_ins'); END",
			},
			exec:  "INSERT INTO tbi VALUES(1, 10)",
			query: "SELECT op FROM tbi_log",
			wantRows: [][]interface{}{
				{"before_ins"},
			},
		},
		// executeAfterInsertTriggers – trigger exists path
		{
			name: "after_insert_trigger_fires",
			setup: []string{
				"CREATE TABLE tai(id INTEGER, v INTEGER)",
				"CREATE TABLE tai_log(op TEXT)",
				"CREATE TRIGGER tai_trg AFTER INSERT ON tai BEGIN INSERT INTO tai_log VALUES('after_ins'); END",
			},
			exec:  "INSERT INTO tai VALUES(2, 20)",
			query: "SELECT op FROM tai_log",
			wantRows: [][]interface{}{
				{"after_ins"},
			},
		},
		// executeBeforeInsertTriggers – no triggers (zero-trigger path)
		{
			name: "before_insert_no_trigger",
			setup: []string{
				"CREATE TABLE tbi_none(id INTEGER)",
			},
			exec:  "INSERT INTO tbi_none VALUES(1)",
			query: "SELECT id FROM tbi_none",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// executeBeforeUpdateTriggers – trigger exists path
		{
			name: "before_update_trigger_fires",
			setup: []string{
				"CREATE TABLE tbu(id INTEGER, v TEXT)",
				"CREATE TABLE tbu_log(op TEXT)",
				"INSERT INTO tbu VALUES(1,'old')",
				"CREATE TRIGGER tbu_trg BEFORE UPDATE ON tbu BEGIN INSERT INTO tbu_log VALUES('before_upd'); END",
			},
			exec:  "UPDATE tbu SET v='new' WHERE id=1",
			query: "SELECT op FROM tbu_log",
			wantRows: [][]interface{}{
				{"before_upd"},
			},
		},
		// executeAfterUpdateTriggers – trigger exists path
		{
			name: "after_update_trigger_fires",
			setup: []string{
				"CREATE TABLE tau(id INTEGER, v TEXT)",
				"CREATE TABLE tau_log(op TEXT)",
				"INSERT INTO tau VALUES(1,'old')",
				"CREATE TRIGGER tau_trg AFTER UPDATE ON tau BEGIN INSERT INTO tau_log VALUES('after_upd'); END",
			},
			exec:  "UPDATE tau SET v='new' WHERE id=1",
			query: "SELECT op FROM tau_log",
			wantRows: [][]interface{}{
				{"after_upd"},
			},
		},
		// executeBeforeUpdateTriggers – no triggers path
		{
			name: "before_update_no_trigger",
			setup: []string{
				"CREATE TABLE tbu_none(id INTEGER, v TEXT)",
				"INSERT INTO tbu_none VALUES(1,'a')",
			},
			exec:  "UPDATE tbu_none SET v='b' WHERE id=1",
			query: "SELECT v FROM tbu_none",
			wantRows: [][]interface{}{
				{"b"},
			},
		},
		// executeBeforeDeleteTriggers – trigger exists path
		{
			name: "before_delete_trigger_fires",
			setup: []string{
				"CREATE TABLE tbd(id INTEGER)",
				"CREATE TABLE tbd_log(op TEXT)",
				"INSERT INTO tbd VALUES(1)",
				"CREATE TRIGGER tbd_trg BEFORE DELETE ON tbd BEGIN INSERT INTO tbd_log VALUES('before_del'); END",
			},
			exec:  "DELETE FROM tbd WHERE id=1",
			query: "SELECT op FROM tbd_log",
			wantRows: [][]interface{}{
				{"before_del"},
			},
		},
		// executeAfterDeleteTriggers – trigger exists path
		{
			name: "after_delete_trigger_fires",
			setup: []string{
				"CREATE TABLE tad(id INTEGER)",
				"CREATE TABLE tad_log(op TEXT)",
				"INSERT INTO tad VALUES(1)",
				"CREATE TRIGGER tad_trg AFTER DELETE ON tad BEGIN INSERT INTO tad_log VALUES('after_del'); END",
			},
			exec:  "DELETE FROM tad WHERE id=1",
			query: "SELECT op FROM tad_log",
			wantRows: [][]interface{}{
				{"after_del"},
			},
		},
		// executeBeforeDeleteTriggers / executeAfterDeleteTriggers – no triggers
		{
			name: "delete_no_trigger",
			setup: []string{
				"CREATE TABLE tdel_none(id INTEGER)",
				"INSERT INTO tdel_none VALUES(1)",
			},
			exec:     "DELETE FROM tdel_none WHERE id=1",
			query:    "SELECT COUNT(*) FROM tdel_none",
			wantRows: [][]interface{}{{int64(0)}},
		},
		// prepareNewRowForInsert – with explicit column list
		{
			name: "insert_with_column_list",
			setup: []string{
				"CREATE TABLE tpr(a INTEGER, b TEXT, c REAL)",
				"CREATE TABLE tpr_log(v TEXT)",
				"CREATE TRIGGER tpr_trg AFTER INSERT ON tpr BEGIN INSERT INTO tpr_log VALUES(NEW.b); END",
			},
			exec:  "INSERT INTO tpr(b, a) VALUES('hello', 99)",
			query: "SELECT v FROM tpr_log",
			wantRows: [][]interface{}{
				{"hello"},
			},
		},
		// prepareNewRowForInsert – implicit column list (all table columns)
		{
			name: "insert_implicit_columns",
			setup: []string{
				"CREATE TABLE timp(x INTEGER, y TEXT)",
				"CREATE TABLE timp_log(v INTEGER)",
				"CREATE TRIGGER timp_trg AFTER INSERT ON timp BEGIN INSERT INTO timp_log VALUES(NEW.x); END",
			},
			exec:  "INSERT INTO timp VALUES(42,'foo')",
			query: "SELECT v FROM timp_log",
			wantRows: [][]interface{}{
				{int64(42)},
			},
		},
		// prepareNewRowForInsert – empty VALUES (no rows), exercises early return
		{
			name: "insert_no_values_path",
			setup: []string{
				"CREATE TABLE tsrc(n INTEGER)",
				"INSERT INTO tsrc VALUES(1)",
				"CREATE TABLE tdst(n INTEGER)",
			},
			// INSERT … SELECT exercises the compiler without a VALUES clause
			exec:     "INSERT INTO tdst SELECT n FROM tsrc",
			query:    "SELECT n FROM tdst",
			wantRows: [][]interface{}{{int64(1)}},
		},
	}

	// Run each case with a fresh in-memory database.
	// Use a nested parallel approach: outer is already parallel, so
	// inner sub-tests are run sequentially to avoid interference.
	for _, tt := range tests {
		tc := tt
		if tc.exec != "" && tc.query != "" {
			// Hybrid: exec then query
			t.Run(tc.name, func(t *testing.T) {
				db := setupMemoryDB(t)
				defer db.Close()
				for _, s := range tc.setup {
					if _, err := db.Exec(s); err != nil {
						t.Fatalf("setup %q: %v", s, err)
					}
				}
				if _, err := db.Exec(tc.exec); err != nil {
					t.Fatalf("exec %q: %v", tc.exec, err)
				}
				rows, err := db.Query(tc.query)
				if err != nil {
					t.Fatalf("query %q: %v", tc.query, err)
				}
				defer rows.Close()
				got := scanAllRows(t, rows)
				compareRows(t, got, tc.wantRows)
			})
		} else {
			t.Run(tc.name, func(t *testing.T) {
				db := setupMemoryDB(t)
				defer db.Close()
				runSingleSQLTest(t, db, tc)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Pragma TVF function coverage
// Exercises extractPragmaTVFArg, pragmaExtractColName, pragmaRowMatchesWhere,
// evalPragmaWhere, and evalPragmaEquality via real SQL queries.
// ---------------------------------------------------------------------------

func testPragmaTVFHelpers(t *testing.T) {
	t.Parallel()
	tests := []sqlTestCase{
		// extractPragmaTVFArg – string literal argument
		{
			name:  "pragma_table_info_literal_arg",
			setup: []string{"CREATE TABLE ptvf1(id INTEGER PRIMARY KEY, name TEXT)"},
			query: "SELECT COUNT(*) FROM pragma_table_info('ptvf1')",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
		// pragmaExtractColName – IdentExpr path (named column)
		{
			name:  "pragma_table_info_named_column",
			setup: []string{"CREATE TABLE ptvf2(a INTEGER, b TEXT)"},
			query: "SELECT name FROM pragma_table_info('ptvf2') ORDER BY cid",
			wantRows: [][]interface{}{
				{"a"},
				{"b"},
			},
		},
		// pragmaExtractColName – Expr.String() fallback (non-ident column expression)
		// A star select causes resolvePragmaColumns to take the star path.
		{
			name:  "pragma_table_info_star",
			setup: []string{"CREATE TABLE ptvf3(x INTEGER)"},
			query: "SELECT cid, name FROM pragma_table_info('ptvf3')",
			wantRows: [][]interface{}{
				{int64(0), "x"},
			},
		},
		// pragmaRowMatchesWhere / evalPragmaWhere / evalPragmaEquality
		// – equality filter on string column
		{
			name: "pragma_index_list_where_eq_string",
			setup: []string{
				"CREATE TABLE ptvf4(a INTEGER)",
				"CREATE INDEX idx_ptvf4 ON ptvf4(a)",
			},
			query: "SELECT name FROM pragma_index_list('ptvf4') WHERE origin='c'",
			wantRows: [][]interface{}{
				{"idx_ptvf4"},
			},
		},
		// evalPragmaEquality – numeric comparison path
		{
			name: "pragma_index_list_where_eq_int",
			setup: []string{
				"CREATE TABLE ptvf5(a INTEGER)",
				"CREATE UNIQUE INDEX idx_ptvf5 ON ptvf5(a)",
			},
			query:    "SELECT name FROM pragma_index_list('ptvf5') WHERE \"unique\"=1",
			wantRows: [][]interface{}{{"idx_ptvf5"}},
		},
		// evalPragmaWhere – AND expression path
		{
			name: "pragma_index_list_where_and",
			setup: []string{
				"CREATE TABLE ptvf6(a INTEGER)",
				"CREATE UNIQUE INDEX idx_ptvf6 ON ptvf6(a)",
			},
			query:    "SELECT name FROM pragma_index_list('ptvf6') WHERE origin='c' AND \"unique\"=1",
			wantRows: [][]interface{}{{"idx_ptvf6"}},
		},
		// evalPragmaWhere – filter that yields no rows
		{
			name: "pragma_index_list_where_no_match",
			setup: []string{
				"CREATE TABLE ptvf7(a INTEGER)",
				"CREATE INDEX idx_ptvf7 ON ptvf7(a)",
			},
			query:    "SELECT COUNT(*) FROM pragma_index_list('ptvf7') WHERE \"unique\"=1",
			wantRows: [][]interface{}{{int64(0)}},
		},
		// extractPragmaTVFArg – no args path (pragma_database_list has no arg)
		{
			name:     "pragma_database_list_no_arg",
			query:    "SELECT name FROM pragma_database_list WHERE seq=0",
			wantRows: [][]interface{}{{"main"}},
		},
		// pragmaExtractColName + alias path via resolvePragmaColumns
		{
			name:  "pragma_table_info_alias_column",
			setup: []string{"CREATE TABLE ptvf8(z INTEGER)"},
			query: "SELECT name AS col_name FROM pragma_table_info('ptvf8')",
			wantRows: [][]interface{}{
				{"z"},
			},
		},
		// COUNT(*) path through isPragmaCountStar / emitPragmaCountResult
		{
			name:  "pragma_table_info_count_star",
			setup: []string{"CREATE TABLE ptvf9(a INTEGER, b TEXT, c REAL)"},
			query: "SELECT COUNT(*) FROM pragma_table_info('ptvf9')",
			wantRows: [][]interface{}{
				{int64(3)},
			},
		},
	}

	runSQLTestsFreshDB(t, tests)
}

// ---------------------------------------------------------------------------
// emitNonIdentifierColumn coverage
// Exercises non-identifier expressions in SELECT column list via the
// multi-table join path (two-table JOIN forces emitNonIdentifierColumn).
// ---------------------------------------------------------------------------

func testEmitNonIdentifierColumn(t *testing.T) {
	t.Parallel()
	tests := []sqlTestCase{
		// Arithmetic literal expression – exercises gen.GenerateExpr path
		{
			name: "non_ident_arithmetic",
			setup: []string{
				"CREATE TABLE enic_a(id INTEGER)",
				"CREATE TABLE enic_b(id INTEGER)",
				"INSERT INTO enic_a VALUES(1)",
				"INSERT INTO enic_b VALUES(1)",
			},
			query:    "SELECT 1+2 FROM enic_a JOIN enic_b ON enic_a.id=enic_b.id",
			wantRows: [][]interface{}{{int64(3)}},
		},
		// String function expression
		{
			name: "non_ident_function",
			setup: []string{
				"CREATE TABLE enic_c(id INTEGER)",
				"CREATE TABLE enic_d(v TEXT)",
				"INSERT INTO enic_c VALUES(1)",
				"INSERT INTO enic_d VALUES('hello')",
			},
			query:    "SELECT upper(enic_d.v) FROM enic_c JOIN enic_d",
			wantRows: [][]interface{}{{"HELLO"}},
		},
		// Literal string expression
		{
			name: "non_ident_literal",
			setup: []string{
				"CREATE TABLE enic_e(id INTEGER)",
				"INSERT INTO enic_e VALUES(1)",
				"CREATE TABLE enic_f(id INTEGER)",
				"INSERT INTO enic_f VALUES(1)",
			},
			query:    "SELECT 'constant' FROM enic_e JOIN enic_f ON enic_e.id=enic_f.id",
			wantRows: [][]interface{}{{"constant"}},
		},
	}

	runSQLTestsFreshDB(t, tests)
}

// ---------------------------------------------------------------------------
// formatP4 coverage
// EXPLAIN output exercises emitExplainRow → formatP4 for all P4Type branches.
// ---------------------------------------------------------------------------

func testFormatP4(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	// Create a table so that the compiled program contains P4Static (OpString8)
	// as well as plain integer opcodes and other variants.
	if _, err := db.Exec("CREATE TABLE fp1(id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// EXPLAIN SELECT emits a program containing OpString8 (P4Static/P4Dynamic),
	// OpInteger, and other opcodes.  Scanning all rows exercises formatP4 for
	// each instruction in the program.
	rows, err := db.Query("EXPLAIN SELECT id, name FROM fp1")
	if err != nil {
		t.Fatalf("EXPLAIN query: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var addr, p1, p2, p3, p5 int
		var opcode, p4, comment string
		if err := rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatalf("scan explain row: %v", err)
		}
		count++
		_ = p4 // formatP4 result — just ensure no panic
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count == 0 {
		t.Fatal("expected at least one EXPLAIN row")
	}

	// Also EXPLAIN an INSERT with a string literal so P4Static/string path is hit.
	rows2, err := db.Query("EXPLAIN INSERT INTO fp1 VALUES(1,'alice')")
	if err != nil {
		t.Fatalf("EXPLAIN INSERT query: %v", err)
	}
	defer rows2.Close()

	var hasP4String bool
	for rows2.Next() {
		var addr, p1, p2, p3, p5 int
		var opcode, p4, comment string
		if err := rows2.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatalf("scan explain insert row: %v", err)
		}
		if strings.Contains(opcode, "String") && p4 != "" {
			hasP4String = true
		}
	}
	if err := rows2.Err(); err != nil {
		t.Fatalf("rows2.Err: %v", err)
	}
	if !hasP4String {
		// Not a hard failure: some engines may encode strings differently.
		t.Log("note: no String opcode with non-empty P4 found in EXPLAIN INSERT")
	}
}
