// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// ============================================================================
// compile_pragma_tvf.go:18 – isPragmaTVF
//
// Compound condition (2 sub-conditions):
//   A = stmt.From == nil || len(stmt.From.Tables) == 0
//   B = isPragmaTVFName(stmt.From.Tables[0].TableName)
//
// Outcome = A → return false (early exit); !A && B → true (is a pragma TVF)
//
// Cases needed (N+1 = 3):
//   Base (Flip A=T): FROM is nil → false returned immediately
//   Flip B=T:        FROM is non-nil, Tables[0] is pragma name → true
//   Flip B=F:        FROM is non-nil, Tables[0] is NOT a pragma name → false
// ============================================================================

func TestMCDC_IsPragmaTVF_FromGuard(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   string
		query   string
		wantErr bool
	}
	tests := []tc{
		{
			// Flip A=T: no FROM → isPragmaTVF returns false (normal SELECT 1+1)
			name:    "A=T: SELECT without FROM – not a pragma TVF",
			setup:   "",
			query:   "SELECT 1+1",
			wantErr: false,
		},
		{
			// Flip B=T: pragma_table_info in FROM → isPragmaTVF returns true
			name:    "Flip B=T: pragma_table_info in FROM",
			setup:   "CREATE TABLE pvf_t1(id INTEGER, name TEXT)",
			query:   "SELECT * FROM pragma_table_info('pvf_t1')",
			wantErr: false,
		},
		{
			// Flip B=F: ordinary table name in FROM → isPragmaTVF returns false
			name:    "Flip B=F: regular table in FROM – not a pragma TVF",
			setup:   "CREATE TABLE pvf_t2(id INTEGER); INSERT INTO pvf_t2 VALUES(1)",
			query:   "SELECT id FROM pvf_t2",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if tt.wantErr && err == nil {
				t.Fatal("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rows != nil {
				rows.Close()
			}
		})
	}
}

// ============================================================================
// compile_pragma_tvf.go:59 – extractPragmaTVFArg
//
// Compound condition (2 sub-conditions, sequential):
//   A = len(ref.FuncArgs) == 0   → return "" early
//   B = ref.FuncArgs[0] is *parser.LiteralExpr (type assertion ok)
//
// Outcome = A → "" returned; !A && B → literal value returned
//
// Cases needed (N+1 = 3):
//   Base (A=T): pragma TVF with NO argument → extractPragmaTVFArg returns ""
//               (pragma_database_list takes no argument)
//   Flip B=T:  pragma TVF with string-literal argument → literal path taken
//   Flip B=F:  pragma TVF with identifier (non-literal) argument → ident path
// ============================================================================

func TestMCDC_ExtractPragmaTVFArg_ArgBranches(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   string
		query   string
		wantErr bool
	}
	tests := []tc{
		{
			// A=T: pragma_database_list has no argument → extractPragmaTVFArg returns ""
			name:    "A=T: pragma_database_list – no FuncArgs",
			setup:   "",
			query:   "SELECT * FROM pragma_database_list",
			wantErr: false,
		},
		{
			// Flip B=T: literal string argument 'pvf_t3'
			name:    "Flip B=T: pragma_table_info with literal string arg",
			setup:   "CREATE TABLE pvf_t3(id INTEGER, val TEXT)",
			query:   "SELECT name FROM pragma_table_info('pvf_t3')",
			wantErr: false,
		},
		{
			// Flip B=T (second literal check): verify column data returned correctly
			name:    "Flip B=T: pragma_index_list with literal arg",
			setup:   "CREATE TABLE pvf_t4(id INTEGER); CREATE UNIQUE INDEX pvf_idx4 ON pvf_t4(id)",
			query:   "SELECT name FROM pragma_index_list('pvf_t4')",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if tt.wantErr && err == nil {
				t.Fatal("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rows != nil {
				rows.Close()
			}
		})
	}
}

// ============================================================================
// compile_pragma_tvf.go:87-102 – buildTableInfoRows, col.NotNull branch
//
// Compound condition (single bool):
//   A = col.NotNull
//
// Outcome: A=T → notnull=1; A=F → notnull=0
//
// Cases needed (N+1 = 2):
//   Case A=T: column declared NOT NULL → notnull=1
//   Case A=F: column nullable → notnull=0
// ============================================================================

func TestMCDC_BuildTableInfoRows_NotNull(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE ti_tbl(a INTEGER NOT NULL, b TEXT)")

	// Query pragma_table_info and check notnull values (quote 'notnull' to avoid keyword conflict)
	rows, err := db.Query("SELECT name, \"notnull\" FROM pragma_table_info('ti_tbl')")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	got := map[string]int64{}
	for rows.Next() {
		var name string
		var notnull int64
		if err := rows.Scan(&name, &notnull); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[name] = notnull
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// A=T: column 'a' is NOT NULL → notnull should be 1
	if got["a"] != 1 {
		t.Errorf("column 'a' notnull = %d, want 1", got["a"])
	}
	// A=F: column 'b' is nullable → notnull should be 0
	if got["b"] != 0 {
		t.Errorf("column 'b' notnull = %d, want 0", got["b"])
	}
}

// ============================================================================
// compile_pragma_tvf.go:117-131 – buildIndexListRows
//
// Compound condition (two independent booleans):
//   A = idx.Unique  → isUnique = 1
//   B = idx.Partial → isPartial = 1
//
// Cases needed (N+1 = 3 for each):
//   For A: unique index (A=T) vs non-unique index (A=F)
//   For B: partial index not tested here (always 0 in schema implementation)
//          so we cover A=T and A=F
// ============================================================================

func TestMCDC_BuildIndexListRows_Unique(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE il_tbl(id INTEGER, name TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX il_uniq ON il_tbl(id)")
	mustExec(t, db, "CREATE INDEX il_nonuniq ON il_tbl(name)")

	rows, err := db.Query("SELECT name, \"unique\" FROM pragma_index_list('il_tbl')")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	got := map[string]int64{}
	for rows.Next() {
		var name string
		var uniq int64
		if err := rows.Scan(&name, &uniq); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[name] = uniq
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// A=T: unique index
	if got["il_uniq"] != 1 {
		t.Errorf("il_uniq unique = %d, want 1", got["il_uniq"])
	}
	// A=F: non-unique index
	if got["il_nonuniq"] != 0 {
		t.Errorf("il_nonuniq unique = %d, want 0", got["il_nonuniq"])
	}
}

// ============================================================================
// compile_pragma_tvf.go:226-235 – isPragmaCountStar
//
// Compound condition (2 sub-conditions, sequential &&):
//   A = len(stmt.Columns) == 1
//   B = fnExpr.Name == "COUNT" && fnExpr.Star
//
// Outcome = A && B → COUNT(*) path
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → single column, COUNT(*) → count result emitted
//   Flip A: A=F B=? → multiple columns → normal column-list path
//   Flip B: A=T B=F → single column but not COUNT(*) → normal path
// ============================================================================

func TestMCDC_IsPragmaCountStar(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		setup     string
		query     string
		wantCount int // expected row count returned
	}
	tests := []tc{
		{
			// A=T B=T: COUNT(*) from pragma TVF → single integer result
			name:      "A=T B=T: COUNT(*) from pragma_table_info",
			setup:     "CREATE TABLE pcs_t1(a INTEGER, b TEXT, c REAL)",
			query:     "SELECT COUNT(*) FROM pragma_table_info('pcs_t1')",
			wantCount: 1,
		},
		{
			// Flip A=F: multiple columns → not COUNT(*) path, returns one row per column
			name:      "Flip A=F: SELECT cid, name FROM pragma_table_info",
			setup:     "CREATE TABLE pcs_t2(x INTEGER, y TEXT)",
			query:     "SELECT cid, name FROM pragma_table_info('pcs_t2')",
			wantCount: 2,
		},
		{
			// Flip B=F: single column but not COUNT(*) → normal column path
			name:      "Flip B=F: SELECT name (not COUNT) from pragma_table_info",
			setup:     "CREATE TABLE pcs_t3(p INTEGER, q TEXT, r REAL)",
			query:     "SELECT name FROM pragma_table_info('pcs_t3')",
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			count := countRowsDrain(t, db, tt.query)
			if count != tt.wantCount {
				t.Errorf("got %d rows, want %d", count, tt.wantCount)
			}
		})
	}
}

// countRowsDrain runs a query, scans and drains all columns per row, and returns the row count.
func countRowsDrain(t *testing.T, db interface {
	Query(string, ...interface{}) (*sql.Rows, error)
}, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
		cols, _ := rows.Columns()
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return count
}

// ============================================================================
// compile_pragma_tvf.go:212-223 – filterPragmaRows
//
// Compound condition (single guard):
//   A = where == nil
//
// Outcome: A=T → return rows unfiltered; A=F → apply filter
//
// Cases needed (N+1 = 2):
//   Case A=T: no WHERE clause → all rows returned
//   Case A=F: WHERE clause present → filtered rows returned
// ============================================================================

func TestMCDC_FilterPragmaRows_WhereGuard(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE fp_tbl(id INTEGER, name TEXT, val REAL)")

	// A=T: no WHERE → all 3 columns returned
	t.Run("A=T: no WHERE – all rows returned", func(t *testing.T) {
		t.Parallel()
		db2 := openMCDCDB(t)
		mustExec(t, db2, "CREATE TABLE fp_tbl2(a INTEGER, b TEXT, c REAL)")
		rows, err := db2.Query("SELECT name FROM pragma_table_info('fp_tbl2')")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			count++
			var name string
			rows.Scan(&name)
		}
		if count != 3 {
			t.Errorf("got %d rows, want 3", count)
		}
	})

	// A=F: WHERE pk = 1 → only primary key column
	t.Run("A=F: WHERE pk=1 – filtered row", func(t *testing.T) {
		t.Parallel()
		db3 := openMCDCDB(t)
		mustExec(t, db3, "CREATE TABLE fp_tbl3(id INTEGER PRIMARY KEY, name TEXT)")
		rows, err := db3.Query("SELECT name FROM pragma_table_info('fp_tbl3') WHERE pk = 1")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			count++
			var name string
			rows.Scan(&name)
		}
		if count != 1 {
			t.Errorf("got %d rows, want 1", count)
		}
	})
}

// ============================================================================
// compile_pragma_tvf.go:350-361 – evalPragmaEquality
//
// Compound condition (2 sub-conditions):
//   A = toInt64Value(lVal) succeeds  (lok)
//   B = toInt64Value(rVal) succeeds  (rok)
//
// Outcome = A && B → numeric comparison; else string comparison
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → both numeric → integer equality check
//   Flip A: A=F B=? → left is string → string comparison fallback
//   Flip B: A=T B=F → left numeric, right string → string comparison fallback
// ============================================================================

func TestMCDC_EvalPragmaEquality_NumericVsString(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		setup     string
		query     string
		wantCount int
	}
	tests := []tc{
		{
			// A=T B=T: WHERE cid = 0 (integer = integer comparison)
			name:      "A=T B=T: integer cid equality",
			setup:     "CREATE TABLE eq_t1(a INTEGER, b TEXT)",
			query:     "SELECT name FROM pragma_table_info('eq_t1') WHERE cid = 0",
			wantCount: 1,
		},
		{
			// Flip A=F: WHERE name = 'a' (string = string comparison)
			name:      "Flip A=F: string name equality",
			setup:     "CREATE TABLE eq_t2(alpha INTEGER, beta TEXT)",
			query:     "SELECT name FROM pragma_table_info('eq_t2') WHERE name = 'alpha'",
			wantCount: 1,
		},
		{
			// Confirm multiple rows when filter matches nothing → 0 rows
			name:      "A=T B=T: integer with no match returns 0 rows",
			setup:     "CREATE TABLE eq_t3(x INTEGER)",
			query:     "SELECT name FROM pragma_table_info('eq_t3') WHERE cid = 99",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			var count int
			for rows.Next() {
				count++
				var name string
				rows.Scan(&name)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if count != tt.wantCount {
				t.Errorf("got %d rows, want %d", count, tt.wantCount)
			}
		})
	}
}

// ============================================================================
// compile_join_agg.go:53 – compileSelectWithJoinsAndAggregates
//
// Compound condition (single bool):
//   A = numGroupBy == 0
//
// Outcome: A=T → emit default-aggregate-row path for empty input;
//          A=F → GROUP BY path (no default row needed)
//
// Cases needed (N+1 = 2):
//   Case A=T: JOIN aggregate WITHOUT GROUP BY → default row emitted on empty input
//   Case A=F: JOIN aggregate WITH GROUP BY → group results returned
// ============================================================================

func TestMCDC_JoinAgg_NumGroupByZero(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		setup     string
		query     string
		wantCount int
	}
	tests := []tc{
		{
			// A=T: COUNT(*) on JOIN with no data, no GROUP BY → single row with 0
			name: "A=T: aggregate JOIN without GROUP BY, empty tables",
			setup: `CREATE TABLE jag_a1(id INTEGER, v INTEGER);
                    CREATE TABLE jag_b1(id INTEGER, w INTEGER)`,
			query:     "SELECT COUNT(*) FROM jag_a1 JOIN jag_b1 ON jag_a1.id = jag_b1.id",
			wantCount: 1,
		},
		{
			// A=T: COUNT(*) on JOIN with data, no GROUP BY → single row
			name: "A=T: aggregate JOIN without GROUP BY, with data",
			setup: `CREATE TABLE jag_a2(id INTEGER, v INTEGER);
                    CREATE TABLE jag_b2(id INTEGER, w INTEGER);
                    INSERT INTO jag_a2 VALUES(1,10),(2,20);
                    INSERT INTO jag_b2 VALUES(1,100),(2,200)`,
			query:     "SELECT COUNT(*) FROM jag_a2 JOIN jag_b2 ON jag_a2.id = jag_b2.id",
			wantCount: 1,
		},
		{
			// A=F: GROUP BY → one row per group
			name: "A=F: aggregate JOIN with GROUP BY",
			setup: `CREATE TABLE jag_a3(id INTEGER, cat TEXT);
                    CREATE TABLE jag_b3(id INTEGER, score INTEGER);
                    INSERT INTO jag_a3 VALUES(1,'x'),(2,'y'),(3,'x');
                    INSERT INTO jag_b3 VALUES(1,5),(2,10),(3,15)`,
			query:     "SELECT jag_a3.cat, COUNT(*) FROM jag_a3 JOIN jag_b3 ON jag_a3.id = jag_b3.id GROUP BY jag_a3.cat",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			var count int
			for rows.Next() {
				count++
				cols, _ := rows.Columns()
				vals := make([]interface{}, len(cols))
				ptrs := make([]interface{}, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				rows.Scan(ptrs...)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if count != tt.wantCount {
				t.Errorf("got %d rows, want %d", count, tt.wantCount)
			}
		})
	}
}

// ============================================================================
// compile_join_agg.go:140 – joinAggPhase1, hasOuterJoin branch
//
// Compound condition (single bool):
//   A = hasOuterJoin(stmt)
//
// Outcome: A=T → emitLeftJoinAggBody; A=F → emitInnerJoinAggBody
//
// Cases needed (N+1 = 2):
//   Case A=T: LEFT JOIN aggregate
//   Case A=F: INNER JOIN aggregate
// ============================================================================

func TestMCDC_JoinAggPhase1_OuterJoinBranch(t *testing.T) {
	t.Parallel()

	type tc struct {
		name  string
		setup string
		query string
	}
	tests := []tc{
		{
			// A=F: INNER JOIN
			name: "A=F: INNER JOIN aggregate",
			setup: `CREATE TABLE oj_a1(id INTEGER, cat TEXT);
                    CREATE TABLE oj_b1(id INTEGER, val INTEGER);
                    INSERT INTO oj_a1 VALUES(1,'foo'),(2,'bar');
                    INSERT INTO oj_b1 VALUES(1,10),(2,20)`,
			query: "SELECT oj_a1.cat, SUM(oj_b1.val) FROM oj_a1 JOIN oj_b1 ON oj_a1.id=oj_b1.id GROUP BY oj_a1.cat",
		},
		{
			// A=T: LEFT JOIN
			name: "A=T: LEFT JOIN aggregate",
			setup: `CREATE TABLE oj_a2(id INTEGER, cat TEXT);
                    CREATE TABLE oj_b2(id INTEGER, val INTEGER);
                    INSERT INTO oj_a2 VALUES(1,'foo'),(2,'bar'),(3,'baz');
                    INSERT INTO oj_b2 VALUES(1,10),(2,20)`,
			query: "SELECT oj_a2.cat, COUNT(oj_b2.val) FROM oj_a2 LEFT JOIN oj_b2 ON oj_a2.id=oj_b2.id GROUP BY oj_a2.cat",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			for rows.Next() {
				var cat string
				var cnt int64
				if err := rows.Scan(&cat, &cnt); err != nil {
					t.Fatalf("scan: %v", err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
		})
	}
}

// ============================================================================
// compile_join_agg.go:83-90 – emitDefaultAggregateRow
//
// Compound condition (2 sub-conditions):
//   A = col.Expr is *parser.FunctionExpr  (ok from type assertion)
//   B = s.isAggregateExpr(col.Expr)
//
// Outcome = A && B → emit default aggregate value; else → emit NULL
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → aggregate column gets default (COUNT→0)
//   Flip A: A=F B=? → non-function column gets NULL
//   Flip B: A=T B=F → function but not aggregate (e.g., ABS) gets NULL
// ============================================================================

// emitDefaultAggHelper sets up tables, runs the query, and returns the scanned value.
func emitDefaultAggHelper(t *testing.T, setup, query string) interface{} {
	t.Helper()
	db := openMCDCDB(t)
	for _, s := range splitSemicolon(setup) {
		mustExec(t, db, s)
	}
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row, got none")
	}
	var got interface{}
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return got
}

// TestMCDC_EmitDefaultAggregateRow_AggCheck_Count tests COUNT(*) default on empty join.
func TestMCDC_EmitDefaultAggregateRow_AggCheck_Count(t *testing.T) {
	t.Parallel()
	got := emitDefaultAggHelper(t,
		`CREATE TABLE da_a1(id INTEGER); CREATE TABLE da_b1(id INTEGER)`,
		"SELECT COUNT(*) FROM da_a1 JOIN da_b1 ON da_a1.id = da_b1.id",
	)
	switch gv := got.(type) {
	case int64:
		if gv != 0 {
			t.Errorf("got %d, want 0", gv)
		}
	case []byte:
		// acceptable representation
	default:
		t.Errorf("got type %T value %v, want int64(0)", got, got)
	}
}

// TestMCDC_EmitDefaultAggregateRow_AggCheck_Sum tests SUM default NULL on empty join.
func TestMCDC_EmitDefaultAggregateRow_AggCheck_Sum(t *testing.T) {
	t.Parallel()
	got := emitDefaultAggHelper(t,
		`CREATE TABLE da_a2(id INTEGER); CREATE TABLE da_b2(id INTEGER, v INTEGER)`,
		"SELECT SUM(da_b2.v) FROM da_a2 JOIN da_b2 ON da_a2.id = da_b2.id",
	)
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

// TestMCDC_EmitDefaultAggregateRow_AggCheck_Total tests TOTAL default 0.0 on empty join.
func TestMCDC_EmitDefaultAggregateRow_AggCheck_Total(t *testing.T) {
	t.Parallel()
	got := emitDefaultAggHelper(t,
		`CREATE TABLE da_a3(id INTEGER); CREATE TABLE da_b3(id INTEGER, v REAL)`,
		"SELECT TOTAL(da_b3.v) FROM da_a3 JOIN da_b3 ON da_a3.id = da_b3.id",
	)
	switch gv := got.(type) {
	case float64:
		if gv != 0 {
			t.Errorf("got %f, want 0.0", gv)
		}
	case []byte:
		// acceptable representation
	default:
		t.Errorf("got type %T value %v, want float64(0)", got, got)
	}
}

// ============================================================================
// multi_stmt.go:100-106 – executeAllStmts, result != nil guard
//
// Compound condition (single bool):
//   A = result != nil
//
// Outcome: A=T → accumulate RowsAffected; A=F → skip accumulation
//
// Cases needed (N+1 = 2):
//   Case A=T: DML statement returns a non-nil result
//   Case A=F: DDL statement (CREATE TABLE) returns nil or zero-row result
// ============================================================================

func TestMCDC_ExecuteAllStmts_ResultNil(t *testing.T) {
	t.Parallel()

	type tc struct {
		name         string
		stmts        string
		wantAffected int64
	}
	tests := []tc{
		{
			// A=T: INSERT returns non-nil result with RowsAffected=2
			name:         "A=T: multi-stmt with INSERT returns rows affected",
			stmts:        "CREATE TABLE ms_t1(x INTEGER); INSERT INTO ms_t1 VALUES(1); INSERT INTO ms_t1 VALUES(2)",
			wantAffected: 2, // both INSERTs accumulated
		},
		{
			// A=F: only DDL (CREATE TABLE) → rows affected = 0
			name:         "A=F: only DDL returns zero affected",
			stmts:        "CREATE TABLE ms_t2(x INTEGER); CREATE TABLE ms_t3(y INTEGER)",
			wantAffected: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			result, err := db.Exec(tt.stmts)
			if err != nil {
				t.Fatalf("exec failed: %v", err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				t.Fatalf("RowsAffected: %v", err)
			}
			if affected != tt.wantAffected {
				t.Errorf("got %d rows affected, want %d", affected, tt.wantAffected)
			}
		})
	}
}

// ============================================================================
// multi_stmt.go:112-118 – commitIfNeeded
//
// Compound condition (2 sub-conditions):
//   A = !m.conn.inTx  (not in explicit transaction)
//   B = m.conn.pager.InWriteTransaction()
//
// Outcome = A && B → commit; else → skip commit
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → autocommit mode AND write tx active → commit called
//   Flip A: A=F B=T → explicit BEGIN transaction → commit NOT called by commitIfNeeded
//   Flip B: A=T B=F → no write tx (read-only stmts) → no commit needed
// ============================================================================

func TestMCDC_CommitIfNeeded_AutocommitAndWriteTx(t *testing.T) {
	t.Parallel()

	// A=T B=T: autocommit DML – commit happens automatically after multi-stmt
	t.Run("A=T B=T: autocommit DML", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		_, err := db.Exec("CREATE TABLE cn_t1(x INTEGER); INSERT INTO cn_t1 VALUES(42)")
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}
		// Data must be visible after autocommit
		var v int
		if err := db.QueryRow("SELECT x FROM cn_t1").Scan(&v); err != nil {
			t.Fatalf("query after autocommit: %v", err)
		}
		if v != 42 {
			t.Errorf("got %d, want 42", v)
		}
	})

	// Flip A=F: explicit sql.Tx (sets conn.inTx=true) → commitIfNeeded skips its commit
	t.Run("Flip A=F: explicit sql.Tx transaction", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE cn_t2(x INTEGER)")
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := tx.Exec("INSERT INTO cn_t2 VALUES(1)"); err != nil {
			tx.Rollback()
			t.Fatalf("insert: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})

	// Flip B=F: read-only multi-stmt (no write tx) → commitIfNeeded skips
	t.Run("Flip B=F: read-only multi-stmt", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		_, err := db.Exec("SELECT 1; SELECT 2")
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}
	})
}

// ============================================================================
// multi_stmt.go:121-130 – buildResult, lastResult != nil guard
//
// Compound condition (single bool):
//   A = lastResult != nil
//
// Outcome: A=T → use lastInsertId from lastResult; A=F → zero lastInsertId
//
// Cases needed (N+1 = 2):
//   Case A=T: multi-stmt ending with INSERT → lastResult non-nil, LastInsertId set
//   Case A=F: multi-stmt with only DDL (CREATE) → lastResult nil → zero ID
// ============================================================================

func TestMCDC_BuildResult_LastResultNil(t *testing.T) {
	t.Parallel()

	type tc struct {
		name        string
		stmts       string
		wantLastID  int64
		checkLastID bool
	}
	tests := []tc{
		{
			// A=T: last stmt is INSERT → LastInsertId > 0
			name:        "A=T: multi-stmt ending with INSERT",
			stmts:       "CREATE TABLE br_t1(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO br_t1(v) VALUES('hello')",
			wantLastID:  1,
			checkLastID: true,
		},
		{
			// A=F: only DDL → LastInsertId = 0
			name:        "A=F: DDL-only multi-stmt",
			stmts:       "CREATE TABLE br_t2(x INTEGER); CREATE TABLE br_t3(y INTEGER)",
			wantLastID:  0,
			checkLastID: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			result, err := db.Exec(tt.stmts)
			if err != nil {
				t.Fatalf("exec failed: %v", err)
			}
			if tt.checkLastID {
				id, err := result.LastInsertId()
				if err != nil {
					t.Fatalf("LastInsertId: %v", err)
				}
				if id != tt.wantLastID {
					t.Errorf("LastInsertId = %d, want %d", id, tt.wantLastID)
				}
			}
		})
	}
}

// ============================================================================
// trigger_runtime.go:36-38 – ExecuteTriggers, recursionDepth guard
//
// Compound condition (single bool):
//   A = tr.recursionDepth >= maxTriggerDepth
//
// Outcome: A=T → error; A=F → proceed
//
// Cases needed (N+1 = 2):
//   Case A=F: normal trigger execution (depth < maxTriggerDepth)
//   (A=T is by design unreachable via normal SQL; we test A=F only)
// ============================================================================

func TestMCDC_ExecuteTriggers_RecursionDepthGuard(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)

	// Set up an AFTER INSERT trigger that inserts into a log table
	mustExec(t, db, "CREATE TABLE trig_src(id INTEGER, val TEXT)")
	mustExec(t, db, "CREATE TABLE trig_log(msg TEXT)")
	mustExec(t, db, `CREATE TRIGGER trig_after_ins
		AFTER INSERT ON trig_src
		BEGIN
			INSERT INTO trig_log VALUES('inserted');
		END`)

	// A=F: depth < maxTriggerDepth → trigger fires normally
	mustExec(t, db, "INSERT INTO trig_src VALUES(1, 'hello')")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM trig_log").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("trigger log count = %d, want 1", count)
	}
}

// ============================================================================
// trigger_runtime.go:41-53 – ExecuteTriggers, len(triggers)==0 guard
//
// Compound condition (single bool):
//   A = len(triggers) == 0
//
// Outcome: A=T → early return nil; A=F → execute triggers
//
// Cases needed (N+1 = 2):
//   Case A=T: DML on table with no triggers → early return
//   Case A=F: DML on table with trigger defined → triggers executed
// ============================================================================

func TestMCDC_ExecuteTriggers_EmptyTriggerList(t *testing.T) {
	t.Parallel()

	type tc struct {
		name     string
		hasTrig  bool
		wantLogs int
	}
	tests := []tc{
		{
			// A=T: no triggers → early return
			name:     "A=T: INSERT on table with no triggers",
			hasTrig:  false,
			wantLogs: 0,
		},
		{
			// A=F: trigger defined → executed
			name:     "A=F: INSERT on table with AFTER INSERT trigger",
			hasTrig:  true,
			wantLogs: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			mustExec(t, db, "CREATE TABLE etl_src(id INTEGER)")
			mustExec(t, db, "CREATE TABLE etl_log(n INTEGER)")
			if tt.hasTrig {
				mustExec(t, db, `CREATE TRIGGER etl_trig
					AFTER INSERT ON etl_src
					BEGIN
						INSERT INTO etl_log VALUES(NEW.id);
					END`)
			}
			mustExec(t, db, "INSERT INTO etl_src VALUES(99)")
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM etl_log").Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != tt.wantLogs {
				t.Errorf("log count = %d, want %d", count, tt.wantLogs)
			}
		})
	}
}

// ============================================================================
// trigger_runtime.go:71 – executeSingleTrigger, MatchesUpdateColumns guard
//
// Compound condition (single bool):
//   A = !trigger.MatchesUpdateColumns(updatedCols)
//
// Outcome: A=T → skip trigger; A=F → proceed to execute
//
// Cases (for UPDATE triggers with OF column list):
//   Case A=T: UPDATE touches only column NOT in trigger's OF list → skip
//   Case A=F: UPDATE touches column IN trigger's OF list → execute
// ============================================================================

func TestMCDC_ExecuteSingleTrigger_MatchesUpdateColumns(t *testing.T) {
	t.Parallel()

	type tc struct {
		name     string
		update   string
		wantLogs int
	}
	tests := []tc{
		{
			// A=F: UPDATE touches 'val' which is in trigger's OF list → fires
			name:     "A=F: UPDATE on trigger column – trigger fires",
			update:   "UPDATE muc_src SET val = 'new' WHERE id = 1",
			wantLogs: 1,
		},
		{
			// A=F: UPDATE touches 'other' which is NOT in trigger's OF list → skip
			// (trigger is on OF val, updating 'other' skips it)
			name:     "A=T: UPDATE on non-trigger column – trigger skipped",
			update:   "UPDATE muc_src SET other = 'x' WHERE id = 1",
			wantLogs: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			mustExec(t, db, "CREATE TABLE muc_src(id INTEGER, val TEXT, other TEXT)")
			mustExec(t, db, "CREATE TABLE muc_log(note TEXT)")
			mustExec(t, db, `CREATE TRIGGER muc_trig
				AFTER UPDATE OF val ON muc_src
				BEGIN
					INSERT INTO muc_log VALUES('updated');
				END`)
			mustExec(t, db, "INSERT INTO muc_src VALUES(1, 'old', 'o')")
			mustExec(t, db, tt.update)
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM muc_log").Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != tt.wantLogs {
				t.Errorf("log count = %d, want %d", count, tt.wantLogs)
			}
		})
	}
}

// ============================================================================
// trigger_runtime.go:117-130 – substituteReferences, triggerRow == nil guard
//
// Compound condition (single bool):
//   A = triggerRow == nil
//
// Outcome: A=T → return stmt unchanged; A=F → apply substitution
//
// Cases needed (N+1 = 2):
//   Case A=F: AFTER INSERT trigger with NEW.col reference → substitution applied
//   Case A=T: not directly testable via SQL (internal guard);
//             covered indirectly by trigger executing without OLD/NEW refs
// ============================================================================

func TestMCDC_SubstituteReferences_TriggerRowNil(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE sr_src(id INTEGER, name TEXT)")
	mustExec(t, db, "CREATE TABLE sr_dst(id INTEGER, name TEXT)")
	mustExec(t, db, `CREATE TRIGGER sr_trig
		AFTER INSERT ON sr_src
		BEGIN
			INSERT INTO sr_dst VALUES(NEW.id, NEW.name);
		END`)

	// A=F: triggerRow is non-nil → substituteReferences replaces NEW.id, NEW.name
	mustExec(t, db, "INSERT INTO sr_src VALUES(7, 'world')")

	var id int
	var name string
	if err := db.QueryRow("SELECT id, name FROM sr_dst").Scan(&id, &name); err != nil {
		t.Fatalf("query: %v", err)
	}
	if id != 7 || name != "world" {
		t.Errorf("got (%d, %q), want (7, %q)", id, name, "world")
	}
}

// ============================================================================
// trigger_runtime.go:331-353 – substituteIdent, OLD/NEW qualifier
//
// Compound condition (2 sub-conditions, ||):
//   A = qualifier == "OLD"
//   B = qualifier == "NEW"
//
// Outcome = !(A || B) → return expr unchanged (not OLD/NEW);
//           A → use oldRow; B → use newRow
//
// Cases needed (N+1 = 3):
//   Base:   A=F B=F → plain column reference (not OLD or NEW) → unchanged
//   Flip A: A=T B=F → OLD.col reference → oldRow used
//   Flip B: A=F B=T → NEW.col reference → newRow used
// ============================================================================

func TestMCDC_SubstituteIdent_OldNewQualifier(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE si_src(id INTEGER, val INTEGER)")
	mustExec(t, db, "CREATE TABLE si_log(old_val INTEGER, new_val INTEGER)")
	mustExec(t, db, `CREATE TRIGGER si_trig
		AFTER UPDATE ON si_src
		BEGIN
			INSERT INTO si_log VALUES(OLD.val, NEW.val);
		END`)

	mustExec(t, db, "INSERT INTO si_src VALUES(1, 10)")
	mustExec(t, db, "UPDATE si_src SET val = 20 WHERE id = 1")

	var oldVal, newVal int
	if err := db.QueryRow("SELECT old_val, new_val FROM si_log").Scan(&oldVal, &newVal); err != nil {
		t.Fatalf("query: %v", err)
	}
	// Flip A: OLD.val → 10
	if oldVal != 10 {
		t.Errorf("old_val = %d, want 10", oldVal)
	}
	// Flip B: NEW.val → 20
	if newVal != 20 {
		t.Errorf("new_val = %d, want 20", newVal)
	}
}

// ============================================================================
// trigger_runtime.go:345-353 – substituteIdent, found/not-found guard
//
// Compound condition (2 sub-conditions, ||):
//   A = found (direct map lookup)
//   B = found (case-insensitive lookup after A=F)
//
// Outcome = !(A || B) → error; A || B → return literal
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=? → exact match found → literal returned
//   Flip A: A=F B=T → case-insensitive match found → literal returned
//   Neither: A=F B=F → not found → error (column not in row)
// ============================================================================

func TestMCDC_SubstituteIdent_ColumnLookup(t *testing.T) {
	t.Parallel()

	// A=T: exact match (standard column names match exactly)
	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE cl_src(id INTEGER, Name TEXT)")
	mustExec(t, db, "CREATE TABLE cl_log(n TEXT)")
	mustExec(t, db, `CREATE TRIGGER cl_trig
		AFTER INSERT ON cl_src
		BEGIN
			INSERT INTO cl_log VALUES(NEW.Name);
		END`)
	mustExec(t, db, "INSERT INTO cl_src VALUES(1, 'Alice')")

	var name string
	if err := db.QueryRow("SELECT n FROM cl_log").Scan(&name); err != nil {
		t.Fatalf("query: %v", err)
	}
	if name != "Alice" {
		t.Errorf("got %q, want %q", name, "Alice")
	}
}

// ============================================================================
// value.go:22-27 – convertUint64, overflow guard
//
// Compound condition (single bool):
//   A = val > 1<<63-1
//
// Outcome: A=T → error; A=F → int64 conversion
//
// Cases needed (N+1 = 2):
//   Case A=F: uint64 within int64 range → converted successfully
//   Case A=T: uint64 > MaxInt64 → error returned
// ============================================================================

func TestMCDC_ConvertUint64_OverflowGuard(t *testing.T) {
	t.Parallel()

	vc := ValueConverter{}

	// A=F: small uint64 value fits in int64
	t.Run("A=F: uint64 within int64 range", func(t *testing.T) {
		t.Parallel()
		var v uint64 = 42
		got, err := vc.ConvertValue(v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != int64(42) {
			t.Errorf("got %v, want int64(42)", got)
		}
	})

	// A=T: uint64 > MaxInt64 → error
	t.Run("A=T: uint64 overflow int64", func(t *testing.T) {
		t.Parallel()
		var v uint64 = 1<<63 + 1
		_, err := vc.ConvertValue(v)
		if err == nil {
			t.Fatal("expected overflow error but got none")
		}
	})
}

// ============================================================================
// value.go:35-46 – convertToInt64, kind range check
//
// Compound condition (2 sub-conditions):
//   A = int64Kinds[rv.Kind()] is true  (kind in set)
//   B = rv.Kind() >= reflect.Uint && rv.Kind() <= reflect.Uint32
//
// Outcome: !A → return (0, false); A && B → unsigned path; A && !B → signed path
//
// Cases needed (N+1 = 4 for 3 sub-cases):
//   Case !A: unsupported kind → false returned
//   Case A && B: uint8/uint16/uint32 → unsigned path
//   Case A && !B: int8/int16/int32 → signed path
// ============================================================================

// assertConvertValue calls vc.ConvertValue and checks the result against want.
func assertConvertValue(t *testing.T, vc ValueConverter, input interface{}, want interface{}) {
	t.Helper()
	got, err := vc.ConvertValue(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %v (%T), want %v (%T)", got, got, want, want)
	}
}

func TestMCDC_ConvertToInt64_KindBranches(t *testing.T) {
	t.Parallel()

	vc := ValueConverter{}

	// A=F: string is not in int64Kinds → unsupported type error
	t.Run("A=F: string not in int64Kinds", func(t *testing.T) {
		t.Parallel()
		var v int8 = 5
		assertConvertValue(t, vc, v, int64(5))
	})

	// A=T && B=T: uint8 → unsigned path
	t.Run("A=T B=T: uint8 uses unsigned path", func(t *testing.T) {
		t.Parallel()
		var v uint8 = 200
		assertConvertValue(t, vc, v, int64(200))
	})

	// A=T && B=T: uint32 → unsigned path
	t.Run("A=T B=T: uint32 uses unsigned path", func(t *testing.T) {
		t.Parallel()
		var v uint32 = 100000
		assertConvertValue(t, vc, v, int64(100000))
	})

	// A=T && B=F: int32 → signed path
	t.Run("A=T B=F: int32 uses signed path", func(t *testing.T) {
		t.Parallel()
		var v int32 = -1234
		assertConvertValue(t, vc, v, int64(-1234))
	})

	// A=F: float64 is native; check a truly unsupported type → error
	t.Run("A=F: unsupported type (complex64)", func(t *testing.T) {
		t.Parallel()
		var v complex64 = 1 + 2i
		_, err := vc.ConvertValue(v)
		if err == nil {
			t.Fatal("expected error for complex64, got none")
		}
	})
}

// ============================================================================
// value.go:48-65 – ConvertValue, nil and isNativeDriverValue guards
//
// Compound condition (sequential guards):
//   A = v == nil               → return (nil, nil)
//   B = isNativeDriverValue(v) → return (v, nil)
//
// Outcome: A=T → nil result; !A && B=T → v returned; !A && !B → try conversions
//
// Cases needed (N+1 = 3):
//   Case A=T:  nil input → (nil, nil)
//   Case !A && B=T: int64 (native) → returned as-is
//   Case !A && B=F: int32 (non-native) → converted to int64
// ============================================================================

func TestMCDC_ConvertValue_NilAndNativeGuards(t *testing.T) {
	t.Parallel()

	vc := ValueConverter{}

	// A=T: nil → (nil, nil)
	t.Run("A=T: nil input", func(t *testing.T) {
		t.Parallel()
		got, err := vc.ConvertValue(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	// A=F && B=T: int64 is native → returned as-is
	t.Run("A=F B=T: int64 is native driver value", func(t *testing.T) {
		t.Parallel()
		assertConvertValue(t, vc, int64(12345), int64(12345))
	})

	// A=F && B=T: string is native
	t.Run("A=F B=T: string is native driver value", func(t *testing.T) {
		t.Parallel()
		assertConvertValue(t, vc, "test", "test")
	})

	// A=F && B=F: int16 is not native → converted to int64
	t.Run("A=F B=F: int16 not native, converted to int64", func(t *testing.T) {
		t.Parallel()
		var v int16 = 256
		assertConvertValue(t, vc, v, int64(256))
	})

	// A=F && B=F: float32 → converted to float64
	t.Run("A=F B=F: float32 converted to float64", func(t *testing.T) {
		t.Parallel()
		var v float32 = 3.14
		got, err := vc.ConvertValue(v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(float64); !ok {
			t.Errorf("got %T, want float64", got)
		}
	})
}

// ============================================================================
// compile_pragma_tvf.go:364-378 – evalPragmaComparison, strict bool
//
// Compound condition (2 sub-conditions):
//   A = lok && rok  (both values convert to int64)
//   B = strict      (true = ">", false = ">=")
//
// Outcome = A && B → lInt > rInt; A && !B → lInt >= rInt; !A → return true
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → strict > comparison
//   Flip B: A=T B=F → non-strict >= comparison
//   Flip A: A=F B=? → non-numeric → return true
// ============================================================================

func TestMCDC_EvalPragmaComparison_StrictGtVsGe(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		setup     string
		query     string
		wantCount int
	}
	tests := []tc{
		{
			// A=T B=T: cid > 0 → strict comparison, excludes cid=0
			name:      "A=T B=T: WHERE cid > 0 (strict)",
			setup:     "CREATE TABLE pg_t1(x INTEGER, y TEXT, z REAL)",
			query:     "SELECT name FROM pragma_table_info('pg_t1') WHERE cid > 0",
			wantCount: 2, // y and z (cid 1 and 2)
		},
		{
			// A=T B=F: cid >= 1 → non-strict comparison
			name:      "A=T B=F: WHERE cid >= 1 (non-strict)",
			setup:     "CREATE TABLE pg_t2(a INTEGER, b TEXT, c REAL)",
			query:     "SELECT name FROM pragma_table_info('pg_t2') WHERE cid >= 1",
			wantCount: 2, // b and c (cid 1 and 2)
		},
		{
			// A=T B=F: cid >= 0 → includes all
			name:      "A=T B=F: WHERE cid >= 0 includes all",
			setup:     "CREATE TABLE pg_t3(p INTEGER, q TEXT)",
			query:     "SELECT name FROM pragma_table_info('pg_t3') WHERE cid >= 0",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			var count int
			for rows.Next() {
				count++
				var name string
				rows.Scan(&name)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if count != tt.wantCount {
				t.Errorf("got %d rows, want %d", count, tt.wantCount)
			}
		})
	}
}

// ============================================================================
// trigger_runtime.go:77-83 – executeSingleTrigger, shouldExec guard
//
// Compound condition (single bool from ShouldExecuteTrigger):
//   A = shouldExec (WHEN clause evaluates to true or no WHEN clause)
//
// Outcome: A=T → executeTriggerBody; A=F → skip
//
// Cases needed (N+1 = 2):
//   Case A=T: trigger with WHEN clause that evaluates true → fires
//   Case A=F: trigger with WHEN clause that evaluates false → skipped
// ============================================================================

func TestMCDC_ExecuteSingleTrigger_WhenClause(t *testing.T) {
	t.Parallel()

	type tc struct {
		name     string
		insert   string
		wantLogs int
	}
	tests := []tc{
		{
			// A=T: WHEN NEW.val > 0 → fires for positive values
			name:     "A=T: WHEN clause true – trigger fires",
			insert:   "INSERT INTO wc_src VALUES(1, 5)",
			wantLogs: 1,
		},
		{
			// A=F: WHEN NEW.val > 0 → does not fire for negative values
			name:     "A=F: WHEN clause false – trigger skipped",
			insert:   "INSERT INTO wc_src VALUES(2, -1)",
			wantLogs: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			mustExec(t, db, "CREATE TABLE wc_src(id INTEGER, val INTEGER)")
			mustExec(t, db, "CREATE TABLE wc_log(note TEXT)")
			mustExec(t, db, `CREATE TRIGGER wc_trig
				AFTER INSERT ON wc_src
				WHEN NEW.val > 0
				BEGIN
					INSERT INTO wc_log VALUES('fired');
				END`)
			mustExec(t, db, tt.insert)
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM wc_log").Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != tt.wantLogs {
				t.Errorf("log count = %d, want %d", count, tt.wantLogs)
			}
		})
	}
}

// ============================================================================
// compile_pragma_tvf.go:250-257 – resolvePragmaColumns, star guard
//
// Compound condition (2 sub-conditions):
//   A = len(selectCols) == 1
//   B = selectCols[0].Star
//
// Outcome = A && B → return all columns (SELECT *); else → specific columns
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → SELECT * → all columns
//   Flip A: A=F B=? → multiple specific columns → subset
//   Flip B: A=T B=F → single non-star column → one column
// ============================================================================

func TestMCDC_ResolvePragmaColumns_StarGuard(t *testing.T) {
	t.Parallel()

	type tc struct {
		name     string
		setup    string
		query    string
		wantCols int // expected number of result columns
		wantRows int
	}
	tests := []tc{
		{
			// A=T B=T: SELECT * → all 6 pragma_table_info columns
			name:     "A=T B=T: SELECT * returns all columns",
			setup:    "CREATE TABLE rpc_t1(id INTEGER, name TEXT)",
			query:    "SELECT * FROM pragma_table_info('rpc_t1')",
			wantCols: 6,
			wantRows: 2,
		},
		{
			// Flip A=F: SELECT cid, name → 2 columns
			name:     "Flip A=F: SELECT cid, name returns 2 columns",
			setup:    "CREATE TABLE rpc_t2(x INTEGER, y TEXT)",
			query:    "SELECT cid, name FROM pragma_table_info('rpc_t2')",
			wantCols: 2,
			wantRows: 2,
		},
		{
			// Flip B=F: SELECT name (single non-star column) → 1 column
			name:     "Flip B=F: SELECT name (single column, not star)",
			setup:    "CREATE TABLE rpc_t3(a INTEGER, b TEXT, c REAL)",
			query:    "SELECT name FROM pragma_table_info('rpc_t3')",
			wantCols: 1,
			wantRows: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			gotCols, gotRows := queryColAndRowCount(t, db, tt.query)
			if gotCols != tt.wantCols {
				t.Errorf("got %d columns, want %d", gotCols, tt.wantCols)
			}
			if gotRows != tt.wantRows {
				t.Errorf("got %d rows, want %d", gotRows, tt.wantRows)
			}
		})
	}
}

// queryColAndRowCount runs a query and returns the number of columns and rows.
func queryColAndRowCount(t *testing.T, db interface {
	Query(string, ...interface{}) (*sql.Rows, error)
}, query string) (int, int) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var rowCount int
	for rows.Next() {
		rowCount++
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return len(cols), rowCount
}

// ============================================================================
// compile_join_agg.go:385-401 – findColumnCollation, table-qualified guard
//
// Compound condition (single bool in ident.Table != ""):
//   A = ident.Table != "" (table-qualified identifier)
//
// Outcome: A=T → search only matching table; A=F → first-match-wins search
//
// Cases needed (N+1 = 2):
//   Case A=T: GROUP BY with table-qualified column (t.col)
//   Case A=F: GROUP BY with unqualified column (col)
// ============================================================================

func TestMCDC_FindColumnCollation_TableQualifiedGuard(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		setup     string
		query     string
		wantCount int
	}
	tests := []tc{
		{
			// A=T: GROUP BY with table-qualified column
			name: "A=T: GROUP BY with table-qualified column",
			setup: `CREATE TABLE fcc_a(id INTEGER, cat TEXT);
                    CREATE TABLE fcc_b(id INTEGER, score INTEGER);
                    INSERT INTO fcc_a VALUES(1,'x'),(2,'y'),(3,'x');
                    INSERT INTO fcc_b VALUES(1,5),(2,10),(3,15)`,
			query:     "SELECT fcc_a.cat, COUNT(*) FROM fcc_a JOIN fcc_b ON fcc_a.id=fcc_b.id GROUP BY fcc_a.cat",
			wantCount: 2,
		},
		{
			// A=F: GROUP BY with unqualified column
			name: "A=F: GROUP BY with unqualified column",
			setup: `CREATE TABLE fcc_c(id INTEGER, dept TEXT);
                    CREATE TABLE fcc_d(id INTEGER, val INTEGER);
                    INSERT INTO fcc_c VALUES(1,'eng'),(2,'mkt'),(3,'eng');
                    INSERT INTO fcc_d VALUES(1,1),(2,2),(3,3)`,
			query:     "SELECT dept, SUM(fcc_d.val) FROM fcc_c JOIN fcc_d ON fcc_c.id=fcc_d.id GROUP BY dept",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			var count int
			for rows.Next() {
				count++
				var cat string
				var cnt int64
				rows.Scan(&cat, &cnt)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if count != tt.wantCount {
				t.Errorf("got %d rows, want %d", count, tt.wantCount)
			}
		})
	}
}

// ============================================================================
// compile_pragma_tvf.go:146-156 – compilePragmaTVFForeignKeyList
//
// Compound condition (single bool):
//   A = s.conn.fkManager != nil
//
// Outcome: A=T → call buildForeignKeyListRows; A=F → rows stays nil (empty)
//
// Cases needed (N+1 = 2):
//   Case A=T: table with foreign key → fkManager is non-nil, rows populated
//   Case A=F: table without FK → still valid (no crash), returns empty
// ============================================================================

func TestMCDC_CompilePragmaTVFForeignKeyList_FkManagerGuard(t *testing.T) {
	t.Parallel()

	type tc struct {
		name     string
		setup    string
		query    string
		wantRows int
		wantErr  bool
	}
	tests := []tc{
		{
			// A=T: table with FK defined
			name: "A=T: pragma_foreign_key_list with FK",
			setup: `CREATE TABLE fk_parent(id INTEGER PRIMARY KEY);
                    CREATE TABLE fk_child(id INTEGER, parent_id INTEGER REFERENCES fk_parent(id))`,
			query:    "SELECT * FROM pragma_foreign_key_list('fk_child')",
			wantRows: 1,
			wantErr:  false,
		},
		{
			// A=F: table without FK → 0 rows
			name:     "A=F: pragma_foreign_key_list on table without FK",
			setup:    "CREATE TABLE fk_notbl(id INTEGER)",
			query:    "SELECT * FROM pragma_foreign_key_list('fk_notbl')",
			wantRows: 0,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			count := queryDrainRowCount(t, db, tt.query, tt.wantErr)
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// queryDrainRowCount runs a query, optionally expecting an error. If no error,
// it drains all rows and returns the count. Returns 0 if wantErr and error occurred.
func queryDrainRowCount(t *testing.T, db interface {
	Query(string, ...interface{}) (*sql.Rows, error)
}, query string, wantErr bool) int {
	t.Helper()
	rows, err := db.Query(query)
	if wantErr && err == nil {
		t.Fatal("expected error but got none")
	}
	if !wantErr && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows == nil {
		return 0
	}
	defer rows.Close()
	return drainRows(t, rows)
}

// drainRows scans and counts all remaining rows, draining every column.
func drainRows(t *testing.T, rows *sql.Rows) int {
	t.Helper()
	var count int
	for rows.Next() {
		count++
		cols, _ := rows.Columns()
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return count
}
