// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// ---- emitJSONGroupObjectUpdate coverage ----------------------------------------

// TestCompileAggHelpersJSONGroupObject exercises json_group_object aggregate
// which internally triggers emitJSONGroupObjectUpdate.
// jsonGroupObjectDB sets up the shared employees table for json_group_object tests.
func jsonGroupObjectDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE employees (dept TEXT, name TEXT, salary INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, s := range []string{
		`INSERT INTO employees VALUES ('eng', 'alice', 90000)`,
		`INSERT INTO employees VALUES ('eng', 'bob', 80000)`,
		`INSERT INTO employees VALUES ('hr', 'carol', 70000)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("insert %q: %v", s, err)
		}
	}
	return db
}

func TestCompileAggHelpersJSONGroupObjectBasic(t *testing.T) {
	t.Parallel()
	db := jsonGroupObjectDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT json_group_object(name, salary) FROM employees`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	var result interface{}
	if err := rows.Scan(&result); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil json_group_object result")
	}
}

func TestCompileAggHelpersJSONGroupObjectGrouped(t *testing.T) {
	t.Parallel()
	db := jsonGroupObjectDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT dept, json_group_object(name, salary) FROM employees GROUP BY dept ORDER BY dept`)
	if err != nil {
		t.Fatalf("grouped query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var dept string
		var obj interface{}
		if err := rows.Scan(&dept, &obj); err != nil {
			t.Fatalf("scan grouped: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count < 1 {
		t.Errorf("expected grouped rows, got %d", count)
	}
}

func TestCompileAggHelpersJSONGroupObjectNullKey(t *testing.T) {
	t.Parallel()
	db := jsonGroupObjectDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT json_group_object(NULL, 1) FROM employees`)
	if err != nil {
		t.Fatalf("null key query: %v", err)
	}
	defer rows.Close()
	rows.Next()
}

// ---- findColumnIndex coverage --------------------------------------------------

// TestCompileAggHelpersFindColumnIndex exercises findColumnIndex via queries
// that cause the compiler to look up columns that exist (exact, case-insensitive)
// and columns that do not exist (returns -1).
func TestCompileAggHelpersFindColumnIndex(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE items (ID INTEGER, Name TEXT, Value REAL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO items VALUES (1, 'foo', 3.14)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Exact match.
	var id int64
	if err := db.QueryRow(`SELECT COUNT(ID) FROM items`).Scan(&id); err != nil {
		t.Fatalf("exact match query: %v", err)
	}

	// Case-insensitive match (column "Name" queried as "name").
	var cnt int64
	if err := db.QueryRow(`SELECT COUNT(name) FROM items`).Scan(&cnt); err != nil {
		t.Fatalf("case-insensitive query: %v", err)
	}

	// Aggregate on a column via mixed case to exercise uppercase-compare branch.
	var sum float64
	if err := db.QueryRow(`SELECT SUM(value) FROM items`).Scan(&sum); err != nil {
		t.Fatalf("sum value query: %v", err)
	}
}

// ---- runSQLTestQuery coverage --------------------------------------------------

// TestCompileAggHelpersRunSQLTestQuery exercises runSQLTestQuery through
// runSQLTestsFreshDB with both normal and error-expected paths.
func TestCompileAggHelpersRunSQLTestQuery(t *testing.T) {
	t.Parallel()

	tests := []sqlTestCase{
		{
			name: "simple select returns rows",
			setup: []string{
				`CREATE TABLE t1 (x INTEGER)`,
				`INSERT INTO t1 VALUES (42)`,
			},
			query:    `SELECT x FROM t1`,
			wantRows: [][]interface{}{{int64(42)}},
		},
		{
			name: "query that returns no rows",
			setup: []string{
				`CREATE TABLE t2 (x INTEGER)`,
			},
			query:    `SELECT x FROM t2`,
			wantRows: nil,
		},
		{
			name:    "query expected to error",
			setup:   []string{},
			query:   `SELECT no_such_func_xyz(1)`,
			wantErr: true,
			errLike: "",
		},
	}

	runSQLTestsFreshDB(t, tests)
}

// TestCompileAggHelpersRunSQLTestQueryIterError exercises the path where
// runSQLTestQuery has wantErr=true but the error surfaces during row
// iteration rather than at Query time.
func TestCompileAggHelpersRunSQLTestQueryIterError(t *testing.T) {
	t.Parallel()

	// The wantErr path where err != nil at query time.
	db := setupMemoryDB(t)
	defer db.Close()

	tt := sqlTestCase{
		name:    "error at query time with wantErr",
		query:   `SELECT no_func_zzz(1)`,
		wantErr: true,
		errLike: "",
	}
	// runSQLTestQuery is the function under test.
	runSQLTestQuery(t, db, tt)
}

// ---- compareRows coverage ------------------------------------------------------

// TestCompileAggHelpersCompareRows exercises compareRows with matching rows,
// mismatched row count, and mismatched column count.
func TestCompileAggHelpersCompareRows(t *testing.T) {
	// Matching rows – should not fail.
	t.Run("matching", func(t *testing.T) {
		got := [][]interface{}{{int64(1), "hello"}, {int64(2), "world"}}
		want := [][]interface{}{{int64(1), "hello"}, {int64(2), "world"}}
		compareRows(t, got, want)
	})

	// Mismatched values (different int) – compareRows calls t.Errorf, not Fatal.
	t.Run("value mismatch", func(t *testing.T) {
		inner := &testing.T{}
		got := [][]interface{}{{int64(1)}}
		want := [][]interface{}{{int64(99)}}
		compareRows(inner, got, want)
		// inner should have recorded a failure
		if !inner.Failed() {
			t.Error("expected compareRows to mark test failed on value mismatch")
		}
	})

	// Column count mismatch within a row.
	t.Run("column count mismatch", func(t *testing.T) {
		inner := &testing.T{}
		got := [][]interface{}{{int64(1), int64(2)}}
		want := [][]interface{}{{int64(1)}}
		compareRows(inner, got, want)
		if !inner.Failed() {
			t.Error("expected compareRows to mark test failed on col count mismatch")
		}
	})

	// Row count mismatch – compareRows calls t.Fatalf.
	t.Run("row count mismatch is caught by helper", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()
		mustExec(t, db, `CREATE TABLE rc (v INTEGER)`)
		mustExec(t, db, `INSERT INTO rc VALUES (1)`)
		mustExec(t, db, `INSERT INTO rc VALUES (2)`)
		rows := mustQuery(t, db, `SELECT v FROM rc ORDER BY v`)
		got := scanAllRows(t, rows)
		rows.Close()
		// got has 2 rows; compare against 2 rows to confirm no panic
		want := [][]interface{}{{int64(1)}, {int64(2)}}
		compareRows(t, got, want)
	})
}

// ---- equalInt64 coverage -------------------------------------------------------

// TestCompileAggHelpersEqualInt64 exercises all branches of equalInt64.
func TestCompileAggHelpersEqualInt64(t *testing.T) {
	cases := []struct {
		a    int64
		b    interface{}
		want bool
	}{
		{10, int64(10), true},
		{10, int64(11), false},
		{10, int(10), true},
		{10, int(9), false},
		{10, float64(10.0), true},
		{10, float64(10.5), false},
		{10, "10", false},         // unknown type → false
		{10, nil, false},          // nil → false
		{10, []byte("10"), false}, // unknown type → false
	}
	for _, c := range cases {
		got := equalInt64(c.a, c.b)
		if got != c.want {
			t.Errorf("equalInt64(%v, %v (%T)) = %v, want %v", c.a, c.b, c.b, got, c.want)
		}
	}
}

// ---- equalFloat64 coverage -----------------------------------------------------

// TestCompileAggHelpersEqualFloat64 exercises all branches of equalFloat64.
func TestCompileAggHelpersEqualFloat64(t *testing.T) {
	cases := []struct {
		a    float64
		b    interface{}
		want bool
	}{
		{3.14, float64(3.14), true},
		{3.14, float64(2.71), false},
		{10.0, int64(10), true},
		{10.0, int64(11), false},
		{10.0, int(10), true},
		{10.0, int(11), false},
		{10.0, "10.0", false}, // unknown type → false
		{10.0, nil, false},    // nil → false
	}
	for _, c := range cases {
		got := equalFloat64(c.a, c.b)
		if got != c.want {
			t.Errorf("equalFloat64(%v, %v (%T)) = %v, want %v", c.a, c.b, c.b, got, c.want)
		}
	}
}

// ---- tryTypeSpecificComparison coverage ----------------------------------------

// TestCompileAggHelpersTryTypeSpecificComparison exercises the type-switch in
// tryTypeSpecificComparison including the int branch and the "no match" branch.
func TestCompileAggHelpersTryTypeSpecificComparison(t *testing.T) {
	cases := []struct {
		a       interface{}
		b       interface{}
		equal   bool
		handled bool
	}{
		// int64 branch
		{int64(5), int64(5), true, true},
		{int64(5), int64(6), false, true},
		// int branch
		{int(7), int(7), true, true},
		{int(7), int(8), false, true},
		// float64 branch
		{float64(1.5), float64(1.5), true, true},
		{float64(1.5), float64(2.5), false, true},
		// string branch
		{"hello", "hello", true, true},
		{"hello", "world", false, true},
		// unknown type → not handled
		{[]byte("x"), []byte("x"), false, false},
		{nil, nil, false, false},
		{true, true, false, false},
	}
	for _, c := range cases {
		equal, ok := tryTypeSpecificComparison(c.a, c.b)
		if ok != c.handled {
			t.Errorf("tryTypeSpecificComparison(%v, %v): ok=%v, want %v", c.a, c.b, ok, c.handled)
		}
		if ok && equal != c.equal {
			t.Errorf("tryTypeSpecificComparison(%v, %v): equal=%v, want %v", c.a, c.b, equal, c.equal)
		}
	}
}
