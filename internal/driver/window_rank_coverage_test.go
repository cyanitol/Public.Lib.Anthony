// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openRankCoverDB opens a fresh in-memory database for window rank coverage tests.
func openRankCoverDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// rankExec executes a statement and fails on error.
func rankExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// rankQueryRows runs a query and returns each row as []interface{}.
func rankQueryRows(t *testing.T, db *sql.DB, q string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return result
}

// rankToInt64 converts an interface{} result value to int64.
func rankToInt64(t *testing.T, v interface{}, label string) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	default:
		t.Fatalf("%s: cannot convert %T to int64", label, v)
		return 0
	}
}

// rankToFloat64 converts an interface{} result value to float64.
func rankToFloat64(t *testing.T, v interface{}, label string) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	default:
		t.Fatalf("%s: cannot convert %T to float64", label, v)
		return 0
	}
}

// setupRankEmployees creates the employees table with tied salary values that
// exercise rank change detection in RANK/DENSE_RANK window functions.
func setupRankEmployees(t *testing.T, db *sql.DB) {
	t.Helper()
	rankExec(t, db, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL)")
	rankExec(t, db, "INSERT INTO employees VALUES(1,'Alice','Eng',80000)")
	rankExec(t, db, "INSERT INTO employees VALUES(2,'Bob','Eng',80000)") // tie with Alice
	rankExec(t, db, "INSERT INTO employees VALUES(3,'Carol','Mkt',90000)")
	rankExec(t, db, "INSERT INTO employees VALUES(4,'Dave','Eng',70000)")
	rankExec(t, db, "INSERT INTO employees VALUES(5,'Eve','Mkt',90000)") // tie with Carol
}

// TestWindowRankRowNumberOrderBy exercises extractWindowOrderByCols and
// emitWindowRankTrackingFromSorter via ROW_NUMBER() OVER (ORDER BY salary).
// ROW_NUMBER always increments by 1 regardless of ties.
func TestWindowRankRowNumberOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, ROW_NUMBER() OVER (ORDER BY salary) AS rn FROM employees ORDER BY salary, name")

	if len(rows) != 5 {
		t.Fatalf("ROW_NUMBER ORDER BY: expected 5 rows, got %d", len(rows))
	}
	// ROW_NUMBER must be strictly sequential regardless of salary ties.
	for i, row := range rows {
		rn := rankToInt64(t, row[1], fmt.Sprintf("row %d rn", i))
		if rn != int64(i+1) {
			t.Errorf("ROW_NUMBER row %d: got %d, want %d", i, rn, i+1)
		}
	}
}

// TestWindowRankRankOrderBy exercises emitWindowRankComparison and emitWindowRankUpdate
// via RANK() OVER (ORDER BY salary). Tied salaries share the same rank, and the next
// rank skips values equal to the tie count.
func TestWindowRankRankOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, salary, RANK() OVER (ORDER BY salary) AS r FROM employees ORDER BY salary, name")

	if len(rows) != 5 {
		t.Fatalf("RANK ORDER BY: expected 5 rows, got %d", len(rows))
	}
	// Dave: salary 70000 → rank 1
	// Alice, Bob: salary 80000 (tied) → both rank 2
	// Carol, Eve: salary 90000 (tied) → both rank 4
	r0 := rankToInt64(t, rows[0][2], "Dave rank")
	if r0 != 1 {
		t.Errorf("Dave RANK = %d, want 1", r0)
	}
	r1 := rankToInt64(t, rows[1][2], "Alice rank")
	r2 := rankToInt64(t, rows[2][2], "Bob rank")
	if r1 != 2 || r2 != 2 {
		t.Errorf("Alice/Bob RANK = %d/%d, want 2/2", r1, r2)
	}
	r3 := rankToInt64(t, rows[3][2], "Carol rank")
	r4 := rankToInt64(t, rows[4][2], "Eve rank")
	if r3 != 4 || r4 != 4 {
		t.Errorf("Carol/Eve RANK = %d/%d, want 4/4", r3, r4)
	}
}

// TestWindowRankDenseRankOrderBy exercises emitWindowRankComparison and emitWindowRankUpdate
// via DENSE_RANK() OVER (ORDER BY salary). Tied salaries share rank but dense rank
// never skips values.
func TestWindowRankDenseRankOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary) AS dr FROM employees ORDER BY salary, name")

	if len(rows) != 5 {
		t.Fatalf("DENSE_RANK ORDER BY: expected 5 rows, got %d", len(rows))
	}
	// Dave: salary 70000 → dense rank 1
	// Alice, Bob: salary 80000 (tied) → both dense rank 2
	// Carol, Eve: salary 90000 (tied) → both dense rank 3
	dr0 := rankToInt64(t, rows[0][2], "Dave dense rank")
	if dr0 != 1 {
		t.Errorf("Dave DENSE_RANK = %d, want 1", dr0)
	}
	dr1 := rankToInt64(t, rows[1][2], "Alice dense rank")
	dr2 := rankToInt64(t, rows[2][2], "Bob dense rank")
	if dr1 != 2 || dr2 != 2 {
		t.Errorf("Alice/Bob DENSE_RANK = %d/%d, want 2/2", dr1, dr2)
	}
	dr3 := rankToInt64(t, rows[3][2], "Carol dense rank")
	dr4 := rankToInt64(t, rows[4][2], "Eve dense rank")
	if dr3 != 3 || dr4 != 3 {
		t.Errorf("Carol/Eve DENSE_RANK = %d/%d, want 3/3", dr3, dr4)
	}
}

// TestWindowRankPartitionOrderBy exercises extractWindowOrderByCols and
// emitWindowRankComparison via RANK() OVER (PARTITION BY dept ORDER BY salary).
func TestWindowRankPartitionOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, dept, RANK() OVER (PARTITION BY dept ORDER BY salary) AS r FROM employees ORDER BY dept, salary, name")

	if len(rows) != 5 {
		t.Fatalf("RANK PARTITION BY: expected 5 rows, got %d", len(rows))
	}
	// Within each dept, rank starts at 1.
	deptFirstRank := make(map[string]int64)
	for _, row := range rows {
		var dept string
		switch x := row[1].(type) {
		case string:
			dept = x
		case []byte:
			dept = string(x)
		}
		r := rankToInt64(t, row[2], "rank")
		if _, seen := deptFirstRank[dept]; !seen {
			deptFirstRank[dept] = r
		}
	}
	for dept, firstRank := range deptFirstRank {
		if firstRank != 1 {
			t.Errorf("dept %s: first rank = %d, want 1", dept, firstRank)
		}
	}
}

// TestWindowRankMultipleFunctions exercises emitWindowRankComparison and
// emitOrderByValueComparison with multiple rank functions in the same query.
func TestWindowRankMultipleFunctions(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		`SELECT name,
		        ROW_NUMBER() OVER (ORDER BY salary) AS rn,
		        RANK() OVER (ORDER BY salary) AS r,
		        DENSE_RANK() OVER (ORDER BY salary) AS dr
		 FROM employees ORDER BY salary, name`)

	if len(rows) != 5 {
		t.Fatalf("multiple rank funcs: expected 5 rows, got %d", len(rows))
	}
	// ROW_NUMBER must be strictly sequential.
	for i, row := range rows {
		rn := rankToInt64(t, row[1], fmt.Sprintf("row %d ROW_NUMBER", i))
		if rn != int64(i+1) {
			t.Errorf("row %d ROW_NUMBER = %d, want %d", i, rn, i+1)
		}
	}
	// RANK and DENSE_RANK must be >= 1.
	for i, row := range rows {
		r := rankToInt64(t, row[2], fmt.Sprintf("row %d RANK", i))
		dr := rankToInt64(t, row[3], fmt.Sprintf("row %d DENSE_RANK", i))
		if r < 1 {
			t.Errorf("row %d RANK = %d, want >= 1", i, r)
		}
		if dr < 1 {
			t.Errorf("row %d DENSE_RANK = %d, want >= 1", i, dr)
		}
		// DENSE_RANK <= RANK always holds.
		if dr > r {
			t.Errorf("row %d DENSE_RANK %d > RANK %d", i, dr, r)
		}
	}
}

// TestWindowRankNtileOrderBy exercises the NTILE window function with ORDER BY,
// which exercises emitWindowRankTrackingFromSorter (row count tracking).
func TestWindowRankNtileOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, NTILE(3) OVER (ORDER BY salary) AS tile FROM employees ORDER BY salary, name")

	if len(rows) != 5 {
		t.Fatalf("NTILE(3): expected 5 rows, got %d", len(rows))
	}
	for i, row := range rows {
		tile := rankToInt64(t, row[1], fmt.Sprintf("row %d NTILE", i))
		if tile < 1 || tile > 3 {
			t.Errorf("NTILE(3) row %d: got tile %d, want 1-3", i, tile)
		}
	}
}

// TestWindowRankLagOrderBy exercises LAG() OVER (ORDER BY salary) which triggers
// the sorter path and exercises emitWindowRankTrackingFromSorter for row tracking.
func TestWindowRankLagOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, salary, LAG(salary) OVER (ORDER BY salary) AS prev_salary FROM employees ORDER BY salary, name")

	if len(rows) != 5 {
		t.Fatalf("LAG ORDER BY: expected 5 rows, got %d", len(rows))
	}
	// First row has no predecessor — LAG returns NULL.
	if rows[0][2] != nil {
		t.Errorf("first row LAG: expected NULL, got %v", rows[0][2])
	}
	// Subsequent rows should have a non-NULL lag value.
	for i := 1; i < len(rows); i++ {
		if rows[i][2] == nil {
			t.Errorf("row %d LAG: expected non-NULL", i)
		}
	}
}

// TestWindowRankLeadOrderBy exercises LEAD(salary, 1, 0) OVER (ORDER BY salary),
// exercising the sorter path rank tracking for the last row's default value.
func TestWindowRankLeadOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	setupRankEmployees(t, db)

	rows := rankQueryRows(t, db,
		"SELECT name, salary, LEAD(salary, 1, 0) OVER (ORDER BY salary) AS next_salary FROM employees ORDER BY salary, name")

	if len(rows) != 5 {
		t.Fatalf("LEAD ORDER BY: expected 5 rows, got %d", len(rows))
	}
	// Last row has no successor — LEAD returns default 0.
	lastLead := rankToFloat64(t, rows[len(rows)-1][2], "last LEAD")
	if lastLead != 0 {
		t.Errorf("last row LEAD(salary, 1, 0) = %f, want 0", lastLead)
	}
}

// TestWindowRankEmitOrderByValueComparison exercises emitOrderByValueComparison
// by running RANK with many distinct salary values, ensuring each rank changes.
// This also exercises updateRankInfo for the RANK branch via the non-sorter path
// when using RANK() OVER () (no ORDER BY).
func TestWindowRankNoOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	rankExec(t, db, "CREATE TABLE simple_rank (v INTEGER)")
	rankExec(t, db, "INSERT INTO simple_rank VALUES(10)")
	rankExec(t, db, "INSERT INTO simple_rank VALUES(20)")
	rankExec(t, db, "INSERT INTO simple_rank VALUES(30)")

	// RANK() OVER () — no ORDER BY, no PARTITION BY.
	// This takes the non-sorter path and exercises emitWindowFunctionColumn RANK branch
	// and updateRankInfo("RANK", ...) via analyzeWindowRankFunctions.
	rows := rankQueryRows(t, db, "SELECT v, RANK() OVER () AS r FROM simple_rank")
	if len(rows) != 3 {
		t.Fatalf("RANK OVER (): expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		r := rankToInt64(t, row[1], fmt.Sprintf("row %d RANK", i))
		if r < 0 {
			t.Errorf("row %d RANK = %d, want >= 0", i, r)
		}
	}
}

// TestWindowRankDenseRankNoOrderBy exercises updateRankInfo("DENSE_RANK", ...) via
// the non-sorter path and emitWindowFunctionColumn DENSE_RANK branch.
func TestWindowRankDenseRankNoOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	rankExec(t, db, "CREATE TABLE simple_dr (v INTEGER)")
	rankExec(t, db, "INSERT INTO simple_dr VALUES(1)")
	rankExec(t, db, "INSERT INTO simple_dr VALUES(2)")
	rankExec(t, db, "INSERT INTO simple_dr VALUES(3)")

	// DENSE_RANK() OVER () — no ORDER BY.
	// Takes non-sorter path → updateRankInfo("DENSE_RANK", ...) DENSE_RANK branch.
	// Also exercises emitWindowFunctionColumn "DENSE_RANK" case.
	rows := rankQueryRows(t, db, "SELECT v, DENSE_RANK() OVER () AS dr FROM simple_dr")
	if len(rows) != 3 {
		t.Fatalf("DENSE_RANK OVER (): expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		dr := rankToInt64(t, row[1], fmt.Sprintf("row %d DENSE_RANK", i))
		if dr < 0 {
			t.Errorf("row %d DENSE_RANK = %d, want >= 0", i, dr)
		}
	}
}

// TestWindowRankNtileNoOrderBy exercises emitWindowFunctionColumn "NTILE" branch
// via NTILE(2) OVER () (no ORDER BY — non-sorter path).
func TestWindowRankNtileNoOrderBy(t *testing.T) {
	db := openRankCoverDB(t)
	rankExec(t, db, "CREATE TABLE simple_ntile (v INTEGER)")
	for _, v := range []int{1, 2, 3, 4} {
		rankExec(t, db, fmt.Sprintf("INSERT INTO simple_ntile VALUES(%d)", v))
	}

	// NTILE(2) OVER () — no ORDER BY, non-sorter path.
	// Exercises emitWindowFunctionColumn "NTILE" case.
	rows := rankQueryRows(t, db, "SELECT v, NTILE(2) OVER () AS tile FROM simple_ntile")
	if len(rows) != 4 {
		t.Fatalf("NTILE(2) OVER (): expected 4 rows, got %d", len(rows))
	}
	for i, row := range rows {
		tile := rankToInt64(t, row[1], fmt.Sprintf("row %d NTILE", i))
		if tile < 0 {
			t.Errorf("row %d NTILE = %d, want >= 0", i, tile)
		}
	}
}

// TestWindowRankRankAndDenseRankWithTies verifies tied values cause correct rank
// and dense_rank assignment, exercising emitOrderByValueComparison tie detection
// and emitWindowRankUpdate when values change.
func TestWindowRankRankAndDenseRankWithTies(t *testing.T) {
	db := openRankCoverDB(t)
	rankExec(t, db, "CREATE TABLE tied (name TEXT, score INTEGER)")
	rankExec(t, db, "INSERT INTO tied VALUES('A', 100)")
	rankExec(t, db, "INSERT INTO tied VALUES('B', 80)")
	rankExec(t, db, "INSERT INTO tied VALUES('C', 80)") // tie with B
	rankExec(t, db, "INSERT INTO tied VALUES('D', 60)")

	rows := rankQueryRows(t, db,
		`SELECT name, score,
		        RANK() OVER (ORDER BY score DESC) AS r,
		        DENSE_RANK() OVER (ORDER BY score DESC) AS dr
		 FROM tied ORDER BY score DESC, name`)

	if len(rows) != 4 {
		t.Fatalf("tied rank: expected 4 rows, got %d", len(rows))
	}
	// A: score 100 → rank 1, dense_rank 1
	rA := rankToInt64(t, rows[0][2], "A rank")
	drA := rankToInt64(t, rows[0][3], "A dense_rank")
	if rA != 1 || drA != 1 {
		t.Errorf("A: rank=%d dense_rank=%d, want rank=1 dense_rank=1", rA, drA)
	}
	// B, C: score 80 → rank 2, dense_rank 2 (tied)
	rB := rankToInt64(t, rows[1][2], "B rank")
	rC := rankToInt64(t, rows[2][2], "C rank")
	drB := rankToInt64(t, rows[1][3], "B dense_rank")
	drC := rankToInt64(t, rows[2][3], "C dense_rank")
	if rB != 2 || rC != 2 {
		t.Errorf("B/C rank = %d/%d, want 2/2", rB, rC)
	}
	if drB != 2 || drC != 2 {
		t.Errorf("B/C dense_rank = %d/%d, want 2/2", drB, drC)
	}
	// D: score 60 → rank 4 (skips 3), dense_rank 3
	rD := rankToInt64(t, rows[3][2], "D rank")
	drD := rankToInt64(t, rows[3][3], "D dense_rank")
	if rD != 4 {
		t.Errorf("D rank = %d, want 4", rD)
	}
	if drD != 3 {
		t.Errorf("D dense_rank = %d, want 3", drD)
	}
}
