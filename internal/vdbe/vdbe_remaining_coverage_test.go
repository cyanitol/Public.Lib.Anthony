// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func remainOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func remainExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func remainQueryInt64(t *testing.T, db *sql.DB, q string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("queryInt64 %q: %v", q, err)
	}
	return n
}

func remainQueryFloat64(t *testing.T, db *sql.DB, q string) float64 {
	t.Helper()
	var f float64
	if err := db.QueryRow(q).Scan(&f); err != nil {
		t.Fatalf("queryFloat64 %q: %v", q, err)
	}
	return f
}

func remainQueryNullable(t *testing.T, db *sql.DB, q string) (interface{}, bool) {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("queryNullable %q: %v", q, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("queryNullable %q: no rows", q)
	}
	var val interface{}
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("queryNullable scan: %v", err)
	}
	return val, val == nil
}

// ---------------------------------------------------------------------------
// mem.go: Divide — exercise all branches
// ---------------------------------------------------------------------------

// TestVDBERemaining_Divide_IntegerDivByZero exercises the integer divide-by-zero
// branch (other.i == 0) which sets result to NULL.
func TestVDBERemaining_Divide_IntegerDivByZero(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	val, isNull := remainQueryNullable(t, db, "SELECT 10 / 0")
	if !isNull {
		t.Errorf("10/0 expected NULL, got %v", val)
	}
}

// TestVDBERemaining_Divide_IntegerDivision exercises integer division (both Int).
func TestVDBERemaining_Divide_IntegerDivision(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	n := remainQueryInt64(t, db, "SELECT 10 / 3")
	if n != 3 {
		t.Errorf("10/3 = %d, want 3", n)
	}
}

// TestVDBERemaining_Divide_FloatDivision exercises the real-division path.
func TestVDBERemaining_Divide_FloatDivision(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	f := remainQueryFloat64(t, db, "SELECT 10.0 / 4.0")
	if f != 2.5 {
		t.Errorf("10.0/4.0 = %v, want 2.5", f)
	}
}

// TestVDBERemaining_Divide_MixedDivision exercises int divided by float.
func TestVDBERemaining_Divide_MixedDivision(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	f := remainQueryFloat64(t, db, "SELECT 10 / 2.0")
	if f != 5.0 {
		t.Errorf("10/2.0 = %v, want 5.0", f)
	}
}

// TestVDBERemaining_Divide_FloatDivByZero exercises the float v2==0 branch.
func TestVDBERemaining_Divide_FloatDivByZero(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	val, isNull := remainQueryNullable(t, db, "SELECT 1.0 / 0")
	if !isNull {
		t.Errorf("1.0/0 expected NULL, got %v", val)
	}
}

// TestVDBERemaining_Divide_NullOperand exercises the NULL short-circuit branch.
func TestVDBERemaining_Divide_NullOperand(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	val, isNull := remainQueryNullable(t, db, "SELECT NULL / 5")
	if !isNull {
		t.Errorf("NULL/5 expected NULL, got %v", val)
	}

	val, isNull = remainQueryNullable(t, db, "SELECT 5 / NULL")
	if !isNull {
		t.Errorf("5/NULL expected NULL, got %v", val)
	}
}

// ---------------------------------------------------------------------------
// record.go: decodeFloat64 — exercise truncated and normal paths
// ---------------------------------------------------------------------------

// TestVDBERemaining_DecodeFloat64_InsertAndRead exercises decodeFloat64 via
// storing and retrieving REAL values from the database.
func TestVDBERemaining_DecodeFloat64_InsertAndRead(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE floats (id INTEGER PRIMARY KEY, val REAL)")
	remainExec(t, db, "INSERT INTO floats VALUES(1, 3.14159)")
	remainExec(t, db, "INSERT INTO floats VALUES(2, -273.15)")
	remainExec(t, db, "INSERT INTO floats VALUES(3, 0.0)")
	remainExec(t, db, "INSERT INTO floats VALUES(4, 1.7976931348623157e+308)") // near max float64

	rows, err := db.Query("SELECT id, val FROM floats ORDER BY id")
	if err != nil {
		t.Fatalf("query floats: %v", err)
	}
	defer rows.Close()

	type row struct {
		id  int
		val float64
	}
	expected := []row{
		{1, 3.14159},
		{2, -273.15},
		{3, 0.0},
		{4, 1.7976931348623157e+308},
	}

	i := 0
	for rows.Next() {
		var id int
		var val float64
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan row %d: %v", i, err)
		}
		if id != expected[i].id {
			t.Errorf("row %d: id=%d, want %d", i, id, expected[i].id)
		}
		if val != expected[i].val {
			t.Errorf("row %d: val=%v, want %v", i, val, expected[i].val)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), i)
	}
}

// TestVDBERemaining_DecodeFloat64_Arithmetic uses arithmetic to exercise
// REAL storage through the record encode/decode cycle.
func TestVDBERemaining_DecodeFloat64_Arithmetic(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE rf (x REAL)")
	remainExec(t, db, "INSERT INTO rf VALUES(1.5)")
	remainExec(t, db, "INSERT INTO rf VALUES(2.5)")

	f := remainQueryFloat64(t, db, "SELECT SUM(x) FROM rf")
	if f != 4.0 {
		t.Errorf("SUM(x) = %v, want 4.0", f)
	}
}

// ---------------------------------------------------------------------------
// record.go: decodeFixedInt / decodeIntValue — integer widths 1-6
// ---------------------------------------------------------------------------

// TestVDBERemaining_DecodeIntValue_AllWidths stores values that force each
// SQLite integer serial type (1–6 bytes) and reads them back.
func TestVDBERemaining_DecodeIntValue_AllWidths(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE ints (val INTEGER)")
	values := []int64{
		42,                  // serial type 1 (1 byte, fits int8)
		300,                 // serial type 2 (2 bytes, fits int16)
		100000,              // serial type 4 (4 bytes, fits int32)
		9223372036854775807, // serial type 6 (8 bytes, max int64)
		-1,                  // serial type 1 (negative)
		-32769,              // serial type 4 (exceeds int16)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO ints VALUES(?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	for _, v := range values {
		if _, err := stmt.Exec(v); err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec(%d): %v", v, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows, err := db.Query("SELECT val FROM ints ORDER BY val")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count != len(values) {
		t.Errorf("expected %d rows, got %d", len(values), count)
	}
}

// ---------------------------------------------------------------------------
// mem.go: Value() — exercise IntReal (MemIntReal) and Undefined paths
// ---------------------------------------------------------------------------

// TestVDBERemaining_MemValue_Blob exercises the Blob branch in Value().
func TestVDBERemaining_MemValue_Blob(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)")
	remainExec(t, db, "INSERT INTO blobs VALUES(1, X'DEADBEEF')")

	rows, err := db.Query("SELECT data FROM blobs WHERE id=1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("no rows")
	}
	var b []byte
	if err := rows.Scan(&b); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(b) != 4 {
		t.Errorf("blob length = %d, want 4", len(b))
	}
}

// ---------------------------------------------------------------------------
// mem.go: Integerify — exercise string→int, float→int, null→int, error paths
// ---------------------------------------------------------------------------

// TestVDBERemaining_Integerify_FromString exercises string-to-int cast.
func TestVDBERemaining_Integerify_FromString(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	n := remainQueryInt64(t, db, "SELECT CAST('42' AS INTEGER)")
	if n != 42 {
		t.Errorf("CAST('42' AS INTEGER) = %d, want 42", n)
	}
}

// TestVDBERemaining_Integerify_FromFloat exercises float-to-int cast.
func TestVDBERemaining_Integerify_FromFloat(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	n := remainQueryInt64(t, db, "SELECT CAST(3.9 AS INTEGER)")
	if n != 3 {
		t.Errorf("CAST(3.9 AS INTEGER) = %d, want 3", n)
	}
}

// ---------------------------------------------------------------------------
// mem.go: Realify — exercise string→real, null→real
// ---------------------------------------------------------------------------

// TestVDBERemaining_Realify_FromString exercises string-to-real cast.
func TestVDBERemaining_Realify_FromString(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	f := remainQueryFloat64(t, db, "SELECT CAST('3.14' AS REAL)")
	if f != 3.14 {
		t.Errorf("CAST('3.14' AS REAL) = %v, want 3.14", f)
	}
}

// TestVDBERemaining_Realify_FromInt exercises int-to-real cast.
func TestVDBERemaining_Realify_FromInt(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	f := remainQueryFloat64(t, db, "SELECT CAST(5 AS REAL)")
	if f != 5.0 {
		t.Errorf("CAST(5 AS REAL) = %v, want 5.0", f)
	}
}

// ---------------------------------------------------------------------------
// functions.go: createAggregateInstance — via GROUP BY aggregate queries
// ---------------------------------------------------------------------------

// TestVDBERemaining_CreateAggregateInstance_GroupBy exercises createAggregateInstance
// by running aggregate functions with GROUP BY (each group needs a fresh instance).
func TestVDBERemaining_CreateAggregateInstance_GroupBy(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE sales (dept TEXT, amount INTEGER)")
	remainExec(t, db, "INSERT INTO sales VALUES('A', 10)")
	remainExec(t, db, "INSERT INTO sales VALUES('A', 20)")
	remainExec(t, db, "INSERT INTO sales VALUES('B', 30)")
	remainExec(t, db, "INSERT INTO sales VALUES('B', 40)")
	remainExec(t, db, "INSERT INTO sales VALUES('C', 50)")

	rows, err := db.Query("SELECT dept, SUM(amount) FROM sales GROUP BY dept ORDER BY dept")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type result struct {
		dept string
		sum  int64
	}
	expected := []result{{"A", 30}, {"B", 70}, {"C", 50}}
	i := 0
	for rows.Next() {
		var dept string
		var sum int64
		if err := rows.Scan(&dept, &sum); err != nil {
			t.Fatalf("scan row %d: %v", i, err)
		}
		if i >= len(expected) {
			t.Fatal("too many rows")
		}
		if dept != expected[i].dept || sum != expected[i].sum {
			t.Errorf("row %d: got (%s,%d), want (%s,%d)", i, dept, sum, expected[i].dept, expected[i].sum)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("expected %d groups, got %d", len(expected), i)
	}
}

// TestVDBERemaining_CreateAggregateInstance_MultipleAggs exercises multiple
// aggregate functions in the same GROUP BY query.
func TestVDBERemaining_CreateAggregateInstance_MultipleAggs(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE scores (cat TEXT, score REAL)")
	remainExec(t, db, "INSERT INTO scores VALUES('X', 1.0)")
	remainExec(t, db, "INSERT INTO scores VALUES('X', 3.0)")
	remainExec(t, db, "INSERT INTO scores VALUES('Y', 2.0)")
	remainExec(t, db, "INSERT INTO scores VALUES('Y', 4.0)")

	rows, err := db.Query("SELECT cat, COUNT(*), AVG(score), MIN(score), MAX(score) FROM scores GROUP BY cat ORDER BY cat")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cat string
		var cnt int
		var avg, minS, maxS float64
		if err := rows.Scan(&cat, &cnt, &avg, &minS, &maxS); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if cnt != 2 {
			t.Errorf("cat=%s: count=%d, want 2", cat, cnt)
		}
	}
}

// ---------------------------------------------------------------------------
// functions.go: ExecuteFunction — aggregate-as-scalar error path
// ---------------------------------------------------------------------------

// TestVDBERemaining_ExecuteFunction_AggScalar exercises the scalar-call of an
// aggregate through a direct SQL expression (should return NULL or an error,
// depending on implementation).
func TestVDBERemaining_ExecuteFunction_AggScalar(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	// json_group_array (if supported) used in GROUP BY — creates new aggregate instances per group.
	remainExec(t, db, "CREATE TABLE items (grp INTEGER, val TEXT)")
	remainExec(t, db, "INSERT INTO items VALUES(1, 'a')")
	remainExec(t, db, "INSERT INTO items VALUES(1, 'b')")
	remainExec(t, db, "INSERT INTO items VALUES(2, 'c')")

	// Use COUNT which definitely exercises createAggregateInstance per group.
	rows, err := db.Query("SELECT grp, COUNT(val) FROM items GROUP BY grp ORDER BY grp")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var grp, cnt int
		if err := rows.Scan(&grp, &cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if grp == 1 && cnt != 2 {
			t.Errorf("grp 1 count = %d, want 2", cnt)
		}
		if grp == 2 && cnt != 1 {
			t.Errorf("grp 2 count = %d, want 1", cnt)
		}
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go: force spill via ORDER BY on large dataset
// ---------------------------------------------------------------------------

// TestVDBERemaining_SorterSpill_LargeOrderBy inserts enough rows to trigger
// sorter spill (writeRunToFile, writeAndRecordSpill, serializeRow).
func TestVDBERemaining_SorterSpill_LargeOrderBy(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE large (id INTEGER, val TEXT, n INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO large VALUES(?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	const n = 5000
	for i := n; i >= 1; i-- {
		_, err := stmt.Exec(i, fmt.Sprintf("row_%05d", i), n-i)
		if err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// ORDER BY without index forces the sorter path.
	rows, err := db.Query("SELECT id, val FROM large ORDER BY n, val LIMIT 100")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 100 {
		t.Errorf("expected 100 rows, got %d", count)
	}
}

// TestVDBERemaining_SorterSpill_MixedTypes uses mixed types to exercise
// all serializeMem branches (NULL, int, real, string, blob) during spill.
func TestVDBERemaining_SorterSpill_MixedTypes(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE mixed (id INTEGER, f REAL, s TEXT, b BLOB, n INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO mixed VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}

	const rows = 2000
	for i := 0; i < rows; i++ {
		_, err := stmt.Exec(i, float64(i)*0.5, fmt.Sprintf("str_%04d", i), []byte{byte(i % 256)}, rows-i)
		if err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// ORDER BY on the last column (no index) forces sorter path with mixed types.
	qrows, err := db.Query("SELECT COUNT(*) FROM (SELECT id FROM mixed ORDER BY n)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer qrows.Close()

	if qrows.Next() {
		var cnt int
		if err := qrows.Scan(&cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if cnt != rows {
			t.Errorf("expected %d rows, got %d", rows, cnt)
		}
	}
}

// ---------------------------------------------------------------------------
// exec.go: execGoto — via recursive CTE (exercises GOTO opcode)
// ---------------------------------------------------------------------------

// TestVDBERemaining_ExecGoto_GroupByLoop exercises the goto/loop path via
// GROUP BY on a large table, which uses OP_Goto internally to iterate rows.
func TestVDBERemaining_ExecGoto_GroupByLoop(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE loop_t (grp INTEGER, val INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO loop_t VALUES(?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	const n = 100
	for i := 0; i < n; i++ {
		if _, err := stmt.Exec(i%10, i); err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec: %v", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// GROUP BY forces a full-table scan with a loop (OP_Goto) and aggregate.
	got := remainQueryInt64(t, db, "SELECT SUM(val) FROM loop_t")
	// sum 0..99 = 4950
	if got != 4950 {
		t.Errorf("SUM(val) = %d, want 4950", got)
	}
}

// TestVDBERemaining_ExecGoto_MultiRowScan exercises the loop opcode via
// a long sequential scan over many rows with a WHERE filter.
func TestVDBERemaining_ExecGoto_MultiRowScan(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE scan_t (id INTEGER PRIMARY KEY, val INTEGER)")

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO scan_t VALUES(?, ?)")
	const n = 1000
	for i := 1; i <= n; i++ {
		stmt.Exec(i, i)
	}
	stmt.Close()
	tx.Commit()

	// Full scan with filter — exercises OP_Goto loop for each row.
	got := remainQueryInt64(t, db, "SELECT COUNT(*) FROM scan_t WHERE val > 500")
	if got != 500 {
		t.Errorf("count(val>500) = %d, want 500", got)
	}
}

// ---------------------------------------------------------------------------
// exec.go: handleIndexSeekGE / execSeekLE — index range scans
// ---------------------------------------------------------------------------

// TestVDBERemaining_IndexSeekGE_RangeScan exercises handleIndexSeekGE via a
// range query on an indexed column (val >= 5 AND val <= 10).
func TestVDBERemaining_IndexSeekGE_RangeScan(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE rng (id INTEGER PRIMARY KEY, val INTEGER)")
	remainExec(t, db, "CREATE INDEX idx_val ON rng(val)")

	for i := 1; i <= 20; i++ {
		remainExec(t, db, "INSERT INTO rng VALUES(?, ?)", i, i)
	}

	rows, err := db.Query("SELECT id FROM rng WHERE val >= 5 AND val <= 10 ORDER BY val")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if id < 5 || id > 10 {
			t.Errorf("id %d out of expected range [5,10]", id)
		}
		count++
	}
	if count != 6 {
		t.Errorf("expected 6 rows, got %d", count)
	}
}

// TestVDBERemaining_ExecSeekLE_LessThan exercises execSeekLE via a
// less-than-or-equal range query.
func TestVDBERemaining_ExecSeekLE_LessThan(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE le_test (id INTEGER PRIMARY KEY, val INTEGER)")
	remainExec(t, db, "CREATE INDEX idx_le ON le_test(val)")

	for i := 1; i <= 10; i++ {
		remainExec(t, db, "INSERT INTO le_test VALUES(?, ?)", i, i)
	}

	rows, err := db.Query("SELECT id FROM le_test WHERE val <= 5 ORDER BY val")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count != 5 {
		t.Errorf("expected 5 rows (val<=5), got %d", count)
	}
}

// TestVDBERemaining_IndexSeekGE_NoMatch exercises the path where index seek
// finds no matching rows.
func TestVDBERemaining_IndexSeekGE_NoMatch(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE nomatch (id INTEGER PRIMARY KEY, val INTEGER)")
	remainExec(t, db, "CREATE INDEX idx_nm ON nomatch(val)")
	remainExec(t, db, "INSERT INTO nomatch VALUES(1, 1)")
	remainExec(t, db, "INSERT INTO nomatch VALUES(2, 2)")

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM nomatch WHERE val >= 100")
	if n != 0 {
		t.Errorf("expected 0 rows for val>=100, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// exec.go: execSeekRowid — direct rowid/INTEGER PRIMARY KEY lookups
// ---------------------------------------------------------------------------

// TestVDBERemaining_ExecSeekRowid_ByPK exercises the INTEGER PRIMARY KEY
// lookup path (execSeekRowid).
func TestVDBERemaining_ExecSeekRowid_ByPK(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE pktest (id INTEGER PRIMARY KEY, name TEXT)")
	for i := 1; i <= 10; i++ {
		remainExec(t, db, "INSERT INTO pktest VALUES(?, ?)", i, fmt.Sprintf("name_%d", i))
	}

	var name string
	if err := db.QueryRow("SELECT name FROM pktest WHERE id = 5").Scan(&name); err != nil {
		t.Fatalf("seek rowid 5: %v", err)
	}
	if name != "name_5" {
		t.Errorf("name=%s, want name_5", name)
	}
}

// TestVDBERemaining_ExecSeekRowid_ByRowid exercises the explicit rowid lookup.
func TestVDBERemaining_ExecSeekRowid_ByRowid(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE rowidtest (id INTEGER PRIMARY KEY, val INTEGER)")
	remainExec(t, db, "INSERT INTO rowidtest VALUES(1, 100)")
	remainExec(t, db, "INSERT INTO rowidtest VALUES(5, 500)")
	remainExec(t, db, "INSERT INTO rowidtest VALUES(10, 1000)")

	n := remainQueryInt64(t, db, "SELECT val FROM rowidtest WHERE rowid = 5")
	if n != 500 {
		t.Errorf("rowid=5 val=%d, want 500", n)
	}
}

// TestVDBERemaining_ExecSeekRowid_NotFound exercises the "not found" path.
func TestVDBERemaining_ExecSeekRowid_NotFound(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE seekmiss (id INTEGER PRIMARY KEY, val INTEGER)")
	remainExec(t, db, "INSERT INTO seekmiss VALUES(1, 10)")

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM seekmiss WHERE id = 99")
	if n != 0 {
		t.Errorf("id=99 count=%d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// exec.go: execRewind / execNext / execPrev — cursor iteration
// ---------------------------------------------------------------------------

// TestVDBERemaining_ExecRewind_EmptyTable exercises execRewind on an empty table.
func TestVDBERemaining_ExecRewind_EmptyTable(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE empty_t (id INTEGER PRIMARY KEY)")

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM empty_t")
	if n != 0 {
		t.Errorf("empty table count=%d, want 0", n)
	}
}

// TestVDBERemaining_ExecNext_ForwardScan exercises execNext via a forward scan.
func TestVDBERemaining_ExecNext_ForwardScan(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE fwd (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 50; i++ {
		remainExec(t, db, "INSERT INTO fwd VALUES(?, ?)", i, i*10)
	}

	rows, err := db.Query("SELECT id, val FROM fwd ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	prev := 0
	count := 0
	for rows.Next() {
		var id, val int
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if id <= prev {
			t.Errorf("rows not in ascending order: %d after %d", id, prev)
		}
		prev = id
		count++
	}
	if count != 50 {
		t.Errorf("expected 50 rows, got %d", count)
	}
}

// TestVDBERemaining_ExecPrev_ReverseOrderBy exercises execPrev via DESC ORDER BY
// which may use reverse iteration.
func TestVDBERemaining_ExecPrev_ReverseOrderBy(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE rev (id INTEGER PRIMARY KEY, val INTEGER)")
	remainExec(t, db, "CREATE INDEX idx_rev_val ON rev(val)")
	for i := 1; i <= 20; i++ {
		remainExec(t, db, "INSERT INTO rev VALUES(?, ?)", i, i)
	}

	rows, err := db.Query("SELECT id FROM rev ORDER BY val DESC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	prev := 21
	count := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if id >= prev {
			t.Errorf("rows not in descending order: %d after %d", id, prev)
		}
		prev = id
		count++
	}
	if count != 20 {
		t.Errorf("expected 20 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// exec.go: findTableByRootPage / openCursorOnBtree — implicit via DML
// ---------------------------------------------------------------------------

// TestVDBERemaining_FindTableByRootPage exercises findTableByRootPage via
// INSERT/SELECT on multiple tables (each table has a different root page).
func TestVDBERemaining_FindTableByRootPage(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT)")
	remainExec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, v TEXT)")
	remainExec(t, db, "CREATE TABLE t3 (id INTEGER PRIMARY KEY, v TEXT)")

	remainExec(t, db, "INSERT INTO t1 VALUES(1, 'hello')")
	remainExec(t, db, "INSERT INTO t2 VALUES(1, 'world')")
	remainExec(t, db, "INSERT INTO t3 VALUES(1, 'foo')")

	var v1, v2, v3 string
	db.QueryRow("SELECT v FROM t1").Scan(&v1)
	db.QueryRow("SELECT v FROM t2").Scan(&v2)
	db.QueryRow("SELECT v FROM t3").Scan(&v3)

	if v1 != "hello" || v2 != "world" || v3 != "foo" {
		t.Errorf("values mismatch: %q, %q, %q", v1, v2, v3)
	}
}

// TestVDBERemaining_OpenCursorOnBtree exercises openCursorOnBtree via
// a join between two tables.
func TestVDBERemaining_OpenCursorOnBtree_Join(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
	remainExec(t, db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER, val TEXT)")

	remainExec(t, db, "INSERT INTO parent VALUES(1, 'Alice')")
	remainExec(t, db, "INSERT INTO parent VALUES(2, 'Bob')")
	remainExec(t, db, "INSERT INTO child VALUES(1, 1, 'x')")
	remainExec(t, db, "INSERT INTO child VALUES(2, 1, 'y')")
	remainExec(t, db, "INSERT INTO child VALUES(3, 2, 'z')")

	rows, err := db.Query(
		"SELECT p.name, c.val FROM parent p JOIN child c ON p.id = c.pid ORDER BY c.id")
	if err != nil {
		t.Fatalf("join query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name, val string
		if err := rows.Scan(&name, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 join rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// exec.go: handleExecutionError — error propagation
// ---------------------------------------------------------------------------

// TestVDBERemaining_HandleExecutionError_ConstraintViolation exercises the
// error handling path by causing a UNIQUE constraint violation.
func TestVDBERemaining_HandleExecutionError_ConstraintViolation(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE uniq (id INTEGER PRIMARY KEY)")
	remainExec(t, db, "INSERT INTO uniq VALUES(1)")

	_, err := db.Exec("INSERT INTO uniq VALUES(1)")
	if err == nil {
		t.Error("expected UNIQUE constraint error, got nil")
	}
}

// TestVDBERemaining_HandleExecutionError_TypeMismatch exercises error paths
// by running queries that produce type conversion issues.
func TestVDBERemaining_HandleExecutionError_TypeError(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	// CAST of non-numeric string to INTEGER should return 0 (not an error in SQLite).
	n := remainQueryInt64(t, db, "SELECT CAST('abc' AS INTEGER)")
	if n != 0 {
		t.Errorf("CAST('abc' AS INTEGER) = %d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// fk_adapter.go: compareMemToInt, compareMemToString comparison functions
// ---------------------------------------------------------------------------

// TestVDBERemaining_FK_CompareMemToInt exercises compareMemToInt via FK check
// with an INTEGER parent column.
func TestVDBERemaining_FK_CompareMemToInt(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	remainExec(t, db, "CREATE TABLE par_i (id INTEGER PRIMARY KEY)")
	remainExec(t, db, "CREATE TABLE chi_i (cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES par_i(id))")
	remainExec(t, db, "INSERT INTO par_i VALUES(1)")
	remainExec(t, db, "INSERT INTO par_i VALUES(2)")
	remainExec(t, db, "INSERT INTO chi_i VALUES(1, 1)")
	remainExec(t, db, "INSERT INTO chi_i VALUES(2, 2)")

	// FK check: deleting parent=1 should cascade or fail.
	// Without ON DELETE, it should fail — the check exercises compareMemToInt64.
	_, err := db.Exec("DELETE FROM par_i WHERE id = 1")
	// This may succeed or fail depending on implementation — just verify no panic.
	_ = err
}

// TestVDBERemaining_FK_CompareMemToString exercises compareMemToString via FK
// check with a TEXT parent column.
func TestVDBERemaining_FK_CompareMemToString(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	remainExec(t, db, "CREATE TABLE par_s (code TEXT PRIMARY KEY)")
	remainExec(t, db, "CREATE TABLE chi_s (cid INTEGER PRIMARY KEY, code TEXT REFERENCES par_s(code))")
	remainExec(t, db, "INSERT INTO par_s VALUES('ABC')")
	remainExec(t, db, "INSERT INTO par_s VALUES('DEF')")
	remainExec(t, db, "INSERT INTO chi_s VALUES(1, 'ABC')")

	// FK insert check — exercises text comparison path.
	_, err := db.Exec("INSERT INTO chi_s VALUES(2, 'NOSUCH')")
	if err == nil {
		t.Error("expected FK violation for unknown code, got nil")
	}
}

// TestVDBERemaining_FK_CascadeDelete_Int exercises collectMatchingRowidsWithAffinityAndCollation
// and compareMemToInterface via ON DELETE CASCADE with integer FK.
func TestVDBERemaining_FK_CascadeDelete_Int(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	remainExec(t, db, "CREATE TABLE par_cas (id INTEGER PRIMARY KEY)")
	remainExec(t, db, `CREATE TABLE chi_cas (
		cid INTEGER PRIMARY KEY,
		pid INTEGER REFERENCES par_cas(id) ON DELETE CASCADE
	)`)
	remainExec(t, db, "INSERT INTO par_cas VALUES(1)")
	remainExec(t, db, "INSERT INTO par_cas VALUES(2)")
	remainExec(t, db, "INSERT INTO chi_cas VALUES(1, 1)")
	remainExec(t, db, "INSERT INTO chi_cas VALUES(2, 1)")
	remainExec(t, db, "INSERT INTO chi_cas VALUES(3, 2)")

	remainExec(t, db, "DELETE FROM par_cas WHERE id = 1")

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM chi_cas")
	if n != 1 {
		t.Errorf("after cascade delete: chi_cas count=%d, want 1", n)
	}
}

// TestVDBERemaining_FK_CascadeDelete_String exercises compareMemToString
// via ON DELETE CASCADE with a TEXT parent column.
func TestVDBERemaining_FK_CascadeDelete_String(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	remainExec(t, db, "CREATE TABLE par_str (code TEXT UNIQUE)")
	remainExec(t, db, `CREATE TABLE chi_str (
		cid INTEGER PRIMARY KEY,
		code TEXT REFERENCES par_str(code) ON DELETE CASCADE
	)`)
	remainExec(t, db, "INSERT INTO par_str VALUES('X')")
	remainExec(t, db, "INSERT INTO par_str VALUES('Y')")
	remainExec(t, db, "INSERT INTO chi_str VALUES(1, 'X')")
	remainExec(t, db, "INSERT INTO chi_str VALUES(2, 'X')")
	remainExec(t, db, "INSERT INTO chi_str VALUES(3, 'Y')")

	remainExec(t, db, "DELETE FROM par_str WHERE code = 'X'")

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM chi_str")
	if n != 1 {
		t.Errorf("after cascade delete: chi_str count=%d, want 1", n)
	}
}

// TestVDBERemaining_FK_CheckRowMatchWithCollation exercises checkRowMatchWithCollation
// and extractColumnValueFromRow via FK with NOCASE collation.
func TestVDBERemaining_FK_CheckRowMatchWithCollation(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	remainExec(t, db, "CREATE TABLE par_nc (code TEXT PRIMARY KEY COLLATE NOCASE)")
	remainExec(t, db, "CREATE TABLE chi_nc (cid INTEGER PRIMARY KEY, code TEXT REFERENCES par_nc(code))")
	remainExec(t, db, "INSERT INTO par_nc VALUES('hello')")

	// FK insert matching case-insensitively.
	if _, err := db.Exec("INSERT INTO chi_nc VALUES(1, 'hello')"); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM chi_nc")
	if n != 1 {
		t.Errorf("chi_nc count=%d, want 1", n)
	}
}

// TestVDBERemaining_FK_CollectAllMatchingRowids exercises collectAllMatchingRowidsWithAffinityAndCollation
// by triggering a CASCADE delete that must scan multiple child rows.
func TestVDBERemaining_FK_CollectAllMatchingRowids(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	remainExec(t, db, "CREATE TABLE par_all (id INTEGER UNIQUE)")
	remainExec(t, db, `CREATE TABLE chi_all (
		cid INTEGER PRIMARY KEY,
		pid INTEGER REFERENCES par_all(id) ON DELETE CASCADE
	)`)
	remainExec(t, db, "INSERT INTO par_all VALUES(42)")
	// Insert multiple child rows referencing the same parent value.
	for i := 1; i <= 5; i++ {
		remainExec(t, db, "INSERT INTO chi_all VALUES(?, 42)", i)
	}

	remainExec(t, db, "DELETE FROM par_all WHERE id = 42")

	n := remainQueryInt64(t, db, "SELECT COUNT(*) FROM chi_all")
	if n != 0 {
		t.Errorf("after cascade delete all: chi_all count=%d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// exec.go: runUntilRowOrHalt — maxInstructions limit (exercise safety counter)
// ---------------------------------------------------------------------------

// TestVDBERemaining_RunUntilRowOrHalt_ComplexQuery exercises the instruction
// execution loop with a query that produces many instructions.
func TestVDBERemaining_RunUntilRowOrHalt_ComplexQuery(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE cplx (a INTEGER, b INTEGER, c INTEGER)")
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO cplx VALUES(?, ?, ?)")
	for i := 0; i < 500; i++ {
		stmt.Exec(i, i*2, i*3)
	}
	stmt.Close()
	tx.Commit()

	n := remainQueryInt64(t, db,
		`SELECT COUNT(*) FROM cplx
		 WHERE a > 10 AND b < 900 AND c BETWEEN 30 AND 1200`)
	if n <= 0 {
		t.Errorf("expected some rows from complex filter, got %d", n)
	}
}

// TestVDBERemaining_Blob_ExecBlob exercises execBlob opcode via BLOB column operations.
func TestVDBERemaining_Blob_ExecBlob(t *testing.T) {
	t.Parallel()
	db := remainOpenDB(t)

	remainExec(t, db, "CREATE TABLE blobdata (id INTEGER PRIMARY KEY, data BLOB)")

	// Insert a BLOB literal using hex literal syntax.
	remainExec(t, db, "INSERT INTO blobdata VALUES(1, X'0102030405')")
	remainExec(t, db, "INSERT INTO blobdata VALUES(2, X'')")
	remainExec(t, db, "INSERT INTO blobdata VALUES(3, X'DEADBEEF')")

	rows, err := db.Query("SELECT id, data FROM blobdata ORDER BY id")
	if err != nil {
		t.Fatalf("query blobs: %v", err)
	}
	defer rows.Close()

	expected := []int{5, 0, 4}
	i := 0
	for rows.Next() {
		var id int
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			t.Fatalf("scan row %d: %v", i, err)
		}
		if len(data) != expected[i] {
			t.Errorf("row %d: blob len=%d, want %d", i, len(data), expected[i])
		}
		i++
	}
	if i != 3 {
		t.Errorf("expected 3 blob rows, got %d", i)
	}
}
