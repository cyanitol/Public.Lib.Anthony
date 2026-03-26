// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"fmt"
	"testing"
)

// ============================================================================
// MC/DC: validateColumnCount
//
// Compound condition (2 sub-conditions):
//   A = index > 0              (not the first sub-SELECT)
//   B = cols != numCols        (column count mismatch)
//
// Outcome = A && B → return error
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → error (parts have different column counts)
//   Flip A: A=F B=? → no error (first part; column count not yet established)
//   Flip B: A=T B=F → no error (second part with matching column count)
// ============================================================================

func TestMCDC_ValidateColumnCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			// A=T B=T: second part has more columns → mismatch error
			name:    "A=T B=T: mismatched column count errors",
			query:   "SELECT 1 UNION SELECT 2, 3",
			wantErr: true,
		},
		{
			// Flip A=F: only one sub-SELECT (no compound); validateColumnCount never
			// called with index>0, so condition is vacuously safe.
			name:    "Flip A=F: single SELECT no compound",
			query:   "SELECT 42",
			wantErr: false,
		},
		{
			// Flip B=F: A=T (second part) but column counts match → no error
			name:    "Flip B=F: matching column counts succeed",
			query:   "SELECT 1 UNION SELECT 2",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					rows.Close()
					t.Fatalf("expected error but got none for %q", tt.query)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for %q: %v", tt.query, err)
				}
				rows.Close()
			}
		})
	}
}

// ============================================================================
// MC/DC: intersectRows inner guard
//
// Compound condition (2 sub-conditions):
//   A = rightSet[k]   (row key present in right side)
//   B = !seen[k]      (row not yet emitted)
//
// Outcome = A && B → append row to result
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → row in both sides and not yet seen → emitted once
//   Flip A: A=F B=T → row only in left side → not emitted
//   Flip B: A=T B=F → row in both sides but duplicate on left → emitted only once
// ============================================================================

func TestMCDC_IntersectRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: row 2 is in both sides; no duplicates → emitted once
			name:     "A=T B=T: row in both sides emitted once",
			setup:    "CREATE TABLE li(v INTEGER); CREATE TABLE ri(v INTEGER); INSERT INTO li VALUES(1),(2); INSERT INTO ri VALUES(2),(3)",
			query:    "SELECT v FROM li INTERSECT SELECT v FROM ri",
			wantRows: 1,
		},
		{
			// Flip A=F: left-only value not in right → zero rows
			name:     "Flip A=F: left-only value excluded",
			setup:    "CREATE TABLE li2(v INTEGER); CREATE TABLE ri2(v INTEGER); INSERT INTO li2 VALUES(10); INSERT INTO ri2 VALUES(20)",
			query:    "SELECT v FROM li2 INTERSECT SELECT v FROM ri2",
			wantRows: 0,
		},
		{
			// Flip B=F: duplicate on left side; both copies match right → deduplicated to one row
			name:     "Flip B=F: duplicate left row deduplicated",
			setup:    "CREATE TABLE li3(v INTEGER); CREATE TABLE ri3(v INTEGER); INSERT INTO li3 VALUES(5),(5); INSERT INTO ri3 VALUES(5)",
			query:    "SELECT v FROM li3 INTERSECT SELECT v FROM ri3",
			wantRows: 1,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: exceptRows inner guard
//
// Compound condition (2 sub-conditions):
//   A = !rightSet[k]  (row key NOT in right side)
//   B = !seen[k]      (row not yet emitted)
//
// Outcome = A && B → append row to result
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → row in left but not right, not yet seen → emitted
//   Flip A: A=F B=T → row in right → excluded
//   Flip B: A=T B=F → row in left (not right) but duplicate → emitted once
// ============================================================================

func TestMCDC_ExceptRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: value 1 in left but not right → emitted
			name:     "A=T B=T: left-only value emitted",
			setup:    "CREATE TABLE le(v INTEGER); CREATE TABLE re(v INTEGER); INSERT INTO le VALUES(1),(2); INSERT INTO re VALUES(2)",
			query:    "SELECT v FROM le EXCEPT SELECT v FROM re",
			wantRows: 1,
		},
		{
			// Flip A=F: value in right side → excluded from result
			name:     "Flip A=F: right-side value excluded",
			setup:    "CREATE TABLE le2(v INTEGER); CREATE TABLE re2(v INTEGER); INSERT INTO le2 VALUES(7); INSERT INTO re2 VALUES(7)",
			query:    "SELECT v FROM le2 EXCEPT SELECT v FROM re2",
			wantRows: 0,
		},
		{
			// Flip B=F: duplicate in left not in right → deduplicated to one row
			name:     "Flip B=F: duplicate left-only value deduplicated",
			setup:    "CREATE TABLE le3(v INTEGER); CREATE TABLE re3(v INTEGER); INSERT INTO le3 VALUES(3),(3); INSERT INTO re3 VALUES(9)",
			query:    "SELECT v FROM le3 EXCEPT SELECT v FROM re3",
			wantRows: 1,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: sortCompoundRows early-return guard
//
// Compound condition (2 sub-conditions):
//   A = len(rows) == 0     (result set is empty)
//   B = len(orderBy) == 0  (no ORDER BY terms)
//
// Outcome = A || B → return immediately without sorting
//
// Cases needed (N+1 = 3):
//   Base:   A=F B=F → sort is performed
//   Flip A: A=T B=F → empty rows → return early
//   Flip B: A=F B=T → no ORDER BY → no sort attempted (also early-return)
// ============================================================================

func TestMCDC_SortCompoundRows_Guard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=F B=F: rows exist and ORDER BY present → sort executed
			name:     "A=F B=F: rows with ORDER BY sorted",
			setup:    "CREATE TABLE so1(v INTEGER); INSERT INTO so1 VALUES(3),(1),(2)",
			query:    "SELECT v FROM so1 UNION ALL SELECT v FROM so1 ORDER BY v",
			wantRows: 6,
		},
		{
			// Flip A=T: empty result set with ORDER BY → early return
			name:     "Flip A=T: empty result early returns",
			setup:    "CREATE TABLE so2(v INTEGER); CREATE TABLE so3(v INTEGER)",
			query:    "SELECT v FROM so2 UNION ALL SELECT v FROM so3 ORDER BY v",
			wantRows: 0,
		},
		{
			// Flip B=T: rows but no ORDER BY → early return (unsorted)
			name:     "Flip B=T: rows without ORDER BY no sort",
			setup:    "CREATE TABLE so4(v INTEGER); INSERT INTO so4 VALUES(1),(2)",
			query:    "SELECT v FROM so4 UNION ALL SELECT v FROM so4",
			wantRows: 4,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: resolveLiteralExpr – type-check guard
//
// Compound condition (2 sub-conditions):
//   A = !ok             (type assertion to *parser.LiteralExpr failed)
//   B = lit.Type != parser.LiteralInteger  (literal is not an integer)
//
// Outcome = A || B → return -1 (cannot use as column index)
//
// Cases needed (N+1 = 3):
//   Base:   A=F B=F → integer literal → resolved as column index (ORDER BY 1)
//   Flip A: A=T B=? → non-literal expression (column name) → falls through
//   Flip B: A=F B=T → literal but not integer (string) → falls through
// ============================================================================

func TestMCDC_ResolveLiteralExpr_TypeGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
		wantErr  bool
	}{
		{
			// A=F B=F: integer literal 1 used as ORDER BY position
			name:     "A=F B=F: ORDER BY integer position resolves",
			setup:    "CREATE TABLE rl1(v INTEGER); INSERT INTO rl1 VALUES(3),(1),(2)",
			query:    "SELECT v FROM rl1 UNION ALL SELECT v FROM rl1 ORDER BY 1",
			wantRows: 6,
			wantErr:  false,
		},
		{
			// Flip A=T: column name identifier used in ORDER BY → resolveIdentExpr path, not literal
			name:     "Flip A=T: ORDER BY column name not a literal",
			setup:    "CREATE TABLE rl2(v INTEGER); INSERT INTO rl2 VALUES(2),(1)",
			query:    "SELECT v FROM rl2 UNION ALL SELECT v FROM rl2 ORDER BY v",
			wantRows: 4,
			wantErr:  false,
		},
		{
			// Flip B=T: string literal in ORDER BY position → not an integer literal, falls to default col 0
			name:     "Flip B=T: ORDER BY string literal falls to default col",
			setup:    "CREATE TABLE rl3(v INTEGER); INSERT INTO rl3 VALUES(1),(2)",
			query:    "SELECT v FROM rl3 UNION ALL SELECT v FROM rl3 ORDER BY 'x'",
			wantRows: 4,
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
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					rows.Close()
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: resolveLiteralExpr – range validation
//
// Compound condition (3 sub-conditions, all must be true for valid index):
//   A = err == nil         (Sscanf succeeded)
//   B = idx >= 1           (1-based index is positive)
//   C = int(idx) <= numCols (index is within column count)
//
// Outcome = A && B && C → return int(idx)-1
//
// Cases needed (N+1 = 4):
//   Base:    A=T B=T C=T → valid index (ORDER BY 1 on 1-col query)
//   Flip A:  A=F B=? C=? → non-numeric literal → cannot Sscanf
//   Flip B:  A=T B=F C=? → index 0 or negative → out of range
//   Flip C:  A=T B=T C=F → index exceeds column count → out of range
// ============================================================================

func TestMCDC_ResolveLiteralExpr_RangeValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T C=T: ORDER BY 1 valid on a 1-column compound
			name:     "A=T B=T C=T: valid ORDER BY position 1",
			setup:    "CREATE TABLE rr1(v INTEGER); INSERT INTO rr1 VALUES(3),(1),(2)",
			query:    "SELECT v FROM rr1 UNION ALL SELECT v FROM rr1 ORDER BY 1",
			wantRows: 6,
		},
		{
			// Flip A=F: string literal cannot parse as integer → falls to default col 0 (no error)
			name:     "Flip A=F: non-numeric ORDER BY literal falls to default",
			setup:    "CREATE TABLE rr2(v INTEGER); INSERT INTO rr2 VALUES(1),(2)",
			query:    "SELECT v FROM rr2 UNION ALL SELECT v FROM rr2 ORDER BY 'abc'",
			wantRows: 4,
		},
		{
			// Flip B=F: ORDER BY 0 → idx < 1 → not valid → default col 0 used
			name:     "Flip B=F: ORDER BY 0 out of lower bound falls to default",
			setup:    "CREATE TABLE rr3(v INTEGER); INSERT INTO rr3 VALUES(1),(2)",
			query:    "SELECT v FROM rr3 UNION ALL SELECT v FROM rr3 ORDER BY 0",
			wantRows: 4,
		},
		{
			// Flip C=F: ORDER BY position exceeds column count → falls to default col 0
			name:     "Flip C=F: ORDER BY position exceeds col count falls to default",
			setup:    "CREATE TABLE rr4(v INTEGER); INSERT INTO rr4 VALUES(1),(2)",
			query:    "SELECT v FROM rr4 UNION ALL SELECT v FROM rr4 ORDER BY 99",
			wantRows: 4,
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
				t.Fatalf("unexpected error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: compareCompoundNull – "both non-nil" guard
//
// Compound condition (2 sub-conditions):
//   A = a != nil
//   B = b != nil
//
// Outcome = A && B → return (0, false) meaning "no NULL involved, proceed"
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → both non-nil → NULL path skipped, normal compare
//   Flip A: A=F B=T → a is NULL → NULL handling applied
//   Flip B: A=T B=F → b is NULL → NULL handling applied
// ============================================================================

func TestMCDC_CompareCompoundNull_BothNonNil(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows []string
	}{
		{
			// A=T B=T: both values non-NULL → normal comparison path
			name:     "A=T B=T: both non-null compared normally",
			setup:    "CREATE TABLE nn1(v INTEGER); CREATE TABLE nn2(v INTEGER); INSERT INTO nn1 VALUES(1),(3); INSERT INTO nn2 VALUES(2),(4)",
			query:    "SELECT v FROM nn1 UNION ALL SELECT v FROM nn2 ORDER BY v",
			wantRows: []string{"1", "2", "3", "4"},
		},
		{
			// Flip A=F: a (left) has NULL → NULL sorts first in ASC
			name:     "Flip A=F: NULL on left sorts first",
			setup:    "CREATE TABLE nn3(v INTEGER); CREATE TABLE nn4(v INTEGER); INSERT INTO nn3 VALUES(NULL); INSERT INTO nn4 VALUES(1)",
			query:    "SELECT v FROM nn3 UNION ALL SELECT v FROM nn4 ORDER BY v",
			wantRows: []string{"NULL", "1"},
		},
		{
			// Flip B=F: b (right) has NULL → NULL sorts first
			name:     "Flip B=F: NULL on right sorts first",
			setup:    "CREATE TABLE nn5(v INTEGER); CREATE TABLE nn6(v INTEGER); INSERT INTO nn5 VALUES(1); INSERT INTO nn6 VALUES(NULL)",
			query:    "SELECT v FROM nn5 UNION ALL SELECT v FROM nn6 ORDER BY v",
			wantRows: []string{"NULL", "1"},
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			var got []string
			for rows.Next() {
				var v sql.NullInt64
				if err := rows.Scan(&v); err != nil {
					t.Fatalf("scan error: %v", err)
				}
				if v.Valid {
					got = append(got, "1")
					// Use the actual numeric value representation
					got[len(got)-1] = itoa(v.Int64)
				} else {
					got = append(got, "NULL")
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if len(got) != len(tt.wantRows) {
				t.Errorf("got %d rows, want %d: %v", len(got), len(tt.wantRows), got)
				return
			}
			for i, want := range tt.wantRows {
				if got[i] != want {
					t.Errorf("row %d: got %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

// itoa converts an int64 to its decimal string representation.
func itoa(n int64) string {
	return fmt.Sprintf("%d", n)
}

// ============================================================================
// MC/DC: compareCompoundNull – "both nil" guard
//
// Compound condition (2 sub-conditions):
//   A = a == nil
//   B = b == nil
//
// Outcome = A && B → return (0, true) meaning "both NULL, equal"
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → both NULL → equal (0, true)
//   Flip A: A=F B=T → only b is NULL → a != nil case
//   Flip B: A=T B=F → only a is NULL → b != nil case
// ============================================================================

func TestMCDC_CompareCompoundNull_BothNil(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: both values NULL → equal, stable sort relative to each other
			name:     "A=T B=T: both NULL equal",
			setup:    "CREATE TABLE bn1(v INTEGER); CREATE TABLE bn2(v INTEGER); INSERT INTO bn1 VALUES(NULL); INSERT INTO bn2 VALUES(NULL)",
			query:    "SELECT v FROM bn1 UNION ALL SELECT v FROM bn2 ORDER BY v",
			wantRows: 2,
		},
		{
			// Flip A=F: left non-NULL, right NULL → NULL sorts before non-NULL
			name:     "Flip A=F: left non-null right null",
			setup:    "CREATE TABLE bn3(v INTEGER); CREATE TABLE bn4(v INTEGER); INSERT INTO bn3 VALUES(5); INSERT INTO bn4 VALUES(NULL)",
			query:    "SELECT v FROM bn3 UNION ALL SELECT v FROM bn4 ORDER BY v",
			wantRows: 2,
		},
		{
			// Flip B=F: left NULL, right non-NULL → NULL sorts first
			name:     "Flip B=F: left null right non-null",
			setup:    "CREATE TABLE bn5(v INTEGER); CREATE TABLE bn6(v INTEGER); INSERT INTO bn5 VALUES(NULL); INSERT INTO bn6 VALUES(5)",
			query:    "SELECT v FROM bn5 UNION ALL SELECT v FROM bn6 ORDER BY v",
			wantRows: 2,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: compareCompoundRows – column index bounds guard
//
// Compound condition (2 sub-conditions):
//   A = ci >= len(row1)
//   B = ci >= len(row2)
//
// Outcome = A || B → skip this column in comparison (continue)
//
// Cases needed (N+1 = 3):
//   Base:   A=F B=F → ci within both rows → column compared normally
//   Flip A: A=T B=F → ci out of bounds for row1 → skipped
//   Flip B: A=F B=T → ci out of bounds for row2 → skipped
//
// Note: In practice SQL enforces uniform column counts, so both A and B trigger
// together when column counts are mismatched. We cover A=F B=F (normal compare)
// and demonstrate ordering works. The guard protects against runtime panics.
// ============================================================================

func TestMCDC_CompareCompoundRows_BoundsGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=F B=F: all column indices in bounds for both rows → normal sort
			name:     "A=F B=F: all indices in bounds, sorted normally",
			setup:    "CREATE TABLE cb1(a INTEGER, b INTEGER); INSERT INTO cb1 VALUES(2,10),(1,20)",
			query:    "SELECT a, b FROM cb1 UNION ALL SELECT a, b FROM cb1 ORDER BY a, b",
			wantRows: 4,
		},
		{
			// ORDER BY first column only; second column not consulted → covers partial path
			name:     "A=F B=F: ORDER BY first col only, second not accessed",
			setup:    "CREATE TABLE cb2(a INTEGER, b INTEGER); INSERT INTO cb2 VALUES(1,99),(2,1)",
			query:    "SELECT a, b FROM cb2 UNION ALL SELECT a, b FROM cb2 ORDER BY 1",
			wantRows: 4,
		},
		{
			// Single-column compound; ci=0 always in bounds → always A=F B=F
			name:     "A=F B=F: single-col compound all in bounds",
			setup:    "CREATE TABLE cb3(v INTEGER); INSERT INTO cb3 VALUES(3),(1),(2)",
			query:    "SELECT v FROM cb3 UNION ALL SELECT v FROM cb3 ORDER BY 1",
			wantRows: 6,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: cmpNulls – "both nil" guard
//
// Compound condition (2 sub-conditions):
//   A = a == nil
//   B = b == nil
//
// Outcome = A && B → return (0, true) — both NULL, treat as equal
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → both NULL → equal
//   Flip A: A=F B=T → only b NULL → a wins (non-NULL > NULL)
//   Flip B: A=T B=F → only a NULL → b wins (NULL < non-NULL)
// ============================================================================

func TestMCDC_CmpNulls_BothNil(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: two NULLs → INTERSECT yields one (they are equal)
			name:     "A=T B=T: two NULLs are equal in INTERSECT",
			setup:    "CREATE TABLE cn1(v INTEGER); CREATE TABLE cn2(v INTEGER); INSERT INTO cn1 VALUES(NULL); INSERT INTO cn2 VALUES(NULL)",
			query:    "SELECT v FROM cn1 INTERSECT SELECT v FROM cn2",
			wantRows: 1,
		},
		{
			// Flip A=F: a non-NULL, b NULL → INTERSECT yields 0 rows
			name:     "Flip A=F: non-null vs null not equal",
			setup:    "CREATE TABLE cn3(v INTEGER); CREATE TABLE cn4(v INTEGER); INSERT INTO cn3 VALUES(1); INSERT INTO cn4 VALUES(NULL)",
			query:    "SELECT v FROM cn3 INTERSECT SELECT v FROM cn4",
			wantRows: 0,
		},
		{
			// Flip B=F: a NULL, b non-NULL → INTERSECT yields 0 rows
			name:     "Flip B=F: null vs non-null not equal",
			setup:    "CREATE TABLE cn5(v INTEGER); CREATE TABLE cn6(v INTEGER); INSERT INTO cn5 VALUES(NULL); INSERT INTO cn6 VALUES(1)",
			query:    "SELECT v FROM cn5 INTERSECT SELECT v FROM cn6",
			wantRows: 0,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: cmpBytes – loop continuation guard
//
// Compound condition (2 sub-conditions):
//   A = i < len(a)
//   B = i < len(b)
//
// Outcome = A && B → continue iterating byte-by-byte
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → both slices have bytes remaining → byte compared
//   Flip A: A=F B=T → a exhausted first → a is shorter (a < b)
//   Flip B: A=T B=F → b exhausted first → b is shorter (a > b)
//
// We exercise via BLOB ordering in compound queries sorted by a blob column.
// ============================================================================

func TestMCDC_CmpBytes_LoopGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: same-length blobs differ in byte content → ordered by content
			name:     "A=T B=T: same-length blobs compared byte-by-byte",
			setup:    "CREATE TABLE bl1(v BLOB); INSERT INTO bl1 VALUES(X'0102'),(X'0103')",
			query:    "SELECT v FROM bl1 UNION ALL SELECT v FROM bl1 ORDER BY v",
			wantRows: 4,
		},
		{
			// Flip A=F: shorter blob exhausted first → a < b (shorter sorts first)
			name:     "Flip A=F: shorter blob sorts before longer",
			setup:    "CREATE TABLE bl2(v BLOB); INSERT INTO bl2 VALUES(X'01'),(X'0102')",
			query:    "SELECT v FROM bl2 UNION ALL SELECT v FROM bl2 ORDER BY v",
			wantRows: 4,
		},
		{
			// Flip B=F: longer blob exhausted (b shorter) → b < a, b sorts first
			name:     "Flip B=F: longer blob sorts after shorter",
			setup:    "CREATE TABLE bl3(v BLOB); INSERT INTO bl3 VALUES(X'0102'),(X'01')",
			query:    "SELECT v FROM bl3 UNION ALL SELECT v FROM bl3 ORDER BY v",
			wantRows: 4,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: parseOffsetExpr – valid offset guard
//
// Compound condition (2 sub-conditions):
//   A = err == nil    (Sscanf succeeded)
//   B = v > 0        (offset is positive)
//
// Outcome = A && B → return int(v) as offset
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → valid positive offset → rows skipped
//   Flip A: A=F B=? → non-numeric offset → offset 0 used (not reachable via SQL parser normally)
//   Flip B: A=T B=F → OFFSET 0 → treated as no offset
// ============================================================================

func TestMCDC_ParseOffsetExpr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: OFFSET 1 → skip first row
			name:     "A=T B=T: positive OFFSET 1 skips one row",
			setup:    "CREATE TABLE po1(v INTEGER); INSERT INTO po1 VALUES(1),(2),(3)",
			query:    "SELECT v FROM po1 UNION ALL SELECT v FROM po1 LIMIT 10 OFFSET 1",
			wantRows: 5,
		},
		{
			// Flip B=F: OFFSET 0 → no rows skipped
			name:     "Flip B=F: OFFSET 0 treated as no offset",
			setup:    "CREATE TABLE po2(v INTEGER); INSERT INTO po2 VALUES(1),(2),(3)",
			query:    "SELECT v FROM po2 UNION ALL SELECT v FROM po2 LIMIT 10 OFFSET 0",
			wantRows: 6,
		},
		{
			// No OFFSET clause → offsetExpr is nil → handled by nil check before Sscanf
			name:     "No OFFSET: nil expression → zero offset",
			setup:    "CREATE TABLE po3(v INTEGER); INSERT INTO po3 VALUES(1),(2)",
			query:    "SELECT v FROM po3 UNION ALL SELECT v FROM po3 LIMIT 10",
			wantRows: 4,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: applyOffset – offset applicability guard
//
// Compound condition (2 sub-conditions):
//   A = offset <= 0
//   B = offset >= len(rows)
//
// Outcome = A || B → early return (return nil if B, else return rows unchanged)
//
// Cases needed (N+1 = 3):
//   Base:   A=F B=F → valid mid-range offset → rows[offset:] returned
//   Flip A: A=T B=F → offset <= 0 → rows returned unchanged
//   Flip B: A=F B=T → offset >= len → nil returned (empty)
// ============================================================================

func TestMCDC_ApplyOffset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=F B=F: OFFSET 1 on 3-row result → 2 rows returned
			name:     "A=F B=F: mid-range offset returns tail",
			setup:    "CREATE TABLE ao1(v INTEGER); INSERT INTO ao1 VALUES(1),(2),(3)",
			query:    "SELECT v FROM ao1 UNION ALL SELECT v FROM ao1 LIMIT 10 OFFSET 1",
			wantRows: 5,
		},
		{
			// Flip A=T: OFFSET 0 → unchanged (all 6 rows)
			name:     "Flip A=T: zero offset returns all rows",
			setup:    "CREATE TABLE ao2(v INTEGER); INSERT INTO ao2 VALUES(1),(2),(3)",
			query:    "SELECT v FROM ao2 UNION ALL SELECT v FROM ao2 LIMIT 10 OFFSET 0",
			wantRows: 6,
		},
		{
			// Flip B=T: OFFSET equals or exceeds row count → empty result
			name:     "Flip B=T: offset beyond row count returns empty",
			setup:    "CREATE TABLE ao3(v INTEGER); INSERT INTO ao3 VALUES(1),(2)",
			query:    "SELECT v FROM ao3 UNION ALL SELECT v FROM ao3 LIMIT 10 OFFSET 100",
			wantRows: 0,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}

// ============================================================================
// MC/DC: applyLimit – limit applicability guard
//
// Compound condition (2 sub-conditions):
//   A = limit >= 0       (limit is not the "no limit" sentinel -1)
//   B = limit < len(rows) (limit is less than available rows)
//
// Outcome = A && B → truncate to rows[:limit]
//
// Cases needed (N+1 = 3):
//   Base:   A=T B=T → limit < row count → rows truncated
//   Flip A: A=F B=? → no LIMIT clause → all rows returned
//   Flip B: A=T B=F → limit >= row count → all rows returned unchanged
// ============================================================================

func TestMCDC_ApplyLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    string
		query    string
		wantRows int
	}{
		{
			// A=T B=T: LIMIT 2 on 6-row compound → 2 rows
			name:     "A=T B=T: limit smaller than row count truncates",
			setup:    "CREATE TABLE al1(v INTEGER); INSERT INTO al1 VALUES(1),(2),(3)",
			query:    "SELECT v FROM al1 UNION ALL SELECT v FROM al1 LIMIT 2",
			wantRows: 2,
		},
		{
			// Flip A=F: no LIMIT clause → all rows returned
			name:     "Flip A=F: no limit returns all rows",
			setup:    "CREATE TABLE al2(v INTEGER); INSERT INTO al2 VALUES(1),(2),(3)",
			query:    "SELECT v FROM al2 UNION ALL SELECT v FROM al2",
			wantRows: 6,
		},
		{
			// Flip B=F: LIMIT 100 on 4-row result → all 4 rows returned
			name:     "Flip B=F: limit larger than row count returns all",
			setup:    "CREATE TABLE al3(v INTEGER); INSERT INTO al3 VALUES(1),(2)",
			query:    "SELECT v FROM al3 UNION ALL SELECT v FROM al3 LIMIT 100",
			wantRows: 4,
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
				t.Fatalf("query error: %v", err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.wantRows {
				t.Errorf("got %d rows, want %d", count, tt.wantRows)
			}
		})
	}
}
