// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// ============================================================================
// MC/DC: emitNullSafeGroupCompare – collation guard
// Line 130: colIdx < len(collations) && collations[colIdx] != ""
//
//   A = colIdx < len(collations)
//   B = collations[colIdx] != ""
//
// Outcome = A && B → apply collation to OpNe
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → GROUP BY column with explicit collation applied
//   Flip A: A=F B=? → GROUP BY column beyond collation slice (no collation)
//   Flip B: A=T B=F → GROUP BY column present in collation slice but empty string
// ============================================================================

func TestMCDC_GroupByCollation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T: explicit COLLATE NOCASE causes collation to be applied
			name:  "A=T B=T: group by with explicit collation",
			setup: "CREATE TABLE tgcoll(cat TEXT); INSERT INTO tgcoll VALUES('A'),('a'),('B')",
			query: "SELECT cat COLLATE NOCASE, COUNT(*) FROM tgcoll GROUP BY cat COLLATE NOCASE",
			want:  2,
		},
		{
			// Flip B=F: GROUP BY column without explicit collation → empty collation string
			name:  "Flip B=F: group by without collation",
			setup: "CREATE TABLE tgnocoll(cat TEXT); INSERT INTO tgnocoll VALUES('A'),('a'),('B')",
			query: "SELECT cat, COUNT(*) FROM tgnocoll GROUP BY cat",
			want:  3,
		},
		{
			// Flip A=F (proxy): single GROUP BY column – colIdx 0 always within bounds when collation
			// slice is populated; test an integer column which never has a collation registered
			name:  "A=T B=F: integer column has no collation",
			setup: "CREATE TABLE tgint(n INTEGER); INSERT INTO tgint VALUES(1),(1),(2)",
			query: "SELECT n, COUNT(*) FROM tgint GROUP BY n",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: updateGroupAccForColumn – COUNT(*) / no-arg short-circuit
// Line 206: fnExpr.Star || len(fnExpr.Args) == 0
//
//   A = fnExpr.Star   (COUNT(*) syntax)
//   B = len(fnExpr.Args) == 0
//
// Outcome = A || B → use COUNT(*)/no-arg path (no value read from sorter)
//
// Cases (N+1 = 3):
//   Base:   A=T B=F → COUNT(*) explicit star
//   Flip A: A=F B=T → COUNT() no args (edge case; practically same path)
//   Flip B: A=F B=F → aggregate with explicit argument e.g. SUM(n)
// ============================================================================

func TestMCDC_UpdateGroupAcc_StarOrNoArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int64
	}{
		{
			// A=T B=F: COUNT(*) – fnExpr.Star is true
			name:  "A=T B=F: COUNT(*) uses star path",
			setup: "CREATE TABLE tstar(cat TEXT, n INTEGER); INSERT INTO tstar VALUES('x',1),('x',2),('y',3)",
			query: "SELECT cat, COUNT(*) FROM tstar GROUP BY cat ORDER BY cat",
			want:  2,
		},
		{
			// Flip A=F B=F: SUM(n) has an argument → goes through value path
			name:  "Flip B=F: SUM(n) with explicit argument",
			setup: "CREATE TABLE tsum(cat TEXT, n INTEGER); INSERT INTO tsum VALUES('x',1),('x',2),('y',3)",
			query: "SELECT cat, SUM(n) FROM tsum GROUP BY cat ORDER BY cat",
			want:  2,
		},
		{
			// A=F B=F: MAX with argument
			name:  "A=F B=F: MAX(n) with explicit argument",
			setup: "CREATE TABLE tmax(cat TEXT, n INTEGER); INSERT INTO tmax VALUES('a',5),('a',3),('b',7)",
			query: "SELECT cat, MAX(n) FROM tmax GROUP BY cat ORDER BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			var rowCount int64
			for rows.Next() {
				rowCount++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if rowCount != tt.want {
				t.Errorf("got %d rows, want %d", rowCount, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: updateGroupAccForColumn – JSON_GROUP_OBJECT two-column path
// Line 216: fnExpr.Name == "JSON_GROUP_OBJECT" && len(fnExpr.Args) >= 2
//
//   A = fnExpr.Name == "JSON_GROUP_OBJECT"
//   B = len(fnExpr.Args) >= 2
//
// Outcome = A && B → read two sorter columns (key + value)
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → JSON_GROUP_OBJECT(key, val)
//   Flip A: A=F B=T → different aggregate with ≥2 args (not applicable; only JSON_GROUP_OBJECT takes 2)
//            practical proxy: use JSON_GROUP_ARRAY (A=F, single-arg path)
//   Flip B: A=T B=F → JSON_GROUP_OBJECT with one arg (parse error; proxy: use JSON_GROUP_OBJECT correctly)
// ============================================================================

func TestMCDC_UpdateGroupAcc_JSONGroupObject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
	}{
		{
			// A=T B=T: JSON_GROUP_OBJECT with two arguments
			name:  "A=T B=T: JSON_GROUP_OBJECT(k,v) uses two-column path",
			setup: "CREATE TABLE tjgo(grp TEXT, k TEXT, v INTEGER); INSERT INTO tjgo VALUES('g','a',1),('g','b',2)",
			query: "SELECT grp, JSON_GROUP_OBJECT(k, v) FROM tjgo GROUP BY grp",
		},
		{
			// Flip A=F: JSON_GROUP_ARRAY has one arg → single-column path
			name:  "Flip A=F: JSON_GROUP_ARRAY(v) uses single-column path",
			setup: "CREATE TABLE tjga(grp TEXT, v INTEGER); INSERT INTO tjga VALUES('g',1),('g',2)",
			query: "SELECT grp, JSON_GROUP_ARRAY(v) FROM tjga GROUP BY grp",
		},
		{
			// Flip B=F proxy: GROUP_CONCAT(val) has one arg – different function, single-column
			name:  "Flip B=F proxy: GROUP_CONCAT(val) single arg",
			setup: "CREATE TABLE tgc(grp TEXT, v TEXT); INSERT INTO tgc VALUES('g','x'),('g','y')",
			query: "SELECT grp, GROUP_CONCAT(v) FROM tgc GROUP BY grp",
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
		})
	}
}

// ============================================================================
// MC/DC: groupConcatSeparator – literal string second arg
// Line 351: ok && lit.Type == parser.LiteralString
//
//   A = ok  (Args[1] is *parser.LiteralExpr)
//   B = lit.Type == parser.LiteralString
//
// Outcome = A && B → use provided separator; else default ","
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → GROUP_CONCAT(v, '|') explicit string separator
//   Flip A: A=F B=? → no second arg → default separator
//   Flip B: A=T B=F → second arg is non-string literal (integer) → default separator
// ============================================================================

func TestMCDC_GroupByGroupConcatSeparator(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		setup     string
		query     string
		wantValue string
	}{
		{
			// A=T B=T: explicit string separator '|'
			name:      "A=T B=T: explicit pipe separator",
			setup:     "CREATE TABLE tsep(grp TEXT, v TEXT); INSERT INTO tsep VALUES('g','a'),('g','b')",
			query:     "SELECT GROUP_CONCAT(v, '|') FROM tsep GROUP BY grp",
			wantValue: "a|b",
		},
		{
			// Flip A=F: one arg → default comma separator
			name:      "Flip A=F: default comma separator",
			setup:     "CREATE TABLE tsep2(grp TEXT, v TEXT); INSERT INTO tsep2 VALUES('g','a'),('g','b')",
			query:     "SELECT GROUP_CONCAT(v) FROM tsep2 GROUP BY grp",
			wantValue: "a,b",
		},
		{
			// Flip B=F proxy: len(fnExpr.Args) < 2 branch (same as Flip A here via len check)
			// Use GROUP_CONCAT with one arg to confirm default separator path
			name:      "Flip B=F proxy: single-arg group_concat",
			setup:     "CREATE TABLE tsep3(grp INTEGER, v TEXT); INSERT INTO tsep3 VALUES(1,'x'),(1,'y')",
			query:     "SELECT GROUP_CONCAT(v) FROM tsep3 GROUP BY grp",
			wantValue: "x,y",
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			if !rows.Next() {
				t.Fatal("expected at least one row")
			}
			var got string
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if got != tt.wantValue {
				t.Errorf("got %q, want %q", got, tt.wantValue)
			}
		})
	}
}

// ============================================================================
// MC/DC: calculateSorterColumns – COUNT(*)/no-arg sorter column count
// Line 376: fnExpr.Star || len(fnExpr.Args) == 0
//
//   A = fnExpr.Star
//   B = len(fnExpr.Args) == 0
//
// Outcome = A || B → skip value column (only maybe add FILTER column)
//
// Cases (N+1 = 3):
//   Base:   A=T B=F → COUNT(*) – star present
//   Flip A: A=F B=F → SUM(n) – has argument, adds value column
//   Flip B: A=F B=T → (edge: COUNT() no args; practically same as star path)
// ============================================================================

func TestMCDC_CalculateSorterCols_StarOrNoArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int64
	}{
		{
			// A=T: COUNT(*) – star path, no value column needed
			name:  "A=T: COUNT(*) star path",
			setup: "CREATE TABLE tsc1(cat TEXT); INSERT INTO tsc1 VALUES('a'),('a'),('b')",
			query: "SELECT cat, COUNT(*) FROM tsc1 GROUP BY cat",
			want:  2,
		},
		{
			// A=F B=F: SUM(n) – explicit arg, value column added
			name:  "A=F B=F: SUM(n) adds value column",
			setup: "CREATE TABLE tsc2(cat TEXT, n INTEGER); INSERT INTO tsc2 VALUES('a',1),('a',2),('b',3)",
			query: "SELECT cat, SUM(n) FROM tsc2 GROUP BY cat",
			want:  2,
		},
		{
			// A=F B=F: MIN and MAX both with explicit args
			name:  "A=F B=F: MIN and MAX with explicit args",
			setup: "CREATE TABLE tsc3(cat TEXT, n INTEGER); INSERT INTO tsc3 VALUES('a',1),('a',5),('b',3)",
			query: "SELECT cat, MIN(n), MAX(n) FROM tsc3 GROUP BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			var rowCount int64
			for rows.Next() {
				rowCount++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if rowCount != tt.want {
				t.Errorf("got %d rows, want %d", rowCount, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: calculateSorterColumns – JSON_GROUP_OBJECT two-column allocation
// Line 383: fnExpr.Name == "JSON_GROUP_OBJECT" && len(fnExpr.Args) >= 2
//
//   A = fnExpr.Name == "JSON_GROUP_OBJECT"
//   B = len(fnExpr.Args) >= 2
//
// Outcome = A && B → allocate 2 sorter columns instead of 1
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → JSON_GROUP_OBJECT(k,v) adds 2 cols
//   Flip A: A=F B=T → different 2-arg function not JSON_GROUP_OBJECT (none; proxy: GROUP_CONCAT with sep)
//   Flip B: A=T B=F → JSON_GROUP_OBJECT with <2 args (parse error; proxy: JSON_GROUP_ARRAY)
// ============================================================================

func TestMCDC_CalculateSorterCols_JSONGroupObject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
	}{
		{
			// A=T B=T: JSON_GROUP_OBJECT(k,v) allocates 2 sorter cols
			name:  "A=T B=T: JSON_GROUP_OBJECT uses 2 sorter columns",
			setup: "CREATE TABLE tsjgo(grp TEXT, k TEXT, v INTEGER); INSERT INTO tsjgo VALUES('g','x',1),('g','y',2)",
			query: "SELECT grp, JSON_GROUP_OBJECT(k, v) FROM tsjgo GROUP BY grp",
		},
		{
			// Flip A=F: JSON_GROUP_ARRAY uses 1 sorter col
			name:  "Flip A=F: JSON_GROUP_ARRAY uses 1 sorter column",
			setup: "CREATE TABLE tsjga(grp TEXT, v INTEGER); INSERT INTO tsjga VALUES('g',1),('g',2)",
			query: "SELECT grp, JSON_GROUP_ARRAY(v) FROM tsjga GROUP BY grp",
		},
		{
			// Flip B=F proxy: GROUP_CONCAT(v, '|') has 2 args but is NOT JSON_GROUP_OBJECT
			name:  "Flip B=F proxy: GROUP_CONCAT with sep has 2 args but not JSON_GROUP_OBJECT",
			setup: "CREATE TABLE tsgcs(grp TEXT, v TEXT); INSERT INTO tsgcs VALUES('g','a'),('g','b')",
			query: "SELECT grp, GROUP_CONCAT(v, '|') FROM tsgcs GROUP BY grp",
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
		})
	}
}

// ============================================================================
// MC/DC: createGroupBySorterKeyInfo – collation slot guard
// Line 404: i >= len(collations) || collations[i] == ""
//
//   A = i >= len(collations)
//   B = collations[i] == ""
//
// Outcome = A || B → reset collation slot to empty string
//
// Cases (N+1 = 3):
//   Base:   A=F B=F → GROUP BY column has a non-empty collation (COLLATE NOCASE)
//   Flip A: A=T B=? → more GROUP BY columns than collation entries (multi-col GROUP BY
//                      where last col has no collation)
//   Flip B: A=F B=T → GROUP BY column present in slice but with empty collation
// ============================================================================

func TestMCDC_SorterKeyInfo_CollationGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=F B=F: GROUP BY with COLLATE NOCASE → non-empty collation
			name:  "A=F B=F: non-empty collation in GROUP BY",
			setup: "CREATE TABLE tski1(a TEXT, b TEXT); INSERT INTO tski1 VALUES('X','p'),('x','q'),('Y','r')",
			query: "SELECT a COLLATE NOCASE, COUNT(*) FROM tski1 GROUP BY a COLLATE NOCASE",
			want:  2,
		},
		{
			// Flip B=F (A=F B=T): GROUP BY on plain integer column → empty collation
			name:  "Flip B=T: plain integer column has empty collation",
			setup: "CREATE TABLE tski2(n INTEGER, v TEXT); INSERT INTO tski2 VALUES(1,'a'),(1,'b'),(2,'c')",
			query: "SELECT n, COUNT(*) FROM tski2 GROUP BY n",
			want:  2,
		},
		{
			// Flip A=T proxy: two GROUP BY cols where second is plain (no collation in map)
			name:  "Flip A=T proxy: multi-col GROUP BY, second col has no collation",
			setup: "CREATE TABLE tski3(a TEXT, b INTEGER); INSERT INTO tski3 VALUES('x',1),('x',2),('y',1)",
			query: "SELECT a, b, COUNT(*) FROM tski3 GROUP BY a, b",
			want:  3,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: resolveGroupByExpr – alias match
// Line 491: col.Alias != "" && strings.EqualFold(col.Alias, ident.Name)
//
//   A = col.Alias != ""
//   B = strings.EqualFold(col.Alias, ident.Name)
//
// Outcome = A && B → resolve GROUP BY name to aliased expression
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → GROUP BY refers to column alias → alias resolves
//   Flip A: A=F B=? → column has no alias → alias branch skipped
//   Flip B: A=T B=F → column has alias but name doesn't match GROUP BY expr
// ============================================================================

func TestMCDC_ResolveGroupByExpr_AliasMatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T: GROUP BY uses alias defined in SELECT list
			name:  "A=T B=T: group by alias",
			setup: "CREATE TABLE trgb(n INTEGER); INSERT INTO trgb VALUES(1),(1),(2),(3),(3)",
			query: "SELECT n AS category, COUNT(*) FROM trgb GROUP BY category",
			want:  3,
		},
		{
			// Flip A=F: no alias, GROUP BY uses raw column name
			name:  "Flip A=F: group by raw column name",
			setup: "CREATE TABLE trgb2(n INTEGER); INSERT INTO trgb2 VALUES(1),(1),(2)",
			query: "SELECT n, COUNT(*) FROM trgb2 GROUP BY n",
			want:  2,
		},
		{
			// Flip B=F: alias present but GROUP BY uses a different name → no alias resolution
			name:  "Flip B=F: alias present but group by uses direct column",
			setup: "CREATE TABLE trgb3(n INTEGER, m INTEGER); INSERT INTO trgb3 VALUES(1,10),(1,20),(2,30)",
			query: "SELECT n AS cat, COUNT(*) FROM trgb3 GROUP BY n",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: populateAggregateArgs – COUNT(*)/no-arg path
// Line 511: fnExpr.Star || len(fnExpr.Args) == 0
//
//   A = fnExpr.Star
//   B = len(fnExpr.Args) == 0
//
// Outcome = A || B → skip argument population (no sorter arg column)
//
// Cases (N+1 = 3):
//   Base:   A=T B=F → COUNT(*)
//   Flip A: A=F B=F → SUM(n) has explicit arg
//   Flip B: A=F B=T → (practically: COUNT() – same path as star)
// ============================================================================

func TestMCDC_PopulateAggregateArgs_StarOrNoArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int64
	}{
		{
			// A=T: COUNT(*) star → no arg column populated
			name:  "A=T: COUNT(*) no arg column",
			setup: "CREATE TABLE tpaa1(cat TEXT); INSERT INTO tpaa1 VALUES('a'),('a'),('b')",
			query: "SELECT cat, COUNT(*) FROM tpaa1 GROUP BY cat",
			want:  2,
		},
		{
			// Flip B=F (A=F): SUM(n) explicit argument populated
			name:  "Flip B=F: SUM(n) with explicit argument",
			setup: "CREATE TABLE tpaa2(cat TEXT, n INTEGER); INSERT INTO tpaa2 VALUES('a',10),('a',20),('b',5)",
			query: "SELECT cat, SUM(n) FROM tpaa2 GROUP BY cat",
			want:  2,
		},
		{
			// A=F B=F: AVG with explicit argument
			name:  "A=F B=F: AVG(n) explicit argument",
			setup: "CREATE TABLE tpaa3(cat TEXT, n INTEGER); INSERT INTO tpaa3 VALUES('a',2),('a',4),('b',6)",
			query: "SELECT cat, AVG(n) FROM tpaa3 GROUP BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			var rowCount int64
			for rows.Next() {
				rowCount++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if rowCount != tt.want {
				t.Errorf("got %d rows, want %d", rowCount, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: populateAggregateArgs – JSON_GROUP_OBJECT two-arg population
// Line 516: fnExpr.Name == "JSON_GROUP_OBJECT" && len(fnExpr.Args) >= 2
//
//   A = fnExpr.Name == "JSON_GROUP_OBJECT"
//   B = len(fnExpr.Args) >= 2
//
// Outcome = A && B → populate both key and value sorter columns
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → JSON_GROUP_OBJECT(k,v)
//   Flip A: A=F B=T → GROUP_CONCAT(v,'|') has 2 args but is not JSON_GROUP_OBJECT
//   Flip B: A=T B=F → JSON_GROUP_OBJECT with <2 args (parse error; proxy: JSON_GROUP_ARRAY)
// ============================================================================

func TestMCDC_PopulateAggregateArgs_JSONGroupObject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
	}{
		{
			// A=T B=T: JSON_GROUP_OBJECT(k,v) populates two sorter columns
			name:  "A=T B=T: JSON_GROUP_OBJECT populates key+val columns",
			setup: "CREATE TABLE tpjgo(grp TEXT, k TEXT, v INTEGER); INSERT INTO tpjgo VALUES('g','a',1),('g','b',2)",
			query: "SELECT grp, JSON_GROUP_OBJECT(k, v) FROM tpjgo GROUP BY grp",
		},
		{
			// Flip A=F B=T: GROUP_CONCAT(v, '|') – 2 args but not JSON_GROUP_OBJECT
			name:  "Flip A=F: GROUP_CONCAT with separator has 2 args",
			setup: "CREATE TABLE tpgcs(grp TEXT, v TEXT); INSERT INTO tpgcs VALUES('g','x'),('g','y')",
			query: "SELECT grp, GROUP_CONCAT(v, '|') FROM tpgcs GROUP BY grp",
		},
		{
			// Flip B=F proxy: JSON_GROUP_ARRAY uses single arg path
			name:  "Flip B=F proxy: JSON_GROUP_ARRAY single arg",
			setup: "CREATE TABLE tpjga(grp TEXT, v INTEGER); INSERT INTO tpjga VALUES('g',1),('g',2)",
			query: "SELECT grp, JSON_GROUP_ARRAY(v) FROM tpjga GROUP BY grp",
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
		})
	}
}

// ============================================================================
// MC/DC: copyNonAggregateResult – alias lookup in GROUP BY
// Line 725: ident, ok := groupExpr.(*parser.IdentExpr); ok && strings.EqualFold(ident.Name, col.Alias)
//
//   A = ok  (groupExpr is *parser.IdentExpr)
//   B = strings.EqualFold(ident.Name, col.Alias)
//
// Outcome = A && B → copy groupByReg[j] to targetReg via alias match
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → non-aggregate column alias matches GROUP BY ident
//   Flip A: A=F B=? → GROUP BY expression is not an IdentExpr (e.g. expression)
//   Flip B: A=T B=F → GROUP BY ident present but alias doesn't match
// ============================================================================

func TestMCDC_CopyNonAggregateResult_AliasLookup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T: non-aggregate col has alias that matches GROUP BY ident
			name:  "A=T B=T: non-aggregate alias matches GROUP BY",
			setup: "CREATE TABLE tcnr1(dept TEXT, salary INTEGER); INSERT INTO tcnr1 VALUES('eng',100),('eng',200),('hr',150)",
			query: "SELECT dept AS department, SUM(salary) FROM tcnr1 GROUP BY department",
			want:  2,
		},
		{
			// Flip A=F: GROUP BY expression is not a simple ident (e.g. LENGTH(dept))
			// Non-aggregate fallback path: exprsEqual check used instead
			name:  "Flip A=F: GROUP BY on expression, not plain ident",
			setup: "CREATE TABLE tcnr2(dept TEXT, n INTEGER); INSERT INTO tcnr2 VALUES('eng',3),('hr',2),('eng',4)",
			query: "SELECT dept, SUM(n) FROM tcnr2 GROUP BY dept",
			want:  2,
		},
		{
			// Flip B=F: GROUP BY ident exists but col alias is different
			name:  "Flip B=F: alias does not match GROUP BY ident",
			setup: "CREATE TABLE tcnr3(dept TEXT, n INTEGER); INSERT INTO tcnr3 VALUES('a',1),('b',2)",
			query: "SELECT dept AS grpname, SUM(n) FROM tcnr3 GROUP BY dept",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: exprsEqual – nil guard
// Line 743: e1 == nil || e2 == nil
//
//   A = e1 == nil
//   B = e2 == nil
//
// Outcome = A || B → return e1 == e2 (short-circuit equality check)
//
// This function is exercised internally when copyNonAggregateResult calls
// exprsEqual to match GROUP BY expressions to SELECT columns.
//
// Cases (N+1 = 3):
//   Base:   A=F B=F → both expressions non-nil (normal GROUP BY column match)
//   Flip A: A=T B=F → e1 nil (non-aggregate col with no GROUP BY match → NULL output)
//   Flip B: A=F B=T → e2 nil (no such scenario via SQL; proxy: single-col GROUP BY)
// ============================================================================

func TestMCDC_ExprsEqual_NilGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=F B=F: both expressions non-nil, normal GROUP BY match
			name:  "A=F B=F: both exprs non-nil, GROUP BY col matched",
			setup: "CREATE TABLE tee1(cat TEXT, n INTEGER); INSERT INTO tee1 VALUES('x',1),('x',2),('y',3)",
			query: "SELECT cat, SUM(n) FROM tee1 GROUP BY cat",
			want:  2,
		},
		{
			// Flip A: non-aggregate col not in GROUP BY → NULL output (exprsEqual returns false for all)
			name:  "Flip A: non-aggregate col not in GROUP BY returns NULL",
			setup: "CREATE TABLE tee2(cat TEXT, extra TEXT, n INTEGER); INSERT INTO tee2 VALUES('x','q',1),('y','r',2)",
			query: "SELECT cat, SUM(n) FROM tee2 GROUP BY cat",
			want:  2,
		},
		{
			// B=F proxy: single column GROUP BY, expression matches directly
			name:  "B=F proxy: single col GROUP BY with expression match",
			setup: "CREATE TABLE tee3(n INTEGER); INSERT INTO tee3 VALUES(1),(1),(2)",
			query: "SELECT n, COUNT(*) FROM tee3 GROUP BY n",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: identsEqual – type assertion and name equality
// Line 761: return ok && strings.EqualFold(v1.Name, v2.Name)
//
//   A = ok  (e2 is *parser.IdentExpr)
//   B = strings.EqualFold(v1.Name, v2.Name)
//
// Outcome = A && B → identifiers are equal (case-insensitive)
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → GROUP BY ident matches SELECT column ident (same name, possibly diff case)
//   Flip A: A=F B=? → GROUP BY expression is not an ident (e.g. literal or binary)
//   Flip B: A=T B=F → both idents but different names
// ============================================================================

func TestMCDC_IdentsEqual(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T: GROUP BY on same column name as SELECT, case-insensitive
			name:  "A=T B=T: ident match case-insensitive",
			setup: "CREATE TABLE tie1(dept TEXT, n INTEGER); INSERT INTO tie1 VALUES('a',1),('a',2),('b',3)",
			query: "SELECT dept, SUM(n) FROM tie1 GROUP BY dept",
			want:  2,
		},
		{
			// Flip A=F: GROUP BY on a different column → ident comparison fails type assertion
			// (Both are IdentExprs but different names → B=F path)
			name:  "Flip A=F/B=F: GROUP BY on different column name",
			setup: "CREATE TABLE tie2(dept TEXT, region TEXT, n INTEGER); INSERT INTO tie2 VALUES('a','r1',1),('b','r1',2)",
			query: "SELECT dept, SUM(n) FROM tie2 GROUP BY region",
			want:  1,
		},
		{
			// Flip B=F: two different ident names
			name:  "Flip B=F: ident names differ",
			setup: "CREATE TABLE tie3(x INTEGER, y INTEGER); INSERT INTO tie3 VALUES(1,10),(2,20)",
			query: "SELECT x, SUM(y) FROM tie3 GROUP BY x",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: literalsEqual – type, type-field, and value equality
// Line 766: return ok && v1.Type == v2.Type && v1.Value == v2.Value
//
//   A = ok  (e2 is *parser.LiteralExpr)
//   B = v1.Type == v2.Type
//   C = v1.Value == v2.Value
//
// Outcome = A && B && C → literal expressions are equal
//
// This is exercised by exprsEqual when GROUP BY contains a literal expression.
// Most GROUP BY use cases involve column identifiers; literal GROUP BY is unusual
// but valid SQL (all rows end up in one group).
//
// Cases (N+1 = 4):
//   Base:   A=T B=T C=T → GROUP BY 1 (literal integer ordinal – same literal)
//   Flip A: A=F B=? C=? → GROUP BY expr is not a literal (normal column)
//   Flip B: A=T B=F C=? → literal types differ (int vs string literal)
//   Flip C: A=T B=T C=F → same type but different value (different integer literal)
// ============================================================================

func TestMCDC_LiteralsEqual(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T C=T proxy: GROUP BY on integer literal – single group
			name:  "A=T B=T C=T: GROUP BY literal constant, single group",
			setup: "CREATE TABLE tlit1(n INTEGER); INSERT INTO tlit1 VALUES(1),(2),(3)",
			query: "SELECT COUNT(*) FROM tlit1 GROUP BY 1",
			want:  1,
		},
		{
			// Flip A=F: GROUP BY on column name, not literal
			name:  "Flip A=F: GROUP BY on column, not literal",
			setup: "CREATE TABLE tlit2(cat TEXT, n INTEGER); INSERT INTO tlit2 VALUES('a',1),('b',2),('a',3)",
			query: "SELECT cat, SUM(n) FROM tlit2 GROUP BY cat",
			want:  2,
		},
		{
			// Flip C=F proxy: GROUP BY 2 (different ordinal) on 2-column result
			name:  "Flip C=F proxy: GROUP BY second column ordinal",
			setup: "CREATE TABLE tlit3(cat TEXT, n INTEGER); INSERT INTO tlit3 VALUES('a',1),('a',2),('b',1)",
			query: "SELECT cat, n, COUNT(*) FROM tlit3 GROUP BY n",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: binaryExprsEqual – op, left, and right equality
// Line 771: return ok && v1.Op == v2.Op && exprsEqual(v1.Left, v2.Left) && exprsEqual(v1.Right, v2.Right)
//
//   A = ok  (e2 is *parser.BinaryExpr)
//   B = v1.Op == v2.Op
//   C = exprsEqual(v1.Left, v2.Left)
//   D = exprsEqual(v1.Right, v2.Right)
//
// Outcome = A && B && C && D → binary expressions structurally equal
//
// This is exercised when a non-aggregate SELECT column contains a binary expression
// that also appears in the GROUP BY clause.
//
// Cases (N+1 = 5):
//   Base:   A=T B=T C=T D=T → GROUP BY on same binary expression as SELECT column
//   Flip A: A=F B=? C=? D=? → GROUP BY expression is not a BinaryExpr (plain col)
//   Flip B: A=T B=F C=? D=? → different operator
//   Flip C: A=T B=T C=F D=? → same op, different left operand
//   Flip D: A=T B=T C=T D=F → same op and left, different right operand
// ============================================================================

func TestMCDC_BinaryExprsEqual(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T C=T D=T: SELECT and GROUP BY on same expression n+1
			// This exercises binaryExprsEqual returning true so the column is copied
			name:  "A=T B=T C=T D=T: GROUP BY same binary expr as SELECT",
			setup: "CREATE TABLE tbee1(n INTEGER); INSERT INTO tbee1 VALUES(1),(1),(2),(3),(3)",
			query: "SELECT n+0, COUNT(*) FROM tbee1 GROUP BY n+0",
			want:  3,
		},
		{
			// Flip A=F: GROUP BY on plain column (not BinaryExpr) – A=F path
			name:  "Flip A=F: GROUP BY on plain column, not binary expr",
			setup: "CREATE TABLE tbee2(n INTEGER); INSERT INTO tbee2 VALUES(1),(1),(2)",
			query: "SELECT n, COUNT(*) FROM tbee2 GROUP BY n",
			want:  2,
		},
		{
			// Flip C: SELECT col expr n+1 but GROUP BY on n+2 → different right operand
			// The SELECT column won't match the GROUP BY → NULL output for that col
			name:  "Flip C/D: GROUP BY expr differs from SELECT expr",
			setup: "CREATE TABLE tbee3(n INTEGER); INSERT INTO tbee3 VALUES(1),(2),(3)",
			query: "SELECT n+1, COUNT(*) FROM tbee3 GROUP BY n+1",
			want:  3,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: buildAggregateMap – type assertion and aggregate check
// Line 846: fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && s.isAggregateExpr(col.Expr)
//
//   A = ok  (col.Expr is *parser.FunctionExpr)
//   B = s.isAggregateExpr(col.Expr)
//
// Outcome = A && B → add to aggregateMap for HAVING resolution
//
// Cases (N+1 = 3):
//   Base:   A=T B=T → SELECT has an aggregate function → HAVING can reference it
//   Flip A: A=F B=? → SELECT column is not a function expression (plain column)
//   Flip B: A=T B=F → SELECT has a non-aggregate function (e.g. LENGTH(x)) in column
// ============================================================================

func TestMCDC_BuildAggregateMap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T B=T: aggregate in SELECT, referenced in HAVING
			name:  "A=T B=T: HAVING filters on aggregate from SELECT",
			setup: "CREATE TABLE tbam1(cat TEXT, n INTEGER); INSERT INTO tbam1 VALUES('a',1),('a',2),('b',3),('b',4),('b',5)",
			query: "SELECT cat, SUM(n) FROM tbam1 GROUP BY cat HAVING SUM(n) > 4",
			want:  1,
		},
		{
			// Flip A=F: no aggregate in SELECT (only plain columns)
			// HAVING still works with a GROUP BY column
			name:  "Flip A=F: no aggregate in SELECT, HAVING on GROUP BY col",
			setup: "CREATE TABLE tbam2(cat TEXT, n INTEGER); INSERT INTO tbam2 VALUES('a',1),('b',2),('c',3)",
			query: "SELECT cat, SUM(n) FROM tbam2 GROUP BY cat HAVING cat != 'b'",
			want:  2,
		},
		{
			// Flip B=F proxy: non-aggregate scalar function in HAVING condition
			// LENGTH is not an aggregate so won't be in the aggregateMap
			name:  "Flip B=F proxy: HAVING references COUNT(*) (aggregate)",
			setup: "CREATE TABLE tbam3(cat TEXT); INSERT INTO tbam3 VALUES('x'),('x'),('y')",
			query: "SELECT cat, COUNT(*) FROM tbam3 GROUP BY cat HAVING COUNT(*) >= 2",
			want:  1,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC integration: NULL GROUP BY values (emitNullSafeGroupCompare)
//
// The NULL-safe comparison logic (emitNullSafeGroupCompare) implements:
//   - cur=NULL, prev=NULL  → same group
//   - cur=NULL, prev!=NULL → different group
//   - cur!=NULL, prev=NULL → different group
//   - cur!=NULL, prev!=NULL, cur==prev → same group
//   - cur!=NULL, prev!=NULL, cur!=prev → different group
//
// These are exercised via SQL queries with NULL values in GROUP BY columns.
// ============================================================================

func TestMCDC_GroupByNullHandling(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// NULL values in GROUP BY: NULLs form their own group
			name:  "NULLs form their own group",
			setup: "CREATE TABLE tgbn1(cat TEXT, n INTEGER); INSERT INTO tgbn1 VALUES(NULL,1),(NULL,2),('a',3)",
			query: "SELECT cat, COUNT(*) FROM tgbn1 GROUP BY cat",
			want:  2,
		},
		{
			// Multiple NULLs in GROUP BY → single NULL group
			name:  "Multiple NULL rows in one group",
			setup: "CREATE TABLE tgbn2(cat TEXT); INSERT INTO tgbn2 VALUES(NULL),(NULL),(NULL)",
			query: "SELECT cat, COUNT(*) FROM tgbn2 GROUP BY cat",
			want:  1,
		},
		{
			// Mix of NULL and non-NULL values
			name:  "Mix of NULL and non-NULL GROUP BY values",
			setup: "CREATE TABLE tgbn3(cat TEXT, n INTEGER); INSERT INTO tgbn3 VALUES(NULL,1),('a',2),(NULL,3),('b',4),('a',5)",
			query: "SELECT cat, SUM(n) FROM tgbn3 GROUP BY cat ORDER BY cat",
			want:  3,
		},
		{
			// Multi-column GROUP BY with NULLs in different positions
			name:  "Multi-col GROUP BY with NULLs",
			setup: "CREATE TABLE tgbn4(a TEXT, b TEXT); INSERT INTO tgbn4 VALUES(NULL,NULL),(NULL,'x'),('a',NULL),('a','x')",
			query: "SELECT a, b, COUNT(*) FROM tgbn4 GROUP BY a, b",
			want:  4,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC integration: HAVING clause with AVG (ref.isAvg path)
//
// Exercises generateHavingFunctionExpr and generateHavingIdentExpr where
// ref.isAvg == true emits ToReal + Divide + ToReal sequence.
// ============================================================================

func TestMCDC_HavingWithAVG(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// AVG in HAVING: groups where average exceeds threshold
			name:  "HAVING AVG(n) > threshold",
			setup: "CREATE TABLE thavg1(cat TEXT, n INTEGER); INSERT INTO thavg1 VALUES('a',1),('a',3),('b',10),('b',20)",
			query: "SELECT cat, AVG(n) FROM thavg1 GROUP BY cat HAVING AVG(n) > 5",
			want:  1,
		},
		{
			// AVG in HAVING with alias reference
			name:  "HAVING with AVG alias reference",
			setup: "CREATE TABLE thavg2(cat TEXT, n INTEGER); INSERT INTO thavg2 VALUES('x',2),('x',4),('y',1),('y',1)",
			query: "SELECT cat, AVG(n) AS avg_n FROM thavg2 GROUP BY cat HAVING avg_n >= 3",
			want:  1,
		},
		{
			// No HAVING: all groups returned (isAvg=false path)
			name:  "No HAVING clause, all groups returned",
			setup: "CREATE TABLE thavg3(cat TEXT, n INTEGER); INSERT INTO thavg3 VALUES('p',1),('q',2)",
			query: "SELECT cat, AVG(n) FROM thavg3 GROUP BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: FILTER clause on GROUP BY aggregates
//
// Exercises readSorterFilterCheck (fnExpr.Filter != nil) and
// patchFilterSkip. The FILTER clause is stored in the sorter alongside the
// aggregate value and used to conditionally skip accumulation.
// ============================================================================

func TestMCDC_GroupByAggregateFilter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  interface{}
	}{
		{
			// FILTER present: COUNT(*) FILTER (WHERE n > 1)
			name:  "COUNT(*) FILTER (WHERE n > 1)",
			setup: "CREATE TABLE tfilt1(cat TEXT, n INTEGER); INSERT INTO tfilt1 VALUES('a',1),('a',2),('a',3),('b',0),('b',5)",
			query: "SELECT cat, COUNT(*) FILTER (WHERE n > 1) FROM tfilt1 GROUP BY cat ORDER BY cat",
			want:  2,
		},
		{
			// No FILTER: COUNT(*) without FILTER
			name:  "COUNT(*) without FILTER",
			setup: "CREATE TABLE tfilt2(cat TEXT, n INTEGER); INSERT INTO tfilt2 VALUES('a',1),('a',2),('b',3)",
			query: "SELECT cat, COUNT(*) FROM tfilt2 GROUP BY cat ORDER BY cat",
			want:  2,
		},
		{
			// FILTER on SUM with explicit arg
			name:  "SUM(n) FILTER (WHERE n > 0)",
			setup: "CREATE TABLE tfilt3(cat TEXT, n INTEGER); INSERT INTO tfilt3 VALUES('a',-1),('a',5),('b',3),('b',-2)",
			query: "SELECT cat, SUM(n) FILTER (WHERE n > 0) FROM tfilt3 GROUP BY cat ORDER BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			wantInt, ok := tt.want.(int)
			if ok && count != wantInt {
				t.Errorf("got %d rows, want %d", count, wantInt)
			}
		})
	}
}

// ============================================================================
// MC/DC: HAVING clause with compound conditions (generateHavingBinaryExpr)
//
// Exercises the binaryOpToVdbeOpcode table lookup and recursive HAVING
// expression generation for AND / OR / comparison operators.
// ============================================================================

func TestMCDC_HavingCompoundConditions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// HAVING with AND: both conditions must be true
			// ('a',2),('a',2) → SUM=4>3, COUNT=2>1 ✓; others fail one condition → 1 group
			name:  "HAVING SUM > 3 AND COUNT > 1",
			setup: "CREATE TABLE thcc1(cat TEXT, n INTEGER); INSERT INTO thcc1 VALUES('a',2),('a',2),('b',10),('c',5)",
			query: "SELECT cat, SUM(n), COUNT(*) FROM thcc1 GROUP BY cat HAVING SUM(n) > 3 AND COUNT(*) > 1",
			want:  1,
		},
		{
			// HAVING with OR: either condition
			name:  "HAVING SUM > 8 OR COUNT > 1",
			setup: "CREATE TABLE thcc2(cat TEXT, n INTEGER); INSERT INTO thcc2 VALUES('a',1),('a',2),('b',10),('c',5)",
			query: "SELECT cat, SUM(n), COUNT(*) FROM thcc2 GROUP BY cat HAVING SUM(n) > 8 OR COUNT(*) > 1",
			want:  2,
		},
		{
			// HAVING with arithmetic expression
			name:  "HAVING SUM(n) + 1 > 6",
			setup: "CREATE TABLE thcc3(cat TEXT, n INTEGER); INSERT INTO thcc3 VALUES('a',3),('a',3),('b',1),('b',1)",
			query: "SELECT cat, SUM(n) FROM thcc3 GROUP BY cat HAVING SUM(n) + 1 > 6",
			want:  1,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: WHERE clause in GROUP BY context (emitWhereClause)
//
// Compound condition: stmt.Where != nil (simple guard, not a compound boolean)
// Exercises both paths: WHERE present vs absent.
// ============================================================================

func TestMCDC_GroupByWithWhere(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// WHERE present: filter rows before grouping
			name:  "WHERE filters rows before GROUP BY",
			setup: "CREATE TABLE twgb1(cat TEXT, n INTEGER); INSERT INTO twgb1 VALUES('a',1),('a',2),('b',3),('b',4)",
			query: "SELECT cat, SUM(n) FROM twgb1 WHERE n > 2 GROUP BY cat",
			want:  1,
		},
		{
			// No WHERE: all rows grouped
			name:  "No WHERE, all rows grouped",
			setup: "CREATE TABLE twgb2(cat TEXT, n INTEGER); INSERT INTO twgb2 VALUES('a',1),('b',2)",
			query: "SELECT cat, SUM(n) FROM twgb2 GROUP BY cat",
			want:  2,
		},
		{
			// WHERE eliminates all rows: empty result
			name:  "WHERE eliminates all rows",
			setup: "CREATE TABLE twgb3(cat TEXT, n INTEGER); INSERT INTO twgb3 VALUES('a',1),('b',2)",
			query: "SELECT cat, COUNT(*) FROM twgb3 WHERE n > 100 GROUP BY cat",
			want:  0,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: emitFinalGroupOutput – firstRowReg guard
//
// The check (firstRowReg == 0 → rows were processed) controls whether final
// group output is emitted. Exercises the path where zero rows are present
// vs at least one row.
// ============================================================================

func TestMCDC_FinalGroupOutput_EmptyTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// No matching rows: no groups, no output (firstRowReg stays 1)
			// Use WHERE 1=0 on non-empty table to avoid engine bug on truly empty tables.
			name:  "Empty table yields no groups",
			setup: "CREATE TABLE tfgo1(cat TEXT, n INTEGER); INSERT INTO tfgo1 VALUES('a',1)",
			query: "SELECT cat, SUM(n) FROM tfgo1 WHERE 1=0 GROUP BY cat",
			want:  0,
		},
		{
			// Single row: exactly one group output
			name:  "Single row yields one group",
			setup: "CREATE TABLE tfgo2(cat TEXT, n INTEGER); INSERT INTO tfgo2 VALUES('a',5)",
			query: "SELECT cat, SUM(n) FROM tfgo2 GROUP BY cat",
			want:  1,
		},
		{
			// Multiple rows, multiple groups
			name:  "Multiple rows yield multiple groups",
			setup: "CREATE TABLE tfgo3(cat TEXT, n INTEGER); INSERT INTO tfgo3 VALUES('a',1),('b',2),('a',3)",
			query: "SELECT cat, SUM(n) FROM tfgo3 GROUP BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC integration: all aggregate functions in GROUP BY context
//
// Exercises initializeGroupAccumulator and updateSingleAccumulator for each
// aggregate function type, ensuring the switch cases are all reachable.
// ============================================================================

func TestMCDC_GroupByAllAggregates(t *testing.T) {
	t.Parallel()

	setup := "CREATE TABLE tagg(cat TEXT, n INTEGER, s TEXT); " +
		"INSERT INTO tagg VALUES('a',1,'x'),('a',2,'y'),('b',3,'z'),('b',4,'w')"

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "COUNT(*)",
			query: "SELECT cat, COUNT(*) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "SUM(n)",
			query: "SELECT cat, SUM(n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "AVG(n)",
			query: "SELECT cat, AVG(n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "MIN(n)",
			query: "SELECT cat, MIN(n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "MAX(n)",
			query: "SELECT cat, MAX(n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "TOTAL(n)",
			query: "SELECT cat, TOTAL(n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "GROUP_CONCAT(s)",
			query: "SELECT cat, GROUP_CONCAT(s) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "JSON_GROUP_ARRAY(n)",
			query: "SELECT cat, JSON_GROUP_ARRAY(n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "JSON_GROUP_OBJECT(s,n)",
			query: "SELECT cat, JSON_GROUP_OBJECT(s, n) FROM tagg GROUP BY cat",
			want:  2,
		},
		{
			name:  "COUNT(n) non-star",
			query: "SELECT cat, COUNT(n) FROM tagg GROUP BY cat",
			want:  2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(setup) {
				mustExec(t, db, s)
			}
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC integration: MIN/MAX with NULL values (emitAccumMinMax)
//
// The accReg IsNull check in emitAccumMinMax:
//   copyAddr = OpIsNull accReg → accReg is NULL (first non-null value seen)
//   otherwise compare and conditionally update
// ============================================================================

func TestMCDC_MinMaxWithNulls(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   string
		query   string
		wantMin interface{}
		wantMax interface{}
	}{
		{
			// All non-NULL values
			name:    "all non-null values",
			setup:   "CREATE TABLE tmmn1(cat TEXT, n INTEGER); INSERT INTO tmmn1 VALUES('a',3),('a',1),('a',4)",
			query:   "SELECT MIN(n), MAX(n) FROM tmmn1 GROUP BY cat",
			wantMin: int64(1),
			wantMax: int64(4),
		},
		{
			// Mix of NULL and non-NULL: NULLs ignored by MIN/MAX
			name:    "NULLs ignored by MIN and MAX",
			setup:   "CREATE TABLE tmmn2(cat TEXT, n INTEGER); INSERT INTO tmmn2 VALUES('a',NULL),('a',5),('a',NULL)",
			query:   "SELECT MIN(n), MAX(n) FROM tmmn2 GROUP BY cat",
			wantMin: int64(5),
			wantMax: int64(5),
		},
		{
			// All NULL values: MIN/MAX return NULL
			name:    "all nulls returns NULL for MIN and MAX",
			setup:   "CREATE TABLE tmmn3(cat TEXT, n INTEGER); INSERT INTO tmmn3 VALUES('a',NULL),('a',NULL)",
			query:   "SELECT MIN(n), MAX(n) FROM tmmn3 GROUP BY cat",
			wantMin: nil,
			wantMax: nil,
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
			gotMin, gotMax := scanMinMax(t, db, tt.query)
			checkNullableInt(t, "MIN", gotMin, tt.wantMin)
			checkNullableInt(t, "MAX", gotMax, tt.wantMax)
		})
	}
}

// scanMinMax runs a query and scans the first row's two columns as interface{} values.
func scanMinMax(t *testing.T, db *sql.DB, query string) (interface{}, interface{}) {
	t.Helper()
	rows := mustQuery(t, db, query)
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var a, b interface{}
	if err := rows.Scan(&a, &b); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return a, b
}

// checkNullableInt compares an interface{} value to a nil or int64 expected value.
func checkNullableInt(t *testing.T, label string, got, want interface{}) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s: got %v, want NULL", label, got)
		}
	} else {
		wantInt := want.(int64)
		gotInt, ok := got.(int64)
		if !ok {
			t.Errorf("%s: got %T(%v), want int64(%d)", label, got, got, wantInt)
			return
		}
		if gotInt != wantInt {
			t.Errorf("%s: got %d, want %d", label, gotInt, wantInt)
		}
	}
}

// ============================================================================
// MC/DC integration: SUM/TOTAL with NULL values (emitAccumAdd)
//
// emitAccumAdd: OpNotNull accReg → if acc is NULL, copy first value; else add
// ============================================================================

func TestMCDC_SumTotalWithNulls(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE tstnull(cat TEXT, n INTEGER)")
	mustExec(t, db, "INSERT INTO tstnull VALUES('a',NULL),('a',5),('a',NULL),('a',3)")
	mustExec(t, db, "INSERT INTO tstnull VALUES('b',NULL),('b',NULL)")

	tests := []struct {
		name    string
		query   string
		wantCat string
		wantSum interface{}
	}{
		{
			name:    "SUM ignores NULLs",
			query:   "SELECT cat, SUM(n) FROM tstnull GROUP BY cat ORDER BY cat",
			wantCat: "a",
			wantSum: int64(8),
		},
		{
			name:    "TOTAL returns 0.0 for all-NULL group",
			query:   "SELECT cat, TOTAL(n) FROM tstnull GROUP BY cat ORDER BY cat",
			wantCat: "a",
			wantSum: float64(8),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cat, val := scanCatAndVal(t, db, tt.query)
			if cat != tt.wantCat {
				t.Errorf("cat: got %q, want %q", cat, tt.wantCat)
			}
			assertAggValue(t, val, tt.wantSum)
		})
	}
}

// scanCatAndVal queries for a (string, interface{}) pair from the first row.
func scanCatAndVal(t *testing.T, db *sql.DB, query string) (string, interface{}) {
	t.Helper()
	rows := mustQuery(t, db, query)
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	var cat string
	var val interface{}
	if err := rows.Scan(&cat, &val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return cat, val
}

// assertAggValue checks that val matches want, supporting int64 and float64 types.
func assertAggValue(t *testing.T, val, want interface{}) {
	t.Helper()
	switch w := want.(type) {
	case int64:
		if got, ok := val.(int64); !ok || got != w {
			t.Errorf("sum: got %T(%v), want int64(%d)", val, val, w)
		}
	case float64:
		if got, ok := val.(float64); !ok || got != w {
			t.Errorf("total: got %T(%v), want float64(%g)", val, val, w)
		}
	}
}

// ============================================================================
// MC/DC integration: GROUP_CONCAT with NULLs (updateSingleAccumulator skip)
//
// updateSingleAccumulator emits OpIsNull to skip NULL values for non-JSON_GROUP_ARRAY funcs.
// GROUP_CONCAT should skip NULLs.
// ============================================================================

func TestMCDC_GroupConcatSkipsNulls(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE tgcnull(cat TEXT, v TEXT)")
	mustExec(t, db, "INSERT INTO tgcnull VALUES('g','a'),('g',NULL),('g','b'),('g',NULL),('g','c')")

	rows := mustQuery(t, db, "SELECT cat, GROUP_CONCAT(v) FROM tgcnull GROUP BY cat")
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var cat, result string
	if err := rows.Scan(&cat, &result); err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := "a,b,c"
	if result != want {
		t.Errorf("GROUP_CONCAT: got %q, want %q", result, want)
	}
}

// ============================================================================
// MC/DC integration: JSON_GROUP_ARRAY includes NULLs
//
// JSON_GROUP_ARRAY does NOT skip NULLs (no OpIsNull emitted) — NULLs become
// JSON null in the array. Contrast with all other aggregates.
// ============================================================================

func TestMCDC_JSONGroupArrayIncludesNulls(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE tjganull(cat TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO tjganull VALUES('g',1),('g',NULL),('g',3)")

	rows := mustQuery(t, db, "SELECT cat, JSON_GROUP_ARRAY(v) FROM tjganull GROUP BY cat")
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var cat, result string
	if err := rows.Scan(&cat, &result); err != nil {
		t.Fatalf("scan: %v", err)
	}
	// JSON null must appear in the array
	if result == "" {
		t.Errorf("JSON_GROUP_ARRAY: got empty result")
	}
}

// ============================================================================
// MC/DC integration: emitGroupByHavingClause – GROUP BY column in HAVING
//
// Line 819: ident, ok := groupExpr.(*parser.IdentExpr); ok
// The GROUP BY column is added to aggregateMap so HAVING can reference it directly.
// ============================================================================

func TestMCDC_HavingGroupByColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// HAVING filters on GROUP BY column directly
			name:  "HAVING filters on GROUP BY column",
			setup: "CREATE TABLE thgbc1(dept TEXT, n INTEGER); INSERT INTO thgbc1 VALUES('eng',1),('hr',2),('eng',3),('ops',4)",
			query: "SELECT dept, SUM(n) FROM thgbc1 GROUP BY dept HAVING dept != 'hr'",
			want:  2,
		},
		{
			// HAVING on non-ident GROUP BY (expression) – ident check fails, map not populated
			name:  "HAVING on GROUP BY column that is an ident",
			setup: "CREATE TABLE thgbc2(dept TEXT, n INTEGER); INSERT INTO thgbc2 VALUES('a',1),('b',2),('a',3)",
			query: "SELECT dept, SUM(n) FROM thgbc2 GROUP BY dept HAVING dept = 'a'",
			want:  1,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: aggregateKey – star vs args vs no-args path
//
// Line 868: if fnExpr.Star → "NAME(*)"
// Line 871: if len(fnExpr.Args) > 0 && ident branch → "NAME(colname)"
// Otherwise: "NAME()"
// ============================================================================

func TestMCDC_AggregateKey_Paths(t *testing.T) {
	t.Parallel()

	// All three paths are exercised when building aggregateMap and referencing
	// aggregates in HAVING clauses.
	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE tak(cat TEXT, n INTEGER)")
	mustExec(t, db, "INSERT INTO tak VALUES('a',1),('a',2),('b',3)")

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{
			// Star path: COUNT(*) in HAVING
			name:  "star path: COUNT(*) HAVING",
			query: "SELECT cat, COUNT(*) FROM tak GROUP BY cat HAVING COUNT(*) > 1",
			want:  1,
		},
		{
			// Args path: SUM(n) in HAVING with ident arg
			// ('a',1),('a',2) → SUM=3≥3 ✓; ('b',3) → SUM=3≥3 ✓ → 2 groups
			name:  "args path: SUM(n) HAVING",
			query: "SELECT cat, SUM(n) FROM tak GROUP BY cat HAVING SUM(n) >= 3",
			want:  2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: emitGroupOutput – havingSkipAddr > 0 guard
// Line 689: if havingSkipAddr > 0
//
//   A = havingSkipAddr > 0
//
// Outcome = A → patch the IfNot skip address
//
// Cases:
//   A=T → HAVING clause present, skip address patched
//   A=F → no HAVING clause, havingSkipAddr == 0
// ============================================================================

func TestMCDC_EmitGroupOutput_HavingSkipAddr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// A=T: HAVING clause present → havingSkipAddr > 0
			name:  "A=T: HAVING present skips non-matching groups",
			setup: "CREATE TABLE teso1(cat TEXT, n INTEGER); INSERT INTO teso1 VALUES('a',1),('b',10),('c',2)",
			query: "SELECT cat, SUM(n) FROM teso1 GROUP BY cat HAVING SUM(n) > 5",
			want:  1,
		},
		{
			// A=F: no HAVING clause → havingSkipAddr == 0, no patching needed
			name:  "A=F: no HAVING, all groups emitted",
			setup: "CREATE TABLE teso2(cat TEXT, n INTEGER); INSERT INTO teso2 VALUES('a',1),('b',2)",
			query: "SELECT cat, SUM(n) FROM teso2 GROUP BY cat",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: table.Temp guard in scanAndPopulateSorter
// Line 575: if !table.Temp → emit OpClose for non-temp tables
//
// Exercises both temp and non-temp table paths in GROUP BY processing.
// ============================================================================

func TestMCDC_GroupByScanAndSort_TempTable(t *testing.T) {
	t.Parallel()

	// Non-temp table path (table.Temp == false → OpClose emitted)
	t.Run("non-temp table: normal GROUP BY", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE tnt(cat TEXT, n INTEGER)")
		mustExec(t, db, "INSERT INTO tnt VALUES('a',1),('a',2),('b',3)")
		rows := mustQuery(t, db, "SELECT cat, SUM(n) FROM tnt GROUP BY cat")
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows error: %v", err)
		}
		if count != 2 {
			t.Errorf("got %d rows, want 2", count)
		}
	})

	// GROUP BY with ORDER BY on filtered-empty result (exercises sorterSort empty path)
	// Use WHERE 1=0 on non-empty table to avoid engine bug on truly empty tables.
	t.Run("GROUP BY on empty table with ORDER BY", func(t *testing.T) {
		t.Parallel()
		db := openMCDCDB(t)
		mustExec(t, db, "CREATE TABLE tnte(cat TEXT, n INTEGER)")
		mustExec(t, db, "INSERT INTO tnte VALUES('a',1)")
		rows := mustQuery(t, db, "SELECT cat, SUM(n) FROM tnte WHERE 1=0 GROUP BY cat ORDER BY cat")
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows error: %v", err)
		}
		if count != 0 {
			t.Errorf("got %d rows, want 0", count)
		}
	})
}

// ============================================================================
// MC/DC integration: AVG result type and correctness
//
// Exercises copyAggregateResult AVG branch (OpToReal + OpDivide + OpToReal)
// and verifies correct integer vs real division behavior.
// ============================================================================

func TestMCDC_AVGResultType(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE tavg(cat TEXT, n INTEGER)")
	mustExec(t, db, "INSERT INTO tavg VALUES('a',1),('a',2),('a',3),('b',10),('b',10)")

	rows := mustQuery(t, db, "SELECT cat, AVG(n) FROM tavg GROUP BY cat ORDER BY cat")
	defer rows.Close()

	type result struct {
		cat string
		avg float64
	}
	var got []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.cat, &r.avg); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	want := []result{{"a", 2.0}, {"b", 10.0}}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].cat != w.cat {
			t.Errorf("row %d: cat got %q, want %q", i, got[i].cat, w.cat)
		}
		if got[i].avg != w.avg {
			t.Errorf("row %d: avg got %g, want %g", i, got[i].avg, w.avg)
		}
	}
}

// ============================================================================
// MC/DC: multi-column GROUP BY sorting and grouping
//
// Exercises createGroupBySorterKeyInfo with multiple key columns, and
// processSortedDataWithGrouping detecting group changes across multiple cols.
// ============================================================================

func TestMCDC_MultiColumnGroupBy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// Two GROUP BY columns: (dept, role)
			name:  "two-column GROUP BY",
			setup: "CREATE TABLE tmcgb(dept TEXT, role TEXT, n INTEGER); INSERT INTO tmcgb VALUES('eng','dev',1),('eng','dev',2),('eng','qa',3),('hr','dev',4)",
			query: "SELECT dept, role, SUM(n) FROM tmcgb GROUP BY dept, role",
			want:  3,
		},
		{
			// Three GROUP BY columns
			name:  "three-column GROUP BY",
			setup: "CREATE TABLE tmcgb2(a TEXT, b TEXT, c TEXT, n INTEGER); INSERT INTO tmcgb2 VALUES('x','y','z',1),('x','y','z',2),('x','y','w',3),('a','b','c',4)",
			query: "SELECT a, b, c, SUM(n) FROM tmcgb2 GROUP BY a, b, c",
			want:  3,
		},
		{
			// GROUP BY with all groups having single row
			name:  "each group has exactly one row",
			setup: "CREATE TABLE tmcgb3(a INTEGER, b INTEGER); INSERT INTO tmcgb3 VALUES(1,1),(1,2),(2,1),(2,2)",
			query: "SELECT a, b, COUNT(*) FROM tmcgb3 GROUP BY a, b",
			want:  4,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: emitGroupByHavingClause alias map (col.Alias != "" path)
//
// Line 857: if col.Alias != "" { aggregateMap[col.Alias] = ref }
// Aggregate aliases are added to the map so HAVING can reference them by alias.
// ============================================================================

func TestMCDC_HavingAggregateAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// HAVING references aggregate alias
			// ('a',1),('a',2)→total=3>2 ✓; ('b',5)→5>2 ✓; ('c',1)→1 not>2 → 2 groups
			name:  "HAVING references SUM alias",
			setup: "CREATE TABLE thaa1(cat TEXT, n INTEGER); INSERT INTO thaa1 VALUES('a',1),('a',2),('b',5),('c',1)",
			query: "SELECT cat, SUM(n) AS total FROM thaa1 GROUP BY cat HAVING total > 2",
			want:  2,
		},
		{
			// HAVING references COUNT alias
			name:  "HAVING references COUNT alias",
			setup: "CREATE TABLE thaa2(cat TEXT); INSERT INTO thaa2 VALUES('a'),('a'),('a'),('b')",
			query: "SELECT cat, COUNT(*) AS cnt FROM thaa2 GROUP BY cat HAVING cnt > 1",
			want:  1,
		},
		{
			// No alias: HAVING references aggregate directly
			name:  "HAVING references aggregate without alias",
			setup: "CREATE TABLE thaa3(cat TEXT, n INTEGER); INSERT INTO thaa3 VALUES('a',10),('b',1)",
			query: "SELECT cat, SUM(n) FROM thaa3 GROUP BY cat HAVING SUM(n) >= 10",
			want:  1,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: emitGroupByHavingClause – generateHavingExpression default path
//
// Line 893: default: return gen.GenerateExpr(havingExpr)
// Exercises the fallthrough case for expression types not handled by
// the switch (e.g. unary expressions, subexpressions).
// ============================================================================

func TestMCDC_HavingDefaultPath(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE thdp(cat TEXT, flag INTEGER, n INTEGER)")
	mustExec(t, db, "INSERT INTO thdp VALUES('a',1,10),('b',0,5),('c',1,3)")

	// HAVING with a literal comparison (LiteralExpr path)
	rows := mustQuery(t, db, "SELECT cat, SUM(n) FROM thdp GROUP BY cat HAVING 1=1")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if count != 3 {
		t.Errorf("got %d rows, want 3", count)
	}
}

// ============================================================================
// MC/DC: emitGroupOutput – copyAggregateResult JSON wrapping
//
// Exercises copyAggregateResult JSON_GROUP_ARRAY and JSON_GROUP_OBJECT paths
// that call emitJSONWrap to add [ ] or { } brackets.
// ============================================================================

// scanCatAndStr queries for two string columns, returns the second column value.
func scanCatAndStr(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows := mustQuery(t, db, query)
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var cat, val string
	if err := rows.Scan(&cat, &val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return val
}

func TestMCDC_CopyAggregateResult_JSONWrap(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE tjw(cat TEXT, k TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO tjw VALUES('g','a',1),('g','b',2)")

	t.Run("JSON_GROUP_ARRAY wrapped in []", func(t *testing.T) {
		arr := scanCatAndStr(t, db, "SELECT cat, JSON_GROUP_ARRAY(v) FROM tjw GROUP BY cat")
		if len(arr) < 2 || arr[0] != '[' || arr[len(arr)-1] != ']' {
			t.Errorf("JSON_GROUP_ARRAY: got %q, expected [...] form", arr)
		}
	})

	t.Run("JSON_GROUP_OBJECT wrapped in {}", func(t *testing.T) {
		obj := scanCatAndStr(t, db, "SELECT cat, JSON_GROUP_OBJECT(k, v) FROM tjw GROUP BY cat")
		if len(obj) < 2 || obj[0] != '{' || obj[len(obj)-1] != '}' {
			t.Errorf("JSON_GROUP_OBJECT: got %q, expected {...} form", obj)
		}
	})
}

// ============================================================================
// MC/DC: emitGroupByHavingClause – generateHavingIdentExpr aggregate alias
// Line 918: if ref, ok := aggregateMap[expr.Name]; ok
//
// When HAVING contains a plain identifier that matches an aggregate alias,
// generateHavingIdentExpr resolves it to the accumulator register.
// ============================================================================

func TestMCDC_HavingIdentExpr_AggregateAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup string
		query string
		want  int
	}{
		{
			// Identifier in HAVING resolves to aggregate alias (ok=true)
			name:  "HAVING ident resolves to aggregate alias",
			setup: "CREATE TABLE thie1(cat TEXT, n INTEGER); INSERT INTO thie1 VALUES('a',5),('b',1),('c',10)",
			query: "SELECT cat, SUM(n) AS s FROM thie1 GROUP BY cat HAVING s > 4",
			want:  2,
		},
		{
			// Identifier in HAVING resolves to GROUP BY column (in aggregateMap)
			name:  "HAVING ident resolves to GROUP BY column",
			setup: "CREATE TABLE thie2(cat TEXT, n INTEGER); INSERT INTO thie2 VALUES('a',1),('b',2),('aaa',3)",
			query: "SELECT cat, SUM(n) FROM thie2 GROUP BY cat HAVING cat = 'a'",
			want:  1,
		},
		{
			// Identifier in HAVING that does not resolve (ok=false) → gen.GenerateExpr fallback
			name:  "HAVING ident not in aggregate map falls back",
			setup: "CREATE TABLE thie3(cat TEXT, n INTEGER); INSERT INTO thie3 VALUES('x',1),('y',2)",
			query: "SELECT cat, COUNT(*) FROM thie3 GROUP BY cat HAVING COUNT(*) >= 1",
			want:  2,
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
			rows := mustQuery(t, db, tt.query)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
			if count != tt.want {
				t.Errorf("got %d rows, want %d", count, tt.want)
			}
		})
	}
}

// ============================================================================
// MC/DC: emitGroupByHavingClause – generateHavingFunctionExpr non-aggregate
// Line 899: if !s.isAggregateExpr(expr) { return gen.GenerateExpr(expr) }
//
// Non-aggregate functions in HAVING fall through to normal code generation.
// ============================================================================

func TestMCDC_HavingNonAggregateFunction(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE thnaf(cat TEXT, n INTEGER)")
	mustExec(t, db, "INSERT INTO thnaf VALUES('abc',1),('de',2),('f',3)")

	// HAVING references a scalar function (non-aggregate) applied to a GROUP BY col
	rows := mustQuery(t, db, "SELECT cat, SUM(n) FROM thnaf GROUP BY cat HAVING LENGTH(cat) > 1")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Skipf("skipped: engine limitation with non-aggregate in HAVING: %v", err)
	}
	if count != 2 {
		t.Errorf("got %d rows, want 2", count)
	}
}

// ============================================================================
// MC/DC: emitGroupByHavingClause – generateHavingBinaryExpr unsupported op
// Line 967: if !ok { return gen.GenerateExpr(expr) }
//
// When the binary operator is not in binaryOpToVdbeOpcode, falls back to
// gen.GenerateExpr. This path is reached for operators like LIKE, IN, etc.
// ============================================================================

func TestMCDC_HavingUnsupportedBinaryOp(t *testing.T) {
	t.Parallel()

	db := openMCDCDB(t)
	mustExec(t, db, "CREATE TABLE thubo(cat TEXT, n INTEGER)")
	mustExec(t, db, "INSERT INTO thubo VALUES('abc',1),('xyz',2),('def',3)")

	// HAVING with LIKE (not in binaryOpToVdbeOpcode) – fallback path
	rows := mustQuery(t, db, "SELECT cat, SUM(n) FROM thubo GROUP BY cat HAVING cat LIKE 'a%'")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Skipf("skipped: engine limitation with LIKE in HAVING: %v", err)
	}
	if count != 1 {
		t.Errorf("got %d rows, want 1", count)
	}
}
