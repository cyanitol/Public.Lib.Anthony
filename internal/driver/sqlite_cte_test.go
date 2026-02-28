package driver

import (
	"fmt"
	"strings"
	"testing"
)

// TestCTEBasicNonRecursive tests basic non-recursive CTEs from with1.test
func TestCTEBasicNonRecursive(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("simple_with_select", func(t *testing.T) {
		execSQL(t, db, "CREATE TABLE t1(x INTEGER, y INTEGER)")
		rows := queryRows(t, db, "WITH x(a) AS (SELECT * FROM t1) SELECT 10")
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "10" {
			t.Errorf("expected [[10]], got %v", rows)
		}
	})

	t.Run("with_in_subquery", func(t *testing.T) {
		rows := queryRows(t, db, "SELECT * FROM (WITH x AS (SELECT * FROM t1) SELECT 10)")
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "10" {
			t.Errorf("expected [[10]], got %v", rows)
		}
	})

	t.Run("with_insert", func(t *testing.T) {
		mustExec(t, db, "WITH x(a) AS (SELECT * FROM t1) INSERT INTO t1 VALUES(1,2)")
	})

	t.Run("with_delete", func(t *testing.T) {
		mustExec(t, db, "WITH x(a) AS (SELECT * FROM t1) DELETE FROM t1")
	})

	t.Run("with_update", func(t *testing.T) {
		mustExec(t, db, "WITH x(a) AS (SELECT * FROM t1) UPDATE t1 SET x = y")
	})
}

// TestCTESimpleQueries tests simple CTE queries from with1.test
func TestCTESimpleQueries(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"DROP TABLE IF EXISTS t1",
		"CREATE TABLE t1(x)",
		"INSERT INTO t1 VALUES(1)",
		"INSERT INTO t1 VALUES(2)",
	)

	t.Run("cte_with_column_name", func(t *testing.T) {
		rows := queryRows(t, db, "WITH tmp AS (SELECT * FROM t1) SELECT x FROM tmp")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("cte_with_explicit_column", func(t *testing.T) {
		rows := queryRows(t, db, "WITH tmp(a) AS (SELECT * FROM t1) SELECT a FROM tmp")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("cte_in_subquery_explicit", func(t *testing.T) {
		rows := queryRows(t, db, "SELECT * FROM (WITH tmp(a) AS (SELECT * FROM t1) SELECT a FROM tmp)")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})
}

// TestCTEMultipleCTEs tests multiple CTEs from with1.test
func TestCTEMultipleCTEs(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"DROP TABLE IF EXISTS t1",
		"CREATE TABLE t1(x)",
		"INSERT INTO t1 VALUES(1)",
		"INSERT INTO t1 VALUES(2)",
	)

	t.Run("two_ctes_forward_ref", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH tmp1(a) AS (SELECT * FROM t1),
			     tmp2(x) AS (SELECT * FROM tmp1)
			SELECT * FROM tmp2
		`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("two_ctes_backward_ref", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH tmp2(x) AS (SELECT * FROM tmp1),
			     tmp1(a) AS (SELECT * FROM t1)
			SELECT * FROM tmp2
		`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})
}

// TestCTEErrors tests CTE error conditions from with1.test
func TestCTEErrors(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("circular_reference", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH tmp2(x) AS (SELECT * FROM tmp1),
			     tmp1(a) AS (SELECT * FROM tmp2)
			SELECT * FROM tmp1
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})

	t.Run("duplicate_cte_name", func(t *testing.T) {
		execSQL(t, db, "CREATE TABLE IF NOT EXISTS t2(x INTEGER)")
		err := expectQueryError(t, db, `
			WITH tmp(a) AS (SELECT * FROM t2),
			     tmp(a) AS (SELECT * FROM t2)
			SELECT * FROM tmp
		`)
		if !strings.Contains(err.Error(), "duplicate") {
			t.Errorf("expected duplicate error, got: %v", err)
		}
	})

	t.Run("column_count_mismatch", func(t *testing.T) {
		_ = expectQueryError(t, db, "WITH i(x, y) AS (VALUES(1)) SELECT * FROM i")
	})

	t.Run("syntax_error_trailing_comma", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH tmp AS (SELECT * FROM t2),
			SELECT * FROM tmp
		`)
		if !strings.Contains(err.Error(), "syntax error") {
			t.Errorf("expected syntax error, got: %v", err)
		}
	})
}

// TestCTEShadowing tests CTE name shadowing from with1.test
func TestCTEShadowing(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE IF NOT EXISTS t3(x)",
		"CREATE TABLE IF NOT EXISTS t4(x)",
		"INSERT INTO t3 VALUES('T3')",
		"INSERT INTO t4 VALUES('T4')",
	)

	t.Run("cte_shadows_table", func(t *testing.T) {
		rows := queryRows(t, db, "WITH t3(a) AS (SELECT * FROM t4) SELECT * FROM t3")
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "T4" {
			t.Errorf("expected T4, got %v", rows)
		}
	})

	t.Run("nested_cte_shadowing", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH tmp AS (SELECT * FROM t3),
			     tmp2 AS (WITH tmp AS (SELECT * FROM t4) SELECT * FROM tmp)
			SELECT * FROM tmp2
		`)
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "T4" {
			t.Errorf("expected T4, got %v", rows)
		}
	})

	t.Run("nested_cte_outer_scope", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH tmp AS (SELECT * FROM t3),
			     tmp2 AS (WITH xxxx AS (SELECT * FROM t4) SELECT * FROM tmp)
			SELECT * FROM tmp2
		`)
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "T3" {
			t.Errorf("expected T3, got %v", rows)
		}
	})
}

// TestCTEWithDML tests CTEs with INSERT/UPDATE/DELETE from with1.test
func TestCTEWithDML(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"DROP TABLE IF EXISTS t1",
		"CREATE TABLE t1(x)",
		"INSERT INTO t1 VALUES(1)",
		"INSERT INTO t1 VALUES(2)",
		"INSERT INTO t1 VALUES(3)",
		"INSERT INTO t1 VALUES(4)",
	)

	t.Run("cte_with_delete", func(t *testing.T) {
		mustExec(t, db, `
			WITH dset AS (SELECT 2 UNION ALL SELECT 4)
			DELETE FROM t1 WHERE x IN dset
		`)
		rows := queryRows(t, db, "SELECT * FROM t1")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows after delete, got %d", len(rows))
		}
	})

	t.Run("cte_with_insert", func(t *testing.T) {
		mustExec(t, db, `
			WITH iset AS (SELECT 2 UNION ALL SELECT 4)
			INSERT INTO t1 SELECT * FROM iset
		`)
	})

	t.Run("cte_with_update", func(t *testing.T) {
		mustExec(t, db, `
			WITH uset(a, b) AS (SELECT 2, 8 UNION ALL SELECT 4, 9)
			UPDATE t1 SET x = COALESCE((SELECT b FROM uset WHERE a=x), x)
		`)
	})
}

// TestRecursiveCTEBasic tests basic recursive CTEs from with1.test
func TestRecursiveCTEBasic(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("simple_counter", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH i(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM i)
			SELECT x FROM i LIMIT 10
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		if fmt.Sprint(rows[0][0]) != "1" || fmt.Sprint(rows[9][0]) != "10" {
			t.Errorf("expected 1 to 10, got first=%v, last=%v", rows[0][0], rows[9][0])
		}
	})

	t.Run("counter_with_order_by", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH i(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM i ORDER BY 1)
			SELECT x FROM i LIMIT 10
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
	})

	t.Run("counter_with_modulo", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH i(x) AS (VALUES(1) UNION ALL SELECT (x+1)%10 FROM i)
			SELECT x FROM i LIMIT 20
		`)
		if len(rows) != 20 {
			t.Errorf("expected 20 rows, got %d", len(rows))
		}
	})

	t.Run("counter_with_union", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH i(x) AS (VALUES(1) UNION SELECT (x+1)%10 FROM i)
			SELECT x FROM i LIMIT 20
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows (UNION removes duplicates), got %d", len(rows))
		}
	})

	t.Run("counter_with_limit_in_recursive", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH i(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM i LIMIT 5)
			SELECT x FROM i
		`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})
}

// TestRecursiveCTETreeTraversal tests tree traversal from with1.test
func TestRecursiveCTETreeTraversal(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE f(id INTEGER PRIMARY KEY, parentid REFERENCES f, name TEXT)`,
		`INSERT INTO f VALUES(0, NULL, '')`,
		`INSERT INTO f VALUES(1, 0, 'bin')`,
		`INSERT INTO f VALUES(2, 1, 'true')`,
		`INSERT INTO f VALUES(3, 1, 'false')`,
		`INSERT INTO f VALUES(4, 1, 'ls')`,
		`INSERT INTO f VALUES(5, 1, 'grep')`,
		`INSERT INTO f VALUES(6, 0, 'etc')`,
		`INSERT INTO f VALUES(7, 6, 'rc.d')`,
		`INSERT INTO f VALUES(8, 7, 'rc.apache')`,
		`INSERT INTO f VALUES(9, 7, 'rc.samba')`,
		`INSERT INTO f VALUES(10, 0, 'home')`,
		`INSERT INTO f VALUES(11, 10, 'dan')`,
		`INSERT INTO f VALUES(12, 11, 'public_html')`,
		`INSERT INTO f VALUES(13, 12, 'index.html')`,
		`INSERT INTO f VALUES(14, 13, 'logo.gif')`,
	)

	t.Run("build_file_paths", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH flat(fid, fpath) AS (
				SELECT id, '' FROM f WHERE parentid IS NULL
				UNION ALL
				SELECT id, fpath || '/' || name FROM f, flat WHERE parentid=fid
			)
			SELECT fpath FROM flat WHERE fpath!='' ORDER BY 1
		`)
		if len(rows) < 10 {
			t.Errorf("expected at least 10 file paths, got %d", len(rows))
		}
	})

	t.Run("count_all_nodes", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH flat(fid, fpath) AS (
				SELECT id, '' FROM f WHERE parentid IS NULL
				UNION ALL
				SELECT id, fpath || '/' || name FROM f, flat WHERE parentid=fid
			)
			SELECT count(*) FROM flat
		`)
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "15" {
			t.Errorf("expected 15 nodes, got %v", rows[0][0])
		}
	})
}

// TestRecursiveCTESimpleTree tests simple tree from with1.test
func TestRecursiveCTESimpleTree(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE tree(i, p)`,
		`INSERT INTO tree VALUES(1, NULL)`,
		`INSERT INTO tree VALUES(2, 1)`,
		`INSERT INTO tree VALUES(3, 1)`,
		`INSERT INTO tree VALUES(4, 2)`,
		`INSERT INTO tree VALUES(5, 4)`,
	)

	t.Run("tree_paths", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH t(id, path) AS (
				SELECT i, '' FROM tree WHERE p IS NULL
				UNION ALL
				SELECT i, path || '/' || i FROM tree, t WHERE p = id
			)
			SELECT path FROM t
		`)
		if len(rows) != 5 {
			t.Errorf("expected 5 paths, got %d", len(rows))
		}
	})

	t.Run("subtree_from_node", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH t(id) AS (
				VALUES(2)
				UNION ALL
				SELECT i FROM tree, t WHERE p = id
			)
			SELECT id FROM t
		`)
		if len(rows) != 3 {
			t.Errorf("expected 3 nodes in subtree, got %d", len(rows))
		}
	})

	t.Run("simple_counter_limit", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH x(i) AS (
				SELECT 1
				UNION ALL
				SELECT i+1 FROM x WHERE i<10
			)
			SELECT count(*) FROM x
		`)
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "10" {
			t.Errorf("expected count of 10, got %v", rows[0][0])
		}
	})
}

// TestRecursiveCTEErrors tests recursive CTE errors from with1.test
func TestRecursiveCTEErrors(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE tree(i, p)`,
		`INSERT INTO tree VALUES(1, NULL)`,
		`INSERT INTO tree VALUES(2, 1)`,
	)

	t.Run("circular_subquery_ref", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH t(id) AS (
				VALUES(2)
				UNION ALL
				SELECT i FROM tree WHERE p IN (SELECT id FROM t)
			)
			SELECT id FROM t
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})

	t.Run("multiple_recursive_refs", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH t(id) AS (
				VALUES(2)
				UNION ALL
				SELECT i FROM tree, t WHERE p = id AND p IN (SELECT id FROM t)
			)
			SELECT id FROM t
		`)
		if !strings.Contains(err.Error(), "multiple recursive") {
			t.Errorf("expected multiple recursive references error, got: %v", err)
		}
	})

	t.Run("circular_ref_in_base", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH t(id) AS (
				SELECT i FROM tree WHERE 2 IN (SELECT id FROM t)
				UNION ALL
				SELECT i FROM tree, t WHERE p = id
			)
			SELECT id FROM t
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})
}

// TestRecursiveCTEMandelbrot tests Mandelbrot set from with1.test
func TestRecursiveCTEMandelbrot(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("mandelbrot_set", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				xaxis(x) AS (VALUES(-2.0) UNION ALL SELECT x+0.05 FROM xaxis WHERE x<1.2),
				yaxis(y) AS (VALUES(-1.0) UNION ALL SELECT y+0.1 FROM yaxis WHERE y<1.0),
				m(iter, cx, cy, x, y) AS (
					SELECT 0, x, y, 0.0, 0.0 FROM xaxis, yaxis
					UNION ALL
					SELECT iter+1, cx, cy, x*x-y*y + cx, 2.0*x*y + cy FROM m
					WHERE (x*x + y*y) < 4.0 AND iter<28
				),
				m2(iter, cx, cy) AS (
					SELECT max(iter), cx, cy FROM m GROUP BY cx, cy
				),
				a(t) AS (
					SELECT group_concat(substr(' .+*#', 1+min(iter/7,4), 1), '')
					FROM m2 GROUP BY cy
				)
			SELECT group_concat(rtrim(t), x'0a') FROM a
		`)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})
}

// TestRecursiveCTESudoku tests Sudoku solver from with1.test
func TestRecursiveCTESudoku(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("sudoku_solver", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				input(sud) AS (
					VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')
				),
				digits(z, lp) AS (
					VALUES('1', 1)
					UNION ALL SELECT
					CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
				),
				x(s, ind) AS (
					SELECT sud, instr(sud, '.') FROM input
					UNION ALL
					SELECT
						substr(s, 1, ind-1) || z || substr(s, ind+1),
						instr(substr(s, 1, ind-1) || z || substr(s, ind+1), '.')
					FROM x, digits AS z
					WHERE ind>0
						AND NOT EXISTS (
							SELECT 1
								FROM digits AS lp
							WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
								OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
								OR z.z = substr(s, (((ind-1)/3) % 3) * 3
										+ ((ind-1)/27) * 27 + lp
										+ ((lp-1) / 3) * 6, 1)
						)
				)
			SELECT s FROM x WHERE ind=0
		`)
		if len(rows) != 1 {
			t.Errorf("expected 1 solution, got %d", len(rows))
		}
		expected := "534678912672195348198342567859761423426853791713924856961537284287419635345286179"
		if fmt.Sprint(rows[0][0]) != expected {
			t.Errorf("wrong solution")
		}
	})
}

// TestRecursiveCTELimitOffset tests LIMIT/OFFSET in recursive CTEs from with1.test
func TestRecursiveCTELimitOffset(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	tests := []struct {
		name   string
		limit  int
		offset int
		want   int
	}{
		{"limit_20_offset_0", 20, 0, 20},
		{"limit_0_offset_0", 0, 0, 0},
		{"limit_19_offset_1", 19, 1, 19},
		{"limit_5_offset_5", 5, 5, 5},
		{"limit_40_offset_neg1", 40, -1, 20},
		{"limit_neg1_offset_neg1", -1, -1, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(`
				WITH ii(a) AS (
					VALUES(1)
					UNION ALL
					SELECT a+1 FROM ii WHERE a<20
					LIMIT %d OFFSET %d
				)
				SELECT * FROM ii
			`, tt.limit, tt.offset)
			rows := queryRows(t, db, query)
			if len(rows) != tt.want {
				t.Errorf("expected %d rows, got %d", tt.want, len(rows))
			}
		})
	}
}

// TestRecursiveCTEOrderBy tests ORDER BY in recursive CTEs from with1.test
func TestRecursiveCTEOrderBy(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE org(name TEXT PRIMARY KEY, boss TEXT REFERENCES org) WITHOUT ROWID`,
		`INSERT INTO org VALUES('Alice', NULL)`,
		`INSERT INTO org VALUES('Bob', 'Alice')`,
		`INSERT INTO org VALUES('Cindy', 'Alice')`,
		`INSERT INTO org VALUES('Dave', 'Bob')`,
		`INSERT INTO org VALUES('Emma', 'Bob')`,
		`INSERT INTO org VALUES('Fred', 'Cindy')`,
		`INSERT INTO org VALUES('Gail', 'Cindy')`,
		`INSERT INTO org VALUES('Harry', 'Dave')`,
		`INSERT INTO org VALUES('Ingrid', 'Dave')`,
		`INSERT INTO org VALUES('Jim', 'Emma')`,
		`INSERT INTO org VALUES('Kate', 'Emma')`,
		`INSERT INTO org VALUES('Lanny', 'Fred')`,
		`INSERT INTO org VALUES('Mary', 'Fred')`,
		`INSERT INTO org VALUES('Noland', 'Gail')`,
		`INSERT INTO org VALUES('Olivia', 'Gail')`,
	)

	t.Run("breadth_first_order_by_asc", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				under_alice(name, level) AS (
					VALUES('Alice', '0')
					UNION ALL
					SELECT org.name, under_alice.level+1
					FROM org, under_alice
					WHERE org.boss=under_alice.name
					ORDER BY 2
				)
			SELECT name FROM under_alice
		`)
		if len(rows) < 5 {
			t.Errorf("expected at least 5 employees, got %d", len(rows))
		}
	})

	t.Run("depth_first_order_by_desc", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				under_alice(name, level) AS (
					VALUES('Alice', '0')
					UNION ALL
					SELECT org.name, under_alice.level+1
					FROM org, under_alice
					WHERE org.boss=under_alice.name
					ORDER BY 2 DESC
				)
			SELECT name FROM under_alice
		`)
		if len(rows) < 5 {
			t.Errorf("expected at least 5 employees, got %d", len(rows))
		}
	})

	t.Run("default_fifo_breadth_first", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				under_alice(name, level) AS (
					VALUES('Alice', '0')
					UNION ALL
					SELECT org.name, under_alice.level+1
					FROM org, under_alice
					WHERE org.boss=under_alice.name
				)
			SELECT name FROM under_alice
		`)
		if len(rows) < 5 {
			t.Errorf("expected at least 5 employees, got %d", len(rows))
		}
	})
}

// TestCTECompoundSelects tests CTEs with compound SELECTs from with1.test
func TestCTECompoundSelects(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("except_with_ctes", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				t1(x) AS (VALUES(2) UNION ALL SELECT x+2 FROM t1 WHERE x<20),
				t2(y) AS (VALUES(3) UNION ALL SELECT y+3 FROM t2 WHERE y<20)
			SELECT x FROM t1 EXCEPT SELECT y FROM t2 ORDER BY 1
		`)
		if len(rows) < 5 {
			t.Errorf("expected multiple rows from EXCEPT, got %d", len(rows))
		}
	})
}

// TestCTEColumnWildcards tests column wildcards from with1.test
func TestCTEColumnWildcards(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("select_star_no_table", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH RECURSIVE c(i) AS (SELECT * UNION ALL SELECT i+1 FROM c WHERE i<10)
			SELECT i FROM c
		`)
		if !strings.Contains(err.Error(), "no tables") {
			t.Errorf("expected 'no tables' error, got: %v", err)
		}
	})

	t.Run("select_with_star_mismatch", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH RECURSIVE c(i) AS (SELECT 5,* UNION ALL SELECT i+1 FROM c WHERE i<10)
			SELECT i FROM c
		`)
		if !strings.Contains(err.Error(), "no tables") {
			t.Errorf("expected 'no tables' error, got: %v", err)
		}
	})
}

// TestCTERowidRestriction tests rowid restriction from with1.test
func TestCTERowidRestriction(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("no_rowid_in_recursive_cte", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH RECURSIVE
				d(x) AS (VALUES(1) UNION ALL SELECT rowid+1 FROM d WHERE rowid<10)
			SELECT x FROM d
		`)
		if !strings.Contains(err.Error(), "no such column: rowid") {
			t.Errorf("expected 'no such column: rowid' error, got: %v", err)
		}
	})
}

// TestCTEAggregateRestriction tests aggregate restriction from with1.test
func TestCTEAggregateRestriction(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("recursive_aggregate_not_supported", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH RECURSIVE
				i(x) AS (VALUES(1) UNION SELECT count(*) FROM i)
			SELECT * FROM i
		`)
		if !strings.Contains(err.Error(), "aggregate") {
			t.Errorf("expected aggregate error, got: %v", err)
		}
	})
}

// TestCTENestedCTEs tests nested CTEs from with1.test
func TestCTENestedCTEs(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("nested_cte_basic", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH x(a) AS (
				WITH y(b) AS (SELECT 10)
				SELECT 9 UNION ALL SELECT * FROM y
			)
			SELECT * FROM x
		`)
		want := [][]interface{}{{int64(9)}, {int64(10)}}
		compareRows(t, rows, want)
	})

	t.Run("nested_cte_union_all", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH x AS (
				WITH y(b) AS (SELECT 10)
				SELECT * FROM y UNION ALL SELECT * FROM y
			)
			SELECT * FROM x
		`)
		want := [][]interface{}{{int64(10)}, {int64(10)}}
		compareRows(t, rows, want)
	})

	t.Run("nested_cte_reuse", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH
				x1 AS (SELECT 10),
				x2 AS (SELECT * FROM x1),
				x3 AS (
					WITH x1 AS (SELECT 11)
					SELECT * FROM x2 UNION ALL SELECT * FROM x2
				)
			SELECT * FROM x3
		`)
		want := [][]interface{}{{int64(10)}, {int64(10)}}
		compareRows(t, rows, want)
	})
}

// TestCTEVariables tests CTEs with variables from with2.test
func TestCTEVariables(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("cte_with_params", func(t *testing.T) {
		stmt, err := db.Prepare(`
			WITH i(x) AS (
				VALUES(?)
				UNION ALL
				SELECT x+1 FROM i WHERE x < ?
			)
			SELECT * FROM i
		`)
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer stmt.Close()

		rows, err := stmt.Query(3, 9)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		if count != 7 {
			t.Errorf("expected 7 rows, got %d", count)
		}
	})
}

// TestCTEScalarSubqueries tests CTEs in scalar subqueries from with2.test
func TestCTEScalarSubqueries(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE t1(a)`,
		`INSERT INTO t1 VALUES(1)`,
		`INSERT INTO t1 VALUES(2)`,
	)

	t.Run("cte_in_scalar_subquery", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH x1 AS (SELECT * FROM t1)
			SELECT (SELECT sum(a) FROM x1)
		`)
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "3" {
			t.Errorf("expected sum of 3, got %v", rows[0][0])
		}
	})

	t.Run("cte_multiple_scalar_subqueries", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH x1 AS (SELECT * FROM t1)
			SELECT (SELECT sum(a) FROM x1), (SELECT max(a) FROM x1)
		`)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("cte_scalar_and_from", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH x1 AS (SELECT * FROM t1)
			SELECT (SELECT sum(a) FROM x1), (SELECT max(a) FROM x1), a FROM x1
		`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})
}

// TestCTENestedScopes tests nested CTE scoping from with2.test
func TestCTENestedScopes(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE t1(a)`,
		`INSERT INTO t1 VALUES(1)`,
		`INSERT INTO t1 VALUES(2)`,
	)

	t.Run("nested_cte_scope", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH
				i(x) AS (
					WITH
						j(x) AS (SELECT * FROM i),
						i(x) AS (SELECT * FROM t1)
					SELECT * FROM j
				)
			SELECT * FROM i
		`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("recursive_cte_in_subquery", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH r(i) AS (
				VALUES('.')
				UNION ALL
				SELECT i || '.' FROM r, (
					SELECT x FROM x INTERSECT SELECT y FROM y
				) WHERE length(i) < 10
			),
			x(x) AS (VALUES(1) UNION ALL VALUES(2) UNION ALL VALUES(3)),
			y(y) AS (VALUES(2) UNION ALL VALUES(4) UNION ALL VALUES(6))
			SELECT * FROM r
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
	})

	t.Run("recursive_with_subquery_limit", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH r(i) AS (
				VALUES('.')
				UNION ALL
				SELECT i || '.' FROM r, (SELECT x FROM x WHERE x=2) WHERE length(i) < 10
			),
			x(x) AS (VALUES(1) UNION ALL VALUES(2) UNION ALL VALUES(3))
			SELECT * FROM r ORDER BY length(i) DESC
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
	})
}

// TestCTECircularReferences tests circular reference detection from with2.test
func TestCTECircularReferences(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("direct_circular_ref", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH i(x, y) AS (VALUES(1, (SELECT x FROM i)))
			SELECT * FROM i
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})

	t.Run("three_way_circular_ref", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH
				i(x) AS (SELECT * FROM j),
				j(x) AS (SELECT * FROM k),
				k(x) AS (SELECT * FROM i)
			SELECT * FROM i
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})

	t.Run("nested_circular_ref", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH
				i(x) AS (
					WITH j(x) AS (SELECT * FROM i)
					SELECT * FROM j
				)
			SELECT * FROM i
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})

	t.Run("circular_in_subquery", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH
				i(x) AS (SELECT * FROM (SELECT * FROM j)),
				j(x) AS (SELECT * FROM (SELECT * FROM i))
			SELECT * FROM i
		`)
		if !strings.Contains(err.Error(), "circular reference") {
			t.Errorf("expected circular reference error, got: %v", err)
		}
	})
}

// TestCTERecursiveInIN tests recursive CTEs in IN expressions from with2.test
func TestCTERecursiveInIN(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE t5(x INTEGER)`,
		`CREATE TABLE t6(y INTEGER)`,
		`WITH s(x) AS (VALUES(7) UNION ALL SELECT x+7 FROM s WHERE x<49) INSERT INTO t5 SELECT * FROM s`,
		`WITH s(x) AS (VALUES(2) UNION ALL SELECT x+2 FROM s WHERE x<49) INSERT INTO t6 SELECT * FROM s`,
	)

	t.Run("non_recursive_cte_in_in", func(t *testing.T) {
		rows := queryRows(t, db, `
			SELECT * FROM t6 WHERE y IN (SELECT x FROM t5)
		`)
		if len(rows) != 3 {
			t.Errorf("expected 3 matching rows, got %d", len(rows))
		}
	})

	t.Run("recursive_cte_in_in_clause", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH ss(x) AS (VALUES(7) UNION ALL SELECT x+7 FROM ss WHERE x<49)
			SELECT * FROM t6 WHERE y IN (SELECT x FROM ss)
		`)
		if len(rows) != 3 {
			t.Errorf("expected 3 matching rows, got %d", len(rows))
		}
	})

	t.Run("cte_in_where_in_subquery", func(t *testing.T) {
		rows := queryRows(t, db, `
			SELECT * FROM t6 WHERE y IN (
				WITH ss(x) AS (VALUES(7) UNION ALL SELECT x+7 FROM ss WHERE x<49)
				SELECT x FROM ss
			)
		`)
		if len(rows) != 3 {
			t.Errorf("expected 3 matching rows, got %d", len(rows))
		}
	})
}

// TestCTEFibonacci tests Fibonacci sequence generation
func TestCTEFibonacci(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("fibonacci_sequence", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE fib(n, a, b) AS (
				VALUES(0, 0, 1)
				UNION ALL
				SELECT n+1, b, a+b FROM fib WHERE n < 10
			)
			SELECT a FROM fib
		`)
		if len(rows) != 11 {
			t.Errorf("expected 11 Fibonacci numbers, got %d", len(rows))
		}
		expected := []int64{0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55}
		for i := 0; i < len(expected) && i < len(rows); i++ {
			if fmt.Sprint(rows[i][0]) != fmt.Sprint(expected[i]) {
				t.Errorf("fib[%d]: expected %d, got %v", i, expected[i], rows[i][0])
			}
		}
	})
}

// TestCTEFactorial tests factorial calculation
func TestCTEFactorial(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("factorial_calculation", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE fact(n, f) AS (
				VALUES(1, 1)
				UNION ALL
				SELECT n+1, f*(n+1) FROM fact WHERE n < 10
			)
			SELECT n, f FROM fact
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 factorials, got %d", len(rows))
		}
		lastRow := rows[len(rows)-1]
		if fmt.Sprint(lastRow[0]) != "10" {
			t.Errorf("expected n=10, got %v", lastRow[0])
		}
	})
}

// TestCTEGraphTraversal tests graph traversal with edges table
func TestCTEGraphTraversal(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE edge(xfrom, xto, seq, PRIMARY KEY(xfrom, xto)) WITHOUT ROWID`,
		`INSERT INTO edge VALUES(0, 1, 10)`,
		`INSERT INTO edge VALUES(1, 2, 20)`,
		`INSERT INTO edge VALUES(0, 3, 30)`,
		`INSERT INTO edge VALUES(2, 4, 40)`,
		`INSERT INTO edge VALUES(3, 4, 40)`,
		`INSERT INTO edge VALUES(2, 5, 50)`,
		`INSERT INTO edge VALUES(3, 6, 60)`,
		`INSERT INTO edge VALUES(5, 7, 70)`,
		`INSERT INTO edge VALUES(3, 7, 70)`,
		`INSERT INTO edge VALUES(4, 8, 80)`,
		`INSERT INTO edge VALUES(7, 8, 80)`,
		`INSERT INTO edge VALUES(8, 9, 90)`,
	)

	t.Run("graph_traversal_union", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				ancest(id, mtime) AS (
					VALUES(0, 0)
					UNION
					SELECT edge.xto, edge.seq FROM edge, ancest
					WHERE edge.xfrom=ancest.id
					ORDER BY 2
				)
			SELECT * FROM ancest
		`)
		if len(rows) != 10 {
			t.Errorf("expected 10 unique nodes, got %d", len(rows))
		}
	})

	t.Run("graph_traversal_union_all", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				ancest(id, mtime) AS (
					VALUES(0, 0)
					UNION ALL
					SELECT edge.xto, edge.seq FROM edge, ancest
					WHERE edge.xfrom=ancest.id
					ORDER BY 2
				)
			SELECT * FROM ancest
		`)
		if len(rows) < 10 {
			t.Errorf("expected more than 10 nodes (with duplicates), got %d", len(rows))
		}
	})

	t.Run("graph_traversal_limit_offset", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE
				ancest(id, mtime) AS (
					VALUES(0, 0)
					UNION ALL
					SELECT edge.xto, edge.seq FROM edge, ancest
					WHERE edge.xfrom=ancest.id
					ORDER BY 2 LIMIT 4 OFFSET 2
				)
			SELECT * FROM ancest
		`)
		if len(rows) != 4 {
			t.Errorf("expected 4 nodes, got %d", len(rows))
		}
	})
}

// TestCTEDistinct tests DISTINCT in recursive CTEs from with1.test
func TestCTEDistinct(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE t(label VARCHAR(10), step INTEGER)`,
		`INSERT INTO t VALUES('a', 1)`,
		`INSERT INTO t VALUES('a', 1)`,
		`INSERT INTO t VALUES('b', 1)`,
	)

	t.Run("distinct_in_recursive_cte", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE cte(label, step) AS (
				SELECT DISTINCT * FROM t
				UNION ALL
				SELECT label, step + 1 FROM cte WHERE step < 3
			)
			SELECT * FROM cte ORDER BY +label, +step
		`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
	})

	t.Run("union_removes_duplicates", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH RECURSIVE cte(label, step) AS (
				SELECT * FROM t
				UNION
				SELECT label, step + 1 FROM cte WHERE step < 3
			)
			SELECT * FROM cte ORDER BY +label, +step
		`)
		if len(rows) != 6 {
			t.Errorf("expected 6 unique rows, got %d", len(rows))
		}
	})

	t.Run("distinct_with_tworow_cross", func(t *testing.T) {
		execSQL(t, db,
			`CREATE TABLE tworow(x)`,
			`INSERT INTO tworow(x) VALUES(1),(2)`,
			`DELETE FROM t WHERE rowid=2`,
		)
		rows := queryRows(t, db, `
			WITH RECURSIVE cte(label, step) AS (
				SELECT * FROM t
				UNION ALL
				SELECT DISTINCT label, step + 1 FROM cte, tworow WHERE step < 3
			)
			SELECT * FROM cte ORDER BY +label, +step
		`)
		if len(rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(rows))
		}
	})
}

// TestCTECorrelated tests correlated CTEs from with2.test
func TestCTECorrelated(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("correlated_cte", func(t *testing.T) {
		rows := queryRows(t, db, `
			SELECT 1 AS c WHERE (
				SELECT (
					WITH t1(a) AS (VALUES(c))
					SELECT (SELECT t1a.a FROM t1 AS t1a, t1 AS t1x)
					FROM t1 AS xyz GROUP BY 1
				)
			)
		`)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})
}

// TestCTEEdgeCases tests edge cases from with3.test
func TestCTEEdgeCases(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	t.Run("cte_with_missing_table", func(t *testing.T) {
		err := expectQueryError(t, db, `
			WITH i(x) AS (
				WITH j AS (SELECT 10)
				SELECT 5 FROM t0 UNION SELECT 8 FROM m
			)
			SELECT * FROM i
		`)
		if !strings.Contains(err.Error(), "no such table") {
			t.Errorf("expected no such table error, got: %v", err)
		}
	})

	t.Run("nested_cte_reuse_complex", func(t *testing.T) {
		rows := queryRows(t, db, `
			WITH
				x1 AS (SELECT 10),
				x2 AS (SELECT 11),
				x3 AS (SELECT * FROM x1 UNION ALL SELECT * FROM x2),
				x4 AS (
					WITH
						x1 AS (SELECT 12),
						x2 AS (SELECT 13)
					SELECT * FROM x3
				)
			SELECT * FROM x4
		`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
	})
}

// TestCTEWithViews tests CTEs in views from with4.test
func TestCTEWithViews(t *testing.T) {
	t.Skip("CTE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		`CREATE TABLE main_t1(a, b)`,
		`INSERT INTO main_t1 VALUES(1, 2)`,
	)

	t.Run("cte_in_view", func(t *testing.T) {
		mustExec(t, db, `
			CREATE VIEW v1 AS
			WITH tmp(x, y) AS (VALUES(5, 6))
			SELECT * FROM main_t1, tmp
		`)

		rows := queryRows(t, db, "SELECT * FROM v1")
		if len(rows) != 1 {
			t.Errorf("expected 1 row from view, got %d", len(rows))
		}
	})
}
