// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"strings"
	"testing"
)

// fkTestCase represents a foreign key test case with declarative fields
type fkTestCase struct {
	name        string
	pragmaFK    bool
	setup       []string
	inserts     []string
	verify      string
	wantErr     bool
	errMsg      string
	wantRows    int
	verifyValue interface{}
	wantNull    bool // true to verify the result is NULL (distinct from verifyValue being nil)
}

// TestSQLiteForeignKey is a comprehensive table-driven test suite for foreign key constraints
// Converted from SQLite's TCL foreign key tests (fkey.test, fkey2.test, fkey3.test, fkey4.test, fkey5.test)
func TestSQLiteForeignKey(t *testing.T) {
	tests := []fkTestCase{
		// ===== PRAGMA foreign_keys TESTS =====
		{
			name:        "fk-pragma-1.1: Foreign keys disabled by default",
			pragmaFK:    false,
			setup:       []string{},
			verify:      "PRAGMA foreign_keys",
			verifyValue: int64(0),
		},
		{
			name:        "fk-pragma-1.2: Enable foreign keys",
			pragmaFK:    true,
			setup:       []string{},
			verify:      "PRAGMA foreign_keys",
			verifyValue: int64(1),
		},

		// ===== BASIC FOREIGN KEY DEFINITION TESTS =====
		{
			name:     "fk-basic-1.1: Create table with inline REFERENCES",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
			},
		},
		{
			name:     "fk-basic-1.2: Create table with table-level FOREIGN KEY",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id))",
			},
		},
		{
			name:     "fk-basic-1.3: Self-referencing foreign key",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id))",
			},
		},
		{
			name:     "fk-basic-1.4: Multiple foreign keys in one table",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE a(id INTEGER PRIMARY KEY)",
				"CREATE TABLE b(id INTEGER PRIMARY KEY)",
				"CREATE TABLE c(cid INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), bid INTEGER REFERENCES b(id))",
			},
		},
		{
			name:     "fk-basic-1.5: Composite foreign key",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"CREATE TABLE child(x INTEGER, y INTEGER, FOREIGN KEY(x, y) REFERENCES parent(a, b))",
			},
		},

		// ===== PRAGMA foreign_key_list TESTS =====
		{
			name:     "fk-list-1.1: Query foreign_key_list for simple FK",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id PRIMARY KEY)",
				"CREATE TABLE child(pid REFERENCES parent(id))",
			},
			verify:   "PRAGMA foreign_key_list(child)",
			wantRows: 1,
		},
		{
			name:     "fk-list-1.2: Query foreign_key_list for multiple FKs",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE t1(a PRIMARY KEY)",
				"CREATE TABLE t2(b REFERENCES t1(a), c REFERENCES t1(a))",
			},
			verify:   "PRAGMA foreign_key_list(t2)",
			wantRows: 2,
		},
		{
			name:     "fk-list-1.3: Query foreign_key_list for composite FK",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(x, y, UNIQUE(x, y))",
				"CREATE TABLE child(a, b, FOREIGN KEY(a, b) REFERENCES parent(x, y))",
			},
			verify:   "PRAGMA foreign_key_list(child)",
			wantRows: 2, // Composite FK shows as 2 rows (one per column)
		},

		// ===== INSERT VIOLATION TESTS =====
		{
			name:     "fk-insert-1.1: INSERT with non-existent parent fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
			},
			inserts: []string{"INSERT INTO child VALUES(1, 999)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-insert-1.2: INSERT with NULL foreign key succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
			},
			inserts:  []string{"INSERT INTO child VALUES(1, NULL)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-insert-1.3: INSERT with valid parent succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(100)",
			},
			inserts:  []string{"INSERT INTO child VALUES(1, 100)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-insert-1.4: INSERT when FK disabled allows orphan",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
			},
			inserts:  []string{"INSERT INTO child VALUES(1, 999)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},

		// ===== UPDATE VIOLATION TESTS =====
		{
			name:     "fk-update-1.1: UPDATE child to invalid parent fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"UPDATE child SET pid = 999 WHERE cid = 10"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-update-1.2: UPDATE child to NULL succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"UPDATE child SET pid = NULL WHERE cid = 10"},
			verify:   "SELECT pid FROM child WHERE cid = 10",
			wantNull: true,
		},
		{
			name:     "fk-update-1.3: UPDATE child to another valid parent succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO parent VALUES(2)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:     []string{"UPDATE child SET pid = 2 WHERE cid = 10"},
			verify:      "SELECT pid FROM child WHERE cid = 10",
			verifyValue: int64(2),
		},
		{
			name:     "fk-update-1.4: UPDATE parent key when referenced fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== DELETE VIOLATION TESTS =====
		{
			name:     "fk-delete-1.1: DELETE referenced parent fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"DELETE FROM parent WHERE id = 1"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-delete-1.2: DELETE unreferenced parent succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO parent VALUES(2)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 2"},
			verify:   "SELECT COUNT(*) FROM parent",
			wantRows: 1,
		},
		{
			name:     "fk-delete-1.3: DELETE child with FK succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"DELETE FROM child WHERE cid = 10"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 0,
		},

		// ===== ON DELETE CASCADE TESTS =====
		{
			name:     "fk-cascade-1.1: ON DELETE CASCADE deletes children",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 0,
		},
		{
			name:     "fk-cascade-1.2: ON DELETE CASCADE with multiple parents",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO parent VALUES(2)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 2)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-cascade-1.3: Recursive CASCADE delete",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id) ON DELETE CASCADE)",
				"INSERT INTO tree VALUES(1, NULL)",
				"INSERT INTO tree VALUES(2, 1)",
				"INSERT INTO tree VALUES(3, 2)",
				"INSERT INTO tree VALUES(4, 2)",
			},
			inserts:  []string{"DELETE FROM tree WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM tree",
			wantRows: 0,
		},
		{
			name:     "fk-cascade-1.4: Partial CASCADE (some children have NULL FK)",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, NULL)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},

		// ===== ON DELETE SET NULL TESTS =====
		{
			name:     "fk-setnull-1.1: ON DELETE SET NULL sets FK to NULL",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT pid FROM child WHERE cid = 10",
			wantNull: true,
		},
		{
			name:     "fk-setnull-1.2: ON DELETE SET NULL with multiple children",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child WHERE pid IS NULL",
			wantRows: 2,
		},
		{
			name:     "fk-setnull-1.3: ON DELETE SET NULL preserves child rows",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 2,
		},

		// ===== ON DELETE SET DEFAULT TESTS =====
		{
			name:     "fk-setdefault-1.1: ON DELETE SET DEFAULT sets FK to default value",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER DEFAULT 0 REFERENCES parent(id) ON DELETE SET DEFAULT)",
				"INSERT INTO parent VALUES(0)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:     []string{"DELETE FROM parent WHERE id = 1"},
			verify:      "SELECT pid FROM child WHERE cid = 10",
			verifyValue: int64(0),
		},
		{
			name:     "fk-setdefault-1.2: ON DELETE SET DEFAULT with multiple children",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99 REFERENCES parent(id) ON DELETE SET DEFAULT)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO parent VALUES(99)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child WHERE pid = 99",
			wantRows: 2,
		},

		// ===== ON DELETE RESTRICT TESTS =====
		{
			name:     "fk-restrict-1.1: ON DELETE RESTRICT prevents delete",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE RESTRICT)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"DELETE FROM parent WHERE id = 1"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-restrict-1.2: ON DELETE RESTRICT allows delete of unreferenced",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE RESTRICT)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO parent VALUES(2)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"DELETE FROM parent WHERE id = 2"},
			verify:   "SELECT COUNT(*) FROM parent",
			wantRows: 1,
		},

		// ===== ON DELETE NO ACTION TESTS =====
		{
			name:     "fk-noaction-1.1: ON DELETE NO ACTION prevents delete",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE NO ACTION)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"DELETE FROM parent WHERE id = 1"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== ON UPDATE CASCADE TESTS =====
		{
			name:     "fk-update-cascade-1.1: ON UPDATE CASCADE updates children",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:     []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			verify:      "SELECT pid FROM child WHERE cid = 10",
			verifyValue: int64(2),
		},
		{
			name:     "fk-update-cascade-1.2: ON UPDATE CASCADE with multiple children",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 1)",
			},
			inserts:  []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child WHERE pid = 2",
			wantRows: 2,
		},
		{
			name:     "fk-update-cascade-1.3: Recursive UPDATE CASCADE",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id) ON UPDATE CASCADE)",
				"INSERT INTO tree VALUES(1, NULL)",
				"INSERT INTO tree VALUES(2, 1)",
				"INSERT INTO tree VALUES(3, 2)",
			},
			inserts:  []string{"UPDATE tree SET id = 10 WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM tree WHERE parent_id = 10",
			wantRows: 1,
		},

		// ===== ON UPDATE SET NULL TESTS =====
		{
			name:     "fk-update-setnull-1.1: ON UPDATE SET NULL sets FK to NULL",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE SET NULL)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			verify:   "SELECT pid FROM child WHERE cid = 10",
			wantNull: true,
		},
		{
			name:     "fk-update-setnull-1.2: ON UPDATE SET NULL with multiple children",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE SET NULL)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
				"INSERT INTO child VALUES(20, 1)",
			},
			inserts:  []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM child WHERE pid IS NULL",
			wantRows: 2,
		},

		// ===== ON UPDATE SET DEFAULT TESTS =====
		{
			name:     "fk-update-setdefault-1.1: ON UPDATE SET DEFAULT sets FK to default",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER DEFAULT 0 REFERENCES parent(id) ON UPDATE SET DEFAULT)",
				"INSERT INTO parent VALUES(0)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:     []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			verify:      "SELECT pid FROM child WHERE cid = 10",
			verifyValue: int64(0),
		},

		// ===== ON UPDATE RESTRICT TESTS =====
		{
			name:     "fk-update-restrict-1.1: ON UPDATE RESTRICT prevents update",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE RESTRICT)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== ON UPDATE NO ACTION TESTS =====
		{
			name:     "fk-update-noaction-1.1: ON UPDATE NO ACTION prevents update",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE NO ACTION)",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"UPDATE parent SET id = 2 WHERE id = 1"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== DEFERRABLE INITIALLY DEFERRED TESTS =====
		{
			name:     "fk-deferred-1.1: DEFERRABLE INITIALLY DEFERRED allows violation in transaction",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO child VALUES(10, 999)",
				"INSERT INTO parent VALUES(999)",
				"COMMIT",
			},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-deferred-1.2: DEFERRABLE constraint checked at commit",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO child VALUES(10, 999)",
				"COMMIT",
			},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-deferred-1.3: Self-referencing DEFERRABLE FK",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id) DEFERRABLE INITIALLY DEFERRED)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO tree VALUES(1, 2)",
				"INSERT INTO tree VALUES(2, NULL)",
				"COMMIT",
			},
			verify:   "SELECT COUNT(*) FROM tree",
			wantRows: 2,
		},

		// ===== DEFERRABLE INITIALLY IMMEDIATE TESTS =====
		{
			name:     "fk-immediate-1.1: DEFERRABLE INITIALLY IMMEDIATE checks immediately",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY IMMEDIATE)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO child VALUES(10, 999)",
			},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== COMPOSITE FOREIGN KEY TESTS =====
		{
			name:     "fk-composite-1.1: Composite FK with valid reference",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"CREATE TABLE child(x INTEGER, y INTEGER, FOREIGN KEY(x, y) REFERENCES parent(a, b))",
				"INSERT INTO parent VALUES(1, 2)",
			},
			inserts:  []string{"INSERT INTO child VALUES(1, 2)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-composite-1.2: Composite FK with invalid reference fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"CREATE TABLE child(x INTEGER, y INTEGER, FOREIGN KEY(x, y) REFERENCES parent(a, b))",
				"INSERT INTO parent VALUES(1, 2)",
			},
			inserts: []string{"INSERT INTO child VALUES(1, 3)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-composite-1.3: Composite FK with partial NULL is allowed",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"CREATE TABLE child(x INTEGER, y INTEGER, FOREIGN KEY(x, y) REFERENCES parent(a, b))",
			},
			inserts:  []string{"INSERT INTO child VALUES(NULL, 2)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-composite-1.4: Composite FK CASCADE delete",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"CREATE TABLE child(x INTEGER, y INTEGER, FOREIGN KEY(x, y) REFERENCES parent(a, b) ON DELETE CASCADE)",
				"INSERT INTO parent VALUES(1, 2)",
				"INSERT INTO child VALUES(1, 2)",
			},
			inserts:  []string{"DELETE FROM parent WHERE a = 1 AND b = 2"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 0,
		},

		// ===== SELF-REFERENCING FOREIGN KEY TESTS =====
		{
			name:     "fk-self-1.1: Self-referencing FK with NULL parent",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id))",
			},
			inserts:  []string{"INSERT INTO tree VALUES(1, NULL)"},
			verify:   "SELECT COUNT(*) FROM tree",
			wantRows: 1,
		},
		{
			name:     "fk-self-1.2: Self-referencing FK with valid parent",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id))",
				"INSERT INTO tree VALUES(1, NULL)",
			},
			inserts:  []string{"INSERT INTO tree VALUES(2, 1)"},
			verify:   "SELECT COUNT(*) FROM tree",
			wantRows: 2,
		},
		{
			name:     "fk-self-1.3: Self-referencing FK with invalid parent fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id))",
			},
			inserts: []string{"INSERT INTO tree VALUES(1, 999)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-self-1.4: Self-referencing row (references itself)",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE t(a INTEGER PRIMARY KEY, b REFERENCES t(a))",
			},
			inserts:  []string{"INSERT INTO t VALUES(1, 1)"},
			verify:   "SELECT COUNT(*) FROM t",
			wantRows: 1,
		},

		// ===== MULTIPLE FOREIGN KEY TESTS =====
		{
			name:     "fk-multiple-1.1: Multiple FKs to different parents",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent1(id INTEGER PRIMARY KEY)",
				"CREATE TABLE parent2(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, p1 INTEGER REFERENCES parent1(id), p2 INTEGER REFERENCES parent2(id))",
				"INSERT INTO parent1 VALUES(1)",
				"INSERT INTO parent2 VALUES(2)",
			},
			inserts:  []string{"INSERT INTO child VALUES(10, 1, 2)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-multiple-1.2: Multiple FKs - first FK violation",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent1(id INTEGER PRIMARY KEY)",
				"CREATE TABLE parent2(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, p1 INTEGER REFERENCES parent1(id), p2 INTEGER REFERENCES parent2(id))",
				"INSERT INTO parent2 VALUES(2)",
			},
			inserts: []string{"INSERT INTO child VALUES(10, 999, 2)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-multiple-1.3: Multiple FKs - second FK violation",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent1(id INTEGER PRIMARY KEY)",
				"CREATE TABLE parent2(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, p1 INTEGER REFERENCES parent1(id), p2 INTEGER REFERENCES parent2(id))",
				"INSERT INTO parent1 VALUES(1)",
			},
			inserts: []string{"INSERT INTO child VALUES(10, 1, 999)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-multiple-1.4: Multiple FKs - both NULL",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent1(id INTEGER PRIMARY KEY)",
				"CREATE TABLE parent2(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, p1 INTEGER REFERENCES parent1(id), p2 INTEGER REFERENCES parent2(id))",
			},
			inserts:  []string{"INSERT INTO child VALUES(10, NULL, NULL)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},

		// ===== FOREIGN KEY WITH INDEX TESTS =====
		{
			name:     "fk-index-1.1: FK on column with index",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"CREATE INDEX idx_child_pid ON child(pid)",
				"INSERT INTO parent VALUES(1)",
			},
			inserts:  []string{"INSERT INTO child VALUES(10, 1)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-index-1.2: FK violation with index",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"CREATE INDEX idx_child_pid ON child(pid)",
			},
			inserts: []string{"INSERT INTO child VALUES(10, 999)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== PRAGMA foreign_key_check TESTS =====
		{
			name:     "fk-check-1.1: No violations returns empty result",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			verify:   "PRAGMA foreign_key_check",
			wantRows: 0,
		},
		{
			name:     "fk-check-1.2: Violation detected with FK disabled",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO child VALUES(10, 999)",
			},
			verify:   "PRAGMA foreign_key_check",
			wantRows: 1,
		},
		{
			name:     "fk-check-1.3: Multiple violations",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO child VALUES(10, 999)",
				"INSERT INTO child VALUES(20, 888)",
			},
			verify:   "PRAGMA foreign_key_check",
			wantRows: 2,
		},
		{
			name:     "fk-check-1.4: NULL FK not a violation",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO child VALUES(10, NULL)",
			},
			verify:   "PRAGMA foreign_key_check",
			wantRows: 0,
		},
		{
			name:     "fk-check-1.5: Check specific table",
			pragmaFK: false,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child1(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"CREATE TABLE child2(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO child1 VALUES(10, 999)",
			},
			verify:   "PRAGMA foreign_key_check(child1)",
			wantRows: 1,
		},

		// ===== COLLATION HANDLING TESTS =====
		{
			name:     "fk-collation-1.1: FK uses parent key collation (NOCASE)",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(name TEXT COLLATE NOCASE PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pname TEXT REFERENCES parent(name))",
				"INSERT INTO parent VALUES('SQLite')",
			},
			inserts:  []string{"INSERT INTO child VALUES(1, 'sqlite')"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-collation-1.2: FK delete with NOCASE collation",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(name TEXT COLLATE NOCASE PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pname TEXT REFERENCES parent(name))",
				"INSERT INTO parent VALUES('SQLite')",
				"INSERT INTO child VALUES(1, 'sqlite')",
			},
			inserts: []string{"DELETE FROM parent WHERE name = 'SQLITE'"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== INTEGER PRIMARY KEY / ROWID TESTS =====
		{
			name:     "fk-rowid-1.1: FK references INTEGER PRIMARY KEY (rowid alias)",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1, 'test')",
			},
			inserts:  []string{"INSERT INTO child VALUES(10, 1)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-rowid-1.2: FK references implicit rowid",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent)",
				"INSERT INTO parent VALUES(1, 'test')",
			},
			inserts:  []string{"INSERT INTO child VALUES(10, 1)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},

		// ===== INSERT OR REPLACE WITH FK TESTS =====
		{
			name:     "fk-replace-1.1: INSERT OR REPLACE with FK",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"INSERT OR REPLACE INTO child VALUES(10, 1)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-replace-1.2: INSERT OR REPLACE with invalid FK fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"INSERT OR REPLACE INTO child VALUES(10, 999)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== DROP TABLE WITH FK REFERENCES TESTS =====
		{
			name:     "fk-drop-1.1: DROP parent table with references fails",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts: []string{"DROP TABLE parent"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-drop-1.2: DROP child table succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(10, 1)",
			},
			inserts:  []string{"DROP TABLE child"},
			verify:   "SELECT COUNT(*) FROM parent",
			wantRows: 1,
		},
		{
			name:     "fk-drop-1.3: DROP parent after dropping child succeeds",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"DROP TABLE child",
			},
			inserts: []string{"DROP TABLE parent"},
		},

		// ===== QUOTED TABLE AND COLUMN NAMES TESTS =====
		{
			name:     "fk-quoted-1.1: Quoted table names in FK",
			pragmaFK: true,
			setup: []string{
				`CREATE TABLE "Parent"(id INTEGER PRIMARY KEY)`,
				`CREATE TABLE "Child"(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES "Parent"(id))`,
				`INSERT INTO "Parent" VALUES(1)`,
			},
			inserts:  []string{`INSERT INTO "Child" VALUES(10, 1)`},
			verify:   `SELECT COUNT(*) FROM "Child"`,
			wantRows: 1,
		},
		{
			name:     "fk-quoted-1.2: Quoted column names in FK",
			pragmaFK: true,
			setup: []string{
				`CREATE TABLE parent("ID" INTEGER PRIMARY KEY)`,
				`CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent("ID"))`,
				`INSERT INTO parent VALUES(1)`,
			},
			inserts:  []string{`INSERT INTO child VALUES(10, 1)`},
			verify:   `SELECT COUNT(*) FROM child`,
			wantRows: 1,
		},

		// ===== AFFINITY/TYPE HANDLING TESTS =====
		{
			name:     "fk-affinity-1.1: FK with different affinities but matching values",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid TEXT REFERENCES parent(id))",
				"INSERT INTO parent VALUES(35)",
			},
			inserts:  []string{"INSERT INTO child VALUES(1, '35')"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-affinity-1.2: FK affinity mismatch prevents delete",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid TEXT REFERENCES parent(id))",
				"INSERT INTO parent VALUES(35)",
				"INSERT INTO child VALUES(1, '35.0')",
			},
			inserts: []string{"DELETE FROM parent WHERE id = 35"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},

		// ===== COMPLEX CASCADE SCENARIOS =====
		{
			name:     "fk-complex-1.1: Multi-level CASCADE delete (3 levels)",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY)",
				"CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER REFERENCES t1(id) ON DELETE CASCADE)",
				"CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INTEGER REFERENCES t2(id) ON DELETE CASCADE)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(10, 1)",
				"INSERT INTO t3 VALUES(100, 10)",
			},
			inserts:  []string{"DELETE FROM t1 WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM t3",
			wantRows: 0,
		},
		{
			name:     "fk-complex-1.2: Mixed CASCADE and SET NULL",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY)",
				"CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER REFERENCES t1(id) ON DELETE CASCADE)",
				"CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INTEGER REFERENCES t2(id) ON DELETE SET NULL)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(10, 1)",
				"INSERT INTO t3 VALUES(100, 10)",
			},
			inserts:  []string{"DELETE FROM t1 WHERE id = 1"},
			verify:   "SELECT COUNT(*) FROM t3",
			wantRows: 1,
		},

		// ===== TRANSACTION BOUNDARY TESTS =====
		{
			name:     "fk-transaction-1.1: FK violation in transaction rolls back",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO child VALUES(10, 999)",
			},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
		{
			name:     "fk-transaction-1.2: Valid FK in transaction commits",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO child VALUES(10, 1)",
				"COMMIT",
			},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},

		// ===== WITHOUT ROWID TABLE TESTS =====
		{
			name:     "fk-without-rowid-1.1: FK to WITHOUT ROWID table",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1, 'test')",
			},
			inserts:  []string{"INSERT INTO child VALUES(10, 1)"},
			verify:   "SELECT COUNT(*) FROM child",
			wantRows: 1,
		},
		{
			name:     "fk-without-rowid-1.2: FK violation with WITHOUT ROWID parent",
			pragmaFK: true,
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID",
				"CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
			},
			inserts: []string{"INSERT INTO child VALUES(10, 999)"},
			wantErr: true,
			errMsg:  "FOREIGN KEY constraint failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			runFKTest(t, tt)
		})
	}
}

// runFKTest executes a single foreign key test case
func runFKTest(t *testing.T, tt fkTestCase) {
	t.Helper()
	db := setupMemoryDB(t)
	defer db.Close()

	fkSetPragma(t, db, tt.pragmaFK)
	fkRunSetup(t, db, tt.setup, tt.wantErr, tt.errMsg)
	lastErr := fkRunInserts(t, db, tt.inserts, tt.wantErr)
	fkCheckError(t, lastErr, tt.wantErr, tt.errMsg)

	if !tt.wantErr && tt.verify != "" {
		fkVerifyResults(t, db, tt)
	}
}

// fkSetPragma sets the foreign_keys pragma if requested
func fkSetPragma(t *testing.T, db *sql.DB, pragmaFK bool) {
	t.Helper()
	if pragmaFK {
		mustExec(t, db, "PRAGMA foreign_keys = ON")
	}
}

// fkRunSetup executes setup statements
func fkRunSetup(t *testing.T, db *sql.DB, setup []string, wantErr bool, errMsg string) {
	t.Helper()
	for _, stmt := range setup {
		if wantErr && strings.HasPrefix(stmt, "DROP TABLE") {
			continue
		}
		_, err := db.Exec(stmt)
		if err != nil {
			if !wantErr || !strings.Contains(err.Error(), errMsg) {
				t.Fatalf("setup failed: %v\nstmt: %s", err, stmt)
			}
			return
		}
	}
}

// fkRunInserts executes insert/update/delete statements
func fkRunInserts(t *testing.T, db *sql.DB, inserts []string, wantErr bool) error {
	t.Helper()
	var lastErr error
	for _, stmt := range inserts {
		_, err := db.Exec(stmt)
		if err != nil {
			lastErr = err
			if !wantErr {
				t.Fatalf("unexpected error: %v\nstmt: %s", err, stmt)
			}
			break
		}
	}
	return lastErr
}

// fkCheckError checks error expectations
func fkCheckError(t *testing.T, lastErr error, wantErr bool, errMsg string) {
	t.Helper()
	if wantErr {
		if lastErr == nil {
			t.Fatalf("expected error containing %q, got nil", errMsg)
		}
		if !strings.Contains(lastErr.Error(), errMsg) {
			t.Errorf("expected error containing %q, got: %v", errMsg, lastErr)
		}
	} else if lastErr != nil {
		t.Fatalf("unexpected error: %v", lastErr)
	}
}

// fkVerifyResults verifies query results
func fkVerifyResults(t *testing.T, db *sql.DB, tt fkTestCase) {
	t.Helper()
	if tt.wantNull {
		fkVerifyNull(t, db, tt.verify)
	} else if tt.verifyValue != nil {
		fkVerifySingleValue(t, db, tt.verify, tt.verifyValue)
	} else if tt.wantRows > 0 {
		fkVerifyRowCount(t, db, tt.verify, tt.wantRows)
	} else {
		fkVerifyRowCount(t, db, tt.verify, 0)
	}
}

// fkVerifyNull verifies the query result is NULL
func fkVerifyNull(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	var result sql.NullInt64
	err := db.QueryRow(query).Scan(&result)
	if err != nil {
		t.Fatalf("query failed: %v\nquery: %s", err, query)
	}
	if result.Valid {
		t.Errorf("expected NULL, got %d\nquery: %s", result.Int64, query)
	}
}

// fkVerifySingleValue verifies a single value result
func fkVerifySingleValue(t *testing.T, db *sql.DB, query string, want interface{}) {
	t.Helper()
	var result interface{}
	var resultInt sql.NullInt64
	var resultStr sql.NullString

	err := db.QueryRow(query).Scan(&resultInt)
	if err == nil {
		if resultInt.Valid {
			result = resultInt.Int64
		} else {
			result = nil
		}
	} else {
		err = db.QueryRow(query).Scan(&resultStr)
		if err == nil {
			if resultStr.Valid {
				result = resultStr.String
			} else {
				result = nil
			}
		} else {
			t.Fatalf("verify query failed: %v\nquery: %s", err, query)
		}
	}

	if !valuesEqual(result, want) {
		t.Errorf("verify value mismatch: got %v (%T), want %v (%T)",
			result, result, want, want)
	}
}

// fkVerifyRowCount verifies row count
// For SELECT COUNT(*) queries, it checks the count value returned
// For other queries, it checks the number of rows returned
func fkVerifyRowCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()

	// Handle COUNT(*) queries specially - check the value, not row count
	if strings.Contains(strings.ToUpper(query), "COUNT(*)") {
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("count query failed: %v\nquery: %s", err, query)
		}
		if count != want {
			t.Errorf("row count mismatch: got %d, want %d\nquery: %s",
				count, want, query)
		}
		return
	}

	// For non-COUNT queries, check number of rows returned
	rows := queryRows(t, db, query)
	if len(rows) != want {
		t.Errorf("row count mismatch: got %d, want %d\nquery: %s",
			len(rows), want, query)
	}
}

// TestForeignKey_ComplexScenarios tests complex foreign key scenarios that don't fit table-driven tests
func fkeyCircularDeferred(t *testing.T) {
	t.Helper()
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "PRAGMA foreign_keys = ON")
	mustExec(t, db, `
		CREATE TABLE a(id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES b(id) DEFERRABLE INITIALLY DEFERRED);
		CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id) DEFERRABLE INITIALLY DEFERRED)
	`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if _, err = tx.Exec("INSERT INTO a VALUES(1, 2)"); err != nil {
		t.Fatalf("failed to insert into a: %v", err)
	}
	if _, err = tx.Exec("INSERT INTO b VALUES(2, 1)"); err != nil {
		t.Fatalf("failed to insert into b: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Errorf("expected commit to succeed: %v", err)
	}
	assertRowCount(t, db, "a", 1)
	assertRowCount(t, db, "b", 1)
}

func TestForeignKey_ComplexScenarios(t *testing.T) {
	t.Run("circular-deferred", fkeyCircularDeferred)

	t.Run("cascade-cycle", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, `
			CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER REFERENCES t(a) ON DELETE CASCADE);
			INSERT INTO t VALUES(1, NULL);
			INSERT INTO t VALUES(2, 1);
			INSERT INTO t VALUES(3, 2);
			INSERT INTO t VALUES(4, 3);
			INSERT INTO t VALUES(5, 4)
		`)

		mustExec(t, db, "DELETE FROM t WHERE a = 1")
		assertRowCount(t, db, "t", 0)
	})

	t.Run("fk-check-with-orphans", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = OFF")
		mustExec(t, db, `
			CREATE TABLE parent(id INTEGER PRIMARY KEY);
			CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
			INSERT INTO parent VALUES(1);
			INSERT INTO parent VALUES(2);
			INSERT INTO child VALUES(10, 1);
			INSERT INTO child VALUES(20, 999);
			INSERT INTO child VALUES(30, 888);
			INSERT INTO child VALUES(40, 2)
		`)

		rows := queryRows(t, db, "PRAGMA foreign_key_check(child)")
		if len(rows) != 2 {
			t.Errorf("expected 2 violations, got %d", len(rows))
		}

		mustExec(t, db, "DELETE FROM parent WHERE id = 1")

		rows = queryRows(t, db, "PRAGMA foreign_key_check(child)")
		if len(rows) != 3 {
			t.Errorf("expected 3 violations after delete, got %d", len(rows))
		}
	})

	t.Run("update-cascade-chain", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, `
			CREATE TABLE t1(id INTEGER PRIMARY KEY);
			CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER REFERENCES t1(id) ON UPDATE CASCADE);
			CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INTEGER REFERENCES t2(id) ON UPDATE CASCADE);
			INSERT INTO t1 VALUES(1);
			INSERT INTO t2 VALUES(10, 1);
			INSERT INTO t3 VALUES(100, 10)
		`)

		mustExec(t, db, "UPDATE t1 SET id = 2 WHERE id = 1")

		result := querySingle(t, db, "SELECT t1_id FROM t2 WHERE id = 10")
		if result.(int64) != 2 {
			t.Errorf("expected t2.t1_id = 2, got %v", result)
		}

		result = querySingle(t, db, "SELECT t2_id FROM t3 WHERE id = 100")
		if result.(int64) != 10 {
			t.Errorf("expected t3.t2_id = 10, got %v", result)
		}
	})

	t.Run("composite-fk-partial-match", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, `
			CREATE TABLE parent(a INTEGER, b INTEGER, c INTEGER, UNIQUE(a, b));
			CREATE TABLE child(x INTEGER, y INTEGER, z INTEGER, FOREIGN KEY(x, y) REFERENCES parent(a, b));
			INSERT INTO parent VALUES(1, 2, 3);
			INSERT INTO parent VALUES(1, 3, 4)
		`)

		mustExec(t, db, "INSERT INTO child VALUES(1, 2, 999)")
		mustExec(t, db, "INSERT INTO child VALUES(1, 3, 888)")

		err := expectError(t, db, "INSERT INTO child VALUES(1, 4, 777)")
		if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			t.Errorf("expected FK constraint error, got: %v", err)
		}
	})
}

// TestForeignKey_MismatchErrors tests foreign key mismatch errors
func TestForeignKey_MismatchErrors(t *testing.T) {
	t.Run("no-unique-parent", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, `
			CREATE TABLE parent(id INTEGER, name TEXT);
			CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))
		`)

		_, err := db.Query("PRAGMA foreign_key_check")
		if err == nil {
			t.Error("expected 'foreign key mismatch' error, got nil")
		} else if !strings.Contains(err.Error(), "foreign key mismatch") {
			t.Errorf("expected 'foreign key mismatch' error, got: %v", err)
		}
	})

	t.Run("column-count-mismatch", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		_, err := db.Exec(`
			CREATE TABLE parent(a INTEGER, b INTEGER, PRIMARY KEY(a, b));
			CREATE TABLE child(x INTEGER, FOREIGN KEY(x) REFERENCES parent(a, b))
		`)
		if err == nil {
			t.Error("expected error for column count mismatch, got nil")
		}
	})

	t.Run("missing-parent-table", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = OFF")
		mustExec(t, db, "CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES nonexistent(id))")

		// Insert a row with non-NULL FK (should be reported as violation since parent doesn't exist)
		mustExec(t, db, "INSERT INTO child VALUES(1, 999)")

		rows, err := db.Query("PRAGMA foreign_key_check")
		if err != nil {
			t.Fatalf("unexpected error from foreign_key_check: %v", err)
		}
		defer rows.Close()

		// Should have at least one violation (the row we inserted)
		violations := 0
		for rows.Next() {
			violations++
		}
		if violations == 0 {
			t.Error("expected violations for missing parent table, got 0")
		}
	})
}

// TestForeignKey_EdgeCases tests edge cases and corner scenarios
func TestForeignKey_EdgeCases(t *testing.T) {
	t.Run("fk-to-view-fails", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, `
			CREATE TABLE t(id INTEGER PRIMARY KEY);
			CREATE VIEW v AS SELECT * FROM t
		`)

		_, err := db.Exec("CREATE TABLE child(cid INTEGER, pid INTEGER REFERENCES v(id))")
		if err == nil {
			t.Error("expected error creating FK to view, got nil")
		}
	})

	t.Run("null-not-equal-null", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, `
			CREATE TABLE parent(id INTEGER PRIMARY KEY);
			CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
			INSERT INTO child VALUES(1, NULL);
			INSERT INTO child VALUES(2, NULL)
		`)

		assertRowCount(t, db, "child", 2)
	})

	t.Run("delete-all-with-self-reference", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, `
			CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES t(id));
			INSERT INTO t VALUES(1, NULL);
			INSERT INTO t VALUES(2, 1);
			INSERT INTO t VALUES(3, 1)
		`)

		_, err := db.Exec("DELETE FROM t")
		if err == nil {
			t.Error("expected FK constraint error on DELETE FROM t, got nil")
		}
	})

	t.Run("replace-with-cascade", func(t *testing.T) {
		db := setupMemoryDB(t)
		defer db.Close()

		mustExec(t, db, "PRAGMA foreign_keys = ON")
		mustExec(t, db, `
			CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER REFERENCES t(a) ON DELETE CASCADE);
			INSERT INTO t VALUES(1, NULL);
			INSERT INTO t VALUES(2, 1);
			INSERT INTO t VALUES(3, 2)
		`)

		_, err := db.Exec("INSERT OR REPLACE INTO t VALUES(2, 3)")
		if err == nil {
			t.Error("expected FK constraint error from REPLACE cascade, got nil")
		}
	})
}
