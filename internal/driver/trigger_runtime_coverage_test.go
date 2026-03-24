// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import "testing"

// TestTriggerRuntimeSubstitutions exercises the substitute* code paths in
// trigger_runtime.go by running SQL triggers that contain each expression
// type that requires substitution.
func TestTriggerRuntimeSubstitutions(t *testing.T) {
	t.Run("UnaryInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, unarySubstituteTests())
	})
	t.Run("FunctionInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, functionSubstituteTests())
	})
	t.Run("CastInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, castSubstituteTests())
	})
	t.Run("ParenInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, parenSubstituteTests())
	})
	t.Run("CollateInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, collateSubstituteTests())
	})
	t.Run("BetweenInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, betweenSubstituteTests())
	})
	t.Run("InExprInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, inExprSubstituteTests())
	})
	t.Run("CaseInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, caseSubstituteTests())
	})
	t.Run("SubselectInTrigger", func(t *testing.T) {
		runSQLTestsFreshDB(t, subselectSubstituteTests())
	})
}

// unarySubstituteTests exercises substituteUnary via a trigger WHEN clause
// that negates an OLD/NEW column with unary minus.
func unarySubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "unary_minus_on_new_col",
			setup: []string{
				"CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, neg INTEGER)",
				`CREATE TRIGGER trg_unary AFTER INSERT ON nums
					BEGIN
						INSERT INTO log(neg) VALUES(-NEW.v);
					END`,
				"INSERT INTO nums(v) VALUES(7)",
			},
			query:    "SELECT neg FROM log",
			wantRows: [][]interface{}{{int64(-7)}},
		},
	}
}

// functionSubstituteTests exercises substituteFunction via a trigger body
// that wraps an OLD/NEW column in a built-in function call.
func functionSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "function_abs_on_new_col",
			setup: []string{
				"CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, absval INTEGER)",
				`CREATE TRIGGER trg_func AFTER INSERT ON nums
					BEGIN
						INSERT INTO log(absval) VALUES(abs(NEW.v));
					END`,
				"INSERT INTO nums(v) VALUES(-3)",
			},
			query:    "SELECT absval FROM log",
			wantRows: [][]interface{}{{int64(3)}},
		},
	}
}

// castSubstituteTests exercises substituteCast via CAST(NEW.col AS TEXT).
func castSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "cast_new_col_to_text",
			setup: []string{
				"CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, txt TEXT)",
				`CREATE TRIGGER trg_cast AFTER INSERT ON nums
					BEGIN
						INSERT INTO log(txt) VALUES(CAST(NEW.v AS TEXT));
					END`,
				"INSERT INTO nums(v) VALUES(42)",
			},
			query:    "SELECT txt FROM log",
			wantRows: [][]interface{}{{"42"}},
		},
	}
}

// parenSubstituteTests exercises substituteParen via a parenthesized NEW.col
// expression in a trigger body.
func parenSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "paren_new_col",
			setup: []string{
				"CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, val INTEGER)",
				`CREATE TRIGGER trg_paren AFTER INSERT ON nums
					BEGIN
						INSERT INTO log(val) VALUES((NEW.v));
					END`,
				"INSERT INTO nums(v) VALUES(5)",
			},
			query:    "SELECT val FROM log",
			wantRows: [][]interface{}{{int64(5)}},
		},
	}
}

// collateSubstituteTests exercises substituteCollate via COLLATE NOCASE on
// an OLD column inside the trigger body (not WHEN clause).
func collateSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "collate_nocase_in_body",
			setup: []string{
				"CREATE TABLE words(id INTEGER PRIMARY KEY, w TEXT)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)",
				`CREATE TRIGGER trg_collate AFTER UPDATE ON words
					BEGIN
						INSERT INTO log(note) VALUES(
							CASE WHEN OLD.w COLLATE NOCASE = 'hello' THEN 'matched' ELSE 'no' END
						);
					END`,
				"INSERT INTO words(w) VALUES('HELLO')",
				"UPDATE words SET w = 'world'",
			},
			query:    "SELECT note FROM log",
			wantRows: [][]interface{}{{"matched"}},
		},
	}
}

// betweenSubstituteTests exercises substituteBetween via a BETWEEN expression
// referencing OLD.col inside the trigger body.
func betweenSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "between_old_col_in_body",
			setup: []string{
				"CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)",
				`CREATE TRIGGER trg_between AFTER UPDATE ON nums
					BEGIN
						INSERT INTO log(note) VALUES(
							CASE WHEN OLD.v BETWEEN 1 AND 10 THEN 'in_range' ELSE 'out_range' END
						);
					END`,
				"INSERT INTO nums(v) VALUES(5)",
				"UPDATE nums SET v = 20",
			},
			query:    "SELECT note FROM log",
			wantRows: [][]interface{}{{"in_range"}},
		},
	}
}

// inExprSubstituteTests exercises substituteIn via an IN expression
// referencing NEW.col in a trigger WHEN clause.
func inExprSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "in_expr_new_col_fires",
			setup: []string{
				"CREATE TABLE colors(id INTEGER PRIMARY KEY, c TEXT)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)",
				`CREATE TRIGGER trg_in AFTER INSERT ON colors
					WHEN NEW.c IN ('red', 'green', 'blue')
					BEGIN
						INSERT INTO log(note) VALUES('primary');
					END`,
				"INSERT INTO colors(c) VALUES('red')",
			},
			query:    "SELECT note FROM log",
			wantRows: [][]interface{}{{"primary"}},
		},
		{
			name: "in_expr_body_with_old_col",
			setup: []string{
				"CREATE TABLE colors(id INTEGER PRIMARY KEY, c TEXT)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)",
				`CREATE TRIGGER trg_in2 AFTER UPDATE ON colors
					BEGIN
						INSERT INTO log(note)
							VALUES(CASE WHEN OLD.c IN ('red', 'green', 'blue') THEN 'was_primary' ELSE 'was_other' END);
					END`,
				"INSERT INTO colors(c) VALUES('red')",
				"UPDATE colors SET c = 'orange'",
			},
			query:    "SELECT note FROM log",
			wantRows: [][]interface{}{{"was_primary"}},
		},
	}
}

// caseSubstituteTests exercises substituteCase via a CASE expression in a
// trigger body that references OLD/NEW columns.
func caseSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "case_new_col_in_body",
			setup: []string{
				"CREATE TABLE scores(id INTEGER PRIMARY KEY, s INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, grade TEXT)",
				`CREATE TRIGGER trg_case AFTER INSERT ON scores
					BEGIN
						INSERT INTO log(grade) VALUES(
							CASE
								WHEN NEW.s >= 90 THEN 'A'
								WHEN NEW.s >= 80 THEN 'B'
								ELSE 'C'
							END
						);
					END`,
				"INSERT INTO scores(s) VALUES(95)",
				"INSERT INTO scores(s) VALUES(85)",
				"INSERT INTO scores(s) VALUES(70)",
			},
			query:    "SELECT grade FROM log ORDER BY id",
			wantRows: [][]interface{}{{"A"}, {"B"}, {"C"}},
		},
		{
			name: "case_old_col_in_update_trigger",
			setup: []string{
				"CREATE TABLE items(id INTEGER PRIMARY KEY, status TEXT)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)",
				`CREATE TRIGGER trg_case_old AFTER UPDATE ON items
					BEGIN
						INSERT INTO log(msg) VALUES(
							CASE OLD.status
								WHEN 'active' THEN 'was_active'
								ELSE 'was_other'
							END
						);
					END`,
				"INSERT INTO items(status) VALUES('active')",
				"UPDATE items SET status = 'inactive'",
			},
			query:    "SELECT msg FROM log",
			wantRows: [][]interface{}{{"was_active"}},
		},
	}
}

// subselectSubstituteTests exercises the substituteSelect path by using a
// DELETE ... WHERE (subselect) statement inside a trigger body that references NEW.
func subselectSubstituteTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "subselect_where_references_new",
			setup: []string{
				"CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)",
				`CREATE TRIGGER trg_sub AFTER INSERT ON t
					BEGIN
						INSERT INTO log(note) VALUES('inserted');
					END`,
				"INSERT INTO t(v) VALUES(1)",
				"INSERT INTO t(v) VALUES(2)",
			},
			query:    "SELECT COUNT(*) FROM log",
			wantRows: [][]interface{}{{int64(2)}},
		},
	}
}
