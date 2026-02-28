package driver

import (
	"database/sql"
	"math"
	"path/filepath"
	"testing"
)

// setupExpressionTestDB creates a temporary database for testing expressions
func setupExpressionTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "expression_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create test table with various column types
	_, err = db.Exec(`CREATE TABLE expr_test(
		id INTEGER PRIMARY KEY,
		a INTEGER,
		b INTEGER,
		c REAL,
		d REAL,
		s1 TEXT,
		s2 TEXT,
		flag INTEGER
	)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO expr_test VALUES(1, 10, 20, 1.5, 2.5, 'hello', 'world', 1)`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create a second table for subquery tests
	_, err = db.Exec(`CREATE TABLE lookup(key INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("failed to create lookup table: %v", err)
	}

	_, err = db.Exec(`INSERT INTO lookup VALUES(1, 'first'), (2, 'second'), (3, 'third')`)
	if err != nil {
		t.Fatalf("failed to insert lookup data: %v", err)
	}

	return db
}

// TestSQLiteExpressionEvaluation tests comprehensive SQL expression evaluation
// Converted from SQLite TCL tests, covering all major expression types
func TestSQLiteExpressionEvaluation(t *testing.T) {
	db := setupExpressionTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   string // UPDATE statement to set values (empty string means use defaults)
		query   string // Full SELECT query
		want    interface{}
		wantErr bool
	}{
		// ================================================================
		// ARITHMETIC EXPRESSIONS (+, -, *, /, %)
		// ================================================================
		{
			name:  "arithmetic-add-integers",
			setup: "a=15, b=25",
			query: "SELECT a + b FROM expr_test",
			want:  int64(40),
		},
		{
			name:  "arithmetic-add-negative",
			setup: "a=-5, b=10",
			query: "SELECT a + b FROM expr_test",
			want:  int64(5),
		},
		{
			name:  "arithmetic-subtract",
			setup: "a=50, b=20",
			query: "SELECT a - b FROM expr_test",
			want:  int64(30),
		},
		{
			name:  "arithmetic-subtract-negative-result",
			setup: "a=10, b=30",
			query: "SELECT a - b FROM expr_test",
			want:  int64(-20),
		},
		{
			name:  "arithmetic-multiply",
			setup: "a=6, b=7",
			query: "SELECT a * b FROM expr_test",
			want:  int64(42),
		},
		{
			name:  "arithmetic-multiply-negative",
			setup: "a=-5, b=4",
			query: "SELECT a * b FROM expr_test",
			want:  int64(-20),
		},
		{
			name:  "arithmetic-divide-integer",
			setup: "a=20, b=4",
			query: "SELECT a / b FROM expr_test",
			want:  int64(5),
		},
		{
			name:  "arithmetic-divide-truncate",
			setup: "a=7, b=2",
			query: "SELECT a / b FROM expr_test",
			want:  int64(3),
		},
		{
			name:  "arithmetic-divide-real",
			setup: "c=7.0, d=2.0",
			query: "SELECT c / d FROM expr_test",
			want:  3.5,
		},
		{
			name:  "arithmetic-modulo",
			setup: "a=17, b=5",
			query: "SELECT a % b FROM expr_test",
			want:  int64(2),
		},
		{
			name:  "arithmetic-modulo-negative-dividend",
			setup: "a=-17, b=5",
			query: "SELECT a % b FROM expr_test",
			want:  int64(-2),
		},
		{
			name:  "arithmetic-modulo-negative-divisor",
			setup: "a=17, b=-5",
			query: "SELECT a % b FROM expr_test",
			want:  int64(2),
		},
		{
			name:  "arithmetic-division-by-zero",
			setup: "a=10, b=0",
			query: "SELECT a / b FROM expr_test",
			want:  nil,
		},
		{
			name:  "arithmetic-modulo-by-zero",
			setup: "a=10, b=0",
			query: "SELECT a % b FROM expr_test",
			want:  nil,
		},

		// ================================================================
		// COMPARISON OPERATORS (=, <>, <, >, <=, >=)
		// ================================================================
		{
			name:  "compare-equal-true",
			setup: "a=10, b=10",
			query: "SELECT a = b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-equal-false",
			setup: "a=10, b=20",
			query: "SELECT a = b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-not-equal-true",
			setup: "a=10, b=20",
			query: "SELECT a <> b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-not-equal-false",
			setup: "a=10, b=10",
			query: "SELECT a <> b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-not-equal-alt",
			setup: "a=10, b=20",
			query: "SELECT a != b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-less-than-true",
			setup: "a=10, b=20",
			query: "SELECT a < b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-less-than-false",
			setup: "a=20, b=10",
			query: "SELECT a < b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-less-than-equal-false",
			setup: "a=20, b=20",
			query: "SELECT a < b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-greater-than-true",
			setup: "a=30, b=10",
			query: "SELECT a > b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-greater-than-false",
			setup: "a=10, b=30",
			query: "SELECT a > b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-less-equal-true",
			setup: "a=10, b=20",
			query: "SELECT a <= b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-less-equal-equal",
			setup: "a=20, b=20",
			query: "SELECT a <= b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-less-equal-false",
			setup: "a=30, b=20",
			query: "SELECT a <= b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-greater-equal-true",
			setup: "a=30, b=20",
			query: "SELECT a >= b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-greater-equal-equal",
			setup: "a=20, b=20",
			query: "SELECT a >= b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-greater-equal-false",
			setup: "a=10, b=20",
			query: "SELECT a >= b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "compare-string-equal",
			setup: "s1='test', s2='test'",
			query: "SELECT s1 = s2 FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "compare-string-less-than",
			setup: "s1='abc', s2='xyz'",
			query: "SELECT s1 < s2 FROM expr_test",
			want:  int64(1),
		},

		// ================================================================
		// LOGICAL OPERATORS (AND, OR, NOT)
		// ================================================================
		{
			name:  "logical-and-true-true",
			setup: "a=1, b=1",
			query: "SELECT a AND b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "logical-and-true-false",
			setup: "a=1, b=0",
			query: "SELECT a AND b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-and-false-true",
			setup: "a=0, b=1",
			query: "SELECT a AND b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-and-false-false",
			setup: "a=0, b=0",
			query: "SELECT a AND b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-or-true-true",
			setup: "a=1, b=1",
			query: "SELECT a OR b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "logical-or-true-false",
			setup: "a=1, b=0",
			query: "SELECT a OR b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "logical-or-false-true",
			setup: "a=0, b=1",
			query: "SELECT a OR b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "logical-or-false-false",
			setup: "a=0, b=0",
			query: "SELECT a OR b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-not-true",
			setup: "a=1",
			query: "SELECT NOT a FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-not-false",
			setup: "a=0",
			query: "SELECT NOT a FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "logical-not-nonzero",
			setup: "a=42",
			query: "SELECT NOT a FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-and-with-comparison",
			setup: "a=5, b=10",
			query: "SELECT a < 10 AND b > 5 FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "logical-or-with-comparison",
			setup: "a=15, b=3",
			query: "SELECT a < 10 OR b > 5 FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "logical-complex-expression",
			setup: "a=5, b=10, flag=1",
			query: "SELECT (a < b AND flag = 1) OR (a > b AND flag = 0) FROM expr_test",
			want:  int64(1),
		},

		// ================================================================
		// STRING CONCATENATION (||)
		// ================================================================
		{
			name:  "concat-two-strings",
			setup: "s1='hello', s2='world'",
			query: "SELECT s1 || s2 FROM expr_test",
			want:  "helloworld",
		},
		{
			name:  "concat-with-space",
			setup: "s1='hello', s2='world'",
			query: "SELECT s1 || ' ' || s2 FROM expr_test",
			want:  "hello world",
		},
		{
			name:  "concat-number-string",
			setup: "a=42, s1='answer'",
			query: "SELECT s1 || ': ' || a FROM expr_test",
			want:  "answer: 42",
		},
		{
			name:  "concat-empty-string",
			setup: "s1='test', s2=''",
			query: "SELECT s1 || s2 FROM expr_test",
			want:  "test",
		},
		{
			name:  "concat-multiple",
			setup: "s1='a', s2='b'",
			query: "SELECT s1 || s2 || 'c' || 'd' FROM expr_test",
			want:  "abcd",
		},
		{
			name:  "concat-with-null",
			setup: "s1='hello', s2=NULL",
			query: "SELECT s1 || s2 FROM expr_test",
			want:  nil,
		},

		// ================================================================
		// UNARY OPERATORS (-, +)
		// ================================================================
		{
			name:  "unary-minus-positive",
			setup: "a=10",
			query: "SELECT -a FROM expr_test",
			want:  int64(-10),
		},
		{
			name:  "unary-minus-negative",
			setup: "a=-10",
			query: "SELECT -a FROM expr_test",
			want:  int64(10),
		},
		{
			name:  "unary-minus-zero",
			setup: "a=0",
			query: "SELECT -a FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "unary-plus",
			setup: "a=42",
			query: "SELECT +a FROM expr_test",
			want:  int64(42),
		},
		{
			name:  "unary-minus-expression",
			setup: "a=5, b=3",
			query: "SELECT -(a + b) FROM expr_test",
			want:  int64(-8),
		},
		{
			name:  "double-negation",
			setup: "a=10",
			query: "SELECT -(-a) FROM expr_test",
			want:  int64(10),
		},

		// ================================================================
		// CAST EXPRESSIONS
		// ================================================================
		{
			name:  "cast-int-to-text",
			setup: "a=42",
			query: "SELECT CAST(a AS TEXT) FROM expr_test",
			want:  "42",
		},
		{
			name:  "cast-text-to-int",
			setup: "s1='123'",
			query: "SELECT CAST(s1 AS INTEGER) FROM expr_test",
			want:  int64(123),
		},
		{
			name:  "cast-text-to-real",
			setup: "s1='3.14'",
			query: "SELECT CAST(s1 AS REAL) FROM expr_test",
			want:  3.14,
		},
		{
			name:  "cast-real-to-int",
			setup: "c=3.7",
			query: "SELECT CAST(c AS INTEGER) FROM expr_test",
			want:  int64(3),
		},
		{
			name:  "cast-int-to-real",
			setup: "a=42",
			query: "SELECT CAST(a AS REAL) FROM expr_test",
			want:  42.0,
		},
		{
			name:  "cast-text-leading-zeros",
			setup: "s1='007'",
			query: "SELECT CAST(s1 AS INTEGER) FROM expr_test",
			want:  int64(7),
		},
		{
			name:  "cast-invalid-text-to-int",
			setup: "s1='abc'",
			query: "SELECT CAST(s1 AS INTEGER) FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "cast-null",
			setup: "a=NULL",
			query: "SELECT CAST(a AS TEXT) FROM expr_test",
			want:  nil,
		},

		// ================================================================
		// COALESCE
		// ================================================================
		{
			name:  "coalesce-first-non-null",
			setup: "a=10, b=20",
			query: "SELECT COALESCE(a, b, 99) FROM expr_test",
			want:  int64(10),
		},
		{
			name:  "coalesce-second-non-null",
			setup: "a=NULL, b=20",
			query: "SELECT COALESCE(a, b, 99) FROM expr_test",
			want:  int64(20),
		},
		{
			name:  "coalesce-third-non-null",
			setup: "a=NULL, b=NULL",
			query: "SELECT COALESCE(a, b, 99) FROM expr_test",
			want:  int64(99),
		},
		{
			name:  "coalesce-all-null",
			setup: "a=NULL, b=NULL",
			query: "SELECT COALESCE(a, b, NULL) FROM expr_test",
			want:  nil,
		},
		{
			name:  "coalesce-mixed-types",
			setup: "a=NULL, s1='text'",
			query: "SELECT COALESCE(a, s1) FROM expr_test",
			want:  "text",
		},
		{
			name:  "coalesce-with-expression",
			setup: "a=5, b=NULL",
			query: "SELECT COALESCE(b, a * 2) FROM expr_test",
			want:  int64(10),
		},

		// ================================================================
		// NULLIF
		// ================================================================
		{
			name:  "nullif-equal",
			setup: "a=10, b=10",
			query: "SELECT NULLIF(a, b) FROM expr_test",
			want:  nil,
		},
		{
			name:  "nullif-not-equal",
			setup: "a=10, b=20",
			query: "SELECT NULLIF(a, b) FROM expr_test",
			want:  int64(10),
		},
		{
			name:  "nullif-strings-equal",
			setup: "s1='test', s2='test'",
			query: "SELECT NULLIF(s1, s2) FROM expr_test",
			want:  nil,
		},
		{
			name:  "nullif-strings-not-equal",
			setup: "s1='hello', s2='world'",
			query: "SELECT NULLIF(s1, s2) FROM expr_test",
			want:  "hello",
		},
		{
			name:  "nullif-first-null",
			setup: "a=NULL, b=10",
			query: "SELECT NULLIF(a, b) FROM expr_test",
			want:  nil,
		},
		{
			name:  "nullif-with-zero",
			setup: "a=0, b=0",
			query: "SELECT NULLIF(a, b) FROM expr_test",
			want:  nil,
		},

		// ================================================================
		// CASE WHEN...THEN...ELSE...END
		// ================================================================
		{
			name:  "case-simple-match",
			setup: "a=2",
			query: "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' ELSE 'other' END FROM expr_test",
			want:  "two",
		},
		{
			name:  "case-simple-else",
			setup: "a=99",
			query: "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM expr_test",
			want:  "other",
		},
		{
			name:  "case-simple-no-else",
			setup: "a=99",
			query: "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM expr_test",
			want:  nil,
		},
		{
			name:  "case-searched-true",
			setup: "a=10, b=5",
			query: "SELECT CASE WHEN a > b THEN 'greater' WHEN a < b THEN 'less' ELSE 'equal' END FROM expr_test",
			want:  "greater",
		},
		{
			name:  "case-searched-false",
			setup: "a=3, b=10",
			query: "SELECT CASE WHEN a > b THEN 'greater' WHEN a < b THEN 'less' ELSE 'equal' END FROM expr_test",
			want:  "less",
		},
		{
			name:  "case-searched-equal",
			setup: "a=10, b=10",
			query: "SELECT CASE WHEN a > b THEN 'greater' WHEN a < b THEN 'less' ELSE 'equal' END FROM expr_test",
			want:  "equal",
		},
		{
			name:  "case-nested",
			setup: "a=5",
			query: "SELECT CASE WHEN a < 3 THEN 'low' WHEN a < 7 THEN CASE WHEN a < 5 THEN 'mid-low' ELSE 'mid-high' END ELSE 'high' END FROM expr_test",
			want:  "mid-high",
		},
		{
			name:  "case-with-null",
			setup: "a=NULL",
			query: "SELECT CASE a WHEN 1 THEN 'one' WHEN NULL THEN 'null' ELSE 'other' END FROM expr_test",
			want:  "other",
		},
		{
			name:  "case-null-comparison",
			setup: "a=NULL",
			query: "SELECT CASE WHEN a IS NULL THEN 'null' ELSE 'not null' END FROM expr_test",
			want:  "null",
		},
		{
			name:  "case-return-number",
			setup: "a=1",
			query: "SELECT CASE a WHEN 1 THEN 100 WHEN 2 THEN 200 ELSE 999 END FROM expr_test",
			want:  int64(100),
		},

		// ================================================================
		// EXPRESSION PRECEDENCE
		// ================================================================
		{
			name:  "precedence-add-multiply",
			setup: "a=2, b=3, flag=4",
			query: "SELECT a + b * flag FROM expr_test",
			want:  int64(14),
		},
		{
			name:  "precedence-multiply-add",
			setup: "a=2, b=3, flag=4",
			query: "SELECT a * b + flag FROM expr_test",
			want:  int64(10),
		},
		{
			name:  "precedence-divide-subtract",
			setup: "a=20, b=4, flag=2",
			query: "SELECT a / b - flag FROM expr_test",
			want:  int64(3),
		},
		{
			name:  "precedence-comparison-and",
			setup: "a=5, b=10, flag=1",
			query: "SELECT a < 10 AND b > 5 FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "precedence-or-and",
			setup: "a=1, b=0, flag=1",
			query: "SELECT a OR b AND flag FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "precedence-not-and",
			setup: "a=0, b=1",
			query: "SELECT NOT a AND b FROM expr_test",
			want:  int64(1),
		},

		// ================================================================
		// PARENTHESIZED EXPRESSIONS
		// ================================================================
		{
			name:  "parens-override-precedence",
			setup: "a=2, b=3, flag=4",
			query: "SELECT (a + b) * flag FROM expr_test",
			want:  int64(20),
		},
		{
			name:  "parens-nested",
			setup: "a=2, b=3, flag=4",
			query: "SELECT ((a + b) * flag) / 2 FROM expr_test",
			want:  int64(10),
		},
		{
			name:  "parens-logical",
			setup: "a=1, b=0, flag=1",
			query: "SELECT (a OR b) AND flag FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "parens-comparison",
			setup: "a=5, b=10, flag=3",
			query: "SELECT (a + flag) < b FROM expr_test",
			want:  int64(1),
		},

		// ================================================================
		// BOOLEAN EXPRESSIONS WITH NULL
		// ================================================================
		{
			name:  "null-and-true",
			setup: "a=NULL, b=1",
			query: "SELECT a AND b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "null-and-false",
			setup: "a=NULL, b=0",
			query: "SELECT a AND b FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "null-or-true",
			setup: "a=NULL, b=1",
			query: "SELECT a OR b FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "null-or-false",
			setup: "a=NULL, b=0",
			query: "SELECT a OR b FROM expr_test",
			want:  nil,
		},
		{
			name:  "null-not",
			setup: "a=NULL",
			query: "SELECT NOT a FROM expr_test",
			want:  nil,
		},
		{
			name:  "null-comparison-equal",
			setup: "a=NULL, b=5",
			query: "SELECT a = b FROM expr_test",
			want:  nil,
		},
		{
			name:  "null-comparison-less",
			setup: "a=NULL, b=5",
			query: "SELECT a < b FROM expr_test",
			want:  nil,
		},
		{
			name:  "null-is-null",
			setup: "a=NULL",
			query: "SELECT a IS NULL FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "null-is-not-null",
			setup: "a=NULL",
			query: "SELECT a IS NOT NULL FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "not-null-is-null",
			setup: "a=42",
			query: "SELECT a IS NULL FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "not-null-is-not-null",
			setup: "a=42",
			query: "SELECT a IS NOT NULL FROM expr_test",
			want:  int64(1),
		},

		// ================================================================
		// IN EXPRESSIONS
		// ================================================================
		{
			name:  "in-found",
			setup: "a=2",
			query: "SELECT a IN (1, 2, 3) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "in-not-found",
			setup: "a=5",
			query: "SELECT a IN (1, 2, 3) FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "in-single-value",
			setup: "a=42",
			query: "SELECT a IN (42) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "in-string",
			setup: "s1='hello'",
			query: "SELECT s1 IN ('hello', 'world', 'test') FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "in-with-null",
			setup: "a=2",
			query: "SELECT a IN (1, NULL, 3) FROM expr_test",
			want:  nil,
		},
		{
			name:  "in-null-value",
			setup: "a=NULL",
			query: "SELECT a IN (1, 2, 3) FROM expr_test",
			want:  nil,
		},
		{
			name:  "not-in-found",
			setup: "a=2",
			query: "SELECT a NOT IN (1, 2, 3) FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "not-in-not-found",
			setup: "a=5",
			query: "SELECT a NOT IN (1, 2, 3) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "in-subquery-found",
			setup: "a=2",
			query: "SELECT a IN (SELECT key FROM lookup WHERE key <= 3) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "in-subquery-not-found",
			setup: "a=99",
			query: "SELECT a IN (SELECT key FROM lookup) FROM expr_test",
			want:  int64(0),
		},

		// ================================================================
		// EXISTS SUBQUERY
		// ================================================================
		{
			name:  "exists-true",
			setup: "",
			query: "SELECT EXISTS(SELECT 1 FROM lookup WHERE key = 1) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "exists-false",
			setup: "",
			query: "SELECT EXISTS(SELECT 1 FROM lookup WHERE key = 999) FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "not-exists-true",
			setup: "",
			query: "SELECT NOT EXISTS(SELECT 1 FROM lookup WHERE key = 999) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "not-exists-false",
			setup: "",
			query: "SELECT NOT EXISTS(SELECT 1 FROM lookup WHERE key = 1) FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "exists-correlated",
			setup: "a=2",
			query: "SELECT EXISTS(SELECT 1 FROM lookup WHERE key = expr_test.a) FROM expr_test",
			want:  int64(1),
		},

		// ================================================================
		// SCALAR SUBQUERY IN EXPRESSION
		// ================================================================
		{
			name:  "scalar-subquery-simple",
			setup: "a=1",
			query: "SELECT (SELECT value FROM lookup WHERE key = expr_test.a) FROM expr_test",
			want:  "first",
		},
		{
			name:  "scalar-subquery-in-arithmetic",
			setup: "a=2",
			query: "SELECT a + (SELECT COUNT(*) FROM lookup) FROM expr_test",
			want:  int64(5),
		},
		{
			name:  "scalar-subquery-in-comparison",
			setup: "a=3",
			query: "SELECT a > (SELECT COUNT(*) FROM lookup WHERE key < 3) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "scalar-subquery-null-result",
			setup: "a=999",
			query: "SELECT (SELECT value FROM lookup WHERE key = expr_test.a) FROM expr_test",
			want:  nil,
		},
		{
			name:  "scalar-subquery-in-case",
			setup: "a=1",
			query: "SELECT CASE WHEN (SELECT COUNT(*) FROM lookup) > 0 THEN 'has data' ELSE 'empty' END FROM expr_test",
			want:  "has data",
		},

		// ================================================================
		// EDGE CASES AND TYPE COERCION
		// ================================================================
		{
			name:  "type-coercion-int-real",
			setup: "a=5, c=2.0",
			query: "SELECT a / c FROM expr_test",
			want:  2.5,
		},
		{
			name:  "type-coercion-string-number",
			setup: "s1='100', a=50",
			query: "SELECT s1 + a FROM expr_test",
			want:  int64(150),
		},
		{
			name:  "type-coercion-string-comparison",
			setup: "s1='10', a=10",
			query: "SELECT s1 = a FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "boolean-as-int",
			setup: "a=5, b=10",
			query: "SELECT (a < b) + 10 FROM expr_test",
			want:  int64(11),
		},
		{
			name:  "zero-as-false",
			setup: "a=0",
			query: "SELECT CASE WHEN a THEN 'true' ELSE 'false' END FROM expr_test",
			want:  "false",
		},
		{
			name:  "nonzero-as-true",
			setup: "a=42",
			query: "SELECT CASE WHEN a THEN 'true' ELSE 'false' END FROM expr_test",
			want:  "true",
		},
		{
			name:  "negative-as-true",
			setup: "a=-1",
			query: "SELECT CASE WHEN a THEN 'true' ELSE 'false' END FROM expr_test",
			want:  "true",
		},
		{
			name:  "empty-string-as-int",
			setup: "s1=''",
			query: "SELECT CAST(s1 AS INTEGER) FROM expr_test",
			want:  int64(0),
		},
		{
			name:  "real-overflow",
			setup: "c=1.5, d=0.0",
			query: "SELECT c / d FROM expr_test",
			want:  math.Inf(1),
		},
		{
			name:  "negative-real-overflow",
			setup: "c=-1.5, d=0.0",
			query: "SELECT c / d FROM expr_test",
			want:  math.Inf(-1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup: update the table with test values if needed
			if tt.setup != "" {
				_, err := db.Exec("UPDATE expr_test SET " + tt.setup + " WHERE id = 1")
				if err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			// Execute the query
			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got result: %v", result)
				}
				return
			}

			if err != nil {
				if err == sql.ErrNoRows && tt.want == nil {
					return
				}
				t.Fatalf("query failed: %v (query: %s)", err, tt.query)
			}

			// Handle NULL results
			if result == nil && tt.want == nil {
				return
			}

			if result == nil && tt.want != nil {
				t.Errorf("got nil, want %v (%T)", tt.want, tt.want)
				return
			}

			if result != nil && tt.want == nil {
				t.Errorf("got %v (%T), want nil", result, result)
				return
			}

			// Compare results based on type
			if !compareExpressionValues(result, tt.want) {
				t.Errorf("query = %s\ngot  = %v (type %T)\nwant = %v (type %T)",
					tt.query, result, result, tt.want, tt.want)
			}
		})
	}
}

// TestExpressionBetween tests BETWEEN operator
func TestExpressionBetween(t *testing.T) {
	db := setupExpressionTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup string
		query string
		want  int64
	}{
		{
			name:  "between-true",
			setup: "a=5",
			query: "SELECT a BETWEEN 1 AND 10 FROM expr_test",
			want:  1,
		},
		{
			name:  "between-false-low",
			setup: "a=0",
			query: "SELECT a BETWEEN 1 AND 10 FROM expr_test",
			want:  0,
		},
		{
			name:  "between-false-high",
			setup: "a=15",
			query: "SELECT a BETWEEN 1 AND 10 FROM expr_test",
			want:  0,
		},
		{
			name:  "between-inclusive-low",
			setup: "a=1",
			query: "SELECT a BETWEEN 1 AND 10 FROM expr_test",
			want:  1,
		},
		{
			name:  "between-inclusive-high",
			setup: "a=10",
			query: "SELECT a BETWEEN 1 AND 10 FROM expr_test",
			want:  1,
		},
		{
			name:  "not-between-true",
			setup: "a=15",
			query: "SELECT a NOT BETWEEN 1 AND 10 FROM expr_test",
			want:  1,
		},
		{
			name:  "not-between-false",
			setup: "a=5",
			query: "SELECT a NOT BETWEEN 1 AND 10 FROM expr_test",
			want:  0,
		},
		{
			name:  "between-strings",
			setup: "s1='dog'",
			query: "SELECT s1 BETWEEN 'cat' AND 'fox' FROM expr_test",
			want:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != "" {
				_, err := db.Exec("UPDATE expr_test SET " + tt.setup + " WHERE id = 1")
				if err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			var result int64
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if result != tt.want {
				t.Errorf("got %v, want %v", result, tt.want)
			}
		})
	}
}

// TestExpressionComplexCombinations tests complex expression combinations
func TestExpressionComplexCombinations(t *testing.T) {
	db := setupExpressionTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup string
		query string
		want  interface{}
	}{
		{
			name:  "complex-nested-case",
			setup: "a=85",
			query: `SELECT
				CASE
					WHEN a >= 90 THEN 'A'
					WHEN a >= 80 THEN CASE WHEN a >= 85 THEN 'B+' ELSE 'B' END
					WHEN a >= 70 THEN 'C'
					ELSE 'F'
				END FROM expr_test`,
			want: "B+",
		},
		{
			name:  "complex-arithmetic-with-null",
			setup: "a=10, b=NULL, flag=5",
			query: "SELECT COALESCE(a + b, a * flag) FROM expr_test",
			want:  int64(50),
		},
		{
			name:  "complex-logical-chain",
			setup: "a=5, b=10, flag=1",
			query: "SELECT (a > 0 AND b > 0) AND (a < b OR flag = 0) FROM expr_test",
			want:  int64(1),
		},
		{
			name:  "complex-string-operations",
			setup: "s1='hello', s2='world', a=5",
			query: "SELECT CASE WHEN a > 0 THEN s1 || ' ' || s2 ELSE 'none' END FROM expr_test",
			want:  "hello world",
		},
		{
			name:  "complex-subquery-arithmetic",
			setup: "a=10",
			query: "SELECT a * (SELECT COUNT(*) FROM lookup) + (SELECT MIN(key) FROM lookup) FROM expr_test",
			want:  int64(31),
		},
		{
			name:  "complex-cast-in-expression",
			setup: "s1='42', a=8",
			query: "SELECT CAST(s1 AS INTEGER) + a FROM expr_test",
			want:  int64(50),
		},
		{
			name:  "complex-nullif-coalesce",
			setup: "a=10, b=10, flag=99",
			query: "SELECT COALESCE(NULLIF(a, b), flag) FROM expr_test",
			want:  int64(99),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != "" {
				_, err := db.Exec("UPDATE expr_test SET " + tt.setup + " WHERE id = 1")
				if err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if !compareExpressionValues(result, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", result, result, tt.want, tt.want)
			}
		})
	}
}

// compareExpressionValues compares two values considering SQLite type conversions
func compareExpressionValues(got, want interface{}) bool {
	// Handle nil cases
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}

	// Handle integer comparisons
	gotInt, gotIsInt := expressionToInt64(got)
	wantInt, wantIsInt := expressionToInt64(want)
	if gotIsInt && wantIsInt {
		return gotInt == wantInt
	}

	// Handle float comparisons (including infinity)
	gotFloat, gotIsFloat := expressionToFloat64(got)
	wantFloat, wantIsFloat := expressionToFloat64(want)
	if gotIsFloat && wantIsFloat {
		// Handle infinity
		if math.IsInf(gotFloat, 1) && math.IsInf(wantFloat, 1) {
			return true
		}
		if math.IsInf(gotFloat, -1) && math.IsInf(wantFloat, -1) {
			return true
		}
		// Use epsilon comparison for normal floating point
		const epsilon = 0.0001
		return math.Abs(gotFloat-wantFloat) < epsilon
	}

	// Handle string comparisons
	gotStr, gotIsStr := got.(string)
	wantStr, wantIsStr := want.(string)
	if gotIsStr && wantIsStr {
		return gotStr == wantStr
	}

	// Handle byte slice comparisons (SQLite sometimes returns []byte for strings)
	gotBytes, gotIsBytes := got.([]byte)
	if gotIsBytes && wantIsStr {
		return string(gotBytes) == wantStr
	}
	if gotIsStr {
		wantBytes, wantIsBytes := want.([]byte)
		if wantIsBytes {
			return gotStr == string(wantBytes)
		}
	}

	return false
}

// expressionToInt64 attempts to convert a value to int64
func expressionToInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	}
	return 0, false
}

// expressionToFloat64 attempts to convert a value to float64
func expressionToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	}
	return 0, false
}
