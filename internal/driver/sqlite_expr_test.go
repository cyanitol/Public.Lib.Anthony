// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"math"
	"path/filepath"
	"testing"
)

// setupExprTestDB creates a temporary database for testing expressions
func setupExprTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "expr_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create test table
	_, err = db.Exec(`CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test1 VALUES(1, 2, 1.1, 2.2, 'hello', 'world')`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

// TestSQLiteExpressions tests SQLite expression evaluation
// This test suite is derived from SQLite's test/expr.test
func TestSQLiteExpressions(t *testing.T) {
	db := setupExprTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   string // UPDATE statement to set values (empty string means use defaults)
		expr    string // Expression to evaluate via SELECT
		want    interface{}
		wantErr bool
	}{
		// Arithmetic operators
		{name: "expr-1.1", setup: "i1=10, i2=20", expr: "i1+i2", want: int64(30)},
		{name: "expr-1.2", setup: "i1=10, i2=20", expr: "i1-i2", want: int64(-10)},
		{name: "expr-1.3", setup: "i1=10, i2=20", expr: "i1*i2", want: int64(200)},
		{name: "expr-1.4", setup: "i1=10, i2=20", expr: "i1/i2", want: int64(0)},
		{name: "expr-1.5", setup: "i1=10, i2=20", expr: "i2/i1", want: int64(2)},
		{name: "expr-1.56", setup: "i1=25, i2=11", expr: "i1%i2", want: int64(3)},

		// Comparison operators
		{name: "expr-1.6", setup: "i1=10, i2=20", expr: "i2<i1", want: int64(0)},
		{name: "expr-1.7", setup: "i1=10, i2=20", expr: "i2<=i1", want: int64(0)},
		{name: "expr-1.8", setup: "i1=10, i2=20", expr: "i2>i1", want: int64(1)},
		{name: "expr-1.9", setup: "i1=10, i2=20", expr: "i2>=i1", want: int64(1)},
		{name: "expr-1.10", setup: "i1=10, i2=20", expr: "i2!=i1", want: int64(1)},
		{name: "expr-1.11", setup: "i1=10, i2=20", expr: "i2=i1", want: int64(0)},
		{name: "expr-1.12", setup: "i1=10, i2=20", expr: "i2<>i1", want: int64(1)},
		{name: "expr-1.13", setup: "i1=10, i2=20", expr: "i2==i1", want: int64(0)},
		{name: "expr-1.14", setup: "i1=20, i2=20", expr: "i2<i1", want: int64(0)},
		{name: "expr-1.15", setup: "i1=20, i2=20", expr: "i2<=i1", want: int64(1)},
		{name: "expr-1.16", setup: "i1=20, i2=20", expr: "i2>i1", want: int64(0)},
		{name: "expr-1.17", setup: "i1=20, i2=20", expr: "i2>=i1", want: int64(1)},
		{name: "expr-1.18", setup: "i1=20, i2=20", expr: "i2!=i1", want: int64(0)},
		{name: "expr-1.19", setup: "i1=20, i2=20", expr: "i2=i1", want: int64(1)},
		{name: "expr-1.20", setup: "i1=20, i2=20", expr: "i2<>i1", want: int64(0)},
		{name: "expr-1.21", setup: "i1=20, i2=20", expr: "i2==i1", want: int64(1)},

		// Operator precedence
		{name: "expr-1.22", setup: "i1=1, i2=2, r1=3.0", expr: "i1+i2*r1", want: 7.0},
		{name: "expr-1.23", setup: "i1=1, i2=2, r1=3.0", expr: "(i1+i2)*r1", want: 9.0},

		// Functions
		{name: "expr-1.24", setup: "i1=1, i2=2", expr: "min(i1,i2,i1+i2,i1-i2)", want: int64(-1)},
		{name: "expr-1.25", setup: "i1=1, i2=2", expr: "max(i1,i2,i1+i2,i1-i2)", want: int64(3)},

		// Logical operators - AND
		{name: "expr-1.27", setup: "i1=1, i2=2", expr: "i1==1 AND i2=2", want: int64(1)},
		{name: "expr-1.28", setup: "i1=1, i2=2", expr: "i1=2 AND i2=1", want: int64(0)},
		{name: "expr-1.29", setup: "i1=1, i2=2", expr: "i1=1 AND i2=1", want: int64(0)},
		{name: "expr-1.30", setup: "i1=1, i2=2", expr: "i1=2 AND i2=2", want: int64(0)},

		// Logical operators - OR
		{name: "expr-1.31", setup: "i1=1, i2=2", expr: "i1==1 OR i2=2", want: int64(1)},
		{name: "expr-1.31b", setup: "i1=1", expr: "0 OR 2", want: int64(1)},
		{name: "expr-1.32", setup: "i1=1, i2=2", expr: "i1=2 OR i2=1", want: int64(0)},
		{name: "expr-1.33", setup: "i1=1, i2=2", expr: "i1=1 OR i2=1", want: int64(1)},
		{name: "expr-1.34", setup: "i1=1, i2=2", expr: "i1=2 OR i2=2", want: int64(1)},

		// Logical operators - NOT
		{name: "expr-1.36", setup: "i1=1, i2=0", expr: "not i1", want: int64(0)},
		{name: "expr-1.37", setup: "i1=1, i2=0", expr: "not i2", want: int64(1)},

		// Unary operators
		{name: "expr-1.38", setup: "i1=1", expr: "-i1", want: int64(-1)},
		{name: "expr-1.39", setup: "i1=1", expr: "+i1", want: int64(1)},
		{name: "expr-1.40", setup: "i1=1, i2=2", expr: "+(i2+i1)", want: int64(3)},
		{name: "expr-1.41", setup: "i1=1, i2=2", expr: "-(i2+i1)", want: int64(-3)},

		// Bitwise operators
		{name: "expr-1.42", setup: "i1=1, i2=2", expr: "i1|i2", want: int64(3)},
		{name: "expr-1.42b", setup: "i1=1, i2=2", expr: "4|2", want: int64(6)},
		{name: "expr-1.43", setup: "i1=1, i2=2", expr: "i1&i2", want: int64(0)},
		{name: "expr-1.43b", setup: "i1=1, i2=2", expr: "4&5", want: int64(4)},
		{name: "expr-1.44", setup: "i1=1", expr: "~i1", want: int64(-2)},

		// Bit shift operators
		{name: "expr-1.45a", setup: "i1=1, i2=3", expr: "i1<<i2", want: int64(8)},
		{name: "expr-1.45b", setup: "i1=1, i2=-3", expr: "i1>>i2", want: int64(8)},
		{name: "expr-1.45c", setup: "i1=1, i2=0", expr: "i1<<i2", want: int64(1)},
		{name: "expr-1.46a", setup: "i1=32, i2=3", expr: "i1>>i2", want: int64(4)},
		{name: "expr-1.46b", setup: "i1=32, i2=6", expr: "i1>>i2", want: int64(0)},

		// Large integer comparisons
		{name: "expr-1.47", setup: "i1=9999999999, i2=8888888888", expr: "i1<i2", want: int64(0)},
		{name: "expr-1.48", setup: "i1=9999999999, i2=8888888888", expr: "i1=i2", want: int64(0)},
		{name: "expr-1.49", setup: "i1=9999999999, i2=8888888888", expr: "i1>i2", want: int64(1)},

		// NULL handling in arithmetic
		{name: "expr-1.58", setup: "i1=NULL, i2=1", expr: "coalesce(i1+i2,99)", want: int64(99)},
		{name: "expr-1.59", setup: "i1=1, i2=NULL", expr: "coalesce(i1+i2,99)", want: int64(99)},
		{name: "expr-1.60", setup: "i1=NULL, i2=NULL", expr: "coalesce(i1+i2,99)", want: int64(99)},
		{name: "expr-1.61", setup: "i1=NULL, i2=1", expr: "coalesce(i1-i2,99)", want: int64(99)},
		{name: "expr-1.64", setup: "i1=NULL, i2=1", expr: "coalesce(i1*i2,99)", want: int64(99)},
		{name: "expr-1.67", setup: "i1=NULL, i2=1", expr: "coalesce(i1/i2,99)", want: int64(99)},

		// NULL handling in comparisons
		{name: "expr-1.70", setup: "i1=NULL, i2=1", expr: "coalesce(i1<i2,99)", want: int64(99)},
		{name: "expr-1.71", setup: "i1=1, i2=NULL", expr: "coalesce(i1>i2,99)", want: int64(99)},
		{name: "expr-1.74", setup: "i1=1, i2=NULL", expr: "coalesce(i1!=i2,99)", want: int64(99)},

		// NULL handling in logical operators
		{name: "expr-1.76", setup: "i1=NULL, i2=NULL", expr: "coalesce(not i1,99)", want: int64(99)},
		{name: "expr-1.77", setup: "i1=NULL, i2=NULL", expr: "coalesce(-i1,99)", want: int64(99)},
		{name: "expr-1.79", setup: "i1=NULL, i2=NULL", expr: "coalesce(i1 IS NULL OR i2=5,99)", want: int64(1)},
		{name: "expr-1.81", setup: "i1=NULL, i2=NULL", expr: "coalesce(i1=5 OR i2 IS NULL,99)", want: int64(1)},

		// BETWEEN operator
		{name: "expr-1.86", setup: "i1=3, i2=8", expr: "5 between i1 and i2", want: int64(1)},
		{name: "expr-1.87", setup: "i1=3, i2=8", expr: "5 not between i1 and i2", want: int64(0)},
		{name: "expr-1.88", setup: "i1=3, i2=8", expr: "55 between i1 and i2", want: int64(0)},
		{name: "expr-1.89", setup: "i1=3, i2=8", expr: "55 not between i1 and i2", want: int64(1)},
		{name: "expr-1.92", setup: "i1=3, i2=NULL", expr: "2 between i1 and i2", want: int64(0)},
		{name: "expr-1.93", setup: "i1=3, i2=NULL", expr: "2 not between i1 and i2", want: int64(1)},

		// Division by zero
		{name: "expr-1.108", setup: "i1=0", expr: "1%0", want: nil},
		{name: "expr-1.109", setup: "i1=0", expr: "1/0", want: nil},

		// IS / IS NOT operators
		{name: "expr-1.111", setup: "i1=NULL, i2=8", expr: "i1 IS i2", want: int64(0)},
		{name: "expr-1.112", setup: "i1=NULL, i2=NULL", expr: "i1 IS i2", want: int64(1)},
		{name: "expr-1.113", setup: "i1=6, i2=NULL", expr: "i1 IS i2", want: int64(0)},
		{name: "expr-1.114", setup: "i1=6, i2=6", expr: "i1 IS i2", want: int64(1)},
		{name: "expr-1.119", setup: "i1=NULL, i2=8", expr: "i1 IS NOT i2", want: int64(1)},
		{name: "expr-1.120", setup: "i1=NULL, i2=NULL", expr: "i1 IS NOT i2", want: int64(0)},
		{name: "expr-1.121", setup: "i1=6, i2=NULL", expr: "i1 IS NOT i2", want: int64(1)},
		{name: "expr-1.122", setup: "i1=6, i2=6", expr: "i1 IS NOT i2", want: int64(0)},

		// Real number operations
		{name: "expr-2.1", setup: "r1=1.23, r2=2.34", expr: "r1+r2", want: 3.57},
		{name: "expr-2.2", setup: "r1=1.23, r2=2.34", expr: "r1-r2", want: -1.11},
		{name: "expr-2.3", setup: "r1=1.23, r2=2.34", expr: "r1*r2", want: 2.8782},
		{name: "expr-2.6", setup: "r1=1.23, r2=2.34", expr: "r2<r1", want: int64(0)},
		{name: "expr-2.8", setup: "r1=1.23, r2=2.34", expr: "r2>r1", want: int64(1)},
		{name: "expr-2.10", setup: "r1=1.23, r2=2.34", expr: "r2!=r1", want: int64(1)},
		{name: "expr-2.11", setup: "r1=1.23, r2=2.34", expr: "r2=r1", want: int64(0)},
		{name: "expr-2.19", setup: "r1=2.34, r2=2.34", expr: "r2=r1", want: int64(1)},
		{name: "expr-2.24", setup: "r1=25.0, r2=11.0", expr: "r1%r2", want: 3.0},

		// String comparisons
		{name: "expr-3.1", setup: "t1='abc', t2='xyz'", expr: "t1<t2", want: int64(1)},
		{name: "expr-3.2", setup: "t1='xyz', t2='abc'", expr: "t1<t2", want: int64(0)},
		{name: "expr-3.3", setup: "t1='abc', t2='abc'", expr: "t1<t2", want: int64(0)},
		{name: "expr-3.13", setup: "t1='abc', t2='xyz'", expr: "t1=t2", want: int64(0)},
		{name: "expr-3.15", setup: "t1='abc', t2='abc'", expr: "t1=t2", want: int64(1)},
		{name: "expr-3.19", setup: "t1='abc', t2='xyz'", expr: "t1<>t2", want: int64(1)},
		{name: "expr-3.21", setup: "t1='abc', t2='abc'", expr: "t1<>t2", want: int64(0)},

		// IS NULL / IS NOT NULL
		{name: "expr-3.25", setup: "t1=NULL, t2='hi'", expr: "t1 isnull", want: int64(1)},
		{name: "expr-3.25b", setup: "t1=NULL, t2='hi'", expr: "t1 is null", want: int64(1)},
		{name: "expr-3.26", setup: "t1=NULL, t2='hi'", expr: "t2 isnull", want: int64(0)},
		{name: "expr-3.27", setup: "t1=NULL, t2='hi'", expr: "t1 notnull", want: int64(0)},
		{name: "expr-3.28", setup: "t1=NULL, t2='hi'", expr: "t2 notnull", want: int64(1)},
		{name: "expr-3.28b", setup: "t1=NULL, t2='hi'", expr: "t2 is not null", want: int64(1)},

		// String concatenation
		{name: "expr-3.29", setup: "t1='xyz', t2='abc'", expr: "t1||t2", want: "xyzabc"},
		{name: "expr-3.32", setup: "t1='xyz', t2='abc'", expr: "t1||' hi '||t2", want: "xyz hi abc"},

		// LIKE operator
		{name: "expr-5.1", setup: "t1='abc', t2='xyz'", expr: "t1 LIKE t2", want: int64(0)},
		{name: "expr-5.2a", setup: "t1='abc', t2='abc'", expr: "t1 LIKE t2", want: int64(1)},
		{name: "expr-5.3a", setup: "t1='abc', t2='a_c'", expr: "t1 LIKE t2", want: int64(1)},
		{name: "expr-5.4", setup: "t1='abc', t2='abc_'", expr: "t1 LIKE t2", want: int64(0)},
		{name: "expr-5.5a", setup: "t1='abc', t2='a%c'", expr: "t1 LIKE t2", want: int64(1)},
		{name: "expr-5.5c", setup: "t1='abdc', t2='a%c'", expr: "t1 LIKE t2", want: int64(1)},
		{name: "expr-5.6a", setup: "t1='abxyzzyc', t2='a%c'", expr: "t1 LIKE t2", want: int64(1)},
		{name: "expr-5.7a", setup: "t1='abxyzzy', t2='a%c'", expr: "t1 LIKE t2", want: int64(0)},
		{name: "expr-5.11", setup: "t1='abc', t2='xyz'", expr: "t1 NOT LIKE t2", want: int64(1)},
		{name: "expr-5.12a", setup: "t1='abc', t2='abc'", expr: "t1 NOT LIKE t2", want: int64(0)},

		// GLOB operator
		{name: "expr-6.1", setup: "t1='abc', t2='xyz'", expr: "t1 GLOB t2", want: int64(0)},
		{name: "expr-6.2", setup: "t1='abc', t2='ABC'", expr: "t1 GLOB t2", want: int64(0)},
		{name: "expr-6.4", setup: "t1='abc', t2='a?c'", expr: "t1 GLOB t2", want: int64(1)},
		{name: "expr-6.5", setup: "t1='abc', t2='abc?'", expr: "t1 GLOB t2", want: int64(0)},
		{name: "expr-6.7", setup: "t1='abc', t2='a*c'", expr: "t1 GLOB t2", want: int64(1)},
		{name: "expr-6.8", setup: "t1='abxyzzyc', t2='a*c'", expr: "t1 GLOB t2", want: int64(1)},
		{name: "expr-6.9", setup: "t1='abxyzzy', t2='a*c'", expr: "t1 GLOB t2", want: int64(0)},
		{name: "expr-6.11", setup: "t1='abc', t2='xyz'", expr: "t1 NOT GLOB t2", want: int64(1)},
		{name: "expr-6.12", setup: "t1='abc', t2='abc'", expr: "t1 NOT GLOB t2", want: int64(0)},
		{name: "expr-6.13", setup: "t1='abc', t2='a[bx]c'", expr: "t1 GLOB t2", want: int64(1)},
		{name: "expr-6.14", setup: "t1='abc', t2='a[cx]c'", expr: "t1 GLOB t2", want: int64(0)},
		{name: "expr-6.15", setup: "t1='abc', t2='a[a-d]c'", expr: "t1 GLOB t2", want: int64(1)},
		{name: "expr-6.24", setup: "t1='ac', t2='a*c'", expr: "t1 GLOB t2", want: int64(1)},

		// CASE expressions
		{name: "expr-case.1", setup: "i1=1, i2=2", expr: "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END", want: "ne"},
		{name: "expr-case.2", setup: "i1=2, i2=2", expr: "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END", want: "eq"},
		{name: "expr-case.3", setup: "i1=NULL, i2=2", expr: "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END", want: "ne"},
		{name: "expr-case.5", setup: "i1=2", expr: "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'error' END", want: "two"},
		{name: "expr-case.6", setup: "i1=1", expr: "CASE i1 WHEN 1 THEN 'one' WHEN NULL THEN 'two' ELSE 'error' END", want: "one"},
		{name: "expr-case.9", setup: "i1=3", expr: "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'error' END", want: "error"},
		{name: "expr-case.10", setup: "i1=3", expr: "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END", want: nil},
		{name: "expr-case.11", setup: "i1=null", expr: "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 3 END", want: int64(3)},
		{name: "expr-case.13", setup: "i1=7", expr: "CASE WHEN i1 < 5 THEN 'low' WHEN i1 < 10 THEN 'medium' WHEN i1 < 15 THEN 'high' ELSE 'error' END", want: "medium"},

		// CAST expressions
		{name: "expr-6.67", setup: "t1='01', t2=1", expr: "t1 = t2", want: int64(0)},
		{name: "expr-6.68", setup: "t1='1', t2=1", expr: "t1 = t2", want: int64(1)},
		{name: "expr-6.69", setup: "t1='01', t2=1", expr: "CAST(t1 AS INTEGER) = t2", want: int64(1)},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Setup: update the table with test values
			if tt.setup != "" {
				_, err := db.Exec("UPDATE test1 SET " + tt.setup)
				if err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			// Execute the expression
			var result interface{}
			query := "SELECT " + tt.expr + " FROM test1"
			err := db.QueryRow(query).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got none")
				}
				return
			}

			if err != nil {
				if tt.want == nil && err == sql.ErrNoRows {
					// NULL result is expected
					return
				}
				t.Fatalf("query failed: %v (query: %s)", err, query)
			}

			// Handle NULL results
			if result == nil && tt.want == nil {
				return
			}

			// Compare results
			if !compareExprValues(result, tt.want) {
				t.Errorf("expr = %q\ngot  = %v (type %T)\nwant = %v (type %T)",
					tt.expr, result, result, tt.want, tt.want)
			}
		})
	}
}

// compareExprValues compares two values considering SQLite type conversions
func compareExprValues(got, want interface{}) bool {
	// Handle nil cases
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}

	// Handle integer comparisons
	gotInt, gotIsInt := exprToInt64(got)
	wantInt, wantIsInt := exprToInt64(want)
	if gotIsInt && wantIsInt {
		return gotInt == wantInt
	}

	// Handle float comparisons
	gotFloat, gotIsFloat := exprToFloat64(got)
	wantFloat, wantIsFloat := exprToFloat64(want)
	if gotIsFloat && wantIsFloat {
		// Use epsilon comparison for floating point
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

	return false
}

// exprToInt64 attempts to convert a value to int64
func exprToInt64(v interface{}) (int64, bool) {
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

// exprToFloat64 attempts to convert a value to float64
func exprToFloat64(v interface{}) (float64, bool) {
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
