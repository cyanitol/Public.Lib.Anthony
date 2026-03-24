// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"strings"
	"testing"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// TestParseLiteralValue tests parsing of literal values.
func TestParseLiteralValue(t *testing.T) {
	tests := []struct {
		name     string
		litType  parser.LiteralType
		value    string
		expected interface{}
	}{
		{
			name:     "null literal",
			litType:  parser.LiteralNull,
			value:    "NULL",
			expected: nil,
		},
		{
			name:     "integer literal",
			litType:  parser.LiteralInteger,
			value:    "42",
			expected: int64(42),
		},
		{
			name:     "negative integer",
			litType:  parser.LiteralInteger,
			value:    "-123",
			expected: int64(-123),
		},
		{
			name:     "float literal",
			litType:  parser.LiteralFloat,
			value:    "3.14",
			expected: float64(3.14),
		},
		{
			name:     "string literal with single quotes",
			litType:  parser.LiteralString,
			value:    "'hello'",
			expected: "hello",
		},
		{
			name:     "string literal with double quotes",
			litType:  parser.LiteralString,
			value:    `"world"`,
			expected: "world",
		},
		{
			name:     "empty string",
			litType:  parser.LiteralString,
			value:    "''",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lit := &parser.LiteralExpr{
				Type:  tt.litType,
				Value: tt.value,
			}
			result := parseLiteralValue(lit)

			if tt.expected == nil && result != nil {
				t.Errorf("expected nil, got %v", result)
			} else if tt.expected != nil && result != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

// TestNewDefaultConstraint tests creating default constraints.
func TestNewDefaultConstraint(t *testing.T) {
	tests := []struct {
		name         string
		expr         parser.Expression
		expectedType DefaultType
		expectedVal  interface{}
		shouldError  bool
	}{
		{
			name: "integer literal",
			expr: &parser.LiteralExpr{
				Type:  parser.LiteralInteger,
				Value: "100",
			},
			expectedType: DefaultLiteral,
			expectedVal:  int64(100),
		},
		{
			name: "string literal",
			expr: &parser.LiteralExpr{
				Type:  parser.LiteralString,
				Value: "'default text'",
			},
			expectedType: DefaultLiteral,
			expectedVal:  "default text",
		},
		{
			name: "null literal",
			expr: &parser.LiteralExpr{
				Type:  parser.LiteralNull,
				Value: "NULL",
			},
			expectedType: DefaultLiteral,
			expectedVal:  nil,
		},
		{
			name: "CURRENT_TIME function",
			expr: &parser.FunctionExpr{
				Name: "CURRENT_TIME",
				Args: []parser.Expression{},
			},
			expectedType: DefaultCurrentTime,
		},
		{
			name: "CURRENT_DATE function",
			expr: &parser.FunctionExpr{
				Name: "CURRENT_DATE",
				Args: []parser.Expression{},
			},
			expectedType: DefaultCurrentDate,
		},
		{
			name: "CURRENT_TIMESTAMP function",
			expr: &parser.FunctionExpr{
				Name: "CURRENT_TIMESTAMP",
				Args: []parser.Expression{},
			},
			expectedType: DefaultCurrentTimestamp,
		},
		{
			name: "custom function",
			expr: &parser.FunctionExpr{
				Name: "random",
				Args: []parser.Expression{},
			},
			expectedType: DefaultFunction,
		},
		{
			name: "binary expression",
			expr: &parser.BinaryExpr{
				Op:    parser.OpPlus,
				Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			},
			expectedType: DefaultExpression,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dc, err := NewDefaultConstraint(tt.expr)

			if tt.shouldError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if dc.Type != tt.expectedType {
				t.Errorf("expected type %v, got %v", tt.expectedType, dc.Type)
			}

			if tt.expectedType == DefaultLiteral {
				if dc.LiteralValue != tt.expectedVal {
					t.Errorf("expected literal value %v, got %v", tt.expectedVal, dc.LiteralValue)
				}
			}
		})
	}
}

// Prefix: defEval_
type defEvalTestCase struct {
	name        string
	dcType      DefaultType
	litValue    interface{}
	wantValue   interface{}
	checkFormat func(*testing.T, interface{})
}

func defEval_checkTimeFormat(t *testing.T, val interface{}) {
	t.Helper()
	timeStr, ok := val.(string)
	if !ok {
		t.Fatalf("expected string, got %T", val)
	}
	if len(timeStr) != 8 || timeStr[2] != ':' || timeStr[5] != ':' {
		t.Errorf("invalid time format: %s", timeStr)
	}
}

func defEval_checkDateFormat(t *testing.T, val interface{}) {
	t.Helper()
	dateStr, ok := val.(string)
	if !ok {
		t.Fatalf("expected string, got %T", val)
	}
	if len(dateStr) != 10 || dateStr[4] != '-' || dateStr[7] != '-' {
		t.Errorf("invalid date format: %s", dateStr)
	}
	today := time.Now().Format("2006-01-02")
	if dateStr != today {
		t.Errorf("expected today's date %s, got %s", today, dateStr)
	}
}

func defEval_checkTimestampFormat(t *testing.T, val interface{}) {
	t.Helper()
	tsStr, ok := val.(string)
	if !ok {
		t.Fatalf("expected string, got %T", val)
	}
	if len(tsStr) != 19 {
		t.Errorf("invalid timestamp length: %s", tsStr)
	}
}

func defEval_runTest(t *testing.T, tc defEvalTestCase) {
	t.Helper()
	dc := &DefaultConstraint{Type: tc.dcType, LiteralValue: tc.litValue}
	val, err := dc.Evaluate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.checkFormat != nil {
		tc.checkFormat(t, val)
	} else if val != tc.wantValue {
		t.Errorf("expected %v, got %v", tc.wantValue, val)
	}
}

// TestEvaluate tests evaluating default constraints.
func TestEvaluate(t *testing.T) {
	tests := []defEvalTestCase{
		{name: "literal integer", dcType: DefaultLiteral, litValue: int64(42), wantValue: int64(42)},
		{name: "literal string", dcType: DefaultLiteral, litValue: "hello", wantValue: "hello"},
		{name: "literal null", dcType: DefaultLiteral, litValue: nil, wantValue: nil},
		{name: "CURRENT_TIME", dcType: DefaultCurrentTime, checkFormat: defEval_checkTimeFormat},
		{name: "CURRENT_DATE", dcType: DefaultCurrentDate, checkFormat: defEval_checkDateFormat},
		{name: "CURRENT_TIMESTAMP", dcType: DefaultCurrentTimestamp, checkFormat: defEval_checkTimestampFormat},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			defEval_runTest(t, tc)
		})
	}
}

// TestShouldApplyDefault tests the decision logic for applying defaults.
func TestShouldApplyDefault(t *testing.T) {
	tests := []struct {
		name             string
		valueProvided    bool
		valueIsNull      bool
		columnAllowsNull bool
		expected         bool
	}{
		{
			name:             "no value provided",
			valueProvided:    false,
			valueIsNull:      false,
			columnAllowsNull: true,
			expected:         true,
		},
		{
			name:             "value provided, not null",
			valueProvided:    true,
			valueIsNull:      false,
			columnAllowsNull: true,
			expected:         false,
		},
		{
			name:             "null provided, column allows null",
			valueProvided:    true,
			valueIsNull:      true,
			columnAllowsNull: true,
			expected:         false,
		},
		{
			name:             "null provided, column NOT NULL",
			valueProvided:    true,
			valueIsNull:      true,
			columnAllowsNull: false,
			expected:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldApplyDefault(tt.valueProvided, tt.valueIsNull, tt.columnAllowsNull)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// applyDefaultsHelper calls ApplyDefaults and asserts no error and expected length.
func applyDefaultsHelper(t *testing.T, tableCols []*ColumnInfo, insertCols []string, insertVals []interface{}, wantLen int) []interface{} {
	t.Helper()
	result, err := ApplyDefaults(tableCols, insertCols, insertVals)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != wantLen {
		t.Fatalf("expected %d values, got %d", wantLen, len(result))
	}
	return result
}

func testApplyDefaultsAllCols(t *testing.T) {
	t.Helper()
	tableCols := []*ColumnInfo{
		{Name: "id", AllowsNull: false, DefaultConstraint: &DefaultConstraint{Type: DefaultLiteral, LiteralValue: int64(0)}},
		{Name: "name", AllowsNull: true, DefaultConstraint: &DefaultConstraint{Type: DefaultLiteral, LiteralValue: "unknown"}},
	}
	result := applyDefaultsHelper(t, tableCols, []string{"id", "name"}, []interface{}{int64(1), "Alice"}, 2)
	if result[0] != int64(1) {
		t.Errorf("expected id=1, got %v", result[0])
	}
	if result[1] != "Alice" {
		t.Errorf("expected name='Alice', got %v", result[1])
	}
}

func testApplyDefaultsMissingWithDefault(t *testing.T) {
	t.Helper()
	tableCols := []*ColumnInfo{
		{Name: "id", AllowsNull: false, DefaultConstraint: nil},
		{Name: "created_at", AllowsNull: false, DefaultConstraint: &DefaultConstraint{Type: DefaultCurrentTimestamp}},
	}
	result := applyDefaultsHelper(t, tableCols, []string{"id"}, []interface{}{int64(1)}, 2)
	if result[0] != int64(1) {
		t.Errorf("expected id=1, got %v", result[0])
	}
	if ts, ok := result[1].(string); !ok || len(ts) != 19 {
		t.Errorf("expected timestamp string, got %v (%T)", result[1], result[1])
	}
}

func testApplyDefaultsNullNotNull(t *testing.T) {
	t.Helper()
	tableCols := []*ColumnInfo{
		{Name: "status", AllowsNull: false, DefaultConstraint: &DefaultConstraint{Type: DefaultLiteral, LiteralValue: "active"}},
	}
	result := applyDefaultsHelper(t, tableCols, []string{"status"}, []interface{}{nil}, 1)
	if result[0] != "active" {
		t.Errorf("expected 'active', got %v", result[0])
	}
}

func testApplyDefaultsNullNullable(t *testing.T) {
	t.Helper()
	tableCols := []*ColumnInfo{
		{Name: "description", AllowsNull: true, DefaultConstraint: &DefaultConstraint{Type: DefaultLiteral, LiteralValue: "no description"}},
	}
	result := applyDefaultsHelper(t, tableCols, []string{"description"}, []interface{}{nil}, 1)
	if result[0] != nil {
		t.Errorf("expected nil, got %v", result[0])
	}
}

func testApplyDefaultsMissingNoDefault(t *testing.T) {
	t.Helper()
	tableCols := []*ColumnInfo{
		{Name: "id", AllowsNull: true, DefaultConstraint: nil},
		{Name: "name", AllowsNull: true, DefaultConstraint: nil},
	}
	result := applyDefaultsHelper(t, tableCols, []string{"id"}, []interface{}{int64(1)}, 2)
	if result[1] != nil {
		t.Errorf("expected nil for missing column without default, got %v", result[1])
	}
}

func testApplyDefaultsCaseInsensitive(t *testing.T) {
	t.Helper()
	tableCols := []*ColumnInfo{
		{Name: "ID", AllowsNull: false, DefaultConstraint: &DefaultConstraint{Type: DefaultLiteral, LiteralValue: int64(0)}},
	}
	result := applyDefaultsHelper(t, tableCols, []string{"id"}, []interface{}{int64(42)}, 1)
	if result[0] != int64(42) {
		t.Errorf("expected 42, got %v", result[0])
	}
}

// TestApplyDefaults tests applying defaults to INSERT operations.
func TestApplyDefaults(t *testing.T) {
	t.Run("all columns specified", func(t *testing.T) { testApplyDefaultsAllCols(t) })
	t.Run("missing column with default", func(t *testing.T) { testApplyDefaultsMissingWithDefault(t) })
	t.Run("null for NOT NULL column with default", func(t *testing.T) { testApplyDefaultsNullNotNull(t) })
	t.Run("null for nullable column with default", func(t *testing.T) { testApplyDefaultsNullNullable(t) })
	t.Run("missing column without default", func(t *testing.T) { testApplyDefaultsMissingNoDefault(t) })
	t.Run("case insensitive column names", func(t *testing.T) { testApplyDefaultsCaseInsensitive(t) })
}

// TestIntegrationDefaultConstraint tests a more realistic scenario.
func TestIntegrationDefaultConstraint(t *testing.T) {
	// Simulate a table with multiple columns and various default types
	tableCols := []*ColumnInfo{
		{
			Name:              "id",
			AllowsNull:        false,
			DefaultConstraint: nil, // Will be auto-generated (not tested here)
		},
		{
			Name:       "username",
			AllowsNull: false,
			DefaultConstraint: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: "guest",
			},
		},
		{
			Name:       "email",
			AllowsNull: true,
			DefaultConstraint: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: nil,
			},
		},
		{
			Name:       "active",
			AllowsNull: false,
			DefaultConstraint: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: int64(1),
			},
		},
		{
			Name:       "created_at",
			AllowsNull: false,
			DefaultConstraint: &DefaultConstraint{
				Type: DefaultCurrentTimestamp,
			},
		},
	}

	// INSERT with partial columns
	insertCols := []string{"id", "email"}
	insertVals := []interface{}{int64(123), "user@example.com"}

	result, err := ApplyDefaults(tableCols, insertCols, insertVals)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify results
	if result[0] != int64(123) {
		t.Errorf("id: expected 123, got %v", result[0])
	}

	if result[1] != "guest" {
		t.Errorf("username: expected 'guest', got %v", result[1])
	}

	if result[2] != "user@example.com" {
		t.Errorf("email: expected 'user@example.com', got %v", result[2])
	}

	if result[3] != int64(1) {
		t.Errorf("active: expected 1, got %v", result[3])
	}

	// Check timestamp format
	if ts, ok := result[4].(string); !ok {
		t.Errorf("created_at: expected string timestamp, got %T", result[4])
	} else if !strings.Contains(ts, "-") || !strings.Contains(ts, ":") {
		t.Errorf("created_at: invalid timestamp format: %s", ts)
	}
}
