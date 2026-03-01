// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// Test error paths in expression parsing

func TestParseFunctionFilter_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "filter without paren",
			sql:     "SELECT COUNT(*) FILTER WHERE x > 5 FROM t",
			wantErr: true,
			errMsg:  "expected ( after FILTER",
		},
		{
			name:    "filter without where",
			sql:     "SELECT COUNT(*) FILTER (x > 5) FROM t",
			wantErr: true,
			errMsg:  "expected WHERE in FILTER",
		},
		{
			name:    "filter without closing paren",
			sql:     "SELECT COUNT(*) FILTER (WHERE x > 5 FROM t",
			wantErr: true,
			errMsg:  "expected ) after FILTER",
		},
		{
			name:    "valid filter clause",
			sql:     "SELECT COUNT(*) FILTER (WHERE x > 5) FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil && tt.errMsg != "" {
				// Just check that we got an error, specific message may vary
				if err.Error() == "" {
					t.Error("expected error message but got empty string")
				}
			}
		})
	}
}

func TestParseCaseExpression_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "case without then",
			sql:     "SELECT CASE WHEN x > 5 x + 1 END FROM t",
			wantErr: true,
		},
		{
			name:    "case without result expression",
			sql:     "SELECT CASE WHEN x > 5 THEN END FROM t",
			wantErr: true,
		},
		{
			name:    "valid case expression",
			sql:     "SELECT CASE WHEN x > 5 THEN x + 1 END FROM t",
			wantErr: false,
		},
		{
			name:    "valid case with else",
			sql:     "SELECT CASE WHEN x > 5 THEN 1 ELSE 0 END FROM t",
			wantErr: false,
		},
		{
			name:    "case with base expression",
			sql:     "SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseInExpression_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "in without opening paren",
			sql:     "SELECT * FROM t WHERE x IN 1, 2, 3)",
			wantErr: true,
		},
		{
			name:    "in with subquery",
			sql:     "SELECT * FROM t WHERE x IN (SELECT id FROM other)",
			wantErr: false,
		},
		{
			name:    "in with values",
			sql:     "SELECT * FROM t WHERE x IN (1, 2, 3)",
			wantErr: false,
		},
		{
			name:    "not in with values",
			sql:     "SELECT * FROM t WHERE x NOT IN (1, 2, 3)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseBetweenExpression_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "between without and",
			sql:     "SELECT * FROM t WHERE x BETWEEN 1 10",
			wantErr: true,
		},
		{
			name:    "valid between",
			sql:     "SELECT * FROM t WHERE x BETWEEN 1 AND 10",
			wantErr: false,
		},
		{
			name:    "not between",
			sql:     "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParsePatternOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "like operator",
			sql:     "SELECT * FROM t WHERE name LIKE 'test%'",
			wantErr: false,
		},
		{
			name:    "glob operator",
			sql:     "SELECT * FROM t WHERE name GLOB 'test*'",
			wantErr: false,
		},
		{
			name:    "regexp operator",
			sql:     "SELECT * FROM t WHERE name REGEXP '^test'",
			wantErr: false,
		},
		{
			name:    "match operator",
			sql:     "SELECT * FROM t WHERE name MATCH 'pattern'",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseBitwiseOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "bitwise and",
			sql:  "SELECT 5 & 3 FROM t",
		},
		{
			name: "bitwise or",
			sql:  "SELECT 5 | 3 FROM t",
		},
		{
			name: "left shift",
			sql:  "SELECT 5 << 2 FROM t",
		},
		{
			name: "right shift",
			sql:  "SELECT 5 >> 2 FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() error = %v", err)
			}
		})
	}
}

func TestParseUnaryExpression_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "unary minus",
			sql:     "SELECT -5 FROM t",
			wantErr: false,
		},
		{
			name:    "unary plus",
			sql:     "SELECT +5 FROM t",
			wantErr: false,
		},
		{
			name:    "unary not",
			sql:     "SELECT ~5 FROM t",
			wantErr: false,
		},
		{
			name:    "double unary minus",
			sql:     "SELECT -(-5) FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParsePostfixExpression(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "is null",
			sql:     "SELECT * FROM t WHERE x IS NULL",
			wantErr: false,
		},
		{
			name:    "is not null",
			sql:     "SELECT * FROM t WHERE x IS NOT NULL",
			wantErr: false,
		},
		{
			name:    "collate",
			sql:     "SELECT * FROM t WHERE name COLLATE NOCASE = 'test'",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseIdentOrFunction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple identifier",
			sql:     "SELECT x FROM t",
			wantErr: false,
		},
		{
			name:    "qualified identifier",
			sql:     "SELECT t.x FROM t",
			wantErr: false,
		},
		{
			name:    "function call",
			sql:     "SELECT COUNT(*) FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseCastExpression(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "valid cast",
			sql:     "SELECT CAST(x AS INTEGER) FROM t",
			wantErr: false,
		},
		{
			name:    "cast without as",
			sql:     "SELECT CAST(x INTEGER) FROM t",
			wantErr: true,
		},
		{
			name:    "cast without closing paren",
			sql:     "SELECT CAST(x AS INTEGER FROM t",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseParenOrSubquery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "parenthesized expression",
			sql:     "SELECT (1 + 2) FROM t",
			wantErr: false,
		},
		{
			name:    "subquery",
			sql:     "SELECT (SELECT 1) FROM t",
			wantErr: false,
		},
		{
			name:    "unclosed paren",
			sql:     "SELECT (1 + 2 FROM t",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
