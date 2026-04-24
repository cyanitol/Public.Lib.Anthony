// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import "testing"

// parseExpectSuccess parses sql and returns the first statement, failing on error.
func parseExpectSuccess(t *testing.T, sql string) Statement {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", sql, err)
	}
	if len(stmts) == 0 {
		t.Fatalf("Parse(%q) returned 0 statements", sql)
	}
	return stmts[0]
}

// parseExpectError parses sql and asserts that an error is returned.
func parseExpectError(t *testing.T, sql string) {
	t.Helper()
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Errorf("Parse(%q) expected error, got nil", sql)
	}
}

// assertParseResult parses sql and checks wantErr. If no error, returns statements.
func assertParseResult(t *testing.T, sql string, wantErr bool) []Statement {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()
	if wantErr {
		if err == nil {
			t.Errorf("Parse(%q) expected error, got nil", sql)
		}
		return nil
	}
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", sql, err)
	}
	return stmts
}
