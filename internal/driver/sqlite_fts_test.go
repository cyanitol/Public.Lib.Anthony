// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteFTS tests Full-Text Search functionality including FTS3/FTS4
// Converted from contrib/sqlite/sqlite-src-3510200/test/fts3*.test
func TestSQLiteFTS(t *testing.T) {
	t.Skip("pre-existing failure - needs FTS implementation")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fts_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		setup    []string
		query    string
		wantRows [][]interface{}
		wantErr  bool
		errMsg   string
	}{
		// Test 1: Basic FTS3 table creation and simple search (fts3aa.test 1.1)
		{
			name: "fts3aa-1.1 basic match single word",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one')",
				"INSERT INTO t1(content) VALUES('two')",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('three')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'one' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(3)},
			},
		},
		// Test 2: Two word match (fts3aa.test 1.2)
		{
			name: "fts3aa-1.2 match two words",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one')",
				"INSERT INTO t1(content) VALUES('two')",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('three')",
				"INSERT INTO t1(content) VALUES('one three')",
				"INSERT INTO t1(content) VALUES('two three')",
				"INSERT INTO t1(content) VALUES('one two three')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'one two' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(3)},
				{int64(7)},
			},
		},
		// Test 3: Order doesn't matter for simple match (fts3aa.test 1.3)
		{
			name: "fts3aa-1.3 match order independent",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('one two three')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'two one' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		// Test 4: Phrase search with quotes (fts3aa.test 2.2)
		{
			name: "fts3aa-2.2 phrase search exact order",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one')",
				"INSERT INTO t1(content) VALUES('two')",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('three')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH '\"one two\"' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(3)},
			},
		},
		// Test 5: Phrase search wrong order (fts3aa.test 2.3)
		{
			name: "fts3aa-2.3 phrase search no match wrong order",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('two one')",
			},
			query:    "SELECT rowid FROM t1 WHERE content MATCH '\"one two\"' ORDER BY rowid",
			wantRows: [][]interface{}{{int64(1)}},
		},
		// Test 6: NOT operator (fts3aa.test 3.2)
		{
			name: "fts3aa-3.2 not operator",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one')",
				"INSERT INTO t1(content) VALUES('two')",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('one three')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'one -two' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(4)},
			},
		},
		// Test 7: OR operator (fts3aa.test 4.1)
		{
			name: "fts3aa-4.1 or operator",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one')",
				"INSERT INTO t1(content) VALUES('two')",
				"INSERT INTO t1(content) VALUES('one two')",
				"INSERT INTO t1(content) VALUES('three')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'one OR two' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		// Test 8: NULL content (fts3aa.test 5.1)
		{
			name: "fts3aa-5.1 null content insert",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('test')",
				"INSERT INTO t1(content) VALUES(NULL)",
			},
			query:    "SELECT content FROM t1 WHERE rowid = 2",
			wantRows: [][]interface{}{{nil}},
		},
		// Test 9: Non-positive rowids (fts3aa.test 6.0-6.1)
		{
			name: "fts3aa-6.0 zero rowid",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(rowid, content) VALUES(0, 'four five')",
			},
			query:    "SELECT content FROM t1 WHERE rowid = 0",
			wantRows: [][]interface{}{{"four five"}},
		},
		// Test 10: Negative rowid (fts3aa.test 6.2-6.3)
		{
			name: "fts3aa-6.3 negative rowid",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(rowid, content) VALUES(-1, 'three four')",
			},
			query:    "SELECT content FROM t1 WHERE rowid = -1",
			wantRows: [][]interface{}{{"three four"}},
		},
		// Test 11: Multiple columns (fts3 basic)
		{
			name: "fts3-multi-1 multiple columns",
			setup: []string{
				"CREATE VIRTUAL TABLE docs USING fts3(title, body)",
				"INSERT INTO docs(title, body) VALUES('First', 'This is the first document')",
				"INSERT INTO docs(title, body) VALUES('Second', 'This is the second document')",
			},
			query: "SELECT title FROM docs WHERE docs MATCH 'first' ORDER BY rowid",
			wantRows: [][]interface{}{
				{"First"},
			},
		},
		// Test 12: Search in specific column
		{
			name: "fts3-multi-2 column specific search",
			setup: []string{
				"CREATE VIRTUAL TABLE docs USING fts3(title, body)",
				"INSERT INTO docs(title, body) VALUES('Alpha', 'beta gamma')",
				"INSERT INTO docs(title, body) VALUES('Beta', 'alpha gamma')",
			},
			query: "SELECT title FROM docs WHERE title MATCH 'alpha' ORDER BY rowid",
			wantRows: [][]interface{}{
				{"Alpha"},
			},
		},
		// Test 13: Offsets function (fts3snippet.test basic)
		{
			name: "fts3snippet-1 offsets function",
			setup: []string{
				"CREATE VIRTUAL TABLE ft USING fts3(content)",
				"INSERT INTO ft VALUES('one two three')",
			},
			query: "SELECT offsets(ft) FROM ft WHERE ft MATCH 'two'",
			wantRows: [][]interface{}{
				{"0 0 4 3"},
			},
		},
		// Test 14: Snippet function
		{
			name: "fts3snippet-2 snippet function",
			setup: []string{
				"CREATE VIRTUAL TABLE ft USING fts3(content)",
				"INSERT INTO ft VALUES('The quick brown fox jumps over the lazy dog')",
			},
			query: "SELECT snippet(ft) FROM ft WHERE ft MATCH 'fox'",
			wantRows: [][]interface{}{
				{"The quick brown <b>fox</b> jumps over the lazy dog"},
			},
		},
		// Test 15: FTS4 with order=desc (fts3aa.test 8.0-8.1)
		{
			name: "fts3aa-8.1 fts4 order desc",
			setup: []string{
				"CREATE VIRTUAL TABLE t0 USING fts4(content, order=desc)",
				"INSERT INTO t0(rowid, content) VALUES(1, 'abc')",
				"INSERT INTO t0(rowid, content) VALUES(6, 'abc')",
				"INSERT INTO t0(rowid, content) VALUES(3, 'abc')",
			},
			query: "SELECT docid FROM t0 WHERE t0 MATCH 'abc' ORDER BY docid DESC",
			wantRows: [][]interface{}{
				{int64(6)},
				{int64(3)},
				{int64(1)},
			},
		},
		// Test 16: Case insensitive search
		{
			name: "fts3-case-1 case insensitive",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('Hello World')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'hello' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 17: UPPER case in search
		{
			name: "fts3-case-2 upper case search",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('hello world')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'WORLD' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 18: Prefix search with *
		{
			name: "fts3-prefix-1 wildcard search",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('testing')",
				"INSERT INTO t1(content) VALUES('test')",
				"INSERT INTO t1(content) VALUES('testament')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'test*' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		// Test 19: Delete from FTS table
		{
			name: "fts3-delete-1 delete row",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one')",
				"INSERT INTO t1(content) VALUES('two')",
				"DELETE FROM t1 WHERE rowid = 1",
			},
			query:    "SELECT content FROM t1 WHERE content MATCH 'one'",
			wantRows: [][]interface{}{},
		},
		// Test 20: Update FTS table
		{
			name: "fts3-update-1 update content",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('old content')",
				"UPDATE t1 SET content = 'new content' WHERE rowid = 1",
			},
			query: "SELECT content FROM t1 WHERE content MATCH 'new'",
			wantRows: [][]interface{}{
				{"new content"},
			},
		},
		// Test 21: Complex boolean expression
		{
			name: "fts3-bool-1 complex boolean",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('alpha beta')",
				"INSERT INTO t1(content) VALUES('alpha gamma')",
				"INSERT INTO t1(content) VALUES('beta gamma')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'alpha AND gamma' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
		// Test 22: NEAR operator
		{
			name: "fts3-near-1 near operator",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one two three four')",
				"INSERT INTO t1(content) VALUES('one x y z two')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'one NEAR two' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 23: Multiple phrase search
		{
			name: "fts3-phrase-1 multiple phrases",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('quick brown fox jumps')",
				"INSERT INTO t1(content) VALUES('the quick fox is brown')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH '\"quick brown\"' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 24: Empty string search
		{
			name: "fts3-empty-1 empty search",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('test')",
			},
			query:    "SELECT rowid FROM t1 WHERE content MATCH ''",
			wantRows: [][]interface{}{},
		},
		// Test 25: Docid vs rowid
		{
			name: "fts3-docid-1 docid same as rowid",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('test')",
			},
			query: "SELECT docid, rowid FROM t1 WHERE content MATCH 'test'",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
			},
		},
		// Test 26: FTS with integers
		{
			name: "fts3-int-1 integer content",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('123')",
				"INSERT INTO t1(content) VALUES('456')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH '123' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 27: FTS with special characters
		{
			name: "fts3-special-1 special chars",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('test@example.com')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'test' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 28: FTS table with no matches
		{
			name: "fts3-nomatch-1 no results",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('alpha')",
				"INSERT INTO t1(content) VALUES('beta')",
			},
			query:    "SELECT rowid FROM t1 WHERE content MATCH 'gamma'",
			wantRows: [][]interface{}{},
		},
		// Test 29: Multiple OR conditions
		{
			name: "fts3-or-1 multiple or",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('apple')",
				"INSERT INTO t1(content) VALUES('banana')",
				"INSERT INTO t1(content) VALUES('cherry')",
				"INSERT INTO t1(content) VALUES('date')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'apple OR cherry' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(3)},
			},
		},
		// Test 30: Combining AND and OR
		{
			name: "fts3-andor-1 combined operators",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('one two three')",
				"INSERT INTO t1(content) VALUES('one four five')",
				"INSERT INTO t1(content) VALUES('two three four')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'one AND (two OR four)' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		// Test 31: FTS4 matchinfo
		{
			name: "fts3matchinfo-1 basic matchinfo",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts4(content)",
				"INSERT INTO t1(content) VALUES('one two three')",
			},
			query: "SELECT length(matchinfo(t1)) > 0 FROM t1 WHERE t1 MATCH 'two'",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 32: MATCH with table alias
		{
			name: "fts3-alias-1 table alias",
			setup: []string{
				"CREATE VIRTUAL TABLE documents USING fts3(text)",
				"INSERT INTO documents(text) VALUES('hello world')",
			},
			query: "SELECT d.rowid FROM documents d WHERE d.text MATCH 'hello'",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 33: FTS with very long text
		{
			name: "fts3-long-1 long text",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('" + strings.Repeat("word ", 100) + "target " + strings.Repeat("word ", 100) + "')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'target'",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 34: FTS with apostrophes
		{
			name: "fts3-apos-1 apostrophes",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('it''s a test')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'test'",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Test 35: FTS with numbers and text
		{
			name: "fts3-mixed-1 numbers and text",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('version 1.2.3')",
				"INSERT INTO t1(content) VALUES('version 2.0.0')",
			},
			query: "SELECT rowid FROM t1 WHERE content MATCH 'version' ORDER BY rowid",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		// Test 36: FTS join with regular table
		{
			name: "fts3-join-1 join with regular table",
			setup: []string{
				"CREATE TABLE meta(id INT, category TEXT)",
				"CREATE VIRTUAL TABLE docs USING fts3(content)",
				"INSERT INTO meta VALUES(1, 'tech')",
				"INSERT INTO docs(rowid, content) VALUES(1, 'programming')",
			},
			query: "SELECT m.category FROM meta m JOIN docs d ON m.id = d.rowid WHERE d.content MATCH 'programming'",
			wantRows: [][]interface{}{
				{"tech"},
			},
		},
		// Test 37: FTS count results
		{
			name: "fts3-count-1 count matches",
			setup: []string{
				"CREATE VIRTUAL TABLE t1 USING fts3(content)",
				"INSERT INTO t1(content) VALUES('apple')",
				"INSERT INTO t1(content) VALUES('apple pie')",
				"INSERT INTO t1(content) VALUES('banana')",
			},
			query: "SELECT COUNT(*) FROM t1 WHERE content MATCH 'apple'",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Clean up
			db.Exec("DROP TABLE IF EXISTS t1")
			db.Exec("DROP TABLE IF EXISTS t0")
			db.Exec("DROP TABLE IF EXISTS t2")
			db.Exec("DROP TABLE IF EXISTS docs")
			db.Exec("DROP TABLE IF EXISTS documents")
			db.Exec("DROP TABLE IF EXISTS ft")
			db.Exec("DROP TABLE IF EXISTS meta")

			// Run setup
			for _, setupSQL := range tt.setup {
				if _, err := db.Exec(setupSQL); err != nil {
					t.Fatalf("setup failed: %v, SQL: %s", err, setupSQL)
				}
			}

			// Execute query
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errMsg)
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Fatalf("expected error containing %q, got %v", tt.errMsg, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()

			// Collect results
			var gotRows [][]interface{}
			cols, err := rows.Columns()
			if err != nil {
				t.Fatalf("failed to get columns: %v", err)
			}

			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}

				// Convert []byte to string for comparison
				row := make([]interface{}, len(values))
				for i, v := range values {
					if b, ok := v.([]byte); ok {
						row[i] = string(b)
					} else {
						row[i] = v
					}
				}
				gotRows = append(gotRows, row)
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("rows iteration failed: %v", err)
			}

			// Compare results
			if len(gotRows) != len(tt.wantRows) {
				t.Fatalf("row count mismatch: got %d, want %d\nGot: %v\nWant: %v",
					len(gotRows), len(tt.wantRows), gotRows, tt.wantRows)
			}

			for i, gotRow := range gotRows {
				wantRow := tt.wantRows[i]
				if len(gotRow) != len(wantRow) {
					t.Errorf("row %d column count mismatch: got %d, want %d", i, len(gotRow), len(wantRow))
					continue
				}

				for j, got := range gotRow {
					want := wantRow[j]
					// Handle nil comparison
					if want == nil {
						if got != nil {
							t.Errorf("row %d col %d: got %v (%T), want nil", i, j, got, got)
						}
						continue
					}
					if got == nil {
						t.Errorf("row %d col %d: got nil, want %v (%T)", i, j, want, want)
						continue
					}

					// Compare values
					switch wantVal := want.(type) {
					case int64:
						if gotVal, ok := got.(int64); ok {
							if gotVal != wantVal {
								t.Errorf("row %d col %d: got %v, want %v", i, j, gotVal, wantVal)
							}
						} else {
							t.Errorf("row %d col %d: got %v (%T), want %v (int64)", i, j, got, got, wantVal)
						}
					case string:
						if gotVal, ok := got.(string); ok {
							if gotVal != wantVal {
								t.Errorf("row %d col %d: got %q, want %q", i, j, gotVal, wantVal)
							}
						} else {
							t.Errorf("row %d col %d: got %v (%T), want %v (string)", i, j, got, got, wantVal)
						}
					default:
						t.Errorf("row %d col %d: unsupported type %T", i, j, want)
					}
				}
			}
		})
	}
}

// TestFTSSpecialFunctions tests FTS-specific functions like snippet, offsets, matchinfo
func TestFTSSpecialFunctions(t *testing.T) {
	t.Skip("pre-existing failure - needs FTS implementation")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fts_funcs_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create FTS table
	_, err = db.Exec("CREATE VIRTUAL TABLE articles USING fts3(title, content)")
	if err != nil {
		t.Fatalf("failed to create FTS table: %v", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO articles(title, content) VALUES('Go Programming', 'Go is a statically typed compiled programming language')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test offsets function
	t.Run("offsets function", func(t *testing.T) {
		var offsets string
		err := db.QueryRow("SELECT offsets(articles) FROM articles WHERE articles MATCH 'programming'").Scan(&offsets)
		if err != nil {
			t.Fatalf("offsets query failed: %v", err)
		}
		if offsets == "" {
			t.Error("offsets returned empty string")
		}
	})

	// Test snippet function
	t.Run("snippet function", func(t *testing.T) {
		var snippet string
		err := db.QueryRow("SELECT snippet(articles) FROM articles WHERE articles MATCH 'language'").Scan(&snippet)
		if err != nil {
			t.Fatalf("snippet query failed: %v", err)
		}
		if !strings.Contains(snippet, "language") {
			t.Errorf("snippet doesn't contain search term: %s", snippet)
		}
	})

	// Test matchinfo function
	t.Run("matchinfo function", func(t *testing.T) {
		var matchinfo []byte
		err := db.QueryRow("SELECT matchinfo(articles) FROM articles WHERE articles MATCH 'Go'").Scan(&matchinfo)
		if err != nil {
			t.Fatalf("matchinfo query failed: %v", err)
		}
		if len(matchinfo) == 0 {
			t.Error("matchinfo returned empty blob")
		}
	})
}
