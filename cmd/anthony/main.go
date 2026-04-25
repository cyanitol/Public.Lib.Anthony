// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

type stringList []string

func defaultCompatMode() string {
	return "hard-compat"
}

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var (
		dsn        string
		compatMode string
		queries    stringList
		files      stringList
	)

	flag.StringVar(&dsn, "db", ":memory:", "database path or DSN")
	flag.StringVar(&compatMode, "compat-mode", defaultCompatMode(), "compatibility mode: hard-compat or extended")
	flag.Var(&queries, "query", "SQL query to run (repeatable)")
	flag.Var(&files, "file", "SQL file to run, or '-' for stdin (repeatable)")
	flag.Parse()

	if len(queries) == 0 && len(files) == 0 {
		fmt.Fprintln(os.Stderr, "missing -query or -file")
		flag.Usage()
		os.Exit(2)
	}

	db, err := anthony.OpenWithCompatibility(dsn, compatMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := runFiles(db, files); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := runQueries(db, queries); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runFiles(db *sql.DB, files []string) error {
	for _, path := range files {
		data, err := readSQL(path)
		if err != nil {
			return err
		}
		statements := splitStatements(data)
		for _, stmt := range statements {
			if err := runStatement(db, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func runQueries(db *sql.DB, queries []string) error {
	for _, stmt := range queries {
		if err := runStatement(db, stmt); err != nil {
			return err
		}
	}
	return nil
}

func readSQL(path string) (string, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return "", fmt.Errorf("open %s: %w", path, err)
		}
		defer file.Close()
		r = file
	}
	buf := &strings.Builder{}
	if _, err := io.Copy(buf, r); err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return buf.String(), nil
}

func splitStatements(input string) []string {
	lex := parser.NewLexer(input)
	statements := make([]string, 0)
	start := -1

	for {
		tok := lex.NextToken()
		if tok.Type == parser.TK_EOF {
			return appendStatement(statements, input, start, len(input))
		}
		if tok.Type == parser.TK_COMMENT && start == -1 {
			continue
		}
		if tok.Type == parser.TK_SEMI {
			statements, start = splitAtSemicolon(statements, input, start, tok.Pos)
			continue
		}
		if start == -1 {
			start = tok.Pos
		}
	}
}

func splitAtSemicolon(statements []string, input string, start, end int) ([]string, int) {
	if start == -1 {
		return statements, start
	}
	return appendStatement(statements, input, start, end), -1
}

func appendStatement(statements []string, input string, start, end int) []string {
	if start == -1 {
		return statements
	}
	stmt := strings.TrimSpace(input[start:end])
	if stmt == "" {
		return statements
	}
	return append(statements, stmt)
}

func runStatement(db *sql.DB, stmt string) error {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return nil
	}

	rows, err := db.Query(stmt)
	if err != nil {
		return execStatement(db, stmt)
	}
	defer rows.Close()

	return printRows(rows)
}

func execStatement(db *sql.DB, stmt string) error {
	result, err := db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("execute statement: %w", err)
	}
	return reportResult(result)
}

func printRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read columns: %w", err)
	}
	printColumns(cols)
	values, scanArgs := makeScanTargets(len(cols))
	return scanAndPrintRows(rows, values, scanArgs)
}

func printColumns(cols []string) {
	if len(cols) > 0 {
		fmt.Println(strings.Join(cols, "\t"))
	}
}

func makeScanTargets(n int) ([]any, []any) {
	values := make([]any, n)
	scanArgs := make([]any, n)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	return values, scanArgs
}

func scanAndPrintRows(rows *sql.Rows, values, scanArgs []any) error {
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		fmt.Println(formatRow(values))
	}
	return rowsError(rows)
}

func rowsError(rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}
	return nil
}

func formatRow(values []any) string {
	out := make([]string, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case nil:
			out[i] = "NULL"
		case []byte:
			out[i] = string(val)
		default:
			out[i] = fmt.Sprintf("%v", val)
		}
	}
	return strings.Join(out, "\t")
}

func reportResult(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		fmt.Println("OK")
		return nil
	}
	fmt.Printf("OK (rows affected: %d)\n", rows)
	return nil
}
