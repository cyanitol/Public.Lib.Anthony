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

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var (
		dsn     string
		queries stringList
		files   stringList
	)

	flag.StringVar(&dsn, "db", ":memory:", "database path or DSN")
	flag.Var(&queries, "query", "SQL query to run (repeatable)")
	flag.Var(&files, "file", "SQL file to run, or '-' for stdin (repeatable)")
	flag.Parse()

	if len(queries) == 0 && len(files) == 0 {
		fmt.Fprintln(os.Stderr, "missing -query or -file")
		flag.Usage()
		os.Exit(2)
	}

	db, err := anthony.Open(dsn)
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
		switch tok.Type {
		case parser.TK_EOF:
			if start != -1 {
				stmt := strings.TrimSpace(input[start:])
				if stmt != "" {
					statements = append(statements, stmt)
				}
			}
			return statements
		case parser.TK_SEMI:
			if start != -1 {
				stmt := strings.TrimSpace(input[start:tok.Pos])
				if stmt != "" {
					statements = append(statements, stmt)
				}
				start = -1
			}
		case parser.TK_COMMENT:
			if start == -1 {
				continue
			}
		default:
			if start == -1 {
				start = tok.Pos
			}
		}
	}
}

func runStatement(db *sql.DB, stmt string) error {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return nil
	}

	rows, err := db.Query(stmt)
	if err != nil {
		result, execErr := db.Exec(stmt)
		if execErr != nil {
			return fmt.Errorf("execute statement: %w", execErr)
		}
		return reportResult(result)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read columns: %w", err)
	}
	if len(cols) > 0 {
		fmt.Println(strings.Join(cols, "\t"))
	}

	values := make([]any, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		fmt.Println(formatRow(values))
	}
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
