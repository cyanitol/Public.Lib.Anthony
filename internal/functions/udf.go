// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// Test edge cases in DDL parsing

func TestParseCreateTableEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create table as select",
			sql:     "CREATE TABLE copy AS SELECT * FROM original",
			wantErr: false,
		},
		{
			name:    "create table with check constraint error",
			sql:     "CREATE TABLE t (x INTEGER CHECK)",
			wantErr: true,
		},
		{
			name:    "create table with named constraint",
			sql:     "CREATE TABLE t (x INTEGER CONSTRAINT pk PRIMARY KEY)",
			wantErr: false,
		},
		{
			name:    "create table with collate",
			sql:     "CREATE TABLE t (name TEXT COLLATE NOCASE)",
			wantErr: false,
		},
		{
			name:    "create table with collate error",
			sql:     "CREATE TABLE t (name TEXT COLLATE)",
			wantErr: true,
		},
		{
			name:    "table constraint primary key",
			sql:     "CREATE TABLE t (id INTEGER, CONSTRAINT pk PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "table constraint unique",
			sql:     "CREATE TABLE t (x INTEGER, y INTEGER, CONSTRAINT uq UNIQUE (x, y))",
			wantErr: false,
		},
		{
			name:    "table constraint check",
			sql:     "CREATE TABLE t (x INTEGER, CONSTRAINT chk CHECK (x > 0))",
			wantErr: false,
		},
		{
			name:    "table constraint check error - no expression",
			sql:     "CREATE TABLE t (x INTEGER, CONSTRAINT chk CHECK)",
			wantErr: true,
		},
		{
			name:    "table constraint check error - no paren",
			sql:     "CREATE TABLE t (x INTEGER, CONSTRAINT chk CHECK x > 0)",
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

func TestParseForeignKeyEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "foreign key basic",
			sql:     "CREATE TABLE t (x INTEGER, FOREIGN KEY (x) REFERENCES other(id))",
			wantErr: false,
		},
		{
			name:    "foreign key missing references",
			sql:     "CREATE TABLE t (x INTEGER, FOREIGN KEY (x) other(id))",
			wantErr: true,
		},
		{
			name:    "foreign key missing table name",
			sql:     "CREATE TABLE t (x INTEGER, FOREIGN KEY (x) REFERENCES)",
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

func TestParseCreateViewEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create view with columns",
			sql:     "CREATE VIEW v (a, b, c) AS SELECT x, y, z FROM t",
			wantErr: false,
		},
		{
			name:    "create view columns error - missing comma",
			sql:     "CREATE VIEW v (a b c) AS SELECT x, y, z FROM t",
			wantErr: true,
		},
		{
			name:    "create view missing as",
			sql:     "CREATE VIEW v SELECT * FROM t",
			wantErr: true,
		},
		{
			name:    "create view missing select",
			sql:     "CREATE VIEW v AS",
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

func TestParseDropIndexEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "drop index simple",
			sql:     "DROP INDEX idx_test",
			wantErr: false,
		},
		{
			name:    "drop index if exists",
			sql:     "DROP INDEX IF EXISTS idx_test",
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

func TestParseCreateTriggerEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "trigger with of columns",
			sql:     "CREATE TRIGGER t AFTER UPDATE OF col1, col2 ON table1 BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger with when clause",
			sql:     "CREATE TRIGGER t AFTER INSERT ON table1 WHEN NEW.x > 0 BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger when missing condition",
			sql:     "CREATE TRIGGER t AFTER INSERT ON table1 WHEN BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "trigger body multiple statements",
			sql:     "CREATE TRIGGER t AFTER INSERT ON table1 BEGIN INSERT INTO log VALUES (1); UPDATE stats SET count = count + 1; END",
			wantErr: false,
		},
		{
			name:    "trigger body with error",
			sql:     "CREATE TRIGGER t AFTER INSERT ON table1 BEGIN SELECT FROM; END",
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

func TestParseCreateStatementEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create temporary table",
			sql:     "CREATE TEMP TABLE t (x INTEGER)",
			wantErr: false,
		},
		{
			name:    "create temporary index",
			sql:     "CREATE TEMP INDEX idx ON t(x)",
			wantErr: false,
		},
		{
			name:    "create temporary view",
			sql:     "CREATE TEMP VIEW v AS SELECT * FROM t",
			wantErr: false,
		},
		{
			name:    "create temporary trigger",
			sql:     "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
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

func TestParseColumnConstraintEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "column check constraint error - no paren",
			sql:     "CREATE TABLE t (x INTEGER CHECK x > 0)",
			wantErr: true,
		},
		{
			name:    "column not null with name",
			sql:     "CREATE TABLE t (x INTEGER CONSTRAINT nn NOT NULL)",
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
