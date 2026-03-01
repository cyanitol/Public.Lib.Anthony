// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

// PagerReader defines read-only operations on a pager.
// This interface provides access to pages and metadata without modifying the database.
type PagerReader interface {
	// Get retrieves a page from the database
	Get(pgno Pgno) (*DbPage, error)

	// Put releases a reference to a page
	Put(page *DbPage)

	// PageSize returns the page size of the database
	PageSize() int

	// PageCount returns the number of pages in the database
	PageCount() Pgno

	// IsReadOnly returns true if the pager is read-only
	IsReadOnly() bool

	// GetHeader returns the database header
	GetHeader() *DatabaseHeader

	// GetFreePageCount returns the number of free pages in the database
	GetFreePageCount() uint32
}

// PagerWriter defines write operations on a pager.
// This interface allows modification of pages and allocation of new pages.
type PagerWriter interface {
	PagerReader

	// Write marks a page as writeable and journals it if necessary
	Write(page *DbPage) error

	// AllocatePage allocates a new page
	AllocatePage() (Pgno, error)

	// FreePage adds a page to the free list for later reuse
	FreePage(pgno Pgno) error

	// Vacuum compacts and rebuilds the database
	Vacuum(opts *VacuumOptions) error
}

// PagerTransaction defines transaction control operations.
// This interface manages transaction lifecycle including savepoints.
type PagerTransaction interface {
	// BeginRead starts a read transaction
	BeginRead() error

	// EndRead ends a read transaction
	EndRead() error

	// BeginWrite starts a write transaction
	BeginWrite() error

	// Commit commits the current write transaction
	Commit() error

	// Rollback rolls back the current write transaction
	Rollback() error

	// InWriteTransaction returns true if a write transaction is active
	InWriteTransaction() bool

	// Savepoint creates a savepoint for nested transaction support
	Savepoint(name string) error

	// Release releases a savepoint
	Release(name string) error

	// RollbackTo rolls back to a savepoint
	RollbackTo(name string) error
}

// PagerInterface defines the common interface for both file-based and memory pagers.
// This allows the driver and btree layers to work with either type transparently.
// It combines all three segregated interfaces for full pager functionality.
type PagerInterface interface {
	PagerReader
	PagerWriter
	PagerTransaction

	// Close closes the pager and releases all resources
	Close() error
}

// Verify that both Pager and MemoryPager implement the interface
var _ PagerInterface = (*Pager)(nil)
var _ PagerInterface = (*MemoryPager)(nil)

// Verify segregated interfaces are satisfied
var _ PagerReader = (*Pager)(nil)
var _ PagerReader = (*MemoryPager)(nil)
var _ PagerWriter = (*Pager)(nil)
var _ PagerWriter = (*MemoryPager)(nil)
var _ PagerTransaction = (*Pager)(nil)
var _ PagerTransaction = (*MemoryPager)(nil)
rr := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			stmt, ok := stmts[0].(*VacuumStmt)
			if !ok {
				t.Fatalf("expected VacuumStmt, got %T", stmts[0])
			}

			if stmt.String() != tt.wantString {
				t.Errorf("String() = %q, want %q", stmt.String(), tt.wantString)
			}
		})
	}
}

func TestParseVacuum_MultipleStatements(t *testing.T) {
	t.Parallel()
	sql := `
		CREATE TABLE test (id INTEGER);
		INSERT INTO test VALUES (1);
		VACUUM;
		SELECT * FROM test;
	`

	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(stmts) != 4 {
		t.Errorf("expected 4 statements, got %d", len(stmts))
	}

	// Verify third statement is VACUUM
	if _, ok := stmts[2].(*VacuumStmt); !ok {
		t.Errorf("statement 2: expected VacuumStmt, got %T", stmts[2])
	}
}

func TestParseVacuum_CaseSensitivity(t *testing.T) {
	t.Parallel()
	tests := []string{
		"VACUUM",
		"vacuum",
		"Vacuum",
		"VaCuUm",
		"VACUUM INTO 'file.db'",
		"vacuum into 'file.db'",
		"VaCuUm InTo 'file.db'",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse(%q) error = %v", sql, err)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			if _, ok := stmts[0].(*VacuumStmt); !ok {
				t.Errorf("expected VacuumStmt, got %T", stmts[0])
			}
		})
	}
}

func TestParseVacuum_AST(t *testing.T) {
	t.Parallel()
	// Verify VACUUM statement implements required interfaces
	var _ Statement = (*VacuumStmt)(nil)
	var _ Node = (*VacuumStmt)(nil)

	// Test node and statement methods
	stmt := &VacuumStmt{}
	stmt.node()
	stmt.statement()

	// Test String() method
	simpleStmt := &VacuumStmt{}
	if simpleStmt.String() != "VACUUM" {
		t.Errorf("Simple VACUUM String() = %q, want %q", simpleStmt.String(), "VACUUM")
	}

	intoStmt := &VacuumStmt{Into: "file.db"}
	if intoStmt.String() != "VACUUM INTO" {
		t.Errorf("VACUUM INTO String() = %q, want %q", intoStmt.String(), "VACUUM INTO")
	}
}

func TestParseVacuum_WithParameter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		wantParam   bool
	}{
		{
			name:      "vacuum into with ? parameter",
			sql:       "VACUUM INTO ?",
			wantErr:   false,
			wantParam: true,
		},
		{
			name:      "vacuum schema into with parameter",
			sql:       "VACUUM main INTO ?",
			wantErr:   false,
			wantParam: true,
		},
		{
			name:      "vacuum into with named parameter",
			sql:       "VACUUM INTO :filename",
			wantErr:   false,
			wantParam: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Fatalf("expected 1 statement, got %d", len(stmts))
				}
				stmt, ok := stmts[0].(*VacuumStmt)
				if !ok {
					t.Fatalf("expected VacuumStmt, got %T", stmts[0])
				}
				if stmt.IntoParam != tt.wantParam {
					t.Errorf("IntoParam = %v, want %v", stmt.IntoParam, tt.wantParam)
				}
			}
		})
	}
}
