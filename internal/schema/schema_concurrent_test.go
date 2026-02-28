package schema

import (
	"fmt"
	"sync"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// TestConcurrentReads tests that multiple goroutines can safely read from the schema simultaneously.
func TestConcurrentReads(t *testing.T) {
	s := NewSchema()

	// Populate schema with test data
	for i := 0; i < 10; i++ {
		stmt := &parser.CreateTableStmt{
			Name: fmt.Sprintf("table_%d", i),
			Columns: []parser.ColumnDef{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
			},
		}
		if _, err := s.CreateTable(stmt); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
	}

	// Start multiple readers
	const numReaders = 50
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			// Perform various read operations
			for j := 0; j < 100; j++ {
				tableName := fmt.Sprintf("table_%d", j%10)

				// Test GetTable
				if _, ok := s.GetTable(tableName); !ok {
					t.Errorf("Reader %d: table not found: %s", id, tableName)
				}

				// Test ListTables
				tables := s.ListTables()
				if len(tables) != 10 {
					t.Errorf("Reader %d: expected 10 tables, got %d", id, len(tables))
				}

				// Test TableCount
				count := s.TableCount()
				if count != 10 {
					t.Errorf("Reader %d: expected count 10, got %d", id, count)
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentReadWrite tests that concurrent reads and writes don't cause race conditions.
func TestConcurrentReadWrite(t *testing.T) {
	s := NewSchema()

	// Pre-populate with some tables
	for i := 0; i < 5; i++ {
		stmt := &parser.CreateTableStmt{
			Name: fmt.Sprintf("initial_table_%d", i),
			Columns: []parser.ColumnDef{
				{Name: "id", Type: "INTEGER"},
			},
		}
		if _, err := s.CreateTable(stmt); err != nil {
			t.Fatalf("Failed to create initial table: %v", err)
		}
	}

	const numWorkers = 20
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Start readers
	for i := 0; i < numWorkers/2; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.ListTables()
				_ = s.ListIndexes()
				s.GetTable(fmt.Sprintf("initial_table_%d", j%5))
			}
		}(i)
	}

	// Start writers
	for i := 0; i < numWorkers/2; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				tableName := fmt.Sprintf("writer_%d_table_%d", id, j)
				stmt := &parser.CreateTableStmt{
					Name: tableName,
					Columns: []parser.ColumnDef{
						{Name: "id", Type: "INTEGER"},
						{Name: "data", Type: "TEXT"},
					},
				}
				_, _ = s.CreateTable(stmt)

				// Also create an index
				indexStmt := &parser.CreateIndexStmt{
					Name:  fmt.Sprintf("idx_%s", tableName),
					Table: tableName,
					Columns: []parser.IndexedColumn{
						{Column: "id"},
					},
				}
				_, _ = s.CreateIndex(indexStmt)
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentTableOperations tests concurrent CREATE, DROP, and RENAME operations.
func TestConcurrentTableOperations(t *testing.T) {
	s := NewSchema()

	const numWorkers = 10
	var wg sync.WaitGroup
	wg.Add(numWorkers * 3)

	// Workers creating tables
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				stmt := &parser.CreateTableStmt{
					Name: fmt.Sprintf("create_worker_%d_table_%d", id, j),
					Columns: []parser.ColumnDef{
						{Name: "id", Type: "INTEGER"},
					},
				}
				_, _ = s.CreateTable(stmt)
			}
		}(i)
	}

	// Workers dropping tables
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				tableName := fmt.Sprintf("create_worker_%d_table_%d", id, j)
				_ = s.DropTable(tableName)
			}
		}(i)
	}

	// Workers reading tables
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.ListTables()
				s.GetTable(fmt.Sprintf("create_worker_%d_table_%d", id%numWorkers, j%20))
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentIndexOperations tests concurrent index creation and deletion.
func TestConcurrentIndexOperations(t *testing.T) {
	s := NewSchema()

	// Create base tables first
	for i := 0; i < 5; i++ {
		stmt := &parser.CreateTableStmt{
			Name: fmt.Sprintf("test_table_%d", i),
			Columns: []parser.ColumnDef{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
				{Name: "value", Type: "REAL"},
			},
		}
		if _, err := s.CreateTable(stmt); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
	}

	const numWorkers = 15
	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	// Workers creating indexes
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				indexStmt := &parser.CreateIndexStmt{
					Name:  fmt.Sprintf("idx_worker_%d_%d", id, j),
					Table: fmt.Sprintf("test_table_%d", j%5),
					Columns: []parser.IndexedColumn{
						{Column: "id"},
					},
				}
				_, _ = s.CreateIndex(indexStmt)
			}
		}(i)
	}

	// Workers reading indexes
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.ListIndexes()
				for k := 0; k < 5; k++ {
					_ = s.GetTableIndexes(fmt.Sprintf("test_table_%d", k))
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentViewOperations tests concurrent view creation and access.
func TestConcurrentViewOperations(t *testing.T) {
	s := NewSchema()

	// Create a base table for views to reference
	tableStmt := &parser.CreateTableStmt{
		Name: "base_table",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}
	if _, err := s.CreateTable(tableStmt); err != nil {
		t.Fatalf("Failed to create base table: %v", err)
	}

	const numWorkers = 10
	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	// Workers creating views
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				// Create a simple select statement
				selectStmt := &parser.SelectStmt{
					Columns: []parser.ResultColumn{
						{Expr: &parser.IdentExpr{Name: "id"}},
						{Expr: &parser.IdentExpr{Name: "name"}},
					},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "base_table"},
						},
					},
				}

				viewStmt := &parser.CreateViewStmt{
					Name:   fmt.Sprintf("view_worker_%d_%d", id, j),
					Select: selectStmt,
				}
				_, _ = s.CreateView(viewStmt)
			}
		}(i)
	}

	// Workers reading views
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.ListViews()
				s.GetView(fmt.Sprintf("view_worker_%d_%d", id, j%10))
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentTriggerOperations tests concurrent trigger creation and access.
func TestConcurrentTriggerOperations(t *testing.T) {
	s := NewSchema()

	// Create base tables
	for i := 0; i < 3; i++ {
		stmt := &parser.CreateTableStmt{
			Name: fmt.Sprintf("trigger_table_%d", i),
			Columns: []parser.ColumnDef{
				{Name: "id", Type: "INTEGER"},
				{Name: "value", Type: "INTEGER"},
			},
		}
		if _, err := s.CreateTable(stmt); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
	}

	const numWorkers = 10
	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	// Workers creating triggers
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				triggerStmt := &parser.CreateTriggerStmt{
					Name:   fmt.Sprintf("trigger_worker_%d_%d", id, j),
					Table:  fmt.Sprintf("trigger_table_%d", j%3),
					Timing: parser.TriggerAfter,
					Event:  parser.TriggerInsert,
					Body:   []parser.Statement{},
				}
				_, _ = s.CreateTrigger(triggerStmt)
			}
		}(i)
	}

	// Workers reading triggers
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.ListTriggers()
				s.GetTrigger(fmt.Sprintf("trigger_worker_%d_%d", id, j%10))
				for k := 0; k < 3; k++ {
					timing := parser.TriggerAfter
					event := parser.TriggerInsert
					_ = s.GetTableTriggers(fmt.Sprintf("trigger_table_%d", k), &timing, &event)
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestReservedNameRejection tests that reserved names are properly rejected.
func TestReservedNameRejection(t *testing.T) {
	s := NewSchema()

	reservedTables := []string{
		"sqlite_master",
		"sqlite_temp_master",
		"sqlite_sequence",
		"SQLITE_MASTER", // Test case insensitivity
		"SQLite_Sequence",
	}

	for _, name := range reservedTables {
		t.Run("Table_"+name, func(t *testing.T) {
			stmt := &parser.CreateTableStmt{
				Name: name,
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
			}
			_, err := s.CreateTable(stmt)
			if err == nil {
				t.Errorf("Expected error for reserved table name %q, got nil", name)
			}
			if err != nil && err.Error() != fmt.Sprintf("table name is reserved: %s", name) {
				t.Errorf("Expected reserved name error, got: %v", err)
			}
		})

		t.Run("Index_"+name, func(t *testing.T) {
			// Create a valid table first
			tableStmt := &parser.CreateTableStmt{
				Name: "valid_table",
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
			}
			s.CreateTable(tableStmt)

			indexStmt := &parser.CreateIndexStmt{
				Name:  name,
				Table: "valid_table",
				Columns: []parser.IndexedColumn{
					{Column: "id"},
				},
			}
			_, err := s.CreateIndex(indexStmt)
			if err == nil {
				t.Errorf("Expected error for reserved index name %q, got nil", name)
			}
		})

		t.Run("View_"+name, func(t *testing.T) {
			selectStmt := &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Expr: &parser.IdentExpr{Name: "id"}},
				},
			}
			viewStmt := &parser.CreateViewStmt{
				Name:   name,
				Select: selectStmt,
			}
			_, err := s.CreateView(viewStmt)
			if err == nil {
				t.Errorf("Expected error for reserved view name %q, got nil", name)
			}
		})
	}
}

// TestIsReservedName tests the IsReservedName function.
func TestIsReservedName(t *testing.T) {
	testCases := []struct {
		name     string
		expected bool
	}{
		{"sqlite_master", true},
		{"sqlite_temp_master", true},
		{"sqlite_sequence", true},
		{"SQLITE_MASTER", true}, // Case insensitive
		{"SQLite_Sequence", true},
		{"my_table", false},
		{"user_data", false},
		{"sqlite_test", false}, // Starts with sqlite_ but not reserved
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsReservedName(tc.name)
			if result != tc.expected {
				t.Errorf("IsReservedName(%q) = %v, expected %v", tc.name, result, tc.expected)
			}
		})
	}
}

// TestConcurrentMixedOperations tests a realistic mix of operations happening concurrently.
func TestConcurrentMixedOperations(t *testing.T) {
	s := NewSchema()

	// Pre-populate with some data
	for i := 0; i < 5; i++ {
		stmt := &parser.CreateTableStmt{
			Name: fmt.Sprintf("base_table_%d", i),
			Columns: []parser.ColumnDef{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
			},
		}
		s.CreateTable(stmt)
	}

	const numWorkers = 20
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Mixed operations
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				switch j % 5 {
				case 0:
					// Create table
					stmt := &parser.CreateTableStmt{
						Name: fmt.Sprintf("dynamic_table_%d_%d", id, j),
						Columns: []parser.ColumnDef{
							{Name: "id", Type: "INTEGER"},
						},
					}
					s.CreateTable(stmt)

				case 1:
					// Read tables
					_ = s.ListTables()
					_ = s.TableCount()

				case 2:
					// Create index
					indexStmt := &parser.CreateIndexStmt{
						Name:  fmt.Sprintf("dynamic_idx_%d_%d", id, j),
						Table: fmt.Sprintf("base_table_%d", id%5),
						Columns: []parser.IndexedColumn{
							{Column: "id"},
						},
					}
					s.CreateIndex(indexStmt)

				case 3:
					// Read indexes
					_ = s.ListIndexes()
					_ = s.GetTableIndexes(fmt.Sprintf("base_table_%d", id%5))

				case 4:
					// Drop table
					_ = s.DropTable(fmt.Sprintf("dynamic_table_%d_%d", id, j-4))
				}
			}
		}(i)
	}

	wg.Wait()
}
