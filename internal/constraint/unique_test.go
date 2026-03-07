// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestNewUniqueConstraint tests creating a new UNIQUE constraint.
func TestNewUniqueConstraint(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
		tableName      string
		columns        []string
		wantIndexName  string
	}{
		{
			name:           "named constraint",
			constraintName: "uk_email",
			tableName:      "users",
			columns:        []string{"email"},
			wantIndexName:  "sqlite_autoindex_users_uk_email",
		},
		{
			name:           "unnamed single column",
			constraintName: "",
			tableName:      "users",
			columns:        []string{"email"},
			wantIndexName:  "sqlite_autoindex_users_email",
		},
		{
			name:           "unnamed composite",
			constraintName: "",
			tableName:      "users",
			columns:        []string{"first_name", "last_name"},
			wantIndexName:  "sqlite_autoindex_users_first_name_last_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := NewUniqueConstraint(tt.constraintName, tt.tableName, tt.columns)

			if uc.Name != tt.constraintName {
				t.Errorf("Name = %v, want %v", uc.Name, tt.constraintName)
			}

			if uc.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", uc.TableName, tt.tableName)
			}

			if len(uc.Columns) != len(tt.columns) {
				t.Fatalf("len(Columns) = %v, want %v", len(uc.Columns), len(tt.columns))
			}

			for i, col := range tt.columns {
				if uc.Columns[i] != col {
					t.Errorf("Columns[%d] = %v, want %v", i, uc.Columns[i], col)
				}
			}

			if uc.IndexName != tt.wantIndexName {
				t.Errorf("IndexName = %v, want %v", uc.IndexName, tt.wantIndexName)
			}
		})
	}
}

// TestUniqueViolationError tests the error message formatting.
func TestUniqueViolationError(t *testing.T) {
	tests := []struct {
		name    string
		err     *UniqueViolationError
		wantMsg string
	}{
		{
			name: "named constraint",
			err: &UniqueViolationError{
				ConstraintName: "uk_email",
				TableName:      "users",
				Columns:        []string{"email"},
			},
			wantMsg: "UNIQUE constraint failed: users.uk_email",
		},
		{
			name: "unnamed constraint",
			err: &UniqueViolationError{
				TableName: "users",
				Columns:   []string{"email"},
			},
			wantMsg: "UNIQUE constraint failed: users.email",
		},
		{
			name: "composite constraint",
			err: &UniqueViolationError{
				TableName: "users",
				Columns:   []string{"first_name", "last_name"},
			},
			wantMsg: "UNIQUE constraint failed: users.first_name,last_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.wantMsg {
				t.Errorf("Error() = %v, want %v", got, tt.wantMsg)
			}
		})
	}
}

// TestValuesEqual tests value comparison logic.
func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		v1   interface{}
		v2   interface{}
		want bool
	}{
		// Nil cases
		{name: "both nil", v1: nil, v2: nil, want: true},
		{name: "first nil", v1: nil, v2: 42, want: false},
		{name: "second nil", v1: 42, v2: nil, want: false},

		// Integer cases
		{name: "int equal", v1: 42, v2: 42, want: true},
		{name: "int not equal", v1: 42, v2: 43, want: false},
		{name: "int and int64 equal", v1: 42, v2: int64(42), want: true},
		{name: "int64 and int equal", v1: int64(42), v2: 42, want: true},
		{name: "int64 equal", v1: int64(42), v2: int64(42), want: true},

		// Float cases
		{name: "float equal", v1: 3.14, v2: 3.14, want: true},
		{name: "float not equal", v1: 3.14, v2: 3.15, want: false},

		// String cases
		{name: "string equal", v1: "hello", v2: "hello", want: true},
		{name: "string not equal", v1: "hello", v2: "world", want: false},
		{name: "string empty", v1: "", v2: "", want: true},

		// Blob (byte array) cases
		{name: "blob equal", v1: []byte{1, 2, 3}, v2: []byte{1, 2, 3}, want: true},
		{name: "blob not equal", v1: []byte{1, 2, 3}, v2: []byte{1, 2, 4}, want: false},
		{name: "blob different length", v1: []byte{1, 2}, v2: []byte{1, 2, 3}, want: false},
		{name: "blob empty", v1: []byte{}, v2: []byte{}, want: true},

		// Type mismatch cases
		{name: "int vs string", v1: 42, v2: "42", want: false},
		{name: "float vs string", v1: 3.14, v2: "3.14", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valuesEqual(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

// TestValuesMatch tests the constraint value matching logic.
func TestValuesMatch(t *testing.T) {
	uc := &UniqueConstraint{
		Columns: []string{"col1", "col2"},
	}

	tests := []struct {
		name    string
		values1 map[string]interface{}
		values2 map[string]interface{}
		want    bool
	}{
		{
			name: "all match",
			values1: map[string]interface{}{
				"col1": 42,
				"col2": "test",
			},
			values2: map[string]interface{}{
				"col1": 42,
				"col2": "test",
			},
			want: true,
		},
		{
			name: "first column differs",
			values1: map[string]interface{}{
				"col1": 42,
				"col2": "test",
			},
			values2: map[string]interface{}{
				"col1": 43,
				"col2": "test",
			},
			want: false,
		},
		{
			name: "second column differs",
			values1: map[string]interface{}{
				"col1": 42,
				"col2": "test",
			},
			values2: map[string]interface{}{
				"col1": 42,
				"col2": "different",
			},
			want: false,
		},
		{
			name: "first column NULL",
			values1: map[string]interface{}{
				"col1": nil,
				"col2": "test",
			},
			values2: map[string]interface{}{
				"col1": 42,
				"col2": "test",
			},
			want: false, // NULL is distinct from non-NULL
		},
		{
			name: "both columns NULL",
			values1: map[string]interface{}{
				"col1": nil,
				"col2": nil,
			},
			values2: map[string]interface{}{
				"col1": nil,
				"col2": nil,
			},
			want: false, // NULL is distinct from NULL
		},
		{
			name: "one column NULL, other matches",
			values1: map[string]interface{}{
				"col1": 42,
				"col2": nil,
			},
			values2: map[string]interface{}{
				"col1": 42,
				"col2": "test",
			},
			want: false, // NULL makes them distinct
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := uc.valuesMatch(tt.values1, tt.values2)
			if got != tt.want {
				t.Errorf("valuesMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestExtractUniqueConstraints tests extracting UNIQUE constraints from table definitions.
func TestExtractUniqueConstraints(t *testing.T) {
	tests := []struct {
		name      string
		table     *schema.Table
		wantCount int
		wantCols  [][]string
	}{
		{
			name: "no constraints",
			table: &schema.Table{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "name", Type: "TEXT"},
				},
			},
			wantCount: 0,
		},
		{
			name: "single column constraint",
			table: &schema.Table{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "email", Type: "TEXT", Unique: true},
				},
			},
			wantCount: 1,
			wantCols:  [][]string{{"email"}},
		},
		{
			name: "multiple column constraints",
			table: &schema.Table{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "email", Type: "TEXT", Unique: true},
					{Name: "username", Type: "TEXT", Unique: true},
				},
			},
			wantCount: 2,
			wantCols:  [][]string{{"email"}, {"username"}},
		},
		{
			name: "table-level constraint",
			table: &schema.Table{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "first_name", Type: "TEXT"},
					{Name: "last_name", Type: "TEXT"},
				},
				Constraints: []schema.TableConstraint{
					{
						Type:    schema.ConstraintUnique,
						Columns: []string{"first_name", "last_name"},
					},
				},
			},
			wantCount: 1,
			wantCols:  [][]string{{"first_name", "last_name"}},
		},
		{
			name: "mixed constraints",
			table: &schema.Table{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "email", Type: "TEXT", Unique: true},
					{Name: "first_name", Type: "TEXT"},
					{Name: "last_name", Type: "TEXT"},
				},
				Constraints: []schema.TableConstraint{
					{
						Type:    schema.ConstraintUnique,
						Columns: []string{"first_name", "last_name"},
					},
				},
			},
			wantCount: 2,
			wantCols:  [][]string{{"email"}, {"first_name", "last_name"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constraints := ExtractUniqueConstraints(tt.table)

			if len(constraints) != tt.wantCount {
				t.Fatalf("ExtractUniqueConstraints() returned %d constraints, want %d",
					len(constraints), tt.wantCount)
			}

			// Verify columns match
			for i, constraint := range constraints {
				if i >= len(tt.wantCols) {
					break
				}

				expectedCols := tt.wantCols[i]
				if len(constraint.Columns) != len(expectedCols) {
					t.Errorf("Constraint %d has %d columns, want %d",
						i, len(constraint.Columns), len(expectedCols))
					continue
				}

				for j, col := range expectedCols {
					if constraint.Columns[j] != col {
						t.Errorf("Constraint %d, column %d = %v, want %v",
							i, j, constraint.Columns[j], col)
					}
				}

				// Verify table name is set
				if constraint.TableName != tt.table.Name {
					t.Errorf("Constraint %d TableName = %v, want %v",
						i, constraint.TableName, tt.table.Name)
				}
			}
		})
	}
}

// TestGenerateIndexSQL tests the SQL generation for backing indexes.
func TestGenerateIndexSQL(t *testing.T) {
	tests := []struct {
		name       string
		constraint *UniqueConstraint
		wantSQL    string
	}{
		{
			name: "simple single column",
			constraint: &UniqueConstraint{
				IndexName: "sqlite_autoindex_users_email",
				TableName: "users",
				Columns:   []string{"email"},
			},
			wantSQL: "CREATE UNIQUE INDEX sqlite_autoindex_users_email ON users(email)",
		},
		{
			name: "composite key",
			constraint: &UniqueConstraint{
				IndexName: "sqlite_autoindex_users_first_name_last_name",
				TableName: "users",
				Columns:   []string{"first_name", "last_name"},
			},
			wantSQL: "CREATE UNIQUE INDEX sqlite_autoindex_users_first_name_last_name ON users(first_name, last_name)",
		},
		{
			name: "partial index",
			constraint: &UniqueConstraint{
				IndexName: "sqlite_autoindex_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Partial:   true,
				Where:     "email IS NOT NULL",
			},
			wantSQL: "CREATE UNIQUE INDEX sqlite_autoindex_users_email ON users(email) WHERE email IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.constraint.generateIndexSQL()
			if got != tt.wantSQL {
				t.Errorf("generateIndexSQL() = %v, want %v", got, tt.wantSQL)
			}
		})
	}
}

// TestValidateNullHandling tests that NULL values are handled correctly per SQL standard.
func TestValidateNullHandling(t *testing.T) {
	// Create a test table
	table := &schema.Table{
		Name:     "test_table",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "email", Type: "TEXT", Unique: true},
		},
	}

	// Create btree
	bt := btree.NewBtree(4096)
	_, err := bt.CreateTable() // Create root page
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	uc := NewUniqueConstraint("", "test_table", []string{"email"})

	tests := []struct {
		name      string
		values    map[string]interface{}
		rowid     int64
		wantError bool
	}{
		{
			name: "NULL value - should pass",
			values: map[string]interface{}{
				"id":    1,
				"email": nil,
			},
			rowid:     1,
			wantError: false,
		},
		{
			name: "non-NULL value - first insert should pass",
			values: map[string]interface{}{
				"id":    2,
				"email": "user@example.com",
			},
			rowid:     2,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := uc.Validate(table, bt, tt.values, tt.rowid)
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}

			// If we expected an error, verify it's a UniqueViolationError
			if tt.wantError && err != nil {
				if _, ok := err.(*UniqueViolationError); !ok {
					t.Errorf("Expected UniqueViolationError, got %T", err)
				}
			}
		})
	}
}

// TestCompositeUniqueConstraint tests composite (multi-column) unique constraints.
func TestCompositeUniqueConstraint(t *testing.T) {
	// Create a test table with composite unique constraint
	_ = &schema.Table{
		Name:     "addresses",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "street", Type: "TEXT"},
			{Name: "city", Type: "TEXT"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:    schema.ConstraintUnique,
				Columns: []string{"street", "city"},
			},
		},
	}

	uc := NewUniqueConstraint("", "addresses", []string{"street", "city"})

	// Test value matching
	tests := []struct {
		name      string
		values1   map[string]interface{}
		values2   map[string]interface{}
		wantMatch bool
	}{
		{
			name: "both columns match",
			values1: map[string]interface{}{
				"street": "123 Main St",
				"city":   "Springfield",
			},
			values2: map[string]interface{}{
				"street": "123 Main St",
				"city":   "Springfield",
			},
			wantMatch: true,
		},
		{
			name: "street differs",
			values1: map[string]interface{}{
				"street": "123 Main St",
				"city":   "Springfield",
			},
			values2: map[string]interface{}{
				"street": "456 Oak Ave",
				"city":   "Springfield",
			},
			wantMatch: false,
		},
		{
			name: "city differs",
			values1: map[string]interface{}{
				"street": "123 Main St",
				"city":   "Springfield",
			},
			values2: map[string]interface{}{
				"street": "123 Main St",
				"city":   "Shelbyville",
			},
			wantMatch: false,
		},
		{
			name: "one column NULL",
			values1: map[string]interface{}{
				"street": "123 Main St",
				"city":   nil,
			},
			values2: map[string]interface{}{
				"street": "123 Main St",
				"city":   "Springfield",
			},
			wantMatch: false,
		},
		{
			name: "both columns NULL in both rows",
			values1: map[string]interface{}{
				"street": nil,
				"city":   nil,
			},
			values2: map[string]interface{}{
				"street": nil,
				"city":   nil,
			},
			wantMatch: false, // NULLs are distinct
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := uc.valuesMatch(tt.values1, tt.values2)
			if got != tt.wantMatch {
				t.Errorf("valuesMatch() = %v, want %v", got, tt.wantMatch)
			}
		})
	}
}

// TestCreateBackingIndex tests automatic index creation.
func TestCreateBackingIndex(t *testing.T) {
	sch := schema.NewSchema()
	bt := btree.NewBtree(4096)

	// Create a test table
	table := &schema.Table{
		Name:     "users",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "email", Type: "TEXT", Unique: true},
		},
	}

	sch.Tables["users"] = table

	uc := NewUniqueConstraint("uk_email", "users", []string{"email"})

	// Create the backing index
	err := uc.CreateBackingIndex(sch, bt)
	if err != nil {
		t.Fatalf("CreateBackingIndex() error = %v", err)
	}

	// Verify index was created
	index, exists := sch.GetIndex(uc.IndexName)
	if !exists {
		t.Fatalf("Index %s was not created", uc.IndexName)
	}

	// Verify index properties
	if index.Table != table.Name {
		t.Errorf("Index table = %v, want %v", index.Table, table.Name)
	}

	if !index.Unique {
		t.Error("Index should be marked as Unique")
	}

	if len(index.Columns) != 1 || index.Columns[0] != "email" {
		t.Errorf("Index columns = %v, want [email]", index.Columns)
	}

	// Verify idempotency - creating again should not fail
	err = uc.CreateBackingIndex(sch, bt)
	if err != nil {
		t.Errorf("Second CreateBackingIndex() error = %v", err)
	}
}

// TestEnsureUniqueIndexes tests the batch index creation function.
func TestEnsureUniqueIndexes(t *testing.T) {
	sch := schema.NewSchema()
	bt := btree.NewBtree(4096)

	// Create a test table with multiple unique constraints
	table := &schema.Table{
		Name:     "users",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "email", Type: "TEXT", Unique: true},
			{Name: "username", Type: "TEXT", Unique: true},
		},
	}

	sch.Tables["users"] = table

	// Ensure all indexes are created
	err := EnsureUniqueIndexes(table, sch, bt)
	if err != nil {
		t.Fatalf("EnsureUniqueIndexes() error = %v", err)
	}

	// Verify both indexes were created
	constraints := ExtractUniqueConstraints(table)
	if len(constraints) != 2 {
		t.Fatalf("Expected 2 constraints, got %d", len(constraints))
	}

	for _, constraint := range constraints {
		_, exists := sch.GetIndex(constraint.IndexName)
		if !exists {
			t.Errorf("Index %s was not created", constraint.IndexName)
		}
	}
}
