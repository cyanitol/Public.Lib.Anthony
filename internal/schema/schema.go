// Package schema provides schema management for SQLite databases.
// It tracks tables, indexes, and their metadata including type affinities.
package schema

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// Schema represents a database schema containing tables and indexes.
// It is safe for concurrent access.
type Schema struct {
	Tables    map[string]*Table
	Indexes   map[string]*Index
	Sequences *SequenceManager // Manages AUTOINCREMENT sequences
	mu        sync.RWMutex
}

// NewSchema creates a new empty schema.
func NewSchema() *Schema {
	return &Schema{
		Tables:    make(map[string]*Table),
		Indexes:   make(map[string]*Index),
		Sequences: NewSequenceManager(),
	}
}

// Table represents a database table definition.
type Table struct {
	Name         string            // Table name
	RootPage     uint32            // B-tree root page number
	SQL          string            // CREATE TABLE statement
	Columns      []*Column         // Column definitions
	PrimaryKey   []string          // Primary key column names
	WithoutRowID bool              // True for WITHOUT ROWID tables
	Strict       bool              // True for STRICT tables
	Temp         bool              // True for temporary tables
	Constraints  []TableConstraint // Table-level constraints
}

// Column represents a table column definition.
type Column struct {
	Name     string      // Column name
	Type     string      // Declared type (e.g., "INTEGER", "TEXT", "VARCHAR(100)")
	Affinity Affinity    // Type affinity
	NotNull  bool        // NOT NULL constraint
	Default  interface{} // Default value (nil if none)

	// Constraints
	PrimaryKey    bool   // Part of PRIMARY KEY
	Unique        bool   // UNIQUE constraint
	Autoincrement bool   // AUTOINCREMENT (only for INTEGER PRIMARY KEY)
	Collation     string // COLLATE clause
	Check         string // CHECK constraint expression

	// Generated columns
	Generated       bool   // GENERATED ALWAYS AS
	GeneratedExpr   string // Generation expression
	GeneratedStored bool   // STORED vs VIRTUAL
}

// TableConstraint represents a table-level constraint.
type TableConstraint struct {
	Type       ConstraintType
	Name       string
	Columns    []string
	Expression string // For CHECK constraints
}

// ConstraintType represents the type of constraint.
type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintUnique
	ConstraintCheck
	ConstraintForeignKey
)

// Index represents a database index definition.
type Index struct {
	Name     string   // Index name
	Table    string   // Table name this index belongs to
	RootPage uint32   // B-tree root page number
	SQL      string   // CREATE INDEX statement
	Columns  []string // Indexed column names
	Unique   bool     // True for UNIQUE indexes
	Partial  bool     // True for partial indexes (WHERE clause)
	Where    string   // WHERE clause for partial indexes
}

// GetTable retrieves a table by name.
// Returns the table and true if found, nil and false otherwise.
func (s *Schema) GetTable(name string) (*Table, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// SQLite table names are case-insensitive
	lowerName := strings.ToLower(name)
	for tableName, table := range s.Tables {
		if strings.ToLower(tableName) == lowerName {
			return table, true
		}
	}
	return nil, false
}

// GetIndex retrieves an index by name.
// Returns the index and true if found, nil and false otherwise.
func (s *Schema) GetIndex(name string) (*Index, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lowerName := strings.ToLower(name)
	for indexName, index := range s.Indexes {
		if strings.ToLower(indexName) == lowerName {
			return index, true
		}
	}
	return nil, false
}

// ListTables returns a sorted list of all table names.
func (s *Schema) ListTables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ListIndexes returns a sorted list of all index names.
func (s *Schema) ListIndexes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.Indexes))
	for name := range s.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GetTableIndexes returns all indexes for a given table.
func (s *Schema) GetTableIndexes(tableName string) []*Index {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var indexes []*Index
	lowerTableName := strings.ToLower(tableName)
	for _, idx := range s.Indexes {
		if strings.ToLower(idx.Table) == lowerTableName {
			indexes = append(indexes, idx)
		}
	}

	// Sort by name for consistency
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})

	return indexes
}

// DropTable removes a table and all its indexes from the schema.
func (s *Schema) DropTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lowerName := strings.ToLower(name)

	// Find the actual table name (case-insensitive)
	var actualName string
	for tableName := range s.Tables {
		if strings.ToLower(tableName) == lowerName {
			actualName = tableName
			break
		}
	}

	if actualName == "" {
		return fmt.Errorf("table not found: %s", name)
	}

	// Remove all indexes for this table
	for indexName, idx := range s.Indexes {
		if strings.ToLower(idx.Table) == lowerName {
			delete(s.Indexes, indexName)
		}
	}

	// Remove sequence if it exists
	s.Sequences.DropSequence(actualName)

	// Remove the table
	delete(s.Tables, actualName)

	return nil
}

// DropIndex removes an index from the schema.
func (s *Schema) DropIndex(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lowerName := strings.ToLower(name)

	// Find the actual index name (case-insensitive)
	for indexName := range s.Indexes {
		if strings.ToLower(indexName) == lowerName {
			delete(s.Indexes, indexName)
			return nil
		}
	}

	return fmt.Errorf("index not found: %s", name)
}

// GetColumn retrieves a column from a table by name.
func (t *Table) GetColumn(name string) (*Column, bool) {
	lowerName := strings.ToLower(name)
	for _, col := range t.Columns {
		if strings.ToLower(col.Name) == lowerName {
			return col, true
		}
	}
	return nil, false
}

// GetColumnIndex returns the index of a column by name, or -1 if not found.
func (t *Table) GetColumnIndex(name string) int {
	lowerName := strings.ToLower(name)
	for i, col := range t.Columns {
		if strings.ToLower(col.Name) == lowerName {
			return i
		}
	}
	return -1
}

// GetColumnCollation returns the collation for a column by index.
// Returns empty string if column doesn't exist or has no explicit collation.
func (t *Table) GetColumnCollation(index int) string {
	if index < 0 || index >= len(t.Columns) {
		return ""
	}
	return t.Columns[index].Collation
}

// GetColumnCollationByName returns the collation for a column by name.
// Returns empty string if column doesn't exist or has no explicit collation.
func (t *Table) GetColumnCollationByName(name string) string {
	col, ok := t.GetColumn(name)
	if !ok {
		return ""
	}
	return col.Collation
}

// GetEffectiveCollation returns the effective collation for a column.
// If the column has no explicit collation, returns "BINARY" (the default).
func (c *Column) GetEffectiveCollation() string {
	if c.Collation == "" {
		return "BINARY"
	}
	return c.Collation
}

// HasRowID returns true if the table has an implicit rowid column.
// Tables have a rowid unless they are declared WITHOUT ROWID.
func (t *Table) HasRowID() bool {
	return !t.WithoutRowID
}

// checkTableExists checks if a table already exists in the schema.
// Returns the existing table if found and ifNotExists is true, otherwise an error.
func (s *Schema) checkTableExists(name string, ifNotExists bool) (*Table, error) {
	lowerName := strings.ToLower(name)
	for tableName, table := range s.Tables {
		if strings.ToLower(tableName) == lowerName {
			if ifNotExists {
				return table, nil
			}
			return nil, fmt.Errorf("table already exists: %s", name)
		}
	}
	return nil, nil
}

// columnConstraintHandler is the function signature used for every entry in
// the constraint dispatch table below.
type columnConstraintHandler func(col *Column, c parser.ColumnConstraint, pkCols *[]string)

// applyPrimaryKeyConstraint marks the column as part of the primary key and
// sets Autoincrement when the parsed sub-node requests it.
func applyPrimaryKeyConstraint(col *Column, c parser.ColumnConstraint, pkCols *[]string) {
	col.PrimaryKey = true
	*pkCols = append(*pkCols, col.Name)
	if c.PrimaryKey != nil && c.PrimaryKey.Autoincrement {
		col.Autoincrement = true
	}
}

// applyDefaultConstraint records the string representation of the DEFAULT
// expression on the column.
func applyDefaultConstraint(col *Column, c parser.ColumnConstraint, _ *[]string) {
	if c.Default != nil {
		col.Default = c.Default.String()
	}
}

// applyCheckConstraint records the string representation of the CHECK
// expression on the column.
func applyCheckConstraint(col *Column, c parser.ColumnConstraint, _ *[]string) {
	if c.Check != nil {
		col.Check = c.Check.String()
	}
}

// applyGeneratedConstraint marks the column as generated and captures its
// expression and storage mode.
func applyGeneratedConstraint(col *Column, c parser.ColumnConstraint, _ *[]string) {
	if c.Generated == nil {
		return
	}
	col.Generated = true
	col.GeneratedStored = c.Generated.Stored
	if c.Generated.Expr != nil {
		col.GeneratedExpr = c.Generated.Expr.String()
	}
}

// columnConstraintHandlers maps each constraint type to its handler.
// Simple single-assignment constraints are expressed as inline closures;
// more complex ones delegate to a named helper above.
var columnConstraintHandlers = map[parser.ConstraintType]columnConstraintHandler{
	parser.ConstraintPrimaryKey: applyPrimaryKeyConstraint,
	parser.ConstraintNotNull:    func(col *Column, _ parser.ColumnConstraint, _ *[]string) { col.NotNull = true },
	parser.ConstraintUnique:     func(col *Column, _ parser.ColumnConstraint, _ *[]string) { col.Unique = true },
	parser.ConstraintCollate:    func(col *Column, c parser.ColumnConstraint, _ *[]string) { col.Collation = c.Collate },
	parser.ConstraintDefault:    applyDefaultConstraint,
	parser.ConstraintCheck:      applyCheckConstraint,
	parser.ConstraintGenerated:  applyGeneratedConstraint,
}

// processColumnConstraint processes a single column constraint.
func processColumnConstraint(col *Column, constraint parser.ColumnConstraint, pkCols *[]string) {
	if handler, ok := columnConstraintHandlers[constraint.Type]; ok {
		handler(col, constraint, pkCols)
	}
}

// convertColumns converts parser column definitions to schema columns.
func convertColumns(colDefs []parser.ColumnDef) ([]*Column, []string) {
	columns := make([]*Column, len(colDefs))
	var primaryKeyColumns []string

	for i, colDef := range colDefs {
		col := &Column{
			Name:     colDef.Name,
			Type:     colDef.Type,
			Affinity: DetermineAffinity(colDef.Type),
		}

		for _, constraint := range colDef.Constraints {
			processColumnConstraint(col, constraint, &primaryKeyColumns)
		}

		columns[i] = col
	}

	return columns, primaryKeyColumns
}

var tableConstraintTypeMap = map[parser.ConstraintType]ConstraintType{
	parser.ConstraintPrimaryKey: ConstraintPrimaryKey,
	parser.ConstraintUnique:     ConstraintUnique,
	parser.ConstraintCheck:      ConstraintCheck,
	parser.ConstraintForeignKey: ConstraintForeignKey,
}

func applyTablePrimaryKey(tc *TableConstraint, c parser.TableConstraint, pkCols *[]string) {
	if c.PrimaryKey == nil {
		return
	}
	for _, col := range c.PrimaryKey.Columns {
		tc.Columns = append(tc.Columns, col.Column)
		*pkCols = append(*pkCols, col.Column)
	}
}

func applyTableUnique(tc *TableConstraint, c parser.TableConstraint, _ *[]string) {
	if c.Unique == nil {
		return
	}
	for _, col := range c.Unique.Columns {
		tc.Columns = append(tc.Columns, col.Column)
	}
}

func applyTableCheck(tc *TableConstraint, c parser.TableConstraint, _ *[]string) {
	if c.Check != nil {
		tc.Expression = c.Check.String()
	}
}

func applyTableForeignKey(tc *TableConstraint, c parser.TableConstraint, _ *[]string) {
	if c.ForeignKey != nil {
		tc.Columns = c.ForeignKey.Columns
	}
}

type tableConstraintHandler func(tc *TableConstraint, c parser.TableConstraint, pkCols *[]string)

var tableConstraintHandlers = map[parser.ConstraintType]tableConstraintHandler{
	parser.ConstraintPrimaryKey: applyTablePrimaryKey,
	parser.ConstraintUnique:     applyTableUnique,
	parser.ConstraintCheck:      applyTableCheck,
	parser.ConstraintForeignKey: applyTableForeignKey,
}

// convertTableConstraint converts a parser table constraint to a schema constraint.
func convertTableConstraint(constraint parser.TableConstraint, pkCols *[]string) TableConstraint {
	tc := TableConstraint{Name: constraint.Name}
	tc.Type = tableConstraintTypeMap[constraint.Type]
	if handler, ok := tableConstraintHandlers[constraint.Type]; ok {
		handler(&tc, constraint, pkCols)
	}
	return tc
}

// convertTableConstraints converts all parser table constraints to schema constraints.
func convertTableConstraints(constraints []parser.TableConstraint, pkCols *[]string) []TableConstraint {
	tableConstraints := make([]TableConstraint, len(constraints))
	for i, constraint := range constraints {
		tableConstraints[i] = convertTableConstraint(constraint, pkCols)
	}
	return tableConstraints
}

// CreateTable creates a table from a CREATE TABLE statement.
func (s *Schema) CreateTable(stmt *parser.CreateTableStmt) (*Table, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil statement")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if table already exists
	if existing, err := s.checkTableExists(stmt.Name, stmt.IfNotExists); err != nil || existing != nil {
		return existing, err
	}

	// Convert columns and constraints
	columns, primaryKeyColumns := convertColumns(stmt.Columns)
	tableConstraints := convertTableConstraints(stmt.Constraints, &primaryKeyColumns)

	// Create the table
	table := &Table{
		Name:         stmt.Name,
		RootPage:     0, // Will be assigned when written to disk
		SQL:          stmt.String(),
		Columns:      columns,
		PrimaryKey:   uniqueStrings(primaryKeyColumns),
		WithoutRowID: stmt.WithoutRowID,
		Strict:       stmt.Strict,
		Temp:         stmt.Temp,
		Constraints:  tableConstraints,
	}

	// Validate AUTOINCREMENT constraints
	if err := table.ValidateAutoincrementColumn(); err != nil {
		return nil, err
	}

	// Initialize sequence if table has AUTOINCREMENT column
	if _, hasAutoincrement := table.HasAutoincrementColumn(); hasAutoincrement {
		s.Sequences.InitSequence(stmt.Name)
	}

	s.Tables[stmt.Name] = table
	return table, nil
}

// CreateIndex creates an index from a CREATE INDEX statement.
func (s *Schema) CreateIndex(stmt *parser.CreateIndexStmt) (*Index, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil statement")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, skip := s.checkExistingIndex(stmt); skip {
		return existing, nil
	} else if existing != nil {
		return nil, fmt.Errorf("index already exists: %s", stmt.Name)
	}

	if !s.tableExistsLocked(stmt.Table) {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	index := s.buildIndex(stmt)
	s.Indexes[stmt.Name] = index
	return index, nil
}

// checkExistingIndex checks if an index already exists.
// Returns (existing, true) if should skip, (existing, false) if error needed, (nil, false) if OK to proceed.
func (s *Schema) checkExistingIndex(stmt *parser.CreateIndexStmt) (*Index, bool) {
	lowerName := strings.ToLower(stmt.Name)
	for indexName := range s.Indexes {
		if strings.ToLower(indexName) == lowerName {
			if stmt.IfNotExists {
				return s.Indexes[indexName], true
			}
			return s.Indexes[indexName], false
		}
	}
	return nil, false
}

// tableExistsLocked checks if a table exists (caller must hold lock).
func (s *Schema) tableExistsLocked(tableName string) bool {
	lowerTableName := strings.ToLower(tableName)
	for name := range s.Tables {
		if strings.ToLower(name) == lowerTableName {
			return true
		}
	}
	return false
}

// buildIndex creates an Index from a CREATE INDEX statement.
func (s *Schema) buildIndex(stmt *parser.CreateIndexStmt) *Index {
	columns := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = col.Column
	}

	index := &Index{
		Name:     stmt.Name,
		Table:    stmt.Table,
		RootPage: 0,
		SQL:      stmt.String(),
		Columns:  columns,
		Unique:   stmt.Unique,
		Partial:  stmt.Where != nil,
	}

	if stmt.Where != nil {
		index.Where = stmt.Where.String()
	}
	return index
}

// uniqueStrings removes duplicates from a slice while preserving order.
func uniqueStrings(strs []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(strs))

	for _, s := range strs {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}

	return result
}
