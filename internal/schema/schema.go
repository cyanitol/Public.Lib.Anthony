// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package schema provides schema management for SQLite databases.
// It tracks tables, indexes, and their metadata including type affinities.
package schema

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// reservedNames contains SQLite reserved schema object names that should not be used.
var reservedNames = map[string]bool{
	"sqlite_master":      true,
	"sqlite_temp_master": true,
	"sqlite_sequence":    true,
}

// IsReservedName checks if a name is reserved by SQLite.
// Reserved names cannot be used for user-created tables, indexes, or views.
func IsReservedName(name string) bool {
	return reservedNames[strings.ToLower(name)]
}

// Schema represents a database schema containing tables, indexes, views, and triggers.
// It is safe for concurrent access.
type Schema struct {
	Tables    map[string]*Table
	Indexes   map[string]*Index
	Views     map[string]*View
	Triggers  map[string]*Trigger // Trigger definitions
	Sequences *SequenceManager    // Manages AUTOINCREMENT sequences
	mu        sync.RWMutex
}

// NewSchema creates a new empty schema.
func NewSchema() *Schema {
	return &Schema{
		Tables:    make(map[string]*Table),
		Indexes:   make(map[string]*Index),
		Views:     make(map[string]*View),
		Triggers:  make(map[string]*Trigger),
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
	Stats        *TableStats       // Table statistics (optional, may be nil)

	// Virtual table fields
	IsVirtual    bool        // True for virtual tables
	Module       string      // Virtual table module name (e.g., "fts5", "rtree")
	ModuleArgs   []string    // Module-specific arguments
	VirtualTable interface{} // Virtual table instance (vtab.VirtualTable)
}

// SetRootPage updates the root page for this table.
func (t *Table) SetRootPage(root uint32) {
	t.RootPage = root
}

// GetRootPage returns the root page for this table.
func (t *Table) GetRootPage() uint32 {
	return t.RootPage
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

// GetDefault returns the default value for this column (for interface access).
func (c *Column) GetDefault() interface{} {
	return c.Default
}

// GetName returns the column name (for interface access from vdbe).
func (c *Column) GetName() string {
	return c.Name
}

// GetType returns the declared type for this column (for interface access from vdbe).
func (c *Column) GetType() string {
	return c.Type
}

// IsUniqueColumn returns true if this column has a UNIQUE constraint (for interface access from vdbe).
func (c *Column) IsUniqueColumn() bool {
	return c.Unique
}

// IsPrimaryKeyColumn returns true if this column is part of the PRIMARY KEY (for interface access from vdbe).
func (c *Column) IsPrimaryKeyColumn() bool {
	return c.PrimaryKey
}

// IsIntegerPrimaryKey returns true if this is an INTEGER PRIMARY KEY (rowid alias).
func (c *Column) IsIntegerPrimaryKey() bool {
	return c.PrimaryKey && (c.Type == "INTEGER" || c.Type == "INT")
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
	Name        string              // Index name
	Table       string              // Table name this index belongs to
	RootPage    uint32              // B-tree root page number
	SQL         string              // CREATE INDEX statement
	Columns     []string            // Indexed column names
	Expressions []parser.Expression // Expression for each indexed column (nil for simple columns)
	Unique      bool                // True for UNIQUE indexes
	Partial     bool                // True for partial indexes (WHERE clause)
	Where       string              // WHERE clause for partial indexes
}

// IsUnique returns true if this is a UNIQUE index.
func (idx *Index) IsUnique() bool {
	return idx.Unique
}

// GetColumns returns the indexed column names.
func (idx *Index) GetColumns() []string {
	return idx.Columns
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

// GetTableByName retrieves a table by name (case-insensitive).
// Returns the table (as interface{}) and true if found, nil and false otherwise.
// The return type is interface{} to avoid import cycles with the vdbe package.
func (s *Schema) GetTableByName(name string) (interface{}, bool) {
	table, found := s.GetTable(name)
	if !found {
		return nil, false
	}
	return table, true
}

// GetTableByRootPage retrieves a table by its root page number.
// Returns the table (as interface{}) and true if found, nil and false otherwise.
// The return type is interface{} to avoid import cycles with the vdbe package.
func (s *Schema) GetTableByRootPage(rootPage uint32) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, table := range s.Tables {
		if table.RootPage == rootPage {
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

// ListIndexesForTable returns all indexes for a table as a slice of interface{}.
// This is used by the VDBE to check unique constraints without importing schema.
func (s *Schema) ListIndexesForTable(tableName string) []interface{} {
	indexes := s.GetTableIndexes(tableName)
	result := make([]interface{}, len(indexes))
	for i, idx := range indexes {
		result[i] = idx
	}
	return result
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

// checkNameConflict checks if a name conflicts with existing tables or indexes.
// Caller must hold the schema lock.
func (s *Schema) checkNameConflict(name string) error {
	lowerName := strings.ToLower(name)

	for tableName := range s.Tables {
		if strings.ToLower(tableName) == lowerName {
			return fmt.Errorf("there is already another table or index with this name: %s", name)
		}
	}

	for indexName := range s.Indexes {
		if strings.ToLower(indexName) == lowerName {
			return fmt.Errorf("there is already another table or index with this name: %s", name)
		}
	}

	return nil
}

// findTableName finds the actual table name (case-insensitive lookup).
// Returns the actual name and true if found, empty string and false otherwise.
// Caller must hold the schema lock.
func (s *Schema) findTableName(name string) (string, bool) {
	lowerName := strings.ToLower(name)
	for tableName := range s.Tables {
		if strings.ToLower(tableName) == lowerName {
			return tableName, true
		}
	}
	return "", false
}

// updateIndexTableReferences updates all index references from oldName to newName.
// Caller must hold the schema lock.
func (s *Schema) updateIndexTableReferences(oldName, newName string) {
	lowerOldName := strings.ToLower(oldName)
	for _, idx := range s.Indexes {
		if strings.ToLower(idx.Table) == lowerOldName {
			idx.Table = newName
		}
	}
}

// RenameTable renames a table in the schema.
func (s *Schema) RenameTable(oldName, newName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if new name conflicts with existing table or index
	if err := s.checkNameConflict(newName); err != nil {
		return err
	}

	// Find the old table
	actualOldName, found := s.findTableName(oldName)
	if !found {
		return fmt.Errorf("table not found: %s", oldName)
	}

	// Get the table and update its name
	table := s.Tables[actualOldName]
	delete(s.Tables, actualOldName)
	table.Name = newName
	s.Tables[newName] = table

	// Update index references
	s.updateIndexTableReferences(oldName, newName)

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

// isRowidAlias checks if a name is a special rowid alias.
func isRowidAlias(name string) bool {
	lowerName := strings.ToLower(name)
	return lowerName == "rowid" || lowerName == "_rowid_" || lowerName == "oid"
}

// findIntegerPrimaryKeyIndex searches for an INTEGER PRIMARY KEY column.
// Returns the column index or -1 if not found.
func (t *Table) findIntegerPrimaryKeyIndex() int {
	for i, col := range t.Columns {
		if col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT") {
			return i
		}
	}
	return -1
}

// GetIntegerPKColumn returns the name of the INTEGER PRIMARY KEY column.
// Returns empty string if no INTEGER PRIMARY KEY column exists.
// This is used by FK validation to include the rowid in extracted values.
func (t *Table) GetIntegerPKColumn() string {
	idx := t.findIntegerPrimaryKeyIndex()
	if idx >= 0 {
		return t.Columns[idx].Name
	}
	return ""
}

// GetColumnIndexWithRowidAliases returns the index of a column by name,
// handling both regular column names and special rowid aliases (rowid, _rowid_, oid).
// Returns -1 if not found. Returns -2 if the name is a rowid alias but no
// INTEGER PRIMARY KEY column exists (indicating implicit rowid should be used).
func (t *Table) GetColumnIndexWithRowidAliases(name string) int {
	// First try exact match
	idx := t.GetColumnIndex(name)
	if idx >= 0 {
		return idx
	}

	// Check if this is a rowid alias
	if !isRowidAlias(name) {
		return -1
	}

	// Look for INTEGER PRIMARY KEY column
	if idx := t.findIntegerPrimaryKeyIndex(); idx >= 0 {
		return idx
	}

	// No INTEGER PRIMARY KEY, but this is a rowid alias - return special marker
	return -2
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

// GetCollation returns the collation for a column (may be empty if not set).
func (c *Column) GetCollation() string {
	return c.Collation
}

// GetColumns returns the columns as a slice of interfaces (for VDBE access).
func (t *Table) GetColumns() []interface{} {
	result := make([]interface{}, len(t.Columns))
	for i, col := range t.Columns {
		result[i] = col
	}
	return result
}

// GetColumnNames returns the column names for this table.
func (t *Table) GetColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		names[i] = col.Name
	}
	return names
}

// HasRowID returns true if the table has an implicit rowid column.
// Tables have a rowid unless they are declared WITHOUT ROWID.
func (t *Table) HasRowID() bool {
	return !t.WithoutRowID
}

// GetRecordColumnNames returns column names that are stored in the B-tree record.
// For normal tables, this excludes the INTEGER PRIMARY KEY column (the rowid alias)
// since it is stored as the B-tree key, not in the record payload.
// For WITHOUT ROWID tables, all columns are included.
func (t *Table) GetRecordColumnNames() []string {
	if t.WithoutRowID {
		return t.GetColumnNames()
	}
	names := make([]string, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT") {
			continue // rowid alias - stored as B-tree key, not in record
		}
		names = append(names, col.Name)
	}
	return names
}

// GetRowidColumnName returns the name of the INTEGER PRIMARY KEY column
// that serves as the rowid alias, or empty string if none exists.
// For WITHOUT ROWID tables, always returns empty string.
func (t *Table) GetRowidColumnName() string {
	if t.WithoutRowID {
		return ""
	}
	for _, col := range t.Columns {
		if col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT") {
			return col.Name
		}
	}
	return ""
}

// GetPrimaryKey returns the primary key column names in constraint order.
// For WITHOUT ROWID tables, this determines the order of key encoding.
func (t *Table) GetPrimaryKey() []string {
	return t.PrimaryKey
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

	// Check for reserved names
	if IsReservedName(stmt.Name) {
		return nil, fmt.Errorf("table name is reserved: %s", stmt.Name)
	}

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

	// Validate WITHOUT ROWID constraints
	if err := table.ValidateWithoutRowIDConstraints(); err != nil {
		return nil, err
	}

	// Initialize sequence if table has AUTOINCREMENT column
	if _, hasAutoincrement := table.HasAutoincrementColumn(); hasAutoincrement {
		s.Sequences.InitSequence(stmt.Name)
	}

	s.Tables[stmt.Name] = table
	return table, nil
}

// CreateVirtualTable creates a virtual table and registers it in the schema.
func (s *Schema) CreateVirtualTable(name, module string, args []string, vtab interface{}, schemaDDL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for reserved names
	if IsReservedName(name) {
		return fmt.Errorf("table name is reserved: %s", name)
	}

	// Check if table already exists
	if _, exists := s.Tables[name]; exists {
		return fmt.Errorf("table already exists: %s", name)
	}

	// Build column definitions from module arguments
	columns := make([]*Column, 0, len(args))
	for _, arg := range args {
		colName := strings.TrimSpace(arg)
		if colName != "" {
			columns = append(columns, &Column{
				Name:     colName,
				Type:     "TEXT",
				Affinity: AffinityText,
			})
		}
	}

	table := &Table{
		Name:         name,
		RootPage:     0,                                                                                           // Virtual tables don't use B-tree pages
		SQL:          fmt.Sprintf("CREATE VIRTUAL TABLE %s USING %s(%s)", name, module, strings.Join(args, ", ")), // nosec: name/module/args are from internal schema registration, not user input
		Columns:      columns,
		IsVirtual:    true,
		Module:       module,
		ModuleArgs:   args,
		VirtualTable: vtab,
	}

	s.Tables[name] = table
	return nil
}

// CreateIndex creates an index from a CREATE INDEX statement.
func (s *Schema) CreateIndex(stmt *parser.CreateIndexStmt) (*Index, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil statement")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for reserved names
	if IsReservedName(stmt.Name) {
		return nil, fmt.Errorf("index name is reserved: %s", stmt.Name)
	}

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

// viewExistsLocked checks if a view exists (caller must hold lock).
func (s *Schema) viewExistsLocked(viewName string) bool {
	lowerViewName := strings.ToLower(viewName)
	for name := range s.Views {
		if strings.ToLower(name) == lowerViewName {
			return true
		}
	}
	return false
}

// buildIndex creates an Index from a CREATE INDEX statement.
func (s *Schema) buildIndex(stmt *parser.CreateIndexStmt) *Index {
	columns := make([]string, len(stmt.Columns))
	expressions := make([]parser.Expression, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = col.Column
		expressions[i] = col.Expr
	}

	index := &Index{
		Name:        stmt.Name,
		Table:       stmt.Table,
		RootPage:    0,
		SQL:         stmt.String(),
		Columns:     columns,
		Expressions: expressions,
		Unique:      stmt.Unique,
		Partial:     stmt.Where != nil,
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

// AddTableUnsafe adds a table to the schema without validation.
// This method is NOT thread-safe and should only be used when the caller
// already holds the schema lock or during initialization.
// For normal use, use CreateTable instead.
func (s *Schema) AddTableUnsafe(table *Table) {
	s.Tables[table.Name] = table
}

// AddIndexUnsafe adds an index to the schema without validation.
// This method is NOT thread-safe and should only be used when the caller
// already holds the schema lock or during initialization.
// For normal use, use CreateIndex instead.
func (s *Schema) AddIndexUnsafe(index *Index) {
	s.Indexes[index.Name] = index
}

// AddViewUnsafe adds a view to the schema without validation.
// This method is NOT thread-safe and should only be used when the caller
// already holds the schema lock or during initialization.
func (s *Schema) AddViewUnsafe(view *View) {
	s.Views[view.Name] = view
}

// AddTriggerUnsafe adds a trigger to the schema without validation.
// This method is NOT thread-safe and should only be used when the caller
// already holds the schema lock or during initialization.
func (s *Schema) AddTriggerUnsafe(trigger *Trigger) {
	if s.Triggers == nil {
		s.Triggers = make(map[string]*Trigger)
	}
	s.Triggers[trigger.Name] = trigger
}

// AddTableDirect adds a table directly to the schema with proper locking.
// This is a low-level method that bypasses validation and is intended
// for special cases like VACUUM or temporary tables.
func (s *Schema) AddTableDirect(table *Table) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Tables[table.Name] = table
}

// AddIndexDirect adds an index directly to the schema with proper locking.
// This is a low-level method that bypasses validation.
func (s *Schema) AddIndexDirect(index *Index) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Indexes[index.Name] = index
}

// AddViewDirect adds a view directly to the schema with proper locking.
// This is a low-level method that bypasses validation.
func (s *Schema) AddViewDirect(view *View) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Views[view.Name] = view
}

// AddTriggerDirect adds a trigger directly to the schema with proper locking.
// This is a low-level method that bypasses validation.
func (s *Schema) AddTriggerDirect(trigger *Trigger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Triggers == nil {
		s.Triggers = make(map[string]*Trigger)
	}
	s.Triggers[trigger.Name] = trigger
}

// IndexCount returns the number of indexes in the schema.
func (s *Schema) IndexCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Indexes)
}

// TableCount returns the number of tables in the schema.
func (s *Schema) TableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Tables)
}

// ViewCount returns the number of views in the schema.
func (s *Schema) ViewCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Views)
}

// TriggerCount returns the number of triggers in the schema.
func (s *Schema) TriggerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Triggers)
}

// IsView checks if a given name refers to a view (not a table).
func (s *Schema) IsView(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.Views[strings.ToLower(name)]
	return exists
}

// TableStats represents statistics for a table.
// These statistics are used by the query planner to estimate costs.
type TableStats struct {
	// RowCount is the approximate number of rows in the table
	RowCount int64

	// AverageRowSize is the average size of a row in bytes
	AverageRowSize int64

	// LastUpdated is a timestamp when statistics were last updated
	// (currently not used, reserved for future use)
	LastUpdated int64
}

// IndexStats represents statistics for an index.
type IndexStats struct {
	// Uniqueness is the average number of rows per distinct key
	// (1.0 for unique indexes, higher for non-unique)
	Uniqueness float64

	// Selectivity is the estimated fraction of rows matched by typical queries
	// (0.01 = 1% of rows, useful for partial indexes)
	Selectivity float64
}

// GetTableStats returns the statistics for a table, or nil if not available.
func (t *Table) GetTableStats() *TableStats {
	return t.Stats
}

// SetTableStats sets the statistics for a table.
func (t *Table) SetTableStats(stats *TableStats) {
	t.Stats = stats
}

// IsNotNull returns true if this column has a NOT NULL constraint (for interface access from vdbe).
func (c *Column) IsNotNull() bool {
	return c.NotNull
}

// GetNotNull returns true if this column has a NOT NULL constraint (for interface access from vdbe).
func (c *Column) GetNotNull() bool {
	return c.NotNull
}

// GetCheck returns the CHECK constraint expression for this column (for interface access from vdbe).
func (c *Column) GetCheck() string {
	return c.Check
}
