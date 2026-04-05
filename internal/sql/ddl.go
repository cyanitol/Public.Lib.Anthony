// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// Schema represents the database schema containing tables and indexes.
type Schema struct {
	Tables  map[string]*Table // Table name -> Table definition
	Indexes map[string]*Index // Index name -> Index definition
	Views   map[string]*View  // View name -> View definition
}

// NewSchema creates a new empty schema.
func NewSchema() *Schema {
	return &Schema{
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
		Views:   make(map[string]*View),
	}
}

// GetTable returns a table by name.
func (s *Schema) GetTable(name string) *Table {
	return s.Tables[name]
}

// AddTable adds a table to the schema.
func (s *Schema) AddTable(table *Table) error {
	if _, exists := s.Tables[table.Name]; exists {
		return fmt.Errorf("table %q already exists", table.Name)
	}
	s.Tables[table.Name] = table
	return nil
}

// RemoveTable removes a table from the schema.
func (s *Schema) RemoveTable(name string) error {
	if _, exists := s.Tables[name]; !exists {
		return fmt.Errorf("table %q does not exist", name)
	}
	delete(s.Tables, name)
	return nil
}

// AddIndex adds an index to the schema.
func (s *Schema) AddIndex(index *Index) error {
	if _, exists := s.Indexes[index.Name]; exists {
		return fmt.Errorf("index %q already exists", index.Name)
	}
	s.Indexes[index.Name] = index
	return nil
}

// RemoveIndex removes an index from the schema.
func (s *Schema) RemoveIndex(name string) error {
	if _, exists := s.Indexes[name]; !exists {
		return fmt.Errorf("index %q does not exist", name)
	}
	delete(s.Indexes, name)
	return nil
}

// Index represents a database index.
type Index struct {
	Name     string   // Index name
	Table    string   // Table name
	Columns  []string // Indexed column names
	Unique   bool     // True for UNIQUE indexes
	RootPage int      // Root page in database file
}

// View represents a database view.
type View struct {
	Name      string             // View name
	Columns   []string           // Optional explicit column names
	Select    *parser.SelectStmt // The SELECT statement defining the view
	SQL       string             // CREATE VIEW statement
	Temporary bool               // True for temporary views
}

// CompileCreateTable generates VDBE bytecode for CREATE TABLE.
func CompileCreateTable(stmt *parser.CreateTableStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	if v, done, err := checkTableExists(stmt, schema); done {
		return v, err
	}
	if err := validateTableName(stmt.Name); err != nil {
		return nil, err
	}
	table, err := createTableFromAST(stmt, bt)
	if err != nil {
		return nil, fmt.Errorf("failed to create table definition: %w", err)
	}
	if err := schema.AddTable(table); err != nil {
		return nil, err
	}
	return buildCreateTableVDBE(stmt, table, bt), nil
}

// checkTableExists returns early if table already exists.
func checkTableExists(stmt *parser.CreateTableStmt, schema *Schema) (*vdbe.VDBE, bool, error) {
	if existingTable := schema.GetTable(stmt.Name); existingTable == nil {
		return nil, false, nil
	}
	if stmt.IfNotExists {
		return noopVDBE(), true, nil
	}
	return nil, true, fmt.Errorf("table %q already exists", stmt.Name)
}

// validateTableName validates that a table name is valid and not reserved.
func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if isReservedName(name) {
		return fmt.Errorf("table name %q is reserved", name)
	}
	return nil
}

// buildCreateTableVDBE builds the VDBE bytecode for CREATE TABLE.
func buildCreateTableVDBE(stmt *parser.CreateTableStmt, table *Table, bt *btree.Btree) *vdbe.VDBE {
	v := vdbe.New()
	v.SetReadOnly(false)
	v.AddOp(vdbe.OpInit, 0, 0, 0)

	rootPage := allocateRootPage(bt)
	table.RootPage = int(rootPage)

	insertMasterRecord(v, masterRecord{
		objType:  "table",
		name:     table.Name,
		tblName:  table.Name,
		rootPage: int(rootPage),
		sql:      generateCreateTableSQL(stmt),
	})
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	return v
}

// CompileDropTable generates VDBE bytecode for DROP TABLE.
func CompileDropTable(stmt *parser.DropTableStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	table := schema.GetTable(stmt.Name)
	if table == nil {
		if stmt.IfExists {
			return noopVDBE(), nil
		}
		return nil, fmt.Errorf("table %q does not exist", stmt.Name)
	}

	if isReservedName(stmt.Name) {
		return nil, fmt.Errorf("cannot drop system table %q", stmt.Name)
	}

	schema.RemoveTable(stmt.Name)
	return buildMasterDeleteVDBE(table.Name), nil
}

// CompileCreateIndex generates VDBE bytecode for CREATE INDEX.
func CompileCreateIndex(stmt *parser.CreateIndexStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	if v, done, err := checkIndexExists(stmt, schema); done {
		return v, err
	}

	table, columnNames, err := validateIndexTarget(stmt, schema)
	if err != nil {
		return nil, err
	}

	rootPage := allocateRootPage(bt)
	index := &Index{
		Name:     stmt.Name,
		Table:    stmt.Table,
		Columns:  columnNames,
		Unique:   stmt.Unique,
		RootPage: int(rootPage),
	}
	if err := schema.AddIndex(index); err != nil {
		return nil, err
	}

	return buildCreateIndexVDBE(stmt, index, table, columnNames, rootPage), nil
}

func checkIndexExists(stmt *parser.CreateIndexStmt, schema *Schema) (*vdbe.VDBE, bool, error) {
	if _, exists := schema.Indexes[stmt.Name]; !exists {
		return nil, false, nil
	}
	if stmt.IfNotExists {
		return noopVDBE(), true, nil
	}
	return nil, true, fmt.Errorf("index %q already exists", stmt.Name)
}

func validateIndexTarget(stmt *parser.CreateIndexStmt, schema *Schema) (*Table, []string, error) {
	table := schema.GetTable(stmt.Table)
	if table == nil {
		return nil, nil, fmt.Errorf("table %q does not exist", stmt.Table)
	}
	columnNames, err := resolveIndexColumns(stmt, table)
	if err != nil {
		return nil, nil, err
	}
	return table, columnNames, nil
}

func resolveIndexColumns(stmt *parser.CreateIndexStmt, table *Table) ([]string, error) {
	tableColIndex := make(map[string]bool, len(table.Columns))
	for _, col := range table.Columns {
		tableColIndex[col.Name] = true
	}
	columnNames := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		if !tableColIndex[col.Column] {
			return nil, fmt.Errorf("column %q does not exist in table %q", col.Column, stmt.Table)
		}
		columnNames[i] = col.Column
	}
	return columnNames, nil
}

func findColumnIndex(table *Table, colName string) int {
	for j, col := range table.Columns {
		if col.Name == colName {
			return j
		}
	}
	return -1
}

func buildCreateIndexVDBE(stmt *parser.CreateIndexStmt, index *Index, table *Table, columnNames []string, rootPage uint32) *vdbe.VDBE {
	v := vdbe.New()
	v.SetReadOnly(false)
	v.AddOp(vdbe.OpInit, 0, 0, 0)

	insertMasterRecord(v, masterRecord{
		objType:  "index",
		name:     index.Name,
		tblName:  index.Table,
		rootPage: int(rootPage),
		sql:      generateCreateIndexSQL(stmt),
	})

	v.AddOp(vdbe.OpOpenRead, 1, table.RootPage, 0)
	v.AllocCursors(3)
	v.AddOp(vdbe.OpOpenWrite, 2, int(rootPage), 0)

	addrRewind := v.AddOp(vdbe.OpRewind, 1, 0, 0)
	addrLoop := v.NumOps()

	for i, colName := range columnNames {
		if colIdx := findColumnIndex(table, colName); colIdx >= 0 {
			v.AddOp(vdbe.OpColumn, 1, colIdx, 10+i)
		}
	}

	v.AddOp(vdbe.OpRowid, 1, 7, 0)
	v.AddOp(vdbe.OpMakeRecord, 10, len(columnNames)+1, 8)
	v.AddOp(vdbe.OpIdxInsert, 2, 8, 0)
	v.AddOp(vdbe.OpNext, 1, addrLoop, 0)

	if instr, _ := v.GetInstruction(addrRewind); instr != nil {
		instr.P2 = v.NumOps()
	}

	v.AddOp(vdbe.OpClose, 0, 0, 0)
	v.AddOp(vdbe.OpClose, 1, 0, 0)
	v.AddOp(vdbe.OpClose, 2, 0, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	return v
}

// createTableFromAST creates a Table definition from the parser AST.
func createTableFromAST(stmt *parser.CreateTableStmt, bt *btree.Btree) (*Table, error) {
	if len(stmt.Columns) == 0 {
		return nil, fmt.Errorf("table must have at least one column")
	}

	table := &Table{
		Name:        stmt.Name,
		NumColumns:  len(stmt.Columns),
		Columns:     make([]Column, len(stmt.Columns)),
		RootPage:    0, // Will be set later
		PrimaryKey:  -1,
		RowidColumn: -1,
	}

	// Process columns
	for i, colDef := range stmt.Columns {
		table.Columns[i] = createColumnFromDef(colDef)
		applyConstraintsToTable(table, i, colDef.Constraints)
	}

	return table, nil
}

// createColumnFromDef creates a Column from a parser.ColumnDef.
func createColumnFromDef(colDef parser.ColumnDef) Column {
	col := Column{
		Name:     colDef.Name,
		DeclType: colDef.Type,
		Affinity: typeNameToAffinity(colDef.Type),
	}
	applyConstraintsToColumn(&col, colDef.Constraints)
	return col
}

// constraintHandler applies a constraint to a column and/or table.
type constraintHandler func(col *Column, table *Table, colIdx int, constraint parser.ColumnConstraint)

var constraintHandlers = map[parser.ConstraintType]constraintHandler{
	parser.ConstraintPrimaryKey: applyPrimaryKey,
	parser.ConstraintNotNull:    applyNotNull,
	parser.ConstraintDefault:    applyDefault,
}

func applyPrimaryKey(col *Column, table *Table, colIdx int, constraint parser.ColumnConstraint) {
	col.PrimaryKey = true
	if table != nil {
		table.PrimaryKey = colIdx
		if constraint.PrimaryKey != nil && constraint.PrimaryKey.Autoincrement {
			table.RowidColumn = colIdx
		}
	}
}

func applyNotNull(col *Column, table *Table, colIdx int, constraint parser.ColumnConstraint) {
	col.NotNull = true
}

func applyDefault(col *Column, table *Table, colIdx int, constraint parser.ColumnConstraint) {
	col.DefaultValue = convertExpr(constraint.Default)
}

// applyConstraintsToColumn applies constraints to a column without table context.
func applyConstraintsToColumn(col *Column, constraints []parser.ColumnConstraint) {
	for _, constraint := range constraints {
		if handler, ok := constraintHandlers[constraint.Type]; ok {
			handler(col, nil, 0, constraint)
		}
	}
}

// applyConstraintsToTable applies constraints that affect the table.
func applyConstraintsToTable(table *Table, colIdx int, constraints []parser.ColumnConstraint) {
	for _, constraint := range constraints {
		if handler, ok := constraintHandlers[constraint.Type]; ok {
			handler(&table.Columns[colIdx], table, colIdx, constraint)
		}
	}
}

// typeNameToAffinity converts a type name to type affinity.
func typeNameToAffinity(typeName string) Affinity {
	if typeName == "" {
		return SQLITE_AFF_BLOB
	}
	return affinityFromUpperTypeName(strings.ToUpper(typeName))
}

func affinityFromUpperTypeName(upper string) Affinity {
	if strings.Contains(upper, "INT") {
		return SQLITE_AFF_INTEGER
	}
	if containsAnyDDL(upper, "CHAR", "CLOB", "TEXT") {
		return SQLITE_AFF_TEXT
	}
	if strings.Contains(upper, "BLOB") {
		return SQLITE_AFF_BLOB
	}
	if containsAnyDDL(upper, "REAL", "FLOA", "DOUB") {
		return SQLITE_AFF_REAL
	}
	return SQLITE_AFF_NUMERIC
}

func containsAnyDDL(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// noopVDBE returns a VDBE that immediately halts with success.
// Used for IF NOT EXISTS / IF EXISTS early returns.
func noopVDBE() *vdbe.VDBE {
	v := vdbe.New()
	v.AddOp(vdbe.OpHalt, 0, 0, 0)
	return v
}

// isReservedName returns true if name is a reserved system table name.
func isReservedName(name string) bool {
	lower := strings.ToLower(name)
	return lower == "sqlite_master" || lower == "sqlite_schema"
}

// masterRecord holds the metadata needed to insert into sqlite_master.
type masterRecord struct {
	objType  string // "table", "index", "view", "trigger"
	name     string
	tblName  string
	rootPage int
	sql      string
}

// insertMasterRecord emits VDBE ops that write a row into sqlite_master
// (registers 1-5) and insert it via cursor 0.
func insertMasterRecord(v *vdbe.VDBE, rec masterRecord) {
	v.AllocMemory(6)
	v.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, rec.objType)
	v.AddOpWithP4Str(vdbe.OpString, 0, 2, 0, rec.name)
	v.AddOpWithP4Str(vdbe.OpString, 0, 3, 0, rec.tblName)
	v.AddOpWithP4Int(vdbe.OpInteger, rec.rootPage, 4, 0, int32(rec.rootPage))
	v.AddOpWithP4Str(vdbe.OpString, 0, 5, 0, rec.sql)

	v.AllocCursors(1)
	v.AddOp(vdbe.OpOpenWrite, 0, 1, 0)
	v.AddOp(vdbe.OpMakeRecord, 1, 5, 6)
	v.AddOp(vdbe.OpInsert, 0, 6, 0)
	v.AddOp(vdbe.OpClose, 0, 0, 0)
}

// buildMasterDeleteVDBE builds a complete VDBE program that scans sqlite_master,
// finds the row with the given name, and deletes it.
func buildMasterDeleteVDBE(name string) *vdbe.VDBE {
	v := vdbe.New()
	v.SetReadOnly(false)
	v.AddOp(vdbe.OpInit, 0, 0, 0)
	v.AllocMemory(3)
	v.AllocCursors(1)
	v.AddOp(vdbe.OpOpenWrite, 0, 1, 0)
	v.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, name)

	addrLoop := v.AddOp(vdbe.OpRewind, 0, 0, 0)
	addrNext := v.NumOps()
	v.AddOp(vdbe.OpColumn, 0, 1, 2)
	addrDelete := v.AddOp(vdbe.OpEq, 1, 0, 2)
	v.AddOp(vdbe.OpNext, 0, addrNext, 0)
	v.AddOp(vdbe.OpGoto, 0, addrLoop+1, 0)

	addrDeleteOp := v.NumOps()
	v.AddOp(vdbe.OpDelete, 0, 0, 0)

	if instr, _ := v.GetInstruction(addrDelete); instr != nil {
		instr.P2 = addrDeleteOp
	}
	if instr, _ := v.GetInstruction(addrLoop); instr != nil {
		instr.P2 = v.NumOps()
	}

	v.AddOp(vdbe.OpClose, 0, 0, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)
	return v
}

// convertExpr converts parser.Expression to sql.Expr.
// This is a simplified conversion for default values.
func convertExpr(expr parser.Expression) *Expr {
	if expr == nil {
		return nil
	}

	// Handle literals
	if lit, ok := expr.(*parser.LiteralExpr); ok {
		result := &Expr{}
		switch lit.Type {
		case parser.LiteralInteger:
			result.Op = TK_INTEGER
			result.StringValue = lit.Value
		case parser.LiteralFloat:
			result.Op = TK_FLOAT
			result.StringValue = lit.Value
		case parser.LiteralString:
			result.Op = TK_STRING
			result.StringValue = lit.Value
		case parser.LiteralNull:
			result.Op = TK_NULL
		}
		return result
	}

	// For other expression types, create a placeholder
	return &Expr{
		Op: TK_NULL,
	}
}

// allocateRootPage allocates a new root page in the btree.
// This is a simplified implementation.
func allocateRootPage(bt *btree.Btree) uint32 {
	// In a real implementation, this would interact with the pager
	// to allocate a new page. For now, we'll use the number of pages + 2
	// (page 1 is sqlite_master, so start from 2)
	return uint32(len(bt.Pages) + 2)
}

var simpleConstraintSQL = map[parser.ConstraintType]string{
	parser.ConstraintNotNull: " NOT NULL",
	parser.ConstraintUnique:  " UNIQUE",
}

func writeConstraint(sql *strings.Builder, constraint parser.ColumnConstraint) {
	if text, ok := simpleConstraintSQL[constraint.Type]; ok {
		sql.WriteString(text)
		return
	}
	if constraint.Type == parser.ConstraintDefault {
		sql.WriteString(" DEFAULT ")
		sql.WriteString(constraint.Default.String())
		return
	}
	if constraint.Type == parser.ConstraintPrimaryKey {
		sql.WriteString(" PRIMARY KEY")
		if constraint.PrimaryKey != nil && constraint.PrimaryKey.Autoincrement {
			sql.WriteString(" AUTOINCREMENT")
		}
	}
}

func writeColumnDef(sql *strings.Builder, col parser.ColumnDef, sep string) {
	sql.WriteString(sep)
	sql.WriteString(col.Name)
	if col.Type != "" {
		sql.WriteString(" ")
		sql.WriteString(col.Type)
	}
	for _, constraint := range col.Constraints {
		writeConstraint(sql, constraint)
	}
}

func generateCreateTableSQL(stmt *parser.CreateTableStmt) string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE ")
	if stmt.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(stmt.Name)
	sql.WriteString(" (")
	for i, col := range stmt.Columns {
		sep := ""
		if i > 0 {
			sep = ", "
		}
		writeColumnDef(&sql, col, sep)
	}
	sql.WriteString(")")
	return sql.String()
}

// generateCreateIndexSQL generates the CREATE INDEX SQL text from the AST.
func generateCreateIndexSQL(stmt *parser.CreateIndexStmt) string {
	var sql strings.Builder
	sql.WriteString("CREATE ")
	if stmt.Unique {
		sql.WriteString("UNIQUE ")
	}
	sql.WriteString("INDEX ")
	if stmt.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(stmt.Name)
	sql.WriteString(" ON ")
	sql.WriteString(stmt.Table)
	sql.WriteString(" (")

	for i, col := range stmt.Columns {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(col.Column)
		switch col.Order {
		case parser.SortAsc:
			sql.WriteString(" ASC")
		case parser.SortDesc:
			sql.WriteString(" DESC")
		}
	}

	sql.WriteString(")")
	return sql.String()
}

// CompileCreateView generates VDBE bytecode for CREATE VIEW.
func CompileCreateView(stmt *parser.CreateViewStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	if v, done, err := checkViewExists(stmt, schema); done {
		return v, err
	}
	if err := validateViewDefinition(stmt, schema); err != nil {
		return nil, err
	}
	return buildCreateViewVDBE(stmt), nil
}

// checkViewExists returns early if view already exists or conflicts with a table.
func checkViewExists(stmt *parser.CreateViewStmt, schema *Schema) (*vdbe.VDBE, bool, error) {
	if existingView, exists := schema.Views[stmt.Name]; existingView != nil && exists {
		if stmt.IfNotExists {
			return noopVDBE(), true, nil
		}
		return nil, true, fmt.Errorf("view %q already exists", stmt.Name)
	}
	if existingTable := schema.GetTable(stmt.Name); existingTable != nil {
		return nil, true, fmt.Errorf("table %q already exists", stmt.Name)
	}
	return nil, false, nil
}

// validateViewDefinition validates view name and SELECT statement.
func validateViewDefinition(stmt *parser.CreateViewStmt, schema *Schema) error {
	if stmt.Name == "" {
		return fmt.Errorf("view name cannot be empty")
	}
	if isReservedName(stmt.Name) {
		return fmt.Errorf("view name %q is reserved", stmt.Name)
	}
	if stmt.Select == nil {
		return fmt.Errorf("view must have a SELECT statement")
	}
	return nil
}

// buildCreateViewVDBE generates VDBE bytecode to insert view definition into sqlite_master.
func buildCreateViewVDBE(stmt *parser.CreateViewStmt) *vdbe.VDBE {
	v := vdbe.New()
	v.SetReadOnly(false)
	v.AddOp(vdbe.OpInit, 0, 0, 0)

	insertMasterRecord(v, masterRecord{
		objType: "view",
		name:    stmt.Name,
		tblName: stmt.Name,
		sql:     generateCreateViewSQL(stmt),
	})
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	return v
}

// CompileDropView generates VDBE bytecode for DROP VIEW.
func CompileDropView(stmt *parser.DropViewStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	view, exists := schema.Views[stmt.Name]
	if !exists || view == nil {
		return handleMissingView(stmt)
	}
	if isReservedName(stmt.Name) {
		return nil, fmt.Errorf("cannot drop system view %q", stmt.Name)
	}
	return buildMasterDeleteVDBE(view.Name), nil
}

// handleMissingView handles the case when a view doesn't exist.
func handleMissingView(stmt *parser.DropViewStmt) (*vdbe.VDBE, error) {
	if stmt.IfExists {
		return noopVDBE(), nil
	}
	return nil, fmt.Errorf("view %q does not exist", stmt.Name)
}

// generateCreateViewSQL generates the CREATE VIEW SQL text from the AST.
func generateCreateViewSQL(stmt *parser.CreateViewStmt) string {
	var sql strings.Builder
	sql.WriteString("CREATE ")
	if stmt.Temporary {
		sql.WriteString("TEMP ")
	}
	sql.WriteString("VIEW ")
	if stmt.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(stmt.Name)

	// Add column list if specified
	if len(stmt.Columns) > 0 {
		sql.WriteString("(")
		for i, col := range stmt.Columns {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(col)
		}
		sql.WriteString(")")
	}

	sql.WriteString(" AS ")
	if stmt.Select != nil {
		sql.WriteString(stmt.Select.String())
	}

	return sql.String()
}

// CompileCreateTrigger generates VDBE bytecode for CREATE TRIGGER.
func CompileCreateTrigger(stmt *parser.CreateTriggerStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	if stmt.Name == "" {
		return nil, fmt.Errorf("trigger name cannot be empty")
	}

	table := schema.GetTable(stmt.Table)
	if table == nil {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	v := vdbe.New()
	v.SetReadOnly(false)
	v.AddOp(vdbe.OpInit, 0, 0, 0)

	insertMasterRecord(v, masterRecord{
		objType: "trigger",
		name:    stmt.Name,
		tblName: stmt.Table,
		sql:     generateCreateTriggerSQL(stmt),
	})
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	return v, nil
}

// CompileDropTrigger generates VDBE bytecode for DROP TRIGGER.
func CompileDropTrigger(stmt *parser.DropTriggerStmt, schema *Schema, bt *btree.Btree) (*vdbe.VDBE, error) {
	if isReservedName(stmt.Name) {
		return nil, fmt.Errorf("cannot drop system trigger %q", stmt.Name)
	}
	return buildMasterDeleteVDBE(stmt.Name), nil
}

// generateCreateTriggerSQL generates the CREATE TRIGGER SQL text from the AST.
func generateCreateTriggerSQL(stmt *parser.CreateTriggerStmt) string {
	var sql strings.Builder
	writeTriggerHeader(&sql, stmt)
	writeTriggerTiming(&sql, stmt.Timing)
	writeTriggerEvent(&sql, stmt)
	writeTriggerTarget(&sql, stmt)
	writeTriggerBody(&sql, stmt)
	return sql.String()
}

// writeTriggerHeader writes the CREATE TRIGGER header clause.
func writeTriggerHeader(sql *strings.Builder, stmt *parser.CreateTriggerStmt) {
	sql.WriteString("CREATE ")
	if stmt.Temp {
		sql.WriteString("TEMP ")
	}
	sql.WriteString("TRIGGER ")
	if stmt.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(stmt.Name)
	sql.WriteString(" ")
}

// writeTriggerTiming writes the trigger timing clause (BEFORE/AFTER/INSTEAD OF).
func writeTriggerTiming(sql *strings.Builder, timing parser.TriggerTiming) {
	switch timing {
	case parser.TriggerBefore:
		sql.WriteString("BEFORE ")
	case parser.TriggerAfter:
		sql.WriteString("AFTER ")
	case parser.TriggerInsteadOf:
		sql.WriteString("INSTEAD OF ")
	}
}

// writeTriggerEvent writes the trigger event clause (INSERT/UPDATE/DELETE).
func writeTriggerEvent(sql *strings.Builder, stmt *parser.CreateTriggerStmt) {
	switch stmt.Event {
	case parser.TriggerInsert:
		sql.WriteString("INSERT")
	case parser.TriggerUpdate:
		sql.WriteString("UPDATE")
		writeTriggerUpdateOf(sql, stmt.UpdateOf)
	case parser.TriggerDelete:
		sql.WriteString("DELETE")
	}
}

// writeTriggerUpdateOf writes the UPDATE OF column list if present.
func writeTriggerUpdateOf(sql *strings.Builder, columns []string) {
	if len(columns) == 0 {
		return
	}
	sql.WriteString(" OF ")
	for i, col := range columns {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(col)
	}
}

// writeTriggerTarget writes the ON table, FOR EACH ROW, and WHEN clauses.
func writeTriggerTarget(sql *strings.Builder, stmt *parser.CreateTriggerStmt) {
	sql.WriteString(" ON ")
	sql.WriteString(stmt.Table)

	if stmt.ForEachRow {
		sql.WriteString(" FOR EACH ROW")
	}

	if stmt.When != nil {
		sql.WriteString(" WHEN ")
		sql.WriteString(stmt.When.String())
	}
}

// writeTriggerBody writes the trigger body (BEGIN...END block).
func writeTriggerBody(sql *strings.Builder, stmt *parser.CreateTriggerStmt) {
	sql.WriteString(" BEGIN")
	for _, bodyStmt := range stmt.Body {
		sql.WriteString(" ")
		sql.WriteString(bodyStmt.String())
		sql.WriteString(";")
	}
	sql.WriteString(" END")
}
