// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	sqlpkg "github.com/cyanitol/Public.Lib.Anthony/internal/sql"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// FROM Subquery Compilation
// ============================================================================

// hasFromSubqueries checks if a SELECT statement has subqueries in FROM clause.
func (s *Stmt) hasFromSubqueries(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}

	// Check base tables
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			return true
		}
	}

	// Check JOIN clauses
	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			return true
		}
	}

	return false
}

// hasFromTableSubqueries checks if any base FROM table (not JOIN) has a subquery.
func (s *Stmt) hasFromTableSubqueries(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			return true
		}
	}
	return false
}

// compileSelectWithFromSubqueries compiles a SELECT with FROM subqueries.
func (s *Stmt) compileSelectWithFromSubqueries(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Special case: single FROM subquery
	if s.isSingleFromSubquery(stmt) {
		return s.compileSingleFromSubquery(vm, stmt, args)
	}

	// Multiple subqueries or joins
	return s.compileMultipleFromSubqueries(vm, stmt, args)
}

// isSingleFromSubquery checks if statement has exactly one FROM subquery with no joins.
func (s *Stmt) isSingleFromSubquery(stmt *parser.SelectStmt) bool {
	return len(stmt.From.Tables) == 1 &&
		stmt.From.Tables[0].Subquery != nil &&
		len(stmt.From.Joins) == 0
}

// compileMultipleFromSubqueries handles multiple FROM subqueries or joins.
func (s *Stmt) compileMultipleFromSubqueries(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Allocate cursors for all subqueries and main query
	numSubqueries := s.countFromSubqueries(stmt)
	vm.AllocCursors(numSubqueries + 1)
	vm.AllocMemory(50)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Compile each FROM subquery
	if err := s.compileFromTableSubqueries(vm, stmt, args); err != nil {
		return nil, err
	}

	// Compile main query or emit placeholder
	return s.finalizeFromSubqueriesCompilation(vm, stmt, args)
}

// compileFromTableSubqueries compiles subqueries from FROM tables.
func (s *Stmt) compileFromTableSubqueries(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) error {
	cursorIdx := 0
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			if err := s.compileAndMergeSubquery(vm, table.Subquery, cursorIdx, args); err != nil {
				return err
			}
			cursorIdx++
		}
	}
	return nil
}

// compileAndMergeSubquery compiles a single subquery and merges it into the main VM.
func (s *Stmt) compileAndMergeSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, cursorIdx int, args []driver.NamedValue) error {
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile FROM subquery: %w", err)
	}

	// Emit a comment and merge the subquery program
	commentOp := vm.AddOp(vdbe.OpNoop, 0, 0, 0)
	vm.Program[commentOp].Comment = fmt.Sprintf("FROM subquery compiled for cursor %d", cursorIdx)
	vm.Program = append(vm.Program, subVM.Program...)
	return nil
}

// finalizeFromSubqueriesCompilation finishes compilation after processing subqueries.
func (s *Stmt) finalizeFromSubqueriesCompilation(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Simplified: compile as if no subquery (assumes flattening occurred)
	if s.hasNonSubqueryTable(stmt) {
		return s.compileSelect(vm, stmt, args)
	}

	// If all tables are subqueries, emit a placeholder result
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// hasNonSubqueryTable checks if any FROM table is not a subquery.
func (s *Stmt) hasNonSubqueryTable(stmt *parser.SelectStmt) bool {
	return len(stmt.From.Tables) > 0 && stmt.From.Tables[0].Subquery == nil
}

// compileSingleFromSubquery compiles a SELECT with a single FROM subquery.
// This handles cases like: SELECT columns FROM (subquery) [WHERE ...] [ORDER BY ...]
func (s *Stmt) compileSingleFromSubquery(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subquery := stmt.From.Tables[0].Subquery

	// Compound subqueries must be materialized first, then the outer query
	// runs over the materialized result set.
	if subquery.Compound != nil {
		return s.compileFromCompoundSubquery(vm, stmt, subquery, args)
	}

	// Special optimization: SELECT * with no WHERE clause
	if s.isSimpleSelectStar(stmt) {
		return s.compileSimpleSubquery(subquery, args)
	}

	// Try to flatten the subquery if possible
	flattened, canFlatten := s.tryFlattenFromSubquery(stmt, subquery)
	if canFlatten {
		return s.compileSelect(vm, flattened, args)
	}

	// Complex case: specific columns or WHERE clause
	return s.compileComplexSubquery(stmt, subquery, args)
}

// compileFromCompoundSubquery materializes a compound subquery and wraps the
// outer SELECT around it. This handles cases like:
//   - SELECT v FROM (SELECT v FROM t1 UNION SELECT v FROM t2)
//   - SELECT COUNT(*) FROM (SELECT v FROM t1 UNION ALL SELECT v FROM t2)
func (s *Stmt) compileFromCompoundSubquery(vm *vdbe.VDBE, outer *parser.SelectStmt, subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Compile and execute the compound subquery to get its rows.
	subVM := vdbe.New()
	subVM.Ctx = vm.Ctx
	compiled, err := s.compileSelect(subVM, subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile compound FROM subquery: %w", err)
	}

	numCols := len(compiled.ResultCols)
	rows, err := s.collectRows(compiled, numCols, "compound FROM subquery")
	if err != nil {
		return nil, err
	}

	// Build a synthetic SELECT over the materialised rows.
	return s.buildOuterOverMaterialized(vm, outer, rows, numCols, compiled.ResultCols)
}

// buildOuterOverMaterialized applies the outer query (aggregates, WHERE, ORDER
// BY, etc.) to pre-materialised rows. For simple SELECT * it just emits the
// rows. For aggregates it evaluates them in-process.
func (s *Stmt) buildOuterOverMaterialized(vm *vdbe.VDBE, outer *parser.SelectStmt, rows [][]interface{}, numCols int, colNames []string) (*vdbe.VDBE, error) {
	// Detect if outer is a simple pass-through (SELECT * or SELECT col list)
	if isSelectStar(outer) && outer.Where == nil {
		vm.ResultCols = colNames
		return emitCompoundResult(vm, rows, numCols)
	}

	// Check for aggregates in outer query
	if s.detectAggregates(outer) {
		return s.evalAggregateOverRows(vm, outer, rows, numCols, colNames)
	}

	// Non-aggregate column selection: filter and project
	vm.ResultCols = colNames
	return emitCompoundResult(vm, rows, numCols)
}

// evalAggregateOverRows evaluates aggregate functions over pre-materialised
// rows and emits the single-row result. Supports COUNT(*) and SUM(col).
func (s *Stmt) evalAggregateOverRows(vm *vdbe.VDBE, outer *parser.SelectStmt, rows [][]interface{}, numCols int, colNames []string) (*vdbe.VDBE, error) {
	resultRow := make([]interface{}, len(outer.Columns))
	for i, col := range outer.Columns {
		val, err := evalAggregate(col, rows, numCols, colNames)
		if err != nil {
			return nil, err
		}
		resultRow[i] = val
	}
	outColNames := make([]string, len(outer.Columns))
	for i, col := range outer.Columns {
		outColNames[i] = resultColumnName(col)
	}
	vm.ResultCols = outColNames
	return emitCompoundResult(vm, [][]interface{}{resultRow}, len(resultRow))
}

// evalAggregate evaluates a single aggregate column over materialised rows.
func evalAggregate(col parser.ResultColumn, rows [][]interface{}, numCols int, colNames []string) (interface{}, error) {
	fn, ok := col.Expr.(*parser.FunctionExpr)
	if !ok {
		return nil, fmt.Errorf("non-aggregate in aggregate context")
	}
	switch name := strings.ToUpper(fn.Name); name {
	case "COUNT":
		return int64(len(rows)), nil
	case "SUM":
		return sumColumn(fn, rows, colNames)
	default:
		return nil, fmt.Errorf("unsupported aggregate: %s", fn.Name)
	}
}

// sumColumn computes SUM for a named column over materialised rows.
func sumColumn(fn *parser.FunctionExpr, rows [][]interface{}, colNames []string) (interface{}, error) {
	idx, err := resolveAggColumnIndex(fn, colNames)
	if err != nil {
		return nil, err
	}
	var sum int64
	for _, row := range rows {
		if idx < len(row) && row[idx] != nil {
			switch v := row[idx].(type) {
			case int64:
				sum += v
			case float64:
				sum += int64(v)
			}
		}
	}
	return sum, nil
}

// resolveAggColumnIndex resolves the column index for a single-argument aggregate function.
func resolveAggColumnIndex(fn *parser.FunctionExpr, colNames []string) (int, error) {
	if len(fn.Args) == 0 {
		return -1, fmt.Errorf("%s requires an argument", fn.Name)
	}
	ident, ok := fn.Args[0].(*parser.IdentExpr)
	if !ok {
		return -1, fmt.Errorf("%s argument must be a column name", fn.Name)
	}
	for i, n := range colNames {
		if n == ident.Name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column not found: %s", ident.Name)
}

// resultColumnName returns a display name for a result column.
func resultColumnName(col parser.ResultColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	if fn, ok := col.Expr.(*parser.FunctionExpr); ok {
		return fn.Name + "(*)"
	}
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return "?"
}

// isSimpleSelectStar checks if statement is SELECT * with no WHERE.
func (s *Stmt) isSimpleSelectStar(stmt *parser.SelectStmt) bool {
	return isSelectStar(stmt) && stmt.Where == nil
}

// tryFlattenFromSubquery attempts to flatten a FROM subquery into the outer query.
// This is possible when the subquery is simple (no GROUP BY, no DISTINCT, etc.)
// Returns the flattened statement and true if flattening succeeded, or nil and false if not.
func (s *Stmt) tryFlattenFromSubquery(outer *parser.SelectStmt, subquery *parser.SelectStmt) (*parser.SelectStmt, bool) {
	// Can't flatten if subquery has features that prevent it
	if !s.canFlattenSubquery(subquery) {
		return nil, false
	}

	// Create flattened statement
	flattened := &parser.SelectStmt{
		Columns: outer.Columns,  // Keep outer columns (may include aggregates)
		From:    subquery.From,  // Use subquery's FROM clause
		Where:   subquery.Where, // Use subquery's WHERE (or merge with outer WHERE if needed)
		GroupBy: outer.GroupBy,  // Keep outer GROUP BY
		Having:  outer.Having,   // Keep outer HAVING
		OrderBy: outer.OrderBy,  // Keep outer ORDER BY
		Limit:   outer.Limit,    // Keep outer LIMIT
	}

	// If outer has a WHERE clause, we'd need to merge it (complex)
	// For now, only flatten if outer has no WHERE
	if outer.Where != nil {
		return nil, false
	}

	return flattened, true
}

// canFlattenSubquery checks if a subquery can be safely flattened.
func (s *Stmt) canFlattenSubquery(subquery *parser.SelectStmt) bool {
	// Can't flatten if subquery has:
	// - GROUP BY
	// - HAVING
	// - DISTINCT
	// - LIMIT
	// - OFFSET
	// - Aggregate functions
	// - UNION/INTERSECT/EXCEPT

	if len(subquery.GroupBy) > 0 {
		return false
	}
	if subquery.Having != nil {
		return false
	}
	if subquery.Distinct {
		return false
	}
	if subquery.Limit != nil {
		return false
	}
	if subquery.Compound != nil {
		return false
	}

	// Check if subquery has aggregates
	if s.detectAggregates(subquery) {
		return false
	}

	return true
}

// compileSimpleSubquery compiles a simple SELECT * subquery.
func (s *Stmt) compileSimpleSubquery(subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subVM, err := s.compileSelect(s.newVDBE(), subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
	}
	// TODO: Handle ORDER BY from outer query
	return subVM, nil
}

// compileComplexSubquery compiles a subquery with column selection or WHERE clause.
func (s *Stmt) compileComplexSubquery(stmt *parser.SelectStmt, subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Compile subquery to get its structure
	subVM, err := s.compileSelect(s.newVDBE(), subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
	}

	// If the outer query contains aggregates, evaluate them over the materialized subquery rows.
	if s.detectAggregates(stmt) {
		numCols := len(subVM.ResultCols)
		rows, err := s.collectRows(subVM, numCols, "FROM subquery")
		if err != nil {
			return nil, err
		}
		aggVM := s.newVDBE()
		return s.evalAggregateOverRows(aggVM, stmt, rows, numCols, subVM.ResultCols)
	}

	// When the outer query has WHERE, materialize the subquery
	// and apply outer clauses as a post-filter over the results.
	if stmt.Where != nil {
		return s.materializeAndFilter(stmt, subVM, args)
	}

	// Map outer columns to subquery columns
	newColumns, err := s.mapSubqueryColumns(stmt, subquery, subVM.ResultCols)
	if err != nil {
		return nil, err
	}

	// Recompile with mapped columns, propagating outer ORDER BY/LIMIT
	modifiedSubquery := copySelectStmtShallow(subquery)
	modifiedSubquery.Columns = newColumns
	if len(stmt.OrderBy) > 0 {
		modifiedSubquery.OrderBy = stmt.OrderBy
	}
	if stmt.Limit != nil {
		modifiedSubquery.Limit = stmt.Limit
	}
	return s.compileSelect(s.newVDBE(), modifiedSubquery, args)
}

// materializeAndFilter materializes subquery rows and applies outer WHERE/ORDER BY.
func (s *Stmt) materializeAndFilter(outer *parser.SelectStmt, subVM *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error) {
	numCols := len(subVM.ResultCols)
	rows, err := s.collectRows(subVM, numCols, "FROM subquery")
	if err != nil {
		return nil, err
	}

	// Filter rows using outer WHERE
	if outer.Where != nil {
		rows = s.filterMaterializedRows(rows, outer.Where, subVM.ResultCols)
	}

	// Project to outer columns
	projRows, projNames := s.projectRows(rows, outer, subVM.ResultCols)

	outVM := s.newVDBE()
	outVM.ResultCols = projNames
	return emitCompoundResult(outVM, projRows, len(projNames))
}

// filterMaterializedRows filters materialized rows by evaluating a WHERE expression.
func (s *Stmt) filterMaterializedRows(rows [][]interface{}, where parser.Expression, colNames []string) [][]interface{} {
	return filterRowsBy(rows, func(row []interface{}) bool {
		return s.evalWhereOnRow(where, row, colNames)
	})
}

// evalWhereOnRow evaluates a WHERE expression against a single materialized row.
func (s *Stmt) evalWhereOnRow(where parser.Expression, row []interface{}, colNames []string) bool {
	switch e := where.(type) {
	case *parser.BinaryExpr:
		return s.evalBinaryOnRow(e, row, colNames)
	default:
		return true // conservative: include row if expression type is unhandled
	}
}

// evalBinaryOnRow evaluates a binary expression against a materialized row.
func (s *Stmt) evalBinaryOnRow(e *parser.BinaryExpr, row []interface{}, colNames []string) bool {
	leftVal := s.evalScalarOnRow(e.Left, row, colNames)
	rightVal := s.evalScalarOnRow(e.Right, row, colNames)

	switch e.Op {
	case parser.OpEq:
		return fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal)
	case parser.OpNe:
		return fmt.Sprintf("%v", leftVal) != fmt.Sprintf("%v", rightVal)
	case parser.OpAnd:
		return isTruthy(leftVal) && isTruthy(rightVal)
	case parser.OpOr:
		return isTruthy(leftVal) || isTruthy(rightVal)
	default:
		return true
	}
}

// evalScalarOnRow resolves a scalar expression to a value from a materialized row.
func (s *Stmt) evalScalarOnRow(e parser.Expression, row []interface{}, colNames []string) interface{} {
	switch ex := e.(type) {
	case *parser.IdentExpr:
		for i, name := range colNames {
			if strings.EqualFold(name, ex.Name) && i < len(row) {
				return row[i]
			}
		}
		return nil
	case *parser.LiteralExpr:
		return ex.Value
	default:
		return nil
	}
}

// isTruthy returns whether a value is truthy in SQL sense.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "0"
	case bool:
		return val
	default:
		return true
	}
}

// projectRows selects columns from materialized rows based on the outer SELECT list.
func (s *Stmt) projectRows(rows [][]interface{}, outer *parser.SelectStmt, colNames []string) ([][]interface{}, []string) {
	indices, names := buildProjectionMap(outer.Columns, colNames)
	result := applyProjection(rows, indices)
	return result, names
}

func buildProjectionMap(columns []parser.ResultColumn, colNames []string) ([]int, []string) {
	indices := make([]int, 0, len(columns))
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		if col.Star {
			for i, name := range colNames {
				indices = append(indices, i)
				names = append(names, name)
			}
			continue
		}
		idx := resolveColumnIndex(col, colNames)
		if idx >= 0 {
			name := selectColName(col, len(names))
			indices = append(indices, idx)
			names = append(names, name)
		}
	}
	return indices, names
}

func resolveColumnIndex(col parser.ResultColumn, colNames []string) int {
	ident, ok := col.Expr.(*parser.IdentExpr)
	if !ok {
		return -1
	}
	for i, name := range colNames {
		if strings.EqualFold(name, ident.Name) {
			return i
		}
	}
	return -1
}

func applyProjection(rows [][]interface{}, indices []int) [][]interface{} {
	result := make([][]interface{}, len(rows))
	for i, row := range rows {
		projected := make([]interface{}, len(indices))
		for j, idx := range indices {
			if idx < len(row) {
				projected[j] = row[idx]
			}
		}
		result[i] = projected
	}
	return result
}

// mapSubqueryColumns maps outer query columns to subquery columns.
func (s *Stmt) mapSubqueryColumns(stmt *parser.SelectStmt, subquery *parser.SelectStmt, subqueryColumns []string) ([]parser.ResultColumn, error) {
	var newColumns []parser.ResultColumn

	for _, outerCol := range stmt.Columns {
		if outerCol.Star {
			// SELECT * - use all subquery columns
			return subquery.Columns, nil
		}

		if ident, ok := outerCol.Expr.(*parser.IdentExpr); ok {
			col, err := s.findSubqueryColumn(ident.Name, subquery, subqueryColumns)
			if err != nil {
				return nil, err
			}
			newColumns = append(newColumns, col)
		}
	}

	return newColumns, nil
}

// findSubqueryColumn finds a column in the subquery by name.
func (s *Stmt) findSubqueryColumn(name string, subquery *parser.SelectStmt, subqueryColumns []string) (parser.ResultColumn, error) {
	// Derive columns from compound's first SELECT when subquery.Columns is empty
	cols := subquery.Columns
	if len(cols) == 0 && subquery.Compound != nil {
		cols = compoundLeafColumns(subquery.Compound)
	}
	for i, subCol := range subqueryColumns {
		if subCol == name {
			if i < len(cols) {
				return cols[i], nil
			}
			// Column exists in result set but no AST node; synthesize one.
			return parser.ResultColumn{
				Expr: &parser.IdentExpr{Name: name},
			}, nil
		}
	}
	return parser.ResultColumn{}, fmt.Errorf("column not found: %s", name)
}

// compoundLeafColumns returns the Columns slice from the leftmost leaf of a
// CompoundSelect tree. This is used when the top-level SelectStmt wrapping a
// compound has an empty Columns slice.
func compoundLeafColumns(c *parser.CompoundSelect) []parser.ResultColumn {
	left := c.Left
	for left.Compound != nil {
		left = left.Compound.Left
	}
	return left.Columns
}

// copySelectStmtShallow makes a shallow copy of a SELECT statement.
func copySelectStmtShallow(stmt *parser.SelectStmt) *parser.SelectStmt {
	if stmt == nil {
		return nil
	}
	copy := *stmt
	return &copy
}

// isSelectStar checks if SELECT is SELECT *.
func isSelectStar(stmt *parser.SelectStmt) bool {
	if len(stmt.Columns) == 1 {
		col := stmt.Columns[0]
		if col.Star && col.Table == "" {
			return true
		}
	}
	return false
}

// countFromSubqueries counts the number of subqueries in FROM clause.
func (s *Stmt) countFromSubqueries(stmt *parser.SelectStmt) int {
	count := 0
	if stmt.From == nil {
		return 0
	}

	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			count++
		}
	}

	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			count++
		}
	}

	return count
}

// ============================================================================
// Expression Subquery Compilation (scalar, EXISTS, IN)
// ============================================================================

// setupSubqueryCompiler configures the CodeGenerator to handle subqueries.
// It provides a callback that compiles subquery SELECT statements.
// The subquery is compiled into a temporary VDBE, then its bytecode is
// adjusted to use cursor indices starting from the parent's next cursor.
// Register allocations are also adjusted to avoid conflicts with parent registers.
// Control flow opcodes (OpInit, OpHalt) are replaced with appropriate jumps.
//
// This follows SQLite's approach where pParse->nMem is shared across parent
// and subquery compilation to ensure register allocations don't conflict.
func (s *Stmt) setupSubqueryCompiler(gen *expr.CodeGenerator) {
	gen.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		// Get the parent VDBE and current register allocation state
		parentVM := gen.GetVDBE()
		cursorOffset := len(parentVM.Cursors)

		// CRITICAL: Get the parent's current register count to avoid conflicts
		// This is analogous to SQLite's pParse->nMem being shared across
		// parent and subquery compilation contexts
		registerOffset := parentVM.NumMem

		// Create a temporary VDBE for the subquery
		subVM := vdbe.New()

		// Copy context from parent so btree is available
		subVM.Ctx = parentVM.Ctx

		// Compile the subquery SELECT statement
		compiledVM, err := s.compileSelect(subVM, selectStmt, nil)
		if err != nil {
			return nil, err
		}

		// Strip OpInit (convert to Noop) and track OpHalt locations
		// OpHalt will be patched by the caller to jump to the end of the subquery
		stripSubqueryControlFlow(compiledVM)

		// Adjust cursor references in the compiled bytecode
		// by adding the cursor offset to all cursor operations
		adjustSubqueryCursors(compiledVM, cursorOffset)

		// CRITICAL: Adjust register references in the compiled bytecode
		// by adding the register offset to all register operations
		// This prevents register conflicts between parent and subquery
		adjustSubqueryRegisters(compiledVM, registerOffset)

		// Ensure parent has enough cursors allocated
		// Find max cursor used in subquery (after adjustment)
		maxCursor := findMaxCursor(compiledVM)
		if maxCursor >= 0 {
			parentVM.AllocCursors(maxCursor + 1)
		}

		// Ensure parent has enough registers allocated
		// Find max register used in subquery (after adjustment)
		maxRegister := findMaxRegister(compiledVM)
		if maxRegister >= 0 {
			parentVM.AllocMemory(maxRegister + 1)
		}

		return compiledVM, nil
	})

	// Also set up the executor for materialised subqueries (IN, EXISTS, scalar).
	s.setupSubqueryExecutor(gen)
}

// setupSubqueryExecutor provides a callback that compiles and executes a
// subquery SELECT to produce concrete result rows. This is used for IN, EXISTS,
// and scalar subqueries where materialisation is simpler and more robust than
// bytecode embedding. Returns an error for correlated subqueries that reference
// outer tables (the caller falls back to bytecode embedding).
func (s *Stmt) setupSubqueryExecutor(gen *expr.CodeGenerator) {
	gen.SetSubqueryExecutor(func(selectStmt *parser.SelectStmt) ([][]interface{}, error) {
		parentVM := gen.GetVDBE()
		subVM := vdbe.New()
		subVM.Ctx = parentVM.Ctx

		compiled, err := s.compileSelect(subVM, selectStmt, nil)
		if err != nil {
			// Compilation failure likely means a correlated subquery
			// referencing outer columns. Signal caller to fall back.
			return nil, err
		}

		numCols := len(compiled.ResultCols)
		if numCols == 0 {
			numCols = 1 // at least one column for scalar subqueries
		}
		return s.collectRows(compiled, numCols, "subquery materialise")
	})
}

// stripSubqueryControlFlow removes OpInit and converts OpHalt to OpNoop.
// OpHalt is converted to Noop so execution continues past the subquery.
// The caller (generateSubquery) will handle the control flow properly.
//
// For OpInit, we find its jump target (P2) which points past the initialization
// code to the actual program start. We adjust this to be relative to the start
// of the subquery by subtracting the OpInit's position.
func stripSubqueryControlFlow(vm *vdbe.VDBE) {
	// First pass: find OpInit's jump target
	startAddr := findOpInitTarget(vm)

	// Second pass: strip control flow opcodes
	for i := range vm.Program {
		stripOpcodeIfNeeded(vm, i, startAddr)
	}
}

// findOpInitTarget finds the jump target of OpInit instruction
func findOpInitTarget(vm *vdbe.VDBE) int {
	for i := range vm.Program {
		if vm.Program[i].Opcode == vdbe.OpInit {
			return vm.Program[i].P2
		}
	}
	return 0
}

// stripOpcodeIfNeeded converts control flow opcodes to Noop
func stripOpcodeIfNeeded(vm *vdbe.VDBE, i int, startAddr int) {
	switch vm.Program[i].Opcode {
	case vdbe.OpInit:
		vm.Program[i].Opcode = vdbe.OpNoop
		vm.Program[i].Comment = "subquery: stripped OpInit"
	case vdbe.OpHalt:
		vm.Program[i].Opcode = vdbe.OpNoop
		vm.Program[i].Comment = "subquery: stripped OpHalt"
	default:
		stripInitCodeIfNeeded(vm, i, startAddr)
	}
}

// stripInitCodeIfNeeded converts initialization code to Noop
func stripInitCodeIfNeeded(vm *vdbe.VDBE, i int, startAddr int) {
	if i < startAddr && startAddr > 0 {
		vm.Program[i].Opcode = vdbe.OpNoop
		vm.Program[i].Comment = "subquery: stripped init code"
	}
}

// ============================================================================
// Subquery Jump Target Adjustment Functions
// ============================================================================
//
// SCAFFOLDING: These functions support advanced subquery bytecode embedding.
// Currently unused because:
// 1. Current subquery implementation uses coroutines (OpInitCoroutine/OpYield)
// 2. Jump target adjustment needed for inline subquery expansion (optimization)
// 3. Will be used when implementing subquery flattening optimization
//
// ============================================================================

// adjustSubqueryJumpTargets adjusts all jump targets in the subquery bytecode.
// When subquery bytecode is embedded into a parent VDBE at address baseAddr,
// all absolute jump targets (in P2) must be adjusted by adding baseAddr.
// This ensures jumps land at the correct locations in the combined program.
//
// Jump targets are used in opcodes like:
// - OpGoto, OpGosub: P2 = absolute jump target
// - OpIf, OpIfNot, OpIfPos, OpIfNeg: P2 = jump if condition met
// - OpOnce: P2 = jump if already executed
// - OpRewind, OpNext, OpPrev, OpLast, OpFirst: P2 = jump on end/no rows
// - OpSeekGE, OpSeekGT, OpSeekLE, OpSeekLT: P2 = jump if not found
// - OpSeekRowid, OpNotExists: P2 = jump if not found
// - OpInitCoroutine: P2 = jump past coroutine body, P3 = entry point
// - OpSorterSort, OpSorterNext: P2 = jump when done
func adjustSubqueryJumpTargets(vm *vdbe.VDBE, baseAddr int) {
	if baseAddr == 0 {
		return
	}

	jumpOpcodes := buildJumpOpcodeMap()
	dualJumpOpcodes := buildDualJumpOpcodeMap()

	adjustJumpTargetsInProgram(vm, baseAddr, jumpOpcodes, dualJumpOpcodes)
}

// buildJumpOpcodeMap returns opcodes that use P2 as jump target
func buildJumpOpcodeMap() map[vdbe.Opcode]bool {
	return map[vdbe.Opcode]bool{
		vdbe.OpIf: true, vdbe.OpIfNot: true, vdbe.OpIfPos: true, vdbe.OpIfNotZero: true,
		vdbe.OpIfNullRow: true, vdbe.OpIsNull: true, vdbe.OpNotNull: true,
		vdbe.OpGoto: true, vdbe.OpGosub: true,
		vdbe.OpRewind: true, vdbe.OpNext: true, vdbe.OpPrev: true, vdbe.OpLast: true, vdbe.OpFirst: true,
		vdbe.OpSeekGE: true, vdbe.OpSeekGT: true, vdbe.OpSeekLE: true, vdbe.OpSeekLT: true,
		vdbe.OpSeekRowid: true, vdbe.OpNotExists: true,
		vdbe.OpSorterSort: true, vdbe.OpSorterNext: true, vdbe.OpOnce: true,
	}
}

// buildDualJumpOpcodeMap returns opcodes that use both P2 and P3 as jump targets
func buildDualJumpOpcodeMap() map[vdbe.Opcode]bool {
	return map[vdbe.Opcode]bool{
		vdbe.OpInitCoroutine: true,
	}
}

// adjustJumpTargetsInProgram adjusts jump targets in entire program
func adjustJumpTargetsInProgram(vm *vdbe.VDBE, baseAddr int, jumpOpcodes, dualJumpOpcodes map[vdbe.Opcode]bool) {
	for i := range vm.Program {
		op := vm.Program[i].Opcode

		// Adjust P2 for jump opcodes
		if jumpOpcodes[op] && vm.Program[i].P2 > 0 {
			vm.Program[i].P2 += baseAddr
		}

		// Adjust both P2 and P3 for dual-jump opcodes
		if dualJumpOpcodes[op] {
			if vm.Program[i].P2 > 0 {
				vm.Program[i].P2 += baseAddr
			}
			if vm.Program[i].P3 > 0 {
				vm.Program[i].P3 += baseAddr
			}
		}
	}
}

// adjustSubqueryCursors adds an offset to all cursor references in the bytecode.
// This allows subquery bytecode to use cursors that don't conflict with the parent.
func adjustSubqueryCursors(vm *vdbe.VDBE, offset int) {
	if offset == 0 {
		return
	}

	// First, find which cursors are opened within this sub-VM.
	// Cursors that appear in operations but are never opened are references
	// to already-open main VM cursors (e.g., CTE ephemeral tables) and must
	// NOT be offset.
	openedCursors := findOpenedCursors(vm)

	// Opcodes that use cursor in P1
	cursorP1Opcodes := map[vdbe.Opcode]bool{
		vdbe.OpOpenRead:      true,
		vdbe.OpOpenWrite:     true,
		vdbe.OpOpenEphemeral: true,
		vdbe.OpClose:         true,
		vdbe.OpRewind:        true,
		vdbe.OpNext:          true,
		vdbe.OpPrev:          true,
		vdbe.OpColumn:        true,
		vdbe.OpRowid:         true,
		vdbe.OpSeekGE:        true,
		vdbe.OpSeekGT:        true,
		vdbe.OpSeekLE:        true,
		vdbe.OpSeekLT:        true,
		vdbe.OpSeekRowid:     true,
		vdbe.OpNotExists:     true,
		vdbe.OpInsert:        true,
		vdbe.OpDelete:        true,
		vdbe.OpSorterOpen:    true,
		vdbe.OpSorterInsert:  true,
		vdbe.OpSorterSort:    true,
		vdbe.OpSorterNext:    true,
		vdbe.OpSorterData:    true,
		vdbe.OpSorterClose:   true,
	}

	for i := range vm.Program {
		if cursorP1Opcodes[vm.Program[i].Opcode] {
			if openedCursors[vm.Program[i].P1] {
				vm.Program[i].P1 += offset
			}
		}
	}
}

// findOpenedCursors returns the set of cursor IDs that are explicitly opened
// within the bytecode via OpenRead, OpenWrite, or OpenEphemeral.
func findOpenedCursors(vm *vdbe.VDBE) map[int]bool {
	opened := make(map[int]bool)
	for _, instr := range vm.Program {
		switch instr.Opcode {
		case vdbe.OpOpenRead, vdbe.OpOpenWrite, vdbe.OpOpenEphemeral:
			opened[instr.P1] = true
		}
	}
	return opened
}

// findMaxCursor finds the maximum cursor index used in the bytecode.
func findMaxCursor(vm *vdbe.VDBE) int {
	maxCursor := -1

	cursorP1Opcodes := map[vdbe.Opcode]bool{
		vdbe.OpOpenRead:      true,
		vdbe.OpOpenWrite:     true,
		vdbe.OpOpenEphemeral: true,
		vdbe.OpClose:         true,
		vdbe.OpRewind:        true,
		vdbe.OpNext:          true,
		vdbe.OpPrev:          true,
		vdbe.OpColumn:        true,
		vdbe.OpRowid:         true,
		vdbe.OpSorterOpen:    true,
	}

	for i := range vm.Program {
		if cursorP1Opcodes[vm.Program[i].Opcode] {
			if vm.Program[i].P1 > maxCursor {
				maxCursor = vm.Program[i].P1
			}
		}
	}

	return maxCursor
}

// adjustSubqueryRegisters adds an offset to all register references in the bytecode.
// This prevents register conflicts between parent and subquery execution contexts.
func adjustSubqueryRegisters(vm *vdbe.VDBE, offset int) {
	if offset == 0 {
		return
	}

	for i := range vm.Program {
		op := vm.Program[i].Opcode

		// Use the table-driven register adjuster if available
		if adjuster, ok := opcodeRegisterAdjusters[op]; ok {
			adjuster(vm.Program[i], offset)
		}
	}
}

// findMaxRegister finds the maximum register index used in the bytecode.
func findMaxRegister(vm *vdbe.VDBE) int {
	maxReg := -1

	for i := range vm.Program {
		op := vm.Program[i].Opcode

		// Use the table-driven register extractor if available
		if extractor, ok := opcodeRegisterExtractors[op]; ok {
			regs := extractor(vm.Program[i])
			updateMaxFromRegs(&maxReg, regs)
		}
	}

	return maxReg
}

// compileScalarSubquery compiles a scalar subquery (returns single value).
func (s *Stmt) compileScalarSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, targetReg int, args []driver.NamedValue) error {
	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile scalar subquery: %w", err)
	}

	// Emit code to execute subquery and store result in targetReg
	// In a full implementation, would:
	// 1. Open a pseudo-cursor for the subquery
	// 2. Execute the subquery
	// 3. Fetch the first (and only) row
	// 4. Store the value in targetReg
	// 5. Verify no more rows (scalar must return 1 row)

	// For now, merge the subquery program and add a comment
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("Scalar subquery -> reg %d", targetReg)

	return nil
}

// compileExistsSubquery compiles an EXISTS subquery.
func (s *Stmt) compileExistsSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, targetReg int, args []driver.NamedValue) error {
	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile EXISTS subquery: %w", err)
	}

	// Emit code to execute subquery and check if any rows exist
	// EXISTS returns 1 if subquery returns any rows, 0 otherwise

	// Strategy:
	// 1. Execute subquery
	// 2. Try to fetch first row
	// 3. If row exists, set targetReg = 1
	// 4. If no rows, set targetReg = 0

	// For now, merge the subquery program
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("EXISTS subquery -> reg %d", targetReg)

	// Set result register (simplified - assumes subquery ran)
	vm.AddOp(vdbe.OpInteger, 1, targetReg, 0)

	return nil
}

// compileInSubquery compiles an IN subquery.
//
// SCAFFOLDING: Alternative IN subquery implementation using ephemeral tables.
// Currently unused - the active implementation uses OpInSubquery opcode.
// This function will be used when implementing:
// 1. Materialized IN subquery optimization (for repeated evaluation)
// 2. Large IN list optimization using ephemeral B-tree index
func (s *Stmt) compileInSubquery(vm *vdbe.VDBE, leftExpr parser.Expression, subquery *parser.SelectStmt, targetReg int, gen *expr.CodeGenerator, args []driver.NamedValue) error {
	// Compile the left expression
	leftReg, err := gen.GenerateExpr(leftExpr)
	if err != nil {
		return fmt.Errorf("failed to compile IN left expression: %w", err)
	}

	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile IN subquery: %w", err)
	}

	// Strategy for IN subquery:
	// 1. Materialize subquery results into a temp table or ephemeral table
	// 2. Use OpFound to check if leftReg value exists in the temp table
	// 3. Set targetReg to 1 if found, 0 otherwise

	// For now, merge the subquery program
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("IN subquery for reg %d -> reg %d", leftReg, targetReg)

	// Simplified: assume value is found
	vm.AddOp(vdbe.OpInteger, 1, targetReg, 0)

	return nil
}

// materializeDerivedTables pre-executes subqueries in JOIN positions and creates
// real B-tree temp tables so the normal JOIN path can read from them.
func (s *Stmt) materializeDerivedTables(stmt *parser.SelectStmt, args []driver.NamedValue) error {
	if stmt.From == nil {
		return nil
	}
	for i := range stmt.From.Joins {
		if stmt.From.Joins[i].Table.Subquery == nil {
			continue
		}
		if err := s.materializeSingleDerived(&stmt.From.Joins[i].Table, args); err != nil {
			return err
		}
	}
	return nil
}

// materializeSingleDerived compiles+executes a subquery, creates a temp table,
// and rewrites the table reference.
func (s *Stmt) materializeSingleDerived(ref *parser.TableOrSubquery, args []driver.NamedValue) error {
	subVM, err := s.compileSelect(vdbe.New(), ref.Subquery, args)
	if err != nil {
		return fmt.Errorf("derived table: %w", err)
	}

	numCols := len(subVM.ResultCols)
	rows, err := s.collectRows(subVM, numCols, "derived table")
	if err != nil {
		return err
	}

	alias := ref.Alias
	if alias == "" {
		alias = "_derived_0"
	}
	tempName := "_dt_" + alias

	tempTable := s.buildDerivedTempTable(tempName, subVM.ResultCols)
	rootPage, err := s.insertDerivedRows(tempTable, rows)
	if err != nil {
		return err
	}
	tempTable.RootPage = rootPage

	s.conn.schema.AddTableDirect(tempTable)

	ref.Subquery = nil
	ref.TableName = tempName
	ref.Alias = alias
	return nil
}

// buildDerivedTempTable creates a schema.Table from result column names.
func (s *Stmt) buildDerivedTempTable(name string, colNames []string) *schema.Table {
	cols := make([]*schema.Column, len(colNames))
	for i, cn := range colNames {
		cols[i] = &schema.Column{Name: cn, Type: ""}
	}
	return &schema.Table{
		Name:    name,
		Columns: cols,
	}
}

// insertDerivedRows creates a B-tree table and inserts rows, returning the root page.
func (s *Stmt) insertDerivedRows(table *schema.Table, rows [][]interface{}) (uint32, error) {
	rootPage, err := s.conn.btree.CreateTable()
	if err != nil {
		return 0, fmt.Errorf("create derived table: %w", err)
	}

	cursor := btree.NewCursor(s.conn.btree, rootPage)

	for i, row := range rows {
		vals := goValuesToSQLValues(row)
		payload, err := sqlpkg.MakeRecord(vals)
		if err != nil {
			return 0, fmt.Errorf("derived table record %d: %w", i, err)
		}
		if err := cursor.Insert(int64(i+1), payload); err != nil {
			return 0, fmt.Errorf("derived table insert %d: %w", i, err)
		}
	}

	return rootPage, nil
}

// goValuesToSQLValues converts Go interface values to sql.Value slice.
func goValuesToSQLValues(row []interface{}) []sqlpkg.Value {
	vals := make([]sqlpkg.Value, len(row))
	for i, v := range row {
		vals[i] = goToSQLValue(v)
	}
	return vals
}

// goToSQLValue converts a single Go interface{} to sql.Value.
func goToSQLValue(v interface{}) sqlpkg.Value {
	switch val := v.(type) {
	case nil:
		return sqlpkg.NullValue()
	case int64:
		return sqlpkg.IntValue(val)
	case float64:
		return sqlpkg.FloatValue(val)
	case string:
		return sqlpkg.TextValue(val)
	case []byte:
		return sqlpkg.BlobValue(val)
	default:
		return sqlpkg.TextValue(fmt.Sprintf("%v", v))
	}
}
