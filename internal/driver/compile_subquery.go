// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
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

// compileSelectWithFromSubqueries compiles a SELECT with FROM subqueries.
func (s *Stmt) compileSelectWithFromSubqueries(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Special case: if we have a single FROM subquery and the outer query is simple
	// (just selecting columns, possibly with WHERE/ORDER BY), we can optimize by
	// compiling the subquery directly and handling column references
	if len(stmt.From.Tables) == 1 && stmt.From.Tables[0].Subquery != nil && len(stmt.From.Joins) == 0 {
		return s.compileSingleFromSubquery(vm, stmt, args)
	}

	// Strategy: compile each FROM subquery into a temp table, then compile main query
	// This is a more complex case with multiple subqueries or joins

	// Allocate cursors for all subqueries and main query
	numSubqueries := s.countFromSubqueries(stmt)
	vm.AllocCursors(numSubqueries + 1)
	vm.AllocMemory(50)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Compile each FROM subquery
	cursorIdx := 0
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			// Compile the subquery
			subVM, err := s.compileSelect(vdbe.New(), table.Subquery, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
			}

			// Create a temp table to hold results
			// In a full implementation, would:
			// 1. Execute subVM to get results
			// 2. Store results in temp table
			// 3. Use temp table in main query

			// For now, emit a comment
			commentOp := vm.AddOp(vdbe.OpNoop, 0, 0, 0)
			vm.Program[commentOp].Comment = fmt.Sprintf("FROM subquery compiled for cursor %d", cursorIdx)
			cursorIdx++

			// Merge the subquery program into main VM
			// This is simplified - real implementation would properly handle temp tables
			vm.Program = append(vm.Program, subVM.Program...)
		}
	}

	// Now compile the main query as normal, but referencing the temp tables
	// For this simplified implementation, we'll just compile it normally
	// A full implementation would track temp table schemas and use them

	// Simplified: compile as if no subquery (assumes flattening occurred)
	if len(stmt.From.Tables) > 0 && stmt.From.Tables[0].Subquery == nil {
		return s.compileSelect(vm, stmt, args)
	}

	// If all tables are subqueries, emit a placeholder result
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compileSingleFromSubquery compiles a SELECT with a single FROM subquery.
// This handles cases like: SELECT columns FROM (subquery) [WHERE ...] [ORDER BY ...]
func (s *Stmt) compileSingleFromSubquery(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subquery := stmt.From.Tables[0].Subquery

	// Special optimization: SELECT * with no WHERE clause
	if s.isSimpleSelectStar(stmt) {
		return s.compileSimpleSubquery(subquery, args)
	}

	// Complex case: specific columns or WHERE clause
	return s.compileComplexSubquery(stmt, subquery, args)
}

// isSimpleSelectStar checks if statement is SELECT * with no WHERE.
func (s *Stmt) isSimpleSelectStar(stmt *parser.SelectStmt) bool {
	return isSelectStar(stmt) && stmt.Where == nil
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

	// Map outer columns to subquery columns
	newColumns, err := s.mapSubqueryColumns(stmt, subquery, subVM.ResultCols)
	if err != nil {
		return nil, err
	}

	// Recompile with mapped columns
	modifiedSubquery := copySelectStmtShallow(subquery)
	modifiedSubquery.Columns = newColumns
	return s.compileSelect(s.newVDBE(), modifiedSubquery, args)
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
	for i, subCol := range subqueryColumns {
		if subCol == name {
			return subquery.Columns[i], nil
		}
	}
	return parser.ResultColumn{}, fmt.Errorf("column not found: %s", name)
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
func (s *Stmt) setupSubqueryCompiler(gen *expr.CodeGenerator) {
	gen.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		// Create a new VDBE for the subquery
		subVM := vdbe.New()
		// Compile the subquery SELECT statement
		return s.compileSelect(subVM, selectStmt, nil)
	})
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
