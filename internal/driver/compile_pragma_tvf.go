// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// isPragmaTVF checks if a SELECT references a pragma table-valued function.
// Pragma TVFs can appear with arguments (pragma_table_info('t'))
// or without (pragma_database_list).
func (s *Stmt) isPragmaTVF(stmt *parser.SelectStmt) bool {
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		return false
	}
	return isPragmaTVFName(stmt.From.Tables[0].TableName)
}

// isPragmaTVFName returns true if name is a known pragma TVF.
func isPragmaTVFName(name string) bool {
	switch strings.ToLower(name) {
	case "pragma_table_info", "pragma_index_list",
		"pragma_database_list", "pragma_foreign_key_list":
		return true
	}
	return false
}

// compileSelectWithPragmaTVF compiles SELECT from a pragma table-valued function.
func (s *Stmt) compileSelectWithPragmaTVF(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	ref := &stmt.From.Tables[0]
	pragmaName := strings.ToLower(ref.TableName)
	tableName := extractPragmaTVFArg(ref)

	switch pragmaName {
	case "pragma_table_info":
		return s.compilePragmaTVFTableInfo(vm, stmt, tableName)
	case "pragma_index_list":
		return s.compilePragmaTVFIndexList(vm, stmt, tableName)
	case "pragma_database_list":
		return s.compilePragmaTVFDatabaseList(vm, stmt)
	case "pragma_foreign_key_list":
		return s.compilePragmaTVFForeignKeyList(vm, stmt, tableName)
	default:
		return nil, fmt.Errorf("unknown pragma function: %s", pragmaName)
	}
}

// extractPragmaTVFArg extracts the table name argument from a pragma TVF call.
func extractPragmaTVFArg(ref *parser.TableOrSubquery) string {
	if len(ref.FuncArgs) == 0 {
		return ""
	}
	if lit, ok := ref.FuncArgs[0].(*parser.LiteralExpr); ok {
		return lit.Value
	}
	if ident, ok := ref.FuncArgs[0].(*parser.IdentExpr); ok {
		return ident.Name
	}
	return ""
}

// compilePragmaTVFTableInfo emits rows for pragma_table_info(tablename).
func (s *Stmt) compilePragmaTVFTableInfo(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string) (*vdbe.VDBE, error) {
	if tableName == "" {
		return nil, fmt.Errorf("pragma_table_info requires a table name")
	}

	table, exists := s.conn.schema.GetTable(tableName)
	if !exists {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	allCols := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
	rows := buildTableInfoRows(table)
	return s.emitPragmaTVFResult(vm, stmt, allCols, rows)
}

// buildTableInfoRows builds pragma_table_info rows from a table definition.
func buildTableInfoRows(table *schema.Table) [][]interface{} {
	rows := make([][]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		notnull := int64(0)
		if col.NotNull {
			notnull = 1
		}
		var dflt interface{}
		if col.Default != nil {
			dflt = fmt.Sprintf("%v", col.Default)
		}
		pk := int64(calculatePrimaryKeyIndex(col, table))
		rows[i] = []interface{}{int64(i), col.Name, col.Type, notnull, dflt, pk}
	}
	return rows
}

// compilePragmaTVFIndexList emits rows for pragma_index_list(tablename).
func (s *Stmt) compilePragmaTVFIndexList(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string) (*vdbe.VDBE, error) {
	if tableName == "" {
		return nil, fmt.Errorf("pragma_index_list requires a table name")
	}

	allCols := []string{"seq", "name", "unique", "origin", "partial"}
	indexes := s.conn.schema.GetTableIndexes(tableName)
	rows := buildIndexListRows(indexes)
	return s.emitPragmaTVFResult(vm, stmt, allCols, rows)
}

// buildIndexListRows builds pragma_index_list rows from indexes.
func buildIndexListRows(indexes []*schema.Index) [][]interface{} {
	rows := make([][]interface{}, len(indexes))
	for i, idx := range indexes {
		isUnique := int64(0)
		if idx.Unique {
			isUnique = 1
		}
		isPartial := int64(0)
		if idx.Partial {
			isPartial = 1
		}
		rows[i] = []interface{}{int64(i), idx.Name, isUnique, "c", isPartial}
	}
	return rows
}

// compilePragmaTVFDatabaseList emits rows for pragma_database_list.
func (s *Stmt) compilePragmaTVFDatabaseList(vm *vdbe.VDBE, stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
	allCols := []string{"seq", "name", "file"}
	databases := s.conn.dbRegistry.ListDatabasesOrdered()
	rows := make([][]interface{}, len(databases))
	for i, db := range databases {
		rows[i] = []interface{}{int64(i), db.Name, db.Path}
	}
	return s.emitPragmaTVFResult(vm, stmt, allCols, rows)
}

// compilePragmaTVFForeignKeyList emits rows for pragma_foreign_key_list(tablename).
func (s *Stmt) compilePragmaTVFForeignKeyList(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string) (*vdbe.VDBE, error) {
	if tableName == "" {
		return nil, fmt.Errorf("pragma_foreign_key_list requires a table name")
	}

	allCols := []string{"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
	var rows [][]interface{}
	if s.conn.fkManager != nil {
		rows = buildForeignKeyListRows(s.conn.fkManager.GetConstraints(tableName))
	}
	return s.emitPragmaTVFResult(vm, stmt, allCols, rows)
}

// buildForeignKeyListRows builds pragma_foreign_key_list rows.
func buildForeignKeyListRows(constraints []*constraint.ForeignKeyConstraint) [][]interface{} {
	var rows [][]interface{}
	for id, fk := range constraints {
		onUpdate := fkActionToString(fk.OnUpdate)
		onDelete := fkActionToString(fk.OnDelete)
		for seq, col := range fk.Columns {
			toCol := ""
			if seq < len(fk.RefColumns) {
				toCol = fk.RefColumns[seq]
			}
			rows = append(rows, []interface{}{
				int64(id), int64(seq), fk.RefTable, col, toCol,
				onUpdate, onDelete, "NONE",
			})
		}
	}
	return rows
}

// emitPragmaTVFResult emits VDBE bytecode for pragma TVF results with column selection and WHERE filtering.
func (s *Stmt) emitPragmaTVFResult(vm *vdbe.VDBE, stmt *parser.SelectStmt, allCols []string, rows [][]interface{}) (*vdbe.VDBE, error) {
	// Filter rows by WHERE clause first
	filtered := filterPragmaRows(rows, stmt.Where, allCols)

	// Handle COUNT(*) aggregate
	if isPragmaCountStar(stmt) {
		return emitPragmaCountResult(vm, filtered)
	}

	outCols, colIndices := resolvePragmaColumns(stmt.Columns, allCols)
	numCols := len(outCols)
	vm.AllocMemory(numCols + 10)
	vm.ResultCols = outCols

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range filtered {
		for outIdx, srcIdx := range colIndices {
			if srcIdx < 0 || srcIdx >= len(row) {
				vm.AddOp(vdbe.OpNull, 0, outIdx, 0)
				continue
			}
			emitPragmaValue(vm, row[srcIdx], outIdx)
		}
		vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// filterPragmaRows filters rows by WHERE clause.
func filterPragmaRows(rows [][]interface{}, where parser.Expression, allCols []string) [][]interface{} {
	if where == nil {
		return rows
	}
	var filtered [][]interface{}
	for _, row := range rows {
		if pragmaRowMatchesWhere(where, row, allCols) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// isPragmaCountStar checks if the SELECT is COUNT(*) from a pragma TVF.
func isPragmaCountStar(stmt *parser.SelectStmt) bool {
	if len(stmt.Columns) != 1 {
		return false
	}
	col := stmt.Columns[0]
	fnExpr, ok := col.Expr.(*parser.FunctionExpr)
	if !ok {
		return false
	}
	return fnExpr.Name == "COUNT" && fnExpr.Star
}

// emitPragmaCountResult emits a single COUNT(*) result row.
func emitPragmaCountResult(vm *vdbe.VDBE, rows [][]interface{}) (*vdbe.VDBE, error) {
	vm.AllocMemory(10)
	vm.ResultCols = []string{"COUNT(*)"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpInteger, len(rows), 0, 0)
	vm.AddOp(vdbe.OpResultRow, 0, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// resolvePragmaColumns maps SELECT columns to pragma output columns.
func resolvePragmaColumns(selectCols []parser.ResultColumn, allCols []string) ([]string, []int) {
	if len(selectCols) == 1 && selectCols[0].Star {
		indices := make([]int, len(allCols))
		for i := range indices {
			indices[i] = i
		}
		return allCols, indices
	}

	names := make([]string, 0, len(selectCols))
	indices := make([]int, 0, len(selectCols))
	for _, col := range selectCols {
		name := pragmaExtractColName(col)
		idx := findPragmaColIndex(name, allCols)
		if col.Alias != "" {
			names = append(names, col.Alias)
		} else {
			names = append(names, name)
		}
		indices = append(indices, idx)
	}
	return names, indices
}

// pragmaExtractColName extracts column name from a result column.
func pragmaExtractColName(col parser.ResultColumn) string {
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	if col.Expr != nil {
		return col.Expr.String()
	}
	return ""
}

// findPragmaColIndex finds the index of a column name in the pragma columns.
func findPragmaColIndex(name string, allCols []string) int {
	lower := strings.ToLower(name)
	for i, c := range allCols {
		if strings.ToLower(c) == lower {
			return i
		}
	}
	return -1
}

// emitPragmaValue emits VDBE instructions for a single pragma value.
func emitPragmaValue(vm *vdbe.VDBE, val interface{}, reg int) {
	switch v := val.(type) {
	case nil:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case int64:
		vm.AddOp(vdbe.OpInteger, int(v), reg, 0)
	case string:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, v)
	default:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, fmt.Sprintf("%v", v))
	}
}

// pragmaRowMatchesWhere evaluates a simple WHERE clause against a pragma row.
// Supports basic equality comparisons (column = 'value').
func pragmaRowMatchesWhere(where parser.Expression, row []interface{}, allCols []string) bool {
	if where == nil {
		return true
	}
	return evalPragmaWhere(where, row, allCols)
}

// evalPragmaWhere evaluates a WHERE expression for a pragma row.
func evalPragmaWhere(expr parser.Expression, row []interface{}, cols []string) bool {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return evalPragmaBinaryExpr(e, row, cols)
	default:
		return true // unknown expressions pass
	}
}

// evalPragmaBinaryExpr evaluates a binary expression for pragma WHERE filtering.
func evalPragmaBinaryExpr(e *parser.BinaryExpr, row []interface{}, cols []string) bool {
	switch e.Op {
	case parser.OpAnd:
		return evalPragmaWhere(e.Left, row, cols) && evalPragmaWhere(e.Right, row, cols)
	case parser.OpOr:
		return evalPragmaWhere(e.Left, row, cols) || evalPragmaWhere(e.Right, row, cols)
	case parser.OpEq:
		return evalPragmaEquality(e, row, cols)
	case parser.OpGe:
		return evalPragmaComparison(e, row, cols, false)
	case parser.OpGt:
		return evalPragmaComparison(e, row, cols, true)
	default:
		return true
	}
}

// evalPragmaEquality evaluates an equality check for pragma WHERE.
func evalPragmaEquality(e *parser.BinaryExpr, row []interface{}, cols []string) bool {
	lVal := resolvePragmaExprValue(e.Left, row, cols)
	rVal := resolvePragmaExprValue(e.Right, row, cols)
	if lVal == nil || rVal == nil {
		return false
	}
	// Try numeric comparison first
	if lInt, lok := toInt64Value(lVal); lok {
		if rInt, rok := toInt64Value(rVal); rok {
			return lInt == rInt
		}
	}
	return fmt.Sprintf("%v", lVal) == fmt.Sprintf("%v", rVal)
}

// evalPragmaComparison evaluates >= or > for pragma WHERE.
func evalPragmaComparison(e *parser.BinaryExpr, row []interface{}, cols []string, strict bool) bool {
	lVal := resolvePragmaExprValue(e.Left, row, cols)
	rVal := resolvePragmaExprValue(e.Right, row, cols)
	if lVal == nil || rVal == nil {
		return false
	}
	lInt, lok := toInt64Value(lVal)
	rInt, rok := toInt64Value(rVal)
	if lok && rok {
		if strict {
			return lInt > rInt
		}
		return lInt >= rInt
	}
	return true
}

// resolvePragmaExprValue resolves an expression to a value given a pragma row.
func resolvePragmaExprValue(expr parser.Expression, row []interface{}, cols []string) interface{} {
	switch e := expr.(type) {
	case *parser.IdentExpr:
		idx := findPragmaColIndex(e.Name, cols)
		if idx >= 0 && idx < len(row) {
			return row[idx]
		}
		return e.Name // return as string for comparison
	case *parser.LiteralExpr:
		// Try to parse as integer for numeric comparison
		if e.Type == parser.LiteralInteger {
			var n int64
			if _, err := fmt.Sscanf(e.Value, "%d", &n); err == nil {
				return n
			}
		}
		return e.Value
	default:
		return nil
	}
}
