// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// isVirtualTable checks whether a table is a virtual table.
func (s *Stmt) isVirtualTable(table *schema.Table) bool {
	return table != nil && table.IsVirtual && table.VirtualTable != nil
}

// getVirtualTable extracts the vtab.VirtualTable from a schema table.
func getVirtualTable(table *schema.Table) (vtab.VirtualTable, error) {
	vt, ok := table.VirtualTable.(vtab.VirtualTable)
	if !ok {
		return nil, fmt.Errorf("virtual table %s: invalid module instance", table.Name)
	}
	return vt, nil
}

// compileVTabSelect compiles a SELECT on a virtual table using the vtab cursor interface.
func (s *Stmt) compileVTabSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	vm.SetReadOnly(true)
	s.noCache = true // vtab rows are baked into bytecode at compile time

	vt, err := getVirtualTable(table)
	if err != nil {
		return nil, err
	}

	// Build IndexInfo from WHERE clause
	info, constraintValues := s.buildVTabIndexInfo(stmt.Where, table, args)
	if err := vt.BestIndex(info); err != nil {
		return nil, fmt.Errorf("virtual table %s: BestIndex failed: %w", table.Name, err)
	}

	// Collect argv for Filter based on BestIndex output
	argv := buildFilterArgv(info, constraintValues)

	cursor, err := openAndFilterVTabCursor(vt, info, argv, table.Name)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	// Resolve output columns
	vtabCols := vtabColumnNames(table)
	outCols, colIndices := resolveVTabColumns(stmt.Columns, vtabCols)

	// Collect all rows
	allIndices := fullColumnIndices(vtabCols)
	allRows, err := collectVTabRows(cursor, allIndices)
	if err != nil {
		return nil, fmt.Errorf("virtual table %s: scan failed: %w", table.Name, err)
	}
	rows := postProcessVTabRows(stmt, info, allRows, vtabCols, args, colIndices, allIndices, outCols)

	return emitVTabBytecode(vm, rows, outCols)
}

func openAndFilterVTabCursor(vt vtab.VirtualTable, info *vtab.IndexInfo, argv []interface{}, tableName string) (vtab.VirtualCursor, error) {
	cursor, err := vt.Open()
	if err != nil {
		return nil, fmt.Errorf("virtual table %s: Open failed: %w", tableName, err)
	}
	if err := cursor.Filter(info.IdxNum, info.IdxStr, argv); err != nil {
		_ = cursor.Close()
		return nil, fmt.Errorf("virtual table %s: Filter failed: %w", tableName, err)
	}
	return cursor, nil
}

func fullColumnIndices(cols []string) []int {
	indices := make([]int, len(cols))
	for i := range indices {
		indices[i] = i
	}
	return indices
}

func postProcessVTabRows(stmt *parser.SelectStmt, info *vtab.IndexInfo, allRows [][]interface{}, vtabCols []string,
	args []driver.NamedValue, colIndices, allIndices []int, outCols []string) [][]interface{} {

	if stmt.Where != nil {
		allRows = filterVTabRowsWhere(allRows, stmt.Where, vtabCols, args)
	}
	rows := projectVTabRows(allRows, colIndices, allIndices)
	if stmt.Distinct {
		rows = deduplicateVTabRows(rows)
	}
	if len(stmt.OrderBy) > 0 && !info.OrderByConsumed {
		sortVTabRows(rows, stmt.OrderBy, outCols)
	}
	return applyVTabLimit(rows, stmt)
}

// buildVTabIndexInfo builds an IndexInfo struct from a WHERE clause.
func (s *Stmt) buildVTabIndexInfo(where parser.Expression, table *schema.Table,
	args []driver.NamedValue) (*vtab.IndexInfo, []interface{}) {

	if where == nil {
		return vtab.NewIndexInfo(0), nil
	}

	constraints, values := extractVTabConstraints(where, table, args)
	info := vtab.NewIndexInfo(len(constraints))
	for i, c := range constraints {
		info.Constraints[i] = c
	}
	return info, values
}

// extractVTabConstraints extracts constraints from a WHERE expression.
func extractVTabConstraints(expr parser.Expression, table *schema.Table,
	args []driver.NamedValue) ([]vtab.IndexConstraint, []interface{}) {

	var constraints []vtab.IndexConstraint
	var values []interface{}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		if e.Op == parser.OpAnd {
			lc, lv := extractVTabConstraints(e.Left, table, args)
			rc, rv := extractVTabConstraints(e.Right, table, args)
			return append(lc, rc...), append(lv, rv...)
		}
		col, op, ok := classifyVTabConstraint(e, table)
		if ok {
			val := evalConstraintValue(e.Right, args)
			constraints = append(constraints, vtab.IndexConstraint{
				Column: col, Op: op, Usable: true,
			})
			values = append(values, val)
		}
	}
	return constraints, values
}

// classifyVTabConstraint maps a binary expression to a vtab constraint.
func classifyVTabConstraint(e *parser.BinaryExpr, table *schema.Table) (int, vtab.ConstraintOp, bool) {
	col := resolveVTabColumnIndex(e.Left, table)
	if col < 0 {
		return 0, 0, false
	}

	op, ok := binaryOpToConstraint(e.Op)
	return col, op, ok
}

var vtabConstraintOps = map[parser.BinaryOp]vtab.ConstraintOp{
	parser.OpEq:    vtab.ConstraintEQ,
	parser.OpGt:    vtab.ConstraintGT,
	parser.OpLe:    vtab.ConstraintLE,
	parser.OpLt:    vtab.ConstraintLT,
	parser.OpGe:    vtab.ConstraintGE,
	parser.OpNe:    vtab.ConstraintNE,
	parser.OpMatch: vtab.ConstraintMatch,
	parser.OpLike:  vtab.ConstraintLike,
	parser.OpGlob:  vtab.ConstraintGlob,
}

// binaryOpToConstraint maps parser BinaryOp to vtab ConstraintOp.
func binaryOpToConstraint(op parser.BinaryOp) (vtab.ConstraintOp, bool) {
	result, ok := vtabConstraintOps[op]
	return result, ok
}

// resolveVTabColumnIndex finds the column index for an expression.
func resolveVTabColumnIndex(expr parser.Expression, table *schema.Table) int {
	if e, ok := expr.(*parser.IdentExpr); ok {
		name := strings.ToLower(e.Name)
		for i, col := range table.Columns {
			if strings.ToLower(col.Name) == name {
				return i
			}
		}
		// FTS5 MATCH uses table name as left operand
		if strings.ToLower(e.Name) == strings.ToLower(table.Name) {
			return 0
		}
	}
	return -1
}

// evalConstraintValue evaluates the right-hand side of a constraint.
func evalConstraintValue(expr parser.Expression, args []driver.NamedValue) interface{} {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return evalLiteralToInterface(e)
	case *parser.VariableExpr:
		return evalConstraintVariableValue(e, args)
	case *parser.UnaryExpr:
		return evalNegatedConstraintValue(e)
	}
	return nil
}

func evalConstraintVariableValue(expr *parser.VariableExpr, args []driver.NamedValue) interface{} {
	for _, a := range args {
		if expr.Name != "" && a.Name == expr.Name {
			return a.Value
		}
	}
	if len(args) > 0 {
		return args[0].Value
	}
	return nil
}

func evalNegatedConstraintValue(expr *parser.UnaryExpr) interface{} {
	if expr.Op != parser.OpNeg {
		return nil
	}
	lit, ok := expr.Expr.(*parser.LiteralExpr)
	if !ok {
		return nil
	}
	return negateValue(evalLiteralToInterface(lit))
}

// evalLiteralToInterface converts a literal to an interface{} value.
func evalLiteralToInterface(lit *parser.LiteralExpr) interface{} {
	switch lit.Type {
	case parser.LiteralString:
		return lit.Value
	case parser.LiteralInteger:
		var n int64
		fmt.Sscanf(lit.Value, "%d", &n)
		return n
	case parser.LiteralFloat:
		var f float64
		fmt.Sscanf(lit.Value, "%f", &f)
		return f
	case parser.LiteralNull:
		return nil
	default:
		return lit.Value
	}
}

// negateValue negates a numeric value.
func negateValue(v interface{}) interface{} {
	switch n := v.(type) {
	case int64:
		return -n
	case float64:
		return -n
	}
	return v
}

// buildFilterArgv orders constraint values according to BestIndex ConstraintUsage.
func buildFilterArgv(info *vtab.IndexInfo, values []interface{}) []interface{} {
	maxIdx := 0
	for _, cu := range info.ConstraintUsage {
		if cu.ArgvIndex > maxIdx {
			maxIdx = cu.ArgvIndex
		}
	}
	if maxIdx == 0 {
		return nil
	}

	argv := make([]interface{}, maxIdx)
	for i, cu := range info.ConstraintUsage {
		if cu.ArgvIndex > 0 && i < len(values) {
			argv[cu.ArgvIndex-1] = values[i]
		}
	}
	return argv
}

// vtabColumnNames returns column names for a virtual table.
func vtabColumnNames(table *schema.Table) []string {
	if len(table.Columns) > 0 {
		names := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			names[i] = col.Name
		}
		return names
	}
	return table.ModuleArgs
}

// resolveVTabColumns maps SELECT columns to virtual table column indices.
// Unlike resolveTVFColumns, this also handles special "rowid" references
// (mapped to index -1) and skips unresolvable columns.
func resolveVTabColumns(selectCols []parser.ResultColumn, vtabCols []string) ([]string, []int) {
	if len(selectCols) == 1 && selectCols[0].Star {
		indices := make([]int, len(vtabCols))
		for i := range indices {
			indices[i] = i
		}
		return vtabCols, indices
	}

	names := make([]string, 0, len(selectCols))
	indices := make([]int, 0, len(selectCols))
	for _, col := range selectCols {
		colName := extractVTabColumnName(col)
		idx := findVTabColumnIndex(colName, vtabCols)
		if idx >= 0 {
			if col.Alias != "" {
				names = append(names, col.Alias)
			} else {
				names = append(names, vtabCols[idx])
			}
			indices = append(indices, idx)
		} else if strings.EqualFold(colName, "rowid") {
			names = append(names, "rowid")
			indices = append(indices, -1)
		}
	}
	return names, indices
}

// extractVTabColumnName gets the column name from a ResultColumn.
// Delegates to extractResultColName.
func extractVTabColumnName(col parser.ResultColumn) string {
	return extractResultColName(col)
}

// findVTabColumnIndex finds a column by name (case-insensitive).
// Delegates to findColIndexCI.
func findVTabColumnIndex(name string, cols []string) int {
	return findColIndexCI(name, cols)
}

// collectVTabRows reads all rows from a vtab cursor.
func collectVTabRows(cursor vtab.VirtualCursor, colIndices []int) ([][]interface{}, error) {
	var rows [][]interface{}
	for !cursor.EOF() {
		row := make([]interface{}, len(colIndices))
		for i, idx := range colIndices {
			if idx == -1 {
				rid, err := cursor.Rowid()
				if err != nil {
					return nil, err
				}
				row[i] = rid
			} else {
				val, err := cursor.Column(idx)
				if err != nil {
					return nil, err
				}
				row[i] = val
			}
		}
		rows = append(rows, row)
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// filterVTabRowsWhere applies WHERE filtering to collected rows.
// Delegates to the generic filterRowsBy helper.
func filterVTabRowsWhere(rows [][]interface{}, where parser.Expression,
	cols []string, args []driver.NamedValue) [][]interface{} {
	return filterRowsBy(rows, func(row []interface{}) bool {
		return matchesVTabWhere(where, row, cols, args)
	})
}

// projectVTabRows projects full rows to requested column indices.
func projectVTabRows(rows [][]interface{}, outIndices, allIndices []int) [][]interface{} {
	result := make([][]interface{}, len(rows))
	for i, row := range rows {
		projected := make([]interface{}, len(outIndices))
		for j, idx := range outIndices {
			if idx >= 0 && idx < len(row) {
				projected[j] = row[idx]
			}
		}
		result[i] = projected
	}
	return result
}

// deduplicateVTabRows removes duplicate rows.
// Delegates to the generic deduplicateRowsBy helper.
func deduplicateVTabRows(rows [][]interface{}) [][]interface{} {
	return deduplicateRowsBy(rows, interfaceRowKey)
}

// sortVTabRows sorts rows by ORDER BY clauses.
func sortVTabRows(rows [][]interface{}, orderBy []parser.OrderingTerm, colNames []string) {
	if len(rows) == 0 || len(orderBy) == 0 {
		return
	}

	type sortKey struct {
		idx        int
		desc       bool
		nullsFirst *bool
	}
	var keys []sortKey
	for _, ob := range orderBy {
		name := extractVTabOrderByName(ob)
		for i, cn := range colNames {
			if strings.EqualFold(cn, name) {
				keys = append(keys, sortKey{idx: i, desc: !ob.Asc, nullsFirst: ob.NullsFirst})
				break
			}
		}
	}
	if len(keys) == 0 {
		return
	}

	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		stableSortVTabRows(rows, k.idx, k.desc, k.nullsFirst)
	}
}

// extractVTabOrderByName gets the column name from an OrderingTerm.
func extractVTabOrderByName(ob parser.OrderingTerm) string {
	if e, ok := ob.Expr.(*parser.IdentExpr); ok {
		return e.Name
	}
	return ""
}

// stableSortVTabRows performs a stable insertion sort on one column.
func stableSortVTabRows(rows [][]interface{}, colIdx int, desc bool, nullsFirst *bool) {
	n := len(rows)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && compareVTabValues(rows[j-1][colIdx], rows[j][colIdx], desc, nullsFirst); j-- {
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}
}

// vtabShouldNullsFirst returns whether NULLs should sort first for a vtab column.
func vtabShouldNullsFirst(desc bool, nullsFirst *bool) bool {
	if nullsFirst != nil {
		return *nullsFirst
	}
	return !desc
}

// compareVTabValues returns true if a should come after b in sort order.
func compareVTabValues(a, b interface{}, desc bool, nullsFirst *bool) bool {
	// Handle NULLs with NULLS FIRST/LAST awareness
	aNull := a == nil
	bNull := b == nil
	if aNull || bNull {
		if aNull && bNull {
			return false
		}
		nf := vtabShouldNullsFirst(desc, nullsFirst)
		if aNull {
			return !nf // a is NULL; if nulls first, a should NOT come after b
		}
		return nf // b is NULL; if nulls first, a should come after b
	}
	cmp := compareInterfaces(a, b)
	if desc {
		return cmp < 0
	}
	return cmp > 0
}

// compareInterfaces compares two interface values.
func compareInterfaces(a, b interface{}) int {
	if cmp, ok := compareNilInterfaces(a, b); ok {
		return cmp
	}
	if cmp, ok := compareNumericInterfaces(a, b); ok {
		return cmp
	}
	return compareStringInterfaces(a, b)
}

func compareNilInterfaces(a, b interface{}) (int, bool) {
	switch {
	case a == nil && b == nil:
		return 0, true
	case a == nil:
		return -1, true
	case b == nil:
		return 1, true
	default:
		return 0, false
	}
}

func compareNumericInterfaces(a, b interface{}) (int, bool) {
	af, aOk := vtabToFloat64(a)
	bf, bOk := vtabToFloat64(b)
	if !aOk || !bOk {
		return 0, false
	}
	switch {
	case af < bf:
		return -1, true
	case af > bf:
		return 1, true
	default:
		return 0, true
	}
}

func compareStringInterfaces(a, b interface{}) int {
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	default:
		return 0
	}
}

// vtabToFloat64 converts a numeric interface value to float64.
func vtabToFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// applyVTabLimit applies LIMIT and OFFSET to rows.
func applyVTabLimit(rows [][]interface{}, stmt *parser.SelectStmt) [][]interface{} {
	if stmt.Limit == nil {
		return rows
	}

	offset := 0
	if stmt.Offset != nil {
		if lit, ok := stmt.Offset.(*parser.LiteralExpr); ok {
			fmt.Sscanf(lit.Value, "%d", &offset)
		}
	}

	if offset >= len(rows) {
		return nil
	}
	rows = rows[offset:]

	if lit, ok := stmt.Limit.(*parser.LiteralExpr); ok {
		var limit int
		fmt.Sscanf(lit.Value, "%d", &limit)
		if limit < len(rows) {
			rows = rows[:limit]
		}
	}
	return rows
}

// emitVTabBytecode generates VDBE bytecode for pre-computed vtab rows.
// Delegates to emitInterfaceRows.
func emitVTabBytecode(vm *vdbe.VDBE, rows [][]interface{}, colNames []string) (*vdbe.VDBE, error) {
	return emitInterfaceRows(vm, rows, colNames)
}

// emitInterfaceValue emits a Go interface{} value into a VDBE register.
func emitInterfaceValue(vm *vdbe.VDBE, val interface{}, reg int) {
	switch v := val.(type) {
	case nil:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case int64:
		emitIntValue(vm, v, reg)
	case int:
		emitIntValue(vm, int64(v), reg)
	case float64:
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, v)
	case string:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, v)
	case []byte:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, string(v))
	default:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, fmt.Sprintf("%v", v))
	}
}

// compileVTabInsert compiles an INSERT on a virtual table.
func (s *Stmt) compileVTabInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt,
	table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	vm.SetReadOnly(false)
	s.noCache = true
	s.invalidateStmtCache() // vtab DML changes data; stale SELECT VDBEs must be evicted

	vt, err := getVirtualTable(table)
	if err != nil {
		return nil, err
	}

	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires VALUES clause")
	}

	vtabCols := vtabColumnNames(table)
	for _, valueRow := range stmt.Values {
		argv, err := buildVTabInsertArgv(valueRow, stmt.Columns, vtabCols, table.Module, args)
		if err != nil {
			return nil, err
		}
		if _, err := vt.Update(len(argv), argv); err != nil {
			return nil, fmt.Errorf("virtual table %s INSERT failed: %w", table.Name, err)
		}
	}

	vm.AllocMemory(2)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// buildVTabInsertArgv builds the argv array for a vtab Update(INSERT) call.
// For modules like rtree where the first column is the rowid, it goes into argv[1]
// and the remaining columns go into argv[2:]. For other modules (fts5),
// argv[1] is nil (auto rowid) and all columns go into argv[2:].
func buildVTabInsertArgv(valueRow []parser.Expression, insertCols []string,
	vtabCols []string, module string, args []driver.NamedValue) ([]interface{}, error) {

	// Evaluate all column values
	numCols := len(vtabCols)
	values := make([]interface{}, numCols)
	if len(insertCols) > 0 {
		for i, colName := range insertCols {
			if i >= len(valueRow) {
				break
			}
			idx := findVTabColumnIndex(colName, vtabCols)
			if idx >= 0 {
				values[idx] = evalConstraintValue(valueRow[i], args)
			}
		}
	} else {
		for i, expr := range valueRow {
			if i >= numCols {
				break
			}
			values[i] = evalConstraintValue(expr, args)
		}
	}

	// R-Tree: first column is the rowid, remaining are coordinates
	if strings.ToLower(module) == "rtree" {
		coordCols := numCols - 1
		argv := make([]interface{}, coordCols+2)
		argv[1] = values[0] // id goes into argv[1]
		copy(argv[2:], values[1:])
		return argv, nil
	}

	// Default (FTS5, etc.): all columns go into argv[2:]
	argv := make([]interface{}, numCols+2)
	copy(argv[2:], values)
	return argv, nil
}

// compileVTabUpdate compiles an UPDATE on a virtual table.
func (s *Stmt) compileVTabUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt,
	table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	vm.SetReadOnly(false)
	s.noCache = true
	s.invalidateStmtCache()

	vt, err := getVirtualTable(table)
	if err != nil {
		return nil, err
	}

	vtabCols := vtabColumnNames(table)
	rows, err := scanVTabRows(vt, stmt.Where, vtabCols, args)
	if err != nil {
		return nil, err
	}

	for _, u := range rows {
		argv := buildVTabUpdateArgv(u.rowid, u.currentRow, stmt.Sets, vtabCols, args)
		if _, err := vt.Update(len(argv), argv); err != nil {
			return nil, fmt.Errorf("virtual table %s UPDATE failed: %w", table.Name, err)
		}
	}

	vm.AllocMemory(2)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

type vtabMatchedRow struct {
	rowid      int64
	currentRow []interface{}
}

func scanVTabRows(vt vtab.VirtualTable, where parser.Expression,
	vtabCols []string, args []driver.NamedValue) ([]vtabMatchedRow, error) {

	cursor, err := vt.Open()
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if err := cursor.Filter(0, "", nil); err != nil {
		return nil, err
	}

	return collectMatchedVTabRows(cursor, where, vtabCols, args)
}

func collectMatchedVTabRows(cursor vtab.VirtualCursor, where parser.Expression,
	vtabCols []string, args []driver.NamedValue) ([]vtabMatchedRow, error) {

	var rows []vtabMatchedRow
	for !cursor.EOF() {
		row, err := readMatchedVTabRow(cursor, vtabCols)
		if err != nil {
			return nil, err
		}
		if where != nil && !matchesVTabWhere(where, row.currentRow, vtabCols, args) {
			if err := cursor.Next(); err != nil {
				return nil, err
			}
			continue
		}
		rows = append(rows, row)
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func readMatchedVTabRow(cursor vtab.VirtualCursor, vtabCols []string) (vtabMatchedRow, error) {
	rowid, err := cursor.Rowid()
	if err != nil {
		return vtabMatchedRow{}, err
	}
	currentRow := make([]interface{}, len(vtabCols))
	for i := range vtabCols {
		currentRow[i], _ = cursor.Column(i)
	}
	return vtabMatchedRow{rowid: rowid, currentRow: currentRow}, nil
}

// buildVTabUpdateArgv builds argv for a vtab Update(UPDATE) call.
func buildVTabUpdateArgv(rowid int64, currentRow []interface{},
	assignments []parser.Assignment, vtabCols []string, args []driver.NamedValue) []interface{} {

	numCols := len(vtabCols)
	argv := make([]interface{}, numCols+2)
	argv[0] = rowid
	argv[1] = rowid

	copy(argv[2:], currentRow)

	for _, a := range assignments {
		idx := findVTabColumnIndex(a.Column, vtabCols)
		if idx >= 0 {
			argv[idx+2] = evalConstraintValue(a.Value, args)
		}
	}
	return argv
}

// compileVTabDelete compiles a DELETE on a virtual table.
func (s *Stmt) compileVTabDelete(vm *vdbe.VDBE, stmt *parser.DeleteStmt,
	table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	vm.SetReadOnly(false)
	s.noCache = true
	s.invalidateStmtCache()

	vt, err := getVirtualTable(table)
	if err != nil {
		return nil, err
	}

	vtabCols := vtabColumnNames(table)
	rows, err := scanVTabRows(vt, stmt.Where, vtabCols, args)
	if err != nil {
		return nil, err
	}

	for _, r := range rows {
		if _, err := vt.Update(1, []interface{}{r.rowid}); err != nil {
			return nil, fmt.Errorf("virtual table %s DELETE failed: %w", table.Name, err)
		}
	}

	vm.AllocMemory(2)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// matchesVTabWhere evaluates a WHERE expression against a virtual table row.
func matchesVTabWhere(expr parser.Expression, row []interface{},
	cols []string, args []driver.NamedValue) bool {

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return evalVTabBinaryWhere(e, row, cols, args)
	case *parser.ParenExpr:
		return matchesVTabWhere(e.Expr, row, cols, args)
	}
	return true
}

// evalVTabBinaryWhere evaluates a binary expression for WHERE matching.
func evalVTabBinaryWhere(e *parser.BinaryExpr, row []interface{},
	cols []string, args []driver.NamedValue) bool {

	if e.Op == parser.OpAnd {
		return matchesVTabWhere(e.Left, row, cols, args) &&
			matchesVTabWhere(e.Right, row, cols, args)
	}
	if e.Op == parser.OpOr {
		return matchesVTabWhere(e.Left, row, cols, args) ||
			matchesVTabWhere(e.Right, row, cols, args)
	}

	leftVal := resolveVTabExprValue(e.Left, row, cols, args)
	rightVal := resolveVTabExprValue(e.Right, row, cols, args)
	return compareOpResult(e.Op, compareInterfaces(leftVal, rightVal))
}

// resolveVTabExprValue resolves an expression to a value given a row context.
func resolveVTabExprValue(expr parser.Expression, row []interface{},
	cols []string, args []driver.NamedValue) interface{} {

	switch e := expr.(type) {
	case *parser.IdentExpr:
		idx := findVTabColumnIndex(e.Name, cols)
		if idx >= 0 && idx < len(row) {
			return row[idx]
		}
	case *parser.LiteralExpr:
		return evalLiteralToInterface(e)
	case *parser.VariableExpr:
		return evalConstraintValue(e, args)
	}
	return nil
}
