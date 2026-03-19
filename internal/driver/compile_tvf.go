// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// hasTableValuedFunction checks if a SELECT has a table-valued function in FROM.
func (s *Stmt) hasTableValuedFunction(stmt *parser.SelectStmt) bool {
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		return false
	}
	ref := &stmt.From.Tables[0]
	if ref.FuncArgs == nil {
		return false
	}
	return s.lookupTVF(ref.TableName) != nil
}

// lookupTVF finds a table-valued function by name in the function registry.
func (s *Stmt) lookupTVF(name string) functions.TableValuedFunction {
	fn, ok := s.conn.funcReg.Lookup(strings.ToLower(name))
	if !ok {
		return nil
	}
	tvf, ok := fn.(functions.TableValuedFunction)
	if !ok {
		return nil
	}
	return tvf
}

// compileSelectWithTVF compiles a SELECT from a table-valued function.
func (s *Stmt) compileSelectWithTVF(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	ref := &stmt.From.Tables[0]
	tvf := s.lookupTVF(ref.TableName)
	if tvf == nil {
		return nil, fmt.Errorf("table-valued function not found: %s", ref.TableName)
	}

	// Evaluate function arguments from AST literals
	funcArgs, err := evalTVFArgs(ref.FuncArgs, args)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref.TableName, err)
	}

	// Execute the TVF to get all result rows
	rows, err := tvf.Open(funcArgs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref.TableName, err)
	}

	// Resolve output columns
	tvfCols := tvf.Columns()

	// Apply WHERE filter before projection
	if stmt.Where != nil {
		rows = filterTVFRows(rows, stmt.Where, tvfCols)
	}

	outCols, colIndices := resolveTVFColumns(stmt.Columns, tvfCols)

	// Project rows to output columns
	projected := projectTVFRows(rows, colIndices)

	// Apply DISTINCT before ORDER BY
	if stmt.Distinct {
		projected = deduplicateTVFRows(projected)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortTVFRows(projected, stmt.OrderBy, outCols)
	}

	return emitTVFProjectedBytecode(vm, projected, outCols)
}

// evalTVFArgs converts parser expressions to functions.Value arguments.
func evalTVFArgs(exprs []parser.Expression, args []driver.NamedValue) ([]functions.Value, error) {
	result := make([]functions.Value, len(exprs))
	for i, e := range exprs {
		val, err := evalLiteralExpr(e, args)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i+1, err)
		}
		result[i] = val
	}
	return result, nil
}

// evalLiteralExpr evaluates a parser expression to a functions.Value.
func evalLiteralExpr(e parser.Expression, args []driver.NamedValue) (functions.Value, error) {
	switch lit := e.(type) {
	case *parser.LiteralExpr:
		return literalToFuncValue(lit), nil
	case *parser.VariableExpr:
		return variableToFuncValue(lit, args)
	default:
		return nil, fmt.Errorf("unsupported expression type in TVF argument")
	}
}

// literalToFuncValue converts a parser LiteralExpr to a functions.Value.
func literalToFuncValue(lit *parser.LiteralExpr) functions.Value {
	switch lit.Type {
	case parser.LiteralString:
		return functions.NewTextValue(lit.Value)
	case parser.LiteralInteger:
		var n int64
		fmt.Sscanf(lit.Value, "%d", &n)
		return functions.NewIntValue(n)
	case parser.LiteralFloat:
		var f float64
		fmt.Sscanf(lit.Value, "%f", &f)
		return functions.NewFloatValue(f)
	default:
		return functions.NewNullValue()
	}
}

// variableToFuncValue resolves a variable/bind parameter to a functions.Value.
func variableToFuncValue(v *parser.VariableExpr, args []driver.NamedValue) (functions.Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no arguments provided for bind parameter")
	}
	// Match by name if available, else use first positional arg
	for _, a := range args {
		if v.Name != "" && a.Name == v.Name {
			return driverValueToFuncValue(a.Value), nil
		}
	}
	if len(args) > 0 {
		return driverValueToFuncValue(args[0].Value), nil
	}
	return nil, fmt.Errorf("bind parameter not found")
}

// driverValueToFuncValue converts a driver.Value to a functions.Value.
func driverValueToFuncValue(v interface{}) functions.Value {
	switch val := v.(type) {
	case int64:
		return functions.NewIntValue(val)
	case float64:
		return functions.NewFloatValue(val)
	case string:
		return functions.NewTextValue(val)
	case []byte:
		return functions.NewBlobValue(val)
	case nil:
		return functions.NewNullValue()
	default:
		return functions.NewTextValue(fmt.Sprintf("%v", val))
	}
}

// resolveTVFColumns maps SELECT columns to TVF output column indices.
// Returns the output column names and indices into the TVF row.
func resolveTVFColumns(selectCols []parser.ResultColumn, tvfCols []string) ([]string, []int) {
	// Handle SELECT *
	if len(selectCols) == 1 && selectCols[0].Star {
		indices := make([]int, len(tvfCols))
		for i := range indices {
			indices[i] = i
		}
		return tvfCols, indices
	}

	names := make([]string, 0, len(selectCols))
	indices := make([]int, 0, len(selectCols))
	for _, col := range selectCols {
		name := extractColName(col)
		idx := findTVFColIndex(name, tvfCols)
		names = append(names, colDisplayName(col, name))
		indices = append(indices, idx)
	}
	return names, indices
}

// extractColName extracts the column name from a result column expression.
func extractColName(col parser.ResultColumn) string {
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return ""
}

// colDisplayName returns the display name for a column.
func colDisplayName(col parser.ResultColumn, fallback string) string {
	if col.Alias != "" {
		return col.Alias
	}
	return fallback
}

// findTVFColIndex finds the index of a column name in TVF columns.
func findTVFColIndex(name string, tvfCols []string) int {
	lower := strings.ToLower(name)
	for i, c := range tvfCols {
		if strings.ToLower(c) == lower {
			return i
		}
	}
	return -1
}

// materializeTVFAsEphemeral executes a TVF, materializes its rows into an
// ephemeral table, and rewrites stmt.From so the aggregate pipeline can
// process the results. The caller should fall through to routeSpecializedSelect.
func (s *Stmt) materializeTVFAsEphemeral(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) error {
	ref := &stmt.From.Tables[0]
	tvf := s.lookupTVF(ref.TableName)
	if tvf == nil {
		return fmt.Errorf("table-valued function not found: %s", ref.TableName)
	}

	funcArgs, err := evalTVFArgs(ref.FuncArgs, args)
	if err != nil {
		return fmt.Errorf("%s: %w", ref.TableName, err)
	}

	rows, err := tvf.Open(funcArgs)
	if err != nil {
		return fmt.Errorf("%s: %w", ref.TableName, err)
	}

	tvfCols := tvf.Columns()
	tempTable, cursorNum := s.createTVFTempTable(ref.TableName, tvfCols, vm)
	s.conn.schema.AddTableDirect(tempTable)
	emitTVFMaterialize(vm, rows, tvfCols, cursorNum)
	rewriteFromForTVF(ref, tempTable.Name)
	return nil
}

// createTVFTempTable creates a temporary schema table and allocates a cursor
// for the ephemeral TVF materialization.
func (s *Stmt) createTVFTempTable(tvfName string, tvfCols []string, vm *vdbe.VDBE) (*schema.Table, int) {
	tableName := fmt.Sprintf("_tvf_%s", tvfName)
	columns := make([]*schema.Column, len(tvfCols))
	for i, name := range tvfCols {
		columns[i] = &schema.Column{Name: name, Type: "ANY"}
	}
	cursorNum := len(vm.Cursors)
	vm.AllocCursors(cursorNum + 1)
	table := &schema.Table{
		Name:     tableName,
		Columns:  columns,
		RootPage: uint32(cursorNum),
		Temp:     true,
	}
	return table, cursorNum
}

// emitTVFMaterialize emits bytecode to open an ephemeral table and insert all
// TVF result rows into it.
func emitTVFMaterialize(vm *vdbe.VDBE, rows [][]functions.Value, tvfCols []string, cursorNum int) {
	numCols := len(tvfCols)
	vm.AllocMemory(numCols + 20)
	vm.AddOp(vdbe.OpOpenEphemeral, cursorNum, numCols, 0)

	recordReg := numCols // register for MakeRecord output
	for _, row := range rows {
		for i := 0; i < numCols; i++ {
			if i < len(row) {
				emitFuncValue(vm, row[i], i)
			} else {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
		vm.AddOp(vdbe.OpMakeRecord, 0, numCols, recordReg)
		vm.AddOp(vdbe.OpInsert, cursorNum, recordReg, 0)
	}
}

// rewriteFromForTVF rewrites a table reference to point at the materialized
// temp table instead of the TVF call.
func rewriteFromForTVF(ref *parser.TableOrSubquery, tempName string) {
	ref.TableName = tempName
	ref.FuncArgs = nil
}

// projectTVFRows projects full TVF rows down to selected output columns.
func projectTVFRows(rows [][]functions.Value, colIndices []int) [][]functions.Value {
	projected := make([][]functions.Value, len(rows))
	for i, row := range rows {
		out := make([]functions.Value, len(colIndices))
		for j, srcIdx := range colIndices {
			if srcIdx >= 0 && srcIdx < len(row) {
				out[j] = row[srcIdx]
			} else {
				out[j] = functions.NewNullValue()
			}
		}
		projected[i] = out
	}
	return projected
}

// deduplicateTVFRows removes duplicate rows using string-key dedup.
func deduplicateTVFRows(rows [][]functions.Value) [][]functions.Value {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]functions.Value, 0, len(rows))
	for _, row := range rows {
		key := tvfRowKey(row)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, row)
	}
	return result
}

// tvfRowKey builds a string key for a row for deduplication.
func tvfRowKey(row []functions.Value) string {
	parts := make([]string, len(row))
	for i, v := range row {
		if v == nil || v.IsNull() {
			parts[i] = "\x00null"
		} else {
			parts[i] = fmt.Sprintf("%d:%s", v.Type(), v.AsString())
		}
	}
	return strings.Join(parts, "\x01")
}

// tvfSortKey describes one ORDER BY column for TVF row sorting.
type tvfSortKey struct {
	colIdx int
	desc   bool
}

// sortTVFRows sorts projected rows according to ORDER BY terms.
func sortTVFRows(rows [][]functions.Value, orderBy []parser.OrderingTerm, outCols []string) {
	keys := make([]tvfSortKey, 0, len(orderBy))
	for _, term := range orderBy {
		idx := resolveTVFOrderByCol(term.Expr, outCols)
		if idx < 0 {
			continue
		}
		keys = append(keys, tvfSortKey{colIdx: idx, desc: !term.Asc})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return tvfRowLess(rows[i], rows[j], keys)
	})
}

// tvfRowLess returns true if row a should sort before row b.
func tvfRowLess(a, b []functions.Value, keys []tvfSortKey) bool {
	for _, k := range keys {
		cmp := compareFuncValues(a[k.colIdx], b[k.colIdx])
		if cmp == 0 {
			continue
		}
		if k.desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

// resolveTVFOrderByCol maps an ORDER BY expression to an output column index.
func resolveTVFOrderByCol(e parser.Expression, outCols []string) int {
	if lit, ok := e.(*parser.LiteralExpr); ok {
		var n int
		if _, err := fmt.Sscanf(lit.Value, "%d", &n); err == nil && n >= 1 && n <= len(outCols) {
			return n - 1
		}
	}
	if ident, ok := e.(*parser.IdentExpr); ok {
		lower := strings.ToLower(ident.Name)
		for i, c := range outCols {
			if strings.ToLower(c) == lower {
				return i
			}
		}
	}
	return -1
}

// compareFuncValues compares two functions.Value using SQLite ordering:
// NULL < integers/floats < text < blob.
func compareFuncValues(a, b functions.Value) int {
	aNil := a == nil || a.IsNull()
	bNil := b == nil || b.IsNull()
	if aNil && bNil {
		return 0
	}
	if aNil {
		return -1
	}
	if bNil {
		return 1
	}
	// Compare by type affinity, then by value
	aType, bType := a.Type(), b.Type()
	if aType != bType {
		return int(aType) - int(bType)
	}
	return compareByType(a, b, aType)
}

// compareByType compares two non-null values of the same type.
func compareByType(a, b functions.Value, typ functions.ValueType) int {
	switch typ {
	case functions.TypeInteger:
		ai, bi := a.AsInt64(), b.AsInt64()
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	case functions.TypeFloat:
		af, bf := a.AsFloat64(), b.AsFloat64()
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	default:
		return strings.Compare(a.AsString(), b.AsString())
	}
}

// emitTVFProjectedBytecode generates VDBE bytecode for pre-projected TVF rows.
func emitTVFProjectedBytecode(vm *vdbe.VDBE, rows [][]functions.Value, colNames []string) (*vdbe.VDBE, error) {
	numCols := len(colNames)
	vm.AllocMemory(numCols + 10)
	vm.ResultCols = colNames

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range rows {
		for i := 0; i < numCols; i++ {
			if i < len(row) {
				emitFuncValue(vm, row[i], i)
			} else {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
		vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitTVFBytecode generates VDBE bytecode that outputs pre-computed TVF rows.
func emitTVFBytecode(vm *vdbe.VDBE, rows [][]functions.Value, colNames []string, colIndices []int) (*vdbe.VDBE, error) {
	numCols := len(colNames)
	vm.AllocMemory(numCols + 10)
	vm.ResultCols = colNames

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range rows {
		emitTVFRow(vm, row, colIndices, numCols)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitTVFRow emits VDBE instructions for a single TVF result row.
func emitTVFRow(vm *vdbe.VDBE, row []functions.Value, colIndices []int, numCols int) {
	for outIdx, srcIdx := range colIndices {
		if srcIdx < 0 || srcIdx >= len(row) {
			vm.AddOp(vdbe.OpNull, 0, outIdx, 0)
			continue
		}
		emitFuncValue(vm, row[srcIdx], outIdx)
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
}

// emitFuncValue emits a VDBE instruction to load a functions.Value into a register.
func emitFuncValue(vm *vdbe.VDBE, val functions.Value, reg int) {
	if val == nil || val.IsNull() {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}

	sv, ok := val.(*functions.SimpleValue)
	if !ok {
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, val.AsString())
		return
	}

	switch sv.Type() {
	case functions.TypeInteger:
		emitIntValue(vm, sv.AsInt64(), reg)
	case functions.TypeFloat:
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, sv.AsFloat64())
	case functions.TypeText:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, sv.AsString())
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// emitIntValue emits an integer value into a register.
// Uses OpInteger for small values, OpInt64 for large ones.
func emitIntValue(vm *vdbe.VDBE, n int64, reg int) {
	if n >= -2147483648 && n <= 2147483647 {
		vm.AddOp(vdbe.OpInteger, int(n), reg, 0)
	} else {
		vm.AddOpWithP4Int64(vdbe.OpInt64, 0, reg, 0, n)
	}
}

// filterTVFRows filters TVF rows by evaluating a WHERE expression.
func filterTVFRows(rows [][]functions.Value, where parser.Expression, tvfCols []string) [][]functions.Value {
	var result [][]functions.Value
	for _, row := range rows {
		if evalTVFWhere(where, row, tvfCols) {
			result = append(result, row)
		}
	}
	return result
}

// evalTVFWhere evaluates a WHERE expression against a single TVF row.
func evalTVFWhere(expr parser.Expression, row []functions.Value, cols []string) bool {
	switch e := expr.(type) {
	case *parser.UnaryExpr:
		return evalTVFUnary(e, row, cols)
	case *parser.BinaryExpr:
		return evalTVFBinary(e, row, cols)
	default:
		return true // conservative: include row for unhandled expressions
	}
}

// evalTVFUnary evaluates a unary expression (IS NULL, IS NOT NULL) against a TVF row.
func evalTVFUnary(e *parser.UnaryExpr, row []functions.Value, cols []string) bool {
	val := resolveTVFValue(e.Expr, row, cols)
	isNull := val == nil || val.IsNull()
	switch e.Op {
	case parser.OpIsNull:
		return isNull
	case parser.OpNotNull:
		return !isNull
	default:
		return true
	}
}

// evalTVFBinary evaluates a binary expression against a TVF row.
func evalTVFBinary(e *parser.BinaryExpr, row []functions.Value, cols []string) bool {
	switch e.Op {
	case parser.OpAnd:
		return evalTVFWhere(e.Left, row, cols) && evalTVFWhere(e.Right, row, cols)
	case parser.OpOr:
		return evalTVFWhere(e.Left, row, cols) || evalTVFWhere(e.Right, row, cols)
	default:
		left := resolveTVFValue(e.Left, row, cols)
		right := resolveTVFValue(e.Right, row, cols)
		return evalTVFComparison(e.Op, left, right)
	}
}

// evalTVFComparison evaluates a comparison between two TVF values.
func evalTVFComparison(op parser.BinaryOp, left, right functions.Value) bool {
	cmp := compareFuncValues(left, right)
	switch op {
	case parser.OpEq:
		return cmp == 0
	case parser.OpNe:
		return cmp != 0
	case parser.OpLt:
		return cmp < 0
	case parser.OpGt:
		return cmp > 0
	case parser.OpLe:
		return cmp <= 0
	case parser.OpGe:
		return cmp >= 0
	default:
		return true
	}
}

// resolveTVFValue resolves an expression to a functions.Value from a TVF row.
func resolveTVFValue(expr parser.Expression, row []functions.Value, cols []string) functions.Value {
	switch e := expr.(type) {
	case *parser.IdentExpr:
		idx := findTVFColIndex(e.Name, cols)
		if idx >= 0 && idx < len(row) {
			return row[idx]
		}
		return functions.NewNullValue()
	case *parser.LiteralExpr:
		return literalToFuncValue(e)
	default:
		return functions.NewNullValue()
	}
}

// hasCorrelatedTVF detects FROM table, tvf(column_ref) pattern.
func (s *Stmt) hasCorrelatedTVF(stmt *parser.SelectStmt) bool {
	if stmt.From == nil || len(stmt.From.Tables) < 2 {
		return false
	}
	for i := 1; i < len(stmt.From.Tables); i++ {
		ref := &stmt.From.Tables[i]
		if ref.FuncArgs != nil && s.lookupTVF(ref.TableName) != nil {
			if s.hasColumnRefArg(ref.FuncArgs) {
				return true
			}
		}
	}
	return false
}

// hasColumnRefArg checks if any argument contains a column reference.
func (s *Stmt) hasColumnRefArg(args []parser.Expression) bool {
	for _, arg := range args {
		if _, ok := arg.(*parser.IdentExpr); ok {
			return true
		}
		if ident, ok := arg.(*parser.IdentExpr); ok && ident.Table != "" {
			return true
		}
	}
	return false
}

// compileCorrelatedTVFJoin compiles a cross-join between a table and a correlated TVF
// by pre-executing the join at compile time.
func (s *Stmt) compileCorrelatedTVFJoin(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	outerRef := &stmt.From.Tables[0]
	tvfRef := s.findCorrelatedTVFRef(stmt)
	if tvfRef == nil {
		return nil, fmt.Errorf("correlated TVF not found")
	}

	tvf := s.lookupTVF(tvfRef.TableName)
	tvfCols := tvf.Columns()

	outerTable, _, _, ok := s.conn.dbRegistry.ResolveTable(outerRef.Schema, outerRef.TableName)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", outerRef.TableName)
	}

	outerAlias := outerRef.TableName
	if outerRef.Alias != "" {
		outerAlias = outerRef.Alias
	}
	tvfAlias := tvfRef.TableName
	if tvfRef.Alias != "" {
		tvfAlias = tvfRef.Alias
	}

	// Read all rows from the outer table
	outerRows, err := s.readAllTableRows(outerTable)
	if err != nil {
		return nil, err
	}

	// For each outer row, evaluate TVF args and call TVF
	joined := s.crossJoinWithTVF(outerRows, outerTable, tvfRef, tvf, args)

	// Build flat joined rows with full column names for filtering/aggregation
	flatCols := buildFlatColNames(outerTable, tvfCols)
	flatRows := flattenCorrelatedRows(joined, outerTable, tvfCols)

	// Apply WHERE filter on full joined data
	if stmt.Where != nil {
		flatRows = filterTVFRows(flatRows, stmt.Where, flatCols)
	}

	// Handle aggregates on full joined data
	if s.detectAggregates(stmt) {
		outCols := s.resolveCorrelatedOutputCols(stmt.Columns, outerTable, tvfCols, outerAlias, tvfAlias)
		return s.emitCorrelatedAggregate(vm, flatRows, stmt, outCols, flatCols)
	}

	// Resolve output columns and project for non-aggregate queries
	outCols := s.resolveCorrelatedOutputCols(stmt.Columns, outerTable, tvfCols, outerAlias, tvfAlias)
	projected := projectFlatRows(flatRows, stmt.Columns, outerTable, tvfCols, flatCols)

	// Apply DISTINCT
	if stmt.Distinct {
		projected = deduplicateTVFRows(projected)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortTVFRows(projected, stmt.OrderBy, outCols)
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		projected = applyTVFLimit(projected, stmt.Limit)
	}

	return emitTVFProjectedBytecode(vm, projected, outCols)
}

// findCorrelatedTVFRef finds the TVF table reference in the FROM clause.
func (s *Stmt) findCorrelatedTVFRef(stmt *parser.SelectStmt) *parser.TableOrSubquery {
	for i := 1; i < len(stmt.From.Tables); i++ {
		ref := &stmt.From.Tables[i]
		if ref.FuncArgs != nil && s.lookupTVF(ref.TableName) != nil {
			return ref
		}
	}
	return nil
}

// readAllTableRows reads all rows from a table using a fresh VM.
// Handles INTEGER PRIMARY KEY columns correctly by using OpRowid.
func (s *Stmt) readAllTableRows(table *schema.Table) ([][]interface{}, error) {
	readVM := vdbe.New()
	numCols := len(table.Columns)
	readVM.AllocMemory(numCols + 5)
	readVM.AllocCursors(1)
	readVM.AddOp(vdbe.OpInit, 0, 0, 0)
	readVM.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), numCols)
	rewindAddr := readVM.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Find IPK column (if any) — its value is the rowid, not in the record
	ipkIdx := -1
	for i, col := range table.Columns {
		if col.IsIntegerPrimaryKey() {
			ipkIdx = i
			break
		}
	}

	recordCol := 0
	for i := 0; i < numCols; i++ {
		if i == ipkIdx {
			readVM.AddOp(vdbe.OpRowid, 0, i, 0)
		} else {
			readVM.AddOp(vdbe.OpColumn, 0, recordCol, i)
			recordCol++
		}
	}

	readVM.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	readVM.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	readVM.Program[rewindAddr].P2 = readVM.NumOps()
	readVM.AddOp(vdbe.OpHalt, 0, 0, 0)
	readVM.Program[0].P2 = 1

	readVM.Ctx = &vdbe.VDBEContext{
		Btree:  s.conn.btree,
		Schema: s.conn.schema,
	}

	return s.collectRows(readVM, numCols, "outer table read")
}

// crossJoinWithTVF evaluates the TVF for each outer row and cross-joins.
// Returns []struct{outerRow, tvfRow}.
func (s *Stmt) crossJoinWithTVF(outerRows [][]interface{}, outerTable *schema.Table,
	tvfRef *parser.TableOrSubquery, tvf functions.TableValuedFunction,
	args []driver.NamedValue) []correlatedRow {

	var result []correlatedRow
	for _, outerRow := range outerRows {
		tvfArgs := s.evalCorrelatedArgs(tvfRef.FuncArgs, outerRow, outerTable, args)
		tvfRows, err := tvf.Open(tvfArgs)
		if err != nil {
			continue
		}
		for _, tvfRow := range tvfRows {
			result = append(result, correlatedRow{outer: outerRow, tvf: tvfRow})
		}
	}
	return result
}

// correlatedRow holds one combined row from the correlated TVF join.
type correlatedRow struct {
	outer []interface{}
	tvf   []functions.Value
}

// buildFlatColNames builds column names for the flattened joined row: outer cols + TVF cols.
func buildFlatColNames(outerTable *schema.Table, tvfCols []string) []string {
	cols := make([]string, 0, len(outerTable.Columns)+len(tvfCols))
	for _, c := range outerTable.Columns {
		cols = append(cols, c.Name)
	}
	cols = append(cols, tvfCols...)
	return cols
}

// flattenCorrelatedRows converts correlatedRow pairs into flat value rows.
func flattenCorrelatedRows(rows []correlatedRow, outerTable *schema.Table, tvfCols []string) [][]functions.Value {
	result := make([][]functions.Value, len(rows))
	for i, r := range rows {
		flat := make([]functions.Value, 0, len(outerTable.Columns)+len(tvfCols))
		for _, v := range r.outer {
			flat = append(flat, goToFuncValue(v))
		}
		flat = append(flat, r.tvf...)
		result[i] = flat
	}
	return result
}

// projectFlatRows projects flat joined rows to SELECT output columns.
func projectFlatRows(rows [][]functions.Value, cols []parser.ResultColumn,
	outerTable *schema.Table, tvfCols, flatCols []string) [][]functions.Value {

	result := make([][]functions.Value, len(rows))
	for i, row := range rows {
		projected := make([]functions.Value, len(cols))
		for j, col := range cols {
			projected[j] = resolveFlatCol(col, row, flatCols)
		}
		result[i] = projected
	}
	return result
}

// resolveFlatCol resolves a single SELECT column from a flat row.
func resolveFlatCol(col parser.ResultColumn, row []functions.Value, flatCols []string) functions.Value {
	switch e := col.Expr.(type) {
	case *parser.IdentExpr:
		lower := strings.ToLower(e.Name)
		for i, name := range flatCols {
			if strings.ToLower(name) == lower && i < len(row) {
				return row[i]
			}
		}
	}
	return functions.NewNullValue()
}

// evalCorrelatedArgs evaluates TVF arguments, resolving column references against the outer row.
func (s *Stmt) evalCorrelatedArgs(exprs []parser.Expression, outerRow []interface{},
	outerTable *schema.Table, args []driver.NamedValue) []functions.Value {

	result := make([]functions.Value, len(exprs))
	for i, e := range exprs {
		result[i] = s.evalCorrelatedArg(e, outerRow, outerTable, args)
	}
	return result
}

// evalCorrelatedArg evaluates a single TVF argument expression.
func (s *Stmt) evalCorrelatedArg(e parser.Expression, outerRow []interface{},
	outerTable *schema.Table, args []driver.NamedValue) functions.Value {

	switch expr := e.(type) {
	case *parser.LiteralExpr:
		return literalToFuncValue(expr)
	case *parser.IdentExpr:
		return s.resolveColumnToFuncValue(expr.Name, outerRow, outerTable)
	case *parser.VariableExpr:
		val, err := variableToFuncValue(expr, args)
		if err != nil {
			return functions.NewNullValue()
		}
		return val
	default:
		return functions.NewNullValue()
	}
}

// resolveColumnToFuncValue looks up a column value in the outer row.
func (s *Stmt) resolveColumnToFuncValue(colName string, outerRow []interface{}, table *schema.Table) functions.Value {
	lower := strings.ToLower(colName)
	for i, col := range table.Columns {
		if strings.ToLower(col.Name) == lower && i < len(outerRow) {
			return goToFuncValue(outerRow[i])
		}
	}
	return functions.NewNullValue()
}

// goToFuncValue converts a Go interface{} to functions.Value.
func goToFuncValue(v interface{}) functions.Value {
	switch val := v.(type) {
	case nil:
		return functions.NewNullValue()
	case int64:
		return functions.NewIntValue(val)
	case float64:
		return functions.NewFloatValue(val)
	case string:
		return functions.NewTextValue(val)
	case []byte:
		return functions.NewBlobValue(val)
	default:
		return functions.NewTextValue(fmt.Sprintf("%v", v))
	}
}

// resolveCorrelatedOutputCols resolves output column names for the correlated join.
func (s *Stmt) resolveCorrelatedOutputCols(cols []parser.ResultColumn,
	outerTable *schema.Table, tvfCols []string, outerAlias, tvfAlias string) []string {

	var out []string
	for _, col := range cols {
		out = append(out, resolveCorrelatedColName(col, outerTable, tvfCols, outerAlias, tvfAlias))
	}
	return out
}

// resolveCorrelatedColName resolves a single column name.
func resolveCorrelatedColName(col parser.ResultColumn, outerTable *schema.Table,
	tvfCols []string, outerAlias, tvfAlias string) string {

	if col.Alias != "" {
		return col.Alias
	}
	if col.Star {
		return "*"
	}
	switch e := col.Expr.(type) {
	case *parser.IdentExpr:
		return e.Name
	case *parser.FunctionExpr:
		return e.Name
	default:
		return "expr"
	}
}

// applyTVFLimit applies LIMIT to TVF rows.
func applyTVFLimit(rows [][]functions.Value, limitExpr parser.Expression) [][]functions.Value {
	limit := parseLimitExpr(limitExpr)
	if limit < 0 || limit >= len(rows) {
		return rows
	}
	return rows[:limit]
}

// emitCorrelatedAggregate handles aggregate queries over correlated TVF results.
// outCols are the SELECT output names; flatCols are the full joined column names.
func (s *Stmt) emitCorrelatedAggregate(vm *vdbe.VDBE, rows [][]functions.Value,
	stmt *parser.SelectStmt, outCols, flatCols []string) (*vdbe.VDBE, error) {

	// For GROUP BY, partition rows and compute aggregates
	if len(stmt.GroupBy) > 0 {
		return s.emitCorrelatedGroupByAggregate(vm, rows, stmt, outCols, flatCols)
	}

	// Simple aggregate: COUNT(*), COUNT(DISTINCT x), etc.
	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 5)
	vm.ResultCols = make([]string, numCols)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for i, col := range stmt.Columns {
		vm.ResultCols[i] = resolveAggColName(col)
		val := s.computeAggregate(col, rows, flatCols)
		emitFuncValue(vm, val, i)
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[0].P2 = 1
	return vm, nil
}

// resolveAggColName resolves the name for an aggregate column.
func resolveAggColName(col parser.ResultColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	if fn, ok := col.Expr.(*parser.FunctionExpr); ok {
		return fn.Name
	}
	return "expr"
}

// computeAggregate computes a single aggregate value over all rows.
func (s *Stmt) computeAggregate(col parser.ResultColumn, rows [][]functions.Value, colNames []string) functions.Value {
	fn, ok := col.Expr.(*parser.FunctionExpr)
	if !ok {
		return functions.NewNullValue()
	}

	name := strings.ToUpper(fn.Name)
	switch name {
	case "COUNT":
		if fn.Star {
			return functions.NewIntValue(int64(len(rows)))
		}
		if fn.Distinct {
			return s.computeCountDistinct(fn, rows, colNames)
		}
		return s.computeCount(fn, rows, colNames)
	default:
		return functions.NewNullValue()
	}
}

// computeCount computes COUNT(expr) over rows.
func (s *Stmt) computeCount(fn *parser.FunctionExpr, rows [][]functions.Value, colNames []string) functions.Value {
	colIdx := s.findTVFColIdx(fn.Args, colNames)
	count := int64(0)
	for _, row := range rows {
		if colIdx >= 0 && colIdx < len(row) && row[colIdx] != nil && row[colIdx].Type() != functions.TypeNull {
			count++
		}
	}
	return functions.NewIntValue(count)
}

// computeCountDistinct computes COUNT(DISTINCT expr) over rows.
func (s *Stmt) computeCountDistinct(fn *parser.FunctionExpr, rows [][]functions.Value, colNames []string) functions.Value {
	colIdx := s.findTVFColIdx(fn.Args, colNames)
	seen := make(map[string]bool)
	for _, row := range rows {
		if colIdx >= 0 && colIdx < len(row) && row[colIdx] != nil && row[colIdx].Type() != functions.TypeNull {
			key := row[colIdx].AsString()
			seen[key] = true
		}
	}
	return functions.NewIntValue(int64(len(seen)))
}

// findTVFColIdx finds the column index for a function argument.
func (s *Stmt) findTVFColIdx(fnArgs []parser.Expression, colNames []string) int {
	if len(fnArgs) == 0 {
		return -1
	}
	switch e := fnArgs[0].(type) {
	case *parser.IdentExpr:
		lower := strings.ToLower(e.Name)
		for i, name := range colNames {
			if strings.ToLower(name) == lower {
				return i
			}
		}
	}
	return -1
}

// emitCorrelatedGroupByAggregate handles GROUP BY over correlated TVF results.
func (s *Stmt) emitCorrelatedGroupByAggregate(vm *vdbe.VDBE, rows [][]functions.Value,
	stmt *parser.SelectStmt, outCols, flatCols []string) (*vdbe.VDBE, error) {

	// Build group keys using flat column names
	groupColIdxs := s.resolveGroupByColIdxs(stmt.GroupBy, flatCols)
	groups := s.groupCorrelatedRows(rows, groupColIdxs)

	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 5)
	outColNames := make([]string, numCols)
	for i, col := range stmt.Columns {
		outColNames[i] = resolveAggColName(col)
	}
	vm.ResultCols = outColNames
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Sort group keys for deterministic output
	sortedKeys := s.sortGroupKeys(groups)

	for _, key := range sortedKeys {
		groupRows := groups[key]
		for i, col := range stmt.Columns {
			val := s.evalGroupByCol(col, groupRows, flatCols)
			emitFuncValue(vm, val, i)
		}
		vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		// Already sorted by group keys; for complex ORDER BY we'd need sorting
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[0].P2 = 1
	return vm, nil
}

// resolveGroupByColIdxs maps GROUP BY expressions to column indices.
func (s *Stmt) resolveGroupByColIdxs(groupBy []parser.Expression, colNames []string) []int {
	var idxs []int
	for _, expr := range groupBy {
		switch e := expr.(type) {
		case *parser.IdentExpr:
			for i, name := range colNames {
				if strings.EqualFold(e.Name, name) {
					idxs = append(idxs, i)
					break
				}
			}
		}
	}
	return idxs
}

// groupCorrelatedRows groups rows by the specified column indices.
func (s *Stmt) groupCorrelatedRows(rows [][]functions.Value, groupIdxs []int) map[string][][]functions.Value {
	groups := make(map[string][][]functions.Value)
	for _, row := range rows {
		key := s.makeGroupKey(row, groupIdxs)
		groups[key] = append(groups[key], row)
	}
	return groups
}

// makeGroupKey creates a string key from the group-by column values.
func (s *Stmt) makeGroupKey(row []functions.Value, idxs []int) string {
	parts := make([]string, len(idxs))
	for i, idx := range idxs {
		if idx < len(row) && row[idx] != nil {
			parts[i] = row[idx].AsString()
		}
	}
	return strings.Join(parts, "\x00")
}

// sortGroupKeys returns group keys in sorted order.
func (s *Stmt) sortGroupKeys(groups map[string][][]functions.Value) []string {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// evalGroupByCol evaluates a column expression for a GROUP BY result.
func (s *Stmt) evalGroupByCol(col parser.ResultColumn, groupRows [][]functions.Value, colNames []string) functions.Value {
	_, isFn := col.Expr.(*parser.FunctionExpr)
	if isFn {
		return s.computeAggregate(col, groupRows, colNames)
	}

	// Non-aggregate column: use value from first row in group
	if len(groupRows) == 0 {
		return functions.NewNullValue()
	}
	firstRow := groupRows[0]

	switch e := col.Expr.(type) {
	case *parser.IdentExpr:
		for i, name := range colNames {
			if strings.EqualFold(e.Name, name) && i < len(firstRow) {
				return firstRow[i]
			}
		}
	}
	return functions.NewNullValue()
}
