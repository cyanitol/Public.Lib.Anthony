package expr

import (
	"strconv"
	"strings"
)

// ----------------------------------------------------------------------------
// GetExprAffinity
// ----------------------------------------------------------------------------

// exprAffinityResult is returned by each opcode handler in exprAffinityDispatch.
// done=true means return the affinity; done=false means loop again with the
// updated *Expr pointer.
type exprAffinityResult struct {
	aff  Affinity
	e    *Expr
	done bool
}

// exprAffinityDispatch maps each OpCode to a handler that computes the next
// step of GetExprAffinity.  Only opcodes that need special treatment are
// present; the default path (stored affinity) is handled by the caller.
//
// Populated by init() to avoid an initialization cycle: the handlers call
// GetExprAffinity, which reads this map.
var exprAffinityDispatch map[OpCode]func(*Expr) exprAffinityResult

func init() {
	exprAffinityDispatch = map[OpCode]func(*Expr) exprAffinityResult{
		OpColumn:       affinityHandleColumn,
		OpAggColumn:    affinityHandleColumn,
		OpSelect:       affinityHandleSelect,
		OpCast:         affinityHandleCast,
		OpSelectColumn: affinityHandleSelectColumn,
		OpVector:       affinityHandleVector,
		OpFunction:     affinityHandleFunction,
		OpCollate:      affinityHandleTransparent,
		OpUnaryPlus:    affinityHandleTransparent,
		OpRegister:     affinityHandleRegister,
	}
}

func affinityHandleColumn(e *Expr) exprAffinityResult {
	return exprAffinityResult{aff: e.Affinity, done: true}
}

func affinityHandleSelect(e *Expr) exprAffinityResult {
	if e.Select != nil && e.Select.Columns != nil &&
		len(e.Select.Columns.Items) > 0 {
		return exprAffinityResult{
			aff:  GetExprAffinity(e.Select.Columns.Items[0].Expr),
			done: true,
		}
	}
	return exprAffinityResult{aff: AFF_NONE, done: true}
}

func affinityHandleCast(e *Expr) exprAffinityResult {
	return exprAffinityResult{aff: AffinityFromType(e.Token), done: true}
}

func affinityHandleSelectColumn(e *Expr) exprAffinityResult {
	if e.Left != nil && e.Left.Op == OpSelect &&
		e.Left.Select != nil && e.Left.Select.Columns != nil {
		cols := e.Left.Select.Columns.Items
		if e.IColumn >= 0 && e.IColumn < len(cols) {
			return exprAffinityResult{
				aff:  GetExprAffinity(cols[e.IColumn].Expr),
				done: true,
			}
		}
	}
	return exprAffinityResult{aff: AFF_NONE, done: true}
}

func affinityHandleVector(e *Expr) exprAffinityResult {
	if e.List != nil && len(e.List.Items) > 0 {
		return exprAffinityResult{
			aff:  GetExprAffinity(e.List.Items[0].Expr),
			done: true,
		}
	}
	return exprAffinityResult{aff: AFF_NONE, done: true}
}

func affinityHandleFunction(e *Expr) exprAffinityResult {
	if e.Affinity == AFF_NONE && e.List != nil && len(e.List.Items) > 0 {
		return exprAffinityResult{
			aff:  GetExprAffinity(e.List.Items[0].Expr),
			done: true,
		}
	}
	return exprAffinityResult{aff: e.Affinity, done: true}
}

// affinityHandleTransparent handles OpCollate and OpUnaryPlus by descending
// into the left operand.
func affinityHandleTransparent(e *Expr) exprAffinityResult {
	if e.Left != nil {
		return exprAffinityResult{e: e.Left, done: false}
	}
	return exprAffinityResult{aff: AFF_NONE, done: true}
}

func affinityHandleRegister(e *Expr) exprAffinityResult {
	if e.IOp2 != 0 {
		// Manufacture a proxy expression carrying only the stored op so the
		// dispatcher can be re-entered without mutating the original node.
		proxy := &Expr{Op: e.IOp2, Affinity: e.Affinity}
		return exprAffinityResult{e: proxy, done: false}
	}
	return exprAffinityResult{aff: e.Affinity, done: true}
}

// GetExprAffinity returns the affinity of an expression.
// This determines how values should be coerced for comparison.
//
// If the expression is a column, returns that column's affinity.
// For CAST expressions, returns the target type's affinity.
// For other expressions, returns NONE.
func GetExprAffinity(e *Expr) Affinity {
	if e == nil {
		return AFF_NONE
	}

	for {
		handler, ok := exprAffinityDispatch[e.Op]
		if !ok {
			// Default: use stored affinity.
			return e.Affinity
		}

		res := handler(e)
		if res.done {
			return res.aff
		}
		// Continue loop with updated expression pointer.
		e = res.e
	}
}

// ----------------------------------------------------------------------------
// AffinityFromType (unchanged – CC is already acceptable)
// ----------------------------------------------------------------------------

// affinityRules maps substring patterns to affinities, checked in order.
var affinityRules = []struct {
	patterns []string
	affinity Affinity
}{
	{[]string{"INT"}, AFF_INTEGER},
	{[]string{"CHAR", "CLOB", "TEXT"}, AFF_TEXT},
	{[]string{"BLOB"}, AFF_BLOB},
	{[]string{"REAL", "FLOA", "DOUB"}, AFF_REAL},
}

// AffinityFromType determines affinity from a type name.
// This implements SQLite's type affinity rules.
func AffinityFromType(typeName string) Affinity {
	if typeName == "" {
		return AFF_BLOB
	}
	upper := strings.ToUpper(typeName)
	for _, rule := range affinityRules {
		if containsAnyAffinity(upper, rule.patterns) {
			return rule.affinity
		}
	}
	return AFF_NUMERIC
}

// containsAnyAffinity returns true if s contains any of the patterns.
func containsAnyAffinity(s string, patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// CompareAffinity (unchanged – CC is already acceptable)
// ----------------------------------------------------------------------------

// CompareAffinity determines the affinity to use when comparing two expressions.
// This implements SQLite's type affinity rules for comparisons.
func CompareAffinity(left, right *Expr) Affinity {
	aff1 := GetExprAffinity(left)
	aff2 := GetExprAffinity(right)

	if aff1 > AFF_NONE && aff2 > AFF_NONE {
		// Both sides are columns
		// If either has numeric affinity, use NUMERIC
		if IsNumericAffinity(aff1) || IsNumericAffinity(aff2) {
			return AFF_NUMERIC
		}
		// Otherwise use BLOB (no affinity conversion)
		return AFF_BLOB
	}

	// One side is a column, use that column's affinity
	if aff1 <= AFF_NONE {
		return aff2
	}
	return aff1
}

// comparisonOps is the set of comparison operators.
var comparisonOps = map[OpCode]bool{
	OpEq: true, OpNe: true, OpLt: true, OpLe: true,
	OpGt: true, OpGe: true, OpIs: true, OpIsNot: true,
}

// GetComparisonAffinity returns the affinity for a comparison expression.
func GetComparisonAffinity(e *Expr) Affinity {
	if e == nil || !comparisonOps[e.Op] {
		return AFF_NONE
	}
	if e.Left == nil {
		return AFF_BLOB
	}
	aff := computeComparisonAffinity(e)
	if aff == AFF_NONE {
		return AFF_BLOB
	}
	return aff
}

// computeComparisonAffinity computes the affinity for a valid comparison expression.
func computeComparisonAffinity(e *Expr) Affinity {
	if e.Right != nil {
		return CompareAffinity(e.Right, e.Left)
	}
	if e.HasProperty(EP_xIsSelect) && e.Select != nil {
		return subqueryComparisonAffinity(e)
	}
	return GetExprAffinity(e.Left)
}

// subqueryComparisonAffinity computes affinity when comparing with a subquery.
func subqueryComparisonAffinity(e *Expr) Affinity {
	if e.Select.Columns != nil && len(e.Select.Columns.Items) > 0 {
		rightAff := GetExprAffinity(e.Select.Columns.Items[0].Expr)
		return CompareAffinity(&Expr{Affinity: rightAff}, e.Left)
	}
	return GetExprAffinity(e.Left)
}

// ----------------------------------------------------------------------------
// ApplyAffinity
// ----------------------------------------------------------------------------

// applyIntegerAffinity converts value to int64 when possible.
func applyIntegerAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f)
		}
		return v
	default:
		return value
	}
}

// applyRealAffinity converts value to float64 when possible.
func applyRealAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return v
	default:
		return value
	}
}

// applyNumericAffinity converts value to the most appropriate numeric type.
func applyNumericAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		return v
	default:
		return value
	}
}

// applyTextAffinity converts value to a string representation.
func applyTextAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return value
	}
}

// applyAffinityDispatch maps each Affinity constant to its converter function.
// AFF_BLOB and AFF_NONE perform no conversion and are absent from the map;
// the caller returns the value unchanged for any missing key.
var applyAffinityDispatch = map[Affinity]func(interface{}) interface{}{
	AFF_INTEGER: applyIntegerAffinity,
	AFF_REAL:    applyRealAffinity,
	AFF_NUMERIC: applyNumericAffinity,
	AFF_TEXT:    applyTextAffinity,
}

// ApplyAffinity converts a value according to the specified affinity.
// This is a simplified version - a real implementation would work with
// actual SQLite values.
func ApplyAffinity(value interface{}, aff Affinity) interface{} {
	if value == nil {
		return nil
	}

	if convert, ok := applyAffinityDispatch[aff]; ok {
		return convert(value)
	}
	// AFF_BLOB, AFF_NONE, and any unknown affinity: no conversion.
	return value
}

// ----------------------------------------------------------------------------
// SetTableColumnAffinity (unchanged – single statement, CC=1)
// ----------------------------------------------------------------------------

// SetTableColumnAffinity sets the affinity for a column expression based
// on the table schema. This would be called during name resolution.
func SetTableColumnAffinity(e *Expr, colType string) {
	if e == nil || e.Op != OpColumn {
		return
	}
	e.Affinity = AffinityFromType(colType)
}

// ----------------------------------------------------------------------------
// PropagateAffinity
// ----------------------------------------------------------------------------

// opAffinityTable maps opcodes that unconditionally produce a fixed affinity.
// Opcodes requiring dynamic computation (OpNegate, OpCase) are handled
// separately by propagateAffinitySpecial.
var opAffinityTable = map[OpCode]Affinity{
	// Arithmetic operators produce numeric results.
	OpPlus:      AFF_NUMERIC,
	OpMinus:     AFF_NUMERIC,
	OpMultiply:  AFF_NUMERIC,
	OpDivide:    AFF_NUMERIC,
	OpRemainder: AFF_NUMERIC,

	// String concatenation produces text.
	OpConcat: AFF_TEXT,

	// Bitwise operators produce integers.
	OpBitAnd: AFF_INTEGER,
	OpBitOr:  AFF_INTEGER,
	OpBitXor: AFF_INTEGER,
	OpLShift: AFF_INTEGER,
	OpRShift: AFF_INTEGER,

	// Comparison and logical operators produce boolean (integer 0/1).
	OpEq: AFF_INTEGER, OpNe: AFF_INTEGER,
	OpLt: AFF_INTEGER, OpLe: AFF_INTEGER,
	OpGt: AFF_INTEGER, OpGe: AFF_INTEGER,
	OpAnd: AFF_INTEGER, OpOr: AFF_INTEGER,
	OpNot: AFF_INTEGER,
	OpIs: AFF_INTEGER, OpIsNot: AFF_INTEGER,
	OpIsNull: AFF_INTEGER, OpNotNull: AFF_INTEGER,
	OpIn: AFF_INTEGER, OpNotIn: AFF_INTEGER,
	OpBetween: AFF_INTEGER, OpNotBetween: AFF_INTEGER,
	OpLike: AFF_INTEGER, OpGlob: AFF_INTEGER,
	OpExists: AFF_INTEGER,
}

// propagateAffinityNegate handles the OpNegate case: unary minus preserves
// numeric affinity of the operand or defaults to NUMERIC.
func propagateAffinityNegate(e *Expr) {
	if e.Left == nil {
		return
	}
	aff := GetExprAffinity(e.Left)
	if IsNumericAffinity(aff) {
		e.Affinity = aff
	} else {
		e.Affinity = AFF_NUMERIC
	}
}

// propagateAffinityCase handles the OpCase case: affinity is taken from
// THEN/ELSE clauses and unified to NONE on disagreement.
func propagateAffinityCase(e *Expr) {
	if e.List == nil {
		return
	}

	var resultAff Affinity = AFF_NONE
	// CASE list layout: WHEN1, THEN1, WHEN2, THEN2, ..., [ELSE]
	for i := 1; i < len(e.List.Items); i += 2 {
		thenAff := GetExprAffinity(e.List.Items[i].Expr)
		if resultAff == AFF_NONE {
			resultAff = thenAff
		} else if resultAff != thenAff {
			resultAff = AFF_NONE
		}
	}

	// Check optional ELSE clause (present when item count is odd).
	if len(e.List.Items)%2 == 1 {
		elseIdx := len(e.List.Items) - 1
		elseAff := GetExprAffinity(e.List.Items[elseIdx].Expr)
		if resultAff == AFF_NONE {
			resultAff = elseAff
		} else if resultAff != elseAff {
			resultAff = AFF_NONE
		}
	}

	e.Affinity = resultAff
}

// propagateAffinitySpecial handles opcodes that require dynamic computation
// rather than a fixed table lookup.
var propagateAffinitySpecial = map[OpCode]func(*Expr){
	OpNegate: propagateAffinityNegate,
	OpCase:   propagateAffinityCase,
}

// PropagateAffinity propagates affinity information through an expression tree.
// This is called during semantic analysis.
func PropagateAffinity(e *Expr) {
	if e == nil {
		return
	}

	// Recursively process children.
	PropagateAffinity(e.Left)
	PropagateAffinity(e.Right)
	if e.List != nil {
		for _, item := range e.List.Items {
			PropagateAffinity(item.Expr)
		}
	}

	// Apply fixed-affinity table first.
	if aff, ok := opAffinityTable[e.Op]; ok {
		e.Affinity = aff
		return
	}

	// Apply dynamic handlers for opcodes that need computation.
	if handler, ok := propagateAffinitySpecial[e.Op]; ok {
		handler(e)
	}
	// For all other opcodes, affinity is already set or remains NONE.
}
