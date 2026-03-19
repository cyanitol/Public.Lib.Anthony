// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package constraint provides constraint handling for SQLite databases.
// It implements DEFAULT value constraints and other column/table constraints.
package constraint

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// DefaultConstraint represents a DEFAULT value constraint for a column.
// It supports literal defaults (numbers, strings, NULL), expression defaults
// (CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP), and function call defaults.
type DefaultConstraint struct {
	// Type indicates the kind of default value
	Type DefaultType

	// Raw expression from the parser
	Expr parser.Expression

	// Cached literal value (for literal defaults)
	LiteralValue interface{}

	// Function name (for function defaults)
	FunctionName string

	// Function arguments (for function defaults)
	FunctionArgs []parser.Expression
}

// DefaultType indicates the type of default value.
type DefaultType int

const (
	// DefaultLiteral represents a literal default value (number, string, NULL)
	DefaultLiteral DefaultType = iota

	// DefaultCurrentTime represents CURRENT_TIME
	DefaultCurrentTime

	// DefaultCurrentDate represents CURRENT_DATE
	DefaultCurrentDate

	// DefaultCurrentTimestamp represents CURRENT_TIMESTAMP
	DefaultCurrentTimestamp

	// DefaultFunction represents a function call default
	DefaultFunction

	// DefaultExpression represents a general expression default
	DefaultExpression
)

// NewDefaultConstraint creates a DefaultConstraint from a parser expression.
// It analyzes the expression and determines the appropriate default type.
func NewDefaultConstraint(expr parser.Expression) (*DefaultConstraint, error) {
	if expr == nil {
		return nil, fmt.Errorf("nil default expression")
	}

	dc := &DefaultConstraint{
		Expr: expr,
	}

	// Analyze the expression type
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		dc.Type = DefaultLiteral
		dc.LiteralValue = parseLiteralValue(e)

	case *parser.FunctionExpr:
		// Check for special current time/date/timestamp functions
		upperName := strings.ToUpper(e.Name)
		switch upperName {
		case "CURRENT_TIME":
			dc.Type = DefaultCurrentTime
		case "CURRENT_DATE":
			dc.Type = DefaultCurrentDate
		case "CURRENT_TIMESTAMP":
			dc.Type = DefaultCurrentTimestamp
		default:
			dc.Type = DefaultFunction
			dc.FunctionName = e.Name
			dc.FunctionArgs = e.Args
		}

	default:
		// General expression (could be arithmetic, etc.)
		dc.Type = DefaultExpression
	}

	return dc, nil
}

// parseLiteralValue converts a LiteralExpr to a Go value.
func parseLiteralValue(lit *parser.LiteralExpr) interface{} {
	switch lit.Type {
	case parser.LiteralNull:
		return nil
	case parser.LiteralInteger:
		return parseIntegerValue(lit.Value)
	case parser.LiteralFloat:
		return parseFloatValue(lit.Value)
	case parser.LiteralString:
		return parseStringValue(lit.Value)
	case parser.LiteralBlob:
		return lit.Value
	default:
		return nil
	}
}

// parseIntegerValue parses an integer literal value.
func parseIntegerValue(value string) interface{} {
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return val
	}
	return nil
}

// parseFloatValue parses a float literal value.
func parseFloatValue(value string) interface{} {
	val, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return val
	}
	return nil
}

// parseStringValue removes surrounding quotes from a string literal.
func parseStringValue(value string) string {
	s := value
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') {
		s = s[1 : len(s)-1]
	}
	return s
}

// Evaluate computes the default value for the constraint.
// For literal defaults, it returns the cached value.
// For expression defaults, it evaluates them at call time.
func (dc *DefaultConstraint) Evaluate() (interface{}, error) {
	switch dc.Type {
	case DefaultLiteral:
		return dc.LiteralValue, nil

	case DefaultCurrentTime:
		return time.Now().Format("15:04:05"), nil

	case DefaultCurrentDate:
		return time.Now().Format("2006-01-02"), nil

	case DefaultCurrentTimestamp:
		return time.Now().Format("2006-01-02 15:04:05"), nil

	case DefaultFunction:
		// For now, return error for unsupported functions
		// In a full implementation, this would call the function registry
		return nil, fmt.Errorf("function defaults not yet supported: %s", dc.FunctionName)

	case DefaultExpression:
		// For now, return error for general expressions
		// In a full implementation, this would use the expression evaluator
		return nil, fmt.Errorf("expression defaults not yet supported")

	default:
		return nil, fmt.Errorf("unknown default type: %d", dc.Type)
	}
}

// ShouldApplyDefault determines whether to apply the default value.
// It returns true if:
// - The column was not specified in the INSERT statement, OR
// - The column was explicitly set to NULL and the column allows NULL
func ShouldApplyDefault(valueProvided bool, valueIsNull bool, columnAllowsNull bool) bool {
	// If no value was provided, always apply default
	if !valueProvided {
		return true
	}

	// If NULL was explicitly provided, only apply default if column is NOT NULL
	// (NOT NULL columns should use their default instead of NULL)
	if valueIsNull && !columnAllowsNull {
		return true
	}

	return false
}

// ApplyDefaults applies default values to an INSERT operation.
// It takes the column definitions, the columns specified in the INSERT,
// and the values provided, and returns the complete set of values with defaults.
func ApplyDefaults(
	tableCols []*ColumnInfo,
	insertCols []string,
	insertVals []interface{},
) ([]interface{}, error) {
	insertColMap := buildInsertColumnMap(insertCols)
	result := make([]interface{}, len(tableCols))

	for i, col := range tableCols {
		val, err := resolveColumnValue(col, insertColMap, insertVals)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}

	return result, nil
}

// buildInsertColumnMap creates a map from column names to their index in the INSERT values.
func buildInsertColumnMap(insertCols []string) map[string]int {
	insertColMap := make(map[string]int)
	for i, colName := range insertCols {
		insertColMap[strings.ToLower(colName)] = i
	}
	return insertColMap
}

// resolveColumnValue determines the final value for a column, applying defaults if needed.
func resolveColumnValue(col *ColumnInfo, insertColMap map[string]int, insertVals []interface{}) (interface{}, error) {
	colNameLower := strings.ToLower(col.Name)
	idx, exists := insertColMap[colNameLower]

	if exists {
		return handleProvidedValue(col, insertVals[idx])
	}
	return handleMissingValue(col)
}

// handleProvidedValue processes a value that was explicitly provided in the INSERT.
func handleProvidedValue(col *ColumnInfo, val interface{}) (interface{}, error) {
	valueIsNull := (val == nil)

	if col.DefaultConstraint != nil && ShouldApplyDefault(true, valueIsNull, col.AllowsNull) {
		return evaluateDefault(col)
	}
	return val, nil
}

// handleMissingValue processes a column that was not provided in the INSERT.
func handleMissingValue(col *ColumnInfo) (interface{}, error) {
	if col.DefaultConstraint != nil {
		return evaluateDefault(col)
	}
	return nil, nil
}

// evaluateDefault evaluates a default constraint and returns the result.
func evaluateDefault(col *ColumnInfo) (interface{}, error) {
	defaultVal, err := col.DefaultConstraint.Evaluate()
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate default for column %s: %w", col.Name, err)
	}
	return defaultVal, nil
}

// ColumnInfo represents column information needed for default value application.
type ColumnInfo struct {
	Name              string
	AllowsNull        bool
	DefaultConstraint *DefaultConstraint
}
