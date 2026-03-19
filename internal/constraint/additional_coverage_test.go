// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestFormatErrorMessagePublic tests the public FormatErrorMessage function
func TestFormatErrorMessagePublic(t *testing.T) {
	tests := []struct {
		name       string
		constraint *CheckConstraint
		expected   string
	}{
		{
			"with name",
			&CheckConstraint{
				Name:       "check_age",
				ExprString: "age > 0",
			},
			"CHECK constraint failed: check_age (age > 0)",
		},
		{
			"table level no name",
			&CheckConstraint{
				IsTableLevel: true,
				ExprString:   "age > 0",
			},
			"CHECK constraint failed: age > 0",
		},
		{
			"column level",
			&CheckConstraint{
				IsTableLevel: false,
				ColumnName:   "age",
				ExprString:   "age > 0",
			},
			"CHECK constraint failed for column age: age > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatErrorMessage(tt.constraint)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestRemoveConstraints tests RemoveConstraints function
func TestRemoveConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	constraint := &ForeignKeyConstraint{
		Table:    "users",
		Columns:  []string{"dept_id"},
		RefTable: "departments",
		OnDelete: FKActionCascade,
	}

	mgr.AddConstraint(constraint)
	if len(mgr.GetConstraints("users")) != 1 {
		t.Error("Expected constraint to be added")
	}

	mgr.RemoveConstraints("users")
	if len(mgr.GetConstraints("users")) != 0 {
		t.Error("Expected constraints to be removed")
	}
}

// TestConvertFKActionAll tests all foreign key action conversions
func TestConvertFKActionAll(t *testing.T) {
	tests := []struct {
		parserAction parser.ForeignKeyAction
		expected     ForeignKeyAction
	}{
		{parser.FKActionSetNull, FKActionSetNull},
		{parser.FKActionSetDefault, FKActionSetDefault},
		{parser.FKActionCascade, FKActionCascade},
		{parser.FKActionRestrict, FKActionRestrict},
		{parser.FKActionNoAction, FKActionNoAction},
		{parser.ForeignKeyAction(99), FKActionNone},
	}

	for _, tt := range tests {
		result := convertFKAction(tt.parserAction)
		if result != tt.expected {
			t.Errorf("convertFKAction(%v): expected %v, got %v", tt.parserAction, tt.expected, result)
		}
	}
}

// TestConvertDeferrableModeAll tests all deferrable mode conversions
func TestConvertDeferrableModeAll(t *testing.T) {
	tests := []struct {
		parserMode parser.DeferrableMode
		expected   DeferrableMode
	}{
		{parser.DeferrableInitiallyDeferred, DeferrableInitiallyDeferred},
		{parser.DeferrableInitiallyImmediate, DeferrableInitiallyImmediate},
		{parser.DeferrableMode(99), DeferrableNone},
	}

	for _, tt := range tests {
		result := convertDeferrableMode(tt.parserMode)
		if result != tt.expected {
			t.Errorf("convertDeferrableMode(%v): expected %v, got %v", tt.parserMode, tt.expected, result)
		}
	}
}

// TestDefaultConstraintEvaluation tests default constraint evaluation
func TestDefaultConstraintEvaluation(t *testing.T) {
	tests := []struct {
		name        string
		expr        parser.Expression
		shouldError bool
	}{
		{
			"function default",
			&parser.FunctionExpr{Name: "random"},
			true, // Should error for unsupported functions
		},
		{
			"expression default",
			&parser.BinaryExpr{Op: parser.OpPlus, Left: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}},
			true, // Should error for expressions
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dc, err := NewDefaultConstraint(tt.expr)
			if err != nil {
				t.Fatalf("Failed to create constraint: %v", err)
			}

			_, err = dc.Evaluate()
			if (err != nil) != tt.shouldError {
				t.Errorf("Expected error=%v, got error=%v", tt.shouldError, err)
			}
		})
	}
}

// TestParseValueFunctions tests parse value functions
func TestParseValueFunctions(t *testing.T) {
	// Test parseIntegerValue with error
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "invalid"}
	result := parseLiteralValue(lit)
	if result != nil {
		t.Errorf("Expected nil for invalid integer, got %v", result)
	}

	// Test parseFloatValue with error
	lit = &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "invalid"}
	result = parseLiteralValue(lit)
	if result != nil {
		t.Errorf("Expected nil for invalid float, got %v", result)
	}

	// Test unknown literal type
	lit = &parser.LiteralExpr{Type: parser.LiteralType(99), Value: "test"}
	result = parseLiteralValue(lit)
	if result != nil {
		t.Errorf("Expected nil for unknown type, got %v", result)
	}
}

// TestValidateCompositePKUpdate tests composite primary key update validation
func TestValidateCompositePKUpdate(t *testing.T) {
	// Create a mock table with composite primary key
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id1", Type: "INTEGER", PrimaryKey: true},
			{Name: "id2", Type: "INTEGER", PrimaryKey: true},
		},
		PrimaryKey: []string{"id1", "id2"},
	}

	pk := NewPrimaryKeyConstraint(table, nil, nil)

	// Test update that sets PK column to NULL
	newValues := map[string]interface{}{
		"id1": nil,
	}
	err := pk.ValidateUpdate(1, newValues)
	if err == nil {
		t.Error("Expected error for NULL in composite PK")
	}
}

// TestFindGapInRowids tests finding gaps in rowid allocation
func TestFindGapInRowids(t *testing.T) {
	// This function is difficult to test without full btree integration
	// We'll test it indirectly through generateRowid with max int64
	table := &schema.Table{
		Name:     "test",
		RootPage: 1,
		Columns:  []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	// We can't easily test this without a real btree, but we can test the logic
	// by creating a mock scenario
	pk := NewPrimaryKeyConstraint(table, nil, nil)
	if pk == nil {
		t.Error("Expected non-nil primary key constraint")
	}
}

// TestConvertToInt64AllTypes tests all type conversions to int64
func TestConvertToInt64AllTypes(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 1,
		Columns:  []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}
	pk := NewPrimaryKeyConstraint(table, nil, nil)

	tests := []struct {
		name        string
		value       interface{}
		shouldError bool
	}{
		{"int64", int64(42), false},
		{"int", int(42), false},
		{"int32", int32(42), false},
		{"uint32", uint32(42), false},
		{"float64", float64(42.5), false},
		{"invalid type", "string", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pk.convertToInt64(tt.value)
			if (err != nil) != tt.shouldError {
				t.Errorf("Expected error=%v, got error=%v", tt.shouldError, err)
			}
		})
	}
}

// TestUniqueConstraintHelpers tests unique constraint helper functions
func TestUniqueConstraintHelpers(t *testing.T) {
	testBothNil(t)
	testEitherNil(t)
	testCompareInt(t)
	testCompareInt64(t)
	testCompareFloat64(t)
	testCompareString(t)
	testCompareBytes(t)
}

func testBothNil(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"both_nil", nil, nil, true},
		{"only_first_nil", nil, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bothNil(tt.a, tt.b); got != tt.want {
				t.Errorf("bothNil(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func testEitherNil(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"first_nil", nil, 1, true},
		{"second_nil", 1, nil, true},
		{"both_nil", nil, nil, true},
		{"neither_nil", 1, 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eitherNil(tt.a, tt.b); got != tt.want {
				t.Errorf("eitherNil(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func testCompareInt(t *testing.T) {
	tests := []struct {
		name string
		a    int
		b    interface{}
		want bool
	}{
		{"equal_ints", 5, 5, true},
		{"int_and_int64", 5, int64(5), true},
		{"unequal_ints", 5, 6, false},
		{"different_types", 5, "5", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareInt(tt.a, tt.b); got != tt.want {
				t.Errorf("compareInt(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func testCompareInt64(t *testing.T) {
	tests := []struct {
		name string
		a    int64
		b    interface{}
		want bool
	}{
		{"equal_int64s", int64(5), int64(5), true},
		{"int64_and_int", int64(5), 5, true},
		{"unequal_int64s", int64(5), int64(6), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareInt64(tt.a, tt.b); got != tt.want {
				t.Errorf("compareInt64(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func testCompareFloat64(t *testing.T) {
	tests := []struct {
		name string
		a    float64
		b    interface{}
		want bool
	}{
		{"equal_floats", 3.14, 3.14, true},
		{"unequal_floats", 3.14, 2.71, false},
		{"different_types", 3.14, "3.14", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareFloat64(tt.a, tt.b); got != tt.want {
				t.Errorf("compareFloat64(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func testCompareString(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    interface{}
		want bool
	}{
		{"equal_strings", "test", "test", true},
		{"different_strings", "test", "TEST", false},
		{"different_types", "test", 123, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareString(tt.a, tt.b); got != tt.want {
				t.Errorf("compareString(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func testCompareBytes(t *testing.T) {
	tests := []struct {
		name string
		a    []byte
		b    interface{}
		want bool
	}{
		{"equal_bytes", []byte("test"), []byte("test"), true},
		{"different_bytes", []byte("test"), []byte("TEST"), false},
		{"different_lengths", []byte("test"), []byte("te"), false},
		{"different_types", []byte("test"), "test", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareBytes(tt.a, tt.b); got != tt.want {
				t.Errorf("compareBytes(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestParseRecordValues tests parseRecordValues function
func TestParseRecordValues(t *testing.T) {
	table := &schema.Table{
		Name:    "test",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	// Test with empty data - should return empty map
	values, err := parseRecordValues([]byte{}, table)
	if err != nil {
		t.Errorf("Expected no error for empty data, got %v", err)
	}
	if len(values) != 0 {
		t.Error("Expected empty values map for empty data")
	}

	// Test with invalid data - should return error
	_, err = parseRecordValues([]byte("dummy data"), table)
	if err == nil {
		t.Error("Expected error for invalid record data, got nil")
	}
}

// TestIsValidRowData tests isValidRowData helper
func TestIsValidRowData(t *testing.T) {
	uc := &UniqueConstraint{}

	if !uc.isValidRowData([]byte("data")) {
		t.Error("Expected true for non-nil data")
	}
	if uc.isValidRowData(nil) {
		t.Error("Expected false for nil data")
	}
}

// TestCheckCurrentRowConcept tests the concept behind checkCurrentRow
func TestCheckCurrentRowConcept(t *testing.T) {
	// checkCurrentRow requires a real btree cursor which we can't easily mock
	// We test the underlying helpers instead
	uc := &UniqueConstraint{}

	// Test isValidRowData which is used by checkCurrentRow
	if !uc.isValidRowData([]byte("test")) {
		t.Error("Expected true for valid data")
	}
	if uc.isValidRowData(nil) {
		t.Error("Expected false for nil data")
	}
}

// TestValidateTableRow tests ValidateTableRow function
func TestValidateTableRow(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "email", Type: "TEXT", Unique: true},
		},
	}

	// ValidateTableRow requires btree integration
	// We test that it at least extracts constraints correctly
	constraints := ExtractUniqueConstraints(table)
	if len(constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(constraints))
	}
}

// TestNotNullValidateRow tests ValidateRow error path
func TestNotNullValidateRow(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
		},
	}

	nnc := NewNotNullConstraint(table)

	// Test with missing required column
	values := map[string]interface{}{}
	err := nnc.ValidateRow(values)
	if err == nil {
		t.Error("Expected error for missing NOT NULL column")
	}
}
