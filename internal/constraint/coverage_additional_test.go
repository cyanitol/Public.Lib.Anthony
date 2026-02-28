package constraint

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// TestUniqueConstraint_CheckDuplicateViaIndex_EmptyTable tests checking empty table
func TestUniqueConstraint_CheckDuplicateViaIndex_EmptyTable(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 2,
		Columns:  []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	uc := NewUniqueConstraint("", "test", []string{"id"})

	values := map[string]interface{}{"id": 1}
	exists, conflictRowid, err := uc.checkDuplicateViaIndex(bt, table, values, 0)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if exists {
		t.Error("Expected no duplicate in empty table")
	}
	if conflictRowid != 0 {
		t.Errorf("Expected conflict rowid 0, got %d", conflictRowid)
	}
}

// TestUniqueConstraint_ValidateWithDefault tests validation when column has default
func TestUniqueConstraint_ValidateWithDefault(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "status", Type: "TEXT", Default: "active"},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	uc := NewUniqueConstraint("", "test", []string{"status"})

	// Values without status - should use default
	values := map[string]interface{}{"id": 1}

	err := uc.Validate(table, bt, values, 1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestCheckConstraint_ValidateWithNoConstraints tests validator with no constraints
func TestCheckConstraint_ValidateWithNoConstraints(t *testing.T) {
	table := &schema.Table{
		Name:    "test",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	validator := NewCheckValidator(table)

	if validator.HasCheckConstraints() {
		t.Error("Expected no CHECK constraints")
	}

	mock := &mockCodeGenerator{}
	err := validator.ValidateInsertWithGenerator(mock)
	if err != nil {
		t.Errorf("ValidateInsertWithGenerator should not fail: %v", err)
	}

	if len(mock.constraints) != 0 {
		t.Errorf("Expected 0 constraints to be generated, got %d", len(mock.constraints))
	}
}

// TestCheckConstraint_FormatErrorMessagePublic tests the public FormatErrorMessage function
func TestCheckConstraint_FormatErrorMessagePublic(t *testing.T) {
	tests := []struct {
		name       string
		constraint *CheckConstraint
		wantSubstr string
	}{
		{
			name: "named constraint",
			constraint: &CheckConstraint{
				Name:       "valid_age",
				ExprString: "age >= 0",
			},
			wantSubstr: "valid_age",
		},
		{
			name: "table-level unnamed",
			constraint: &CheckConstraint{
				Name:         "",
				ExprString:   "price > 0",
				IsTableLevel: true,
			},
			wantSubstr: "price > 0",
		},
		{
			name: "column-level",
			constraint: &CheckConstraint{
				Name:         "",
				ExprString:   "length > 0",
				IsTableLevel: false,
				ColumnName:   "length",
			},
			wantSubstr: "length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := FormatErrorMessage(tt.constraint)
			if msg == "" {
				t.Error("Expected non-empty error message")
			}
		})
	}
}

// TestDefaultConstraint_ShouldApplyDefault tests the ShouldApplyDefault function
func TestDefaultConstraint_ShouldApplyDefault(t *testing.T) {
	tests := []struct {
		name             string
		valueProvided    bool
		valueIsNull      bool
		columnAllowsNull bool
		want             bool
	}{
		{
			name:             "no value provided",
			valueProvided:    false,
			valueIsNull:      false,
			columnAllowsNull: true,
			want:             true,
		},
		{
			name:             "null provided to NOT NULL column",
			valueProvided:    true,
			valueIsNull:      true,
			columnAllowsNull: false,
			want:             true,
		},
		{
			name:             "null provided to nullable column",
			valueProvided:    true,
			valueIsNull:      true,
			columnAllowsNull: true,
			want:             false,
		},
		{
			name:             "value provided",
			valueProvided:    true,
			valueIsNull:      false,
			columnAllowsNull: true,
			want:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldApplyDefault(tt.valueProvided, tt.valueIsNull, tt.columnAllowsNull)
			if got != tt.want {
				t.Errorf("ShouldApplyDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDefaultConstraint_ApplyDefaults tests ApplyDefaults function
func TestDefaultConstraint_ApplyDefaults(t *testing.T) {
	// Create column info with default constraints
	tableCols := []*ColumnInfo{
		{
			Name:       "id",
			AllowsNull: false,
			DefaultConstraint: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: int64(0),
			},
		},
		{
			Name:       "status",
			AllowsNull: true,
			DefaultConstraint: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: "active",
			},
		},
		{
			Name:       "name",
			AllowsNull: true,
			DefaultConstraint: nil,
		},
	}

	tests := []struct {
		name        string
		insertCols  []string
		insertVals  []interface{}
		wantValues  []interface{}
		wantError   bool
	}{
		{
			name:       "apply defaults for missing columns",
			insertCols: []string{},
			insertVals: []interface{}{},
			wantValues: []interface{}{int64(0), "active", nil},
			wantError:  false,
		},
		{
			name:       "use provided values",
			insertCols: []string{"id", "status", "name"},
			insertVals: []interface{}{int64(42), "custom", "test"},
			wantValues: []interface{}{int64(42), "custom", "test"},
			wantError:  false,
		},
		{
			name:       "partial values with defaults",
			insertCols: []string{"id"},
			insertVals: []interface{}{int64(99)},
			wantValues: []interface{}{int64(99), "active", nil},
			wantError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyDefaults(tableCols, tt.insertCols, tt.insertVals)

			if tt.wantError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if len(result) != len(tt.wantValues) {
					t.Errorf("len(result) = %d, want %d", len(result), len(tt.wantValues))
				}

				for i, want := range tt.wantValues {
					if result[i] != want {
						t.Errorf("result[%d] = %v, want %v", i, result[i], want)
					}
				}
			}
		})
	}
}

// TestDefaultConstraint_Evaluate tests the Evaluate method
func TestDefaultConstraint_Evaluate(t *testing.T) {
	tests := []struct {
		name      string
		dc        *DefaultConstraint
		wantError bool
		checkType bool
	}{
		{
			name: "literal string",
			dc: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: "default_value",
			},
			wantError: false,
		},
		{
			name: "literal integer",
			dc: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: int64(42),
			},
			wantError: false,
		},
		{
			name: "literal null",
			dc: &DefaultConstraint{
				Type:         DefaultLiteral,
				LiteralValue: nil,
			},
			wantError: false,
		},
		{
			name: "current time",
			dc: &DefaultConstraint{
				Type: DefaultCurrentTime,
			},
			wantError: false,
			checkType: true,
		},
		{
			name: "current date",
			dc: &DefaultConstraint{
				Type: DefaultCurrentDate,
			},
			wantError: false,
			checkType: true,
		},
		{
			name: "current timestamp",
			dc: &DefaultConstraint{
				Type: DefaultCurrentTimestamp,
			},
			wantError: false,
			checkType: true,
		},
		{
			name: "unsupported function",
			dc: &DefaultConstraint{
				Type:         DefaultFunction,
				FunctionName: "RANDOM",
			},
			wantError: true,
		},
		{
			name: "unsupported expression",
			dc: &DefaultConstraint{
				Type: DefaultExpression,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.dc.Evaluate()

			if tt.wantError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if tt.checkType && result == nil {
					t.Error("Expected non-nil result for time/date function")
				}
			}
		})
	}
}

// TestForeignKeyManager_ColumnsChanged tests the columnsChanged helper
func TestForeignKeyManager_ColumnsChanged(t *testing.T) {
	tests := []struct {
		name      string
		columns   []string
		oldValues map[string]interface{}
		newValues map[string]interface{}
		want      bool
	}{
		{
			name:      "no change",
			columns:   []string{"id"},
			oldValues: map[string]interface{}{"id": 1},
			newValues: map[string]interface{}{"id": 1},
			want:      false,
		},
		{
			name:      "single column changed",
			columns:   []string{"id"},
			oldValues: map[string]interface{}{"id": 1},
			newValues: map[string]interface{}{"id": 2},
			want:      true,
		},
		{
			name:      "multi-column one changed",
			columns:   []string{"a", "b"},
			oldValues: map[string]interface{}{"a": 1, "b": 2},
			newValues: map[string]interface{}{"a": 1, "b": 3},
			want:      true,
		},
		{
			name:      "multi-column no change",
			columns:   []string{"a", "b"},
			oldValues: map[string]interface{}{"a": 1, "b": 2},
			newValues: map[string]interface{}{"a": 1, "b": 2},
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := columnsChanged(tt.columns, tt.oldValues, tt.newValues)
			if got != tt.want {
				t.Errorf("columnsChanged() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestForeignKeyManager_ExtractKeyValues tests extractKeyValues helper
func TestForeignKeyManager_ExtractKeyValues(t *testing.T) {
	values := map[string]interface{}{
		"id":   1,
		"name": "test",
		"age":  30,
	}

	tests := []struct {
		name    string
		columns []string
		want    []interface{}
	}{
		{
			name:    "single column",
			columns: []string{"id"},
			want:    []interface{}{1},
		},
		{
			name:    "multiple columns",
			columns: []string{"name", "age"},
			want:    []interface{}{"test", 30},
		},
		{
			name:    "column order matters",
			columns: []string{"age", "name"},
			want:    []interface{}{30, "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractKeyValues(values, tt.columns)
			if len(got) != len(tt.want) {
				t.Errorf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("index %d: got %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestForeignKeyManager_ExtractForeignKeyValues tests extractForeignKeyValues
func TestForeignKeyManager_ExtractForeignKeyValues(t *testing.T) {
	tests := []struct {
		name        string
		values      map[string]interface{}
		columns     []string
		wantHasNull bool
	}{
		{
			name:        "all values present",
			values:      map[string]interface{}{"a": 1, "b": 2},
			columns:     []string{"a", "b"},
			wantHasNull: false,
		},
		{
			name:        "one value null",
			values:      map[string]interface{}{"a": 1, "b": nil},
			columns:     []string{"a", "b"},
			wantHasNull: true,
		},
		{
			name:        "one value missing",
			values:      map[string]interface{}{"a": 1},
			columns:     []string{"a", "b"},
			wantHasNull: true,
		},
		{
			name:        "all null",
			values:      map[string]interface{}{"a": nil, "b": nil},
			columns:     []string{"a", "b"},
			wantHasNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, hasNull := extractForeignKeyValues(tt.values, tt.columns)
			if hasNull != tt.wantHasNull {
				t.Errorf("hasNull = %v, want %v", hasNull, tt.wantHasNull)
			}
		})
	}
}

// TestConvertFKAction tests action conversion
func TestConvertFKAction(t *testing.T) {
	tests := []struct {
		name   string
		action parser.ForeignKeyAction
		want   ForeignKeyAction
	}{
		{"SetNull", parser.FKActionSetNull, FKActionSetNull},
		{"SetDefault", parser.FKActionSetDefault, FKActionSetDefault},
		{"Cascade", parser.FKActionCascade, FKActionCascade},
		{"Restrict", parser.FKActionRestrict, FKActionRestrict},
		{"NoAction", parser.FKActionNoAction, FKActionNoAction},
		{"None/Unknown", parser.ForeignKeyAction(999), FKActionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertFKAction(tt.action)
			if got != tt.want {
				t.Errorf("convertFKAction() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConvertDeferrableMode tests deferrable mode conversion
func TestConvertDeferrableMode(t *testing.T) {
	tests := []struct {
		name string
		mode parser.DeferrableMode
		want DeferrableMode
	}{
		{"InitiallyDeferred", parser.DeferrableInitiallyDeferred, DeferrableInitiallyDeferred},
		{"InitiallyImmediate", parser.DeferrableInitiallyImmediate, DeferrableInitiallyImmediate},
		{"None", parser.DeferrableMode(999), DeferrableNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertDeferrableMode(tt.mode)
			if got != tt.want {
				t.Errorf("convertDeferrableMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPrimaryKeyConstraint_ValidateUpdate_NoPrimaryKey tests update on table without PK
func TestPrimaryKeyConstraint_ValidateUpdate_NoPrimaryKey(t *testing.T) {
	columns := []*schema.Column{
		{Name: "name", Type: "TEXT"},
		{Name: "age", Type: "INTEGER"},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	table := &schema.Table{
		Name:       "test",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: []string{},
	}

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	newValues := map[string]interface{}{"name": "Updated"}

	err := pk.ValidateUpdate(10, newValues)
	if err != nil {
		t.Errorf("ValidateUpdate should succeed for table without PK: %v", err)
	}
}

// TestValuesEqual_EdgeCases tests edge cases in value comparison
func TestValuesEqual_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		v1   interface{}
		v2   interface{}
		want bool
	}{
		{"nil vs nil", nil, nil, true},
		{"nil vs value", nil, 42, false},
		{"value vs nil", 42, nil, false},
		{"different types", 42, "42", false},
		{"int vs float", int(42), float64(42), false},
		{"empty byte slices", []byte{}, []byte{}, true},
		{"byte slice vs nil", []byte{1, 2}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valuesEqual(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}
