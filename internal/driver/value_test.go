package driver

import (
	"reflect"
	"testing"
	"time"
)

func TestConvertValue(t *testing.T) {
	vc := ValueConverter{}

	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
		wantErr  bool
	}{
		{"nil", nil, nil, false},
		{"int64", int64(42), int64(42), false},
		{"float64", float64(3.14), float64(3.14), false},
		{"bool", true, true, false},
		{"string", "hello", "hello", false},
		{"[]byte", []byte("data"), []byte("data"), false},
		{"time.Time", time.Now(), time.Now(), false},
		{"int", int(42), int64(42), false},
		{"int8", int8(42), int64(42), false},
		{"int16", int16(42), int64(42), false},
		{"int32", int32(42), int64(42), false},
		{"uint", uint(42), int64(42), false},
		{"uint8", uint8(42), int64(42), false},
		{"uint16", uint16(42), int64(42), false},
		{"uint32", uint32(42), int64(42), false},
		{"uint64 ok", uint64(100), int64(100), false},
		{"uint64 max safe", uint64(1<<63 - 1), int64(1<<63 - 1), false},
		{"uint64 overflow", uint64(1 << 63), nil, true},
		{"uint64 large", uint64(1<<63 + 1), nil, true},
		{"float32", float32(3.14), float64(float32(3.14)), false},
		{"unsupported struct", struct{}{}, nil, true},
		{"unsupported map", map[string]int{"a": 1}, nil, true},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result, err := vc.ConvertValue(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// For time.Time, just check it's not nil
				if _, ok := tt.input.(time.Time); ok {
					if result == nil {
						t.Errorf("ConvertValue() = nil, want time.Time")
					}
					return
				}
				// For byte slices, use DeepEqual
				if reflect.TypeOf(result) == reflect.TypeOf([]byte{}) {
					if !reflect.DeepEqual(result, tt.expected) {
						t.Errorf("ConvertValue() = %v, want %v", result, tt.expected)
					}
					return
				}
				// For other types
				if result != tt.expected {
					t.Errorf("ConvertValue() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestIsNativeDriverValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected bool
	}{
		{"int64", int64(42), true},
		{"float64", float64(3.14), true},
		{"bool", true, true},
		{"string", "hello", true},
		{"[]byte", []byte("data"), true},
		{"time.Time", time.Now(), true},
		{"int", int(42), false},
		{"int32", int32(42), false},
		{"float32", float32(3.14), false},
		{"uint64", uint64(42), false},
		{"struct", struct{}{}, false},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result := isNativeDriverValue(tt.input)
			if result != tt.expected {
				t.Errorf("isNativeDriverValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestConvertUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected int64
		wantErr  bool
	}{
		{"zero", 0, 0, false},
		{"small", 100, 100, false},
		{"max safe", 1<<63 - 1, 1<<63 - 1, false},
		{"overflow", 1 << 63, 0, true},
		{"large overflow", 1<<63 + 100, 0, true},
		{"max uint64", ^uint64(0), 0, true},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertUint64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertUint64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if result != tt.expected {
					t.Errorf("convertUint64() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestConvertToInt64(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		expected  int64
		expectOk  bool
	}{
		{"int", int(42), 42, true},
		{"int8", int8(42), 42, true},
		{"int16", int16(42), 42, true},
		{"int32", int32(42), 42, true},
		{"uint", uint(42), 42, true},
		{"uint8", uint8(42), 42, true},
		{"uint16", uint16(42), 42, true},
		{"uint32", uint32(42), 42, true},
		{"int negative", int(-42), -42, true},
		{"int8 negative", int8(-42), -42, true},
		{"int16 negative", int16(-42), -42, true},
		{"int32 negative", int32(-42), -42, true},
		{"int64", int64(42), 0, false},
		{"uint64", uint64(42), 0, false},
		{"float64", float64(3.14), 0, false},
		{"string", "42", 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result, ok := convertToInt64(tt.input)
			if ok != tt.expectOk {
				t.Errorf("convertToInt64() ok = %v, want %v", ok, tt.expectOk)
				return
			}
			if tt.expectOk && result != tt.expected {
				t.Errorf("convertToInt64() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestResultMethods(t *testing.T) {
	r := &Result{
		lastInsertID: 42,
		rowsAffected: 10,
	}

	// Test LastInsertId
	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId() error = %v", err)
	}
	if id != 42 {
		t.Errorf("LastInsertId() = %d, want 42", id)
	}

	// Test RowsAffected
	rows, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected() error = %v", err)
	}
	if rows != 10 {
		t.Errorf("RowsAffected() = %d, want 10", rows)
	}
}

func TestResultZeroValues(t *testing.T) {
	r := &Result{
		lastInsertID: 0,
		rowsAffected: 0,
	}

	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId() error = %v", err)
	}
	if id != 0 {
		t.Errorf("LastInsertId() = %d, want 0", id)
	}

	rows, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected() error = %v", err)
	}
	if rows != 0 {
		t.Errorf("RowsAffected() = %d, want 0", rows)
	}
}

func TestResultNegativeValues(t *testing.T) {
	r := &Result{
		lastInsertID: -1,
		rowsAffected: -1,
	}

	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId() error = %v", err)
	}
	if id != -1 {
		t.Errorf("LastInsertId() = %d, want -1", id)
	}

	rows, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected() error = %v", err)
	}
	if rows != -1 {
		t.Errorf("RowsAffected() = %d, want -1", rows)
	}
}

func TestSqliteValueConverter(t *testing.T) {
	// Test that the global converter is initialized
	if sqliteValueConverter == (ValueConverter{}) {
		// This is actually fine, as ValueConverter is a zero-size struct
		// Just verify it can be used
		_, err := sqliteValueConverter.ConvertValue(int64(42))
		if err != nil {
			t.Errorf("sqliteValueConverter.ConvertValue() error = %v", err)
		}
	}
}
