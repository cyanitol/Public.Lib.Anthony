// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"bytes"
	"testing"
)

// compareDecodedValue checks if a decoded value matches the original.
func compareDecodedValue(t *testing.T, i int, original, result interface{}) {
	t.Helper()
	if original == nil {
		if result != nil {
			t.Errorf("value[%d]: expected nil, got %v", i, result)
		}
		return
	}
	compareDecodedNonNil(t, i, original, result)
}

func compareDecodedNonNil(t *testing.T, i int, original, result interface{}) {
	t.Helper()
	if !decodedValuesMatch(original, result) {
		t.Errorf("value[%d]: expected %v (%T), got %v (%T)", i, original, original, result, result)
	}
}

func decodedValuesMatch(original, result interface{}) bool {
	switch orig := original.(type) {
	case int64:
		return decodedInt64Match(orig, result)
	case float64:
		return decodedFloat64Match(orig, result)
	case string:
		res, ok := result.(string)
		return ok && res == orig
	case []byte:
		res, ok := result.([]byte)
		return ok && bytes.Equal(res, orig)
	}
	return false
}

func decodedInt64Match(orig int64, result interface{}) bool {
	res, ok := result.(int64)
	return ok && res == orig
}

func decodedFloat64Match(orig float64, result interface{}) bool {
	res, ok := result.(float64)
	return ok && res == orig
}

func TestEncodeDecodeRecord(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		values []interface{}
	}{
		{"empty record", []interface{}{}},
		{"single NULL", []interface{}{nil}},
		{"integer 0", []interface{}{int64(0)}},
		{"integer 1", []interface{}{int64(1)}},
		{"int8", []interface{}{int64(42)}},
		{"int8 negative", []interface{}{int64(-100)}},
		{"int16", []interface{}{int64(1000)}},
		{"int32", []interface{}{int64(100000)}},
		{"int64", []interface{}{int64(9223372036854775807)}},
		{"float64", []interface{}{3.14159}},
		{"text", []interface{}{"Hello, World!"}},
		{"blob", []interface{}{[]byte{0x01, 0x02, 0x03, 0x04}}},
		{"mixed types", []interface{}{int64(42), "test string", nil, 3.14159, []byte{0xDE, 0xAD, 0xBE, 0xEF}}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded := encodeSimpleRecord(tt.values)
			decoded, err := decodeRecord(encoded)
			if err != nil {
				t.Fatalf("decodeRecord failed: %v", err)
			}
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded length %d != original length %d", len(decoded), len(tt.values))
			}
			for i, original := range tt.values {
				compareDecodedValue(t, i, original, decoded[i])
			}
		})
	}
}

func TestSerialTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		value      interface{}
		serialType uint64
	}{
		{"NULL", nil, 0},
		{"int 0", int64(0), 8},
		{"int 1", int64(1), 9},
		{"int8 positive", int64(100), 1},
		{"int8 negative", int64(-100), 1},
		{"int16", int64(1000), 2},
		{"int24", int64(100000), 3},   // 100000 fits in 24 bits
		{"int32", int64(10000000), 4}, // 10000000 needs 32 bits
		{"int64", int64(10000000000), 6},
		{"float64", float64(3.14), 7},
		{"text empty", "", 13},                   // 13 + 2*0
		{"text hello", "hello", 23},              // 13 + 2*5
		{"blob empty", []byte{}, 12},             // 12 + 2*0
		{"blob 4 bytes", []byte{1, 2, 3, 4}, 20}, // 12 + 2*4
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Encode to get serial type
			encoded := encodeSimpleRecord([]interface{}{tt.value})

			// Decode header to verify serial type
			if len(encoded) == 0 {
				t.Fatal("encoded record is empty")
			}

			headerSize, n := getVarint(encoded, 0)
			if n == 0 {
				t.Fatal("failed to read header size")
			}

			serialType, n2 := getVarint(encoded, n)
			if n2 == 0 {
				t.Fatal("failed to read serial type")
			}

			if serialType != tt.serialType {
				t.Errorf("expected serial type %d, got %d", tt.serialType, serialType)
			}

			_ = headerSize // unused but shows we read it
		})
	}
}
