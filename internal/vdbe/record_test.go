package vdbe

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{
			name:   "empty record",
			values: []interface{}{},
		},
		{
			name:   "single NULL",
			values: []interface{}{nil},
		},
		{
			name:   "integer 0",
			values: []interface{}{int64(0)},
		},
		{
			name:   "integer 1",
			values: []interface{}{int64(1)},
		},
		{
			name:   "int8",
			values: []interface{}{int64(42)},
		},
		{
			name:   "int8 negative",
			values: []interface{}{int64(-100)},
		},
		{
			name:   "int16",
			values: []interface{}{int64(1000)},
		},
		{
			name:   "int32",
			values: []interface{}{int64(100000)},
		},
		{
			name:   "int64",
			values: []interface{}{int64(9223372036854775807)},
		},
		{
			name:   "float64",
			values: []interface{}{3.14159},
		},
		{
			name:   "text",
			values: []interface{}{"Hello, World!"},
		},
		{
			name:   "blob",
			values: []interface{}{[]byte{0x01, 0x02, 0x03, 0x04}},
		},
		{
			name: "mixed types",
			values: []interface{}{
				int64(42),
				"test string",
				nil,
				3.14159,
				[]byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := encodeSimpleRecord(tt.values)

			// Decode
			decoded, err := decodeRecord(encoded)
			if err != nil {
				t.Fatalf("decodeRecord failed: %v", err)
			}

			// Verify length matches
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded length %d != original length %d", len(decoded), len(tt.values))
			}

			// Verify each value
			for i, original := range tt.values {
				result := decoded[i]

				// Handle type comparisons
				switch orig := original.(type) {
				case nil:
					if result != nil {
						t.Errorf("value[%d]: expected nil, got %v", i, result)
					}

				case int64:
					if res, ok := result.(int64); !ok {
						t.Errorf("value[%d]: expected int64, got %T", i, result)
					} else if res != orig {
						t.Errorf("value[%d]: expected %d, got %d", i, orig, res)
					}

				case float64:
					if res, ok := result.(float64); !ok {
						t.Errorf("value[%d]: expected float64, got %T", i, result)
					} else if res != orig {
						t.Errorf("value[%d]: expected %f, got %f", i, orig, res)
					}

				case string:
					if res, ok := result.(string); !ok {
						t.Errorf("value[%d]: expected string, got %T", i, result)
					} else if res != orig {
						t.Errorf("value[%d]: expected %q, got %q", i, orig, res)
					}

				case []byte:
					if res, ok := result.([]byte); !ok {
						t.Errorf("value[%d]: expected []byte, got %T", i, result)
					} else if !bytes.Equal(res, orig) {
						t.Errorf("value[%d]: expected %v, got %v", i, orig, res)
					}

				default:
					t.Errorf("value[%d]: unsupported type %T", i, original)
				}
			}
		})
	}
}

func TestSerialTypes(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
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
