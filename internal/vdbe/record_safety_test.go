package vdbe

import (
	"bytes"
	"errors"
	"testing"
)

// TestDecodeInt24ValueBufferOverflow tests bounds checking for 24-bit integer decoding
func TestDecodeInt24ValueBufferOverflow(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		offset    int
		expectErr bool
	}{
		{
			name:      "valid 24-bit data",
			data:      []byte{0x12, 0x34, 0x56},
			offset:    0,
			expectErr: false,
		},
		{
			name:      "truncated to 2 bytes",
			data:      []byte{0x12, 0x34},
			offset:    0,
			expectErr: true,
		},
		{
			name:      "truncated to 1 byte",
			data:      []byte{0x12},
			offset:    0,
			expectErr: true,
		},
		{
			name:      "empty buffer",
			data:      []byte{},
			offset:    0,
			expectErr: true,
		},
		{
			name:      "offset too large",
			data:      []byte{0x12, 0x34, 0x56},
			offset:    1,
			expectErr: true,
		},
		{
			name:      "negative offset",
			data:      []byte{0x12, 0x34, 0x56},
			offset:    -1,
			expectErr: true,
		},
		{
			name:      "valid with offset",
			data:      []byte{0x00, 0x12, 0x34, 0x56},
			offset:    1,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeInt24Value(tt.data, tt.offset)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
				if !errors.Is(err, ErrBufferOverflow) {
					t.Errorf("expected ErrBufferOverflow but got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestDecodeInt48ValueBufferOverflow tests bounds checking for 48-bit integer decoding
func TestDecodeInt48ValueBufferOverflow(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		offset    int
		expectErr bool
	}{
		{
			name:      "valid 48-bit data",
			data:      []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC},
			offset:    0,
			expectErr: false,
		},
		{
			name:      "truncated to 5 bytes",
			data:      []byte{0x12, 0x34, 0x56, 0x78, 0x9A},
			offset:    0,
			expectErr: true,
		},
		{
			name:      "truncated to 3 bytes",
			data:      []byte{0x12, 0x34, 0x56},
			offset:    0,
			expectErr: true,
		},
		{
			name:      "empty buffer",
			data:      []byte{},
			offset:    0,
			expectErr: true,
		},
		{
			name:      "offset too large",
			data:      []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC},
			offset:    1,
			expectErr: true,
		},
		{
			name:      "negative offset",
			data:      []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC},
			offset:    -1,
			expectErr: true,
		},
		{
			name:      "valid with offset",
			data:      []byte{0x00, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC},
			offset:    2,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeInt48Value(tt.data, tt.offset)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
				if !errors.Is(err, ErrBufferOverflow) {
					t.Errorf("expected ErrBufferOverflow but got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestDecodeInt24ValueCorrectness tests that valid 24-bit values decode correctly
func TestDecodeInt24ValueCorrectness(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		offset   int
		expected int64
	}{
		{
			name:     "zero",
			data:     []byte{0x00, 0x00, 0x00},
			offset:   0,
			expected: 0,
		},
		{
			name:     "positive value",
			data:     []byte{0x00, 0x00, 0x42},
			offset:   0,
			expected: 66,
		},
		{
			name:     "max positive 24-bit",
			data:     []byte{0x7F, 0xFF, 0xFF},
			offset:   0,
			expected: 8388607,
		},
		{
			name:     "negative value (sign extension)",
			data:     []byte{0xFF, 0xFF, 0xFF},
			offset:   0,
			expected: -1,
		},
		{
			name:     "min negative 24-bit",
			data:     []byte{0x80, 0x00, 0x00},
			offset:   0,
			expected: -8388608,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeInt24Value(tt.data, tt.offset)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected %d but got %d", tt.expected, result)
			}
		})
	}
}

// TestDecodeInt48ValueCorrectness tests that valid 48-bit values decode correctly
func TestDecodeInt48ValueCorrectness(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		offset   int
		expected int64
	}{
		{
			name:     "zero",
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			offset:   0,
			expected: 0,
		},
		{
			name:     "positive value",
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
			offset:   0,
			expected: 256,
		},
		{
			name:     "max positive 48-bit",
			data:     []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			offset:   0,
			expected: 140737488355327,
		},
		{
			name:     "negative value (sign extension)",
			data:     []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			offset:   0,
			expected: -1,
		},
		{
			name:     "min negative 48-bit",
			data:     []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00},
			offset:   0,
			expected: -140737488355328,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeInt48Value(tt.data, tt.offset)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected %d but got %d", tt.expected, result)
			}
		})
	}
}

// TestDecodeRecordMaxSize tests that records exceeding MaxRecordSize are rejected
func TestDecodeRecordMaxSize(t *testing.T) {
	t.Run("empty record", func(t *testing.T) {
		_, err := decodeRecord([]byte{})
		if err == nil {
			t.Error("expected error for empty record")
		}
	})

	t.Run("record at max size", func(t *testing.T) {
		// Create a large but valid header
		data := make([]byte, MaxRecordSize)
		// Header size = 2 (varint encoding of small number)
		data[0] = 0x02
		// Serial type for NULL
		data[1] = 0x00

		_, err := decodeRecord(data)
		// This may or may not succeed depending on actual content,
		// but it should not trigger the size error
		if err != nil && errors.Is(err, ErrRecordTooLarge) {
			t.Error("record at max size should not trigger size error")
		}
	})

	t.Run("record exceeding max size", func(t *testing.T) {
		// Create a record that's too large
		data := make([]byte, MaxRecordSize+1)
		data[0] = 0x02
		data[1] = 0x00

		_, err := decodeRecord(data)
		if err == nil {
			t.Error("expected error for oversized record")
		}
		if !errors.Is(err, ErrRecordTooLarge) {
			t.Errorf("expected ErrRecordTooLarge but got: %v", err)
		}
	})
}

// TestDecodeRecordTruncated tests decoding with truncated data
func TestDecodeRecordTruncated(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "truncated header",
			data: []byte{0x80}, // Incomplete varint
		},
		{
			name: "truncated serial type",
			data: []byte{0x03, 0x80}, // Header says 3 bytes, but serial type is incomplete
		},
		{
			name: "truncated int24 value",
			data: []byte{0x03, 0x03, 0x12}, // Serial type 3 (int24) but only 1 byte of data
		},
		{
			name: "truncated int48 value",
			data: []byte{0x03, 0x05, 0x12, 0x34}, // Serial type 5 (int48) but only 2 bytes of data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeRecord(tt.data)
			if err == nil {
				t.Error("expected error for truncated data")
			}
		})
	}
}

// TestDecodeRecordIntEdgeCases tests integer decoding edge cases
func TestDecodeRecordIntEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		minBytes int // Minimum bytes needed to encode this value
	}{
		{"int8 max", 127, 1},
		{"int8 min", -128, 1},
		{"int16 max", 32767, 2},
		{"int16 min", -32768, 2},
		{"int24 max", 8388607, 3},
		{"int24 min", -8388608, 3},
		{"int32 max", 2147483647, 4},
		{"int32 min", -2147483648, 4},
		{"int48 max", 140737488355327, 6},
		{"int48 min", -140737488355328, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the value
			encoded := encodeSimpleRecord([]interface{}{tt.value})

			// Decode it back
			decoded, err := decodeRecord(encoded)
			if err != nil {
				t.Fatalf("decoding failed: %v", err)
			}

			if len(decoded) != 1 {
				t.Fatalf("expected 1 value, got %d", len(decoded))
			}

			result, ok := decoded[0].(int64)
			if !ok {
				t.Fatalf("expected int64, got %T", decoded[0])
			}

			if result != tt.value {
				t.Errorf("expected %d, got %d", tt.value, result)
			}
		})
	}
}

// TestDecodeRecordOffsetEdgeCases tests offset validation
func TestDecodeRecordOffsetEdgeCases(t *testing.T) {
	// Create a valid record with multiple values
	values := []interface{}{
		int64(100),   // serial type 1 (1 byte)
		int64(1000),  // serial type 2 (2 bytes)
		int64(100000), // serial type 3 (3 bytes)
	}

	encoded := encodeSimpleRecord(values)

	// Test that normal decoding works
	decoded, err := decodeRecord(encoded)
	if err != nil {
		t.Fatalf("decoding failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Errorf("expected %d values, got %d", len(values), len(decoded))
	}

	// Verify all values match
	for i := range values {
		if v, ok := decoded[i].(int64); ok {
			if v != values[i].(int64) {
				t.Errorf("value[%d]: expected %v, got %v", i, values[i], v)
			}
		}
	}
}

// TestDecodeRecordBlobAndText tests blob and text handling with various sizes
func TestDecodeRecordBlobAndText(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"empty text", ""},
		{"empty blob", []byte{}},
		{"small text", "hello"},
		{"small blob", []byte{1, 2, 3, 4, 5}},
		{"medium text", bytes.Repeat([]byte("test"), 100)},
		{"medium blob", bytes.Repeat([]byte{0xAB}, 256)},
		{"large text", bytes.Repeat([]byte("large"), 1000)},
		{"large blob", bytes.Repeat([]byte{0xFF}, 1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeSimpleRecord([]interface{}{tt.value})
			decoded, err := decodeRecord(encoded)
			if err != nil {
				t.Fatalf("decoding failed: %v", err)
			}

			if len(decoded) != 1 {
				t.Fatalf("expected 1 value, got %d", len(decoded))
			}

			switch expected := tt.value.(type) {
			case string:
				result, ok := decoded[0].(string)
				if !ok {
					t.Fatalf("expected string, got %T", decoded[0])
				}
				if result != expected {
					t.Errorf("text mismatch: expected length %d, got %d", len(expected), len(result))
				}
			case []byte:
				result, ok := decoded[0].([]byte)
				if !ok {
					t.Fatalf("expected []byte, got %T", decoded[0])
				}
				if !bytes.Equal(result, expected) {
					t.Errorf("blob mismatch: expected length %d, got %d", len(expected), len(result))
				}
			}
		})
	}
}
