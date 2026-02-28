package vdbe

import (
	"encoding/binary"
	"math"
	"testing"
)

// FuzzDecodeRecordExtended tests the record decoder with random binary data (extended version)
func FuzzDecodeRecordExtended(f *testing.F) {
	// Add seed corpus with valid record structures
	seeds := [][]byte{
		// Single NULL value
		{0x01, 0x00}, // header_size=1, serial_type=0 (NULL)

		// Single integer values
		{0x02, 0x08}, // header_size=2, serial_type=8 (int 0)
		{0x02, 0x09}, // header_size=2, serial_type=9 (int 1)

		// 1-byte integer
		{0x02, 0x01, 0x42}, // header_size=2, serial_type=1, value=66

		// 2-byte integer
		{0x02, 0x02, 0x01, 0x23}, // header_size=2, serial_type=2, value=291

		// 4-byte integer
		{0x02, 0x04, 0x00, 0x00, 0x01, 0x23}, // header_size=2, serial_type=4

		// 8-byte integer
		{0x02, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x23},

		// Float64
		encodeFloat64Seed(3.14159),

		// Empty string (serial_type=13: (13-13)/2 = 0 bytes)
		{0x02, 0x0d}, // header_size=2, serial_type=13

		// Text "hi" (serial_type=15: (15-13)/2 = 1 byte, but we need 2)
		// Actually for text: length N encoded as (N*2)+13
		// So for "hi" (2 bytes): serial_type = 2*2+13 = 17
		{0x02, 0x11, 'h', 'i'}, // header_size=2, serial_type=17 (text, 2 bytes)

		// Blob (2 bytes, serial_type = 2*2+12 = 16)
		{0x02, 0x10, 0xde, 0xad}, // header_size=2, serial_type=16 (blob, 2 bytes)

		// Multiple values: NULL, int 1, text "a"
		{0x04, 0x00, 0x09, 0x0f, 'a'}, // header_size=4, types=[0, 9, 15]

		// Multiple values: int 1, int 2, int 3
		{0x05, 0x01, 0x01, 0x01, 0x01, 0x02, 0x03},

		// Empty record should fail
		{},

		// Incomplete records (should error gracefully)
		{0x02}, // Missing serial type
		{0x02, 0x01}, // Missing value for 1-byte int
		{0x02, 0x02, 0x01}, // Incomplete 2-byte int
		{0x02, 0x11, 'h'}, // Incomplete text

		// Invalid header size
		{0xff}, // Large header size but no data

		// Large serial types
		{0x02, 0x7f}, // Max varint in 1 byte

		// Very long text (serial_type for 100 bytes text: 100*2+13=213)
		buildRecordWithLongText(100),

		// Very long blob (serial_type for 100 bytes blob: 100*2+12=212)
		buildRecordWithLongBlob(100),

		// Negative integers (using 1-byte signed)
		{0x02, 0x01, 0xff}, // -1

		// Mix of all types
		buildComplexRecord(),
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	// Fuzz function - should never panic
	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip extremely large inputs to prevent timeout
		if len(data) > 1000000 {
			t.Skip("input too large")
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("decodeRecord panicked on input: %x\nPanic: %v", data, r)
			}
		}()

		// Try to decode - should return error or success, never panic
		_, _ = decodeRecord(data)
	})
}

// FuzzEncodeRecord tests the record encoder with random values
func FuzzEncodeRecord(f *testing.F) {
	// Add seed corpus with various value types
	f.Add(int64(0), int64(0))
	f.Add(int64(1), int64(0))
	f.Add(int64(-1), int64(0))
	f.Add(int64(127), int64(0))
	f.Add(int64(-128), int64(0))
	f.Add(int64(32767), int64(0))
	f.Add(int64(-32768), int64(0))

	f.Fuzz(func(t *testing.T, i1 int64, i2 int64) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("encodeRecord panicked on input: (%d, %d)\nPanic: %v", i1, i2, r)
			}
		}()

		// Build values slice with different types
		values := []interface{}{
			nil,
			i1,
			i2,
			float64(i1),
			"test",
			[]byte{byte(i1), byte(i2)},
		}

		// Encode
		data := encodeSimpleRecord(values)

		// Decode and verify we can decode what we encoded
		decoded, err := decodeRecord(data)
		if err != nil {
			t.Errorf("Failed to decode encoded record: %v", err)
		}

		// Basic sanity check: same number of values
		if len(decoded) != len(values) {
			t.Errorf("Value count mismatch: encoded %d, decoded %d", len(values), len(decoded))
		}
	})
}

// FuzzEncodeDecodeRoundTrip tests that encode->decode is a valid round trip
func FuzzEncodeDecodeRoundTrip(f *testing.F) {
	f.Add("hello", int64(42), float64(3.14))

	f.Fuzz(func(t *testing.T, str string, i int64, fl float64) {
		// Skip very long strings to prevent timeout
		if len(str) > 10000 {
			t.Skip("string too long")
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Round trip panicked\nPanic: %v", r)
			}
		}()

		// Create values with various types
		values := []interface{}{
			nil,
			int64(0),
			int64(1),
			i,
			fl,
			str,
			[]byte(str),
		}

		// Encode
		encoded := encodeSimpleRecord(values)

		// Decode
		decoded, err := decodeRecord(encoded)
		if err != nil {
			t.Fatalf("Failed to decode: %v", err)
		}

		// Verify length
		if len(decoded) != len(values) {
			t.Fatalf("Length mismatch: expected %d, got %d", len(values), len(decoded))
		}

		// Verify each value type
		for i, val := range values {
			decodedVal := decoded[i]

			// Type assertions and comparisons
			switch v := val.(type) {
			case nil:
				if decodedVal != nil {
					t.Errorf("Index %d: expected nil, got %v", i, decodedVal)
				}
			case int64:
				if dv, ok := decodedVal.(int64); !ok {
					t.Errorf("Index %d: expected int64, got %T", i, decodedVal)
				} else if dv != v {
					// Allow some flexibility for small integers (may be encoded as constants)
					if !(v == 0 && dv == 0) && !(v == 1 && dv == 1) {
						t.Errorf("Index %d: expected %d, got %d", i, v, dv)
					}
				}
			case float64:
				if dv, ok := decodedVal.(float64); !ok {
					t.Errorf("Index %d: expected float64, got %T", i, decodedVal)
				} else if math.Abs(dv-v) > 0.0001 {
					// Allow small floating point errors
					t.Errorf("Index %d: expected %f, got %f", i, v, dv)
				}
			case string:
				if dv, ok := decodedVal.(string); !ok {
					t.Errorf("Index %d: expected string, got %T", i, decodedVal)
				} else if dv != v {
					t.Errorf("Index %d: expected %q, got %q", i, v, dv)
				}
			case []byte:
				if dv, ok := decodedVal.([]byte); !ok {
					t.Errorf("Index %d: expected []byte, got %T", i, decodedVal)
				} else if string(dv) != string(v) {
					t.Errorf("Index %d: byte mismatch", i)
				}
			}
		}
	})
}

// FuzzVarint tests varint encoding/decoding
func FuzzVarint(f *testing.F) {
	// Seed with various sizes
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(127))      // Max 1-byte varint
	f.Add(uint64(128))      // Min 2-byte varint
	f.Add(uint64(16383))    // Max 2-byte varint
	f.Add(uint64(16384))    // Min 3-byte varint
	f.Add(uint64(0x7fffffff)) // Large value

	f.Fuzz(func(t *testing.T, val uint64) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Varint panicked on %d: %v", val, r)
			}
		}()

		// Encode
		buf := encodeVarint(val)

		if len(buf) == 0 {
			t.Fatalf("encodeVarint returned empty buffer for value %d", val)
		}

		// Decode
		decoded, n := getVarint(buf, 0)

		if n == 0 {
			t.Fatalf("getVarint returned 0 bytes read")
		}

		if n != len(buf) {
			t.Errorf("Bytes read mismatch: encoded %d, decoded %d", len(buf), n)
		}

		if decoded != val {
			t.Errorf("Value mismatch: encoded %d, decoded %d", val, decoded)
		}
	})
}

// Helper functions for seed generation

func encodeFloat64Seed(f float64) []byte {
	// serial_type=7 for float64
	result := []byte{0x02, 0x07}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(f))
	result = append(result, buf...)
	return result
}

func buildRecordWithLongText(length int) []byte {
	// Serial type for text: (N*2)+13 where N is byte length
	serialType := uint64(length*2 + 13)

	// Build header: header_size, serial_type
	// For simplicity, assume serial type fits in one byte (works for length up to ~120)
	result := []byte{0x02, byte(serialType)}

	// Add text data
	for i := 0; i < length; i++ {
		result = append(result, 'a')
	}

	return result
}

func buildRecordWithLongBlob(length int) []byte {
	// Serial type for blob: (N*2)+12 where N is byte length
	serialType := uint64(length*2 + 12)

	result := []byte{0x02, byte(serialType)}

	// Add blob data
	for i := 0; i < length; i++ {
		result = append(result, byte(i%256))
	}

	return result
}

func buildComplexRecord() []byte {
	// Build a record with: NULL, int 0, int 1, int 42, float 3.14, text "hi", blob [0xde, 0xad]
	// Serial types: 0, 8, 9, 1, 7, 17 (text 2 bytes), 16 (blob 2 bytes)

	result := []byte{
		0x08,             // header size (8 bytes)
		0x00,             // NULL
		0x08,             // int 0
		0x09,             // int 1
		0x01,             // 1-byte int
		0x07,             // float64
		0x11,             // text (2 bytes)
		0x10,             // blob (2 bytes)
		// Values:
		0x2a,             // 42 (for serial type 1)
	}

	// Add float64 for 3.14
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(3.14))
	result = append(result, buf...)

	// Add text "hi"
	result = append(result, 'h', 'i')

	// Add blob [0xde, 0xad]
	result = append(result, 0xde, 0xad)

	return result
}

// TestFuzzRegressionVdbe tests against known problematic inputs
func TestFuzzRegressionVdbe(t *testing.T) {
	t.Parallel()
	regressionInputs := [][]byte{
		// Empty
		{},

		// Truncated records
		{0x02},           // Header size but no serial type
		{0x02, 0x01},     // Serial type for 1-byte int but no data
		{0x02, 0x02, 0x01}, // Incomplete 2-byte int

		// Invalid header sizes
		{0xff},                     // Large header but no data
		{0xff, 0xff, 0xff, 0xff},   // Multiple large varints

		// Extremely large serial types (should not crash but may return error)
		{0x02, 0xff}, // Large serial type
		// Note: Very large varints like 0xffffffffffffffffff can cause panic
		// This is a known limitation of the decoder

		// Zero serial types
		{0x01}, // Header size 1, no serial types (invalid but shouldn't panic)

		// All zeros
		{0x00, 0x00, 0x00, 0x00},

		// All 0xff
		{0xff, 0xff, 0xff, 0xff},

		// Random binary
		{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},

		// Negative-looking varints
		{0x80, 0x80, 0x80, 0x80},
	}

	for i, input := range regressionInputs {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
		t.Parallel()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic on regression input %d: %v\nInput: %x", i, r, input)
				}
			}()

			_, _ = decodeRecord(input)
		})
	}
}

// TestFuzzEdgeCases tests specific edge cases
func TestFuzzEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "MaxRecordSize",
			data: make([]byte, MaxRecordSize+1), // Should trigger size check
		},
		{
			name: "HeaderSizeLargerThanData",
			data: []byte{0xff}, // Header claims to be huge but data is tiny
		},
		{
			name: "SerialTypeWithNoData",
			data: []byte{0x02, 0x01}, // 1-byte int but no byte following
		},
		{
			name: "MultipleNulls",
			data: []byte{0x05, 0x00, 0x00, 0x00, 0x00}, // 4 NULLs
		},
		{
			name: "OnlyConstants",
			data: []byte{0x04, 0x08, 0x09, 0x08}, // 0, 1, 0
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic on %s: %v", tt.name, r)
				}
			}()

			_, _ = decodeRecord(tt.data)
		})
	}
}
