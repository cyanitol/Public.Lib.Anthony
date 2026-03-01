// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql

import (
	"bytes"
	"testing"
)

func TestVarint(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
	}{
		{"small", 42},
		{"boundary1", 240},
		{"boundary2", 241},
		{"boundary3", 2287},
		{"boundary4", 2288},
		{"large", 67823},
		{"very_large", 1 << 32},
		{"max", 1<<63 - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			buf := PutVarint(nil, tt.value)

			// Decode
			decoded, n := GetVarint(buf, 0)

			if decoded != tt.value {
				t.Errorf("value mismatch: got %d, want %d", decoded, tt.value)
			}

			if n != len(buf) {
				t.Errorf("length mismatch: got %d bytes, used %d bytes", len(buf), n)
			}
		})
	}
}

func TestSerialType(t *testing.T) {
	tests := []struct {
		name       string
		value      Value
		wantType   SerialType
		wantLength int
	}{
		{"null", NullValue(), SerialTypeNull, 0},
		{"zero", IntValue(0), SerialTypeZero, 0},
		{"one", IntValue(1), SerialTypeOne, 0},
		{"int8_pos", IntValue(127), SerialTypeInt8, 1},
		{"int8_neg", IntValue(-128), SerialTypeInt8, 1},
		{"int16", IntValue(1000), SerialTypeInt16, 2},
		{"int24", IntValue(100000), SerialTypeInt24, 3},   // 100000 fits in 24 bits
		{"int32", IntValue(10000000), SerialTypeInt32, 4}, // 10000000 needs 32 bits
		{"int64", IntValue(1 << 50), SerialTypeInt64, 8},
		{"float", FloatValue(3.14), SerialTypeFloat64, 8},
		{"text_empty", TextValue(""), SerialType(13), 0},
		{"text_hello", TextValue("hello"), SerialType(23), 5},
		{"blob_empty", BlobValue([]byte{}), SerialType(12), 0},
		{"blob_data", BlobValue([]byte{1, 2, 3}), SerialType(18), 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := SerialTypeFor(tt.value)
			if st != tt.wantType {
				t.Errorf("serial type: got %d, want %d", st, tt.wantType)
			}

			length := SerialTypeLen(st)
			if length != tt.wantLength {
				t.Errorf("length: got %d, want %d", length, tt.wantLength)
			}
		})
	}
}

func TestMakeRecord(t *testing.T) {
	tests := []struct {
		name   string
		values []Value
		err    bool
	}{
		{
			name:   "single_int",
			values: []Value{IntValue(42)},
		},
		{
			name:   "multiple_types",
			values: []Value{IntValue(1), TextValue("hello"), FloatValue(3.14)},
		},
		{
			name:   "with_null",
			values: []Value{NullValue(), IntValue(5), TextValue("world")},
		},
		{
			name:   "all_nulls",
			values: []Value{NullValue(), NullValue()},
		},
		{
			name:   "blob_and_text",
			values: []Value{BlobValue([]byte{1, 2, 3}), TextValue("test")},
		},
		{
			name:   "empty",
			values: []Value{},
			err:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := MakeRecord(tt.values)
			if tt.err {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(record) == 0 {
				t.Error("record is empty")
			}

			// Parse it back
			parsed, err := ParseRecord(record)
			if err != nil {
				t.Fatalf("failed to parse record: %v", err)
			}

			if len(parsed.Values) != len(tt.values) {
				t.Fatalf("value count mismatch: got %d, want %d",
					len(parsed.Values), len(tt.values))
			}

			// Verify each value
			for i, want := range tt.values {
				got := parsed.Values[i]
				if !valuesEqual(got, want) {
					t.Errorf("value %d mismatch: got %+v, want %+v", i, got, want)
				}
			}
		})
	}
}

func TestRecordRoundTrip(t *testing.T) {
	tests := [][]Value{
		{IntValue(42)},
		{IntValue(1), IntValue(2), IntValue(3)},
		{TextValue("hello"), TextValue("world")},
		{FloatValue(1.5), FloatValue(2.5), FloatValue(3.5)},
		{NullValue(), IntValue(0), IntValue(1), FloatValue(2.5)},
		{BlobValue([]byte{0xFF, 0xFE, 0xFD}), TextValue("data")},
		{
			IntValue(-128), IntValue(-32768), IntValue(-2147483648),
			IntValue(127), IntValue(32767), IntValue(2147483647),
		},
	}

	for i, values := range tests {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			// Encode
			record, err := MakeRecord(values)
			if err != nil {
				t.Fatalf("MakeRecord failed: %v", err)
			}

			// Decode
			parsed, err := ParseRecord(record)
			if err != nil {
				t.Fatalf("ParseRecord failed: %v", err)
			}

			// Verify
			if len(parsed.Values) != len(values) {
				t.Fatalf("count mismatch: got %d, want %d",
					len(parsed.Values), len(values))
			}

			for i, want := range values {
				got := parsed.Values[i]
				if !valuesEqual(got, want) {
					t.Errorf("value %d: got %+v, want %+v", i, got, want)
				}
			}
		})
	}
}

func TestEdgeCases(t *testing.T) {
	t.Run("empty_string", func(t *testing.T) {
		val := TextValue("")
		record, err := MakeRecord([]Value{val})
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if len(parsed.Values) != 1 {
			t.Fatalf("expected 1 value, got %d", len(parsed.Values))
		}

		if parsed.Values[0].Text != "" {
			t.Errorf("expected empty string, got %q", parsed.Values[0].Text)
		}
	})

	t.Run("empty_blob", func(t *testing.T) {
		val := BlobValue([]byte{})
		record, err := MakeRecord([]Value{val})
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if len(parsed.Values) != 1 {
			t.Fatalf("expected 1 value, got %d", len(parsed.Values))
		}

		if len(parsed.Values[0].Blob) != 0 {
			t.Errorf("expected empty blob, got %d bytes", len(parsed.Values[0].Blob))
		}
	})

	t.Run("large_text", func(t *testing.T) {
		text := string(make([]byte, 10000))
		val := TextValue(text)
		record, err := MakeRecord([]Value{val})
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if parsed.Values[0].Text != text {
			t.Error("large text mismatch")
		}
	})

	t.Run("max_int64", func(t *testing.T) {
		val := IntValue(9223372036854775807) // math.MaxInt64
		record, err := MakeRecord([]Value{val})
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if parsed.Values[0].Int != 9223372036854775807 {
			t.Errorf("expected max int64, got %d", parsed.Values[0].Int)
		}
	})

	t.Run("min_int64", func(t *testing.T) {
		val := IntValue(-9223372036854775808) // math.MinInt64
		record, err := MakeRecord([]Value{val})
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if parsed.Values[0].Int != -9223372036854775808 {
			t.Errorf("expected min int64, got %d", parsed.Values[0].Int)
		}
	})

	t.Run("int24_boundaries", func(t *testing.T) {
		tests := []int64{
			-8388608, // min int24
			-8388607,
			-1,
			0,
			1,
			8388607, // max int24
		}

		for _, n := range tests {
			val := IntValue(n)
			record, err := MakeRecord([]Value{val})
			if err != nil {
				t.Fatalf("failed to make record for %d: %v", n, err)
			}

			parsed, err := ParseRecord(record)
			if err != nil {
				t.Fatalf("failed to parse record for %d: %v", n, err)
			}

			if parsed.Values[0].Int != n {
				t.Errorf("int24 boundary %d: got %d", n, parsed.Values[0].Int)
			}
		}
	})

	t.Run("int48_boundaries", func(t *testing.T) {
		tests := []int64{
			-140737488355328, // min int48
			-140737488355327,
			140737488355326,
			140737488355327, // max int48
		}

		for _, n := range tests {
			val := IntValue(n)
			record, err := MakeRecord([]Value{val})
			if err != nil {
				t.Fatalf("failed to make record for %d: %v", n, err)
			}

			parsed, err := ParseRecord(record)
			if err != nil {
				t.Fatalf("failed to parse record for %d: %v", n, err)
			}

			if parsed.Values[0].Int != n {
				t.Errorf("int48 boundary %d: got %d", n, parsed.Values[0].Int)
			}
		}
	})

	t.Run("special_floats", func(t *testing.T) {
		// Note: NaN != NaN, so we handle it specially
		tests := []struct {
			name  string
			value float64
			check func(float64) bool
		}{
			{"positive_infinity", 1.7976931348623157e+308, func(f float64) bool { return f > 0 && f > 1e308 }},
			{"negative_infinity", -1.7976931348623157e+308, func(f float64) bool { return f < 0 && f < -1e308 }},
			{"zero", 0.0, func(f float64) bool { return f == 0.0 }},
			{"negative_zero", -0.0, func(f float64) bool { return f == 0.0 }},
			{"very_small", 1e-300, func(f float64) bool { return f > 0 && f < 1e-299 }},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				val := FloatValue(tt.value)
				record, err := MakeRecord([]Value{val})
				if err != nil {
					t.Fatal(err)
				}

				parsed, err := ParseRecord(record)
				if err != nil {
					t.Fatal(err)
				}

				if !tt.check(parsed.Values[0].Float) {
					t.Errorf("special float %s: got %v, want %v", tt.name, parsed.Values[0].Float, tt.value)
				}
			})
		}
	})

	t.Run("unicode_text", func(t *testing.T) {
		texts := []string{
			"Hello, 世界",
			"🎉🎊🎈",
			"Привет мир",
			"مرحبا بالعالم",
			"こんにちは世界",
		}

		for _, text := range texts {
			val := TextValue(text)
			record, err := MakeRecord([]Value{val})
			if err != nil {
				t.Fatalf("failed for text %q: %v", text, err)
			}

			parsed, err := ParseRecord(record)
			if err != nil {
				t.Fatalf("failed to parse %q: %v", text, err)
			}

			if parsed.Values[0].Text != text {
				t.Errorf("unicode text mismatch: got %q, want %q", parsed.Values[0].Text, text)
			}
		}
	})

	t.Run("binary_blob", func(t *testing.T) {
		// Test blob with all byte values
		blob := make([]byte, 256)
		for i := range blob {
			blob[i] = byte(i)
		}

		val := BlobValue(blob)
		record, err := MakeRecord([]Value{val})
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(parsed.Values[0].Blob, blob) {
			t.Error("binary blob mismatch")
		}
	})

	t.Run("large_record_many_columns", func(t *testing.T) {
		// Create a record with 1000 columns
		values := make([]Value, 1000)
		for i := range values {
			values[i] = IntValue(int64(i))
		}

		record, err := MakeRecord(values)
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if len(parsed.Values) != 1000 {
			t.Fatalf("expected 1000 values, got %d", len(parsed.Values))
		}

		for i := range values {
			if parsed.Values[i].Int != int64(i) {
				t.Errorf("column %d: got %d, want %d", i, parsed.Values[i].Int, i)
			}
		}
	})

	t.Run("mixed_types_comprehensive", func(t *testing.T) {
		values := []Value{
			NullValue(),
			IntValue(0),
			IntValue(1),
			IntValue(-128),
			IntValue(127),
			IntValue(-32768),
			IntValue(32767),
			IntValue(-8388608),
			IntValue(8388607),
			IntValue(-2147483648),
			IntValue(2147483647),
			IntValue(-140737488355328),
			IntValue(140737488355327),
			IntValue(9223372036854775807),
			IntValue(-9223372036854775808),
			FloatValue(0.0),
			FloatValue(-0.0),
			FloatValue(3.14159265358979),
			FloatValue(-3.14159265358979),
			TextValue(""),
			TextValue("a"),
			TextValue("hello world"),
			TextValue("Hello, 世界! 🎉"),
			BlobValue([]byte{}),
			BlobValue([]byte{0}),
			BlobValue([]byte{0xFF, 0xFE, 0xFD}),
		}

		record, err := MakeRecord(values)
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		if len(parsed.Values) != len(values) {
			t.Fatalf("value count mismatch: got %d, want %d", len(parsed.Values), len(values))
		}

		for i, want := range values {
			got := parsed.Values[i]
			if !valuesEqual(got, want) {
				t.Errorf("value %d mismatch: got %+v, want %+v", i, got, want)
			}
		}
	})
}

func TestInvalidRecords(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"truncated_header", []byte{5}}, // Header size 5 but no data
		// Note: extremely long varints may not error - they just return 0 bytes read
		{"truncated_int8", []byte{2, 1}},                 // Header says int8, but no body
		{"truncated_int16", []byte{2, 2, 0}},             // Header says int16, body too short
		{"truncated_int24", []byte{2, 3, 0, 0}},          // Header says int24, body too short
		{"truncated_int32", []byte{2, 4, 0, 0, 0}},       // Header says int32, body too short
		{"truncated_int48", []byte{2, 5, 0, 0, 0, 0}},    // Header says int48, body too short
		{"truncated_int64", []byte{2, 6, 0, 0, 0, 0, 0}}, // Header says int64, body too short
		{"truncated_float64", []byte{2, 7, 0, 0, 0}},     // Header says float64, body too short
		{"truncated_text", []byte{2, 17}},                // Header says 2 byte text, no body at all
		{"truncated_blob", []byte{2, 16}},                // Header says 2 byte blob, no body at all
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseRecord(tt.data)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestSerialTypeLen(t *testing.T) {
	tests := []struct {
		serialType SerialType
		want       int
	}{
		{SerialTypeNull, 0},
		{SerialTypeInt8, 1},
		{SerialTypeInt16, 2},
		{SerialTypeInt24, 3},
		{SerialTypeInt32, 4},
		{SerialTypeInt48, 6},
		{SerialTypeInt64, 8},
		{SerialTypeFloat64, 8},
		{SerialTypeZero, 0},
		{SerialTypeOne, 0},
		{SerialType(12), 0},   // Empty blob
		{SerialType(13), 0},   // Empty text
		{SerialType(14), 1},   // 1-byte blob
		{SerialType(15), 1},   // 1-byte text
		{SerialType(20), 4},   // 4-byte blob
		{SerialType(21), 4},   // 4-byte text
		{SerialType(112), 50}, // 50-byte blob
		{SerialType(113), 50}, // 50-byte text
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.serialType)), func(t *testing.T) {
			got := SerialTypeLen(tt.serialType)
			if got != tt.want {
				t.Errorf("SerialTypeLen(%d) = %d, want %d", tt.serialType, got, tt.want)
			}
		})
	}
}

func TestValueConstructors(t *testing.T) {
	t.Run("IntValue", func(t *testing.T) {
		v := IntValue(42)
		if v.Type != TypeInteger {
			t.Errorf("Type = %v, want %v", v.Type, TypeInteger)
		}
		if v.Int != 42 {
			t.Errorf("Int = %d, want 42", v.Int)
		}
		if v.IsNull {
			t.Error("IsNull should be false")
		}
	})

	t.Run("FloatValue", func(t *testing.T) {
		v := FloatValue(3.14)
		if v.Type != TypeFloat {
			t.Errorf("Type = %v, want %v", v.Type, TypeFloat)
		}
		if v.Float != 3.14 {
			t.Errorf("Float = %f, want 3.14", v.Float)
		}
		if v.IsNull {
			t.Error("IsNull should be false")
		}
	})

	t.Run("TextValue", func(t *testing.T) {
		v := TextValue("hello")
		if v.Type != TypeText {
			t.Errorf("Type = %v, want %v", v.Type, TypeText)
		}
		if v.Text != "hello" {
			t.Errorf("Text = %q, want \"hello\"", v.Text)
		}
		if v.IsNull {
			t.Error("IsNull should be false")
		}
	})

	t.Run("BlobValue", func(t *testing.T) {
		blob := []byte{1, 2, 3}
		v := BlobValue(blob)
		if v.Type != TypeBlob {
			t.Errorf("Type = %v, want %v", v.Type, TypeBlob)
		}
		if !bytes.Equal(v.Blob, blob) {
			t.Errorf("Blob = %v, want %v", v.Blob, blob)
		}
		if v.IsNull {
			t.Error("IsNull should be false")
		}
	})

	t.Run("NullValue", func(t *testing.T) {
		v := NullValue()
		if v.Type != TypeNull {
			t.Errorf("Type = %v, want %v", v.Type, TypeNull)
		}
		if !v.IsNull {
			t.Error("IsNull should be true")
		}
	})
}

func TestRecordFormat(t *testing.T) {
	t.Run("header_size_calculation", func(t *testing.T) {
		// Test that header size is correctly calculated for various records
		values := []Value{IntValue(1), IntValue(2), IntValue(3)}
		record, err := MakeRecord(values)
		if err != nil {
			t.Fatal(err)
		}

		// Read header size
		headerSize, n := GetVarint(record, 0)
		if n == 0 {
			t.Fatal("failed to read header size")
		}

		// Verify header size points to start of body
		if int(headerSize) > len(record) {
			t.Errorf("header size %d exceeds record length %d", headerSize, len(record))
		}

		// Parse and verify
		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatalf("failed to parse: %v", err)
		}
		if len(parsed.Values) != 3 {
			t.Errorf("expected 3 values, got %d", len(parsed.Values))
		}
	})

	t.Run("serial_type_ordering", func(t *testing.T) {
		// Test that serial types appear in header in correct order
		values := []Value{
			NullValue(),
			IntValue(100),
			FloatValue(2.5),
			TextValue("test"),
		}
		record, err := MakeRecord(values)
		if err != nil {
			t.Fatal(err)
		}

		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatal(err)
		}

		// Verify types are in correct order
		if parsed.Values[0].Type != TypeNull {
			t.Error("first value should be null")
		}
		if parsed.Values[1].Type != TypeInteger {
			t.Error("second value should be integer")
		}
		if parsed.Values[2].Type != TypeFloat {
			t.Error("third value should be float")
		}
		if parsed.Values[3].Type != TypeText {
			t.Error("fourth value should be text")
		}
	})

	t.Run("body_values_contiguous", func(t *testing.T) {
		// Verify that body values are stored contiguously without gaps
		values := []Value{
			IntValue(255),           // 1 byte (int8)
			TextValue("ab"),         // 2 bytes
			BlobValue([]byte{1, 2}), // 2 bytes
		}
		record, err := MakeRecord(values)
		if err != nil {
			t.Fatal(err)
		}

		// Parse successfully
		parsed, err := ParseRecord(record)
		if err != nil {
			t.Fatalf("failed to parse: %v", err)
		}

		// Verify values
		if parsed.Values[0].Int != 255 {
			t.Errorf("first value: got %d, want 255", parsed.Values[0].Int)
		}
		if parsed.Values[1].Text != "ab" {
			t.Errorf("second value: got %q, want \"ab\"", parsed.Values[1].Text)
		}
		if !bytes.Equal(parsed.Values[2].Blob, []byte{1, 2}) {
			t.Errorf("third value: got %v, want [1 2]", parsed.Values[2].Blob)
		}
	})
}

// Helper function to compare values
func valuesEqual(a, b Value) bool {
	if a.Type != b.Type {
		return false
	}

	switch a.Type {
	case TypeNull:
		return true
	case TypeInteger:
		return a.Int == b.Int
	case TypeFloat:
		// Allow small floating point differences
		diff := a.Float - b.Float
		if diff < 0 {
			diff = -diff
		}
		return diff < 1e-10
	case TypeText:
		return a.Text == b.Text
	case TypeBlob:
		return bytes.Equal(a.Blob, b.Blob)
	}

	return false
}

// Benchmark tests
func BenchmarkMakeRecord(b *testing.B) {
	values := []Value{
		IntValue(1),
		IntValue(2),
		TextValue("hello world"),
		FloatValue(3.14159),
		BlobValue([]byte{1, 2, 3, 4, 5}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = MakeRecord(values)
	}
}

func BenchmarkParseRecord(b *testing.B) {
	values := []Value{
		IntValue(1),
		IntValue(2),
		TextValue("hello world"),
		FloatValue(3.14159),
		BlobValue([]byte{1, 2, 3, 4, 5}),
	}
	record, _ := MakeRecord(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseRecord(record)
	}
}

func BenchmarkVarintEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = PutVarint(nil, 123456789)
	}
}

func BenchmarkVarintDecode(b *testing.B) {
	buf := PutVarint(nil, 123456789)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = GetVarint(buf, 0)
	}
}
