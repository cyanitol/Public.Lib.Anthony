// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestExtractLeadingNumeric tests the extractLeadingNumeric helper.
func TestExtractLeadingNumeric(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"abc", ""},
		{"  42", ""},
		{"123", "123"},
		{"12.34", "12.34"},
		{"12.", "12."},
		{".5", ".5"},
		{"+5", "+5"},
		{"-3.14", "-3.14"},
		{"1e5", "1"},
		{"5abc", "5"},
		{"3.14xyz", "3.14"},
		{"+", ""},
		{"-", ""},
		{"+-1", ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := extractLeadingNumeric(tt.input)
			if got != tt.want {
				t.Errorf("extractLeadingNumeric(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestSkipDigits tests the skipDigits helper.
func TestSkipDigits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s    string
		i    int
		want int
	}{
		{"", 0, 0},
		{"abc", 0, 0},
		{"123", 0, 3},
		{"123abc", 0, 3},
		{"123", 1, 3},
		{"123", 3, 3},
		{"00", 0, 2},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.s+"_"+string(rune('0'+tt.i)), func(t *testing.T) {
			t.Parallel()
			got := skipDigits(tt.s, tt.i)
			if got != tt.want {
				t.Errorf("skipDigits(%q, %d) = %d, want %d", tt.s, tt.i, got, tt.want)
			}
		})
	}
}

// TestToDistinctKey tests ToDistinctKey for each Mem type.
func TestToDistinctKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mem  *Mem
		want string
	}{
		{"null", NewMemNull(), "NULL"},
		{"int_pos", NewMemInt(42), "I:42"},
		{"int_neg", NewMemInt(-7), "I:-7"},
		{"real", NewMemReal(3.14), "R:3.14"},
		{"real_neg", NewMemReal(-1.5), "R:-1.5"},
		{"str", NewMemStr("hello"), "S:hello"},
		{"str_empty", NewMemStr(""), "S:"},
		{"blob", NewMemBlob([]byte{0xde, 0xad}), "B:dead"},
		{"blob_empty", NewMemBlob([]byte{}), "B:"},
		{"undefined", NewMem(), "UNDEFINED"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.mem.ToDistinctKey()
			if got != tt.want {
				t.Errorf("ToDistinctKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestToDistinctKeyWithCollation tests ToDistinctKeyWithCollation for collation variants.
func TestToDistinctKeyWithCollation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mem       *Mem
		collation string
		want      string
	}{
		// empty collation falls back to ToDistinctKey
		{"null_no_coll", NewMemNull(), "", "NULL"},
		{"int_no_coll", NewMemInt(5), "", "I:5"},
		{"str_no_coll", NewMemStr("Hi"), "", "S:Hi"},
		// NOCASE: text lowercased
		{"null_nocase", NewMemNull(), "NOCASE", "NULL"},
		{"int_nocase", NewMemInt(9), "NOCASE", "I:9"},
		{"real_nocase", NewMemReal(2.0), "NOCASE", "R:2"},
		{"str_nocase", NewMemStr("Hello"), "NOCASE", "S:hello"},
		{"blob_nocase", NewMemBlob([]byte{0x0f}), "NOCASE", "B:0f"},
		{"undef_nocase", NewMem(), "NOCASE", "UNDEFINED"},
		// RTRIM: trailing spaces stripped
		{"str_rtrim", NewMemStr("hi   "), "RTRIM", "S:hi"},
		{"str_rtrim_nospace", NewMemStr("hi"), "RTRIM", "S:hi"},
		// BINARY (default): no transformation
		{"str_binary", NewMemStr("Hello"), "BINARY", "S:Hello"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.mem.ToDistinctKeyWithCollation(tt.collation, nil)
			if got != tt.want {
				t.Errorf("ToDistinctKeyWithCollation(%q) = %q, want %q", tt.collation, got, tt.want)
			}
		})
	}
}

// TestNormalizeDistinctText tests normalizeDistinctText directly.
func TestNormalizeDistinctText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s        string
		collName string
		want     string
	}{
		{"Hello", "NOCASE", "hello"},
		{"WORLD", "nocase", "world"},
		{"hi   ", "RTRIM", "hi"},
		{"hi", "RTRIM", "hi"},
		{"Hello", "BINARY", "Hello"},
		{"Hello", "", "Hello"},
		{"Hello", "OTHER", "Hello"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.s+"_"+tt.collName, func(t *testing.T) {
			t.Parallel()
			got := normalizeDistinctText(tt.s, tt.collName, nil)
			if got != tt.want {
				t.Errorf("normalizeDistinctText(%q, %q) = %q, want %q", tt.s, tt.collName, got, tt.want)
			}
		})
	}
}

// TestIntValue tests IntValue across all Mem types.
func TestIntValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mem  *Mem
		want int64
	}{
		{"int", NewMemInt(42), 42},
		{"int_neg", NewMemInt(-10), -10},
		{"real", NewMemReal(3.9), 3},
		{"real_neg", NewMemReal(-2.7), -2},
		{"str_int", NewMemStr("99"), 99},
		{"str_float", NewMemStr("3.7"), 3},
		{"str_leading", NewMemStr("5abc"), 5},
		{"str_unparseable", NewMemStr("abc"), 0},
		{"str_empty", NewMemStr(""), 0},
		{"blob_int", NewMemBlob([]byte("12")), 12},
		{"null", NewMemNull(), 0},
		{"undefined", NewMem(), 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.mem.IntValue()
			if got != tt.want {
				t.Errorf("IntValue() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestIntegerify tests Integerify across all Mem types.
func TestIntegerify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mem     *Mem
		wantVal int64
		wantErr bool
	}{
		{"already_int", NewMemInt(7), 7, false},
		{"real_to_int", NewMemReal(4.9), 4, false},
		{"str_int", NewMemStr("55"), 55, false},
		{"str_float", NewMemStr("3.7"), 3, false},
		{"null_to_int", NewMemNull(), 0, false},
		{"str_invalid", NewMemStr("xyz"), 0, true},
		{"blob_int", NewMemBlob([]byte("8")), 8, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.mem.Integerify()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Integerify() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if tt.mem.i != tt.wantVal {
					t.Errorf("i = %d, want %d", tt.mem.i, tt.wantVal)
				}
				if tt.mem.flags&MemInt == 0 {
					t.Errorf("MemInt flag not set after Integerify")
				}
			}
		})
	}
}

// TestRealify tests Realify across all Mem types.
func TestRealify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mem     *Mem
		wantVal float64
		wantErr bool
	}{
		{"already_real", NewMemReal(1.5), 1.5, false},
		{"int_to_real", NewMemInt(3), 3.0, false},
		{"str_float", NewMemStr("2.5"), 2.5, false},
		{"str_int_str", NewMemStr("10"), 10.0, false},
		{"null_to_real", NewMemNull(), 0.0, false},
		{"str_invalid", NewMemStr("xyz"), 0, true},
		{"blob_float", NewMemBlob([]byte("1.25")), 1.25, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.mem.Realify()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Realify() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if tt.mem.r != tt.wantVal {
					t.Errorf("r = %v, want %v", tt.mem.r, tt.wantVal)
				}
				if tt.mem.flags&MemReal == 0 {
					t.Errorf("MemReal flag not set after Realify")
				}
			}
		})
	}
}
