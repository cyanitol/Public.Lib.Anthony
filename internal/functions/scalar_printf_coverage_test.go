// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// TestFormatPrintfQuoted covers formatPrintfQuoted (%q and %Q specifiers).
func TestFormatPrintfQuoted(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			name:   "q_simple_string",
			format: "%q",
			arg:    NewTextValue("hello"),
			want:   "'hello'",
		},
		{
			name:   "q_string_with_single_quote",
			format: "%q",
			arg:    NewTextValue("hello'world"),
			want:   "'hello''world'",
		},
		{
			name:   "q_null_returns_empty",
			format: "%q",
			arg:    NewNullValue(),
			want:   "",
		},
		{
			name:   "Q_null_returns_NULL",
			format: "%Q",
			arg:    NewNullValue(),
			want:   "NULL",
		},
		{
			name:   "Q_string_with_single_quote",
			format: "%Q",
			arg:    NewTextValue("it's"),
			want:   "'it''s'",
		},
		{
			name:   "q_integer_arg",
			format: "%q",
			arg:    NewIntValue(42),
			want:   "'42'",
		},
		{
			name:   "q_float_arg",
			format: "%q",
			arg:    NewFloatValue(3.14),
			want:   "'3.14'",
		},
		{
			name:   "q_empty_string",
			format: "%q",
			arg:    NewTextValue(""),
			want:   "''",
		},
		{
			name:   "q_multiple_single_quotes",
			format: "%q",
			arg:    NewTextValue("a'b'c"),
			want:   "'a''b''c'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc() error = %v", err)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc(%q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

// TestFormatPrintfChar covers formatPrintfChar (%c specifier).
func TestFormatPrintfChar(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			name:   "c_integer_65_is_A",
			format: "%c",
			arg:    NewIntValue(65),
			want:   "A",
		},
		{
			name:   "c_integer_97_is_a",
			format: "%c",
			arg:    NewIntValue(97),
			want:   "a",
		},
		{
			name:   "c_integer_zero_empty",
			format: "%c",
			arg:    NewIntValue(0),
			want:   "",
		},
		{
			name:   "c_null_empty",
			format: "%c",
			arg:    NewNullValue(),
			want:   "",
		},
		{
			name:   "c_text_first_char",
			format: "%c",
			arg:    NewTextValue("Hello"),
			want:   "H",
		},
		{
			name:   "c_text_empty_string",
			format: "%c",
			arg:    NewTextValue(""),
			want:   "",
		},
		{
			name:   "c_with_precision_repeats",
			format: "%.3c",
			arg:    NewIntValue(65),
			want:   "AAA",
		},
		{
			name:   "c_with_width_padding",
			format: "%5c",
			arg:    NewIntValue(65),
			want:   "    A",
		},
		{
			name:   "c_unicode_code_point",
			format: "%c",
			arg:    NewIntValue(0x1F600),
			want:   "\U0001F600",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc() error = %v", err)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc(%q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

// TestProcessPrintfFormatCode covers processPrintfFormatCode branches:
// %%, %n (ignored), %p (hex pointer), unknown specifier, missing argument.
func TestProcessPrintfFormatCode(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "percent_escape",
			args: []Value{NewTextValue("100%%")},
			want: "100%",
		},
		{
			name: "n_specifier_ignored",
			args: []Value{NewTextValue("a%nb")},
			want: "ab",
		},
		{
			name: "p_specifier_hex_pointer",
			args: []Value{NewTextValue("%p"), NewIntValue(255)},
			want: "FF",
		},
		{
			name: "p_specifier_no_arg",
			args: []Value{NewTextValue("%p")},
			want: "0",
		},
		{
			name: "unknown_specifier_passthrough",
			args: []Value{NewTextValue("%y"), NewTextValue("x")},
			want: "%y",
		},
		{
			name: "missing_arg_uses_null",
			args: []Value{NewTextValue("%d")},
			want: "0",
		},
		{
			name: "trailing_percent_no_specifier",
			args: []Value{NewTextValue("abc%")},
			want: "abc%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc(tt.args)
			if err != nil {
				t.Fatalf("printfFunc() error = %v", err)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPrintfAllFormatSpecifiers covers all format specifiers in one place.
func TestPrintfAllFormatSpecifiers(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{name: "d_decimal", format: "%d", arg: NewIntValue(42), want: "42"},
		{name: "i_decimal", format: "%i", arg: NewIntValue(42), want: "42"},
		{name: "o_octal", format: "%o", arg: NewIntValue(8), want: "10"},
		{name: "x_hex_lower", format: "%x", arg: NewIntValue(255), want: "ff"},
		{name: "X_hex_upper", format: "%X", arg: NewIntValue(255), want: "FF"},
		{name: "f_float", format: "%.2f", arg: NewFloatValue(3.14159), want: "3.14"},
		{name: "e_scientific_lower", format: "%.2e", arg: NewFloatValue(1234.5), want: "1.23e+03"},
		{name: "E_scientific_upper", format: "%.2E", arg: NewFloatValue(1234.5), want: "1.23E+03"},
		{name: "g_compact_lower", format: "%g", arg: NewFloatValue(100.0), want: "100"},
		{name: "G_compact_upper", format: "%G", arg: NewFloatValue(100.0), want: "100"},
		{name: "s_string", format: "%s", arg: NewTextValue("hi"), want: "hi"},
		{name: "q_quoted", format: "%q", arg: NewTextValue("hi"), want: "'hi'"},
		{name: "Q_quoted_upper", format: "%Q", arg: NewTextValue("hi"), want: "'hi'"},
		{name: "c_char", format: "%c", arg: NewIntValue(65), want: "A"},
		{name: "percent_literal", format: "%%", arg: NewNullValue(), want: "%"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc(%q) error = %v", tt.format, err)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc(%q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

// TestPrintfFormatFunc covers the format() alias and NULL format string.
func TestPrintfFormatFunc(t *testing.T) {
	// format() is registered as an alias for printfFunc; test it via printfFunc directly.
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
	}{
		{
			name: "format_alias_basic",
			args: []Value{NewTextValue("value=%d"), NewIntValue(7)},
			want: "value=7",
		},
		{
			name:     "null_format_returns_null",
			args:     []Value{NewNullValue()},
			wantNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc(tt.args)
			if err != nil {
				t.Fatalf("printfFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("printfFunc() = %q, want NULL", result.AsString())
				}
				return
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPrintfDynamicWidthAndPrecision covers parsePrintfWidth and parsePrintfPrecision
// with the '*' dynamic specifier.
func TestPrintfDynamicWidthAndPrecision(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "dynamic_width_star",
			args: []Value{NewTextValue("%*d"), NewIntValue(6), NewIntValue(42)},
			want: "    42",
		},
		{
			name: "dynamic_precision_star",
			args: []Value{NewTextValue("%.*f"), NewIntValue(2), NewFloatValue(3.14159)},
			want: "3.14",
		},
		{
			name: "dynamic_width_and_precision_star",
			args: []Value{NewTextValue("%*.*f"), NewIntValue(8), NewIntValue(2), NewFloatValue(3.14159)},
			want: "    3.14",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc(tt.args)
			if err != nil {
				t.Fatalf("printfFunc() error = %v", err)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPrintfEdgeCases covers edge cases: negative numbers, floats as NULL, string precision.
func TestPrintfEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			name:   "negative_integer",
			format: "%d",
			arg:    NewIntValue(-42),
			want:   "-42",
		},
		{
			name:   "float_null_becomes_zero",
			format: "%.1f",
			arg:    NewNullValue(),
			want:   "0.0",
		},
		{
			name:   "string_precision_truncates",
			format: "%.3s",
			arg:    NewTextValue("hello"),
			want:   "hel",
		},
		{
			name:   "s_null_empty_string",
			format: "%s",
			arg:    NewNullValue(),
			want:   "",
		},
		{
			name:   "z_alias_for_s",
			format: "%z",
			arg:    NewTextValue("test"),
			want:   "test",
		},
		{
			name:   "length_modifier_l_skipped",
			format: "%ld",
			arg:    NewIntValue(99),
			want:   "99",
		},
		{
			name:   "length_modifier_h_skipped",
			format: "%hd",
			arg:    NewIntValue(5),
			want:   "5",
		},
		{
			name:   "u_unsigned_format",
			format: "%u",
			arg:    NewIntValue(42),
			want:   "42",
		},
		{
			name:   "F_float_upper",
			format: "%.2F",
			arg:    NewFloatValue(1.5),
			want:   "1.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc(%q) error = %v", tt.format, err)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("printfFunc(%q) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}
