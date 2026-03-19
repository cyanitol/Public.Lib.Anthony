// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Test helper functions that extract values from expressions

func TestIntValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		expr    Expression
		want    int64
		wantErr bool
	}{
		{
			name:    "valid integer",
			expr:    &LiteralExpr{Type: LiteralInteger, Value: "42"},
			want:    42,
			wantErr: false,
		},
		{
			name:    "negative integer",
			expr:    &LiteralExpr{Type: LiteralInteger, Value: "-100"},
			want:    -100,
			wantErr: false,
		},
		{
			name:    "zero",
			expr:    &LiteralExpr{Type: LiteralInteger, Value: "0"},
			want:    0,
			wantErr: false,
		},
		{
			name:    "not an integer - string",
			expr:    &LiteralExpr{Type: LiteralString, Value: "hello"},
			want:    0,
			wantErr: true,
		},
		{
			name:    "not a literal",
			expr:    &IdentExpr{Name: "x"},
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid integer format",
			expr:    &LiteralExpr{Type: LiteralInteger, Value: "not_a_number"},
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := IntValue(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("IntValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFloatValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		expr    Expression
		want    float64
		wantErr bool
	}{
		{
			name:    "float literal",
			expr:    &LiteralExpr{Type: LiteralFloat, Value: "3.14"},
			want:    3.14,
			wantErr: false,
		},
		{
			name:    "integer as float",
			expr:    &LiteralExpr{Type: LiteralInteger, Value: "42"},
			want:    42.0,
			wantErr: false,
		},
		{
			name:    "negative float",
			expr:    &LiteralExpr{Type: LiteralFloat, Value: "-2.5"},
			want:    -2.5,
			wantErr: false,
		},
		{
			name:    "scientific notation",
			expr:    &LiteralExpr{Type: LiteralFloat, Value: "1.5e10"},
			want:    1.5e10,
			wantErr: false,
		},
		{
			name:    "not a number - string",
			expr:    &LiteralExpr{Type: LiteralString, Value: "hello"},
			want:    0,
			wantErr: true,
		},
		{
			name:    "not a literal",
			expr:    &IdentExpr{Name: "x"},
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := FloatValue(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("FloatValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("FloatValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		expr    Expression
		want    string
		wantErr bool
	}{
		{
			name:    "valid string",
			expr:    &LiteralExpr{Type: LiteralString, Value: "hello"},
			want:    "hello",
			wantErr: false,
		},
		{
			name:    "empty string",
			expr:    &LiteralExpr{Type: LiteralString, Value: ""},
			want:    "",
			wantErr: false,
		},
		{
			name:    "string with spaces",
			expr:    &LiteralExpr{Type: LiteralString, Value: "hello world"},
			want:    "hello world",
			wantErr: false,
		},
		{
			name:    "not a string - integer",
			expr:    &LiteralExpr{Type: LiteralInteger, Value: "42"},
			want:    "",
			wantErr: true,
		},
		{
			name:    "not a literal",
			expr:    &IdentExpr{Name: "x"},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := StringValue(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("StringValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("StringValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test parser helper methods

func TestParserHelpers(t *testing.T) {
	t.Parallel()
	t.Run("peek at end", func(t *testing.T) {
		t.Parallel()
		p := NewParser("")
		tok := p.peek()
		if tok.Type != TK_EOF {
			t.Errorf("peek() at end = %v, want TK_EOF", tok.Type)
		}
	})

	t.Run("checkIdentifier at end", func(t *testing.T) {
		t.Parallel()
		p := NewParser("")
		if p.checkIdentifier() {
			t.Error("checkIdentifier() at end should be false")
		}
	})
}
