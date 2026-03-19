// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

func TestPrintfFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "basic string",
			args: []Value{NewTextValue("hello")},
			want: "hello",
		},
		{
			name: "integer format",
			args: []Value{NewTextValue("%d"), NewIntValue(42)},
			want: "42",
		},
		{
			name: "float format",
			args: []Value{NewTextValue("%.2f"), NewFloatValue(3.14159)},
			want: "3.14",
		},
		{
			name: "string format",
			args: []Value{NewTextValue("Hello %s"), NewTextValue("World")},
			want: "Hello World",
		},
		{
			name: "hex format lowercase",
			args: []Value{NewTextValue("%x"), NewIntValue(255)},
			want: "ff",
		},
		{
			name: "hex format uppercase",
			args: []Value{NewTextValue("%X"), NewIntValue(255)},
			want: "FF",
		},
		{
			name: "percent escape",
			args: []Value{NewTextValue("100%%")},
			want: "100%",
		},
		{
			name: "multiple formats",
			args: []Value{NewTextValue("%d %s %.2f"), NewIntValue(42), NewTextValue("hello"), NewFloatValue(3.14)},
			want: "42 hello 3.14",
		},
		{
			name: "NULL handling",
			args: []Value{NewTextValue("%d"), NewNullValue()},
			want: "0",
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
