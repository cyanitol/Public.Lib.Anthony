package schema

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
)

func TestDetermineAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typeName string
		want     Affinity
	}{
		// INTEGER affinity
		{"INTEGER", AffinityInteger},
		{"INT", AffinityInteger},
		{"TINYINT", AffinityInteger},
		{"SMALLINT", AffinityInteger},
		{"MEDIUMINT", AffinityInteger},
		{"BIGINT", AffinityInteger},
		{"UNSIGNED BIG INT", AffinityInteger},
		{"INT2", AffinityInteger},
		{"INT8", AffinityInteger},

		// TEXT affinity
		{"TEXT", AffinityText},
		{"CLOB", AffinityText},
		{"CHARACTER(20)", AffinityText},
		{"VARCHAR(255)", AffinityText},
		{"VARYING CHARACTER(255)", AffinityText},
		{"NCHAR(55)", AffinityText},
		{"NATIVE CHARACTER(70)", AffinityText},
		{"NVARCHAR(100)", AffinityText},

		// BLOB affinity
		{"BLOB", AffinityBlob},
		{"", AffinityBlob}, // Empty type gets BLOB affinity

		// REAL affinity
		{"REAL", AffinityReal},
		{"DOUBLE", AffinityReal},
		{"DOUBLE PRECISION", AffinityReal},
		{"FLOAT", AffinityReal},

		// NUMERIC affinity (default for unrecognized types)
		{"NUMERIC", AffinityNumeric},
		{"DECIMAL(10,5)", AffinityNumeric},
		{"BOOLEAN", AffinityNumeric},
		{"DATE", AffinityNumeric},
		{"DATETIME", AffinityNumeric},

		// Case insensitivity
		{"integer", AffinityInteger},
		{"Integer", AffinityInteger},
		{"TEXT", AffinityText},
		{"text", AffinityText},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.typeName, func(t *testing.T) {
			t.Parallel()
			got := DetermineAffinity(tt.typeName)
			if got != tt.want {
				t.Errorf("DetermineAffinity(%q) = %v, want %v",
					tt.typeName, AffinityName(got), AffinityName(tt.want))
			}
		})
	}
}

func TestIsNumericAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		affinity Affinity
		want     bool
	}{
		{AffinityNone, false},
		{AffinityBlob, false},
		{AffinityText, false},
		{AffinityNumeric, true},
		{AffinityInteger, true},
		{AffinityReal, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(AffinityName(tt.affinity), func(t *testing.T) {
			t.Parallel()
			got := IsNumericAffinity(tt.affinity)
			if got != tt.want {
				t.Errorf("IsNumericAffinity(%v) = %v, want %v",
					AffinityName(tt.affinity), got, tt.want)
			}
		})
	}
}

func TestAffinityName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		affinity Affinity
		want     string
	}{
		{AffinityNone, "NONE"},
		{AffinityText, "TEXT"},
		{AffinityNumeric, "NUMERIC"},
		{AffinityInteger, "INTEGER"},
		{AffinityReal, "REAL"},
		{AffinityBlob, "BLOB"},
		{Affinity(99), "UNKNOWN"}, // Unknown affinity
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			got := AffinityName(tt.affinity)
			if got != tt.want {
				t.Errorf("AffinityName(%d) = %q, want %q",
					tt.affinity, got, tt.want)
			}
		})
	}
}

func TestAffinityConstants(t *testing.T) {
	t.Parallel()
	// Verify that our constants match the expr package constants
	if AffinityNone != expr.AFF_NONE {
		t.Errorf("AffinityNone mismatch")
	}
	if AffinityText != expr.AFF_TEXT {
		t.Errorf("AffinityText mismatch")
	}
	if AffinityNumeric != expr.AFF_NUMERIC {
		t.Errorf("AffinityNumeric mismatch")
	}
	if AffinityInteger != expr.AFF_INTEGER {
		t.Errorf("AffinityInteger mismatch")
	}
	if AffinityReal != expr.AFF_REAL {
		t.Errorf("AffinityReal mismatch")
	}
	if AffinityBlob != expr.AFF_BLOB {
		t.Errorf("AffinityBlob mismatch")
	}
}

// Benchmark affinity determination
func BenchmarkDetermineAffinity(b *testing.B) {
	typeNames := []string{
		"INTEGER",
		"VARCHAR(255)",
		"BLOB",
		"REAL",
		"NUMERIC",
		"TEXT",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DetermineAffinity(typeNames[i%len(typeNames)])
	}
}
