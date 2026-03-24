// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestFormatP4Direct calls formatP4 directly for every P4Type branch,
// covering the P4Int32, P4Int64, P4Real, P4Static, P4Dynamic, and default paths.
func TestFormatP4Direct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		instr  *vdbe.Instruction
		want   string
	}{
		{
			name:  "P4Int32_positive",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Int32, P4: vdbe.P4Union{I: 42}},
			want:  "42",
		},
		{
			name:  "P4Int32_negative",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Int32, P4: vdbe.P4Union{I: -7}},
			want:  "-7",
		},
		{
			name:  "P4Int32_zero",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Int32, P4: vdbe.P4Union{I: 0}},
			want:  "0",
		},
		{
			name:  "P4Int64_large",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Int64, P4: vdbe.P4Union{I64: 9223372036854775807}},
			want:  "9223372036854775807",
		},
		{
			name:  "P4Int64_negative",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Int64, P4: vdbe.P4Union{I64: -1}},
			want:  "-1",
		},
		{
			name:  "P4Real_float",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Real, P4: vdbe.P4Union{R: 3.14}},
			want:  "3.14",
		},
		{
			name:  "P4Real_zero",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Real, P4: vdbe.P4Union{R: 0.0}},
			want:  "0",
		},
		{
			name:  "P4Static_string",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Static, P4: vdbe.P4Union{Z: "hello"}},
			want:  "hello",
		},
		{
			name:  "P4Dynamic_string",
			instr: &vdbe.Instruction{P4Type: vdbe.P4Dynamic, P4: vdbe.P4Union{Z: "world"}},
			want:  "world",
		},
		{
			name:  "P4NotUsed_empty",
			instr: &vdbe.Instruction{P4Type: vdbe.P4NotUsed, P4: vdbe.P4Union{}},
			want:  "",
		},
		{
			name:  "P4KeyInfo_default",
			instr: &vdbe.Instruction{P4Type: vdbe.P4KeyInfo, P4: vdbe.P4Union{}},
			want:  "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := formatP4(tt.instr)
			if got != tt.want {
				t.Errorf("formatP4 = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestParseNumericParameterDirect calls parseNumericParameter directly to cover
// all switch branches and both valid/invalid input paths.
func TestParseNumericParameterDirect(t *testing.T) {
	t.Parallel()

	t.Run("cache_size_valid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "cache_size", "2048"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.CacheSize != 2048 {
			t.Errorf("CacheSize = %d, want 2048", cfg.Pager.CacheSize)
		}
	})

	t.Run("cachesize_alias", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "cachesize", "512"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.CacheSize != 512 {
			t.Errorf("CacheSize = %d, want 512", cfg.Pager.CacheSize)
		}
	})

	t.Run("cache_size_invalid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "cache_size", "notanumber"); err == nil {
			t.Fatal("expected error for invalid cache_size, got nil")
		}
	})

	t.Run("page_size_valid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "page_size", "4096"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.PageSize != 4096 {
			t.Errorf("PageSize = %d, want 4096", cfg.Pager.PageSize)
		}
	})

	t.Run("pagesize_alias", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "pagesize", "8192"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.PageSize != 8192 {
			t.Errorf("PageSize = %d, want 8192", cfg.Pager.PageSize)
		}
	})

	t.Run("page_size_invalid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "page_size", "abc"); err == nil {
			t.Fatal("expected error for invalid page_size, got nil")
		}
	})

	t.Run("wal_autocheckpoint_valid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "wal_autocheckpoint", "1000"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.WALAutocheckpoint != 1000 {
			t.Errorf("WALAutocheckpoint = %d, want 1000", cfg.Pager.WALAutocheckpoint)
		}
	})

	t.Run("walautocheckpoint_alias", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "walautocheckpoint", "500"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.WALAutocheckpoint != 500 {
			t.Errorf("WALAutocheckpoint = %d, want 500", cfg.Pager.WALAutocheckpoint)
		}
	})

	t.Run("max_page_count_valid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "max_page_count", "100000"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.MaxPageCount != 100000 {
			t.Errorf("MaxPageCount = %d, want 100000", cfg.Pager.MaxPageCount)
		}
	})

	t.Run("maxpagecount_alias", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "maxpagecount", "50000"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.MaxPageCount != 50000 {
			t.Errorf("MaxPageCount = %d, want 50000", cfg.Pager.MaxPageCount)
		}
	})

	t.Run("busy_timeout_valid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "busy_timeout", "3000"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.BusyTimeout != 3000*time.Millisecond {
			t.Errorf("BusyTimeout = %v, want %v", cfg.Pager.BusyTimeout, 3000*time.Millisecond)
		}
	})

	t.Run("busytimeout_alias", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "busytimeout", "250"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pager.BusyTimeout != 250*time.Millisecond {
			t.Errorf("BusyTimeout = %v, want %v", cfg.Pager.BusyTimeout, 250*time.Millisecond)
		}
	})

	t.Run("busy_timeout_invalid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "busy_timeout", "bad"); err == nil {
			t.Fatal("expected error for invalid busy_timeout, got nil")
		}
	})

	t.Run("query_timeout_valid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "query_timeout", "5000"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.QueryTimeout != 5000*time.Millisecond {
			t.Errorf("QueryTimeout = %v, want %v", cfg.QueryTimeout, 5000*time.Millisecond)
		}
	})

	t.Run("querytimeout_alias", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "querytimeout", "100"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.QueryTimeout != 100*time.Millisecond {
			t.Errorf("QueryTimeout = %v, want %v", cfg.QueryTimeout, 100*time.Millisecond)
		}
	})

	t.Run("query_timeout_invalid", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "query_timeout", "xyz"); err == nil {
			t.Fatal("expected error for invalid query_timeout, got nil")
		}
	})

	t.Run("unknown_key_returns_error", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "no_such_param", "42"); err == nil {
			t.Fatal("expected error for unknown parameter, got nil")
		}
	})

	t.Run("zero_value", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "cache_size", "0"); err != nil {
			t.Fatalf("unexpected error for zero: %v", err)
		}
		if cfg.Pager.CacheSize != 0 {
			t.Errorf("CacheSize = %d, want 0", cfg.Pager.CacheSize)
		}
	})

	t.Run("negative_value", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultDriverConfig()
		if err := parseNumericParameter(cfg, "cache_size", "-2000"); err != nil {
			t.Fatalf("unexpected error for negative value: %v", err)
		}
		if cfg.Pager.CacheSize != -2000 {
			t.Errorf("CacheSize = %d, want -2000", cfg.Pager.CacheSize)
		}
	})
}

// TestPragmaExtractColNameDirect calls pragmaExtractColName directly to cover
// all three branches: IdentExpr, non-nil Expr fallback, and nil Expr.
func TestPragmaExtractColNameDirect(t *testing.T) {
	t.Parallel()

	t.Run("ident_expr", func(t *testing.T) {
		t.Parallel()
		col := parser.ResultColumn{
			Expr: &parser.IdentExpr{Name: "mycolumn"},
		}
		got := pragmaExtractColName(col)
		if got != "mycolumn" {
			t.Errorf("pragmaExtractColName = %q, want %q", got, "mycolumn")
		}
	})

	t.Run("ident_expr_with_table", func(t *testing.T) {
		t.Parallel()
		col := parser.ResultColumn{
			Expr: &parser.IdentExpr{Name: "col", Table: "tbl"},
		}
		got := pragmaExtractColName(col)
		// IdentExpr.String() would return "tbl.col" but the IdentExpr branch
		// returns only ident.Name, so we expect "col".
		if got != "col" {
			t.Errorf("pragmaExtractColName = %q, want %q", got, "col")
		}
	})

	t.Run("non_ident_expr_string_fallback", func(t *testing.T) {
		t.Parallel()
		// Use a LiteralExpr (not IdentExpr) so the second branch is taken.
		col := parser.ResultColumn{
			Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
		}
		got := pragmaExtractColName(col)
		if got != "42" {
			t.Errorf("pragmaExtractColName(LiteralExpr) = %q, want %q", got, "42")
		}
	})

	t.Run("nil_expr_returns_empty", func(t *testing.T) {
		t.Parallel()
		col := parser.ResultColumn{Expr: nil}
		got := pragmaExtractColName(col)
		if got != "" {
			t.Errorf("pragmaExtractColName with nil Expr = %q, want %q", got, "")
		}
	})
}
