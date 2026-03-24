// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package format

import (
	"testing"
)

// TestMCDC_IsValidPageSize exercises the two compound boolean conditions inside IsValidPageSize.
//
// IsValidPageSize logic:
//
//	if size < MinPageSize || size > MaxPageSize { return false }
//	return size&(size-1) == 0
//
// Condition 1: size < MinPageSize || size > MaxPageSize
//
//	A: size=256    → size<Min true  (short-circuits; flips outcome vs C)
//	B: size=131072 → size<Min false, size>Max true (flips outcome vs C)
//	C: size=4096   → both false (in-range)
//
// Condition 2 (power-of-2 check): size&(size-1) == 0
//
//	D: size=4096 (power of 2) → true  (independently flips outcome vs E)
//	E: size=4000 (not power of 2) → false (independently flips outcome vs D)
func TestMCDC_IsValidPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		want bool
		// MC/DC documentation fields
		belowMin   bool
		aboveMax   bool
		isPowerOf2 bool
	}{
		// Condition 1 sub-A: size<MinPageSize true → invalid (flips outcome independently)
		{
			name: "MCDC_below_min_invalid",
			size: 256, want: false,
			belowMin: true, aboveMax: false, isPowerOf2: true, // 256 is power of 2 but below min
		},
		{
			name: "MCDC_size_1_below_min",
			size: 1, want: false,
			belowMin: true, aboveMax: false, isPowerOf2: false,
		},
		// Condition 1 sub-B: size>MaxPageSize true → invalid (flips outcome independently)
		{
			name: "MCDC_above_max_invalid",
			size: 131072, want: false,
			belowMin: false, aboveMax: true, isPowerOf2: true,
		},
		{
			name: "MCDC_size_65537_above_max",
			size: 65537, want: false,
			belowMin: false, aboveMax: true, isPowerOf2: false,
		},
		// Condition 1 sub-C: both false (in range); Condition 2 sub-D: power of 2
		{
			name: "MCDC_in_range_power_of_2_valid",
			size: 4096, want: true,
			belowMin: false, aboveMax: false, isPowerOf2: true,
		},
		{
			name: "MCDC_min_boundary_valid",
			size: 512, want: true,
			belowMin: false, aboveMax: false, isPowerOf2: true,
		},
		{
			name: "MCDC_max_boundary_valid",
			size: 65536, want: true,
			belowMin: false, aboveMax: false, isPowerOf2: true,
		},
		// Condition 1 both false; Condition 2 sub-E: not power of 2 (flips outcome vs sub-D)
		{
			name: "MCDC_in_range_not_power_of_2_invalid",
			size: 4000, want: false,
			belowMin: false, aboveMax: false, isPowerOf2: false,
		},
		{
			name: "MCDC_in_range_1000_invalid",
			size: 1000, want: false,
			belowMin: false, aboveMax: false, isPowerOf2: false,
		},
		// Zero and negative – both below minimum
		{
			name: "MCDC_zero_invalid",
			size: 0, want: false,
			belowMin: true, aboveMax: false, isPowerOf2: false,
		},
		{
			name: "MCDC_negative_invalid",
			size: -512, want: false,
			belowMin: true, aboveMax: false, isPowerOf2: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := IsValidPageSize(tt.size)
			if got != tt.want {
				t.Errorf("IsValidPageSize(%d) = %v, want %v (belowMin=%v aboveMax=%v isPow2=%v)",
					tt.size, got, tt.want, tt.belowMin, tt.aboveMax, tt.isPowerOf2)
			}
		})
	}
}

// TestMCDC_HeaderValidateSchemaAndEncoding exercises the compound range conditions in
// validateSchemaAndEncoding.
//
// Condition 1: h.SchemaFormat < 1 || h.SchemaFormat > 4
//
//	A: SchemaFormat=0  → <1 true  (flips outcome independently)
//	B: SchemaFormat=5  → <1 false, >4 true (flips outcome independently)
//	C: SchemaFormat=2  → both false (valid middle value)
//
// Condition 2: h.TextEncoding < EncodingUTF8 || h.TextEncoding > EncodingUTF16BE
//
//	D: TextEncoding=0  → <1 true  (flips outcome independently)
//	E: TextEncoding=4  → <1 false, >3 true (flips outcome independently)
//	F: TextEncoding=2  → both false (valid middle value)
func TestMCDC_HeaderValidateSchemaAndEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func() *Header
		wantErr bool
	}{
		// Condition 1 sub-A: SchemaFormat<1 → error (flips outcome)
		{
			name: "MCDC_schema_format_zero_invalid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 0
				return h
			},
			wantErr: true,
		},
		// Condition 1 sub-B: SchemaFormat>4 → error (flips outcome)
		{
			name: "MCDC_schema_format_five_invalid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 5
				return h
			},
			wantErr: true,
		},
		// Condition 1 sub-C: SchemaFormat in [1,4], Condition 2 sub-F: TextEncoding valid
		{
			name: "MCDC_schema_format_one_valid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 1
				return h
			},
			wantErr: false,
		},
		{
			name: "MCDC_schema_format_four_valid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 4
				return h
			},
			wantErr: false,
		},
		{
			name: "MCDC_schema_format_middle_valid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 2
				return h
			},
			wantErr: false,
		},
		// Condition 2 sub-D: TextEncoding<1 → error (flips outcome)
		{
			name: "MCDC_text_encoding_zero_invalid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = 0
				return h
			},
			wantErr: true,
		},
		// Condition 2 sub-E: TextEncoding>3 → error (flips outcome)
		{
			name: "MCDC_text_encoding_four_invalid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = 4
				return h
			},
			wantErr: true,
		},
		// Condition 2 sub-F: TextEncoding in [1,3] valid
		{
			name: "MCDC_text_encoding_utf8_valid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = EncodingUTF8
				return h
			},
			wantErr: false,
		},
		{
			name: "MCDC_text_encoding_utf16le_valid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = EncodingUTF16LE
				return h
			},
			wantErr: false,
		},
		{
			name: "MCDC_text_encoding_utf16be_valid",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = EncodingUTF16BE
				return h
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := tt.setup()
			err := h.validateSchemaAndEncoding()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSchemaAndEncoding() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_NewHeaderPageSizeEncoding exercises the conditional in NewHeader:
//
//	if pageSize == MaxPageSize { pageSizeVal = 1 } else { pageSizeVal = uint16(pageSize) }
//
// This is a simple if/else with one condition, so we need two cases that independently
// flip the branch:
//
//	A: pageSize == MaxPageSize (65536) → stored as 1
//	B: pageSize != MaxPageSize (any other valid size) → stored verbatim
func TestMCDC_NewHeaderPageSizeEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		pageSize      int
		wantPageSize  uint16 // stored uint16 value
		wantGetResult int    // what GetPageSize() should return
	}{
		// A: pageSize == MaxPageSize → stored as 1
		{
			name:          "MCDC_max_page_size_stored_as_one",
			pageSize:      MaxPageSize,
			wantPageSize:  1,
			wantGetResult: MaxPageSize,
		},
		// B: pageSize != MaxPageSize → stored verbatim
		{
			name:          "MCDC_normal_page_size_stored_verbatim",
			pageSize:      4096,
			wantPageSize:  4096,
			wantGetResult: 4096,
		},
		{
			name:          "MCDC_min_page_size_stored_verbatim",
			pageSize:      512,
			wantPageSize:  512,
			wantGetResult: 512,
		},
		{
			name:          "MCDC_32768_page_size_stored_verbatim",
			pageSize:      32768,
			wantPageSize:  32768,
			wantGetResult: 32768,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := NewHeader(tt.pageSize)
			if h.PageSize != tt.wantPageSize {
				t.Errorf("NewHeader(%d).PageSize = %d, want %d",
					tt.pageSize, h.PageSize, tt.wantPageSize)
			}
			if got := h.GetPageSize(); got != tt.wantGetResult {
				t.Errorf("NewHeader(%d).GetPageSize() = %d, want %d",
					tt.pageSize, got, tt.wantGetResult)
			}
		})
	}
}
