// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package format

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestConstants(t *testing.T) {
	t.Parallel()
	// Test that constants have expected values
	if HeaderSize != 100 {
		t.Errorf("HeaderSize = %d, want 100", HeaderSize)
	}

	if DefaultPageSize != 4096 {
		t.Errorf("DefaultPageSize = %d, want 4096", DefaultPageSize)
	}

	if MinPageSize != 512 {
		t.Errorf("MinPageSize = %d, want 512", MinPageSize)
	}

	if MaxPageSize != 65536 {
		t.Errorf("MaxPageSize = %d, want 65536", MaxPageSize)
	}

	if len(MagicString) != 16 {
		t.Errorf("MagicString length = %d, want 16", len(MagicString))
	}

	if MagicString != "SQLite format 3\000" {
		t.Errorf("MagicString = %q, want %q", MagicString, "SQLite format 3\000")
	}
}

func TestPageTypeConstants(t *testing.T) {
	t.Parallel()
	// Test page type constants
	if PageTypeInteriorIndex != 0x02 {
		t.Errorf("PageTypeInteriorIndex = 0x%02x, want 0x02", PageTypeInteriorIndex)
	}

	if PageTypeInteriorTable != 0x05 {
		t.Errorf("PageTypeInteriorTable = 0x%02x, want 0x05", PageTypeInteriorTable)
	}

	if PageTypeLeafIndex != 0x0a {
		t.Errorf("PageTypeLeafIndex = 0x%02x, want 0x0a", PageTypeLeafIndex)
	}

	if PageTypeLeafTable != 0x0d {
		t.Errorf("PageTypeLeafTable = 0x%02x, want 0x0d", PageTypeLeafTable)
	}
}

func TestEncodingConstants(t *testing.T) {
	t.Parallel()
	if EncodingUTF8 != 1 {
		t.Errorf("EncodingUTF8 = %d, want 1", EncodingUTF8)
	}

	if EncodingUTF16LE != 2 {
		t.Errorf("EncodingUTF16LE = %d, want 2", EncodingUTF16LE)
	}

	if EncodingUTF16BE != 3 {
		t.Errorf("EncodingUTF16BE = %d, want 3", EncodingUTF16BE)
	}
}

func TestBtreeHeaderSizeConstants(t *testing.T) {
	t.Parallel()
	if BtreeHeaderSizeLeaf != 8 {
		t.Errorf("BtreeHeaderSizeLeaf = %d, want 8", BtreeHeaderSizeLeaf)
	}

	if BtreeHeaderSizeInterior != 12 {
		t.Errorf("BtreeHeaderSizeInterior = %d, want 12", BtreeHeaderSizeInterior)
	}
}

func TestIsValidPageSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		size int
		want bool
	}{
		{"too small (256)", 256, false},
		{"min valid (512)", 512, true},
		{"power of 2 (1024)", 1024, true},
		{"power of 2 (2048)", 2048, true},
		{"power of 2 (4096)", 4096, true},
		{"power of 2 (8192)", 8192, true},
		{"power of 2 (16384)", 16384, true},
		{"power of 2 (32768)", 32768, true},
		{"max valid (65536)", 65536, true},
		{"too large (131072)", 131072, false},
		{"not power of 2 (4000)", 4000, false},
		{"not power of 2 (1000)", 1000, false},
		{"zero", 0, false},
		{"negative", -1, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := IsValidPageSize(tt.size)
			if got != tt.want {
				t.Errorf("IsValidPageSize(%d) = %v, want %v", tt.size, got, tt.want)
			}
		})
	}
}

// assertNewHeaderDefaults validates the default values of a newly created header.
func assertNewHeaderDefaults(t *testing.T, h *Header, pageSize int) {
	t.Helper()
	if h == nil {
		t.Fatal("NewHeader() returned nil")
	}
	if string(h.Magic[:]) != MagicString {
		t.Errorf("Magic = %q, want %q", h.Magic, MagicString)
	}
	if h.GetPageSize() != pageSize {
		t.Errorf("PageSize = %d, want %d", h.GetPageSize(), pageSize)
	}
	checks := []struct {
		name string
		got  uint32
		want uint32
	}{
		{"WriteVersion", uint32(h.WriteVersion), 1},
		{"ReadVersion", uint32(h.ReadVersion), 1},
		{"MaxPayloadFrac", uint32(h.MaxPayloadFrac), 64},
		{"MinPayloadFrac", uint32(h.MinPayloadFrac), 32},
		{"LeafPayloadFrac", uint32(h.LeafPayloadFrac), 32},
		{"SchemaFormat", h.SchemaFormat, 4},
		{"TextEncoding", h.TextEncoding, EncodingUTF8},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if err := h.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}
}

func TestNewHeader(t *testing.T) {
	t.Parallel()
	pageSizes := []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}

	for _, pageSize := range pageSizes {
		pageSize := pageSize
		t.Run("", func(t *testing.T) {
			t.Parallel()
			assertNewHeaderDefaults(t, NewHeader(pageSize), pageSize)
		})
	}
}

func TestHeader_Parse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func() []byte
		wantErr bool
	}{
		{
			name: "valid header",
			setup: func() []byte {
				h := NewHeader(4096)
				return h.Serialize()
			},
			wantErr: false,
		},
		{
			name: "invalid magic header",
			setup: func() []byte {
				data := make([]byte, HeaderSize)
				copy(data, "Invalid format 3\x00")
				return data
			},
			wantErr: true,
		},
		{
			name: "too short",
			setup: func() []byte {
				return make([]byte, 50)
			},
			wantErr: true,
		},
		{
			name: "max page size (65536)",
			setup: func() []byte {
				h := NewHeader(65536)
				return h.Serialize()
			},
			wantErr: false,
		},
		{
			name: "min page size (512)",
			setup: func() []byte {
				h := NewHeader(512)
				return h.Serialize()
			},
			wantErr: false,
		},
		{
			name: "invalid page size",
			setup: func() []byte {
				data := make([]byte, HeaderSize)
				copy(data, MagicString)
				// Set invalid page size (not power of 2)
				binary.BigEndian.PutUint16(data[OffsetPageSize:], 4000)
				return data
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := tt.setup()
			h := &Header{}
			err := h.Parse(data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() unexpected error: %v", err)
				return
			}

			// Validate that we can serialize and get the same data back
			serialized := h.Serialize()
			if !bytes.Equal(data, serialized) {
				t.Errorf("Serialize() didn't produce same data")
			}
		})
	}
}

func TestHeader_Serialize(t *testing.T) {
	t.Parallel()
	h := NewHeader(4096)
	h.DatabaseSize = 100
	h.FileChangeCounter = 42
	h.SchemaCookie = 7
	h.UserVersion = 123
	h.AppID = 456

	data := h.Serialize()

	if len(data) != HeaderSize {
		t.Errorf("Serialize() length = %d, want %d", len(data), HeaderSize)
	}

	// Parse it back
	h2 := &Header{}
	if err := h2.Parse(data); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Compare fields
	if h2.DatabaseSize != h.DatabaseSize {
		t.Errorf("DatabaseSize = %d, want %d", h2.DatabaseSize, h.DatabaseSize)
	}

	if h2.FileChangeCounter != h.FileChangeCounter {
		t.Errorf("FileChangeCounter = %d, want %d", h2.FileChangeCounter, h.FileChangeCounter)
	}

	if h2.SchemaCookie != h.SchemaCookie {
		t.Errorf("SchemaCookie = %d, want %d", h2.SchemaCookie, h.SchemaCookie)
	}

	if h2.UserVersion != h.UserVersion {
		t.Errorf("UserVersion = %d, want %d", h2.UserVersion, h.UserVersion)
	}

	if h2.AppID != h.AppID {
		t.Errorf("AppID = %d, want %d", h2.AppID, h.AppID)
	}
}

func TestHeader_GetPageSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pageSize uint16
		want     int
	}{
		{
			name:     "normal page size (4096)",
			pageSize: 4096,
			want:     4096,
		},
		{
			name:     "max page size stored as 1",
			pageSize: 1,
			want:     65536,
		},
		{
			name:     "min page size (512)",
			pageSize: 512,
			want:     512,
		},
		{
			name:     "8192",
			pageSize: 8192,
			want:     8192,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &Header{PageSize: tt.pageSize}
			got := h.GetPageSize()

			if got != tt.want {
				t.Errorf("GetPageSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHeader_Validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func() *Header
		wantErr bool
	}{
		{
			name: "valid header",
			setup: func() *Header {
				return NewHeader(4096)
			},
			wantErr: false,
		},
		{
			name: "invalid magic",
			setup: func() *Header {
				h := NewHeader(4096)
				copy(h.Magic[:], "Invalid\x00")
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid page size (too small)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.PageSize = 256
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid page size (too large)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.PageSize = 2 // Not a valid power of 2 page size
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid page size (not power of 2)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.PageSize = 4000
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid write version (0)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.WriteVersion = 0
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid write version (3)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.WriteVersion = 3
				return h
			},
			wantErr: true,
		},
		{
			name: "valid write version (2)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.WriteVersion = 2
				return h
			},
			wantErr: false,
		},
		{
			name: "invalid read version",
			setup: func() *Header {
				h := NewHeader(4096)
				h.ReadVersion = 99
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid max payload fraction",
			setup: func() *Header {
				h := NewHeader(4096)
				h.MaxPayloadFrac = 100
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid min payload fraction",
			setup: func() *Header {
				h := NewHeader(4096)
				h.MinPayloadFrac = 100
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid leaf payload fraction",
			setup: func() *Header {
				h := NewHeader(4096)
				h.LeafPayloadFrac = 100
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid schema format (0)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 0
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid schema format (5)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 5
				return h
			},
			wantErr: true,
		},
		{
			name: "valid schema format (1)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 1
				return h
			},
			wantErr: false,
		},
		{
			name: "valid schema format (4)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.SchemaFormat = 4
				return h
			},
			wantErr: false,
		},
		{
			name: "invalid text encoding (0)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = 0
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid text encoding (4)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = 4
				return h
			},
			wantErr: true,
		},
		{
			name: "valid text encoding (UTF-16LE)",
			setup: func() *Header {
				h := NewHeader(4096)
				h.TextEncoding = EncodingUTF16LE
				return h
			},
			wantErr: false,
		},
		{
			name: "valid text encoding (UTF-16BE)",
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
			err := h.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("Validate() expected error, got nil")
			}

			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

// assertHeaderFieldsMatch compares two headers field by field.
func assertHeaderFieldsMatch(t *testing.T, h, h2 *Header) {
	t.Helper()
	checks := []struct {
		name string
		got  uint32
		want uint32
	}{
		{"WriteVersion", uint32(h2.WriteVersion), uint32(h.WriteVersion)},
		{"ReadVersion", uint32(h2.ReadVersion), uint32(h.ReadVersion)},
		{"DatabaseSize", h2.DatabaseSize, h.DatabaseSize},
		{"FileChangeCounter", h2.FileChangeCounter, h.FileChangeCounter},
		{"FirstFreelist", h2.FirstFreelist, h.FirstFreelist},
		{"FreelistCount", h2.FreelistCount, h.FreelistCount},
		{"SchemaCookie", h2.SchemaCookie, h.SchemaCookie},
		{"SchemaFormat", h2.SchemaFormat, h.SchemaFormat},
		{"DefaultCacheSize", h2.DefaultCacheSize, h.DefaultCacheSize},
		{"TextEncoding", h2.TextEncoding, h.TextEncoding},
		{"UserVersion", uint32(h2.UserVersion), uint32(h.UserVersion)},
		{"AppID", h2.AppID, h.AppID},
		{"SQLiteVersion", h2.SQLiteVersion, h.SQLiteVersion},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if h2.GetPageSize() != h.GetPageSize() {
		t.Errorf("PageSize = %d, want %d", h2.GetPageSize(), h.GetPageSize())
	}
}

func TestHeader_RoundTrip(t *testing.T) {
	t.Parallel()
	h := NewHeader(8192)
	h.DatabaseSize = 1000
	h.FileChangeCounter = 12345
	h.FirstFreelist = 50
	h.FreelistCount = 10
	h.SchemaCookie = 42
	h.SchemaFormat = 4
	h.DefaultCacheSize = 2000
	h.LargestRootPage = 100
	h.TextEncoding = EncodingUTF8
	h.UserVersion = 999
	h.IncrVacuum = 0
	h.AppID = 0x12345678
	h.VersionValidFor = 54321
	h.SQLiteVersion = 3051020

	data := h.Serialize()

	h2 := &Header{}
	if err := h2.Parse(data); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	assertHeaderFieldsMatch(t, h, h2)
}

func TestHeader_MaxPageSizeEncoding(t *testing.T) {
	t.Parallel()
	// Test that page size 65536 is encoded as 1
	h := NewHeader(65536)

	data := h.Serialize()

	// Check the encoded value at offset 16
	encodedPageSize := binary.BigEndian.Uint16(data[OffsetPageSize : OffsetPageSize+2])
	if encodedPageSize != 1 {
		t.Errorf("Encoded page size = %d, want 1", encodedPageSize)
	}

	// Parse it back
	h2 := &Header{}
	if err := h2.Parse(data); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check that we get 65536 back
	if h2.GetPageSize() != 65536 {
		t.Errorf("GetPageSize() = %d, want 65536", h2.GetPageSize())
	}
}

func TestHeader_OffsetConstants(t *testing.T) {
	t.Parallel()
	// Verify offset constants are correct according to SQLite spec
	offsets := map[string]int{
		"OffsetMagic":             0,
		"OffsetPageSize":          16,
		"OffsetWriteVersion":      18,
		"OffsetReadVersion":       19,
		"OffsetReservedSpace":     20,
		"OffsetMaxPayloadFrac":    21,
		"OffsetMinPayloadFrac":    22,
		"OffsetLeafPayloadFrac":   23,
		"OffsetFileChangeCounter": 24,
		"OffsetDatabaseSize":      28,
		"OffsetFirstFreelist":     32,
		"OffsetFreelistCount":     36,
		"OffsetSchemaCookie":      40,
		"OffsetSchemaFormat":      44,
		"OffsetDefaultCacheSize":  48,
		"OffsetLargestRootPage":   52,
		"OffsetTextEncoding":      56,
		"OffsetUserVersion":       60,
		"OffsetIncrVacuum":        64,
		"OffsetAppID":             68,
		"OffsetReserved":          72,
		"OffsetVersionValidFor":   92,
		"OffsetSQLiteVersion":     96,
	}

	// Actual values from constants
	actual := map[string]int{
		"OffsetMagic":             OffsetMagic,
		"OffsetPageSize":          OffsetPageSize,
		"OffsetWriteVersion":      OffsetWriteVersion,
		"OffsetReadVersion":       OffsetReadVersion,
		"OffsetReservedSpace":     OffsetReservedSpace,
		"OffsetMaxPayloadFrac":    OffsetMaxPayloadFrac,
		"OffsetMinPayloadFrac":    OffsetMinPayloadFrac,
		"OffsetLeafPayloadFrac":   OffsetLeafPayloadFrac,
		"OffsetFileChangeCounter": OffsetFileChangeCounter,
		"OffsetDatabaseSize":      OffsetDatabaseSize,
		"OffsetFirstFreelist":     OffsetFirstFreelist,
		"OffsetFreelistCount":     OffsetFreelistCount,
		"OffsetSchemaCookie":      OffsetSchemaCookie,
		"OffsetSchemaFormat":      OffsetSchemaFormat,
		"OffsetDefaultCacheSize":  OffsetDefaultCacheSize,
		"OffsetLargestRootPage":   OffsetLargestRootPage,
		"OffsetTextEncoding":      OffsetTextEncoding,
		"OffsetUserVersion":       OffsetUserVersion,
		"OffsetIncrVacuum":        OffsetIncrVacuum,
		"OffsetAppID":             OffsetAppID,
		"OffsetReserved":          OffsetReserved,
		"OffsetVersionValidFor":   OffsetVersionValidFor,
		"OffsetSQLiteVersion":     OffsetSQLiteVersion,
	}

	for name, expected := range offsets {
		expected := expected
		if actual[name] != expected {
			t.Errorf("%s = %d, want %d", name, actual[name], expected)
		}
	}
}

// Benchmarks

func BenchmarkHeader_Parse(b *testing.B) {
	h := NewHeader(4096)
	data := h.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h2 := &Header{}
		_ = h2.Parse(data)
	}
}

func BenchmarkHeader_Serialize(b *testing.B) {
	h := NewHeader(4096)
	h.DatabaseSize = 1000
	h.FileChangeCounter = 12345

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Serialize()
	}
}

func BenchmarkHeader_Validate(b *testing.B) {
	h := NewHeader(4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Validate()
	}
}

func BenchmarkIsValidPageSize(b *testing.B) {
	pageSizes := []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 4000, 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, size := range pageSizes {
			_ = IsValidPageSize(size)
		}
	}
}

func BenchmarkHeader_RoundTrip(b *testing.B) {
	h := NewHeader(4096)
	h.DatabaseSize = 1000
	h.FileChangeCounter = 12345

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := h.Serialize()
		h2 := &Header{}
		_ = h2.Parse(data)
	}
}
