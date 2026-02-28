package pager

import (
	"bytes"
	"testing"
)

func TestParseDatabaseHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func() []byte
		wantErr bool
	}{
		{
			name: "valid header",
			setup: func() []byte {
				header := NewDatabaseHeader(4096)
				return header.Serialize()
			},
			wantErr: false,
		},
		{
			name: "invalid magic header",
			setup: func() []byte {
				data := make([]byte, DatabaseHeaderSize)
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
				header := NewDatabaseHeader(65536)
				return header.Serialize()
			},
			wantErr: false,
		},
		{
			name: "min page size (512)",
			setup: func() []byte {
				header := NewDatabaseHeader(512)
				return header.Serialize()
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := tt.setup()
			header, err := ParseDatabaseHeader(data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseDatabaseHeader() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("ParseDatabaseHeader() unexpected error: %v", err)
				return
			}

			if header == nil {
				t.Errorf("ParseDatabaseHeader() returned nil header")
				return
			}

			// Validate that we can serialize and get the same data back
			serialized := header.Serialize()
			if !bytes.Equal(data, serialized) {
				t.Errorf("Serialize() didn't produce same data")
			}
		})
	}
}

func TestDatabaseHeader_Validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func() *DatabaseHeader
		wantErr bool
	}{
		{
			name: "valid header",
			setup: func() *DatabaseHeader {
				return NewDatabaseHeader(4096)
			},
			wantErr: false,
		},
		{
			name: "invalid magic",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				copy(h.Magic[:], "Invalid\x00")
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid page size (too small)",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				h.PageSize = 256
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid page size (not power of 2)",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				h.PageSize = 4000
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid file format write version",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				h.FileFormatWrite = 99
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid max payload fraction",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				h.MaxPayloadFrac = 100
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid schema format",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				h.SchemaFormat = 99
				return h
			},
			wantErr: true,
		},
		{
			name: "invalid text encoding",
			setup: func() *DatabaseHeader {
				h := NewDatabaseHeader(4096)
				h.TextEncoding = 99
				return h
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			header := tt.setup()
			err := header.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("Validate() expected error, got nil")
			}

			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

func TestDatabaseHeader_GetPageSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pageSize uint16
		want     int
	}{
		{
			name:     "normal page size",
			pageSize: 4096,
			want:     4096,
		},
		{
			name:     "max page size (stored as 1)",
			pageSize: 1,
			want:     65536,
		},
		{
			name:     "min page size",
			pageSize: 512,
			want:     512,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			header := &DatabaseHeader{PageSize: tt.pageSize}
			got := header.GetPageSize()

			if got != tt.want {
				t.Errorf("GetPageSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDatabaseHeader(t *testing.T) {
	t.Parallel()
	pageSizes := []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}

	for _, pageSize := range pageSizes {
		pageSize := pageSize
		t.Run("page_size_"+string(rune(pageSize)), func(t *testing.T) {
			t.Parallel()
			header := NewDatabaseHeader(pageSize)

			if header == nil {
				t.Fatal("NewDatabaseHeader() returned nil")
			}

			// Check magic header
			if string(header.Magic[:]) != MagicHeaderString {
				t.Errorf("Magic = %q, want %q", header.Magic, MagicHeaderString)
			}

			// Check page size
			actualPageSize := header.GetPageSize()
			if actualPageSize != pageSize {
				t.Errorf("PageSize = %d, want %d", actualPageSize, pageSize)
			}

			// Check default values
			if header.FileFormatWrite != 1 {
				t.Errorf("FileFormatWrite = %d, want 1", header.FileFormatWrite)
			}

			if header.FileFormatRead != 1 {
				t.Errorf("FileFormatRead = %d, want 1", header.FileFormatRead)
			}

			if header.MaxPayloadFrac != 64 {
				t.Errorf("MaxPayloadFrac = %d, want 64", header.MaxPayloadFrac)
			}

			if header.MinPayloadFrac != 32 {
				t.Errorf("MinPayloadFrac = %d, want 32", header.MinPayloadFrac)
			}

			if header.LeafPayloadFrac != 32 {
				t.Errorf("LeafPayloadFrac = %d, want 32", header.LeafPayloadFrac)
			}

			if header.TextEncoding != EncodingUTF8 {
				t.Errorf("TextEncoding = %d, want %d", header.TextEncoding, EncodingUTF8)
			}

			// Validate the header
			if err := header.Validate(); err != nil {
				t.Errorf("Validate() error = %v", err)
			}
		})
	}
}

func TestDatabaseHeader_Serialize(t *testing.T) {
	t.Parallel()
	header := NewDatabaseHeader(4096)
	header.DatabaseSize = 100
	header.FileChangeCounter = 42
	header.SchemaCookie = 7
	header.UserVersion = 123
	header.ApplicationID = 456

	data := header.Serialize()

	if len(data) != DatabaseHeaderSize {
		t.Errorf("Serialize() length = %d, want %d", len(data), DatabaseHeaderSize)
	}

	// Parse it back
	parsed, err := ParseDatabaseHeader(data)
	if err != nil {
		t.Fatalf("ParseDatabaseHeader() error = %v", err)
	}

	// Compare fields
	if parsed.DatabaseSize != header.DatabaseSize {
		t.Errorf("DatabaseSize = %d, want %d", parsed.DatabaseSize, header.DatabaseSize)
	}

	if parsed.FileChangeCounter != header.FileChangeCounter {
		t.Errorf("FileChangeCounter = %d, want %d", parsed.FileChangeCounter, header.FileChangeCounter)
	}

	if parsed.SchemaCookie != header.SchemaCookie {
		t.Errorf("SchemaCookie = %d, want %d", parsed.SchemaCookie, header.SchemaCookie)
	}

	if parsed.UserVersion != header.UserVersion {
		t.Errorf("UserVersion = %d, want %d", parsed.UserVersion, header.UserVersion)
	}

	if parsed.ApplicationID != header.ApplicationID {
		t.Errorf("ApplicationID = %d, want %d", parsed.ApplicationID, header.ApplicationID)
	}
}

func TestIsValidPageSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		size int
		want bool
	}{
		{256, false},    // too small
		{512, true},     // min valid
		{1024, true},    // power of 2
		{2048, true},    // power of 2
		{4096, true},    // power of 2
		{8192, true},    // power of 2
		{16384, true},   // power of 2
		{32768, true},   // power of 2
		{65536, true},   // max valid
		{131072, false}, // too large
		{4000, false},   // not power of 2
		{0, false},      // invalid
		{-1, false},     // negative
	}

	for _, tt := range tests {
		tt := tt
		t.Run("size_"+string(rune(tt.size)), func(t *testing.T) {
			t.Parallel()
			got := isValidPageSize(tt.size)
			if got != tt.want {
				t.Errorf("isValidPageSize(%d) = %v, want %v", tt.size, got, tt.want)
			}
		})
	}
}

func BenchmarkParseDatabaseHeader(b *testing.B) {
	header := NewDatabaseHeader(4096)
	data := header.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseDatabaseHeader(data)
	}
}

func BenchmarkSerializeDatabaseHeader(b *testing.B) {
	header := NewDatabaseHeader(4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = header.Serialize()
	}
}
