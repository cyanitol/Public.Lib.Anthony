package parser

import (
	"testing"
)

// Test edge cases in PRAGMA parsing

func TestParsePragmaEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "pragma simple",
			sql:     "PRAGMA cache_size",
			wantErr: false,
		},
		{
			name:    "pragma with schema",
			sql:     "PRAGMA main.cache_size",
			wantErr: false,
		},
		{
			name:    "pragma with equals value",
			sql:     "PRAGMA cache_size = 2000",
			wantErr: false,
		},
		{
			name:    "pragma with function syntax",
			sql:     "PRAGMA cache_size(2000)",
			wantErr: false,
		},
		{
			name:    "pragma with negative value",
			sql:     "PRAGMA cache_size = -2000",
			wantErr: false,
		},
		{
			name:    "pragma with string value",
			sql:     "PRAGMA journal_mode = 'WAL'",
			wantErr: false,
		},
		{
			name:    "pragma with keyword value",
			sql:     "PRAGMA journal_mode = WAL",
			wantErr: false,
		},
		{
			name:    "pragma schema with temp",
			sql:     "PRAGMA temp.cache_size",
			wantErr: false,
		},
		{
			name:    "pragma error - missing name",
			sql:     "PRAGMA",
			wantErr: true,
		},
		{
			name:    "pragma error - missing name after schema",
			sql:     "PRAGMA main.",
			wantErr: true,
		},
		{
			name:    "pragma error - missing closing paren",
			sql:     "PRAGMA cache_size(2000",
			wantErr: true,
		},
		{
			name:    "pragma error - missing value",
			sql:     "PRAGMA cache_size =",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
