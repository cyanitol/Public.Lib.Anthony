// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"testing"
)

// TestDbPageGetPgno tests GetPgno method
func TestDbPageGetPgno(t *testing.T) {
	t.Parallel()
	page := NewDbPage(42, 4096)

	if page.GetPgno() != 42 {
		t.Errorf("expected pgno 42, got %d", page.GetPgno())
	}
}

// TestDbPageGetData tests GetData method
func TestDbPageGetData(t *testing.T) {
	t.Parallel()
	page := NewDbPage(1, 4096)
	page.Data[0] = 0xAA
	page.Data[100] = 0xBB

	data := page.GetData()
	if data[0] != 0xAA {
		t.Errorf("expected data[0] = 0xAA, got 0x%02X", data[0])
	}
	if data[100] != 0xBB {
		t.Errorf("expected data[100] = 0xBB, got 0x%02X", data[100])
	}
}

// TestDbPageSize tests Size method
func TestDbPageSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pageSize int
	}{
		{"1KB", 1024},
		{"4KB", 4096},
		{"8KB", 8192},
		{"16KB", 16384},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			page := NewDbPage(1, tt.pageSize)
			if page.Size() != tt.pageSize {
				t.Errorf("expected size %d, got %d", tt.pageSize, page.Size())
			}
		})
	}
}

// TestDbPageSetDontWrite tests SetDontWrite method
func TestDbPageSetDontWrite(t *testing.T) {
	t.Parallel()
	page := NewDbPage(1, 4096)

	// Should write by default
	if !page.ShouldWrite() {
		t.Error("page should write by default")
	}

	// Set dont write
	page.SetDontWrite()

	if page.ShouldWrite() {
		t.Error("page should not write after SetDontWrite")
	}
}
