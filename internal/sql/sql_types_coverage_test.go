// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// TestSQLTypes_ReleaseReg verifies that ReleaseReg is a no-op that does not
// modify Parse.Mem, and that it does not panic for any register value.
func TestSQLTypes_ReleaseReg(t *testing.T) {
	tests := []struct {
		name string
		mem  int
		reg  int
	}{
		{"zero mem zero reg", 0, 0},
		{"positive mem lower reg", 10, 3},
		{"positive mem equal reg", 5, 5},
		{"positive mem higher reg", 4, 7},
		{"negative-like reg", 8, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Parse{Mem: tt.mem}
			p.ReleaseReg(tt.reg)
			if p.Mem != tt.mem {
				t.Errorf("ReleaseReg(%d) modified Mem: got %d, want %d",
					tt.reg, p.Mem, tt.mem)
			}
		})
	}
}

// TestSQLTypes_ReleaseRegs verifies that ReleaseRegs is a no-op that does not
// modify Parse.Mem, and that it does not panic for any base/n combination.
func TestSQLTypes_ReleaseRegs(t *testing.T) {
	tests := []struct {
		name string
		mem  int
		base int
		n    int
	}{
		{"zero base zero n", 0, 0, 0},
		{"normal range", 10, 3, 4},
		{"single register", 7, 5, 1},
		{"large n", 100, 1, 50},
		{"base equals mem", 6, 6, 1},
		{"n is zero", 5, 2, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Parse{Mem: tt.mem}
			p.ReleaseRegs(tt.base, tt.n)
			if p.Mem != tt.mem {
				t.Errorf("ReleaseRegs(%d, %d) modified Mem: got %d, want %d",
					tt.base, tt.n, p.Mem, tt.mem)
			}
		})
	}
}

// TestSQLTypes_ReleaseReg_AfterAlloc verifies that allocating then releasing
// a register leaves the Parse in the expected state (release is no-op).
func TestSQLTypes_ReleaseReg_AfterAlloc(t *testing.T) {
	p := &Parse{Mem: 0}
	reg := p.AllocReg()
	if reg != 1 {
		t.Fatalf("AllocReg() = %d, want 1", reg)
	}
	p.ReleaseReg(reg)
	// Mem should still be 1 since ReleaseReg is no-op.
	if p.Mem != 1 {
		t.Errorf("after ReleaseReg, Mem = %d, want 1", p.Mem)
	}
}

// TestSQLTypes_ReleaseRegs_AfterAllocRegs verifies that allocating multiple
// registers then releasing them leaves Parse.Mem unchanged.
func TestSQLTypes_ReleaseRegs_AfterAllocRegs(t *testing.T) {
	p := &Parse{Mem: 0}
	base := p.AllocRegs(5)
	if base != 1 {
		t.Fatalf("AllocRegs(5) base = %d, want 1", base)
	}
	if p.Mem != 5 {
		t.Fatalf("after AllocRegs(5), Mem = %d, want 5", p.Mem)
	}
	p.ReleaseRegs(base, 5)
	// Mem should still be 5 since ReleaseRegs is no-op.
	if p.Mem != 5 {
		t.Errorf("after ReleaseRegs, Mem = %d, want 5", p.Mem)
	}
}
