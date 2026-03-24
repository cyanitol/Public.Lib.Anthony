// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"
)

// TestWhereLoopPartialIndexUsableWithMatchingTerm tests that partialIndexUsable
// returns true when a query term references the table that has the partial index.
func TestWhereLoopPartialIndexUsableWithMatchingTerm(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 0},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	index := &IndexInfo{
		Name:        "idx_partial_a",
		Table:       "users",
		Partial:     true,
		WhereClause: "a > 0",
		Columns: []IndexColumn{
			{Name: "id", Index: 0, Ascending: true},
		},
		ColumnStats: []LogEst{NewLogEst(5000)},
	}

	if !builder.partialIndexUsable(index) {
		t.Error("partialIndexUsable should return true when a query term references the table")
	}
}

// TestWhereLoopPartialIndexUsableNoTerms tests that partialIndexUsable returns
// false when there are no query terms referencing the table.
func TestWhereLoopPartialIndexUsableNoTerms(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	builder := NewWhereLoopBuilder(table, 0, nil, cm)

	index := &IndexInfo{
		Name:        "idx_partial_a",
		Table:       "users",
		Partial:     true,
		WhereClause: "a > 0",
		Columns: []IndexColumn{
			{Name: "id", Index: 0, Ascending: true},
		},
		ColumnStats: []LogEst{NewLogEst(5000)},
	}

	if builder.partialIndexUsable(index) {
		t.Error("partialIndexUsable should return false when no terms reference the table")
	}
}

// TestWhereLoopPartialIndexUsableNoWhereClause tests that partialIndexUsable
// returns false when the index has no WHERE clause text.
func TestWhereLoopPartialIndexUsableNoWhereClause(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 0},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	index := &IndexInfo{
		Name:        "idx_no_where",
		Table:       "users",
		Partial:     true,
		WhereClause: "",
		Columns: []IndexColumn{
			{Name: "id", Index: 0, Ascending: true},
		},
		ColumnStats: []LogEst{NewLogEst(5000)},
	}

	if builder.partialIndexUsable(index) {
		t.Error("partialIndexUsable should return false when WhereClause is empty")
	}
}

// TestWhereLoopPartialIndexUsableWrongCursor tests that partialIndexUsable
// returns false when query terms reference a different cursor/table.
func TestWhereLoopPartialIndexUsableWrongCursor(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Terms that reference cursor 1 (a different table), not cursor 0
	terms := []*WhereTerm{
		{Operator: WO_GT, LeftCursor: 1, LeftColumn: 0},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	index := &IndexInfo{
		Name:        "idx_partial_a",
		Table:       "users",
		Partial:     true,
		WhereClause: "a > 0",
		Columns: []IndexColumn{
			{Name: "id", Index: 0, Ascending: true},
		},
		ColumnStats: []LogEst{NewLogEst(5000)},
	}

	if builder.partialIndexUsable(index) {
		t.Error("partialIndexUsable should return false when terms reference a different cursor")
	}
}

// TestWhereLoopAddIndexScansSkipsUnusablePartialIndex tests that addIndexScans
// skips partial indexes whose WHERE clause is not satisfied by query terms.
func TestWhereLoopAddIndexScansSkipsUnusablePartialIndex(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// No terms at all - partial index should not be usable
	builder := NewWhereLoopBuilder(table, 0, nil, cm)

	partialIndex := &IndexInfo{
		Name:        "idx_partial_skip",
		Table:       "users",
		Partial:     true,
		WhereClause: "age > 0",
		Columns: []IndexColumn{
			{Name: "age", Index: 2, Ascending: true},
		},
		ColumnStats: []LogEst{NewLogEst(5000)},
	}

	initialLoops := len(builder.Loops)
	builder.addIndexScans(partialIndex)

	if len(builder.Loops) != initialLoops {
		t.Error("addIndexScans should skip partial indexes that are not usable")
	}
}

// TestWhereLoopAddIndexScansUsesUsablePartialIndex tests that addIndexScans
// does add loops for partial indexes that are implied by query terms.
func TestWhereLoopAddIndexScansUsesUsablePartialIndex(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 2},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	partialIndex := &IndexInfo{
		Name:        "idx_partial_use",
		Table:       "users",
		Partial:     true,
		WhereClause: "age > 0",
		Columns: []IndexColumn{
			{Name: "age", Index: 2, Ascending: true},
		},
		ColumnStats: []LogEst{NewLogEst(5000)},
	}

	builder.addIndexScans(partialIndex)

	if len(builder.Loops) == 0 {
		t.Error("addIndexScans should add loops for usable partial indexes")
	}
}

// TestWhereLoopFormatFlagsIN tests that formatWhereLoopFlags includes the IN flag.
func TestWhereLoopFormatFlagsIN(t *testing.T) {
	flags := WHERE_INDEXED | WHERE_COLUMN_IN | WHERE_IN_ABLE
	s := formatWhereLoopFlags(flags)
	if !strings.Contains(s, "IN") {
		t.Errorf("expected 'IN' in flags string, got: %q", s)
	}
}

// TestWhereLoopFormatFlagsSkipScan tests that formatWhereLoopFlags includes SKIPSCAN.
func TestWhereLoopFormatFlagsSkipScan(t *testing.T) {
	flags := WHERE_INDEXED | WHERE_SKIPSCAN
	s := formatWhereLoopFlags(flags)
	if !strings.Contains(s, "SKIPSCAN") {
		t.Errorf("expected 'SKIPSCAN' in flags string, got: %q", s)
	}
}

// TestWhereLoopFormatFlagsAllBranches tests all flag branches of formatWhereLoopFlags.
func TestWhereLoopFormatFlagsAllBranches(t *testing.T) {
	tests := []struct {
		name     string
		flags    WhereFlags
		contains string
	}{
		{"ONEROW", WHERE_ONEROW, "ONEROW"},
		{"EQ", WHERE_COLUMN_EQ, "EQ"},
		{"RANGE", WHERE_COLUMN_RANGE, "RANGE"},
		{"IN", WHERE_COLUMN_IN, "IN"},
		{"COVERING", WHERE_IDX_ONLY, "COVERING"},
		{"SKIPSCAN", WHERE_SKIPSCAN, "SKIPSCAN"},
		{"no flags", 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := formatWhereLoopFlags(tt.flags)
			if tt.contains == "" {
				if s != "" {
					t.Errorf("expected empty string for no flags, got: %q", s)
				}
				return
			}
			if !strings.Contains(s, tt.contains) {
				t.Errorf("expected %q in flags string, got: %q", tt.contains, s)
			}
		})
	}
}
