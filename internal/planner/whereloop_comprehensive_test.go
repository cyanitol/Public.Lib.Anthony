// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"
)

// Comprehensive tests for whereloop.go functions

func TestNewWhereLoopBuilder(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()
	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	if builder == nil {
		t.Fatal("NewWhereLoopBuilder returned nil")
	}
	if builder.Table != table {
		t.Error("Table not set correctly")
	}
	if builder.Cursor != 0 {
		t.Error("Cursor not set correctly")
	}
	if len(builder.Terms) != 1 {
		t.Error("Terms not set correctly")
	}
	if builder.CostModel != cm {
		t.Error("CostModel not set correctly")
	}
}

func TestWhereLoopBuilderBuild(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()
	builder := NewWhereLoopBuilder(table, 0, nil, cm)

	loops := builder.Build()

	if len(loops) == 0 {
		t.Fatal("Build() returned no loops")
	}

	// Should have at least full scan
	hasFullScan := false
	for _, loop := range loops {
		if loop.Index == nil {
			hasFullScan = true
			break
		}
	}
	if !hasFullScan {
		t.Error("Expected at least one full scan loop")
	}
}

func TestAddFullScan(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()
	builder := NewWhereLoopBuilder(table, 0, nil, cm)

	builder.addFullScan()

	if len(builder.Loops) != 1 {
		t.Fatalf("Expected 1 loop, got %d", len(builder.Loops))
	}

	loop := builder.Loops[0]
	if loop.Index != nil {
		t.Error("Full scan should not use an index")
	}
	if loop.TabIndex != 0 {
		t.Error("TabIndex not set correctly")
	}
	if !loop.MaskSelf.Has(0) {
		t.Error("MaskSelf not set correctly")
	}
}

func TestAddIndexScans(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Create terms that can use the index
	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0}, // id =
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	// Get the primary key index
	index := table.Indexes[0]

	builder.addIndexScans(index)

	if len(builder.Loops) == 0 {
		t.Error("Expected index scan loops")
	}
}

func TestFindUsableTerms(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0}, // id
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 1}, // name
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)
	index := table.Indexes[0] // idx_users_id

	usable := builder.findUsableTerms(index, 1)

	if len(usable) == 0 {
		t.Error("Expected usable terms")
	}
}

func TestFindTermForIndexColumn(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0},
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 2},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	// Find term for column 0
	term := builder.findTermForIndexColumn(0)
	if term == nil {
		t.Error("Expected to find term for column 0")
	}
	if term.Operator != WO_EQ {
		t.Error("Expected WO_EQ operator")
	}

	// Find term for column 2
	term = builder.findTermForIndexColumn(2)
	if term == nil {
		t.Error("Expected to find term for column 2")
	}
	if term.Operator != WO_GT {
		t.Error("Expected WO_GT operator")
	}

	// No term for column 99
	term = builder.findTermForIndexColumn(99)
	if term != nil {
		t.Error("Expected no term for column 99")
	}
}

func TestIsUsableOperator(t *testing.T) {
	tests := []struct {
		op       WhereOperator
		expected bool
	}{
		{WO_EQ, true},
		{WO_LT, true},
		{WO_LE, true},
		{WO_GT, true},
		{WO_GE, true},
		{WO_IN, true},
		{WO_ISNULL, true},
		{WO_OR, false},
		{WO_IS, false},
	}

	for _, tt := range tests {
		result := isUsableOperator(tt.op)
		if result != tt.expected {
			t.Errorf("isUsableOperator(%v) = %v, want %v", tt.op, result, tt.expected)
		}
	}
}

func TestHasLowerBound(t *testing.T) {
	tests := []struct {
		name     string
		terms    []*WhereTerm
		expected bool
	}{
		{
			name: "has GT",
			terms: []*WhereTerm{
				{Operator: WO_GT},
			},
			expected: true,
		},
		{
			name: "has GE",
			terms: []*WhereTerm{
				{Operator: WO_GE},
			},
			expected: true,
		},
		{
			name: "no lower bound",
			terms: []*WhereTerm{
				{Operator: WO_LT},
				{Operator: WO_EQ},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasLowerBound(tt.terms)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestHasUpperBound(t *testing.T) {
	tests := []struct {
		name     string
		terms    []*WhereTerm
		expected bool
	}{
		{
			name: "has LT",
			terms: []*WhereTerm{
				{Operator: WO_LT},
			},
			expected: true,
		},
		{
			name: "has LE",
			terms: []*WhereTerm{
				{Operator: WO_LE},
			},
			expected: true,
		},
		{
			name: "no upper bound",
			terms: []*WhereTerm{
				{Operator: WO_GT},
				{Operator: WO_EQ},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasUpperBound(tt.terms)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestApplyTermsToLoop(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0, PrereqRight: 0},
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 2, PrereqRight: 0},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	loop := &WhereLoop{
		TabIndex: 0,
		NOut:     table.RowLogEst,
		Terms:    make([]*WhereTerm, 0),
	}

	builder.applyTermsToLoop(loop)

	if len(loop.Terms) != 2 {
		t.Errorf("Expected 2 terms applied, got %d", len(loop.Terms))
	}

	// NOut should be reduced by selectivity
	if loop.NOut >= table.RowLogEst {
		t.Error("NOut should be reduced after applying terms")
	}
}

func TestSetPrerequisites(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()
	builder := NewWhereLoopBuilder(table, 0, nil, cm)

	loop := &WhereLoop{
		TabIndex: 0,
		Terms: []*WhereTerm{
			{PrereqRight: Bitmask(2)}, // Requires table 1
			{PrereqRight: Bitmask(4)}, // Requires table 2
		},
	}
	loop.MaskSelf.Set(0)

	builder.setPrerequisites(loop)

	// Should have prerequisites for tables 1 and 2
	if !loop.Prereq.Has(1) || !loop.Prereq.Has(2) {
		t.Error("Prerequisites not set correctly")
	}

	// Should not include self
	if loop.Prereq.Has(0) {
		t.Error("Prerequisites should not include self")
	}
}

func TestWhereLoopString(t *testing.T) {
	loop := &WhereLoop{
		TabIndex: 0,
		Run:      LogEst(100),
		NOut:     LogEst(50),
		Index:    nil,
		Flags:    0,
	}

	str := loop.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	if !strings.Contains(str, "WhereLoop") {
		t.Error("String should contain 'WhereLoop'")
	}
	if !strings.Contains(str, "tab=0") {
		t.Error("String should contain tab index")
	}
	if !strings.Contains(str, "scan=FULL") {
		t.Error("String should indicate full scan")
	}
}

func TestWhereLoopStringWithIndex(t *testing.T) {
	index := &IndexInfo{Name: "idx_test"}
	loop := &WhereLoop{
		TabIndex: 0,
		Run:      LogEst(100),
		NOut:     LogEst(50),
		Index:    index,
		Flags:    WHERE_INDEXED | WHERE_COLUMN_EQ | WHERE_ONEROW,
	}

	str := loop.String()
	if !strings.Contains(str, "idx_test") {
		t.Error("String should contain index name")
	}
	if !strings.Contains(str, "ONEROW") {
		t.Error("String should contain ONEROW flag")
	}
	if !strings.Contains(str, "EQ") {
		t.Error("String should contain EQ flag")
	}
}

func assertWhereLoopFieldsMatch(t *testing.T, clone, original *WhereLoop) {
	t.Helper()
	checks := []struct {
		name string
		ok   bool
	}{
		{"Prereq", clone.Prereq == original.Prereq},
		{"MaskSelf", clone.MaskSelf == original.MaskSelf},
		{"TabIndex", clone.TabIndex == original.TabIndex},
		{"Setup", clone.Setup == original.Setup},
		{"Run", clone.Run == original.Run},
		{"NOut", clone.NOut == original.NOut},
		{"Flags", clone.Flags == original.Flags},
		{"Index", clone.Index == original.Index},
		{"Terms length", len(clone.Terms) == len(original.Terms)},
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("%s not cloned correctly", c.name)
		}
	}
}

func TestWhereLoopClone(t *testing.T) {
	original := &WhereLoop{
		Prereq:   Bitmask(3),
		MaskSelf: Bitmask(1),
		TabIndex: 5,
		Setup:    LogEst(10),
		Run:      LogEst(100),
		NOut:     LogEst(50),
		Flags:    WHERE_INDEXED,
		Index:    &IndexInfo{Name: "test"},
		Terms:    []*WhereTerm{{Operator: WO_EQ}},
	}

	clone := original.Clone()
	if clone == nil {
		t.Fatal("Clone returned nil")
	}

	assertWhereLoopFieldsMatch(t, clone, original)

	// Modify clone shouldn't affect original
	clone.TabIndex = 99
	if original.TabIndex == 99 {
		t.Error("Clone is not independent")
	}
}

func TestAnalyzeTermConstraints(t *testing.T) {
	tests := []struct {
		name        string
		terms       []*WhereTerm
		expectedEq  int
		expectedRng bool
	}{
		{
			name: "equality only",
			terms: []*WhereTerm{
				{Operator: WO_EQ},
				{Operator: WO_EQ},
			},
			expectedEq:  2,
			expectedRng: false,
		},
		{
			name: "range only",
			terms: []*WhereTerm{
				{Operator: WO_GT},
			},
			expectedEq:  0,
			expectedRng: true,
		},
		{
			name: "mixed",
			terms: []*WhereTerm{
				{Operator: WO_EQ},
				{Operator: WO_LT},
			},
			expectedEq:  1,
			expectedRng: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := analyzeTermConstraints(tt.terms)
			if c.nEq != tt.expectedEq {
				t.Errorf("Expected nEq=%d, got %d", tt.expectedEq, c.nEq)
			}
			if c.hasRange != tt.expectedRng {
				t.Errorf("Expected hasRange=%v, got %v", tt.expectedRng, c.hasRange)
			}
		})
	}
}

func TestComputeIndexFlags(t *testing.T) {
	tests := []struct {
		name     string
		c        termConstraints
		terms    []*WhereTerm
		index    *IndexInfo
		covering bool
		hasFlag  WhereFlags
	}{
		{
			name:     "equality",
			c:        termConstraints{nEq: 1, hasRange: false},
			terms:    []*WhereTerm{},
			index:    &IndexInfo{},
			covering: false,
			hasFlag:  WHERE_INDEXED | WHERE_COLUMN_EQ,
		},
		{
			name:     "range",
			c:        termConstraints{nEq: 0, hasRange: true},
			terms:    []*WhereTerm{{Operator: WO_GT}},
			index:    &IndexInfo{},
			covering: false,
			hasFlag:  WHERE_INDEXED | WHERE_COLUMN_RANGE | WHERE_BTM_LIMIT,
		},
		{
			name:     "covering",
			c:        termConstraints{nEq: 1, hasRange: false},
			terms:    []*WhereTerm{},
			index:    &IndexInfo{},
			covering: true,
			hasFlag:  WHERE_INDEXED | WHERE_COLUMN_EQ | WHERE_IDX_ONLY,
		},
		{
			name:  "unique",
			c:     termConstraints{nEq: 2, hasRange: false},
			terms: []*WhereTerm{},
			index: &IndexInfo{
				Unique: true,
				Columns: []IndexColumn{
					{Name: "col1"},
					{Name: "col2"},
				},
			},
			covering: false,
			hasFlag:  WHERE_ONEROW,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := computeIndexFlags(tt.c, tt.terms, tt.index, tt.covering)
			if flags&tt.hasFlag == 0 {
				t.Errorf("Expected flag %v to be set in %v", tt.hasFlag, flags)
			}
		})
	}
}

func TestCountEqualityTerms(t *testing.T) {
	terms := []*WhereTerm{
		{Operator: WO_EQ},
		{Operator: WO_GT},
		{Operator: WO_EQ},
		{Operator: WO_LT},
		{Operator: WO_EQ},
	}

	count := countEqualityTerms(terms)
	if count != 3 {
		t.Errorf("Expected 3 equality terms, got %d", count)
	}
}

func TestAddPrimaryKeyLookup(t *testing.T) {
	table := createTestTable()
	table.PrimaryKey = table.Indexes[0]
	cm := NewCostModel()

	// Term with rowid constraint
	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: -1}, // -1 = rowid
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)
	builder.addPrimaryKeyLookup()

	if len(builder.Loops) != 1 {
		t.Fatalf("Expected 1 loop, got %d", len(builder.Loops))
	}

	loop := builder.Loops[0]
	if loop.Flags&WHERE_IPK == 0 {
		t.Error("Expected WHERE_IPK flag")
	}
	if loop.Flags&WHERE_ONEROW == 0 {
		t.Error("Expected WHERE_ONEROW flag")
	}
}

func TestAddPrimaryKeyLookupNoTerm(t *testing.T) {
	table := createTestTable()
	table.PrimaryKey = table.Indexes[0]
	cm := NewCostModel()

	builder := NewWhereLoopBuilder(table, 0, nil, cm)
	builder.addPrimaryKeyLookup()

	// Should not add a loop if no primary key term
	if len(builder.Loops) != 0 {
		t.Error("Should not add primary key lookup without term")
	}
}

func TestOptimizeForSkipScan(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Use the compound index idx_users_city_age
	index := table.Indexes[2]

	// Terms for the second column (age), but not the first (city)
	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 2}, // age column
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)
	loop := builder.OptimizeForSkipScan(index)

	if loop == nil {
		t.Fatal("Expected skip-scan loop to be created")
	}

	if loop.Flags&WHERE_SKIPSCAN == 0 {
		t.Error("Expected WHERE_SKIPSCAN flag")
	}
}

func TestOptimizeForSkipScanNoTerms(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()
	index := table.Indexes[2]

	builder := NewWhereLoopBuilder(table, 0, nil, cm)
	loop := builder.OptimizeForSkipScan(index)

	if loop != nil {
		t.Error("Should not create skip-scan without later terms")
	}
}

func TestOptimizeForSkipScanSingleColumn(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Single column index
	index := table.Indexes[0]

	builder := NewWhereLoopBuilder(table, 0, nil, cm)
	loop := builder.OptimizeForSkipScan(index)

	if loop != nil {
		t.Error("Should not create skip-scan for single-column index")
	}
}

func TestOptimizeForSkipScanFirstColumnConstrained(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	index := table.Indexes[2] // idx_users_city_age

	// Terms for both columns
	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 3}, // city (first column)
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 2}, // age (second column)
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)
	loop := builder.OptimizeForSkipScan(index)

	if loop != nil {
		t.Error("Should not create skip-scan when first column is constrained")
	}
}

func TestColumnConstrained(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 1},
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 3},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	if !builder.columnConstrained(1) {
		t.Error("Column 1 should be constrained")
	}
	if !builder.columnConstrained(3) {
		t.Error("Column 3 should be constrained")
	}
	if builder.columnConstrained(0) {
		t.Error("Column 0 should not be constrained")
	}
}

func TestLaterColumnTerms(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	index := table.Indexes[2] // idx_users_city_age (columns 3, 2)

	terms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 3}, // city (first column)
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 2}, // age (second column)
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 1}, // name (not in index)
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)
	laterTerms := builder.laterColumnTerms(index)

	// Should find term for age (second column in index)
	if len(laterTerms) == 0 {
		t.Error("Expected to find later column terms")
	}
}

func TestFindInTermForColumn(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	terms := []*WhereTerm{
		{Operator: WO_IN, LeftCursor: 0, LeftColumn: 1},
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 2},
	}

	builder := NewWhereLoopBuilder(table, 0, terms, cm)

	inTerm := builder.findInTermForColumn(1)
	if inTerm == nil {
		t.Error("Expected to find IN term for column 1")
	}
	if inTerm.Operator != WO_IN {
		t.Error("Expected WO_IN operator")
	}

	inTerm = builder.findInTermForColumn(2)
	if inTerm != nil {
		t.Error("Should not find IN term for column 2")
	}
}

func TestComputeInOperatorFlags(t *testing.T) {
	tests := []struct {
		name     string
		nEq      int
		covering bool
		hasFlag  WhereFlags
	}{
		{
			name:     "basic IN",
			nEq:      0,
			covering: false,
			hasFlag:  WHERE_INDEXED | WHERE_COLUMN_IN | WHERE_IN_ABLE,
		},
		{
			name:     "IN with equality",
			nEq:      1,
			covering: false,
			hasFlag:  WHERE_COLUMN_EQ,
		},
		{
			name:     "covering IN",
			nEq:      0,
			covering: true,
			hasFlag:  WHERE_IDX_ONLY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := computeInOperatorFlags(tt.nEq, tt.covering)
			if flags&tt.hasFlag == 0 {
				t.Errorf("Expected flag %v to be set in %v", tt.hasFlag, flags)
			}
		})
	}
}

func TestSkipScanCostViable(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()
	builder := NewWhereLoopBuilder(table, 0, nil, cm)

	index := table.Indexes[2]
	laterTerms := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 2},
	}

	cost, nOut, ok := builder.skipScanCostViable(index, laterTerms)

	if !ok {
		t.Error("Skip scan should be viable")
	}
	if cost <= 0 {
		t.Error("Cost should be positive")
	}
	if nOut <= 0 {
		t.Error("Output rows should be positive")
	}
}
