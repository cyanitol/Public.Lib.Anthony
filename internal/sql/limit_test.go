package sql

import (
	"testing"
)

// TestNewLimitCompiler tests creating a new limit compiler
func TestNewLimitCompiler(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	if lc == nil {
		t.Fatal("NewLimitCompiler() returned nil")
	}
	if lc.parse != p {
		t.Error("NewLimitCompiler() did not set parse correctly")
	}
}

// TestCompileLimitOffset tests compiling LIMIT and OFFSET
func TestCompileLimitOffset(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}, Mem: 0}
	lc := NewLimitCompiler(p)
	sel := &Select{Limit: 10, Offset: 5}

	info, err := lc.CompileLimitOffset(sel)
	if err != nil {
		t.Fatalf("CompileLimitOffset() returned error: %v", err)
	}
	if info == nil {
		t.Fatal("CompileLimitOffset() returned nil info")
	}
	if info.Limit != 10 {
		t.Errorf("info.Limit = %d, want 10", info.Limit)
	}
	if info.Offset != 5 {
		t.Errorf("info.Offset = %d, want 5", info.Offset)
	}
}

// TestCompileLimitOffsetNoLimit tests compiling with no LIMIT
func TestCompileLimitOffsetNoLimit(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}, Mem: 0}
	lc := NewLimitCompiler(p)
	sel := &Select{Limit: 0, Offset: 0}

	info, err := lc.CompileLimitOffset(sel)
	if err != nil {
		t.Fatalf("CompileLimitOffset() returned error: %v", err)
	}
	if info.Limit != 0 {
		t.Errorf("info.Limit = %d, want 0", info.Limit)
	}
	if info.LimitReg != 0 {
		t.Errorf("info.LimitReg = %d, want 0", info.LimitReg)
	}
}

// TestOptimizeLimitWithIndex tests LIMIT optimization with index
func TestOptimizeLimitWithIndex(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)
	sel := &Select{
		Limit:   10,
		GroupBy: NewExprList(),
		OrderBy: NewExprList(),
	}
	info := &LimitInfo{Limit: 10}

	// With GROUP BY, optimization should be false
	sel.GroupBy.Append(ExprListItem{Name: "col1"})
	if lc.OptimizeLimitWithIndex(sel, info) {
		t.Error("OptimizeLimitWithIndex() with GROUP BY should return false")
	}
}

// TestOptimizeLimitNoLimit tests optimization with no LIMIT
func TestOptimizeLimitNoLimit(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)
	sel := &Select{}
	info := &LimitInfo{Limit: 0}

	if lc.OptimizeLimitWithIndex(sel, info) {
		t.Error("OptimizeLimitWithIndex() with no LIMIT should return false")
	}
}

// TestComputeLimitOffset tests computing static LIMIT/OFFSET
func TestComputeLimitOffset(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name        string
		limitExpr   *Expr
		offsetExpr  *Expr
		wantLimit   int
		wantOffset  int
		wantErr     bool
	}{
		{
			name:       "both limit and offset",
			limitExpr:  &Expr{Op: TK_INTEGER, IntValue: 10},
			offsetExpr: &Expr{Op: TK_INTEGER, IntValue: 5},
			wantLimit:  10,
			wantOffset: 5,
			wantErr:    false,
		},
		{
			name:       "only limit",
			limitExpr:  &Expr{Op: TK_INTEGER, IntValue: 20},
			offsetExpr: nil,
			wantLimit:  20,
			wantOffset: 0,
			wantErr:    false,
		},
		{
			name:       "negative limit",
			limitExpr:  &Expr{Op: TK_INTEGER, IntValue: -1},
			offsetExpr: nil,
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    true,
		},
		{
			name:       "negative offset",
			limitExpr:  &Expr{Op: TK_INTEGER, IntValue: 10},
			offsetExpr: &Expr{Op: TK_INTEGER, IntValue: -1},
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    true,
		},
		{
			name:       "non-constant limit",
			limitExpr:  &Expr{Op: TK_COLUMN, StringValue: "col"},
			offsetExpr: nil,
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit, offset, err := lc.ComputeLimitOffset(tt.limitExpr, tt.offsetExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ComputeLimitOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if limit != tt.wantLimit {
					t.Errorf("ComputeLimitOffset() limit = %d, want %d", limit, tt.wantLimit)
				}
				if offset != tt.wantOffset {
					t.Errorf("ComputeLimitOffset() offset = %d, want %d", offset, tt.wantOffset)
				}
			}
		})
	}
}

// TestGenerateLimitOffsetPlan tests generating LIMIT/OFFSET plan
func TestGenerateLimitOffsetPlan(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name    string
		sel     *Select
		wantErr bool
	}{
		{
			name:    "simple query",
			sel:     &Select{Limit: 10, Offset: 5},
			wantErr: false,
		},
		{
			name: "with ORDER BY",
			sel: &Select{
				Limit:   10,
				OrderBy: &ExprList{Items: []ExprListItem{{Name: "col1"}}},
			},
			wantErr: false,
		},
		{
			name: "with GROUP BY",
			sel: &Select{
				Limit:   10,
				GroupBy: &ExprList{Items: []ExprListItem{{Name: "col1"}}},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := lc.GenerateLimitOffsetPlan(tt.sel)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateLimitOffsetPlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && plan == nil {
				t.Error("GenerateLimitOffsetPlan() returned nil plan")
			}
		})
	}
}

// TestCombineLimitOffset tests combining LIMIT and OFFSET
func TestCombineLimitOffset(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name     string
		limit    int
		offset   int
		expected int
	}{
		{"no limit", 0, 10, 0},
		{"limit only", 10, 0, 10},
		{"limit and offset", 10, 5, 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lc.CombineLimitOffset(tt.limit, tt.offset)
			if got != tt.expected {
				t.Errorf("CombineLimitOffset(%d, %d) = %d, want %d",
					tt.limit, tt.offset, got, tt.expected)
			}
		})
	}
}

// TestSplitLimitOffset tests splitting effective limit
func TestSplitLimitOffset(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name      string
		effective int
		offset    int
		expected  int
	}{
		{"no offset", 10, 0, 10},
		{"with offset", 15, 5, 10},
		{"offset larger than effective", 5, 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lc.SplitLimitOffset(tt.effective, tt.offset)
			if got != tt.expected {
				t.Errorf("SplitLimitOffset(%d, %d) = %d, want %d",
					tt.effective, tt.offset, got, tt.expected)
			}
		})
	}
}

// TestValidateLimitOffset tests validation
func TestValidateLimitOffset(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name    string
		limit   int
		offset  int
		wantErr bool
	}{
		{"valid values", 10, 5, false},
		{"zero values", 0, 0, false},
		{"negative limit", -1, 0, true},
		{"negative offset", 10, -1, true},
		{"both negative", -1, -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := lc.ValidateLimitOffset(tt.limit, tt.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateLimitOffset(%d, %d) error = %v, wantErr %v",
					tt.limit, tt.offset, err, tt.wantErr)
			}
		})
	}
}
