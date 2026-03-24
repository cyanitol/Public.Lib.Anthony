// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// TestMCDC_SrcListGet exercises the compound condition in SrcList.Get.
//
// Condition: sl == nil || idx < 0 || idx >= len(sl.Items)
//
// MC/DC cases (three sub-conditions using short-circuit OR):
//
//	A: sl=nil                → first sub true (flips outcome independently)
//	B: sl!=nil, idx=-1       → first false, second true (flips outcome independently)
//	C: sl!=nil, idx>=len     → first false, second false, third true (flips outcome)
//	D: sl!=nil, idx in range → all false → returns valid item
func TestMCDC_SrcListGet(t *testing.T) {
	t.Parallel()

	items := []SrcListItem{
		{Name: "t1"},
		{Name: "t2"},
	}

	tests := []struct {
		name    string
		sl      *SrcList
		idx     int
		wantNil bool
	}{
		// A: nil receiver → nil result
		{name: "MCDC_nil_srclist_returns_nil", sl: nil, idx: 0, wantNil: true},
		// B: negative index → nil result
		{name: "MCDC_negative_index_returns_nil",
			sl:      &SrcList{Items: items},
			idx:     -1,
			wantNil: true,
		},
		// C: index >= len → nil result
		{name: "MCDC_index_equals_len_returns_nil",
			sl:      &SrcList{Items: items},
			idx:     2,
			wantNil: true,
		},
		{name: "MCDC_index_beyond_len_returns_nil",
			sl:      &SrcList{Items: items},
			idx:     99,
			wantNil: true,
		},
		// D: valid index → returns item (all sub-conditions false)
		{name: "MCDC_valid_index_zero_returns_item",
			sl:      &SrcList{Items: items},
			idx:     0,
			wantNil: false,
		},
		{name: "MCDC_valid_index_one_returns_item",
			sl:      &SrcList{Items: items},
			idx:     1,
			wantNil: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.sl.Get(tt.idx)
			if tt.wantNil && got != nil {
				t.Errorf("Get(%d) = %+v, want nil", tt.idx, got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("Get(%d) = nil, want non-nil", tt.idx)
			}
		})
	}
}

// TestMCDC_ExprListGet exercises the compound condition in ExprList.Get.
//
// Condition: el == nil || idx < 0 || idx >= len(el.Items)
//
// MC/DC cases:
//
//	A: el=nil              → first sub true (flips outcome)
//	B: el!=nil, idx=-1     → second sub true (flips outcome)
//	C: el!=nil, idx>=len   → third sub true (flips outcome)
//	D: all false           → returns item
func TestMCDC_ExprListGet(t *testing.T) {
	t.Parallel()

	el := NewExprList()
	el.Append(ExprListItem{Name: "col_a"})
	el.Append(ExprListItem{Name: "col_b"})

	tests := []struct {
		name    string
		list    *ExprList
		idx     int
		wantNil bool
	}{
		// A: nil receiver
		{name: "MCDC_nil_list_returns_nil", list: nil, idx: 0, wantNil: true},
		// B: negative index
		{name: "MCDC_negative_index_returns_nil", list: el, idx: -1, wantNil: true},
		// C: index out of bounds
		{name: "MCDC_index_equals_len_returns_nil", list: el, idx: 2, wantNil: true},
		{name: "MCDC_index_large_returns_nil", list: el, idx: 100, wantNil: true},
		// D: valid index
		{name: "MCDC_index_zero_returns_item", list: el, idx: 0, wantNil: false},
		{name: "MCDC_index_one_returns_item", list: el, idx: 1, wantNil: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.list.Get(tt.idx)
			if tt.wantNil && got != nil {
				t.Errorf("ExprList.Get(%d) = %+v, want nil", tt.idx, got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("ExprList.Get(%d) = nil, want non-nil", tt.idx)
			}
		})
	}
}

// TestMCDC_ValidateLimitOffset exercises the three conditions in ValidateLimitOffset.
//
// Condition 1: limit < 0
//
//	A: limit=-1 → true → error (flips outcome independently)
//	B: limit=0  → false (independent baseline)
//
// Condition 2: offset < 0
//
//	C: offset=-1 → true → error (flips outcome independently, limit>=0 required)
//	D: offset=0  → false
//
// Condition 3: limit > 0 && offset > 0 && limit > maxInt-offset
//
//	E: limit>0 true, offset>0 true, overflow true → error (all true flip)
//	F: limit>0 true, offset=0 → second sub false (short-circuit, no overflow)
//	G: limit=0, offset>0 → first sub false (short-circuit)
func TestMCDC_ValidateLimitOffset(t *testing.T) {
	t.Parallel()

	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	const maxInt = int(^uint(0) >> 1)

	tests := []struct {
		name    string
		limit   int
		offset  int
		wantErr bool
	}{
		// Condition 1 sub-A: limit<0 → error
		{name: "MCDC_negative_limit_errors", limit: -1, offset: 0, wantErr: true},
		{name: "MCDC_negative_limit_with_offset_errors", limit: -5, offset: 10, wantErr: true},
		// Condition 2 sub-C: offset<0 → error
		{name: "MCDC_negative_offset_errors", limit: 0, offset: -1, wantErr: true},
		{name: "MCDC_negative_offset_with_limit_errors", limit: 10, offset: -1, wantErr: true},
		// Condition 3 sub-E: overflow → error
		{name: "MCDC_overflow_errors", limit: maxInt, offset: 1, wantErr: true},
		// Condition 3 sub-F: limit>0, offset=0 → no overflow check
		{name: "MCDC_limit_only_no_overflow", limit: 100, offset: 0, wantErr: false},
		// Condition 3 sub-G: limit=0, offset>0 → no overflow check
		{name: "MCDC_offset_only_no_overflow", limit: 0, offset: 100, wantErr: false},
		// All conditions false (both zero)
		{name: "MCDC_both_zero_valid", limit: 0, offset: 0, wantErr: false},
		// Normal positive values
		{name: "MCDC_normal_limit_and_offset_valid", limit: 10, offset: 5, wantErr: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := lc.ValidateLimitOffset(tt.limit, tt.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateLimitOffset(%d, %d) error=%v, wantErr=%v",
					tt.limit, tt.offset, err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_GenerateLimitOffsetPlan exercises the compound conditions that determine
// which plan flags get set in GenerateLimitOffsetPlan.
//
// Condition 1: sel.OrderBy != nil && sel.OrderBy.Len() > 0
//
//	A: OrderBy=nil         → first sub false (ApplyAfterSort=false)
//	B: OrderBy non-nil, len=0 → second sub false (ApplyAfterSort=false)
//	C: OrderBy non-nil, len>0 → both true → ApplyAfterSort=true
//
// Condition 2: sel.GroupBy != nil && sel.GroupBy.Len() > 0  (only reached when C false)
//
//	D: GroupBy=nil         → first sub false (ApplyAfterGroup=false)
//	E: GroupBy non-nil, len=0 → second sub false (ApplyAfterGroup=false)
//	F: GroupBy non-nil, len>0 → both true → ApplyAfterGroup=true
func TestMCDC_GenerateLimitOffsetPlan(t *testing.T) {
	t.Parallel()

	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name           string
		sel            *Select
		wantAfterSort  bool
		wantAfterGroup bool
		wantDuringScan bool
	}{
		// Condition 1 sub-A: OrderBy=nil
		{
			name:          "MCDC_nil_orderby_not_after_sort",
			sel:           &Select{Limit: 5, OrderBy: nil},
			wantAfterSort: false, wantDuringScan: true,
		},
		// Condition 1 sub-B: OrderBy non-nil but empty (Len=0)
		{
			name:          "MCDC_empty_orderby_not_after_sort",
			sel:           &Select{Limit: 5, OrderBy: NewExprList()},
			wantAfterSort: false, wantDuringScan: true,
		},
		// Condition 1 sub-C: OrderBy with items → ApplyAfterSort
		{
			name: "MCDC_orderby_with_items_after_sort",
			sel: &Select{
				Limit:   5,
				OrderBy: &ExprList{Items: []ExprListItem{{Name: "col1"}}},
			},
			wantAfterSort: true, wantDuringScan: false,
		},
		// Condition 2 sub-D: GroupBy=nil (OrderBy also nil)
		{
			name:           "MCDC_nil_groupby_not_after_group",
			sel:            &Select{Limit: 5, GroupBy: nil},
			wantAfterGroup: false, wantDuringScan: true,
		},
		// Condition 2 sub-E: GroupBy non-nil but empty
		{
			name:           "MCDC_empty_groupby_not_after_group",
			sel:            &Select{Limit: 5, GroupBy: NewExprList()},
			wantAfterGroup: false, wantDuringScan: true,
		},
		// Condition 2 sub-F: GroupBy with items → ApplyAfterGroup
		{
			name: "MCDC_groupby_with_items_after_group",
			sel: &Select{
				Limit:   5,
				GroupBy: &ExprList{Items: []ExprListItem{{Name: "col1"}}},
			},
			wantAfterGroup: true, wantDuringScan: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan, err := lc.GenerateLimitOffsetPlan(tt.sel)
			if err != nil {
				t.Fatalf("GenerateLimitOffsetPlan() returned unexpected error: %v", err)
			}
			if plan == nil {
				t.Fatal("GenerateLimitOffsetPlan() returned nil plan")
			}
			if plan.ApplyAfterSort != tt.wantAfterSort {
				t.Errorf("ApplyAfterSort = %v, want %v", plan.ApplyAfterSort, tt.wantAfterSort)
			}
			if plan.ApplyAfterGroup != tt.wantAfterGroup {
				t.Errorf("ApplyAfterGroup = %v, want %v", plan.ApplyAfterGroup, tt.wantAfterGroup)
			}
			if plan.ApplyDuringScan != tt.wantDuringScan {
				t.Errorf("ApplyDuringScan = %v, want %v", plan.ApplyDuringScan, tt.wantDuringScan)
			}
		})
	}
}

// TestMCDC_EstimateDistinct exercises two key compound conditions in estimateDistinct.
//
// Condition 1: sampleSize == 0 || uniqueInSample == 0
//
//	A: sampleSize=0  → first sub true → return 1 (flips outcome independently)
//	B: sampleSize>0, uniqueInSample=0 → second sub true → return 1 (flips outcome)
//	C: both >0 → both false → proceed to calculation
//
// Condition 2 (inside C): sampleSize >= totalCount
//
//	D: sampleSize >= totalCount → return uniqueInSample exactly
//	E: sampleSize < totalCount  → extrapolate
func TestMCDC_EstimateDistinct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sampleSize     int64
		uniqueInSample int64
		totalCount     int64
		wantResult     int64
		wantAtLeast    int64 // minimum acceptable value (for extrapolated cases)
		wantAtMost     int64 // maximum acceptable value
	}{
		// Condition 1 sub-A: sampleSize=0 → returns 1
		{
			name:       "MCDC_zero_sample_size_returns_one",
			sampleSize: 0, uniqueInSample: 5, totalCount: 100,
			wantResult: 1,
		},
		// Condition 1 sub-B: uniqueInSample=0 → returns 1
		{
			name:       "MCDC_zero_unique_returns_one",
			sampleSize: 50, uniqueInSample: 0, totalCount: 100,
			wantResult: 1,
		},
		// Both zero
		{
			name:       "MCDC_both_zero_returns_one",
			sampleSize: 0, uniqueInSample: 0, totalCount: 100,
			wantResult: 1,
		},
		// Condition 2 sub-D: sampleSize >= totalCount → exact count
		{
			name:       "MCDC_sample_covers_total_returns_exact",
			sampleSize: 100, uniqueInSample: 42, totalCount: 100,
			wantResult: 42,
		},
		{
			name:       "MCDC_sample_exceeds_total_returns_exact",
			sampleSize: 200, uniqueInSample: 15, totalCount: 100,
			wantResult: 15,
		},
		// Condition 2 sub-E: sampleSize < totalCount → extrapolated value
		// We only check bounds: result in [uniqueInSample, totalCount]
		{
			name:       "MCDC_partial_sample_extrapolates",
			sampleSize: 10, uniqueInSample: 5, totalCount: 100,
			wantAtLeast: 5,   // >= uniqueInSample
			wantAtMost:  100, // <= totalCount
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := estimateDistinct(tt.sampleSize, tt.uniqueInSample, tt.totalCount)
			if tt.wantResult != 0 {
				if got != tt.wantResult {
					t.Errorf("estimateDistinct(%d, %d, %d) = %d, want %d",
						tt.sampleSize, tt.uniqueInSample, tt.totalCount, got, tt.wantResult)
				}
			} else {
				if got < tt.wantAtLeast {
					t.Errorf("estimateDistinct(%d, %d, %d) = %d, want >= %d",
						tt.sampleSize, tt.uniqueInSample, tt.totalCount, got, tt.wantAtLeast)
				}
				if got > tt.wantAtMost {
					t.Errorf("estimateDistinct(%d, %d, %d) = %d, want <= %d",
						tt.sampleSize, tt.uniqueInSample, tt.totalCount, got, tt.wantAtMost)
				}
			}
		})
	}
}

// TestMCDC_SplitLimitOffset exercises the compound condition in SplitLimitOffset.
//
// Condition: effective == 0 || offset == 0
//
//	A: effective=0 → first sub true → return effective unchanged (flips outcome)
//	B: effective>0, offset=0 → second sub true → return effective unchanged (flips outcome)
//	C: both >0 → both false → compute effective-offset (or 0 if negative)
func TestMCDC_SplitLimitOffset(t *testing.T) {
	t.Parallel()

	p := &Parse{DB: &Database{Name: "test"}}
	lc := NewLimitCompiler(p)

	tests := []struct {
		name      string
		effective int
		offset    int
		want      int
	}{
		// A: effective=0 → returns 0
		{name: "MCDC_zero_effective_returns_zero", effective: 0, offset: 5, want: 0},
		// B: offset=0 → returns effective as-is
		{name: "MCDC_zero_offset_returns_effective", effective: 15, offset: 0, want: 15},
		// C sub-case: effective > offset → effective-offset
		{name: "MCDC_effective_gt_offset_returns_diff", effective: 15, offset: 5, want: 10},
		// C sub-case: effective <= offset → returns 0
		{name: "MCDC_effective_eq_offset_returns_zero", effective: 5, offset: 5, want: 0},
		{name: "MCDC_effective_lt_offset_returns_zero", effective: 3, offset: 10, want: 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := lc.SplitLimitOffset(tt.effective, tt.offset)
			if got != tt.want {
				t.Errorf("SplitLimitOffset(%d, %d) = %d, want %d",
					tt.effective, tt.offset, got, tt.want)
			}
		})
	}
}
