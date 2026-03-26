// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

// MC/DC (Modified Condition/Decision Coverage) tests for the schema package — file 2.
//
// This file covers compound boolean conditions that were NOT covered in
// schema_mcdc_test.go.  Each test group is documented with:
//   - source file:line
//   - the exact compound condition
//   - sub-condition labels
//
// Run with: go test -run MCDC ./internal/schema/...

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// ---------------------------------------------------------------------------
// 15. trigger.go – validateTriggerTarget
//
//     Condition (line ~80):
//       !s.tableExistsLocked(stmt.Table) && !s.viewExistsLocked(stmt.Table)
//
//     Sub-conditions:
//       A = !tableExists   (table does NOT exist)
//       B = !viewExists    (view does NOT exist)
//     Compound: A && B  → both must be absent to return "table not found"
//
//     MC/DC pairs:
//       case 1: A=T B=T → error (neither table nor view found)
//       case 2: A=F B=T → no error on first guard (table exists)
//       case 3: A=T B=F → no error on first guard (view exists)
// ---------------------------------------------------------------------------

func TestMCDC_ValidateTriggerTarget_NotFoundGuard(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T → neither table nor view → error
	t.Run("MCDC_trigger_target_no_table_no_view_A_true_B_true", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		stmt := &parser.CreateTriggerStmt{
			Name:  "trg",
			Table: "nonexistent",
		}
		err := s.validateTriggerTarget(stmt)
		if err == nil {
			t.Error("expected error when table and view both absent")
		}
	})

	// case 2: A=F, B=T → table exists → no error from the first guard
	t.Run("MCDC_trigger_target_table_exists_A_false_B_true", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		s.Tables["users"] = &Table{Name: "users"}
		stmt := &parser.CreateTriggerStmt{
			Name:   "trg",
			Table:  "users",
			Timing: parser.TriggerAfter,
			Event:  parser.TriggerInsert,
		}
		err := s.validateTriggerTarget(stmt)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// case 3: A=T, B=F → view exists → no error from the first guard
	t.Run("MCDC_trigger_target_view_exists_A_true_B_false", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		s.Views["v_users"] = &View{Name: "v_users"}
		stmt := &parser.CreateTriggerStmt{
			Name:   "trg",
			Table:  "v_users",
			Timing: parser.TriggerInsteadOf,
			Event:  parser.TriggerInsert,
		}
		err := s.validateTriggerTarget(stmt)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 16. trigger.go – validateTriggerTarget INSTEAD OF guard
//
//     Condition (line ~83):
//       stmt.Timing == parser.TriggerInsteadOf && !s.viewExistsLocked(stmt.Table)
//
//     Sub-conditions:
//       A = (stmt.Timing == TriggerInsteadOf)
//       B = !viewExists
//     Compound: A && B  → INSTEAD OF on a non-view → error
//
//     MC/DC pairs:
//       case 1: A=T B=T → error (INSTEAD OF on table, not a view)
//       case 2: A=T B=F → no error (INSTEAD OF on a view is valid)
//       case 3: A=F B=T → no error (non-INSTEAD OF on table is valid)
// ---------------------------------------------------------------------------

func TestMCDC_ValidateTriggerTarget_InsteadOfGuard(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T → INSTEAD OF, but target is a table → error
	t.Run("MCDC_trigger_instead_of_table_A_true_B_true", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		s.Tables["users"] = &Table{Name: "users"}
		stmt := &parser.CreateTriggerStmt{
			Name:   "trg",
			Table:  "users",
			Timing: parser.TriggerInsteadOf,
			Event:  parser.TriggerInsert,
		}
		err := s.validateTriggerTarget(stmt)
		if err == nil {
			t.Error("expected error for INSTEAD OF on a regular table")
		}
	})

	// case 2: A=T, B=F → INSTEAD OF on a view → no error
	t.Run("MCDC_trigger_instead_of_view_A_true_B_false", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		s.Views["v_users"] = &View{Name: "v_users"}
		stmt := &parser.CreateTriggerStmt{
			Name:   "trg",
			Table:  "v_users",
			Timing: parser.TriggerInsteadOf,
			Event:  parser.TriggerInsert,
		}
		err := s.validateTriggerTarget(stmt)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// case 3: A=F, B=T → AFTER trigger on a table (not INSTEAD OF) → no error
	t.Run("MCDC_trigger_after_table_A_false_B_true", func(t *testing.T) {
		t.Parallel()
		s := NewSchema()
		s.Tables["orders"] = &Table{Name: "orders"}
		stmt := &parser.CreateTriggerStmt{
			Name:   "trg",
			Table:  "orders",
			Timing: parser.TriggerAfter,
			Event:  parser.TriggerInsert,
		}
		err := s.validateTriggerTarget(stmt)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 17. trigger.go – evaluateBinaryExpr logical dispatch
//
//     Condition (line ~208):
//       expr.Op == parser.OpAnd || expr.Op == parser.OpOr
//
//     Sub-conditions:
//       A = (expr.Op == OpAnd)
//       B = (expr.Op == OpOr)
//     Compound: A || B
//
//     MC/DC pairs:
//       case 1: A=T B=F → logical path (AND) → evaluateLogicalOp
//       case 2: A=F B=T → logical path (OR)  → evaluateLogicalOp
//       case 3: A=F B=F → comparison path    → evaluateComparisonOp
// ---------------------------------------------------------------------------

func TestMCDC_EvaluateBinaryExpr_LogicalDispatch(t *testing.T) {
	t.Parallel()

	lit1 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	lit0 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}

	tests := []struct {
		name    string
		expr    *parser.BinaryExpr
		oldRow  map[string]interface{}
		newRow  map[string]interface{}
		want    bool
		wantErr bool
	}{
		{
			// A=T, B=F: AND  →  1 AND 0 = false
			name:   "MCDC_trigger_eval_OpAnd_A_true_B_false",
			expr:   &parser.BinaryExpr{Left: lit1, Op: parser.OpAnd, Right: lit0},
			want:   false,
			newRow: nil,
			oldRow: nil,
		},
		{
			// A=F, B=T: OR  →  1 OR 0 = true
			name:   "MCDC_trigger_eval_OpOr_A_false_B_true",
			expr:   &parser.BinaryExpr{Left: lit1, Op: parser.OpOr, Right: lit0},
			want:   true,
			newRow: nil,
			oldRow: nil,
		},
		{
			// A=F, B=F: comparison (OpEq)  →  1 == 1 = true
			name:   "MCDC_trigger_eval_OpEq_A_false_B_false",
			expr:   &parser.BinaryExpr{Left: lit1, Op: parser.OpEq, Right: lit1},
			want:   true,
			newRow: nil,
			oldRow: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := evaluateBinaryExpr(tt.expr, tt.oldRow, tt.newRow)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("evaluateBinaryExpr() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 18. database.go – checkAttachLimit special-name guard
//
//     Condition (line ~64):
//       lowerName == "main" || lowerName == "temp"
//
//     Sub-conditions:
//       A = (lowerName == "main")
//       B = (lowerName == "temp")
//     Compound: A || B  → if either, skip limit check
//
//     MC/DC pairs:
//       case 1: A=T B=F → "main" → no limit error
//       case 2: A=F B=T → "temp" → no limit error
//       case 3: A=F B=F → other  → limit check performed
// ---------------------------------------------------------------------------

func TestMCDC_CheckAttachLimit_SpecialNameGuard(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=F → "main" is always allowed
	t.Run("MCDC_attach_limit_main_A_true_B_false", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		// Fill up the registry with MaxAttachedDatabases user databases
		for i := 0; i < MaxAttachedDatabases; i++ {
			name := "db" + string(rune('a'+i))
			dr.databases[name] = &Database{Name: name}
		}
		err := dr.checkAttachLimit("main")
		if err != nil {
			t.Errorf("expected no error for 'main', got: %v", err)
		}
	})

	// case 2: A=F, B=T → "temp" is always allowed
	t.Run("MCDC_attach_limit_temp_A_false_B_true", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		for i := 0; i < MaxAttachedDatabases; i++ {
			name := "db" + string(rune('a'+i))
			dr.databases[name] = &Database{Name: name}
		}
		err := dr.checkAttachLimit("temp")
		if err != nil {
			t.Errorf("expected no error for 'temp', got: %v", err)
		}
	})

	// case 3: A=F, B=F → ordinary user database → limit check fires
	t.Run("MCDC_attach_limit_user_db_A_false_B_false", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		for i := 0; i < MaxAttachedDatabases; i++ {
			name := "db" + string(rune('a'+i))
			dr.databases[name] = &Database{Name: name}
		}
		err := dr.checkAttachLimit("extra")
		if err == nil {
			t.Error("expected error when max attached databases reached")
		}
	})
}

// ---------------------------------------------------------------------------
// 19. database.go – checkAttachLimit user-DB exclusion inside counter loop
//
//     Condition (line ~69):
//       name != "main" && name != "temp"
//
//     Sub-conditions:
//       A = (name != "main")
//       B = (name != "temp")
//     Compound: A && B  → true means count this as a user DB
//
//     MC/DC pairs (observed via counter result):
//       case 1: A=T B=T → user DB counted (name = "foo")
//       case 2: A=F B=T → main not counted (name = "main")
//       case 3: A=T B=F → temp not counted (name = "temp")
// ---------------------------------------------------------------------------

func TestMCDC_CheckAttachLimit_UserDBExclusion(t *testing.T) {
	t.Parallel()

	// case 1: A=T, B=T → user database, counted → reaches limit
	t.Run("MCDC_attach_exclude_user_db_counted_A_true_B_true", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		// Exactly MaxAttachedDatabases user databases
		for i := 0; i < MaxAttachedDatabases; i++ {
			name := "user" + string(rune('a'+i))
			dr.databases[name] = &Database{Name: name}
		}
		// Trying to attach one more user database must fail
		err := dr.checkAttachLimit("newuser")
		if err == nil {
			t.Error("expected limit error when user DBs are exhausted")
		}
	})

	// case 2: A=F, B=T → "main" not counted, so limit not triggered
	t.Run("MCDC_attach_exclude_main_not_counted_A_false_B_true", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		// Fill with MaxAttachedDatabases - 1 user dbs and one "main"
		for i := 0; i < MaxAttachedDatabases-1; i++ {
			name := "user" + string(rune('a'+i))
			dr.databases[name] = &Database{Name: name}
		}
		dr.databases["main"] = &Database{Name: "main"}
		err := dr.checkAttachLimit("newuser")
		if err != nil {
			t.Errorf("unexpected error (main should not count): %v", err)
		}
	})

	// case 3: A=T, B=F → "temp" not counted, so limit not triggered
	t.Run("MCDC_attach_exclude_temp_not_counted_A_true_B_false", func(t *testing.T) {
		t.Parallel()
		dr := NewDatabaseRegistry()
		for i := 0; i < MaxAttachedDatabases-1; i++ {
			name := "user" + string(rune('a'+i))
			dr.databases[name] = &Database{Name: name}
		}
		dr.databases["temp"] = &Database{Name: "temp"}
		err := dr.checkAttachLimit("newuser")
		if err != nil {
			t.Errorf("unexpected error (temp should not count): %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 20. trigger.go – evaluateLogicalOp AND/OR dispatch
//
//     Condition (line ~230):
//       if expr.Op == parser.OpAnd  →  left && right  else  left || right
//
//     This is a simple if/else on Op, not a compound boolean, but it has two
//     independent branches that each flip the outcome:
//       A: Op == OpAnd → returns left && right
//       B: Op == OpOr  → returns left || right
//
//     MC/DC pairs (2 cases required):
//       case 1: Op == OpAnd, left=T, right=F → false
//       case 2: Op == OpOr,  left=F, right=T → true
// ---------------------------------------------------------------------------

func TestMCDC_EvaluateLogicalOp_AndOrBranch(t *testing.T) {
	t.Parallel()

	lit1 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	lit0 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}

	tests := []struct {
		name string
		expr *parser.BinaryExpr
		want bool
	}{
		{
			// OpAnd, left=1 right=0 → false
			name: "MCDC_logical_op_AND_true_false",
			expr: &parser.BinaryExpr{Left: lit1, Op: parser.OpAnd, Right: lit0},
			want: false,
		},
		{
			// OpOr, left=0 right=1 → true
			name: "MCDC_logical_op_OR_false_true",
			expr: &parser.BinaryExpr{Left: lit0, Op: parser.OpOr, Right: lit1},
			want: true,
		},
		{
			// OpAnd, left=1 right=1 → true (both true)
			name: "MCDC_logical_op_AND_both_true",
			expr: &parser.BinaryExpr{Left: lit1, Op: parser.OpAnd, Right: lit1},
			want: true,
		},
		{
			// OpOr, left=0 right=0 → false (both false)
			name: "MCDC_logical_op_OR_both_false",
			expr: &parser.BinaryExpr{Left: lit0, Op: parser.OpOr, Right: lit0},
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := evaluateLogicalOp(tt.expr, nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("evaluateLogicalOp() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 21. trigger.go – compareLessThanOrEqual compound
//
//     Condition (line ~376):
//       compareLessThan(left, right) || compareEqual(left, right)
//
//     Sub-conditions:
//       A = compareLessThan(left, right)
//       B = compareEqual(left, right)
//     Compound: A || B
//
//     MC/DC pairs:
//       case 1: A=T B=F → left < right  → true
//       case 2: A=F B=T → left == right → true
//       case 3: A=F B=F → left > right  → false
// ---------------------------------------------------------------------------

func TestMCDC_CompareLessThanOrEqual(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		want  bool
	}{
		// A=T, B=F: 1 < 2  → true
		{"MCDC_lte_less_A_true_B_false", int64(1), int64(2), true},
		// A=F, B=T: 2 == 2 → true
		{"MCDC_lte_equal_A_false_B_true", int64(2), int64(2), true},
		// A=F, B=F: 3 > 2  → false
		{"MCDC_lte_greater_A_false_B_false", int64(3), int64(2), false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compareLessThanOrEqual(tt.left, tt.right)
			if got != tt.want {
				t.Errorf("compareLessThanOrEqual(%v, %v) = %v, want %v",
					tt.left, tt.right, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 22. trigger.go – compareGreaterThan compound
//
//     Condition (line ~381):
//       !compareLessThan(left, right) && !compareEqual(left, right)
//
//     Sub-conditions:
//       A = !compareLessThan  (left is NOT less)
//       B = !compareEqual     (left is NOT equal)
//     Compound: A && B  → both must hold for left > right
//
//     MC/DC pairs:
//       case 1: A=T B=T → 3 > 2  → true
//       case 2: A=T B=F → 2 == 2 → false (B flips)
//       case 3: A=F B=T → 1 < 2  → false (A flips)
// ---------------------------------------------------------------------------

func TestMCDC_CompareGreaterThan(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		want  bool
	}{
		// A=T, B=T: 3 > 2 → true
		{"MCDC_gt_greater_A_true_B_true", int64(3), int64(2), true},
		// A=T, B=F: 2 == 2 → false (not less, but equal)
		{"MCDC_gt_equal_A_true_B_false", int64(2), int64(2), false},
		// A=F, B=T: 1 < 2 → false (less than, so A is false)
		{"MCDC_gt_less_A_false_B_true", int64(1), int64(2), false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compareGreaterThan(tt.left, tt.right)
			if got != tt.want {
				t.Errorf("compareGreaterThan(%v, %v) = %v, want %v",
					tt.left, tt.right, got, tt.want)
			}
		})
	}
}
