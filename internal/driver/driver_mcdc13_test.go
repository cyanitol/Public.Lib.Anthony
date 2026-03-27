// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// MC/DC 13 — internal + SQL injection for remaining low-coverage paths
//
// Targets:
//   trigger_runtime.go:117   substituteReferences  (75.0%) — nil triggerRow early return
//   compile_vtab.go:154      binaryOpToConstraint  (72.7%) — default (unsupported op)

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// substituteReferences — nil triggerRow early return
// ---------------------------------------------------------------------------

// TestMCDC13_SubstituteReferences_NilRow exercises the nil-triggerRow early
// return in substituteReferences (returns stmt unchanged without substitution).
func TestMCDC13_SubstituteReferences_NilRow(t *testing.T) {
	t.Parallel()

	tr := &TriggerRuntime{conn: &Conn{}}
	stmt := &parser.SelectStmt{}

	got, err := tr.substituteReferences(stmt, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != stmt {
		t.Error("expected stmt returned unchanged when triggerRow is nil")
	}
}

// ---------------------------------------------------------------------------
// binaryOpToConstraint — default (unsupported operator) path
// ---------------------------------------------------------------------------

// TestMCDC13_BinaryOpToConstraint_Default exercises the default branch in
// binaryOpToConstraint by calling it with an arithmetic operator that does not
// map to any vtab ConstraintOp (e.g., OpPlus = +).
func TestMCDC13_BinaryOpToConstraint_Default(t *testing.T) {
	t.Parallel()

	_, ok := binaryOpToConstraint(parser.OpPlus)
	if ok {
		t.Error("expected ok=false for unsupported operator OpPlus")
	}
}

// TestMCDC13_BinaryOpToConstraint_Concat exercises the default path with
// OpConcat (||), another unsupported vtab constraint operator.
func TestMCDC13_BinaryOpToConstraint_Concat(t *testing.T) {
	t.Parallel()

	_, ok := binaryOpToConstraint(parser.OpConcat)
	if ok {
		t.Error("expected ok=false for unsupported operator OpConcat")
	}
}

// ---------------------------------------------------------------------------
// resolveWindowStateIdx — map hit (successful lookup) path
// ---------------------------------------------------------------------------

// TestMCDC13_ResolveWindowStateIdx_MapHit exercises the successful map lookup
// path in resolveWindowStateIdx by pre-populating windowStateMap.
func TestMCDC13_ResolveWindowStateIdx_MapHit(t *testing.T) {
	t.Parallel()

	s := &Stmt{}
	over := &parser.WindowSpec{}
	fn := &parser.FunctionExpr{Name: "ROW_NUMBER", Over: over}

	// Pre-populate the windowStateMap with the key that makeOverClauseKey
	// returns for an empty WindowSpec (no ORDER BY, no PARTITION BY).
	key := s.makeOverClauseKey(nil, nil)
	s.windowStateMap = map[string]int{key: 42}

	idx := s.resolveWindowStateIdx(fn, nil)
	if idx != 42 {
		t.Errorf("expected windowStateMap hit (42), got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// emitNonIdentifierColumn — nil gen early return
// ---------------------------------------------------------------------------

// TestMCDC13_EmitNonIdentifierColumn_NilGen exercises the gen==nil early
// return in emitNonIdentifierColumn (emits OpNull and returns nil).
func TestMCDC13_EmitNonIdentifierColumn_NilGen(t *testing.T) {
	t.Parallel()

	s := &Stmt{}
	vm := vdbe.New()
	vm.AllocMemory(4)
	col := parser.ResultColumn{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}}

	err := s.emitNonIdentifierColumn(vm, col, 1, nil)
	if err != nil {
		t.Errorf("unexpected error with nil gen: %v", err)
	}
}
