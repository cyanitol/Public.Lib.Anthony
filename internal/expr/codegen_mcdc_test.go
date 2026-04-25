// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// MC/DC Coverage Tests for internal/expr/codegen.go
//
// For each compound boolean condition, N+1 test cases are provided where each
// sub-condition independently determines the overall outcome.
// ============================================================================

// ----------------------------------------------------------------------------
// Condition: strings.HasPrefix(clean, "0x") || strings.HasPrefix(clean, "0X")
// (generateIntegerLiteral, line ~440)
//
// Sub-conditions:
//   A = HasPrefix(clean, "0x")
//   B = HasPrefix(clean, "0X")
// MC/DC cases (3 needed):
//   A=true,  B=false  → overall true  (hex lowercase)
//   A=false, B=true   → overall true  (hex uppercase)
//   A=false, B=false  → overall false (decimal)
// ----------------------------------------------------------------------------

func TestMCDC_GenerateIntegerLiteral_HexPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string // documents A, B states
		value      string
		wantOpcode vdbe.Opcode
	}{
		{
			name:       "A=true B=false: lowercase 0x prefix → hex path",
			value:      "0xff",
			wantOpcode: vdbe.OpInteger,
		},
		{
			name:       "A=false B=true: uppercase 0X prefix → hex path",
			value:      "0XFF",
			wantOpcode: vdbe.OpInteger,
		},
		{
			name:       "A=false B=false: decimal → decimal path",
			value:      "255",
			wantOpcode: vdbe.OpInteger,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: tt.value}
			_, err := gen.GenerateExpr(e)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}
			if len(v.Program) == 0 {
				t.Fatal("expected at least one instruction")
			}
			if v.Program[0].Opcode != tt.wantOpcode {
				t.Errorf("opcode: got %s, want %s", v.Program[0].Opcode.String(), tt.wantOpcode.String())
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: val >= -2147483648 && val <= 2147483647
// (generateIntegerLiteral, line ~456)
//
// Sub-conditions:
//   A = val >= -2147483648
//   B = val <= 2147483647
// MC/DC cases (3 needed):
//   A=true,  B=true  → OpInteger   (value fits in 32-bit)
//   A=true,  B=false → OpInt64     (value exceeds upper 32-bit bound)
//   A=false, B=true  → OpInt64     (value below lower 32-bit bound)
// ----------------------------------------------------------------------------

func TestMCDC_GenerateIntegerLiteral_Int32Range(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string // documents A, B states
		value      string
		wantOpcode vdbe.Opcode
	}{
		{
			name:       "A=true B=true: value 0 fits int32 → OpInteger",
			value:      "0",
			wantOpcode: vdbe.OpInteger,
		},
		{
			name:       "A=true B=false: value 2147483648 exceeds int32 upper → OpInt64",
			value:      "2147483648",
			wantOpcode: vdbe.OpInt64,
		},
		{
			name:       "A=false B=true: value -2147483649 below int32 lower → OpInt64",
			value:      "-2147483649",
			wantOpcode: vdbe.OpInt64,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: tt.value}
			_, err := gen.GenerateExpr(e)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}
			if len(v.Program) == 0 {
				t.Fatal("expected at least one instruction")
			}
			if v.Program[0].Opcode != tt.wantOpcode {
				t.Errorf("opcode: got %s, want %s", v.Program[0].Opcode.String(), tt.wantOpcode.String())
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: e.Op == parser.OpAnd || e.Op == parser.OpOr
// (generateBinary, line ~651)
//
// Sub-conditions:
//   A = (e.Op == parser.OpAnd)
//   B = (e.Op == parser.OpOr)
// MC/DC cases (3 needed):
//   A=true,  B=false → logical path (AND)
//   A=false, B=true  → logical path (OR)
//   A=false, B=false → standard binary path (e.g. OpPlus → OpAdd)
// ----------------------------------------------------------------------------

func TestMCDC_GenerateBinary_AndOrDispatch(t *testing.T) {
	t.Parallel()

	lit1 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	lit0 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}

	tests := []struct {
		name       string
		expr       *parser.BinaryExpr
		wantOpcode vdbe.Opcode // opcode we expect somewhere in the program
	}{
		{
			name:       "A=true B=false: OpAnd → logical AND path (emits OpAnd)",
			expr:       &parser.BinaryExpr{Left: lit1, Op: parser.OpAnd, Right: lit0},
			wantOpcode: vdbe.OpAnd,
		},
		{
			name:       "A=false B=true: OpOr → logical OR path (emits OpOr)",
			expr:       &parser.BinaryExpr{Left: lit1, Op: parser.OpOr, Right: lit0},
			wantOpcode: vdbe.OpOr,
		},
		{
			name:       "A=false B=false: OpPlus → standard path (emits OpAdd)",
			expr:       &parser.BinaryExpr{Left: lit1, Op: parser.OpPlus, Right: lit0},
			wantOpcode: vdbe.OpAdd,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}
			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantOpcode {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected opcode %s in program, not found", tt.wantOpcode.String())
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: e.Op == parser.OpLike && e.Escape != nil
// (generateBinary, line ~656)
//
// Sub-conditions:
//   A = (e.Op == parser.OpLike)
//   B = (e.Escape != nil)
// MC/DC cases (3 needed):
//   A=true,  B=true  → LIKE with ESCAPE path (3-arg function call, P5=3)
//   A=true,  B=false → regular LIKE path (2-arg function call, P5=2)
//   A=false, B=true  → not LIKE, so escape is irrelevant; goes to standard binary path
//                       (B irrelevant when A=false: use OpEq with Escape set to show it is ignored)
// ----------------------------------------------------------------------------

func TestMCDC_GenerateBinary_LikeEscapeCondition(t *testing.T) {
	t.Parallel()

	litVal := &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}
	litPat := &parser.LiteralExpr{Type: parser.LiteralString, Value: "%ell%"}
	litEsc := &parser.LiteralExpr{Type: parser.LiteralString, Value: "\\"}

	tests := []struct {
		name   string
		expr   *parser.BinaryExpr
		wantP5 uint16 // expected function arg-count (P5 on OpFunction)
		wantOp vdbe.Opcode
	}{
		{
			name:   "A=true B=true: LIKE with Escape → 3-arg like function",
			expr:   &parser.BinaryExpr{Left: litVal, Op: parser.OpLike, Right: litPat, Escape: litEsc},
			wantOp: vdbe.OpFunction,
			wantP5: 3,
		},
		{
			name:   "A=true B=false: LIKE without Escape → 2-arg like function",
			expr:   &parser.BinaryExpr{Left: litVal, Op: parser.OpLike, Right: litPat, Escape: nil},
			wantOp: vdbe.OpFunction,
			wantP5: 2,
		},
		{
			name:   "A=false B=true: not LIKE (OpEq with Escape set) → standard equality",
			expr:   &parser.BinaryExpr{Left: litVal, Op: parser.OpEq, Right: litVal, Escape: litEsc},
			wantOp: vdbe.OpEq,
			wantP5: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}
			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantOp {
					if tt.wantOp == vdbe.OpFunction {
						if instr.P5 == tt.wantP5 {
							found = true
							break
						}
					} else {
						found = true
						break
					}
				}
			}
			if !found {
				t.Errorf("expected opcode %s (P5=%d) in program, not found", tt.wantOp.String(), tt.wantP5)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: isComparison && collation != ""
// (emitBinaryOpcode, line ~715)
//
// Sub-conditions:
//   A = isComparison  (op >= OpEq && op <= OpGe)
//   B = collation != ""
// MC/DC cases (3 needed):
//   A=true,  B=true  → collated comparison path (P4Type set to P4Static)
//   A=true,  B=false → plain comparison path (no collation set on instruction)
//   A=false, B=true  → arithmetic op with collation tracked but not applied
//                       (collation irrelevant for non-comparison ops)
//
// We drive collation through SetCollationForReg on the result register before
// using it, which is what generateColumn normally does.
// ----------------------------------------------------------------------------

// runCollationConditionCase is a helper for TestMCDC_EmitBinaryOpcode_CollationCondition subtests.
func runCollationConditionCase(t *testing.T, left, right parser.Expression, op parser.BinaryOp, collation string, wantP4Type bool, wantOpcode vdbe.Opcode) {
	t.Helper()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	leftReg, err := gen.GenerateExpr(left)
	if err != nil {
		t.Fatalf("GenerateExpr(left) failed: %v", err)
	}
	if collation != "" {
		gen.SetCollationForReg(leftReg, collation)
	}

	rightReg, err := gen.GenerateExpr(right)
	if err != nil {
		t.Fatalf("GenerateExpr(right) failed: %v", err)
	}

	_, err = gen.generateStandardBinaryOp(op, leftReg, rightReg)
	if err != nil {
		t.Fatalf("generateStandardBinaryOp failed: %v", err)
	}

	verifyOpcodeP4(t, v.Program, wantOpcode, wantP4Type)
}

// verifyOpcodeP4 checks that the expected opcode exists and has the expected P4Static state.
func verifyOpcodeP4(t *testing.T, program []*vdbe.Instruction, wantOpcode vdbe.Opcode, wantP4Type bool) {
	t.Helper()
	for _, instr := range program {
		if instr.Opcode == wantOpcode {
			gotP4Static := instr.P4Type == vdbe.P4Static
			if gotP4Static != wantP4Type {
				t.Errorf("P4Static: got %v, want %v", gotP4Static, wantP4Type)
			}
			return
		}
	}
	t.Errorf("expected opcode %s in program, not found", wantOpcode.String())
}

func TestMCDC_EmitBinaryOpcode_CollationCondition(t *testing.T) {
	t.Parallel()

	litA := &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}
	litB := &parser.LiteralExpr{Type: parser.LiteralString, Value: "world"}

	tests := []struct {
		name       string
		left       parser.Expression
		right      parser.Expression
		op         parser.BinaryOp
		collation  string // collation to pre-register on the left register
		wantP4Type bool   // true if we expect P4Static to be set on the key instruction
		wantOpcode vdbe.Opcode
	}{
		{
			name:       "A=true B=true: EQ comparison with NOCASE collation → collated path",
			left:       litA,
			right:      litB,
			op:         parser.OpEq,
			collation:  "NOCASE",
			wantP4Type: true,
			wantOpcode: vdbe.OpEq,
		},
		{
			name:       "A=true B=false: EQ comparison without collation → plain path",
			left:       litA,
			right:      litB,
			op:         parser.OpEq,
			collation:  "",
			wantP4Type: false,
			wantOpcode: vdbe.OpEq,
		},
		{
			name:       "A=false B=true: ADD (non-comparison) with collation → collation ignored",
			left:       litA,
			right:      litB,
			op:         parser.OpPlus,
			collation:  "NOCASE",
			wantP4Type: false,
			wantOpcode: vdbe.OpAdd,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runCollationConditionCase(t, tt.left, tt.right, tt.op, tt.collation, tt.wantP4Type, tt.wantOpcode)
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: g.args == nil || g.paramIdx >= len(g.args)
// (generateVariable, line ~1889)
//
// Sub-conditions:
//   A = (g.args == nil)
//   B = (g.paramIdx >= len(g.args))
// MC/DC cases (3 needed):
//   A=true,  B=irrelevant → null param emitted  (args is nil)
//   A=false, B=true       → null param emitted  (args set but exhausted)
//   A=false, B=false      → real param emitted  (args available)
// ----------------------------------------------------------------------------

func TestMCDC_GenerateVariable_ArgsCondition(t *testing.T) {
	t.Parallel()

	varExpr := &parser.VariableExpr{Name: "?"}

	tests := []struct {
		name       string
		args       []interface{} // nil means A=true
		paramIdx   int
		wantOpcode vdbe.Opcode
	}{
		{
			name:       "A=true B=n/a: args nil → OpNull (no value)",
			args:       nil,
			paramIdx:   0,
			wantOpcode: vdbe.OpNull,
		},
		{
			name:       "A=false B=true: args set but paramIdx exhausted → OpNull",
			args:       []interface{}{int64(42)},
			paramIdx:   1, // paramIdx >= len(args)
			wantOpcode: vdbe.OpNull,
		},
		{
			name:       "A=false B=false: args available → OpInteger (real value)",
			args:       []interface{}{int64(42)},
			paramIdx:   0, // paramIdx < len(args)
			wantOpcode: vdbe.OpInteger,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			gen.SetArgs(tt.args)
			gen.SetParamIndex(tt.paramIdx)

			_, err := gen.GenerateExpr(varExpr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}
			if len(v.Program) == 0 {
				t.Fatal("expected at least one instruction")
			}
			if v.Program[0].Opcode != tt.wantOpcode {
				t.Errorf("opcode: got %s, want %s", v.Program[0].Opcode.String(), tt.wantOpcode.String())
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition (blob literal, line ~488):
//   len(hexStr) >= 3 && (hexStr[0]=='X' || hexStr[0]=='x') && hexStr[1]=='\'' && hexStr[len-1]=='\''
//
// This is A && (B || C) && D && E — four effective sub-conditions treating
// (B||C) as a single unit.  MC/DC for the full conjunction requires flipping
// each sub-condition independently:
//   len<3               → overall false  (A false, rest irrelevant)
//   len>=3, bad[0]      → overall false  (B||C false, A & D & E would be true)
//   len>=3, good[0], bad[1] → overall false  (D false)
//   len>=3, good[0], good[1], bad[-1] → overall false (E false)
//   All true            → overall true (strip and decode)
//
// Rather than testing internal predicate directly, we drive it via the
// public GenerateExpr path and observe whether the blob was decoded correctly.
// ----------------------------------------------------------------------------

func TestMCDC_GenerateBlobLiteral_WrapperCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string // documents which sub-condition is false
		value     string // raw lexeme passed as LiteralExpr.Value
		wantError bool
	}{
		{
			// A=false: len < 3 → skip strip, pass raw to hex.DecodeString → error
			name:      "A=false: too short (len<3) → no strip → hex decode fails",
			value:     "X'",
			wantError: true,
		},
		{
			// B||C = false: first char is not X or x → no strip → raw passed → error
			name:      "B||C=false: first byte not X/x → no strip → hex decode fails",
			value:     "Y'FF'",
			wantError: true,
		},
		{
			// D = false: second char not single-quote → no strip
			// "XzFF'" — starts with X, second char 'z' (not '), last char '
			name:      "D=false: second byte not quote → no strip → hex decode fails",
			value:     "XzFF'",
			wantError: true,
		},
		{
			// E = false: last char not single-quote → no strip
			// "X'FF" — starts correctly but missing trailing quote
			name:      "E=false: last byte not quote → no strip → hex decode fails",
			value:     "X'FF",
			wantError: true,
		},
		{
			// All true: canonical X'...' form → strip succeeds, blob decoded
			name:      "All=true: canonical X'FF' form → strip → blob decoded OK",
			value:     "X'FF'",
			wantError: false,
		},
		{
			// B=false C=true: lowercase x → also strips correctly
			name:      "B=false C=true: lowercase x'FF' form → strip → blob decoded OK",
			value:     "x'FF'",
			wantError: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			e := &parser.LiteralExpr{Type: parser.LiteralBlob, Value: tt.value}
			_, err := gen.GenerateExpr(e)
			if tt.wantError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: v >= -2147483648 && v <= 2147483647
// (emitInt64Value, line ~1969)
//
// Sub-conditions:
//   A = (v >= -2147483648)
//   B = (v <= 2147483647)
// MC/DC cases (3 needed):
//   A=true,  B=true  → OpInteger  (fits int32)
//   A=true,  B=false → OpInt64   (exceeds upper bound)
//   A=false, B=true  → OpInt64   (below lower bound)
// ----------------------------------------------------------------------------

func TestMCDC_EmitInt64Value_RangeCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arg        int64
		wantOpcode vdbe.Opcode
	}{
		{
			name:       "A=true B=true: 100 fits int32 → OpInteger",
			arg:        100,
			wantOpcode: vdbe.OpInteger,
		},
		{
			name:       "A=true B=false: 2147483648 exceeds upper → OpInt64",
			arg:        2147483648,
			wantOpcode: vdbe.OpInt64,
		},
		{
			name:       "A=false B=true: -2147483649 below lower → OpInt64",
			arg:        -2147483649,
			wantOpcode: vdbe.OpInt64,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			// Drive emitInt64Value via the variable/parameter path
			varExpr := &parser.VariableExpr{Name: "?"}
			gen.SetArgs([]interface{}{tt.arg})
			_, err := gen.GenerateExpr(varExpr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}
			if len(v.Program) == 0 {
				t.Fatal("expected at least one instruction")
			}
			if v.Program[0].Opcode != tt.wantOpcode {
				t.Errorf("opcode: got %s, want %s", v.Program[0].Opcode.String(), tt.wantOpcode.String())
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: rule.adjustP2 && rule.adjustP3
// (adjustSubqueryJumpTargets, line ~2129)
//
// Sub-conditions:
//   A = rule.adjustP2
//   B = rule.adjustP3
// MC/DC cases (3 needed):
//   A=true,  B=true  → adjustDualJump called (OpInitCoroutine has both)
//   A=true,  B=false → adjustJumpP2 called   (OpGoto has only P2)
//   A=false, B=false → neither called         (an opcode not in the table)
//
// We test via adjustSubqueryJumpTargets which is package-internal.
// Construct a minimal subVM with one instruction of each kind, embed at offset
// 10, and verify P2/P3 are adjusted correctly.
// ----------------------------------------------------------------------------

func TestMCDC_AdjustSubqueryJumpTargets_AdjustP2AndP3(t *testing.T) {
	t.Parallel()

	const baseAddr = 10

	tests := []struct {
		name   string
		opcode vdbe.Opcode
		p2in   int
		p3in   int
		wantP2 int
		wantP3 int
	}{
		{
			// A=true B=true: OpInitCoroutine → both P2 and P3 adjusted
			name:   "A=true B=true: OpInitCoroutine adjusts P2 and P3",
			opcode: vdbe.OpInitCoroutine,
			p2in:   5,
			p3in:   3,
			wantP2: 5 + baseAddr,
			wantP3: 3 + baseAddr,
		},
		{
			// A=true B=false: OpGoto → only P2 adjusted
			name:   "A=true B=false: OpGoto adjusts P2 only",
			opcode: vdbe.OpGoto,
			p2in:   5,
			p3in:   3,
			wantP2: 5 + baseAddr,
			wantP3: 3, // unchanged
		},
		{
			// A=false B=false: OpNoop → not in table, nothing adjusted
			name:   "A=false B=false: OpNoop not in table, no adjustment",
			opcode: vdbe.OpNoop,
			p2in:   5,
			p3in:   3,
			wantP2: 5, // unchanged
			wantP3: 3, // unchanged
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			subVM := vdbe.New()
			subVM.AddOp(tt.opcode, 0, tt.p2in, tt.p3in)

			v := vdbe.New()
			gen := NewCodeGenerator(v)
			gen.adjustSubqueryJumpTargets(subVM, baseAddr)

			gotP2 := subVM.Program[0].P2
			gotP3 := subVM.Program[0].P3
			if gotP2 != tt.wantP2 {
				t.Errorf("P2: got %d, want %d", gotP2, tt.wantP2)
			}
			if gotP3 != tt.wantP3 {
				t.Errorf("P3: got %d, want %d", gotP3, tt.wantP3)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: len(rows) == 0 || len(rows[0]) == 0
// (generateSubqueryMaterialised, line ~1395)
//
// This is tested indirectly via the SubqueryExecutor callback since
// generateSubqueryMaterialised is not exported.
//
// Sub-conditions:
//   A = (len(rows) == 0)
//   B = (len(rows[0]) == 0)
// MC/DC cases (3 needed):
//   A=true,  B=irrelevant → OpNull  (no rows at all)
//   A=false, B=true       → OpNull  (row exists but has no columns)
//   A=false, B=false      → OpInteger (row with a real value)
// ----------------------------------------------------------------------------

func TestMCDC_GenerateSubqueryMaterialised_EmptyRowsCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rows       [][]interface{}
		wantOpcode vdbe.Opcode
	}{
		{
			name:       "A=true B=n/a: no rows → OpNull",
			rows:       [][]interface{}{},
			wantOpcode: vdbe.OpNull,
		},
		{
			name:       "A=false B=true: one row with no columns → OpNull",
			rows:       [][]interface{}{{}},
			wantOpcode: vdbe.OpNull,
		},
		{
			name:       "A=false B=false: one row with int64 value → OpInteger",
			rows:       [][]interface{}{{int64(42)}},
			wantOpcode: vdbe.OpInteger,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			capturedRows := tt.rows
			gen.SetSubqueryExecutor(func(sel *parser.SelectStmt) ([][]interface{}, error) {
				return capturedRows, nil
			})

			subqExpr := &parser.SubqueryExpr{
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{
						{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
					},
				},
			}

			_, err := gen.GenerateExpr(subqExpr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantOpcode {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected opcode %s in program, not found", tt.wantOpcode.String())
			}
		})
	}
}
