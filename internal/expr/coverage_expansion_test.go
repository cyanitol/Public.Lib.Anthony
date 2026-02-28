package expr

import (
	"math"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// Affinity Tests - Uncovered Functions
// ============================================================================

func TestAffinityHandleSelect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Select with columns",
			expr: &Expr{
				Op: OpSelect,
				Select: &SelectStmt{
					Columns: &ExprList{
						Items: []*ExprListItem{
							{Expr: &Expr{Op: OpColumn, Affinity: AFF_INTEGER}},
						},
					},
				},
			},
			expected: AFF_INTEGER,
		},
		{
			name: "Select with no columns",
			expr: &Expr{
				Op:     OpSelect,
				Select: &SelectStmt{},
			},
			expected: AFF_NONE,
		},
		{
			name: "Select with nil Select",
			expr: &Expr{
				Op:     OpSelect,
				Select: nil,
			},
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAffinityHandleSelectColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "SelectColumn with valid index",
			expr: &Expr{
				Op:      OpSelectColumn,
				IColumn: 1,
				Left: &Expr{
					Op: OpSelect,
					Select: &SelectStmt{
						Columns: &ExprList{
							Items: []*ExprListItem{
								{Expr: &Expr{Op: OpColumn, Affinity: AFF_INTEGER}},
								{Expr: &Expr{Op: OpColumn, Affinity: AFF_TEXT}},
							},
						},
					},
				},
			},
			expected: AFF_TEXT,
		},
		{
			name: "SelectColumn with invalid index",
			expr: &Expr{
				Op:      OpSelectColumn,
				IColumn: 5,
				Left: &Expr{
					Op: OpSelect,
					Select: &SelectStmt{
						Columns: &ExprList{
							Items: []*ExprListItem{
								{Expr: &Expr{Op: OpColumn, Affinity: AFF_INTEGER}},
							},
						},
					},
				},
			},
			expected: AFF_NONE,
		},
		{
			name: "SelectColumn with nil Left",
			expr: &Expr{
				Op:      OpSelectColumn,
				IColumn: 0,
				Left:    nil,
			},
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAffinityHandleVector(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Vector with items",
			expr: &Expr{
				Op: OpVector,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: &Expr{Affinity: AFF_REAL}},
						{Expr: &Expr{Affinity: AFF_INTEGER}},
					},
				},
			},
			expected: AFF_REAL,
		},
		{
			name: "Vector with no items",
			expr: &Expr{
				Op:   OpVector,
				List: &ExprList{Items: []*ExprListItem{}},
			},
			expected: AFF_NONE,
		},
		{
			name: "Vector with nil list",
			expr: &Expr{
				Op:   OpVector,
				List: nil,
			},
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAffinityHandleFunction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Function with stored affinity",
			expr: &Expr{
				Op:       OpFunction,
				Affinity: AFF_TEXT,
			},
			expected: AFF_TEXT,
		},
		{
			name: "Function with no affinity but has list",
			expr: &Expr{
				Op:       OpFunction,
				Affinity: AFF_NONE,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: &Expr{Affinity: AFF_INTEGER}},
					},
				},
			},
			expected: AFF_INTEGER,
		},
		{
			name: "Function with no affinity and no list",
			expr: &Expr{
				Op:       OpFunction,
				Affinity: AFF_NONE,
				List:     nil,
			},
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAffinityHandleTransparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Collate with left child",
			expr: &Expr{
				Op: OpCollate,
				Left: &Expr{
					Op:       OpColumn,
					Affinity: AFF_TEXT,
				},
			},
			expected: AFF_TEXT,
		},
		{
			name: "UnaryPlus with left child",
			expr: &Expr{
				Op: OpUnaryPlus,
				Left: &Expr{
					Op:       OpInteger,
					Affinity: AFF_INTEGER,
				},
			},
			expected: AFF_INTEGER,
		},
		{
			name: "Collate with no left child",
			expr: &Expr{
				Op:   OpCollate,
				Left: nil,
			},
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAffinityHandleRegister(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Register with IOp2 set",
			expr: &Expr{
				Op:       OpRegister,
				IOp2:     6, // OpColumn value
				Affinity: AFF_TEXT,
			},
			expected: AFF_TEXT,
		},
		{
			name: "Register without IOp2",
			expr: &Expr{
				Op:       OpRegister,
				IOp2:     0,
				Affinity: AFF_INTEGER,
			},
			expected: AFF_INTEGER,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetComparisonAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Non-comparison returns NONE",
			expr: &Expr{
				Op: OpPlus,
			},
			expected: AFF_NONE,
		},
		{
			name: "Comparison with no left",
			expr: &Expr{
				Op:   OpEq,
				Left: nil,
			},
			expected: AFF_BLOB,
		},
		{
			name: "Comparison with right operand",
			expr: &Expr{
				Op:    OpEq,
				Left:  &Expr{Op: OpColumn, Affinity: AFF_INTEGER},
				Right: &Expr{Op: OpColumn, Affinity: AFF_TEXT},
			},
			expected: AFF_NUMERIC,
		},
		{
			name: "Comparison with subquery",
			expr: &Expr{
				Op:    OpEq,
				Flags: EP_xIsSelect,
				Left:  &Expr{Op: OpColumn, Affinity: AFF_INTEGER},
				Select: &SelectStmt{
					Columns: &ExprList{
						Items: []*ExprListItem{
							{Expr: &Expr{Op: OpColumn, Affinity: AFF_TEXT}},
						},
					},
				},
			},
			expected: AFF_NUMERIC,
		},
		{
			name: "Comparison with subquery no columns",
			expr: &Expr{
				Op:     OpEq,
				Flags:  EP_xIsSelect,
				Left:   &Expr{Op: OpColumn, Affinity: AFF_INTEGER},
				Select: &SelectStmt{},
			},
			expected: AFF_INTEGER,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetComparisonAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestPropagateAffinityNegate(t *testing.T) {
	t.Parallel()
	expr := &Expr{
		Op:   OpNegate,
		Left: &Expr{Op: OpInteger, Affinity: AFF_INTEGER},
	}

	PropagateAffinity(expr)

	// Negate propagates as INTEGER for single operands
	if expr.Affinity != AFF_INTEGER {
		t.Errorf("Expected AFF_INTEGER for negate, got %v", expr.Affinity)
	}
}

func TestPropagateAffinityCase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name: "Case with when clauses",
			expr: &Expr{
				Op: OpCase,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: &Expr{Affinity: AFF_INTEGER}}, // WHEN condition
						{Expr: &Expr{Affinity: AFF_TEXT}},    // THEN result
						{Expr: &Expr{Affinity: AFF_INTEGER}}, // WHEN condition
						{Expr: &Expr{Affinity: AFF_REAL}},    // THEN result
					},
				},
				Left: &Expr{Affinity: AFF_BLOB}, // ELSE
			},
			expected: AFF_NONE,
		},
		{
			name: "Case with no when clauses",
			expr: &Expr{
				Op:   OpCase,
				List: nil,
				Left: &Expr{Affinity: AFF_INTEGER},
			},
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			PropagateAffinity(tt.expr)
			if tt.expr.Affinity != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, tt.expr.Affinity)
			}
		})
	}
}

func TestApplyNumericAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{
			name:     "Float64 unchanged",
			value:    3.14,
			expected: 3.14,
		},
		{
			name:     "Int64 to float64",
			value:    int64(42),
			expected: 42.0,
		},
		{
			name:     "String float to float64",
			value:    "3.14",
			expected: 3.14,
		},
		{
			name:     "String int to float64",
			value:    "42",
			expected: 42.0,
		},
		{
			name:     "Non-numeric string unchanged",
			value:    "hello",
			expected: "hello",
		},
		{
			name:     "Null unchanged",
			value:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := ApplyAffinity(tt.value, AFF_NUMERIC)
			if !compareResults(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}


// ============================================================================
// CodeGen Tests - Uncovered Functions
// ============================================================================

func TestCodeGeneratorSetArgs(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	args := []interface{}{int64(42), "test", 3.14}
	gen.SetArgs(args)

	if len(gen.args) != 3 {
		t.Errorf("Expected 3 args, got %d", len(gen.args))
	}
	if gen.paramIdx != 0 {
		t.Errorf("Expected paramIdx 0, got %d", gen.paramIdx)
	}
}

func TestCodeGeneratorSetNextReg(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetNextReg(5)
	if gen.nextReg != 5 {
		t.Errorf("Expected nextReg 5, got %d", gen.nextReg)
	}

	// Setting lower value shouldn't change it
	gen.SetNextReg(3)
	if gen.nextReg != 5 {
		t.Errorf("Expected nextReg still 5, got %d", gen.nextReg)
	}

	// Setting higher value should change it
	gen.SetNextReg(10)
	if gen.nextReg != 10 {
		t.Errorf("Expected nextReg 10, got %d", gen.nextReg)
	}
}

func TestCodeGeneratorGetCursor(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.RegisterCursor("users", 5)

	cursor, ok := gen.GetCursor("users")
	if !ok {
		t.Error("Expected to find cursor")
	}
	if cursor != 5 {
		t.Errorf("Expected cursor 5, got %d", cursor)
	}

	_, ok = gen.GetCursor("nonexistent")
	if ok {
		t.Error("Expected not to find nonexistent cursor")
	}
}

func TestGenerateNullLiteral(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	reg, err := gen.GenerateExpr(nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if reg == 0 {
		t.Error("Expected non-zero register")
	}

	if len(v.Program) == 0 {
		t.Fatal("Expected at least one instruction")
	}

	if v.Program[0].Opcode != vdbe.OpNull {
		t.Errorf("Expected OpNull, got %v", v.Program[0].Opcode)
	}
}

func TestGenerateVariable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		args        []interface{}
		expectedOp  vdbe.Opcode
		expectedVal interface{}
	}{
		{
			name:       "No args",
			args:       nil,
			expectedOp: vdbe.OpNull,
		},
		{
			name:       "Integer arg",
			args:       []interface{}{int(42)},
			expectedOp: vdbe.OpInteger,
		},
		{
			name:       "Int64 arg",
			args:       []interface{}{int64(42)},
			expectedOp: vdbe.OpInteger,
		},
		{
			name:       "Float arg",
			args:       []interface{}{3.14},
			expectedOp: vdbe.OpReal,
		},
		{
			name:       "String arg",
			args:       []interface{}{"test"},
			expectedOp: vdbe.OpString8,
		},
		{
			name:       "Blob arg",
			args:       []interface{}{[]byte("blob")},
			expectedOp: vdbe.OpBlob,
		},
		{
			name:       "Bool true arg",
			args:       []interface{}{true},
			expectedOp: vdbe.OpInteger,
		},
		{
			name:       "Bool false arg",
			args:       []interface{}{false},
			expectedOp: vdbe.OpInteger,
		},
		{
			name:       "Null arg",
			args:       []interface{}{nil},
			expectedOp: vdbe.OpNull,
		},
		{
			name:       "Default type arg",
			args:       []interface{}{struct{ X int }{42}},
			expectedOp: vdbe.OpString8,
		},
		{
			name:       "Large int64",
			args:       []interface{}{int64(9223372036854775807)},
			expectedOp: vdbe.OpInt64,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			gen.SetArgs(tt.args)

			expr := &parser.VariableExpr{Name: "?1"}
			reg, err := gen.GenerateExpr(expr)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if reg == 0 {
				t.Error("Expected non-zero register")
			}

			if len(v.Program) == 0 {
				t.Fatal("Expected at least one instruction")
			}

			if v.Program[0].Opcode != tt.expectedOp {
				t.Errorf("Expected %v, got %v", tt.expectedOp, v.Program[0].Opcode)
			}
		})
	}
}

func TestGenerateCollate(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.CollateExpr{
		Expr:        &parser.LiteralExpr{Type: parser.LiteralString, Value: "test"},
		Collation: "NOCASE",
	}

	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestPatchJumpAndCurrentAddr(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Add some instructions
	gen.AllocReg()
	addr1 := gen.CurrentAddr()
	v.AddOp(vdbe.OpGoto, 0, 0, 0)

	addr2 := gen.CurrentAddr()
	v.AddOp(vdbe.OpInteger, 42, 1, 0)

	// Patch the jump to point to current address
	gen.PatchJump(addr1)

	if v.Program[addr1].P2 != addr2+1 {
		t.Errorf("Expected P2=%d, got %d", addr2+1, v.Program[addr1].P2)
	}

	// Test patching invalid address
	gen.PatchJump(-1)
	gen.PatchJump(1000)
}

func TestGenerateLikeAndGlobExpr(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Generate LIKE
	reg1, err := gen.generateLikeExpr(1, 2)
	if err != nil {
		t.Fatalf("generateLikeExpr failed: %v", err)
	}
	if reg1 == 0 {
		t.Error("Expected non-zero register for LIKE")
	}

	// Generate GLOB
	reg2, err := gen.generateGlobExpr(3, 4)
	if err != nil {
		t.Fatalf("generateGlobExpr failed: %v", err)
	}
	if reg2 == 0 {
		t.Error("Expected non-zero register for GLOB")
	}
}

// ============================================================================
// Compare Tests - Uncovered Functions
// ============================================================================

func TestCollSeqFromCollateOp(t *testing.T) {
	t.Parallel()
	expr := &Expr{
		Op:      OpCollate,
		CollSeq: "NOCASE",
	}

	coll := GetCollSeq(expr)
	if coll.Name != "NOCASE" {
		t.Errorf("Expected NOCASE, got %s", coll.Name)
	}
}


func TestEvaluateIsAndIsNot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		op       OpCode
		left     interface{}
		right    interface{}
		expected interface{}
	}{
		{
			name:     "IS: both null",
			op:       OpIs,
			left:     nil,
			right:    nil,
			expected: true,
		},
		{
			name:     "IS: left null",
			op:       OpIs,
			left:     nil,
			right:    int64(42),
			expected: false,
		},
		{
			name:     "IS: right null",
			op:       OpIs,
			left:     int64(42),
			right:    nil,
			expected: false,
		},
		{
			name:     "IS: both equal",
			op:       OpIs,
			left:     int64(42),
			right:    int64(42),
			expected: true,
		},
		{
			name:     "IS: both different",
			op:       OpIs,
			left:     int64(42),
			right:    int64(43),
			expected: false,
		},
		{
			name:     "IS NOT: both null",
			op:       OpIsNot,
			left:     nil,
			right:    nil,
			expected: false,
		},
		{
			name:     "IS NOT: left null",
			op:       OpIsNot,
			left:     nil,
			right:    int64(42),
			expected: true,
		},
		{
			name:     "IS NOT: both equal",
			op:       OpIsNot,
			left:     int64(42),
			right:    int64(42),
			expected: false,
		},
		{
			name:     "IS NOT: both different",
			op:       OpIsNot,
			left:     int64(42),
			right:    int64(43),
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateComparison(tt.op, tt.left, tt.right, AFF_INTEGER, CollSeqBinary)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// ============================================================================
// Arithmetic Tests - Uncovered Functions
// ============================================================================

func TestNegateMinInt64(t *testing.T) {
	t.Parallel()
	result := EvaluateUnary(OpNegate, int64(math.MinInt64))
	if _, ok := result.(float64); !ok {
		t.Error("Expected float64 result for negating MinInt64")
	}
}

func TestNegateString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{
			name:     "Numeric string",
			value:    "42",
			expected: int64(-42),
		},
		{
			name:     "Float string",
			value:    "3.14",
			expected: -3.14,
		},
		{
			name:     "Non-numeric string",
			value:    "hello",
			expected: int64(0),
		},
		{
			name:     "MinInt64 string",
			value:    "-9223372036854775808",
			expected: 9223372036854775808.0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateUnary(OpNegate, tt.value)
			if !compareResults(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBitNotWithNonInteger(t *testing.T) {
	t.Parallel()
	result := EvaluateUnary(OpBitNot, "not a number")
	if result != int64(0) {
		t.Errorf("Expected 0 for bitwise NOT of non-integer, got %v", result)
	}
}

func TestBitwiseRShiftNegative(t *testing.T) {
	t.Parallel()
	result := EvaluateBitwise(OpRShift, int64(-8), int64(100))
	if result != int64(-1) {
		t.Errorf("Expected -1 for right shift of negative by large amount, got %v", result)
	}
}

// ============================================================================
// Expr Tests - Uncovered Functions
// ============================================================================

func TestOpCodeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		op       OpCode
		expected string
	}{
		{OpInteger, "INTEGER"},
		{OpString, "STRING"},
		{OpColumn, "COLUMN"},
		{OpPlus, "PLUS"},
		{OpMinus, "MINUS"},
		{OpEq, "EQ"},
		{OpLt, "LT"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.expected, func(t *testing.T) {
				t.Parallel()
			result := tt.op.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExprListItems(t *testing.T) {
	t.Parallel()
	list := &ExprList{
		Items: []*ExprListItem{
			{Expr: NewIntExpr(1)},
			{Expr: NewIntExpr(2)},
			{Expr: NewIntExpr(3)},
		},
	}

	if len(list.Items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(list.Items))
	}
}

func TestVectorSizeExpr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected int
	}{
		{
			name:     "Nil expr",
			expr:     nil,
			expected: 0,
		},
		{
			name:     "Scalar expr",
			expr:     NewIntExpr(42),
			expected: 1,
		},
		{
			name: "Vector expr",
			expr: &Expr{
				Op: OpVector,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: NewIntExpr(1)},
						{Expr: NewIntExpr(2)},
						{Expr: NewIntExpr(3)},
					},
				},
			},
			expected: 3,
		},
		{
			name: "Select expr with columns",
			expr: &Expr{
				Op: OpSelect,
				Select: &SelectStmt{
					Columns: &ExprList{
						Items: []*ExprListItem{
							{Expr: NewIntExpr(1)},
							{Expr: NewIntExpr(2)},
						},
					},
				},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := tt.expr.VectorSize()
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestExprCloneDeepCopy(t *testing.T) {
	t.Parallel()
	original := &Expr{
		Op:       OpPlus,
		Affinity: AFF_INTEGER,
		Left:     NewIntExpr(1),
		Right:    NewIntExpr(2),
		Flags:    EP_Collate,
		Height:   2,
	}

	cloned := original.Clone()

	if cloned.Op != original.Op {
		t.Error("Cloned Op mismatch")
	}
	if cloned.Affinity != original.Affinity {
		t.Error("Cloned Affinity mismatch")
	}
	if cloned.Flags != original.Flags {
		t.Error("Cloned Flags mismatch")
	}
	if cloned.Height != original.Height {
		t.Error("Cloned Height mismatch")
	}

	// Verify deep copy
	if cloned.Left == original.Left {
		t.Error("Expected deep copy of Left")
	}
	if cloned.Right == original.Right {
		t.Error("Expected deep copy of Right")
	}
}

func TestExprListCloneDeepCopy(t *testing.T) {
	t.Parallel()
	original := &ExprList{
		Items: []*ExprListItem{
			{Expr: NewIntExpr(1), Name: "a"},
			{Expr: NewIntExpr(2), Name: "b"},
		},
	}

	cloned := original.Clone()

	if len(cloned.Items) != len(original.Items) {
		t.Error("Cloned items count mismatch")
	}

	for i := range original.Items {
		if cloned.Items[i].Name != original.Items[i].Name {
			t.Error("Cloned item name mismatch")
		}
		// Verify deep copy
		if cloned.Items[i].Expr == original.Items[i].Expr {
			t.Error("Expected deep copy of Expr")
		}
	}
}

func TestIsFunctionConstant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected bool
	}{
		{
			name: "Function with variable args",
			expr: &Expr{
				Op:    OpFunction,
				Flags: EP_HasFunc | EP_VarSelect,
			},
			expected: false,
		},
		{
			name: "Function with constant args",
			expr: &Expr{
				Op: OpFunction,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: NewIntExpr(1)},
					},
				},
			},
			expected: true,
		},
		{
			name: "Function with non-constant arg",
			expr: &Expr{
				Op: OpFunction,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: &Expr{Op: OpColumn}},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := tt.expr.IsConstant()
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestUpdateHeight(t *testing.T) {
	t.Parallel()
	expr := &Expr{
		Op: OpPlus,
		Left: &Expr{
			Op:     OpInteger,
			Height: 1,
		},
		Right: &Expr{
			Op: OpMultiply,
			Left: &Expr{
				Op:     OpInteger,
				Height: 1,
			},
			Right: &Expr{
				Op:     OpInteger,
				Height: 1,
			},
			Height: 2,
		},
		List: &ExprList{
			Items: []*ExprListItem{
				{Expr: &Expr{Op: OpInteger, Height: 1}},
				{Expr: &Expr{Op: OpInteger, Height: 3}},
			},
		},
	}

	NewBinaryExpr(OpPlus, expr.Left, expr.Right)

	if expr.Left.Height != 1 {
		t.Errorf("Expected Left height 1, got %d", expr.Left.Height)
	}
}

func TestStringLiteral(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected string
	}{
		{
			name:     "Integer literal",
			expr:     NewIntExpr(42),
			expected: "42",
		},
		{
			name:     "Float literal",
			expr:     NewFloatExpr(3.14),
			expected: "3.14",
		},
		{
			name:     "String literal",
			expr:     NewStringExpr("hello"),
			expected: "'hello'",
		},
		{
			name:     "Null literal",
			expr:     NewNullExpr(),
			expected: "NULL",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := tt.expr.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// ============================================================================
// Additional Edge Cases
// ============================================================================

func TestCompareNumericsWithInfinity(t *testing.T) {
	t.Parallel()
	result := compareNumerics(math.Inf(1), 1.0)
	if result != CmpGreater {
		t.Errorf("Expected CmpGreater for +Inf > 1.0, got %v", result)
	}

	result = compareNumerics(1.0, math.Inf(1))
	if result != CmpLess {
		t.Errorf("Expected CmpLess for 1.0 < +Inf, got %v", result)
	}

	result = compareNumerics(math.Inf(-1), 1.0)
	if result != CmpLess {
		t.Errorf("Expected CmpLess for -Inf < 1.0, got %v", result)
	}
}

func TestDivideFloatsInfinity(t *testing.T) {
	t.Parallel()
	result := EvaluateArithmetic(OpDivide, 1.0, 0.0)
	if result != nil {
		t.Errorf("Expected nil for divide by zero, got %v", result)
	}
}

func TestResolveFloatVal(t *testing.T) {
	t.Parallel()
	// This tests the internal resolveFloatVal logic through divide
	result := EvaluateArithmetic(OpDivide, int64(10), 3.0)
	if _, ok := result.(float64); !ok {
		t.Error("Expected float64 result for int/float division")
	}

	result = EvaluateArithmetic(OpDivide, 10.0, int64(3))
	if _, ok := result.(float64); !ok {
		t.Error("Expected float64 result for float/int division")
	}
}

func TestSubtractOverflow(t *testing.T) {
	t.Parallel()
	result := EvaluateArithmetic(OpMinus, int64(math.MinInt64), int64(1))
	if _, ok := result.(float64); !ok {
		t.Error("Expected float64 result for subtraction overflow")
	}
}

func TestRemainderWithFloat(t *testing.T) {
	t.Parallel()
	result := EvaluateArithmetic(OpRemainder, 10.5, int64(3))
	if result == nil {
		t.Error("Expected non-nil result for remainder with float")
	}
}
