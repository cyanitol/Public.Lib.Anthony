// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package btree

import (
	"fmt"
	"testing"
)

func TestPutGetVarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint64
		want  int // expected length
	}{
		{"1-byte", 0x00, 1},
		{"1-byte max", 0x7f, 1},
		{"2-byte min", 0x80, 2},
		{"2-byte", 0x100, 2},
		{"2-byte max", 0x3fff, 2},
		{"3-byte min", 0x4000, 3},
		{"3-byte", 0x12345, 3},
		{"3-byte max", 0x1fffff, 3},
		{"4-byte min", 0x200000, 4},
		{"4-byte", 0x1234567, 4},
		{"5-byte", 0x12345678, 5},
		{"9-byte max", 0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], tt.value)
			if n != tt.want {
				t.Errorf("PutVarint() length = %d, want %d", n, tt.want)
			}

			got, m := GetVarint(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint() = %d, want %d", got, tt.value)
			}
			if m != n {
				t.Errorf("GetVarint() length = %d, want %d", m, n)
			}
		})
	}
}

func TestGetVarint32(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint32
		want  int
	}{
		{"1-byte", 0x00, 1},
		{"1-byte max", 0x7f, 1},
		{"2-byte", 0x80, 2},
		{"3-byte", 0x4000, 3},
		{"4-byte", 0x200000, 4},
		{"max uint32", 0xffffffff, 5},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], uint64(tt.value))
			if n != tt.want {
				t.Errorf("PutVarint() length = %d, want %d", n, tt.want)
			}

			got, m := GetVarint32(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint32() = %d, want %d", got, tt.value)
			}
			if m != n {
				t.Errorf("GetVarint32() length = %d, want %d", m, n)
			}
		})
	}
}

func TestVarintLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		want  int
	}{
		{0x00, 1},
		{0x7f, 1},
		{0x80, 2},
		{0x3fff, 2},
		{0x4000, 3},
		{0x1fffff, 3},
		{0x200000, 4},
		{0xfffffff, 4},
		{0x10000000, 5},
		{0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		got := VarintLen(tt.value)
		if got != tt.want {
			t.Errorf("VarintLen(0x%x) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	t.Parallel()
	// Test all powers of 2 and nearby values
	for i := uint(0); i < 64; i++ {
		values := []uint64{
			1 << i,
			(1 << i) - 1,
			(1 << i) + 1,
		}

		for _, v := range values {
			var buf [9]byte
			n := PutVarint(buf[:], v)
			got, m := GetVarint(buf[:])

			if got != v {
				t.Errorf("RoundTrip(%d): got %d", v, got)
			}
			if m != n {
				t.Errorf("RoundTrip(%d): length mismatch: put=%d, get=%d", v, n, m)
			}
		}
	}
}

func BenchmarkPutVarint1Byte(b *testing.B) {
	var buf [9]byte
	for i := 0; i < b.N; i++ {
		PutVarint(buf[:], 0x7f)
	}
}

func BenchmarkPutVarint9Byte(b *testing.B) {
	var buf [9]byte
	for i := 0; i < b.N; i++ {
		PutVarint(buf[:], 0xffffffffffffffff)
	}
}

func BenchmarkGetVarint1Byte(b *testing.B) {
	var buf [9]byte
	PutVarint(buf[:], 0x7f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetVarint(buf[:])
	}
}

func BenchmarkGetVarint9Byte(b *testing.B) {
	var buf [9]byte
	PutVarint(buf[:], 0xffffffffffffffff)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetVarint(buf[:])
	}
}

// TestGetVarintEdgeCases tests edge cases for GetVarint
func TestGetVarintEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func() []byte
		want  uint64
	}{
		{
			name: "single byte zero",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0)
				return buf[:]
			},
			want: 0,
		},
		{
			name: "single byte max",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0x7f)
				return buf[:]
			},
			want: 0x7f,
		},
		{
			name: "two byte boundary",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0x80)
				return buf[:]
			},
			want: 0x80,
		},
		{
			name: "max 9-byte value",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0xffffffffffffffff)
				return buf[:]
			},
			want: 0xffffffffffffffff,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.setup()
			got, _ := GetVarint(buf)
			if got != tt.want {
				t.Errorf("GetVarint() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestDecodeShortVarint tests the short varint decoding path
func TestDecodeShortVarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"127", 127},
		{"128", 128},
		{"255", 255},
		{"256", 256},
		{"16383", 16383},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], tt.value)
			got, m := GetVarint(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint() = %d, want %d", got, tt.value)
			}
			if m != n {
				t.Errorf("GetVarint() length = %d, want %d", m, n)
			}
		})
	}
}

// TestSlowBtreeVarint32 tests the slow path for GetVarint32
func TestSlowBtreeVarint32(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint32
	}{
		{"boundary 0x4000", 0x4000},
		{"boundary 0x200000", 0x200000},
		{"max uint32", 0xffffffff},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			PutVarint(buf[:], uint64(tt.value))
			got, _ := GetVarint32(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint32() = %d, want %d", got, tt.value)
			}
		})
	}
}

// TestGetVarintBufferTooSmall tests behavior with insufficient buffer
func TestGetVarintBufferTooSmall(t *testing.T) {
	t.Parallel()
	var buf [9]byte
	PutVarint(buf[:], 0xffffffffffffffff)

	// Try to decode with only first few bytes
	// Should still work or return partial result
	got, n := GetVarint(buf[:9])
	if n != 9 {
		t.Errorf("Expected 9 bytes read, got %d", n)
	}
	if got != 0xffffffffffffffff {
		t.Errorf("GetVarint() = 0x%x, want 0xffffffffffffffff", got)
	}
}

// TestVarintLenBoundaries tests VarintLen at all boundaries
func TestVarintLenBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		want  int
	}{
		{0, 1},
		{0x7f, 1},
		{0x80, 2},
		{0x3fff, 2},
		{0x4000, 3},
		{0x1fffff, 3},
		{0x200000, 4},
		{0xfffffff, 4},
		{0x10000000, 5},
		{0x7ffffffff, 5},
		{0x800000000, 6},
		{0x3ffffffffff, 6},
		{0x40000000000, 7},
		{0x1ffffffffffff, 7},
		{0x2000000000000, 8},
		{0xffffffffffffff, 8},
		{0x100000000000000, 9},
		{0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		got := VarintLen(tt.value)
		if got != tt.want {
			t.Errorf("VarintLen(0x%x) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

// TestPutVarintAllSizes tests PutVarint for all possible sizes
func TestPutVarintAllSizes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		size  int
	}{
		{0x00, 1},
		{0x80, 2},
		{0x4000, 3},
		{0x200000, 4},
		{0x10000000, 5},
		{0x800000000, 6},
		{0x40000000000, 7},
		{0x2000000000000, 8},
		{0x100000000000000, 9},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("size=%d", tt.size), func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], tt.value)
			if n != tt.size {
				t.Errorf("PutVarint(0x%x) size = %d, want %d", tt.value, n, tt.size)
			}

			// Verify we can decode it back
			got, m := GetVarint(buf[:])
			if got != tt.value {
				t.Errorf("Round trip failed: got 0x%x, want 0x%x", got, tt.value)
			}
			if m != n {
				t.Errorf("Decode size %d != encode size %d", m, n)
			}
		})
	}
}

// TestGetVarint32Overflow tests GetVarint32 with values that fit in uint32
func TestGetVarint32Overflow(t *testing.T) {
	t.Parallel()
	var buf [9]byte

	// Test with max uint32
	PutVarint(buf[:], 0xffffffff)
	got, n := GetVarint32(buf[:])
	if got != 0xffffffff {
		t.Errorf("GetVarint32(max) = 0x%x, want 0xffffffff", got)
	}
	if n != 5 {
		t.Errorf("GetVarint32(max) size = %d, want 5", n)
	}
}

// TestVarintZeroValue tests zero value encoding/decoding
func TestVarintZeroValue(t *testing.T) {
	t.Parallel()
	var buf [9]byte
	n := PutVarint(buf[:], 0)
	if n != 1 {
		t.Errorf("PutVarint(0) size = %d, want 1", n)
	}

	got, m := GetVarint(buf[:])
	if got != 0 {
		t.Errorf("GetVarint() = %d, want 0", got)
	}
	if m != 1 {
		t.Errorf("GetVarint() size = %d, want 1", m)
	}

	length := VarintLen(0)
	if length != 1 {
		t.Errorf("VarintLen(0) = %d, want 1", length)
	}
}
t      *SelectStmt
	IfNotExists bool
	Temporary   bool
}

func (c *CreateViewStmt) node()      {}
func (c *CreateViewStmt) statement() {}
func (c *CreateViewStmt) String() string {
	return "CREATE VIEW"
}

// DropViewStmt represents a DROP VIEW statement.
type DropViewStmt struct {
	Name     string
	IfExists bool
}

func (d *DropViewStmt) node()      {}
func (d *DropViewStmt) statement() {}
func (d *DropViewStmt) String() string {
	return "DROP VIEW"
}

// CreateTriggerStmt represents a CREATE TRIGGER statement.
type CreateTriggerStmt struct {
	Name        string
	Temp        bool
	IfNotExists bool
	Timing      TriggerTiming
	Event       TriggerEvent
	UpdateOf    []string // columns for UPDATE OF
	Table       string
	ForEachRow  bool
	When        Expression
	Body        []Statement
}

func (c *CreateTriggerStmt) node()      {}
func (c *CreateTriggerStmt) statement() {}
func (c *CreateTriggerStmt) String() string {
	return "CREATE TRIGGER"
}

// TriggerTiming represents when a trigger fires.
type TriggerTiming int

const (
	TriggerBefore TriggerTiming = iota
	TriggerAfter
	TriggerInsteadOf
)

// TriggerEvent represents the event that activates a trigger.
type TriggerEvent int

const (
	TriggerInsert TriggerEvent = iota
	TriggerUpdate
	TriggerDelete
)

// DropTriggerStmt represents a DROP TRIGGER statement.
type DropTriggerStmt struct {
	Name     string
	IfExists bool
}

func (d *DropTriggerStmt) node()      {}
func (d *DropTriggerStmt) statement() {}
func (d *DropTriggerStmt) String() string {
	return "DROP TRIGGER"
}

// BeginStmt represents a BEGIN/START TRANSACTION statement.
type BeginStmt struct {
	Mode TransactionMode
}

func (b *BeginStmt) node()      {}
func (b *BeginStmt) statement() {}
func (b *BeginStmt) String() string {
	return "BEGIN"
}

type TransactionMode int

const (
	TransactionDeferred TransactionMode = iota
	TransactionImmediate
	TransactionExclusive
)

// CommitStmt represents a COMMIT statement.
type CommitStmt struct{}

func (c *CommitStmt) node()      {}
func (c *CommitStmt) statement() {}
func (c *CommitStmt) String() string {
	return "COMMIT"
}

// RollbackStmt represents a ROLLBACK statement.
type RollbackStmt struct {
	Savepoint string
}

func (r *RollbackStmt) node()      {}
func (r *RollbackStmt) statement() {}
func (r *RollbackStmt) String() string {
	return "ROLLBACK"
}

// ExplainStmt represents an EXPLAIN or EXPLAIN QUERY PLAN statement.
type ExplainStmt struct {
	QueryPlan bool      // true for EXPLAIN QUERY PLAN, false for EXPLAIN
	Statement Statement // the statement being explained
}

func (e *ExplainStmt) node()      {}
func (e *ExplainStmt) statement() {}
func (e *ExplainStmt) String() string {
	if e.QueryPlan {
		return "EXPLAIN QUERY PLAN"
	}
	return "EXPLAIN"
}


// AttachStmt represents an ATTACH DATABASE statement.
type AttachStmt struct {
	Filename   Expression // String literal or expression for the database file path
	SchemaName string     // The schema name to attach as
}

func (a *AttachStmt) node()      {}
func (a *AttachStmt) statement() {}
func (a *AttachStmt) String() string {
	return "ATTACH"
}

// DetachStmt represents a DETACH DATABASE statement.
type DetachStmt struct {
	SchemaName string // The schema name to detach
}

func (d *DetachStmt) node()      {}
func (d *DetachStmt) statement() {}
func (d *DetachStmt) String() string {
	return "DETACH"
}

// PragmaStmt represents a PRAGMA statement.
type PragmaStmt struct {
	Schema string     // optional schema name
	Name   string     // pragma name
	Value  Expression // optional value (for = or () syntax)
}

func (p *PragmaStmt) node()      {}
func (p *PragmaStmt) statement() {}
func (p *PragmaStmt) String() string {
	return "PRAGMA"
}

// AlterTableStmt represents an ALTER TABLE statement.
type AlterTableStmt struct {
	Table  string
	Action AlterTableAction
}

func (a *AlterTableStmt) node()      {}
func (a *AlterTableStmt) statement() {}
func (a *AlterTableStmt) String() string {
	return "ALTER TABLE"
}

// VacuumStmt represents a VACUUM statement.
type VacuumStmt struct {
	Schema    string // optional schema name
	Into      string // optional INTO filename
	IntoParam bool   // true if INTO filename comes from a parameter
}

func (v *VacuumStmt) node()      {}
func (v *VacuumStmt) statement() {}
func (v *VacuumStmt) String() string {
	if v.Into != "" || v.IntoParam {
		return "VACUUM INTO"
	}
	return "VACUUM"
}

// AlterTableAction represents the action to perform in ALTER TABLE.
type AlterTableAction interface {
	Node
	alterTableAction()
}

// RenameTableAction represents RENAME TO newname.
type RenameTableAction struct {
	NewName string
}

func (r *RenameTableAction) node()             {}
func (r *RenameTableAction) alterTableAction() {}
func (r *RenameTableAction) String() string {
	return "RENAME TO"
}

// RenameColumnAction represents RENAME COLUMN oldname TO newname.
type RenameColumnAction struct {
	OldName string
	NewName string
}

func (r *RenameColumnAction) node()             {}
func (r *RenameColumnAction) alterTableAction() {}
func (r *RenameColumnAction) String() string {
	return "RENAME COLUMN"
}

// AddColumnAction represents ADD COLUMN column_def.
type AddColumnAction struct {
	Column ColumnDef
}

func (a *AddColumnAction) node()             {}
func (a *AddColumnAction) alterTableAction() {}
func (a *AddColumnAction) String() string {
	return "ADD COLUMN"
}

// DropColumnAction represents DROP COLUMN [IF EXISTS] column_name.
type DropColumnAction struct {
	ColumnName string
	IfExists   bool
}

func (d *DropColumnAction) node()             {}
func (d *DropColumnAction) alterTableAction() {}
func (d *DropColumnAction) String() string {
	return "DROP COLUMN"
}

// =============================================================================
// Expressions
// =============================================================================

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Left  Expression
	Op    BinaryOp
	Right Expression
}

func (b *BinaryExpr) node()       {}
func (b *BinaryExpr) expression() {}
func (b *BinaryExpr) String() string {
	left := "nil"
	right := "nil"
	if b.Left != nil {
		left = b.Left.String()
	}
	if b.Right != nil {
		right = b.Right.String()
	}
	return left + " " + b.Op.String() + " " + right
}

type BinaryOp int

const (
	OpEq BinaryOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
	OpPlus
	OpMinus
	OpMul
	OpDiv
	OpRem
	OpConcat
	OpBitAnd
	OpBitOr
	OpLShift
	OpRShift
	OpLike
	OpGlob
	OpRegexp
	OpMatch
)

var binaryOpStrings = map[BinaryOp]string{
	OpEq:     "=",
	OpNe:     "!=",
	OpLt:     "<",
	OpLe:     "<=",
	OpGt:     ">",
	OpGe:     ">=",
	OpAnd:    "AND",
	OpOr:     "OR",
	OpPlus:   "+",
	OpMinus:  "-",
	OpMul:    "*",
	OpDiv:    "/",
	OpRem:    "%",
	OpConcat: "||",
	OpBitAnd: "&",
	OpBitOr:  "|",
	OpLShift: "<<",
	OpRShift: ">>",
	OpLike:   "LIKE",
	OpGlob:   "GLOB",
	OpRegexp: "REGEXP",
	OpMatch:  "MATCH",
}

func (o BinaryOp) String() string {
	if s, ok := binaryOpStrings[o]; ok {
		return s
	}
	return "?"
}

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expression
}

func (u *UnaryExpr) node()       {}
func (u *UnaryExpr) expression() {}
func (u *UnaryExpr) String() string {
	expr := "nil"
	if u.Expr != nil {
		expr = u.Expr.String()
	}
	switch u.Op {
	case OpNot:
		return "NOT " + expr
	case OpNeg:
		return "-" + expr
	case OpBitNot:
		return "~" + expr
	case OpIsNull:
		return expr + " IS NULL"
	case OpNotNull:
		return expr + " IS NOT NULL"
	default:
		return "?" + expr
	}
}

type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNeg
	OpBitNot
	OpIsNull
	OpNotNull
)

// LiteralExpr represents a literal value.
type LiteralExpr struct {
	Type  LiteralType
	Value string
}

func (l *LiteralExpr) node()       {}
func (l *LiteralExpr) expression() {}
func (l *LiteralExpr) String() string {
	switch l.Type {
	case LiteralNull:
		return "NULL"
	case LiteralString:
		// Escape single quotes by doubling them
		escaped := strings.ReplaceAll(l.Value, "'", "''")
		return "'" + escaped + "'"
	case LiteralBlob:
		return "X'" + l.Value + "'"
	default:
		// Integer and Float are returned as-is
		return l.Value
	}
}

type LiteralType int

const (
	LiteralInteger LiteralType = iota
	LiteralFloat
	LiteralString
	LiteralBlob
	LiteralNull
)

// IdentExpr represents an identifier (column name).
type IdentExpr struct {
	Name  string
	Table string // optional table qualifier
}

func (i *IdentExpr) node()       {}
func (i *IdentExpr) expression() {}
func (i *IdentExpr) String() string {
	if i.Table != "" {
		return i.Table + "." + i.Name
	}
	return i.Name
}

// FunctionExpr represents a function call.
type FunctionExpr struct {
	Name     string
	Args     []Expression
	Distinct bool
	Star     bool // for COUNT(*)
	Filter   Expression
	Over     *WindowSpec
}

func (f *FunctionExpr) node()       {}
func (f *FunctionExpr) expression() {}
func (f *FunctionExpr) String() string {
	if f.Star {
		return f.Name + "(*)"
	}
	var args []string
	for _, arg := range f.Args {
		if arg != nil {
			args = append(args, arg.String())
		}
	}
	prefix := ""
	if f.Distinct {
		prefix = "DISTINCT "
	}
	return f.Name + "(" + prefix + strings.Join(args, ", ") + ")"
}

// WindowSpec represents a window specification for window functions.
type WindowSpec struct {
	PartitionBy []Expression
	OrderBy     []OrderingTerm
	Frame       *FrameSpec
}

// FrameSpec represents a frame specification in a window.
type FrameSpec struct {
	Mode  FrameMode
	Start FrameBound
	End   FrameBound
}

type FrameMode int

const (
	FrameRange FrameMode = iota
	FrameRows
	FrameGroups
)

// FrameBound represents a frame boundary.
type FrameBound struct {
	Type   FrameBoundType
	Offset Expression
}

type FrameBoundType int

const (
	BoundUnboundedPreceding FrameBoundType = iota
	BoundPreceding
	BoundCurrentRow
	BoundFollowing
	BoundUnboundedFollowing
)

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr        Expression // optional CASE expr
	WhenClauses []WhenClause
	ElseClause  Expression
}

func (c *CaseExpr) node()       {}
func (c *CaseExpr) expression() {}
func (c *CaseExpr) String() string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if c.Expr != nil {
		sb.WriteString(" ")
		sb.WriteString(c.Expr.String())
	}
	for _, w := range c.WhenClauses {
		sb.WriteString(" WHEN ")
		if w.Condition != nil {
			sb.WriteString(w.Condition.String())
		}
		sb.WriteString(" THEN ")
		if w.Result != nil {
			sb.WriteString(w.Result.String())
		}
	}
	if c.ElseClause != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(c.ElseClause.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

// WhenClause represents a WHEN clause in a CASE expression.
type WhenClause struct {
	Condition Expression
	Result    Expression
}

// InExpr represents an IN expression.
type InExpr struct {
	Expr   Expression
	Values []Expression
	Select *SelectStmt
	Not    bool
}

func (i *InExpr) node()       {}
func (i *InExpr) expression() {}
func (i *InExpr) String() string {
	var sb strings.Builder
	if i.Expr != nil {
		sb.WriteString(i.Expr.String())
	}
	if i.Not {
		sb.WriteString(" NOT")
	}
	sb.WriteString(" IN (")
	var vals []string
	for _, v := range i.Values {
		if v != nil {
			vals = append(vals, v.String())
		}
	}
	sb.WriteString(strings.Join(vals, ", "))
	sb.WriteString(")")
	return sb.String()
}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
	Not   bool
}

func (b *BetweenExpr) node()       {}
func (b *BetweenExpr) expression() {}
func (b *BetweenExpr) String() string {
	var sb strings.Builder
	if b.Expr != nil {
		sb.WriteString(b.Expr.String())
	}
	if b.Not {
		sb.WriteString(" NOT")
	}
	sb.WriteString(" BETWEEN ")
	if b.Lower != nil {
		sb.WriteString(b.Lower.String())
	}
	sb.WriteString(" AND ")
	if b.Upper != nil {
		sb.WriteString(b.Upper.String())
	}
	return sb.String()
}

// CastExpr represents a CAST expression.
type CastExpr struct {
	Expr Expression
	Type string
}

func (c *CastExpr) node()       {}
func (c *CastExpr) expression() {}
func (c *CastExpr) String() string {
	expr := "nil"
	if c.Expr != nil {
		expr = c.Expr.String()
	}
	return "CAST(" + expr + " AS " + c.Type + ")"
}

// CollateExpr represents a COLLATE expression.
type CollateExpr struct {
	Expr      Expression
	Collation string
}

func (c *CollateExpr) node()       {}
func (c *CollateExpr) expression() {}
func (c *CollateExpr) String() string {
	expr := "nil"
	if c.Expr != nil {
		expr = c.Expr.String()
	}
	return expr + " COLLATE " + c.Collation
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expression
}

func (p *ParenExpr) node()       {}
func (p *ParenExpr) expression() {}
func (p *ParenExpr) String() string {
	expr := "nil"
	if p.Expr != nil {
		expr = p.Expr.String()
	}
	return "(" + expr + ")"
}

// SubqueryExpr represents a subquery expression.
type SubqueryExpr struct {
	Select *SelectStmt
}

func (s *SubqueryExpr) node()       {}
func (s *SubqueryExpr) expression() {}
func (s *SubqueryExpr) String() string {
	// We can't easily convert a full SELECT statement to string here,
	// so we return a placeholder. Full subquery serialization would require
	// implementing String() on SelectStmt as well.
	return "(SELECT ...)"
}

// ExistsExpr represents an EXISTS (SELECT ...) expression.
type ExistsExpr struct {
	Select *SelectStmt
	Not    bool // true for NOT EXISTS
}

func (e *ExistsExpr) node()       {}
func (e *ExistsExpr) expression() {}
func (e *ExistsExpr) String() string {
	if e.Not {
		return "NOT EXISTS (SELECT ...)"
	}
	return "EXISTS (SELECT ...)"
}

// VariableExpr represents a parameter placeholder.
type VariableExpr struct {
	Name string
}

func (v *VariableExpr) node()       {}
func (v *VariableExpr) expression() {}
func (v *VariableExpr) String() string {
	// SQL parameters are typically represented as ? or :name or $name
	if v.Name == "" {
		return "?"
	}
	return ":" + v.Name
}

// =============================================================================
// UPSERT (ON CONFLICT) Clause
// =============================================================================

// UpsertClause represents an ON CONFLICT clause in an INSERT statement.
type UpsertClause struct {
	Target *ConflictTarget
	Action ConflictAction
	Update *DoUpdateClause
}

// ConflictTarget specifies which conflict to handle.
type ConflictTarget struct {
	Columns        []IndexedColumn // columns for ON CONFLICT (col1, col2)
	Where          Expression      // WHERE clause for partial indexes
	ConstraintName string          // ON CONSTRAINT name
}

// ConflictAction specifies what to do on conflict.
type ConflictAction int

const (
	ConflictDoNothing ConflictAction = iota
	ConflictDoUpdate
)

// DoUpdateClause represents DO UPDATE SET clause.
type DoUpdateClause struct {
	Sets  []Assignment // SET column = value
	Where Expression   // WHERE clause for conditional updates
}
