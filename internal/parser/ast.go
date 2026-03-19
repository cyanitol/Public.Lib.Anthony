// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import "strings"

// Node is the interface that all AST nodes implement.
type Node interface {
	node()
	String() string
}

// Statement represents a SQL statement.
type Statement interface {
	Node
	statement()
}

// Expression represents a SQL expression.
type Expression interface {
	Node
	expression()
}

// =============================================================================
// Statements
// =============================================================================

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	With     *WithClause // Common Table Expressions (WITH clause)
	Distinct bool
	Columns  []ResultColumn
	From     *FromClause
	Where    Expression
	GroupBy  []Expression
	Having   Expression
	OrderBy    []OrderingTerm
	Limit      Expression
	Offset     Expression
	WindowDefs []WindowDef        // Named window definitions (WINDOW clause)
	Compound   *CompoundSelect    // For UNION, EXCEPT, INTERSECT
}

// WindowDef represents a named window definition in a WINDOW clause.
type WindowDef struct {
	Name string
	Spec *WindowSpec
}

func (s *SelectStmt) node()      {}
func (s *SelectStmt) statement() {}
func (s *SelectStmt) String() string {
	return "SELECT"
}

// WithClause represents a WITH clause containing Common Table Expressions.
type WithClause struct {
	Recursive bool
	CTEs      []CTE
}

// CTE represents a Common Table Expression.
type CTE struct {
	Name    string
	Columns []string    // Optional column list
	Select  *SelectStmt // The SELECT query defining the CTE
}

// CompoundSelect represents compound SELECT operators (UNION, EXCEPT, INTERSECT).
type CompoundSelect struct {
	Op    CompoundOp
	Left  *SelectStmt
	Right *SelectStmt
}

type CompoundOp int

const (
	CompoundUnion CompoundOp = iota
	CompoundUnionAll
	CompoundExcept
	CompoundIntersect
)

func (op CompoundOp) String() string {
	switch op {
	case CompoundUnion:
		return "UNION"
	case CompoundUnionAll:
		return "UNION ALL"
	case CompoundExcept:
		return "EXCEPT"
	case CompoundIntersect:
		return "INTERSECT"
	default:
		return "UNKNOWN"
	}
}

// ResultColumn represents a column in the SELECT clause.
type ResultColumn struct {
	Expr  Expression
	Alias string
	Star  bool   // true for SELECT *
	Table string // for SELECT table.*
}

// FromClause represents the FROM clause.
type FromClause struct {
	Tables []TableOrSubquery
	Joins  []JoinClause
}

// TableOrSubquery represents a table or subquery in FROM.
type TableOrSubquery struct {
	Schema    string // optional schema name for qualified table references
	TableName string
	Alias     string
	Subquery  *SelectStmt
	Indexed   string // index name for INDEXED BY
	FuncArgs  []Expression // arguments for table-valued functions (e.g., json_each('...'))
}

// JoinClause represents a JOIN clause.
type JoinClause struct {
	Type      JoinType
	Natural   bool
	Table     TableOrSubquery
	Condition JoinCondition
}

type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
	JoinCross
)

// JoinCondition represents ON or USING join condition.
type JoinCondition struct {
	On    Expression
	Using []string
}

// OrderingTerm represents an ORDER BY term.
type OrderingTerm struct {
	Expr      Expression
	Asc       bool
	Collation string // COLLATE clause
}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Schema      string // optional schema name for qualified table references
	Table       string
	Columns     []string
	Values      [][]Expression
	Select      *SelectStmt
	OnConflict  OnConflictClause
	DefaultVals bool
	Upsert      *UpsertClause
}

func (i *InsertStmt) node()      {}
func (i *InsertStmt) statement() {}
func (i *InsertStmt) String() string {
	return "INSERT"
}

type OnConflictClause int

const (
	OnConflictNone OnConflictClause = iota
	OnConflictRollback
	OnConflictAbort
	OnConflictFail
	OnConflictIgnore
	OnConflictReplace
)

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Schema     string // optional schema name for qualified table references
	Table      string
	Sets       []Assignment
	Where      Expression
	OrderBy    []OrderingTerm
	Limit      Expression
	OnConflict OnConflictClause
}

func (u *UpdateStmt) node()      {}
func (u *UpdateStmt) statement() {}
func (u *UpdateStmt) String() string {
	return "UPDATE"
}

// Assignment represents a column assignment in UPDATE.
type Assignment struct {
	Column string
	Value  Expression
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Schema  string // optional schema name for qualified table references
	Table   string
	Where   Expression
	OrderBy []OrderingTerm
	Limit   Expression
}

func (d *DeleteStmt) node()      {}
func (d *DeleteStmt) statement() {}
func (d *DeleteStmt) String() string {
	return "DELETE"
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Schema       string // optional schema name for qualified table references
	Name         string
	IfNotExists  bool
	Temp         bool
	Columns      []ColumnDef
	Constraints  []TableConstraint
	Select       *SelectStmt
	WithoutRowID bool
	Strict       bool
}

func (c *CreateTableStmt) node()      {}
func (c *CreateTableStmt) statement() {}
func (c *CreateTableStmt) String() string {
	return "CREATE TABLE"
}

// ColumnDef represents a column definition.
type ColumnDef struct {
	Name        string
	Type        string
	Constraints []ColumnConstraint
}

// ColumnConstraint represents a column constraint.
type ColumnConstraint struct {
	Type       ConstraintType
	Name       string
	PrimaryKey *PrimaryKeyConstraint
	NotNull    bool
	Unique     bool
	Check      Expression
	Default    Expression
	Collate    string
	ForeignKey *ForeignKeyConstraint
	Generated  *GeneratedConstraint
}

type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintNotNull
	ConstraintUnique
	ConstraintCheck
	ConstraintDefault
	ConstraintCollate
	ConstraintForeignKey
	ConstraintGenerated
)

// PrimaryKeyConstraint represents a PRIMARY KEY constraint.
type PrimaryKeyConstraint struct {
	Autoincrement bool
	Order         SortOrder
	OnConflict    OnConflictClause
}

type SortOrder int

const (
	SortDefault SortOrder = iota
	SortAsc
	SortDesc
)

// ForeignKeyConstraint represents a FOREIGN KEY constraint.
type ForeignKeyConstraint struct {
	Table      string
	Columns    []string
	OnDelete   ForeignKeyAction
	OnUpdate   ForeignKeyAction
	Match      string
	Deferrable DeferrableMode
}

type ForeignKeyAction int

const (
	FKActionNone ForeignKeyAction = iota
	FKActionSetNull
	FKActionSetDefault
	FKActionCascade
	FKActionRestrict
	FKActionNoAction
)

type DeferrableMode int

const (
	DeferrableNone DeferrableMode = iota
	DeferrableInitiallyDeferred
	DeferrableInitiallyImmediate
)

// GeneratedConstraint represents a GENERATED ALWAYS AS constraint.
type GeneratedConstraint struct {
	Expr    Expression
	Stored  bool
	Virtual bool
}

// TableConstraint represents a table-level constraint.
type TableConstraint struct {
	Type       ConstraintType
	Name       string
	PrimaryKey *PrimaryKeyTableConstraint
	Unique     *UniqueTableConstraint
	Check      Expression
	ForeignKey *ForeignKeyTableConstraint
}

// PrimaryKeyTableConstraint represents a table-level PRIMARY KEY constraint.
type PrimaryKeyTableConstraint struct {
	Columns    []IndexedColumn
	OnConflict OnConflictClause
}

// UniqueTableConstraint represents a table-level UNIQUE constraint.
type UniqueTableConstraint struct {
	Columns    []IndexedColumn
	OnConflict OnConflictClause
}

// ForeignKeyTableConstraint represents a table-level FOREIGN KEY constraint.
type ForeignKeyTableConstraint struct {
	Columns    []string
	ForeignKey ForeignKeyConstraint
}

// IndexedColumn represents a column in an index.
type IndexedColumn struct {
	Column string
	Expr   Expression // Expression for expression indexes (nil for simple column references)
	Order  SortOrder
}

// CreateVirtualTableStmt represents a CREATE VIRTUAL TABLE statement.
type CreateVirtualTableStmt struct {
	Name        string
	IfNotExists bool
	Module      string
	Args        []string
}

func (c *CreateVirtualTableStmt) node()      {}
func (c *CreateVirtualTableStmt) statement() {}
func (c *CreateVirtualTableStmt) String() string {
	return "CREATE VIRTUAL TABLE"
}

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	Name     string
	IfExists bool
}

func (d *DropTableStmt) node()      {}
func (d *DropTableStmt) statement() {}
func (d *DropTableStmt) String() string {
	return "DROP TABLE"
}

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	Name        string
	Table       string
	Columns     []IndexedColumn
	Unique      bool
	IfNotExists bool
	Where       Expression
}

func (c *CreateIndexStmt) node()      {}
func (c *CreateIndexStmt) statement() {}
func (c *CreateIndexStmt) String() string {
	return "CREATE INDEX"
}

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	Name     string
	IfExists bool
}

func (d *DropIndexStmt) node()      {}
func (d *DropIndexStmt) statement() {}
func (d *DropIndexStmt) String() string {
	return "DROP INDEX"
}

// CreateViewStmt represents a CREATE VIEW statement.
type CreateViewStmt struct {
	Name        string
	Columns     []string
	Select      *SelectStmt
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

// SavepointStmt represents a SAVEPOINT statement.
type SavepointStmt struct {
	Name string
}

func (s *SavepointStmt) node()      {}
func (s *SavepointStmt) statement() {}
func (s *SavepointStmt) String() string {
	return "SAVEPOINT"
}

// ReleaseStmt represents a RELEASE [SAVEPOINT] statement.
type ReleaseStmt struct {
	Name string
}

func (r *ReleaseStmt) node()      {}
func (r *ReleaseStmt) statement() {}
func (r *ReleaseStmt) String() string {
	return "RELEASE"
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

// ReindexStmt represents a REINDEX statement that rebuilds indexes.
type ReindexStmt struct {
	Schema string
	Name   string
}

func (r *ReindexStmt) node()      {}
func (r *ReindexStmt) statement() {}
func (r *ReindexStmt) String() string {
	return "REINDEX"
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
	Left   Expression
	Op     BinaryOp
	Right  Expression
	Escape Expression // optional ESCAPE clause for LIKE
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
	OpIs    // a IS b  (null-safe equality)
	OpIsNot // a IS NOT b (null-safe inequality)
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
	OpIs:     "IS",
	OpIsNot:  "IS NOT",
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
	BaseName    string         // Reference to a named window (OVER w)
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
// RAISE Expression (for trigger bodies)
// =============================================================================

// RaiseType represents the type of RAISE function in a trigger.
type RaiseType int

const (
	RaiseIgnore   RaiseType = iota // RAISE(IGNORE) - skip the rest of the trigger
	RaiseRollback                  // RAISE(ROLLBACK, msg) - rollback transaction
	RaiseAbort                     // RAISE(ABORT, msg) - abort current statement
	RaiseFail                      // RAISE(FAIL, msg) - fail but keep prior changes
)

// RaiseExpr represents a RAISE function call within a trigger body.
type RaiseExpr struct {
	Type    RaiseType // The raise action type
	Message string    // Error message (empty for IGNORE)
}

func (r *RaiseExpr) node()       {}
func (r *RaiseExpr) expression() {}
func (r *RaiseExpr) String() string {
	switch r.Type {
	case RaiseIgnore:
		return "RAISE(IGNORE)"
	case RaiseRollback:
		return "RAISE(ROLLBACK, " + r.Message + ")"
	case RaiseAbort:
		return "RAISE(ABORT, " + r.Message + ")"
	case RaiseFail:
		return "RAISE(FAIL, " + r.Message + ")"
	default:
		return "RAISE(UNKNOWN)"
	}
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
