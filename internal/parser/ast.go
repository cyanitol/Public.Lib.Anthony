package parser

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
	Distinct bool
	Columns  []ResultColumn
	From     *FromClause
	Where    Expression
	GroupBy  []Expression
	Having   Expression
	OrderBy  []OrderingTerm
	Limit    Expression
	Offset   Expression
	Compound *CompoundSelect // For UNION, EXCEPT, INTERSECT
}

func (s *SelectStmt) node()      {}
func (s *SelectStmt) statement() {}
func (s *SelectStmt) String() string {
	return "SELECT"
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
	TableName string
	Alias     string
	Subquery  *SelectStmt
	Indexed   string // index name for INDEXED BY
}

// JoinClause represents a JOIN clause.
type JoinClause struct {
	Type      JoinType
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
	Expr Expression
	Asc  bool
}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Table       string
	Columns     []string
	Values      [][]Expression
	Select      *SelectStmt
	OnConflict  OnConflictClause
	DefaultVals bool
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
	Order  SortOrder
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
	return "BinaryExpr"
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

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expression
}

func (u *UnaryExpr) node()       {}
func (u *UnaryExpr) expression() {}
func (u *UnaryExpr) String() string {
	return "UnaryExpr"
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
	return "Literal:" + l.Value
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
	return "Function:" + f.Name
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
	return "CASE"
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
	return "IN"
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
	return "BETWEEN"
}

// CastExpr represents a CAST expression.
type CastExpr struct {
	Expr Expression
	Type string
}

func (c *CastExpr) node()       {}
func (c *CastExpr) expression() {}
func (c *CastExpr) String() string {
	return "CAST"
}

// CollateExpr represents a COLLATE expression.
type CollateExpr struct {
	Expr      Expression
	Collation string
}

func (c *CollateExpr) node()       {}
func (c *CollateExpr) expression() {}
func (c *CollateExpr) String() string {
	return "COLLATE"
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expression
}

func (p *ParenExpr) node()       {}
func (p *ParenExpr) expression() {}
func (p *ParenExpr) String() string {
	return "Paren"
}

// SubqueryExpr represents a subquery expression.
type SubqueryExpr struct {
	Select *SelectStmt
}

func (s *SubqueryExpr) node()       {}
func (s *SubqueryExpr) expression() {}
func (s *SubqueryExpr) String() string {
	return "Subquery"
}

// VariableExpr represents a parameter placeholder.
type VariableExpr struct {
	Name string
}

func (v *VariableExpr) node()       {}
func (v *VariableExpr) expression() {}
func (v *VariableExpr) String() string {
	return "Variable:" + v.Name
}
