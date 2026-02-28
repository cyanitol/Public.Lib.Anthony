package vtab

// ConstraintOp represents the type of constraint operation in a WHERE clause.
type ConstraintOp int

const (
	// Constraint operation types - these match SQLite's SQLITE_INDEX_CONSTRAINT_* constants
	ConstraintEQ         ConstraintOp = 2   // =
	ConstraintGT         ConstraintOp = 4   // >
	ConstraintLE         ConstraintOp = 8   // <=
	ConstraintLT         ConstraintOp = 16  // <
	ConstraintGE         ConstraintOp = 32  // >=
	ConstraintMatch      ConstraintOp = 64  // MATCH
	ConstraintLike       ConstraintOp = 65  // LIKE
	ConstraintGlob       ConstraintOp = 66  // GLOB
	ConstraintRegexp     ConstraintOp = 67  // REGEXP
	ConstraintNE         ConstraintOp = 68  // != or <>
	ConstraintIsNot      ConstraintOp = 69  // IS NOT
	ConstraintIsNotNull  ConstraintOp = 70  // IS NOT NULL
	ConstraintIsNull     ConstraintOp = 71  // IS NULL
	ConstraintIs         ConstraintOp = 72  // IS
	ConstraintLimit      ConstraintOp = 73  // LIMIT
	ConstraintOffset     ConstraintOp = 74  // OFFSET
	ConstraintFunction   ConstraintOp = 150 // Function constraint (undocumented)
)

// String returns a string representation of the constraint operation.
func (op ConstraintOp) String() string {
	switch op {
	case ConstraintEQ:
		return "="
	case ConstraintGT:
		return ">"
	case ConstraintLE:
		return "<="
	case ConstraintLT:
		return "<"
	case ConstraintGE:
		return ">="
	case ConstraintMatch:
		return "MATCH"
	case ConstraintLike:
		return "LIKE"
	case ConstraintGlob:
		return "GLOB"
	case ConstraintRegexp:
		return "REGEXP"
	case ConstraintNE:
		return "!="
	case ConstraintIsNot:
		return "IS NOT"
	case ConstraintIsNotNull:
		return "IS NOT NULL"
	case ConstraintIsNull:
		return "IS NULL"
	case ConstraintIs:
		return "IS"
	case ConstraintLimit:
		return "LIMIT"
	case ConstraintOffset:
		return "OFFSET"
	default:
		return "UNKNOWN"
	}
}

// IndexConstraint represents a single constraint from a WHERE clause.
type IndexConstraint struct {
	// Column is the column index on the left-hand side of the constraint.
	// The leftmost column is column 0. If the constraint is on the rowid, Column will be -1.
	Column int

	// Op is the constraint operator (=, <, >, etc.)
	Op ConstraintOp

	// Usable is true if the constraint can be used by the virtual table.
	// If false, the constraint must be checked by the SQLite core after reading the data.
	Usable bool

	// Collation is the name of the collation sequence to use for string comparisons.
	// May be empty for non-string comparisons.
	Collation string
}

// IndexConstraintUsage describes how a constraint will be used by the virtual table.
type IndexConstraintUsage struct {
	// ArgvIndex determines the order of constraint values passed to Filter().
	// If ArgvIndex > 0, this constraint's value will be passed as argv[ArgvIndex-1].
	// If ArgvIndex = 0, this constraint will not be used (must be checked by SQLite core).
	ArgvIndex int

	// Omit indicates whether the constraint can be assumed to be fully handled by the virtual table.
	// If true, SQLite will not double-check the constraint.
	Omit bool
}

// OrderBy represents an ORDER BY term.
type OrderBy struct {
	// Column is the column number for this ORDER BY term.
	Column int

	// Desc is true if the sort order is descending, false for ascending.
	Desc bool
}

// IndexInfo contains information about query constraints and ordering.
// This is passed to VirtualTable.BestIndex() to help the virtual table
// choose the most efficient query plan.
type IndexInfo struct {
	// Inputs from SQLite query planner:

	// Constraints is the array of WHERE clause constraints available to the virtual table.
	Constraints []IndexConstraint

	// OrderBy is the array of ORDER BY terms.
	OrderBy []OrderBy

	// ColUsed is a bitmask of columns used by the statement.
	// Bit 0 corresponds to column 0, bit 1 to column 1, etc.
	// If bit 63 is set, it means column 63 or higher is used.
	ColUsed uint64

	// Outputs to SQLite query planner:

	// ConstraintUsage describes how each constraint will be used.
	// This array must be the same length as Constraints.
	// Virtual tables should populate this to indicate which constraints they can handle.
	ConstraintUsage []IndexConstraintUsage

	// IdxNum is an arbitrary integer that SQLite passes to Filter().
	// The virtual table can use this to identify which index/strategy to use.
	IdxNum int

	// IdxStr is an arbitrary string that SQLite passes to Filter().
	// The virtual table can use this to store additional query plan information.
	IdxStr string

	// OrderByConsumed should be set to true if the virtual table will output
	// rows in the order specified by the OrderBy array. If true, SQLite won't
	// perform its own sorting.
	OrderByConsumed bool

	// EstimatedCost is the estimated cost of using this index.
	// Lower is better. SQLite uses this to choose between different query plans.
	EstimatedCost float64

	// EstimatedRows is the estimated number of rows returned.
	// Used by the query planner for optimization.
	EstimatedRows int64

	// IdxFlags are flags that provide additional information to SQLite.
	IdxFlags int

	// DistinctColumns indicates which columns must be distinct for DISTINCT queries.
	// If 0, no DISTINCT optimization is used.
	DistinctColumns int
}

// NewIndexInfo creates a new IndexInfo with the given number of constraints.
func NewIndexInfo(numConstraints int) *IndexInfo {
	return &IndexInfo{
		Constraints:     make([]IndexConstraint, numConstraints),
		ConstraintUsage: make([]IndexConstraintUsage, numConstraints),
		OrderBy:         make([]OrderBy, 0),
		EstimatedCost:   1000000.0, // Default: assume expensive full table scan
		EstimatedRows:   1000000,   // Default: assume large table
	}
}

// SetConstraintUsage sets the usage information for a constraint.
// This is a convenience method for virtual table implementations.
func (info *IndexInfo) SetConstraintUsage(index int, argvIndex int, omit bool) {
	if index >= 0 && index < len(info.ConstraintUsage) {
		info.ConstraintUsage[index].ArgvIndex = argvIndex
		info.ConstraintUsage[index].Omit = omit
	}
}

// CountUsableConstraints returns the number of usable constraints.
func (info *IndexInfo) CountUsableConstraints() int {
	count := 0
	for _, c := range info.Constraints {
		if c.Usable {
			count++
		}
	}
	return count
}

// FindConstraint finds the first constraint matching the given column and operator.
// Returns the index of the constraint, or -1 if not found.
func (info *IndexInfo) FindConstraint(column int, op ConstraintOp) int {
	for i, c := range info.Constraints {
		if c.Column == column && c.Op == op && c.Usable {
			return i
		}
	}
	return -1
}

// HasOrderBy returns true if there are any ORDER BY terms.
func (info *IndexInfo) HasOrderBy() bool {
	return len(info.OrderBy) > 0
}

// IsColumnUsed returns true if the specified column is used in the query.
func (info *IndexInfo) IsColumnUsed(column int) bool {
	if column < 0 || column >= 64 {
		// For columns >= 64, bit 63 being set indicates usage
		if column >= 64 {
			return (info.ColUsed & (1 << 63)) != 0
		}
		return false
	}
	return (info.ColUsed & (1 << uint(column))) != 0
}
